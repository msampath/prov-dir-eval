"""FHIR HTTP client: async httpx + tenacity retry + per-host politeness.

`FhirClient` binds an :class:`Endpoint` to an :class:`AuthStrategy` and a shared
httpx.AsyncClient. It applies known server quirks (e.g. Optum's bare-Practitioner
500) and exposes paginating helpers that follow ``Bundle.link[next]``.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, AsyncIterator, Optional

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from .auth import AuthStrategy, build_auth
from .config import Endpoint, Settings, get_settings, load_manifest
from .logging_setup import get_logger

log = get_logger(__name__)

FHIR_ACCEPT = "application/fhir+json"


class _HostLimiter:
    """Per-host concurrency cap + minimum interval between request starts."""

    def __init__(self, concurrency: int, min_interval: float) -> None:
        self._sem = asyncio.Semaphore(max(1, concurrency))
        self._min_interval = max(0.0, min_interval)
        self._last_start = 0.0
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "_HostLimiter":
        await self._sem.acquire()
        if self._min_interval > 0:
            async with self._lock:
                now = time.monotonic()
                wait = self._min_interval - (now - self._last_start)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_start = time.monotonic()
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._sem.release()


# One limiter per host, shared process-wide.
_host_limiters: dict[str, _HostLimiter] = {}


def _limiter_for(endpoint: Endpoint, settings: Settings) -> _HostLimiter:
    """One limiter per host; endpoint quirks override the global politeness
    (rate-limited APIs like HealthPartners' 100 req/hour).

    Several payers share a host (api-gateway.healthsparq.com, flex.optum.com,
    api-ext.amerihealthcaritas.com) and may declare DIFFERENT politeness. The
    limiter is created once per host, so taking whichever endpoint happened to
    build it first would silently discard a stricter sibling's cap and make the
    effective rate depend on scheduling order. Resolve the strictest declared
    politeness across every manifest endpoint on this host instead.
    """
    host = endpoint.host
    if host not in _host_limiters:
        concurrency = endpoint.quirks.max_concurrency or settings.http_per_host_concurrency
        interval = (
            endpoint.quirks.min_request_interval
            if endpoint.quirks.min_request_interval is not None
            else settings.http_per_host_min_interval
        )
        try:
            siblings = [e for e in load_manifest().endpoints if e.host == host]
        except Exception:  # noqa: BLE001 - a manifest problem must not break requests
            siblings = []
        for sib in siblings:
            q = sib.quirks
            if q.max_concurrency:
                concurrency = min(concurrency, q.max_concurrency)
            if q.min_request_interval is not None:
                interval = max(interval, q.min_request_interval)
        _host_limiters[host] = _HostLimiter(concurrency=concurrency, min_interval=interval)
    return _host_limiters[host]


def _is_retryable(exc: BaseException) -> bool:
    # TimeoutError covers our asyncio.wait_for total-timeout (a slow-trickle/held-
    # open response that httpx's read-timeout never trips). Treated as transient.
    if isinstance(exc, (httpx.TransportError, httpx.TimeoutException, TimeoutError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code == 429 or 500 <= code < 600
    return False


class FhirError(Exception):
    """A FHIR request failed after retries."""

    def __init__(self, message: str, status: Optional[int] = None) -> None:
        super().__init__(message)
        self.status = status


class FhirClient:
    def __init__(
        self,
        endpoint: Endpoint,
        client: httpx.AsyncClient,
        auth: AuthStrategy,
        settings: Settings,
    ) -> None:
        self.endpoint = endpoint
        self._client = client
        self._auth = auth
        self._settings = settings
        self._limiter = _limiter_for(endpoint, settings)

    # -- request primitive ---------------------------------------------------
    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        retries = self._settings.http_max_retries
        retrying = AsyncRetrying(
            stop=stop_after_attempt(retries + 1),
            wait=wait_exponential(multiplier=1, min=1, max=12),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        )
        async for attempt in retrying:
            with attempt:
                async with self._limiter:
                    auth_headers = await self._auth.headers(self._client)
                    headers = {"Accept": FHIR_ACCEPT, **auth_headers}
                    if self.endpoint.quirks.user_agent:
                        headers["User-Agent"] = self.endpoint.quirks.user_agent
                    headers.update(kwargs.pop("headers", {}))
                    started = time.monotonic()
                    # Per-endpoint timeout overrides for slow servers; else global.
                    q = self.endpoint.quirks
                    read_to = q.read_timeout or self._settings.http_timeout_seconds
                    total_to = q.request_timeout or self._settings.http_total_timeout
                    # Total wall-clock timeout: httpx's read-timeout only fires on a
                    # gap between bytes, so a slow-trickle / held-open response can hang
                    # forever and starve the per-host concurrency slots. wait_for aborts
                    # it (and frees the slot) regardless.
                    resp = await asyncio.wait_for(
                        self._client.request(
                            method, url, headers=headers,
                            timeout=httpx.Timeout(read_to), **kwargs,
                        ),
                        timeout=total_to,
                    )
                    elapsed_ms = round((time.monotonic() - started) * 1000)
                    log.debug(
                        "%s %s -> %s (%dms)",
                        method,
                        url,
                        resp.status_code,
                        elapsed_ms,
                        extra={"payer": self.endpoint.key, "status": resp.status_code},
                    )
                    if resp.status_code == 429 or 500 <= resp.status_code < 600:
                        resp.raise_for_status()
                    return resp
        raise FhirError(f"exhausted retries for {url}")  # pragma: no cover

    async def get_json(self, path_or_url: str, params: Optional[dict] = None) -> dict:
        url = path_or_url if path_or_url.startswith("http") else f"{self.endpoint.base_url}/{path_or_url.lstrip('/')}"
        resp = await self._request("GET", url, params=params)
        if resp.status_code >= 400:
            raise FhirError(f"GET {url} returned {resp.status_code}", status=resp.status_code)
        return resp.json()

    # -- FHIR helpers --------------------------------------------------------
    async def metadata(self) -> dict:
        return await self.get_json("metadata")

    async def liveness(self) -> dict:
        """Confirm the server is alive without /metadata.

        Used for endpoints whose gateway doesn't register /metadata (Banner).
        Tries each configured liveness resource until one returns a Bundle.
        Returns ``{"ok", "resource", "total", "returned"}`` or raises FhirError.
        """
        last_exc: Optional[Exception] = None
        for resource in self.endpoint.quirks.liveness_resources:
            try:
                bundle = await self.search_page(resource, {"_count": 1})
                if bundle.get("resourceType") == "Bundle":
                    return {
                        "ok": True,
                        "resource": resource,
                        "total": bundle.get("total"),
                        "returned": len(bundle.get("entry", []) or []),
                    }
            except Exception as exc:  # noqa: BLE001 - try the next resource
                last_exc = exc
        raise FhirError(
            f"liveness failed for {self.endpoint.key}: no resource responded"
            + (f" (last: {last_exc})" if last_exc else "")
        )

    def _apply_quirks(self, resource_type: str, params: dict) -> dict:
        params = dict(params or {})
        q = self.endpoint.quirks
        if resource_type == "Practitioner" and q.practitioner_requires_filter:
            filt = q.default_practitioner_filter or "address-state=FL"
            key, _, val = filt.partition("=")
            params.setdefault(key, val)
        if "_count" not in params:
            # Per-resource override, then per-endpoint, then the global default.
            params["_count"] = (
                q.page_size_by_resource.get(resource_type)
                or q.page_size
                or self._settings.http_default_count
            )
        return params

    async def search_page(self, resource_type: str, params: Optional[dict] = None) -> dict:
        """Single search page; returns a FHIR Bundle dict."""
        params = self._apply_quirks(resource_type, params or {})
        return await self.get_json(resource_type, params=params)

    async def iter_bundles(
        self,
        resource_type: str,
        params: Optional[dict] = None,
        max_pages: Optional[int] = None,
    ) -> AsyncIterator[dict]:
        """Yield successive Bundle pages, following ``link[rel=next]``."""
        bundle = await self.search_page(resource_type, params)
        page = 0
        while True:
            yield bundle
            page += 1
            if max_pages is not None and page >= max_pages:
                return
            next_url = _next_link(bundle)
            if not next_url:
                return
            bundle = await self.get_json(next_url)

    async def iter_resources(
        self,
        resource_type: str,
        params: Optional[dict] = None,
        max_pages: Optional[int] = None,
    ) -> AsyncIterator[dict]:
        async for bundle in self.iter_bundles(resource_type, params, max_pages=max_pages):
            for entry in bundle.get("entry", []) or []:
                res = entry.get("resource")
                if res:
                    yield res


def _next_link(bundle: dict) -> Optional[str]:
    for link in bundle.get("link", []) or []:
        if link.get("relation") == "next":
            return link.get("url")
    return None


class FhirSession:
    """Async-context manager owning the shared httpx client for a set of endpoints."""

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or get_settings()
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "FhirSession":
        limits = httpx.Limits(max_connections=64, max_keepalive_connections=32)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.settings.http_timeout_seconds),
            headers={"User-Agent": self.settings.http_user_agent},
            limits=limits,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()

    def client_for(self, endpoint: Endpoint) -> FhirClient:
        assert self._client is not None, "FhirSession must be entered before use"
        auth = build_auth(endpoint, self.settings)
        return FhirClient(endpoint, self._client, auth, self.settings)
