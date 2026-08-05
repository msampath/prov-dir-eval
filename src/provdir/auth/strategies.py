"""Concrete authentication strategies."""

from __future__ import annotations

import time
from typing import Optional

import httpx

from .base import AuthStrategy


class NoAuth(AuthStrategy):
    """Fully-open endpoints (Humana, Capital Blue, Excellus, BCBS-KS/MN, ...)."""

    name = "none"

    async def headers(self, client: httpx.AsyncClient) -> dict[str, str]:
        return {}


class ClientIdHeader(AuthStrategy):
    """Single public client-id header (Florida Blue: X-IBM-Client-ID)."""

    name = "client_id_header"

    def __init__(self, header_name: str, client_id: str) -> None:
        self.header_name = header_name
        self.client_id = client_id

    async def headers(self, client: httpx.AsyncClient) -> dict[str, str]:
        return {self.header_name: self.client_id} if self.client_id else {}

    def describe(self) -> str:
        return f"client_id_header({self.header_name})"


class ClientIdSecretHeaders(AuthStrategy):
    """ClientId + ClientSecret request headers (HCSC MuleSoft)."""

    name = "client_id_secret_headers"

    def __init__(self, client_id: str, client_secret: str,
                 id_header: str = "ClientId", secret_header: str = "ClientSecret") -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.id_header = id_header
        self.secret_header = secret_header

    async def headers(self, client: httpx.AsyncClient) -> dict[str, str]:
        if not (self.client_id and self.client_secret):
            return {}
        return {self.id_header: self.client_id, self.secret_header: self.client_secret}


class ApiKeyHeader(AuthStrategy):
    """Single API-key header (Highmark provider directory)."""

    name = "api_key_header"

    def __init__(self, header_name: str, api_key: str) -> None:
        self.header_name = header_name
        self.api_key = api_key

    async def headers(self, client: httpx.AsyncClient) -> dict[str, str]:
        return {self.header_name: self.api_key} if self.api_key else {}

    def describe(self) -> str:
        return f"api_key_header({self.header_name})"


class _CachedToken:
    def __init__(self) -> None:
        self.value: Optional[str] = None
        self.expires_at: float = 0.0

    def valid(self, skew: float = 30.0) -> bool:
        return bool(self.value) and (time.monotonic() + skew) < self.expires_at

    def set(self, value: str, ttl_seconds: float) -> None:
        self.value = value
        self.expires_at = time.monotonic() + ttl_seconds


class OAuth2ClientCredentials(AuthStrategy):
    """OAuth2 client-credentials grant with token caching.

    Used by Aetna, Arkansas BCBS, BCBS Louisiana, BCBS Alabama, BCBS SC.
    The token endpoint URL must be configured per payer (auth.token_url).
    """

    name = "oauth2_client_credentials"

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: Optional[str] = None,
    ) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self._token = _CachedToken()

    async def _fetch_token(self, client: httpx.AsyncClient) -> None:
        data = {"grant_type": "client_credentials"}
        if self.scope:
            data["scope"] = self.scope
        resp = await client.post(
            self.token_url,
            data=data,
            auth=(self.client_id, self.client_secret),
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        body = resp.json()
        token = body["access_token"]
        ttl = float(body.get("expires_in", 300))
        self._token.set(token, ttl)

    async def headers(self, client: httpx.AsyncClient) -> dict[str, str]:
        if not (self.token_url and self.client_id and self.client_secret):
            return {}
        if not self._token.valid():
            await self._fetch_token(client)
        return {"Authorization": f"Bearer {self._token.value}"}

    def describe(self) -> str:
        return f"oauth2_client_credentials({self.token_url})"


class HealthSparqPublicToken(AuthStrategy):
    """HealthSparq/Kyruus public token (Regence): POST body -> Subject-Token.

    The token is short-lived (~15 min) and returned in a response header.
    """

    name = "healthsparq_public_token"

    def __init__(
        self,
        token_url: str,
        insurer_code: str,
        brand_code: str,
        product_code: Optional[str] = None,
    ) -> None:
        self.token_url = token_url
        self.insurer_code = insurer_code
        self.brand_code = brand_code
        self.product_code = product_code
        self._token = _CachedToken()

    async def _fetch_token(self, client: httpx.AsyncClient) -> None:
        body = {"insurerCode": self.insurer_code, "brandCode": self.brand_code}
        if self.product_code:
            body["productCode"] = self.product_code
        resp = await client.post(
            self.token_url,
            json=body,
            headers={"Accept": "application/json"},
        )
        resp.raise_for_status()
        token = resp.headers.get("Subject-Token") or resp.json().get("token", "")
        # Refresh a little before the documented 15-minute expiry.
        self._token.set(token, ttl_seconds=14 * 60)

    async def headers(self, client: httpx.AsyncClient) -> dict[str, str]:
        if not (self.token_url and self.insurer_code):
            return {}
        if not self._token.valid():
            await self._fetch_token(client)
        return {"Subject-Token": self._token.value or ""}

    def describe(self) -> str:
        return f"healthsparq_public_token({self.token_url})"
