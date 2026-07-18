"""AuthStrategy interface + factory.

A strategy contributes request headers. Token-based strategies fetch and cache
tokens lazily using a shared httpx.AsyncClient passed at call time, so the
connection layer stays uniform across open and authenticated endpoints.
"""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import httpx

    from ..config import Endpoint, Settings


class AuthStrategy(abc.ABC):
    """Adds authentication to outgoing FHIR requests."""

    name: str = "abstract"

    @abc.abstractmethod
    async def headers(self, client: "httpx.AsyncClient") -> dict[str, str]:
        """Return headers to merge into a request (may fetch/refresh a token)."""

    def describe(self) -> str:
        return self.name


def build_auth(endpoint: "Endpoint", settings: "Settings") -> AuthStrategy:
    """Construct the AuthStrategy for an endpoint from its manifest config."""
    # Imported lazily to avoid a circular import at module load.
    from .strategies import (
        ApiKeyHeader,
        ClientIdHeader,
        ClientIdSecretHeaders,
        HealthSparqPublicToken,
        NoAuth,
        OAuth2ClientCredentials,
    )

    auth = endpoint.auth
    strat = auth.strategy

    if strat == "none":
        return NoAuth()

    if strat == "client_id_header":
        header_name = auth.header_name or "X-IBM-Client-ID"
        key = auth.secret_keys[0] if auth.secret_keys else None
        return ClientIdHeader(header_name=header_name, client_id=settings.secret(key) if key else "")

    if strat == "client_id_secret_headers":
        cid = settings.secret(auth.secret_keys[0]) if len(auth.secret_keys) > 0 else ""
        csec = settings.secret(auth.secret_keys[1]) if len(auth.secret_keys) > 1 else ""
        return ClientIdSecretHeaders(client_id=cid, client_secret=csec)

    if strat == "api_key_header":
        header_name = auth.header_name or "apikey"
        key = auth.secret_keys[0] if auth.secret_keys else None
        return ApiKeyHeader(header_name=header_name, api_key=settings.secret(key) if key else "")

    if strat == "oauth2_client_credentials":
        cid = settings.secret(auth.secret_keys[0]) if len(auth.secret_keys) > 0 else ""
        csec = settings.secret(auth.secret_keys[1]) if len(auth.secret_keys) > 1 else ""
        return OAuth2ClientCredentials(
            token_url=auth.token_url or "",
            client_id=cid,
            client_secret=csec,
        )

    if strat == "healthsparq_public_token":
        # Public tenant codes live in the manifest (auth.params), not .env —
        # they are public, not secrets. Fall back to Regence env keys for the
        # original placeholder entry.
        p = auth.params or {}
        return HealthSparqPublicToken(
            token_url=auth.token_url or "",
            insurer_code=p.get("insurer_code") or settings.secret("REGENCE_INSURER_CODE"),
            brand_code=p.get("brand_code") or settings.secret("REGENCE_BRAND_CODE"),
            product_code=p.get("product_code"),
        )

    raise ValueError(f"Unknown auth strategy: {strat!r}")
