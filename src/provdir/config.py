"""Configuration: environment settings + endpoint manifest loading.

Two sources of truth:

* `.env` (loaded via pydantic-settings) -> :class:`Settings`: connection
  details, HTTP behaviour, and payer credentials.
* `config/endpoints.yaml` -> :class:`Endpoint` models: the runtime endpoint
  manifest (one entry per distinct base URL).

An endpoint is *runnable* only when every secret key its auth strategy needs is
present and non-empty in the environment. Endpoints missing credentials are
reported as ``skipped: missing-credentials`` rather than failing the run.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, Optional
from urllib.parse import quote_plus, urlparse

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from . import CONFIG_DIR, REPO_ROOT

AuthStrategyName = Literal[
    "none",
    "client_id_header",
    "client_id_secret_headers",
    "api_key_header",
    "oauth2_client_credentials",
    "healthsparq_public_token",
]

EndpointStatus = Literal["known", "unknown", "blocked"]


class Settings(BaseSettings):
    """Environment-backed settings loaded from `.env` / process env."""

    model_config = SettingsConfigDict(
        env_file=str(REPO_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="allow",  # tolerate the many per-payer secret keys
        case_sensitive=False,
    )

    # Postgres
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "provdir"
    postgres_user: str = "provdir"
    postgres_password: str = ""

    # HTTP client
    http_timeout_seconds: float = 30.0   # httpx read/connect timeout
    http_total_timeout: float = 60.0     # hard wall-clock cap per request (kills slow-trickle hangs)
    http_max_retries: int = 2
    http_per_host_concurrency: int = 8
    http_per_host_min_interval: float = 0.0
    # Default _count for searches. Many servers return a tiny default page
    # (bcbs_mn returns 50 with NO next link; premera returns 10) so a high
    # _count is essential for complete ingestion. Per-endpoint quirks.page_size
    # overrides this (e.g. Excellus caps at 25).
    http_default_count: int = 1000
    # Browser-prefixed UA: several payer WAFs (Cloudflare) 403 any UA that does
    # not begin with "Mozilla/". The "compatible" token keeps it honest.
    http_user_agent: str = (
        "Mozilla/5.0 (compatible; CMS-9115F-ProvDirEval/0.1; +https://github.com/prov-dir-eval)"
    )

    # Reference
    plannet_ig_version: str = "1.2.0"

    @property
    def sqlalchemy_url(self) -> str:
        # URL-encode user/password: payer-grade passwords often contain @ ! etc.
        user = quote_plus(self.postgres_user)
        pw = quote_plus(self.postgres_password)
        return (
            f"postgresql+psycopg://{user}:{pw}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def sqlalchemy_url_redacted(self) -> str:
        return (
            f"postgresql+psycopg://{self.postgres_user}:***"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    def secret(self, key: str) -> str:
        """Look up a payer secret by its .env key name (case-insensitive)."""
        # pydantic-settings lower-cases extras; also check raw env as a fallback.
        val = getattr(self, key.lower(), None)
        if val is None:
            val = os.environ.get(key) or os.environ.get(key.upper())
        return (val or "").strip()


@lru_cache
def get_settings() -> Settings:
    return Settings()


class AuthConfig(BaseModel):
    strategy: AuthStrategyName = "none"
    secret_keys: list[str] = Field(default_factory=list)
    header_name: Optional[str] = None
    token_url: Optional[str] = None
    # Non-secret, strategy-specific public parameters carried in the manifest
    # (e.g. HealthSparq insurer_code/brand_code/product_code — public tenant codes).
    params: dict = Field(default_factory=dict)


class Quirks(BaseModel):
    practitioner_requires_filter: bool = False
    default_practitioner_filter: Optional[str] = None
    page_size: Optional[int] = None
    total_cap: Optional[int] = None
    # Per-endpoint politeness overrides (rate-limited APIs, e.g. HealthPartners
    # at 100 req/hour): minimum seconds between request starts to this host, and
    # a per-host concurrency cap. None => the global settings apply.
    min_request_interval: Optional[float] = None
    max_concurrency: Optional[int] = None
    # Per-endpoint timeout overrides for SLOW servers (e.g. AmeriHealth LAEX,
    # whose pages exceed the default 30s read timeout and abort pagination).
    # read_timeout = httpx read/connect; request_timeout = hard wall-clock.
    read_timeout: Optional[float] = None
    request_timeout: Optional[float] = None
    # Some gateways (Banner/Innovaccer) do not register /metadata and 403 it.
    # Liveness is then confirmed by probing a resource instead.
    no_metadata: bool = False
    liveness_resources: list[str] = Field(default_factory=lambda: ["Practitioner", "Organization", "Location"])
    # Banner's Bundle.link[next] points at an auth-gated /fhirwrapper/ path;
    # [from, to] string replacement fixes the next URL back to the public path.
    next_link_replace: Optional[list[str]] = None
    # Constant filter merged into every ETL search AND coverage count for this
    # endpoint (NOT conformance probes). Used to scope a pull, e.g.
    # {_lastUpdated: "ge2026-01-01"} for "current/active directory only". Applied
    # so coverage % is measured against the SAME scoped total (honest denominator).
    base_params: dict = Field(default_factory=dict)
    # Adaptive count-guided partitioning to bypass per-search caps / broken paging.
    # Maps resource_type -> {param, mode, bucket_max, page1_only}:
    #   param      search param to partition on (e.g. "family", "name")
    #   mode       "prefix" (recursive a-z/0-9 subdivision) | "values" (fixed list)
    #   values     fixed value list when mode == "values" (e.g. US states)
    #   bucket_max subdivide while count > this; fetch when <= this
    #   page1_only take only page 1 of each bucket (servers with broken next links)
    adaptive: dict = Field(default_factory=dict)


class Endpoint(BaseModel):
    key: str
    payer_name: str
    parent_org: Optional[str] = None
    brands: list[str] = Field(default_factory=list)
    base_url: str
    status: EndpointStatus = "known"
    mvp: bool = False
    fhir_version: str = "4.0.1"
    ig_version: str = "1.1.0"
    auth: AuthConfig = Field(default_factory=AuthConfig)
    resource_subset: Optional[list[str]] = None
    quirks: Quirks = Field(default_factory=Quirks)

    @field_validator("base_url")
    @classmethod
    def _validate_https(cls, v: str) -> str:
        parsed = urlparse(v)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError(f"base_url must be a valid HTTPS URL, got: {v!r}")
        return v.rstrip("/")

    @property
    def host(self) -> str:
        return urlparse(self.base_url).netloc

    @property
    def metadata_url(self) -> str:
        return f"{self.base_url}/metadata"

    def expected_resources(self, all_resources: list[str]) -> list[str]:
        """The Plan-Net resources this endpoint is expected to expose."""
        return self.resource_subset if self.resource_subset else list(all_resources)

    def missing_secret_keys(self, settings: Settings) -> list[str]:
        return [k for k in self.auth.secret_keys if not settings.secret(k)]

    def is_runnable(self, settings: Settings) -> bool:
        """True when reachable in principle: known status + all secrets present.

        OAuth endpoints additionally require a configured token_url.
        """
        if self.status == "blocked":
            return False
        if self.missing_secret_keys(settings):
            return False
        if self.auth.strategy == "oauth2_client_credentials" and not self.auth.token_url:
            return False
        return True

    def skip_reason(self, settings: Settings) -> Optional[str]:
        if self.status == "blocked":
            return "blocked"
        if self.status == "unknown":
            # Base URL known but auth/specifics unconfirmed; still attemptable
            # when no secrets are required, otherwise skip.
            if self.missing_secret_keys(settings):
                return "unconfirmed-auth"
        missing = self.missing_secret_keys(settings)
        if missing:
            return f"missing-credentials: {', '.join(missing)}"
        if self.auth.strategy == "oauth2_client_credentials" and not self.auth.token_url:
            return "missing-token-url"
        return None


class EndpointManifest(BaseModel):
    ig_version: str = "1.2.0"
    fhir_version: str = "4.0.1"
    plannet_resources: list[str]
    endpoints: list[Endpoint]

    def by_key(self, key: str) -> Endpoint:
        for ep in self.endpoints:
            if ep.key == key:
                return ep
        raise KeyError(f"No endpoint with key {key!r}")

    def known(self) -> list[Endpoint]:
        return [e for e in self.endpoints if e.status == "known"]

    def mvp(self) -> list[Endpoint]:
        return [e for e in self.endpoints if e.mvp]

    def subset(self, keys: list[str]) -> list[Endpoint]:
        return [self.by_key(k) for k in keys]


DEFAULT_MANIFEST_PATH = CONFIG_DIR / "endpoints.yaml"


@lru_cache
def load_manifest(path: str | Path = DEFAULT_MANIFEST_PATH) -> EndpointManifest:
    data: dict[str, Any] = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    meta = data.get("meta", {})
    return EndpointManifest(
        ig_version=meta.get("ig_version", "1.2.0"),
        fhir_version=meta.get("fhir_version", "4.0.1"),
        plannet_resources=meta.get("plannet_resources", []),
        endpoints=data.get("endpoints", []),
    )


def load_known_endpoints() -> list[Endpoint]:
    return load_manifest().known()


def load_subset(keys: Optional[list[str]] = None) -> list[Endpoint]:
    """Load a named subset, or the MVP (fully-open) subset when keys is None."""
    manifest = load_manifest()
    if keys:
        return manifest.subset(keys)
    return manifest.mvp()


# The credential-free MVP subset (B3). BCBS-KS was reclassified out of the open
# set (requires an Azure APIM subscription key); BCBS-AZ was reclassified in
# after live testing found its real Innovaccer base URL is open.
MVP_SUBSET_KEYS = [
    "humana",
    "capital_blue",
    "excellus",
    "bcbs_mn",
    "premera",
    "banner",
    "bcbs_az",
]
