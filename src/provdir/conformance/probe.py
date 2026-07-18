"""Phase 3.4 — Active probes that validate declared capabilities against live data.

Probes are deliberately conservative and lean: benign, widely-valid parameter
values and small `_count`s, so they neither trip rate limits nor depend on
payer-specific code systems, and so the request volume per endpoint stays low.

The basic-search probe is *adaptive*: many Plan-Net servers reject a bare search
("a valid search parameter was not provided"), which is allowed behaviour — so a
bare search that fails is retried with one safe SHALL parameter. A resource is
credited with a working search if either form returns a Bundle, and the
filter-required behaviour is recorded as a quirk rather than a failure.
"""

from __future__ import annotations


from ..http_client import FhirClient
from ..logging_setup import get_logger

log = get_logger(__name__)

# Search params we can probe with a generic value without knowing payer code systems.
SAFE_PARAM_VALUES: dict[str, str] = {
    "name": "a",
    "family": "a",
    "given": "a",
    "address": "a",
    "address-city": "a",
    "address-state": "NY",
    "phonetic": "a",
}

# Resources that carry a `name` (so `name:exact` is meaningful).
NAME_BEARING = {"Organization", "Practitioner", "Location", "HealthcareService", "InsurancePlan"}


def _is_bundle(payload: dict) -> bool:
    return payload.get("resourceType") == "Bundle"


async def _run(client: FhirClient, resource: str, params: dict) -> dict:
    record: dict = {"resource": resource, "params": dict(params)}
    try:
        bundle = await client.search_page(resource, params)
        record["status"] = 200
        record["ok"] = _is_bundle(bundle)
        record["total"] = bundle.get("total")
        record["returned"] = len(bundle.get("entry", []) or [])
        if not record["ok"]:
            rt = bundle.get("resourceType")
            issue = (bundle.get("issue") or [{}])[0] if rt == "OperationOutcome" else {}
            detail = (issue.get("details") or {}).get("text") or issue.get("diagnostics")
            record["note"] = f"{rt}: {detail}" if detail else f"unexpected resourceType {rt!r}"
    except Exception as exc:  # noqa: BLE001
        status = getattr(exc, "status", None) or getattr(
            getattr(exc, "response", None), "status_code", None
        )
        record["status"] = status
        record["ok"] = False
        record["note"] = f"{type(exc).__name__}: {exc}"
    return record


async def probe_resource(
    client: FhirClient,
    resource: str,
    shall_params_declared: list[str],
    includes_declared: list[str],
    revincludes_declared: list[str],
) -> dict:
    """Run the (lean) probe battery for one resource; returns named probe records."""
    probes: dict[str, dict] = {}
    probe_param = next((p for p in shall_params_declared if p in SAFE_PARAM_VALUES), None)

    # 1. Basic search, adaptive: bare first, then a single safe SHALL filter.
    base = await _run(client, resource, {"_count": 2})
    base["check"] = "basic search returns a Bundle"
    if not base.get("ok") and probe_param:
        filtered = await _run(client, resource, {probe_param: SAFE_PARAM_VALUES[probe_param], "_count": 2})
        filtered["check"] = f"search with required filter '{probe_param}' returns a Bundle"
        filtered["requires_search_param"] = True
        filtered["bare_search_note"] = base.get("note") or base.get("status")
        filtered["param"] = probe_param
        base = filtered
    if base.get("ok") and base.get("returned") is not None:
        base["count_respected"] = base["returned"] <= 2
        base.setdefault("param", probe_param)  # which SHALL param (if any) was exercised
    probes["search"] = base

    server_alive = bool(base.get("ok"))

    # 2. :exact modifier on name (deeper IG requirement).
    if server_alive and resource in NAME_BEARING:
        rec = await _run(client, resource, {"name:exact": "Health", "_count": 1})
        rec["check"] = "name:exact modifier accepted"
        probes["modifier_exact"] = rec

    # 3. _include (chained resolution) where the server declares one.
    if server_alive and includes_declared:
        inc = includes_declared[0]
        rec = await _run(client, resource, {"_include": inc, "_count": 1})
        rec["check"] = f"_include '{inc}' accepted"
        probes["include"] = rec

    # 4. _revinclude where declared.
    if server_alive and revincludes_declared:
        rev = revincludes_declared[0]
        rec = await _run(client, resource, {"_revinclude": rev, "_count": 1})
        rec["check"] = f"_revinclude '{rev}' accepted"
        probes["revinclude"] = rec

    return probes
