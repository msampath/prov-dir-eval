"""Phase 3.3/3.5 — Compare a live CapabilityStatement to the IG requirements.

Produces a per-resource coverage matrix and classifies each (resource, capability)
as one of:

    supported              declared by the server (and, where probed, working)
    declared-but-failing   declared by the server but the live probe failed
    not-declared           the IG expects it but the server does not declare it
    not-applicable         the IG does not require it for this resource / the
                           endpoint legitimately omits this resource (subset)
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from .ig import ResourceRequirement

SUPPORTED = "supported"
DECLARED_NOT_PROBED = "declared"
DECLARED_BUT_FAILING = "declared-but-failing"
NOT_DECLARED = "not-declared"
NOT_APPLICABLE = "not-applicable"

REQUIRED_INTERACTIONS = ["read", "search-type"]


class DeclaredCapability(BaseModel):
    """What the live server's CapabilityStatement declares for one resource."""

    resource_type: str
    interactions: list[str] = Field(default_factory=list)
    search_params: list[str] = Field(default_factory=list)
    includes: list[str] = Field(default_factory=list)
    revincludes: list[str] = Field(default_factory=list)


def parse_live_capability(metadata: dict) -> dict[str, DeclaredCapability]:
    """Extract declared capabilities per resource from a live /metadata response."""
    out: dict[str, DeclaredCapability] = {}
    rest = (metadata.get("rest") or [{}])[0]
    for res in rest.get("resource", []) or []:
        rtype = res.get("type")
        if not rtype:
            continue
        out[rtype] = DeclaredCapability(
            resource_type=rtype,
            interactions=[i.get("code") for i in res.get("interaction", []) or [] if i.get("code")],
            search_params=[s.get("name") for s in res.get("searchParam", []) or [] if s.get("name")],
            includes=list(res.get("searchInclude", []) or []),
            revincludes=list(res.get("searchRevInclude", []) or []),
        )
    return out


class ResourceConformance(BaseModel):
    resource_type: str
    applicable: bool = True
    declared: bool = False
    interactions: dict[str, str] = Field(default_factory=dict)
    shall_search_params: dict[str, str] = Field(default_factory=dict)
    should_search_params: dict[str, str] = Field(default_factory=dict)
    includes_declared: list[str] = Field(default_factory=list)
    revincludes_declared: list[str] = Field(default_factory=list)
    probes: dict[str, dict] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)


def build_resource_matrix(
    req: ResourceRequirement,
    declared: Optional[DeclaredCapability],
    expected: bool,
) -> ResourceConformance:
    """Declared-capability matrix (pre-probe). `expected` = endpoint should expose it."""
    rc = ResourceConformance(resource_type=req.resource_type, applicable=expected)

    if not expected:
        # Endpoint legitimately omits this resource (subset declared in manifest).
        for inter in REQUIRED_INTERACTIONS:
            rc.interactions[inter] = NOT_APPLICABLE
        for sp in req.shall_search_params:
            rc.shall_search_params[sp] = NOT_APPLICABLE
        for sp in req.should_search_params:
            rc.should_search_params[sp] = NOT_APPLICABLE
        return rc

    rc.declared = declared is not None
    declared_interactions = set(declared.interactions) if declared else set()
    declared_params = set(declared.search_params) if declared else set()

    for inter in REQUIRED_INTERACTIONS:
        # Only flag interactions the IG actually expects for this resource.
        if inter in req.interactions:
            rc.interactions[inter] = SUPPORTED if inter in declared_interactions else NOT_DECLARED
        else:
            rc.interactions[inter] = NOT_APPLICABLE
    for sp in req.shall_search_params:
        rc.shall_search_params[sp] = DECLARED_NOT_PROBED if sp in declared_params else NOT_DECLARED
    for sp in req.should_search_params:
        rc.should_search_params[sp] = DECLARED_NOT_PROBED if sp in declared_params else NOT_DECLARED

    if declared:
        rc.includes_declared = declared.includes
        rc.revincludes_declared = declared.revincludes
    return rc


def score_resource(rc: ResourceConformance) -> dict:
    """Per-resource roll-up percentages over SHALL items."""
    if not rc.applicable:
        return {"applicable": False}

    def pct(values: list[str]) -> Optional[float]:
        if not values:
            return None
        good = sum(1 for v in values if v in (SUPPORTED, DECLARED_NOT_PROBED))
        return round(100.0 * good / len(values), 1)

    interactions = [v for v in rc.interactions.values() if v != NOT_APPLICABLE]
    return {
        "applicable": True,
        "declared": rc.declared,
        "required_interaction_pct": pct(interactions),
        "shall_param_declared_pct": pct(list(rc.shall_search_params.values())),
        "should_param_declared_pct": pct(list(rc.should_search_params.values())),
    }
