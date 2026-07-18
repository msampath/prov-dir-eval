"""Phase 3.1 — Load the Plan-Net IG CapabilityStatement (target v1.2.0).

The reference artifact is saved to `reference/plannet-capabilitystatement.json`
(the `reference/` dir is git-ignored). `fetch_ig_capability_statement()` tries the
versioned publication paths for the requested IG version and validates that the
response is a FHIR CapabilityStatement before saving.

It also parses the statement into a normalized :class:`IgRequirements` describing,
per resource type, the required interactions and SHALL/SHOULD search params.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import httpx
from pydantic import BaseModel, Field

from .. import REFERENCE_DIR
from ..config import get_settings
from ..logging_setup import get_logger

log = get_logger(__name__)

IG_CAPABILITY_PATH = REFERENCE_DIR / "plannet-capabilitystatement.json"

# Map IG semver -> the published path segment on hl7.org.
_VERSION_PATH = {
    "1.0.0": "STU1",
    "1.1.0": "STU1.1",
    "1.2.0": "STU1.2",
}


def _candidate_urls(version: str) -> list[str]:
    seg = _VERSION_PATH.get(version, version)
    bases = [
        f"https://hl7.org/fhir/us/davinci-pdex-plan-net/{seg}",
        f"http://hl7.org/fhir/us/davinci-pdex-plan-net/{seg}",
        f"https://hl7.org/fhir/us/davinci-pdex-plan-net/{version}",
    ]
    files = ["CapabilityStatement-plan-net.json"]
    return [f"{b}/{f}" for b in bases for f in files]


def fetch_ig_capability_statement(
    version: Optional[str] = None,
    dest: Path = IG_CAPABILITY_PATH,
    force: bool = False,
) -> Path:
    version = version or get_settings().plannet_ig_version
    if dest.exists() and not force:
        log.info("IG CapabilityStatement already present at %s (use force to refetch)", dest)
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    errors = []
    for url in _candidate_urls(version):
        try:
            resp = httpx.get(url, headers={"Accept": "application/fhir+json, application/json"}, timeout=60, follow_redirects=True)
            if resp.status_code != 200:
                errors.append(f"{url} -> HTTP {resp.status_code}")
                continue
            data = resp.json()
            if data.get("resourceType") != "CapabilityStatement":
                errors.append(f"{url} -> not a CapabilityStatement")
                continue
            dest.write_text(json.dumps(data, indent=2), encoding="utf-8")
            log.info("Fetched Plan-Net IG %s CapabilityStatement from %s", version, url)
            return dest
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url} -> {type(exc).__name__}: {exc}")
    raise RuntimeError(
        "Could not fetch Plan-Net IG CapabilityStatement v"
        f"{version}. Tried:\n  " + "\n  ".join(errors)
    )


class ResourceRequirement(BaseModel):
    resource_type: str
    interactions: list[str] = Field(default_factory=list)
    shall_search_params: list[str] = Field(default_factory=list)
    should_search_params: list[str] = Field(default_factory=list)
    all_search_params: list[str] = Field(default_factory=list)
    supported_includes: list[str] = Field(default_factory=list)
    supported_revincludes: list[str] = Field(default_factory=list)


class IgRequirements(BaseModel):
    ig_version: str
    fhir_version: Optional[str] = None
    resources: dict[str, ResourceRequirement] = Field(default_factory=dict)

    def resource_types(self) -> list[str]:
        return sorted(self.resources.keys())


def _expectation(obj: dict) -> Optional[str]:
    """Read the conformance-expectation extension (SHALL / SHOULD / MAY)."""
    for ext in obj.get("extension", []) or []:
        if ext.get("url", "").endswith("capabilitystatement-expectation"):
            return ext.get("valueCode")
    return None


def parse_capability_statement(path: Path = IG_CAPABILITY_PATH) -> IgRequirements:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    version = data.get("version") or get_settings().plannet_ig_version
    reqs = IgRequirements(ig_version=version, fhir_version=data.get("fhirVersion"))

    rest = (data.get("rest") or [{}])[0]
    for res in rest.get("resource", []):
        rtype = res.get("type")
        if not rtype:
            continue
        rr = ResourceRequirement(resource_type=rtype)
        for inter in res.get("interaction", []) or []:
            code = inter.get("code")
            if code:
                rr.interactions.append(code)
        for sp in res.get("searchParam", []) or []:
            name = sp.get("name")
            if not name:
                continue
            rr.all_search_params.append(name)
            exp = _expectation(sp)
            if exp == "SHALL":
                rr.shall_search_params.append(name)
            elif exp == "SHOULD":
                rr.should_search_params.append(name)
        for inc in res.get("searchInclude", []) or []:
            rr.supported_includes.append(inc)
        for rev in res.get("searchRevInclude", []) or []:
            rr.supported_revincludes.append(rev)
        reqs.resources[rtype] = rr

    log.info(
        "Parsed IG %s: %d resources, %d total SHALL search params",
        version,
        len(reqs.resources),
        sum(len(r.shall_search_params) for r in reqs.resources.values()),
    )
    return reqs
