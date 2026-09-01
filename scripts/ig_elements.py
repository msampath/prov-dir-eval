"""Extract the complete testable element surface from a FHIR IG package.

Drives auditing from the IG itself rather than a hand-curated list: every
element in every profile snapshot, with the facts needed to test it —
cardinality, type(s), binding, constraints, mustSupport, and the slice
discriminator when the element is a slice.

FHIR element paths are not JSONPath. Two transforms matter:
  * slices          PractitionerRole.extension:newpatients
                    -> $.extension[*] ? (@.url == "<slice url>")
  * choice types    Extension.value[x] with type Reference
                    -> $.valueReference
Both are resolved here, so downstream code never has to know FHIR pathing.
"""
from __future__ import annotations

import glob
import json
import os
from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class IGElement:
    profile: str
    resource_type: str
    path: str                    # FHIR path, slices included
    jsonpath: str                # executable jsonb_path against the resource
    min: int
    max: str
    types: list[str] = field(default_factory=list)
    must_support: bool = False
    binding_strength: Optional[str] = None
    binding_valueset: Optional[str] = None
    slice_name: Optional[str] = None
    slice_url: Optional[str] = None
    constraints: list[str] = field(default_factory=list)
    is_reference: bool = False
    ref_targets: list[str] = field(default_factory=list)
    depth: int = 0
    # True when the element was synthesised by walking a referenced Extension's
    # own StructureDefinition rather than read from the profile's snapshot. A
    # complex extension inlined into a profile snapshot yields BOTH, and the two
    # can disagree: PlannetPractitionerRole narrows qualification.code to
    # IndividualSpecialtyAndDegreeLicenseCertificateVS, while the shared
    # Qualification extension still says SpecialtyAndDegreeLicenseCertificateVS.
    # The profile's own snapshot is authoritative for that profile, so consumers
    # de-duplicating on (profile, path) must prefer from_expansion=False.
    from_expansion: bool = False


def _load(pkg_dir: str) -> list[dict]:
    out = []
    for f in glob.glob(os.path.join(pkg_dir, "*.json")):
        try:
            d = json.load(open(f, encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        if d.get("resourceType") == "StructureDefinition":
            out.append(d)
    return out


def _ext_url_for_slice(sds: list[dict], profile_sd: dict, elem: dict) -> Optional[str]:
    """The canonical URL an extension slice is pinned to, from its type profile."""
    for t in elem.get("type", []):
        if t.get("code") == "Extension":
            prof = t.get("profile") or []
            if prof:
                return prof[0]
    return None


def _segment_to_jsonpath(seg: str, slice_url: Optional[str], types: list[str]) -> str:
    """One FHIR path segment -> one JSONPath segment."""
    if ":" in seg:                       # a slice
        base = seg.split(":", 1)[0]
        if slice_url:
            return f'{base}[*] ? (@.url == "{slice_url}")'
        return f"{base}[*]"
    if seg.endswith("[x]"):              # choice type
        stem = seg[:-3]
        if types:
            t = types[0]
            return stem + t[0].upper() + t[1:]
        return stem
    return seg


def _mk(sd_name, rtype, path, jsonpath, e, slice_name, slice_url, depth,
        from_expansion: bool = False) -> IGElement:
    types = [t.get("code") for t in e.get("type", []) if t.get("code")]
    binding = e.get("binding") or {}
    refs = []
    for t in e.get("type", []):
        if t.get("code") == "Reference":
            for tp in t.get("targetProfile", []) or []:
                refs.append(tp.rsplit("/", 1)[-1])
    return IGElement(
        profile=sd_name, resource_type=rtype, path=path, jsonpath=jsonpath,
        min=int(e.get("min") or 0), max=str(e.get("max") or "*"),
        types=types, must_support=bool(e.get("mustSupport")),
        binding_strength=binding.get("strength"),
        binding_valueset=binding.get("valueSet"),
        slice_name=slice_name, slice_url=slice_url,
        constraints=[c.get("key") for c in e.get("constraint", []) if c.get("key")],
        is_reference=("Reference" in types), ref_targets=sorted(set(refs)),
        depth=depth, from_expansion=from_expansion,
    )


def _expand_extension(ext_sd: dict, base_path: str, base_jsonpath: str,
                      profile_name: str, rtype: str, depth: int,
                      by_url: dict, seen: set, out: list) -> None:
    """Walk an Extension StructureDefinition and emit its internal elements.

    Sub-extensions (Extension.extension:fromNetwork) and the value[x] choice
    live only here, never in the referencing profile's snapshot. Skipping this
    is how a nested reference — e.g. newpatients.fromNetwork, which carries
    PractitionerRole's network linkage — becomes invisible to an audit.
    """
    url = ext_sd.get("url")
    if not url or url in seen or depth > 6:
        return
    seen = seen | {url}
    for e in ext_sd.get("snapshot", {}).get("element", []):
        eid = e.get("id") or ""
        if "." not in eid:
            continue
        segs = eid.split(".")[1:]
        if not segs or segs[0] in ("id", "url"):
            continue
        sub_slice = e.get("sliceName")
        types = [t.get("code") for t in e.get("type", []) if t.get("code")]

        if segs[0].startswith("extension") and sub_slice:
            sub_url = sub_slice          # sub-extensions are keyed by plain url
            jp = f'{base_jsonpath}.extension[*] ? (@.url == "{sub_url}")'
            p = f"{base_path}.extension:{sub_slice}"
            out.append(_mk(profile_name, rtype, p, jp, e, sub_slice, sub_url,
                           depth + 1, from_expansion=True))
            nested = by_url.get(_ext_url_for_slice([], ext_sd, e) or "")
            if nested:
                _expand_extension(nested, p, jp, profile_name, rtype,
                                  depth + 1, by_url, seen, out)
        elif segs[0].startswith("value") and len(segs) == 1:
            jp = base_jsonpath + "." + _segment_to_jsonpath(segs[0], None, types)
            p = f"{base_path}.{segs[0]}"
            out.append(_mk(profile_name, rtype, p, jp, e, None, None,
                           depth + 1, from_expansion=True))

        elif (len(segs) == 2 and segs[0].startswith("extension:")
              and segs[1].startswith("value")):
            # Extension.extension:<name>.value[x] — the sub-extension's own value.
            # This is where a nested reference actually lives: the sub-extension
            # node carries no type, only this child does. Missing it is why
            # newpatients.fromNetwork (PractitionerRole -> Network linkage on
            # ~253k rows) reads as "no reference published".
            sub = segs[0].split(":", 1)[1]
            jp = (f'{base_jsonpath}.extension[*] ? (@.url == "{sub}").'
                  + _segment_to_jsonpath(segs[1], None, types))
            p = f"{base_path}.extension:{sub}.{segs[1]}"
            out.append(_mk(profile_name, rtype, p, jp, e, sub, sub,
                           depth + 2, from_expansion=True))


def build(pkg_dir: str) -> list[IGElement]:
    sds = _load(pkg_dir)
    by_url = {d.get("url"): d for d in sds}
    profiles = [d for d in sds
                if d.get("derivation") == "constraint" and d.get("type") != "Extension"]

    out: list[IGElement] = []
    for sd in sorted(profiles, key=lambda x: x["name"]):
        rtype = sd["type"]
        for e in sd.get("snapshot", {}).get("element", []):
            path = e.get("id") or e.get("path") or ""
            if not path or "." not in path:
                continue
            types = [t.get("code") for t in e.get("type", []) if t.get("code")]
            slice_name = e.get("sliceName")
            is_ext_slice = slice_name and ".extension:" in ("." + path)
            slice_url = _ext_url_for_slice(sds, sd, e) if is_ext_slice else None

            segs = path.split(".")[1:]          # drop the resource type
            jp_parts = []
            for s in segs:
                jp_parts.append(_segment_to_jsonpath(
                    s, slice_url if s.startswith("extension") else None, types))
            if not jp_parts:
                continue
            jsonpath = "$." + ".".join(jp_parts)

            out.append(_mk(sd["name"], rtype, path, jsonpath, e,
                           slice_name, slice_url, len(segs)))

            if slice_url and slice_url in by_url:
                _expand_extension(by_url[slice_url], path, jsonpath,
                                  sd["name"], rtype, len(segs), by_url, set(), out)
    return out


if __name__ == "__main__":
    import sys
    pkg = sys.argv[1] if len(sys.argv) > 1 else \
        r"F:/fhir_cache/packages/hl7.fhir.us.davinci-pdex-plan-net#1.2.0/package"
    els = build(pkg)
    print(f"{len(els)} testable elements across "
          f"{len({e.profile for e in els})} profiles")
    print(f"  mustSupport : {sum(1 for e in els if e.must_support)}")
    print(f"  required    : {sum(1 for e in els if e.min > 0)}")
    print(f"  references  : {sum(1 for e in els if e.is_reference)}")
    print(f"  bound       : {sum(1 for e in els if e.binding_valueset)}")
    if len(sys.argv) > 2 and sys.argv[2] == "--dump":
        json.dump([asdict(e) for e in els], open(sys.argv[3], "w"), indent=1)
        print("dumped ->", sys.argv[3])
