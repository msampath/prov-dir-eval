"""Resolve and verify the FHIR package closure a Plan-Net audit depends on.

`scripts/ig_elements.py` reads one package. Plan-Net profiles derive from US
Core and R4 core, bind to value sets defined in hl7.terminology, and reference
extensions defined in hl7.fhir.uv.extensions -- so "is this element surface
complete" is a question about the whole dependency CLOSURE, not that one
package. This module resolves the closure transitively from each package's
own `package.json` dependencies and then answers three questions the audit
would otherwise have to assume:

  * is the on-disk Plan-Net package complete and current (version, .index.json,
    snapshots actually present rather than differentials only)?
  * does every canonical a Plan-Net element points at -- baseDefinition,
    type.profile, targetProfile, binding.valueSet -- resolve inside the
    closure?
  * do US Core-inherited elements really appear in the extracted surface, or
    is the snapshot thinner than it looks?

Package loading itself is NOT reimplemented: the cache layout, the download,
and the canonical index all come from provdir.quality.bindings.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from provdir.quality.bindings import (  # noqa: E402
    DEFAULT_CACHE,
    _download_package,
    _pkg_dir,
    load_index,
)

PLAN_NET = ("hl7.fhir.us.davinci-pdex-plan-net", "1.2.0")
REGISTRY = "https://packages.fhir.org"


@dataclass
class ClosureReport:
    packages: list[tuple[str, str]] = field(default_factory=list)
    missing_packages: list[tuple[str, str]] = field(default_factory=list)
    downloaded: list[tuple[str, str]] = field(default_factory=list)
    index_json_present: bool = False
    n_canonicals: int = 0
    profiles_without_snapshot: list[str] = field(default_factory=list)
    unresolved: list[tuple[str, str, str]] = field(default_factory=list)  # (profile, kind, url)
    published_versions: Optional[list[str]] = None
    latest_published: Optional[str] = None
    is_current: Optional[bool] = None
    registry_error: Optional[str] = None
    us_core_inherited: list[tuple[str, str]] = field(default_factory=list)


INDEXED_TYPES = {"ValueSet", "CodeSystem", "StructureDefinition"}


class LazyIndex:
    """canonical url -> resource, resolved through each package's .index.json.

    bindings.load_index() parses every JSON file in every package to learn its
    url. Over Plan-Net's full transitive closure that is ~87,000 files -- three
    copies of us.nlm.vsac at 15,333 files each, r4.core at 4,582, five
    generations of hl7.terminology -- which takes tens of minutes off a cold
    cache and holds hundreds of MB of resources the audit never looks at. FHIR
    npm packages ship `.index.json`, a manifest of (filename, resourceType,
    url), so the url map can be built from ~30 small reads and a resource body
    parsed only when that canonical is actually asked for.

    Same semantics as load_index otherwise: only ValueSet / CodeSystem /
    StructureDefinition are indexed, and the FIRST package in `packages` order
    that claims a url wins, so Plan-Net's own canonicals shadow a dependency
    that happens to define the same url. Packages without an .index.json fall
    back to bindings.load_index for that package alone.
    """

    def __init__(self, cache, packages: list[tuple[str, str]]):
        self._files: dict[str, Path] = {}
        self._cache: dict[str, Optional[dict]] = {}
        self._eager: dict[str, dict] = {}
        for name, version in packages:
            pkg = _pkg_dir(cache, name, version) / "package"
            if not pkg.is_dir():
                continue
            manifest = pkg / ".index.json"
            if not manifest.is_file():
                for url, res in load_index(cache, [(name, version)]).items():
                    self._eager.setdefault(url, res)
                continue
            try:
                entries = json.loads(manifest.read_text(encoding="utf-8")).get("files", [])
            except (OSError, ValueError):
                continue
            for ent in entries:
                url = ent.get("url")
                if (not url or ent.get("resourceType") not in INDEXED_TYPES
                        or url in self._files or url in self._eager):
                    continue
                self._files[url] = pkg / ent["filename"]

    def get(self, url: str, default=None):
        if url in self._eager:
            return self._eager[url]
        if url in self._cache:
            return self._cache[url] if self._cache[url] is not None else default
        path = self._files.get(url)
        if path is None:
            return default
        try:
            res = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            res = None
        self._cache[url] = res
        return res if res is not None else default

    def __contains__(self, url: str) -> bool:
        return url in self._files or url in self._eager

    def __len__(self) -> int:
        return len(self._files) + len(self._eager)

    def keys(self):
        return list(self._files.keys()) + list(self._eager.keys())

    def urls_with_prefix(self, prefix: str) -> list[str]:
        return [u for u in self.keys() if u.startswith(prefix)]


def _read_deps(cache, name: str, version: str) -> Optional[dict]:
    pj = _pkg_dir(cache, name, version) / "package" / "package.json"
    if not pj.is_file():
        return None
    try:
        return json.loads(pj.read_text(encoding="utf-8")).get("dependencies") or {}
    except (OSError, ValueError):
        return {}


def resolve_closure(cache=DEFAULT_CACHE, root: tuple[str, str] = PLAN_NET,
                    download: bool = True, report: Optional[ClosureReport] = None
                    ) -> list[tuple[str, str]]:
    """Every (name, version) reachable from `root` via package.json deps.

    Breadth-first from the root so the root's own canonicals stay FIRST in the
    returned order -- load_index() resolves collisions by first-wins, and
    Plan-Net must shadow anything a dependency defines at the same url. A
    package that is absent and cannot be downloaded is recorded and skipped,
    never silently treated as empty.
    """
    rep = report or ClosureReport()
    order: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    queue: list[tuple[str, str]] = [root]
    while queue:
        name, version = queue.pop(0)
        if (name, version) in seen:
            continue
        seen.add((name, version))
        deps = _read_deps(cache, name, version)
        if deps is None and download:
            try:
                _download_package(cache, name, version)
                rep.downloaded.append((name, version))
                deps = _read_deps(cache, name, version)
            except Exception as exc:  # noqa: BLE001 - a registry outage is a finding, not a crash
                rep.missing_packages.append((name, version))
                rep.registry_error = rep.registry_error or f"{name}#{version}: {exc}"
                continue
        if deps is None:
            rep.missing_packages.append((name, version))
            continue
        order.append((name, version))
        for dname, dversion in sorted(deps.items()):
            if (dname, dversion) not in seen:
                queue.append((dname, dversion))
    rep.packages = order
    return order


def _canonicals_used(index: dict[str, dict], profile_prefix: str) -> list[tuple[str, str, str]]:
    """(profile name, kind, canonical) for every reference a Plan-Net profile
    makes out to another definition."""
    out: list[tuple[str, str, str]] = []
    for url in index.urls_with_prefix(profile_prefix):
        res = index.get(url) or {}
        if res.get("resourceType") != "StructureDefinition":
            continue
        name = res.get("name") or url
        base = res.get("baseDefinition")
        if base:
            out.append((name, "baseDefinition", base))
        for e in (res.get("snapshot") or {}).get("element", []) or []:
            for t in e.get("type", []) or []:
                for prof in (t.get("profile") or []):
                    out.append((name, "type.profile", prof))
                for tp in (t.get("targetProfile") or []):
                    out.append((name, "targetProfile", tp))
            vs = (e.get("binding") or {}).get("valueSet")
            if vs:
                out.append((name, "binding.valueSet", vs))
    return out


def verify(cache=DEFAULT_CACHE, root: tuple[str, str] = PLAN_NET,
           element_paths: Optional[set[tuple[str, str]]] = None,
           check_registry: bool = True) -> ClosureReport:
    """Resolve the closure and check it. `element_paths` is the set of
    (profile, path) the audit actually extracted, used to confirm US
    Core-inherited elements really made it through."""
    rep = ClosureReport()
    packages = resolve_closure(cache, root, report=rep)
    index = LazyIndex(cache, packages)
    rep.n_canonicals = len(index)

    pkg_dir = _pkg_dir(cache, *root) / "package"
    rep.index_json_present = (pkg_dir / ".index.json").is_file()

    prefix = "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/"
    for url in index.urls_with_prefix(prefix):
        res = index.get(url) or {}
        if res.get("resourceType") == "StructureDefinition" and not (
                res.get("snapshot") or {}).get("element"):
            rep.profiles_without_snapshot.append(res.get("name") or url)

    for name, kind, url in _canonicals_used(index, prefix):
        bare = url.split("|", 1)[0]
        if bare in index:
            continue
        # base-spec canonicals (http://hl7.org/fhir/StructureDefinition/<Type>)
        # for primitive/complex datatypes are defined by r4.core; anything else
        # missing is a real unresolved reference.
        rep.unresolved.append((name, kind, url))
    rep.unresolved = sorted(set(rep.unresolved))

    if check_registry:
        try:
            import httpx
            r = httpx.get(f"{REGISTRY}/{root[0]}", timeout=30.0, follow_redirects=True)
            r.raise_for_status()
            data = r.json()
            vers = sorted((data.get("versions") or {}).keys())
            rep.published_versions = vers
            rep.latest_published = data.get("dist-tags", {}).get("latest") or (vers[-1] if vers else None)
            rep.is_current = (rep.latest_published == root[1])
        except Exception as exc:  # noqa: BLE001
            rep.registry_error = f"registry check failed: {exc}"

    if element_paths is not None:
        # US Core constrains a handful of elements Plan-Net inherits rather
        # than restating; if the snapshot were differential-only these would be
        # absent from the extracted surface.
        probes = [
            ("PlannetPractitioner", "Practitioner.name"),
            ("PlannetPractitioner", "Practitioner.identifier"),
            ("PlannetPractitioner", "Practitioner.qualification.code"),
            ("PlannetOrganization", "Organization.address"),
            ("PlannetOrganization", "Organization.telecom"),
            ("PlannetLocation", "Location.address.line"),
            ("PlannetPractitionerRole", "PractitionerRole.telecom.system"),
        ]
        rep.us_core_inherited = [(p, path) for p, path in probes
                                 if (p, path) in element_paths]
    return rep


def format_report(rep: ClosureReport) -> str:
    lines = [f"packages in closure: {len(rep.packages)}"]
    for n, v in rep.packages:
        lines.append(f"  {n}#{v}")
    if rep.downloaded:
        lines.append(f"downloaded this run: {rep.downloaded}")
    if rep.missing_packages:
        lines.append(f"MISSING packages   : {rep.missing_packages}")
    lines.append(f"canonicals indexed : {rep.n_canonicals}")
    lines.append(f".index.json present: {rep.index_json_present}")
    lines.append(f"profiles w/o snapshot: {rep.profiles_without_snapshot or 'none'}")
    lines.append(f"latest published   : {rep.latest_published} "
                 f"(on disk 1.2.0, current={rep.is_current})")
    if rep.registry_error:
        lines.append(f"registry           : {rep.registry_error}")
    lines.append(f"unresolved canonicals: {len(rep.unresolved)}")
    for name, kind, url in rep.unresolved[:40]:
        lines.append(f"  {name} {kind} -> {url}")
    if rep.us_core_inherited:
        lines.append(f"US Core-inherited probes found: {len(rep.us_core_inherited)}")
        for p, path in rep.us_core_inherited:
            lines.append(f"  {p} {path}")
    return "\n".join(lines)


PKG_DIR = r"F:/fhir_cache/packages/hl7.fhir.us.davinci-pdex-plan-net#1.2.0/package"


def main(argv: Optional[list[str]] = None) -> int:
    """Print the closure report; optionally file it as evidence.

    --no-registry exists because verify(check_registry=True) makes a live
    httpx.get to packages.fhir.org. That is the right default -- "is the package
    on disk still the published latest" is a question only the registry can
    answer -- but it makes an offline or air-gapped run print a registry error
    beside otherwise complete findings, which reads like a failure and is not.
    """
    import argparse

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from ig_elements import build

    ap = argparse.ArgumentParser(description="verify the Plan-Net package closure")
    ap.add_argument("--pkg", default=PKG_DIR,
                    help="the Plan-Net package directory to read elements from")
    ap.add_argument("--out", default=None,
                    help="also write the report here (parents created)")
    ap.add_argument("--no-registry", action="store_true",
                    help="skip the live packages.fhir.org version check")
    args = ap.parse_args(argv)

    els = build(args.pkg)
    text = format_report(verify(element_paths={(e.profile, e.path) for e in els},
                                check_registry=not args.no_registry))
    print(text)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text + "\n", encoding="utf-8")
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
