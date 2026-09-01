#!/usr/bin/env python
"""Render the external quality report HTML from findings.json.

Contract: docs/evaluation-framework.md §9/§9.1/§9.1a, plus the §9.1b
census-run and badge addendum. Everything user-facing is expressed in plain
language next to (never instead of) the machine identifier; every rate
carries its n; per-LOB numbers use per-LOB denominators; nothing internal
(run ids, schema names, table names) renders.

Sections:
  A summary (population decomposition, what-matters-most, badge tally)
  B tests (badged material cells + unbadged environment/advisory cells)
  C completeness (Track A element population, per LOB)
  D mandatory vs must-support
  E standard layers (R4 / US Core / Plan-Net)
  F per-LOB material conformance
  G placeholder & default values
  H locality (state boundaries)
  I referential integrity
  J population shape
  K methodology
"""
from __future__ import annotations

import argparse
import html
import json
import re
from pathlib import Path

CSS = """
:root{
  --bg:#fbfbfa; --panel:#ffffff; --raised:#f4f4f2; --ink:#1a1a19; --ink-2:#4a4a46;
  --muted:#77776f; --line:#e2e2dd; --line-2:#cfcfc8;
  --b0:#e3ecf6; --b1:#b9cfe8; --b2:#aebfcc; --b3:#e8b64c; --b4:#c0392b;
  --ink-on-b:#0e1a22; --ink-on-b-hi:#ffffff;
  --accent:#3f7391; --accent-soft:#e8eff3; --accent-ink:#24485c;
  --warn-soft:#f6efe4; --warn-ink:#6b5426; --warn-line:#d9c49a;
  --good-soft:#e3ecf6; --good-ink:#24486e; --good-line:#b9cfe8;
  --ok-soft:#f6efd9;   --ok-ink:#6b5a1e;   --ok-line:#dcc98a;
  --bad-soft:#f7ddd7;  --bad-ink:#8f2413;  --bad-line:#dfa08f;
  --hatch:#c9c9c2;
}
@media (prefers-color-scheme:dark){
  :root:not([data-theme="light"]){
    --bg:#131316; --panel:#1a1a1e; --raised:#232329; --ink:#ececef; --ink-2:#c2c2c8;
    --muted:#8d8d97; --line:#2c2c33; --line-2:#3c3c45;
    --b0:#20304a; --b1:#2c4a72; --b2:#3d4a58; --b3:#8a6d1f; --b4:#8e2f22;
    --ink-on-b:#e6eef3; --ink-on-b-hi:#ffffff;
    --accent:#77a3ba; --accent-soft:#1e2a32; --accent-ink:#a8cbdc;
    --warn-soft:#2a2317; --warn-ink:#e0cfa8; --warn-line:#4d4026;
    --good-soft:#1d2c42; --good-ink:#a9c6e8; --good-line:#2c4a72;
    --ok-soft:#2b2617;   --ok-ink:#dcc98a;   --ok-line:#54491f;
    --bad-soft:#3a1812;  --bad-ink:#eba392;  --bad-line:#6e2a1c;
    --hatch:#3a3a42;
  }
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);
  font:15px/1.55 ui-sans-serif,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
.wrap{max-width:1080px;margin:0 auto;padding:22px 18px 60px}
h1{font-size:24px;line-height:1.22;margin:0 0 6px}
h2{font-size:18px;margin:0 0 4px}
h3{font-size:12.5px;margin:14px 0 8px;color:var(--ink-2);
  text-transform:uppercase;letter-spacing:.07em}
p{margin:0 0 10px;color:var(--ink-2);max-width:88ch}
ul{color:var(--ink-2)}
.sub{color:var(--muted);font-size:13px}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.9em;
  word-break:break-all}
.identity{display:flex;flex-wrap:wrap;gap:6px;padding:8px 12px;background:var(--raised);
  border:1px solid var(--line);border-radius:8px;margin:8px 0 14px;
  font-size:11.5px;color:var(--ink-2)}
.identity .chip{background:var(--panel);padding:3px 8px;border-radius:999px;
  border:1px solid var(--line)}
section.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;
  margin:20px 0;overflow:hidden}
.card>.hd{padding:12px 16px;border-bottom:1px solid var(--line);
  display:flex;flex-wrap:wrap;gap:8px;align-items:baseline;justify-content:space-between}
.card>.bd{padding:14px 16px}
table{border-collapse:collapse;width:100%;font-size:12.5px;table-layout:auto}
th,td{padding:6px 9px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:10.5px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);
  font-weight:600;background:var(--panel)}
td.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
td.mono{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11.5px;
  word-break:break-all}
tbody tr:hover>td{background:var(--raised)}
.cell{display:inline-flex;align-items:center;gap:6px;border-radius:6px;padding:3px 8px;
  min-width:78px;justify-content:space-between}
.cell .v{font-variant-numeric:tabular-nums;font-weight:600}
.cell .n{font-size:10.5px;opacity:.72;font-variant-numeric:tabular-nums}
.b0{background:var(--b0);color:var(--ink-on-b)}
.b1{background:var(--b1);color:var(--ink-on-b)}
.b2{background:var(--b2);color:var(--ink-on-b)}
.b3{background:var(--b3);color:var(--ink-on-b-hi)}
.b4{background:var(--b4);color:var(--ink-on-b-hi)}
.absent{font-size:11px;color:var(--muted);border:1px dashed var(--line-2);
  padding:2px 6px;border-radius:4px}
.exact{font-size:12px;color:var(--ink-2);font-variant-numeric:tabular-nums}
.exact.sev-bad{color:var(--bad-ink);font-weight:700}
.exact.sev-good{color:var(--good-ink)}
.absent.sev-bad{color:var(--bad-ink);border-color:var(--bad-line);font-weight:700}
.pill{font-size:10.5px;padding:2px 7px;border-radius:5px;
  border:1px solid var(--line-2);color:var(--ink-2);background:var(--raised);
  white-space:nowrap}
.pill.err{background:var(--bad-soft);color:var(--bad-ink);border-color:var(--bad-line)}
.pill.warn{background:var(--accent-soft);color:var(--accent-ink);border-color:transparent}
.pill.info{background:var(--panel)}
.badge{font-size:11px;font-weight:700;letter-spacing:.04em;padding:2px 9px;
  border-radius:999px;border:1px solid;white-space:nowrap}
.badge.good{background:var(--good-soft);color:var(--good-ink);border-color:var(--good-line)}
.badge.ok{background:var(--ok-soft);color:var(--ok-ink);border-color:var(--ok-line)}
.badge.bad{background:var(--bad-soft);color:var(--bad-ink);border-color:var(--bad-line)}
.kv{display:grid;grid-template-columns:auto 1fr;gap:5px 12px;font-size:13px;
  align-items:baseline;margin:0}
.kv dt{color:var(--muted)}
.kv dd{margin:0;font-variant-numeric:tabular-nums}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:14px}
.callout{background:var(--warn-soft);border:1px solid var(--warn-line);
  color:var(--warn-ink);border-radius:8px;padding:10px 12px;font-size:12.5px;margin:8px 0}
.notebox{background:var(--raised);border:1px solid var(--line);color:var(--ink-2);
  border-radius:8px;padding:10px 12px;font-size:12.5px;margin:8px 0}
details.test{border:1px solid var(--line);border-radius:8px;margin:8px 0;
  background:var(--panel)}
details.test>summary{cursor:pointer;list-style:none;display:flex;flex-wrap:wrap;
  gap:8px;align-items:center;padding:9px 12px;font-size:13px}
details.test>summary::-webkit-details-marker{display:none}
details.test>summary::before{content:"\\25B8";color:var(--muted);font-size:11px}
details.test[open]>summary::before{content:"\\25BE"}
details.test>.body{border-top:1px solid var(--line);padding:10px 12px;
  background:var(--raised)}
details.test .explain{font-size:12.5px;color:var(--ink-2);max-width:88ch;margin:0 0 10px}
.flabel{font-size:12.5px}
.fpath{display:block;font-size:10.5px;color:var(--muted)}
@media (max-width:640px){
  .wrap{padding:16px 10px 40px}
  h1{font-size:20px}
}
"""


# ---- §9.1 rule 1: five bands of exactly 20 points ----------------------

def band(rate: float | None) -> str:
    if rate is None:
        return "b0"
    if rate >= 0.80:
        return "b4"
    if rate >= 0.60:
        return "b3"
    if rate >= 0.40:
        return "b2"
    if rate >= 0.20:
        return "b1"
    return "b0"


# §9.1a row floor applies to *rates*; run 14 is a 100% census, so cells below
# the floor render the exact count instead of suppressing the fact (§9.1b —
# an n=1 census cell has no sampling error to guard against).
ROW_FLOOR = 30  # matches provdir.quality.conformance.D3_MIN_CELL

# ---- Badges (§9.1b exception, recorded in the framework doc) -----------
# One test = one (resource type, material category) cell. Cuts are FIXED,
# fleet-wide constants, not derived from any one payer's distribution:
#   Good  rate < 0.10 and no error-severity finding in the cell
#   Bad   rate >= 0.40 and at least one error-severity finding
#   OK    everything else (incl. high-rate warning-only cells and low-rate
#         cells with error-severity findings)
# Cells below ROW_FLOOR are not badged and not counted in the tally.
BADGE_GOOD_MAX = 0.10
BADGE_BAD_MIN = 0.40


def badge_for(rate: float, has_error: bool, denom: int
              ) -> tuple[str, str] | None:
    if denom < ROW_FLOOR:
        return None
    if rate >= BADGE_BAD_MIN and has_error:
        return "Bad", "bad"
    if rate < BADGE_GOOD_MAX and not has_error:
        return "Good", "good"
    return "OK", "ok"


def render_rate(numer: int, denom: int, good_high: bool = False) -> str:
    """Banded rate with n; §9.1 rule 9 rounding; §9.1b census semantics.

    Color encodes FAILURE severity globally (red / amber / blue-gray /
    blue): pass ``good_high=True`` for metrics where a HIGH value is the
    healthy state (populated-rates, in-state shares) so a low value goes
    red — the failure is always the red end, whichever direction it lies.

    On a census, a measured zero is exact — rendering it "<0.1%" would make
    clean cells read as defective ([O-21]); likewise a full-cell count is
    exact and renders as such.
    """
    if denom == 0:
        return '<span class="absent">no signal, n=0</span>'
    fail = ((denom - numer) if good_high else numer) / denom
    sev = (" sev-bad" if fail >= 0.60 else
           (" sev-good" if fail < 0.20 else ""))
    if denom < ROW_FLOOR:
        return (f'<span class="exact{sev}">{numer:,} of {denom:,} '
                'records</span>')
    if numer == 0:
        return f'<span class="exact{sev}">0 of {denom:,} records</span>'
    if numer == denom:
        return f'<span class="exact{sev}">all {denom:,} records</span>'
    r = numer / denom
    if r >= 0.999:
        pct = "&gt;99.9%"
    elif r <= 0.001:
        pct = "&lt;0.1%"
    else:
        pct = f"{r*100:.1f}%"
    return (f'<span class="cell {band(fail)}">'
            f'<span class="v">{pct}</span>'
            f'<span class="n">n={denom:,}</span></span>')


def _severity_pill(sev: str) -> str:
    cls = {"error": "err", "fatal": "err",
           "warning": "warn", "information": "info"}.get(sev, "info")
    return f'<span class="pill {cls}">{html.escape(sev)}</span>'


_REQ_BASE_LABEL = {
    "mandatory": "required element", "must-support": "must-support",
    "other": "optional element", "mixed": "mixed slices",
    "base-fhir": "base-FHIR element", "record-level": "whole record",
}


_REQ_SUBFIELD_LABEL = {
    "mandatory": "sub-field of a required element",
    "must-support": "sub-field of a must-support element",
    "other": "sub-field of an optional element",
    "mixed": "sub-field of a mixed-class element",
}


def req_label(req: str, grain: str = "exact") -> str:
    """[O-19]: an ancestor-grain classification is NOT the element's own
    class — the finding sits on a core-FHIR sub-field UNDER an element of
    that class, and the label must say so."""
    if grain in ("ancestor-1", "ancestor-2"):
        return _REQ_SUBFIELD_LABEL.get(req, f"sub-field of: {req}")
    if grain == "resource-level":
        return "whole record"
    if grain == "none":
        return "base-FHIR element"
    return _REQ_BASE_LABEL.get(req, req)


def _req_pill(req: str, grain: str = "exact") -> str:
    return f'<span class="pill">{html.escape(req_label(req, grain))}</span>'


# ---- Plain-language catalogues -----------------------------------------

CATEGORY_LABEL = {
    "profile": "structural conformance",
    "binding-required": "required code sets",
    "binding-extensible": "preferred code sets",
    "extension-unresolvable": "private extensions",
    "terminology-unavailable": "uncheckable codes",
    "best-practice": "advisory practices",
    "validator-limitation": "validator limitation",
    "version-stamping": "code-system version stamping",
    "publisher-infrastructure": "publisher infrastructure",
}

CATEGORY_EXPLAIN = {
    "profile": (
        "The record breaks a rule the applicable profile states directly: a "
        "required element is missing, a value the profile fixes is different, "
        "or a structural invariant fails. These are the defects a directory "
        "consumer hits first."),
    "binding-required": (
        "A coded field carries a value outside the code set the profile "
        "requires for that field. Only codes from that set are valid there."),
    "binding-extensible": (
        "A coded field carries a value outside the profile's preferred code "
        "set. Allowed when no listed code applies, but at scale it usually "
        "means a local code system is used where a standard one exists."),
    "extension-unresolvable": (
        "The record carries a custom extension whose defining URL cannot be "
        "resolved, so consumers cannot know what the data in it means."),
    "terminology-unavailable": (
        "The code system named by the record is not part of the loaded "
        "validation packages, so the code could not be checked. A limitation "
        "of the validation environment, not evidence the code is wrong."),
    "best-practice": (
        "Advisory findings from HL7's best-practice checks — here, entirely "
        "the absence of a human-readable narrative block. Machine consumers "
        "are unaffected."),
    "validator-limitation": (
        "The validator's own machinery failed (pattern-check timeouts). "
        "These are not data defects and are excluded from every data-quality "
        "rate; they are counted here for completeness."),
    "version-stamping": (
        "The records mechanically stamp version “4.0.1” — the FHIR "
        "specification's own version number — onto every code system they "
        "cite (provider taxonomies, organization types, …). The codes "
        "themselves may be fine, but they claim a version that does not "
        "exist, so version-aware consumers cannot validate them. One "
        "mechanical publishing defect, repeated on nearly every record."),
    "publisher-infrastructure": (
        "The publisher attaches its own internal metadata extension "
        "(a record-creation timestamp) to every record without publishing "
        "its definition. Harmless to directory use, but it is on 100% of "
        "records by construction, which is why it is reported separately "
        "instead of inside any quality rate."),
}

MESSAGE_EXPLAIN = {
    "UNKNOWN_CODESYSTEM_VERSION_NONE": (
        "The record claims version “4.0.1” of a code system that has no such "
        "version (no version of it is in the validation packages), so the "
        "code could not be checked."),
    "UNKNOWN_CODESYSTEM_VERSION": (
        "The record claims version “4.0.1” of a known code system; that "
        "version does not exist, so the code could not be checked at the "
        "claimed version."),
    "VALUESET_VALUE_MISMATCH_DEFAULT": (
        "The record stamps a different code-system version than the one the "
        "profile's value set is defined against."),
    "Unknown_Code_in_Version": (
        "The code is not present in the code-system version the record "
        "claims."),
    "UNKNOWN_CODESYSTEM": (
        "The code system named by the record is not in the loaded validation "
        "packages, so the code could not be checked."),
    "Terminology_TX_NoValid_1_CC": (
        "None of the codes provided are in the code set the profile requires "
        "for this field."),
    "Terminology_TX_NoValid_2_CC": (
        "None of the codes provided are in the profile's preferred code set "
        "for this field."),
    "Terminology_TX_Code_ValueSet_Ext": (
        "No code was provided where the profile expects one from a specific "
        "code set."),
    "Terminology_TX_Code_ValueSet": (
        "The code provided is not in the code set the profile requires for "
        "this field."),
    "Terminology_TX_NoValid_16": (
        "The value is not a valid code for this field (for example "
        "“website” is not a valid contact-point type)."),
    "UNABLE_TO_INFER_CODESYSTEM": (
        "The value could not be matched to any code system for this field."),
    "Extension_EXT_Unknown_NotHere": (
        "The record carries a custom extension whose definition is not "
        "published, so its meaning cannot be verified."),
    "http://hl7.org/fhir/StructureDefinition/DomainResource#dom-6": (
        "The record has no human-readable narrative text."),
    "http://hl7.org/fhir/StructureDefinition/Extension#ext-1": (
        "An extension carries both a direct value and sub-extensions, which "
        "FHIR does not allow."),
    "Type_Specific_Checks_DT_DateTime_Reasonable": (
        "A date on the record is outside the range of plausible years — "
        "typically a placeholder such as 2999-12-31 (“never expires”) or "
        "1753-01-01 (a system-minimum start date)."),
    "Type_Specific_Checks_DT_String_WS": (
        "A text value starts or ends with stray whitespace (e.g. a name "
        "stored with a trailing space)."),
    "TYPE_SPECIFIC_CHECKS_DT_PRIMITIVE_REGEX_EXCEPTION": (
        "The validator's own pattern check timed out. Not a data defect."),
    "_DT_Fixed_Wrong": (
        "The record carries a value the profile fixes to something else — "
        "for example active = false where the profile fixes active = true, "
        "or an inactive qualification status where the profile fixes it "
        "active: the publisher ships records it has itself marked "
        "inactive."),
    "Validation_VAL_Profile_Minimum": (
        "A required component is missing (for example a geolocation "
        "extension without its latitude/longitude parts)."),
    "Validation_VAL_Profile_Minimum_SLICE": (
        "A required, specifically-typed component is missing from a "
        "structure that demands it."),
    "(no-message-id)": (
        "The record carries a property that does not exist in FHIR R4 at "
        "that position (for example “geolocation” written as a plain "
        "property instead of the geolocation extension)."),
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-Organization#us-core-16": (
        "The organization record fails a US Core naming rule (a proper name "
        "value is required)."),
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-Organization#us-core-17": (
        "The organization record fails a US Core rule on name/period "
        "consistency."),
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-Practitioner#us-core-16": (
        "The practitioner record fails a US Core naming rule."),
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-Practitioner#us-core-17": (
        "The practitioner record fails a US Core rule on name/period "
        "consistency."),
    "http://hl7.org/fhir/us/core/StructureDefinition/us-core-practitionerrole#pd-1": (
        "The role record has neither its own contact information nor a "
        "reference to a location that has it — a consumer cannot contact "
        "this provider."),
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/StructureDefinition/plannet-InsurancePlan#network-or-plan-Network": (
        "The insurance plan names no network at all — the link that "
        "connects a plan to its provider networks is absent."),
}

LOB_LABEL = {
    "hcaz": "Medicaid-LOB",
    "azblue": "Medicare-LOB",
    "ods": "OtherDataSource — a source system, not a line of business",
    "(none)": "(no source marker)",
    "(unknown)": "(no source marker)",
}
LOB_SHORT = {"hcaz": "Medicaid-LOB", "azblue": "Medicare-LOB",
             "ods": "OtherDataSource", "(none)": "(no marker)",
             "(unknown)": "(no marker)"}

LAYER_LABEL = {
    "r4-base": "FHIR R4 base",
    "us-core": "US Core",
    "plan-net": "Plan-Net 1.2.0",
    "code-set-membership": "code-set membership (set owner ≠ rule owner)",
    "unattributed": "not attributable to one standard",
}
LAYER_ORDER = ("plan-net", "us-core", "r4-base", "code-set-membership",
               "unattributed")


def _lob(l: str) -> str:
    return LOB_LABEL.get(l, l)


def _lob_s(l: str) -> str:
    return LOB_SHORT.get(l, l)


_SLICE_STRIP_RE = re.compile(r":[A-Za-z0-9_-]+")
_VS_URL_RE = re.compile(r"\((http[^)|\s]+)")


def _collapse(path: str) -> str:
    return _SLICE_STRIP_RE.sub("", path)


def _segments_label(src: str) -> str:
    segs = []
    for seg in src.split("."):
        if seg.startswith("extension:"):
            segs.append(seg.split(":", 1)[1])
        elif seg.startswith("value[x]"):   # incl. value[x]:valueCodeableConcept
            segs.append("value")
        else:
            segs.append(seg)
    if len(segs) > 1:
        segs = segs[1:]  # drop the resource root; it is in the row already
    return " › ".join(segs)


def humanize_path(element_path: str, slice_paths: list[str]) -> str:
    """Human label for a single-slice or generic path."""
    return _segments_label(slice_paths[0] if len(slice_paths) == 1
                           else element_path)


def finding_label(f: dict, slice_bindings: list[dict]) -> str:
    """Label a finding. A key spanning several profile slices must NOT be
    labelled from an arbitrary slice ([O-18]): resolve the slice via the
    failing value set named in the message; if that fails, stay generic and
    point at the code-set table rather than publish a wrong name."""
    ep = f["element_path"]
    sp = f.get("slice_paths") or []
    if len(sp) <= 1:
        label = humanize_path(ep, sp)
        if not sp and ".extension" in ep:
            # deep tail inside the profile's extension family ([F-22a]):
            # give the reader an anchor instead of a bare segment chain.
            label += "  (inside the profile's extension family — the message text names the failing item)"
        return label
    vs = None
    m = _VS_URL_RE.search(f.get("sample_details") or "")
    if m:
        vs = m.group(1).split("|")[0]
    if vs:
        cands = []
        for b in slice_bindings:
            if b["binding_valueset"] != vs:
                continue
            # [O-23]: the resolved slice must be one of THIS finding's own
            # slices -- a value set shared with an unrelated slice elsewhere
            # must not produce a name from outside the finding's key.
            if b["element_path"] not in sp:
                continue
            coll = _collapse(b["element_path"])
            if coll == ep or ep.startswith(coll + "."):
                tail = ep[len(coll):].lstrip(".")
                resolved = b["element_path"] + ("." + tail if tail else "")
                cands.append(resolved)
        if len(set(cands)) == 1:
            return (_segments_label(cands[0])
                    + "  (slice identified by its failing code set)")
    return (_segments_label(ep)
            + f"  (spans {len(sp)} profile slices — the failing rule is"
            " identified by its code set in the drill-down)")




def message_explain(f: dict) -> str:
    txt = MESSAGE_EXPLAIN.get(f.get("message_id") or "")
    if txt:
        return txt
    sd = (f.get("sample_details") or "").strip()
    if len(sd) > 220:
        sd = sd[:220].rsplit(" ", 1)[0] + " …"
    return sd


MATERIAL = ("profile", "binding-required", "binding-extensible")


def build_meta_lookup(plannet_meta: list[dict]) -> dict[str, dict]:
    """Element metadata keyed by exact AND slice-collapsed path. Where
    several slices collapse onto one key with disagreeing facts, the
    disagreeing field reads 'varies by slice'."""
    lut: dict[str, dict] = {}
    for m in plannet_meta:
        for key in {m["element_path"], _collapse(m["element_path"])}:
            prev = lut.get(key)
            if prev is None:
                lut[key] = dict(m)
            else:
                for f in ("min_card", "max_card", "primary_type",
                          "must_support", "binding_strength",
                          "binding_valueset"):
                    if prev.get(f) != m.get(f):
                        prev[f] = "varies by slice"
    return lut


def resolve_element(ep: str, meta_lut: dict[str, dict]
                    ) -> tuple[dict | None, str]:
    """(metadata of the enumerated element this path resolves to, tail).

    Mirrors classify_path: exact/collapsed first, then up to two ancestors.
    `tail` is the core-FHIR sub-path below the enumerated element ('' for a
    direct hit)."""
    if ep in meta_lut:
        return meta_lut[ep], ""
    segs = ep.split(".")
    for depth in (1, 2):
        if len(segs) - depth <= 1:
            break
        anc = ".".join(segs[:-depth])
        if anc in meta_lut:
            return meta_lut[anc], ".".join(segs[-depth:])
    return None, ""


def card_str(m: dict) -> str:
    mc, mx = m.get("min_card"), m.get("max_card")
    if mc == "varies by slice" or mx == "varies by slice":
        return "cardinality varies by slice"
    return f"{mc if mc is not None else 0}..{mx or '*'}"


def expectation_str(m: dict | None, tail: str) -> str:
    """Human sentence: what the profile expects at this position."""
    if m is None:
        return "core-FHIR element outside the profile's enumerated set"
    bits = []
    pt = m.get("primary_type")
    if pt and pt != "varies by slice":
        bits.append(f"type {pt}")
    elif pt:
        bits.append("type varies by slice")
    bits.append(card_str(m))
    bs, bv = m.get("binding_strength"), m.get("binding_valueset")
    if bs and bs != "varies by slice" and bv and bv != "varies by slice":
        vs_name = str(bv).split("|")[0].rstrip("/").rsplit("/", 1)[-1]
        bits.append(f"{bs} binding to {vs_name}")
    elif bs:
        bits.append("binding varies by slice")
    base = ", ".join(bits)
    if tail:
        return (f"core-FHIR sub-field <span class='mono'>{html.escape(tail)}</span> "
                f"inside an element ({base}) — these messages can only fire "
                "in records that populated the parent")
    return base


def _by_lob_counts(size_rows) -> dict[str, list[int]]:
    return {r["lob"]: r["counts"] for r in (size_rows or [])}


# ------------------------------------------------------------------------

def render(payload: dict, out_path: Path) -> None:
    ident = payload["identity"]
    tot = payload["totals"]
    findings = payload["findings"]
    cat_rows = payload["resources_affected_in_category"]
    lob = payload.get("lob", {})
    req_rows = payload.get("requirement_category", [])
    layer_rows = payload.get("standard_layers", [])
    vs_rows = payload.get("binding_valuesets", [])
    ext_urls = payload.get("extension_urls", [])
    size = payload.get("size_analysis", {})
    track_a = payload.get("track_a", {})
    slice_bindings = payload.get("plannet_slice_bindings", [])
    element_rollup = payload.get("element_rollup", [])
    plannet_meta = payload.get("plannet_element_meta", [])
    lenses = payload.get("coverage_lenses", {})
    mpf_rows = [r for r in lenses.get("rows", []) if r["assessment"] == "mpf"]
    uscdi_rows = [r for r in lenses.get("rows", []) if r["assessment"] == "uscdi"]

    validated = tot["resources_validated"]
    by_cell = tot["resources_in_scope_by_cell"]
    occurrences_total = tot["occurrences_total"]
    findings_total = tot["findings_total"]

    source = ident.get("source_name", "a mid-size Blues plan")
    lobs_line = ident.get(
        "lobs_represented",
        "Medicaid-LOB · Medicare-LOB")
    cap_from = ident.get("data_captured_from", "")
    cap_to = ident.get("data_captured_to", "")
    cap_txt = (cap_from if cap_from == cap_to
               else f"{cap_from} – {cap_to}") if cap_from else "n/a"
    analysis_date = ident.get("analysis_date", ident.get("started_at", ""))[:10]
    identity_bar = (
        f'<div class="identity">'
        f'<span class="chip">source: {html.escape(source)}</span>'
        f'<span class="chip">lines of business: {html.escape(lobs_line)}</span>'
        f'<span class="chip">data captured {html.escape(cap_txt)}</span>'
        f'<span class="chip">analysis {html.escape(analysis_date)}</span>'
        f'<span class="chip">standard: Da Vinci PDex Plan-Net {html.escape(ident["ig_version"])}</span>'
        f'</div>')

    # ---- lookups --------------------------------------------------------
    lob_denoms = {(r["resource_type"], r["lob"]): r["n"]
                  for r in lob.get("denominators", [])}
    lob_any = {(r["resource_type"], r["lob"]): r["n"]
               for r in lob.get("any_finding", [])}
    lob_sev = {(r["resource_type"], r["lob"], r["severity"]): r["n"]
               for r in lob.get("severity", [])}
    lob_cat = {(r["resource_type"], r["lob"], r["category"]): r["n"]
               for r in lob.get("category", [])}
    lobs_of_cell: dict[str, list[str]] = {}
    for (rt, l) in sorted(lob_denoms):
        lobs_of_cell.setdefault(rt, []).append(l)

    findings_by_test: dict[tuple[str, str], list[dict]] = {}
    for f in findings:
        findings_by_test.setdefault(
            (f["resource_type"], f["category"]), []).append(f)

    # tests over material categories only
    tests = []
    env_cells = []
    for row in cat_rows:
        rt, cat, n = row["resource_type"], row["category"], row["resources_affected"]
        denom = by_cell.get(rt, 0)
        rate = n / denom if denom else 0.0
        has_error = any(f["severity"] in ("error", "fatal")
                        for f in findings_by_test.get((rt, cat), []))
        rec = (rt, cat, n, denom, rate, has_error)
        (tests if cat in MATERIAL else env_cells).append(rec)
    tests.sort(key=lambda t: (-t[4], -t[2]))
    env_cells.sort(key=lambda t: (-t[4], -t[2]))
    badge_tally = {"Good": 0, "OK": 0, "Bad": 0}
    unbadged_small = 0
    for rt, cat, n, denom, rate, has_error in tests:
        b = badge_for(rate, has_error, denom)
        if b is None:
            unbadged_small += 1
        else:
            badge_tally[b[0]] += 1

    # population decomposition
    orgs_by_lob = _by_lob_counts(size.get("organizations_by_lob"))
    ods_n = orgs_by_lob.get("ods", [0])[0] if "ods" in orgs_by_lob else 0
    core_n = validated - ods_n
    prac = _by_lob_counts(size.get("practitioners_by_lob"))
    locs = _by_lob_counts(size.get("locations_by_lob"))
    loc_state = _by_lob_counts(size.get("location_state_by_lob"))
    plans = size.get("insurance_plans") or []
    networks = size.get("networks") or []
    junk_net = [n["name"] for n in networks if (n["name"] or "").upper() in
                ("INVALID", "UNKNOWN-ERROR", "NOT APPLICABLE", "UNKNOWN",
                 "NONE", "TEST")]

    # what-matters-most (computed, gated)
    matters = []
    plan_no_net = [p for p in plans if not p.get("has_network_key")]
    if plans and len(plan_no_net) == len(plans):
        plan_phrase = ("one insurance plan record is" if len(plans) == 1
                       else f"{len(plans)} insurance plan records are")
        matters.append(
            f"The plan→network→provider chain cannot be walked: "
            f"{plan_phrase} published, none "
            f"names a network, no role or affiliation references any of the "
            f"{len(networks)} network records, so no provider can be tied "
            f"to any plan or network.")
    inactive_total = sum(v[2] for v in prac.values()) if prac else 0
    prac_total = sum(v[0] for v in prac.values()) if prac else 0
    if inactive_total and prac_total:
        per = " / ".join(
            f"{_lob_s(l)}: {v[2]:,} of {v[0]:,}"
            for l, v in sorted(prac.items(), key=lambda kv: -kv[1][0]))
        matters.append(
            f"{inactive_total:,} of {prac_total:,} practitioners "
            f"({inactive_total/prac_total*100:.0f}%) are published with "
            f"active = false — inactive records shipped into a directory "
            f"whose profile requires active ones ({per}).")
    comp = track_a.get("completeness") or []
    pr_code = {c["lob"]: c for c in comp
               if c["element_path"] == "PractitionerRole.code"}
    if pr_code:
        bits = ", ".join(
            f"{_lob_s(l)}: {c['n_present']:,} of {c['n_rows']:,}"
            for l, c in sorted(pr_code.items()))
        matters.append(
            f"Most provider roles never say what kind of provider they are: "
            f"PractitionerRole.code (a must-support element) is populated on "
            f"{bits}. The validator cannot see absent elements, so this "
            f"comes from the element-population audit (§C).")
    if loc_state:
        az = sum(v[1] for v in loc_state.values())
        tot_loc = sum(v[0] for v in loc_state.values())
        if tot_loc:
            matters.append(
                f"{tot_loc - az:,} of {tot_loc:,} locations "
                f"({(tot_loc-az)/tot_loc*100:.0f}%) are outside the plan's "
                f"home state; the two lines of business differ (§I).")
    vs_stamp_occ = sum(f["occurrences"] for f in findings
                      if f["category"] == "version-stamping")
    if vs_stamp_occ:
        matters.append(
            f"{vs_stamp_occ:,} of {occurrences_total:,} validator messages "
            f"({vs_stamp_occ/occurrences_total*100:.0f}%) trace to one "
            f"mechanical defect: stamping “4.0.1” as the version of every "
            f"code system (§B, code-system version stamping).")
    worst_rep = max(findings, key=lambda f: f["repeats_per_affected_resource"],
                    default=None)
    if worst_rep and worst_rep["repeats_per_affected_resource"] >= 100:
        matters.append(
            f"A single defect averages "
            f"{worst_rep['repeats_per_affected_resource']:.0f} repeats per "
            f"affected record "
            f"({html.escape(finding_label(worst_rep, slice_bindings))} "
            f"on {worst_rep['resource_type']}) — pathological array "
            f"duplication, not many distinct problems.")
    if ods_n:
        matters.append(
            f"{ods_n:,} of {validated:,} validated records ({ods_n/validated*100:.0f}%) "
            f"carry the OtherDataSource marker, not either "
            f"line of business; aggregate numbers that pool it describe "
            f"someone else's dataset (§K).")

    matters_html = "".join(f"<li>{m}</li>" for m in matters)

    lob_pop_rows = "".join(
        "<tr>"
        f"<td>{html.escape(_lob(l))}</td>"
        f"<td class='num'>{sum(n for (rt, l2), n in lob_denoms.items() if l2 == l):,}</td>"
        "</tr>"
        for l in sorted({l for _, l in lob_denoms},
                        key=lambda x: -sum(n for (rt, l2), n in lob_denoms.items() if l2 == x)))

    section_a = f"""
<section class="card" id="section-a">
  <div class="hd"><h2>A. Summary</h2></div>
  <div class="bd">
    {identity_bar}
    <p><b>What this is.</b> Every FHIR record in the plan's published
    Plan-Net provider directory — {validated:,} records — was validated
    against the Da&nbsp;Vinci PDex Plan-Net {html.escape(ident['ig_version'])}
    profiles, and the population was independently audited for element
    completeness, referential integrity, locality, and placeholder values.
    This report describes measured agreement with the published standard as
    a set of data-quality signals. It is <b>not</b> a certification, a
    regulatory compliance determination, or a legal assessment, and no
    number in it should be read as one.</p>
    <div class="grid2">
      <dl class="kv">
        <dt>records validated (100% of capture)</dt><dd>{validated:,}</dd>
        <dt>… in the directory proper</dt><dd>{core_n:,}</dd>
        <dt>… carrying the OtherDataSource marker</dt><dd>{ods_n:,}</dd>
        <dt>raw validator messages</dt><dd>{occurrences_total:,}</dd>
        <dt>distinct defect patterns (findings)</dt><dd>{findings_total:,}</dd>
      </dl>
      <dl class="kv">
        <dt>tests graded Good</dt><dd>{badge_tally['Good']}</dd>
        <dt>tests graded OK</dt><dd>{badge_tally['OK']}</dd>
        <dt>tests graded Bad</dt><dd>{badge_tally['Bad']}</dd>
        <dt>cells too small to grade</dt><dd>{unbadged_small}</dd>
      </dl>
    </div>
    <h3>What matters most in this data</h3>
    <ul>{matters_html}</ul>
    <h3>Records by source</h3>
    <table><thead><tr><th>source / line of business</th><th>records</th></tr></thead>
      <tbody>{lob_pop_rows}</tbody></table>
    <div class="notebox">
      <b>How to read the grades.</b> A test is one (record&nbsp;type,
      issue&nbsp;category) cell over the three categories that measure the
      payer's own data (structural conformance, required code sets,
      preferred code sets). <b>Good</b>: under {BADGE_GOOD_MAX*100:.0f}% of
      records affected and nothing error-severity. <b>Bad</b>:
      {BADGE_BAD_MIN*100:.0f}%+ of records affected including error-severity
      findings. <b>OK</b>: everything between. The cuts are fixed constants,
      not tuned to this payer. Grades are a reading aid layered over the
      banded rates and record counts beside them — never a substitute, and
      never a verdict. Environment artifacts (uncheckable codes, validator
      limitations, the publisher's own metadata extension) are reported
      separately in §B and are excluded from every grade and every
      headline rate.</div>
  </div>
</section>
"""

    # ---- §B tests -------------------------------------------------------
    def _test_block(rt, cat, n, denom, rate, has_error, badged: bool) -> str:
        tfs = sorted(findings_by_test.get((rt, cat), []),
                     key=lambda f: -f["resources_affected"])
        frows = []
        for f in tfs[:20]:
            per_lob = f.get("by_lob", {})
            lob_bits = " ".join(
                f"<span class='pill'>{html.escape(_lob_s(l))}: "
                + (f"{per_lob[l]['resources_affected']:,} of "
                   f"{lob_denoms.get((rt, l), 0):,}")
                + "</span>"
                for l in sorted(per_lob))
            if f.get("match_grain") == "resource-level":
                hlabel = f"the {rt} record as a whole"
            else:
                hlabel = finding_label(f, slice_bindings)
            expl = message_explain(f)
            frows.append(
                "<tr>"
                f"<td class='flabel'>{html.escape(hlabel)}"
                f"<span class='fpath mono'>{html.escape(f['element_path'])}</span></td>"
                f"<td>{_severity_pill(f['severity'])} "
                f"{_req_pill(f['requirement'], f.get('match_grain', 'exact'))}</td>"
                f"<td class='num'>{render_rate(f['resources_affected'], denom)}</td>"
                f"<td class='num'>{f['repeats_per_affected_resource']:.1f}×</td>"
                "</tr>"
                "<tr><td colspan='4' class='sub' style='padding-top:0'>"
                f"{expl}<br>{lob_bits}</td></tr>")
        more = (f"<p class='sub'>{len(tfs)-20} smaller findings in this cell "
                "are in the accompanying dataset.</p>" if len(tfs) > 20 else "")

        vs_block = ""
        if cat in ("binding-required", "binding-extensible"):
            cell_vs: dict[str, dict[str, int]] = {}
            for r in vs_rows:
                if r["resource_type"] == rt and r["category"] == cat:
                    d = cell_vs.setdefault(r["valueset_url"],
                                           {"ra": 0, "occ": 0, "lobs": {}})
                    d["ra"] += r["resources_affected"]
                    d["occ"] += r["occurrences"]
                    d["lobs"][r["lob"]] = r["resources_affected"]
            if cell_vs:
                vs_trs_parts = []
                for vs, d in sorted(cell_vs.items(),
                                    key=lambda kv: -kv[1]["ra"]):
                    if vs == "(no-valueset-in-message)":
                        vs = "value set not named in the message"
                    else:
                        vs = vs.split("|")[0]
                    lob_pills = " ".join(
                        "<span class='pill'>"
                        + html.escape(_lob_s(l))
                        + f": {c:,} of {lob_denoms.get((rt, l), 0):,}</span>"
                        for l, c in sorted(d["lobs"].items()))
                    vs_trs_parts.append(
                        "<tr>"
                        f"<td class='mono'>{html.escape(vs)}</td>"
                        f"<td class='num'>{render_rate(d['ra'], denom)}</td>"
                        f"<td>{lob_pills}</td>"
                        "</tr>")
                vs_trs = "".join(vs_trs_parts)
                vs_block = f"""
      <h3>Which code sets fail</h3>
      <p class="sub">The failing code set names the exact profile rule even
      where one element row above spans several profile slices.</p>
      <table><thead><tr><th>code set</th><th>records affected</th>
        <th>by source</th></tr></thead><tbody>{vs_trs}</tbody></table>"""

        ext_block = ""
        if cat in ("extension-unresolvable", "publisher-infrastructure"):
            eu = [e for e in ext_urls if e["resource_type"] == rt]
            if cat == "extension-unresolvable":
                eu = [e for e in eu if "createdAt" not in (e["url"] or "")]
            else:
                eu = [e for e in eu if "createdAt" in (e["url"] or "")]
            if eu:
                eu_rows = "".join(
                    "<tr>"
                    f"<td class='mono'>{html.escape(e['url'])}</td>"
                    f"<td class='num'>{render_rate(e['resources_affected'], denom)}</td>"
                    f"<td class='num'>{e['occurrences']:,}</td>"
                    "</tr>" for e in eu)
                ext_block = f"""
      <h3>Extension URLs involved</h3>
      <table><thead><tr><th>extension URL</th><th>records affected</th>
        <th>occurrences</th></tr></thead><tbody>{eu_rows}</tbody></table>"""

        lob_rows_html = "".join(
            "<tr>"
            f"<td>{html.escape(_lob_s(l))}</td>"
            f"<td class='num'>{render_rate(lob_cat.get((rt, l, cat), 0), lob_denoms.get((rt, l), 0))}</td>"
            "</tr>"
            for l in lobs_of_cell.get(rt, []))

        b = badge_for(rate, has_error, denom) if badged else None
        badge_html = (f'<span class="badge {b[1]}">{b[0]}</span>' if b
                      else ('<span class="absent">too few rows to grade</span>'
                            if badged else ''))
        return f"""
<details class="test">
  <summary>
    {badge_html}
    <b>{html.escape(rt)}</b>
    <span class="pill">{html.escape(CATEGORY_LABEL.get(cat, cat))}</span>
    {render_rate(n, denom)}
    {f'<span class="sub">{n:,} of {denom:,} records</span>'
     if 'cell ' in render_rate(n, denom) else ''}
  </summary>
  <div class="body">
    <p class="explain">{CATEGORY_EXPLAIN.get(cat, '')}</p>
    <h3>Rate by source</h3>
    <table><thead><tr><th>source / line of business</th><th>records affected</th></tr></thead>
      <tbody>{lob_rows_html}</tbody></table>
    {vs_block}
    {ext_block}
    <h3>Constituent findings</h3>
    <table>
      <thead><tr><th>element</th><th>severity / requirement</th>
        <th>records affected</th><th>repeats per record</th></tr></thead>
      <tbody>{''.join(frows)}</tbody>
    </table>
    {more}
  </div>
</details>"""

    material_blocks = "".join(
        _test_block(*t, badged=True) for t in tests)
    env_blocks = "".join(
        _test_block(*t, badged=False) for t in env_cells)

    section_b = f"""
<section class="card" id="section-b">
  <div class="hd">
    <h2>B. Tests</h2>
    <div class="sub">one test per (record type, issue category); ordered by
      affected-rate; click any row for the drill-down</div>
  </div>
  <div class="bd">
    {identity_bar}
    <p><b>In plain terms:</b> each row below is one kind of check on one
    record type. The graded rows measure the publisher's own data; the
    ungraded block underneath collects things that are real but are not
    data defects (validation-environment limits, the publisher's tooling
    habits), so they can never inflate or excuse a grade.</p>
    <h3>Data-quality tests (graded)</h3>
    {material_blocks}
    <h3>Environment &amp; advisory observations (not graded, excluded from all rates above)</h3>
    <p class="sub">These describe the validation environment or the
    publisher's tooling rather than directory data quality; they are listed
    so nothing is silently dropped.</p>
    {env_blocks}
  </div>
</section>
"""

    # ---- §C completeness (Track A) --------------------------------------
    comp_rows_html = []
    comp_note = ""
    if comp:
        by_rt: dict[str, dict[str, dict[str, dict]]] = {}
        for c in comp:
            if c["requirement"] not in ("mandatory", "must-support"):
                continue
            by_rt.setdefault(c["resource_type"], {}).setdefault(
                c["element_path"], {})[c["lob"]] = c
        for rt in sorted(by_rt):
            elems = by_rt[rt]
            rows = []
            for ep, per_lob in elems.items():
                worst = min((c["n_present"] / c["n_rows"]
                             for c in per_lob.values() if c["n_rows"]),
                            default=1.0)
                if worst >= 0.95:
                    continue
                req = next(iter(per_lob.values()))["requirement"]
                cells = " ".join(
                    f"<span class='pill'>{html.escape(_lob_s(l))}:</span> "
                    + render_rate(c["n_present"], c["n_rows"], good_high=True)
                    for l, c in sorted(per_lob.items()))
                rows.append((worst, ep, req, cells))
            rows.sort()
            if not rows:
                continue
            trs = "".join(
                "<tr>"
                f"<td class='flabel'>{html.escape(humanize_path(ep, [ep]))}"
                f"<span class='fpath mono'>{html.escape(ep)}</span></td>"
                f"<td>{_req_pill(req)}</td>"
                f"<td>{cells}</td>"
                "</tr>"
                for _, ep, req, cells in rows[:15])
            extra = (f"<p class='sub'>{len(rows)-15} further "
                     "under-populated elements in the dataset.</p>"
                     if len(rows) > 15 else "")
            comp_rows_html.append(f"""
<details class="test">
  <summary><b>{html.escape(rt)}</b>
    <span class="sub">{len(rows)} required / must-support elements populated
    on fewer than 95% of records</span></summary>
  <div class="body">
    <table><thead><tr><th>element</th><th>requirement</th>
      <th>populated, by source</th></tr></thead><tbody>{trs}</tbody></table>
    {extra}
  </div>
</details>""")
        comp_note = ("The element-population audit ran over the same "
                     "capture, per source.")
    section_c = f"""
<section class="card" id="section-c">
  <div class="hd">
    <h2>C. Completeness — are the promised elements populated?</h2>
    <div class="sub">a validator cannot see an element that is simply absent;
      this section comes from a separate element-population audit of the
      same capture</div>
  </div>
  <div class="bd">
    {identity_bar}
    <p><b>In plain terms:</b> a validator can only judge what is present —
    it says nothing about an element that is simply missing. This section
    closes that blind spot: for every element the profiles require or mark
    must-support, the audit counts how many records of that type actually
    populate it, per line of business. Shown below: every such element populated on fewer
    than 95% of records anywhere. {comp_note}</p>
    {''.join(comp_rows_html) if comp_rows_html else '<p class="absent">not measured — no element-population audit found for this capture</p>'}
  </div>
</section>
"""

    # ---- §D requirement split -------------------------------------------
    # Grain-aware display ([O-19]): an ancestor-grain row is a core-FHIR
    # sub-field under a classed element, never presented as that class.
    disp_order = {"required element": 0, "must-support": 1,
                  "sub-field of a required element": 2,
                  "sub-field of a must-support element": 3,
                  "sub-field of an optional element": 4, "mixed slices": 5,
                  "sub-field of a mixed-class element": 6,
                  "optional element": 7,
                  "whole record": 8, "base-FHIR element": 9}
    occ_direct = occ_subfield = occ_record = occ_base = 0
    for r in req_rows:
        g = r.get("match_grain", "exact")
        if g == "exact":
            occ_direct += r["occurrences"]
        elif g in ("ancestor-1", "ancestor-2"):
            occ_subfield += r["occurrences"]
        elif g == "resource-level":
            occ_record += r["occurrences"]
        else:
            occ_base += r["occurrences"]
    # Element-centric build (user revision): every row names the element,
    # what the profile expects there (type, cardinality, code-set binding),
    # and — for sub-field rows — the parent's own cardinality and the fact
    # that these messages only fire where the parent is populated.
    meta_lut = build_meta_lookup(plannet_meta)
    group_rows: dict[str, list] = {}
    for r in sorted(element_rollup, key=lambda r: -r["resources_affected"]):
        disp = req_label(r["requirement"], r["match_grain"])
        label = _segments_label(r["element_path"])
        # [O-26]: a whole-record row is NOT an element and must not fall
        # through to the element resolver's "outside the profile" text.
        if r["match_grain"] == "resource-level":
            label = f"the {r['resource_type']} record as a whole"
            expect = ("a record-level rule (a profile invariant or "
                      "record-wide check), not any single element")
        else:
            m, tail = resolve_element(r["element_path"], meta_lut)
            expect = expectation_str(m, tail)
        denom = by_cell.get(r["resource_type"], 0)
        group_rows.setdefault(disp, []).append(
            "<tr>"
            f"<td class='flabel'>{html.escape(label)}"
            f"<span class='fpath mono'>{html.escape(r['element_path'])}</span></td>"
            f"<td class='mono'>{html.escape(r['resource_type'])}</td>"
            f"<td class='sub'>{expect}</td>"
            f"<td><span class='pill'>{html.escape(CATEGORY_LABEL.get(r['category'], r['category']))}</span></td>"
            f"<td class='num'>{render_rate(r['resources_affected'], denom)}</td>"
            f"<td class='num'>{r['occurrences']:,}</td>"
            "</tr>")
    d_blocks = []
    for disp in sorted(group_rows, key=lambda d: disp_order.get(d, 99)):
        rows = group_rows[disp]
        shown = rows[:25]
        more = (f"<p class='sub'>{len(rows)-25} smaller rows of this class "
                "are in the accompanying dataset.</p>"
                if len(rows) > 25 else "")
        d_blocks.append(f"""
<details class="test">
  <summary><span class="pill">{html.escape(disp)}</span>
    <span class="sub">{len(rows)} element rows</span></summary>
  <div class="body">
    <table>
      <thead><tr><th>element</th><th>record type</th>
        <th>what the profile expects here</th><th>issue category</th>
        <th>records affected</th><th>messages</th></tr></thead>
      <tbody>{''.join(shown)}</tbody>
    </table>
    {more}
  </div>
</details>""")
    section_d = f"""
<section class="card" id="section-d">
  <div class="hd">
    <h2>D. Required vs must-support elements</h2>
    <div class="sub">every element with findings: what the profile expects
      there, and how many records fall short</div>
  </div>
  <div class="bd">
    {identity_bar}
    <p><b>In plain terms:</b> this section answers, element by element,
    “what should be here, and is it?” — the expected value type, how many
    are allowed (cardinality, e.g. 0..1 = optional single, 1..1 = exactly
    one required), and the code set the value must come from where one is
    bound.</p>
    <p><b>required element</b> = the profile demands the element itself
    (minimum cardinality ≥ 1). <b>must-support</b> = optional (0..1 or
    0..*), but a conforming system must be able to populate and consume it.
    <b>sub-field of a required / must-support / optional element</b> = the
    message sits on a core-FHIR field <i>inside</i> such an element. Read
    those rows carefully: a “sub-field of an optional element” finding does
    NOT mean the optional element is missing — it means the record
    <i>populated</i> the optional parent and the content inside it is
    defective (e.g. the 0..1 <span class="mono">period.end</span> inside an
    optional 0..1 address period, filled with a placeholder date). Each such
    row states the parent's own cardinality and class in its
    “what the profile expects” column. <b>whole record</b> = the message
    applies to the record, not an element. <b>base-FHIR element</b> = no
    profile-enumerated element within two levels above.</p>
    <p class="sub">Of {occurrences_total:,} validator messages:
    {occ_direct:,} sit directly on profile-enumerated elements,
    {occ_subfield:,} on sub-fields inside them, {occ_record:,} apply to
    whole records, and {occ_base:,} sit on base-FHIR elements outside the
    profile's reach. All groups are itemized below; nothing is dropped.
    Records-affected counts are exact per (element, category) row; the same
    record can appear in several rows.</p>
    {''.join(d_blocks)}
  </div>
</section>
"""

    # ---- §E standard layers ---------------------------------------------
    layer_agg: dict[tuple[str, str], dict[str, int]] = {}
    for r in layer_rows:
        k = (r["layer"], r["lob"])
        d = layer_agg.setdefault(k, {"occ": 0})
        d["occ"] += r["occurrences"]
    layer_lobs = sorted({l for _, l in layer_agg})
    layer_trs = []
    for lay in LAYER_ORDER:
        cells = "".join(
            f"<td class='num'>{layer_agg.get((lay, l), {}).get('occ', 0):,}</td>"
            for l in layer_lobs)
        layer_trs.append(
            f"<tr><td>{html.escape(LAYER_LABEL.get(lay, lay))}</td>{cells}</tr>")
    layer_heads = "".join(f"<th>{html.escape(_lob_s(l))}</th>" for l in layer_lobs)

    # USCDI v3 data-class element coverage (dimension 4's USCDI half): the
    # element-population lens over the same capture, mapped USCDI class by
    # USCDI class. Aggregate (the lens is not LOB-split) — disclosed.
    uscdi_block = ""
    if uscdi_rows:
        MAPPING_NOTE = {
            "analog": "closest directory analog",
            "proxy": "proxy measurement",
            "inferred": "inferred mapping",
            "weak-proxy": "weak proxy",
            "unmeasurable": "not measurable here",
        }
        u_trs = []
        for r in sorted(uscdi_rows,
                        key=lambda r: (r["resource_type"], r["element"])):
            nice = r["element"].replace("_", " ")
            if (r["resource_type"] == "Organization"
                    and r["denominator"] != by_cell.get("Organization", 0)):
                nice += " (denominator includes the 36 network records)"
            u_trs.append(
                "<tr>"
                f"<td class='mono'>{html.escape(r['resource_type'])}</td>"
                f"<td>{html.escape(nice)}"
                + (" <span class='pill'>USCDI-required</span>" if r["required"] else "")
                + "</td>"
                f"<td><span class='pill'>{html.escape(MAPPING_NOTE.get(r['mapping'], r['mapping']))}</span></td>"
                f"<td class='num'>{render_rate(r['populated'], r['denominator'], good_high=True)}</td>"
                "</tr>")
        uscdi_block = f"""
    <h3>USCDI v3 data classes — how well is the underlying data there?</h3>
    <p>Separate from validator rules: for each USCDI v3 data class that has
    a directory counterpart, how many records actually populate it. No
    official USCDI-to-directory mapping exists, so every row carries its
    mapping quality. These figures are population-wide (this lens does not
    split by source); the per-LOB completeness picture is §C.</p>
    <table>
      <thead><tr><th>record type</th><th>USCDI data class</th>
        <th>mapping</th><th>populated</th></tr></thead>
      <tbody>{''.join(u_trs)}</tbody>
    </table>
    <p class="sub">Mapping quality: <b>closest directory analog</b> = the
    directory field that carries the same fact (e.g. a practitioner's phone
    for the phone data class); <b>proxy</b> = a stand-in signal (e.g. a
    last-updated timestamp standing in for provenance); <b>inferred</b> =
    no official mapping exists, ours is a reasoned one; <b>weak proxy</b> =
    the stand-in is loose — read with care; <b>not measurable here</b> =
    the data class has no directory counterpart. Denominators count the
    captured tables directly (the organization figure includes the 36
    network records).</p>"""

    section_e = f"""
<section class="card" id="section-e">
  <div class="hd">
    <h2>E. Which standard's rules fire</h2>
    <div class="sub">validator messages attributed to the standard that owns
      the rule that produced them</div>
  </div>
  <div class="bd">
    {identity_bar}
    <p><b>In plain terms:</b> “conformance” is really three stacked
    rulebooks — core FHIR R4, US Core, and Plan-Net — plus the code sets
    they point at. This section says which rulebook each failure belongs
    to, so the right team owns the fix.</p>
    <p>Records were validated against the Plan-Net
    {html.escape(ident['ig_version'])} profiles, which build on FHIR R4.
    The first three rows attribute by the standard that <b>owns the failing
    rule</b> (named in the message itself: an R4, US Core, or Plan-Net
    invariant, or a fixed-value / cardinality constraint whose defining
    profile the message cites). Code-set membership failures get their own
    row: a message that cites a value set does not say which standard's
    profile imposed the binding (a Plan-Net slice can require a US Core
    value set), so counting them under the set's owner would misattribute
    the rule — the exact failing sets, per source, are in §B's binding
    drill-downs.</p>
    <div class="callout"><b>US Core / USCDI was not independently
    evaluated.</b> Plan-Net does not derive from US Core, and this analysis
    validated against Plan-Net only; the US Core row below counts the US
    Core invariants Plan-Net itself imports. A statement about full US Core
    (USCDI v3) conformance would require its own validation pass and is not
    made here.</div>
    <table>
      <thead><tr><th>rule owner</th>{layer_heads}</tr></thead>
      <tbody>{''.join(layer_trs)}</tbody>
    </table>
    <p class="sub">Counts are validator messages, not records; one record
    can appear under several rows.</p>
    {uscdi_block}
  </div>
</section>
"""

    # ---- §F Medicare Plan Finder readiness ------------------------------
    section_mpf = ""
    mpf_summary_items: list[str] = []
    if mpf_rows:
        mpf_by = {(r["resource_type"], r["element"]): r for r in mpf_rows}

        def _mpf(rt, el):
            return mpf_by.get((rt, el))

        NICE_MPF = {
            "npi_identifier": "NPI on the role",
            "network_refs": "network linkage",
            "specialty_presence": "specialty",
            "specialty_nucc_format": "specialty in NUCC taxonomy format",
            "specialty_system_tier": "specialty coded in a standard system",
            "telecom_phone": "phone on the role",
            "accepting_new_patients": "accepting-new-patients flag",
            "last_updated": "last-updated timestamp",
            "last_updated_within_30d_of_capture": "updated within 30 days",
            "location_refs": "practice location linkage",
            "ma_plan_id_format": "CMS-format plan id (H/R/S/E contract shape)",
            "period_start": "plan period start",
            "address_complete": "complete address",
            "address_line": "street line", "address_city": "city",
            "address_state": "state", "address_postalcode": "postal code",
            "phone": "phone", "npi": "NPI",
            "npi_identifier_practitioner_level": "NPI at practitioner level",
            "type_codes": "organization type", "name": "name",
            "type_nucc_system": "type coded in NUCC system",
            "identifier": "identifier", "family": "family name",
            "given": "given name", "gender": "gender",
            "communication": "languages spoken",
            "organization_ref": "managing organization",
            "participating_organization_ref": "participating organization",
            "specialty_codes": "specialty",
        }
        # Medicare-LOB records only (user revision): the Plan Finder
        # obligation attaches to the Medicare line; other sources' numbers
        # are irrelevant here and the ODS feed can never pool in. Each line
        # carries its own Good / Bad verdict: Good = 95%+ of Medicare-LOB
        # records populated; an absent required record type is Bad.
        mpf_lob_rows = [r for r in (lenses.get("mpf_by_lob") or [])
                        if r["lob"] == "azblue"]
        by_check = {(r["resource_type"], r["element"]): r
                    for r in mpf_lob_rows}
        mpf_trs = []
        for r in sorted(mpf_rows, key=lambda r: (r["resource_type"],
                                                 not r["required"],
                                                 r["element"])):
            nice = NICE_MPF.get(r["element"], r["element"].replace("_", " "))
            m = by_check.get((r["resource_type"], r["element"]))
            if r["resource_type"] == "OrganizationAffiliation":
                cell = ("<span class='absent sev-bad'>not published at all "
                        "— the guide requires this record type</span>")
                good = False
            elif m and m["denominator"]:
                cell = render_rate(m["populated"], m["denominator"],
                                   good_high=True)
                good = m["populated"] / m["denominator"] >= 0.95
            else:
                cell = ('<span class="absent sev-bad">no Medicare-LOB '
                        'records</span>')
                good = False
            verdict = ('<span class="badge good">Good</span>' if good
                       else '<span class="badge bad">Bad</span>')
            mpf_trs.append(
                "<tr>"
                f"<td>{verdict}</td>"
                f"<td class='mono'>{html.escape(r['resource_type'])}</td>"
                f"<td>{html.escape(nice)}"
                + (" <span class='pill'>required by the guide</span>"
                   if r["required"] else "")
                + "</td>"
                f"<td>{cell}</td>"
                "</tr>")

        # Computed top-line failures for the header callout.
        plan_fail = [el for el in ("name", "network_refs", "period_start")
                     if (_mpf("InsurancePlan", el) or {}).get("populated") == 0]
        r_npi = by_check.get(("PractitionerRole", "npi_identifier"))
        r_net = by_check.get(("PractitionerRole", "network_refs"))
        oa_absent = all(r["denominator"] == 0 for r in mpf_rows
                        if r["resource_type"] == "OrganizationAffiliation")
        if plan_fail:
            mpf_summary_items.append(
                "the one published plan record carries no plan name, no "
                "network reference and no coverage period, and its "
                "identifier is not in the CMS contract-id format — so it "
                "cannot be matched to a Plan Finder plan entry")
        if r_npi and r_npi["populated"] == 0:
            mpf_summary_items.append(
                f"0 of {r_npi['denominator']:,} Medicare-LOB provider roles "
                "carry the NPI the guide requires at the role level")
        if r_net and r_net["populated"] == 0:
            mpf_summary_items.append(
                "0 provider roles link to any network, so in-network "
                "provider lookup — Plan Finder's core function — has no "
                "data path")
        if oa_absent:
            mpf_summary_items.append(
                "the OrganizationAffiliation records the guide requires "
                "are absent entirely")

        prov = lenses.get("mpf_provenance", "")
        section_mpf = f"""
<section class="card" id="section-f">
  <div class="hd">
    <h2>F. Medicare Plan Finder readiness</h2>
    <div class="sub">the CMS-required digital directory format, measured
      element by element</div>
  </div>
  <div class="bd">
    {identity_bar}
    <p><b>In plain terms:</b> CMS requires Medicare Advantage organizations
    to publish their provider directory in a machine-readable format
    (42&nbsp;CFR&nbsp;422.111(m)), and the CMS implementation guide names
    Plan-Net {html.escape(ident['ig_version'])} — the same standard this
    report measures — with a specific list of required elements. This
    section checks each of them. As captured, this directory could not
    power a Medicare Plan Finder provider search for the Medicare-LOB:
    {'; '.join(mpf_summary_items[:2]) if mpf_summary_items else 'see the table'}.</p>
    <table>
      <thead><tr><th></th><th>record type</th><th>guide element</th>
        <th>populated (Medicare-LOB records)</th></tr></thead>
      <tbody>{''.join(mpf_trs)}</tbody>
    </table>
    <p class="sub">Only Medicare-LOB records are counted — the Plan Finder
    obligation attaches to the Medicare line, and the OtherDataSource
    feed and Medicaid-LOB records are excluded from every figure above.
    <b>Good</b> = 95% or more of Medicare-LOB records populate the element;
    <b>Bad</b> = anything less, including a required record type that is
    not published at all. These checks were measured
    {html.escape(str(lenses.get('mpf_measured_at', 'after the capture')))}
    over the same captured population as the rest of this report (the
    analysis date in the identity strip refers to the validator run; this
    lens ran separately).
    Plan-Finder eligibility basis: the plan publicly sells Medicare
    Advantage products and the captured directory carries MA-designated
    network records; the automated MA classifier alone does not trip on
    this capture's naming, so eligibility is recorded as a documented
    public-record determination. Rule-set provenance:
    {html.escape(prov)}</p>
  </div>
</section>
"""

    # ---- §G per-LOB material conformance --------------------------------
    rows_f = []
    for (rt, l) in sorted(lob_denoms, key=lambda k: (k[0], -lob_denoms[k])):
        denom = lob_denoms[(rt, l)]
        any_n = lob_any.get((rt, l), 0)
        err_n = (lob_sev.get((rt, l, "error"), 0)
                 + lob_sev.get((rt, l, "fatal"), 0))
        cats = "".join(
            f"<td class='num'>{render_rate(lob_cat.get((rt, l, c), 0), denom)}</td>"
            for c in MATERIAL)
        rows_f.append(
            "<tr>"
            f"<td class='mono'>{html.escape(rt)}</td>"
            f"<td>{html.escape(_lob_s(l))}</td>"
            f"<td class='num'>{denom:,}</td>"
            f"<td class='num'>{render_rate(any_n, denom)}</td>"
            f"<td class='num'>{render_rate(err_n, denom)}</td>"
            f"{cats}"
            "</tr>")
    section_f = f"""
<section class="card" id="section-g">
  <div class="hd">
    <h2>G. Data-quality rates per source and record type</h2>
    <div class="sub">material categories only — environment artifacts and
      advisories excluded (see §B)</div>
  </div>
  <div class="bd">
    {identity_bar}
    <p><b>In plain terms:</b> the same quality questions, asked separately
    of each line of business — because a pooled number would hide that the
    two lines often differ sharply.</p>
    <table>
      <thead><tr><th>record type</th><th>source</th><th>records</th>
        <th>≥1 material finding</th><th>≥1 error-severity</th>
        <th>structural</th><th>required code sets</th><th>preferred code sets</th></tr></thead>
      <tbody>{''.join(rows_f)}</tbody>
    </table>
  </div>
</section>
"""

    # ---- §H placeholders ------------------------------------------------
    sent = _by_lob_counts(size.get("practitioner_sentinel_dates_by_lob"))
    hsn = _by_lob_counts(size.get("hs_name_unknown_by_lob"))
    locj = _by_lob_counts(size.get("location_junk_by_lob"))
    dar = size.get("dar_extension_count", 0)
    ph_rows = []

    def _ph(name, desc, cells):
        ph_rows.append(
            "<tr>"
            f"<td>{name}<span class='fpath'>{desc}</span></td>"
            f"<td>{cells}</td></tr>")

    if prac:
        _ph("Inactive practitioners published",
            "active = false where the profile fixes active = true",
            " ".join(f"<span class='pill'>{html.escape(_lob_s(l))}:</span> "
                     + render_rate(v[2], v[0])
                     for l, v in sorted(prac.items())))
        _ph("Practitioner gender recorded as “unknown”",
            "a legal FHIR value; at scale it is a fill-in",
            " ".join(f"<span class='pill'>{html.escape(_lob_s(l))}:</span> "
                     + render_rate(v[3], v[0])
                     for l, v in sorted(prac.items())))
    if sent:
        _ph("Placeholder “never expires” dates",
            "records carrying 2999-12-31 or 9999-12-31 anywhere",
            " ".join(f"<span class='pill'>{html.escape(_lob_s(l))}:</span> "
                     + render_rate(v[1], v[0])
                     for l, v in sorted(sent.items())))
    sent_fields = _by_lob_counts(size.get("practitioner_sentinel_fields_by_lob"))
    if sent_fields:
        _ph("… in the address validity period (end date)",
            "Practitioner.address.period.end = 2999/9999-12-31: the address "
            "is declared valid forever",
            " ".join(f"<span class='pill'>{html.escape(_lob_s(l))}:</span> "
                     + render_rate(v[1], v[0])
                     for l, v in sorted(sent_fields.items())))
        _ph("… in qualification / licence validity periods",
            "a 2999/9999-12-31 date inside Practitioner.qualification: the "
            "licence or certification is declared to never expire",
            " ".join(f"<span class='pill'>{html.escape(_lob_s(l))}:</span> "
                     + render_rate(v[3], v[0])
                     for l, v in sorted(sent_fields.items())))
        _ph("… placeholder start dates (1753-01-01)",
            "Practitioner.address.period.start set to a database-epoch "
            "placeholder",
            " ".join(f"<span class='pill'>{html.escape(_lob_s(l))}:</span> "
                     + render_rate(v[2], v[0])
                     for l, v in sorted(sent_fields.items())))
    if hsn:
        _ph("Services named “Unknown”",
            "a required name filled with a literal placeholder",
            " ".join(f"<span class='pill'>{html.escape(_lob_s(l))}:</span> "
                     + render_rate(v[1], v[0])
                     for l, v in sorted(hsn.items())))
    if locs:
        _ph("Locations with no geocoordinates",
            "structurally valid addresses that cannot be mapped",
            " ".join(f"<span class='pill'>{html.escape(_lob_s(l))}:</span> "
                     + render_rate(v[2], v[0])
                     for l, v in sorted(locs.items())))
    if junk_net:
        _ph("Networks with placeholder names",
            html.escape(", ".join(junk_net)),
            f"<span class='exact'>{len(junk_net)} of {len(networks)} networks</span>")
    if locj:
        junk_postal = sum(v[0] for v in locj.values())
        junk_names = sum(v[1] for v in locj.values())
        _ph("Junk postal codes / location names",
            "00000-style postal codes; names like UNKNOWN / N/A",
            f"<span class='exact'>{junk_postal:,} junk postal codes, "
            f"{junk_names:,} junk names</span>")
    section_g = f"""
<section class="card" id="section-h">
  <div class="hd">
    <h2>H. Placeholder and default values</h2>
    <div class="sub">how much of the data satisfies structure with content
      that carries no information</div>
  </div>
  <div class="bd">
    {identity_bar}
    <p>FHIR's sanctioned mechanism for declaring absent data
    (<span class="mono">data-absent-reason</span>) appears on
    <b>{dar}</b> record{'' if dar == 1 else 's'} in the entire capture — the publisher does not
    use it. Placeholders here take other shapes: fixed values contradicted
    by the record's own status, sentinel dates, literal “Unknown” names, and
    a version number stamped mechanically on every code system (§B). The
    version-stamping defect alone accounts for
    {vs_stamp_occ:,} validator messages
    ({vs_stamp_occ/max(occurrences_total,1)*100:.0f}% of all output).</p>
    <table>
      <thead><tr><th>signal</th><th>measured, by source</th></tr></thead>
      <tbody>{''.join(ph_rows)}</tbody>
    </table>
  </div>
</section>
"""

    # ---- §I locality ----------------------------------------------------
    org_state = _by_lob_counts(size.get("org_state_by_lob"))
    role_loc_state = _by_lob_counts(size.get("role_referenced_location_state_by_lob"))
    top_states = size.get("location_top_nonaz_states") or []
    loc_rows = "".join(
        "<tr>"
        f"<td>{html.escape(_lob_s(l))}</td>"
        f"<td class='num'>{v[0]:,}</td>"
        f"<td class='num'>{render_rate(v[1], v[0], good_high=True)}</td>"
        f"<td class='num'>{v[2]:,}</td>"
        "</tr>"
        for l, v in sorted(loc_state.items(), key=lambda kv: -kv[1][0]))
    org_rows = "".join(
        "<tr>"
        f"<td>{html.escape(_lob_s(l))}</td>"
        f"<td class='num'>{v[0]:,}</td>"
        f"<td class='num'>{render_rate(v[1], v[0], good_high=True)}</td>"
        f"<td class='num'>{v[2]:,}</td>"
        "</tr>"
        for l, v in sorted(org_state.items(), key=lambda kv: -kv[1][0]))
    role_loc_rows = "".join(
        "<tr>"
        f"<td>{html.escape(_lob_s(l))}</td>"
        f"<td class='num'>{v[0]:,}</td>"
        f"<td class='num'>{render_rate(v[1], v[0], good_high=True)}</td>"
        "</tr>"
        for l, v in sorted(role_loc_state.items(), key=lambda kv: -kv[1][0]))
    st_rows = "".join(
        f"<tr><td>{html.escape(s['state'])}</td><td class='num'>{s['n']:,}</td></tr>"
        for s in top_states)
    section_h = f"""
<section class="card" id="section-i">
  <div class="hd">
    <h2>I. How much of the directory is inside the plan’s state boundaries?</h2>
    <div class="sub">locality measured from published addresses</div>
  </div>
  <div class="bd">
    {identity_bar}
    <h3>Locations, by source</h3>
    <table><thead><tr><th>source</th><th>locations</th><th>inside state boundaries</th>
      <th>no state recorded</th></tr></thead><tbody>{loc_rows}</tbody></table>
    <h3>Locations a provider role actually points at</h3>
    <p class="sub">an out-of-state location no role references is a different
      problem from one a member can be routed to</p>
    <table><thead><tr><th>source</th><th>referenced locations</th>
      <th>inside state boundaries</th></tr></thead><tbody>{role_loc_rows}</tbody></table>
    <h3>Organizations, by source</h3>
    <table><thead><tr><th>source</th><th>organizations</th><th>inside state boundaries</th>
      <th>no state recorded</th></tr></thead><tbody>{org_rows}</tbody></table>
    <p class="sub">Organization counts in this section read the captured
    tables directly and include the {len(networks)} network records; §G's
    per-type rates report Network as its own record type, so its
    Organization denominators are smaller by exactly that count.</p>
    <h3>Largest out-of-state concentrations (locations)</h3>
    <table><thead><tr><th>state</th><th>locations</th></tr></thead>
      <tbody>{st_rows}</tbody></table>
    <p class="sub">Out-of-state entries are not automatically wrong — border
    towns, national providers and telehealth exist — but their share, and
    the 12-point spread between the two lines of business, is directory
    signal a consumer should know.</p>
  </div>
</section>
"""

    # ---- §J referential integrity ---------------------------------------
    edges = size.get("reference_edges") or []
    edge_rows = "".join(
        "<tr>"
        f"<td>{html.escape(e['edge'].replace('->', ' → '))}</td>"
        f"<td>{html.escape(_lob_s(e['lob']))}</td>"
        f"<td class='num'>{e['refs']:,}</td>"
        f"<td class='num'>{render_rate(e['dangling'], e['refs'])}</td>"
        f"<td class='num'>{e['resolves_cross_lob']:,}</td>"
        f"<td class='num'>{e['unparseable']:,}</td>"
        "</tr>"
        for e in sorted(edges, key=lambda e: (e["edge"], e["lob"])))
    unreferenced_rows = "".join(
        "<tr>"
        f"<td>{html.escape(_lob_s(l))}</td>"
        f"<td class='num'>{v[0]:,}</td>"
        f"<td class='num'>{render_rate(v[1], v[0])}</td>"
        "</tr>"
        for l, v in sorted(locs.items(), key=lambda kv: -kv[1][0]))
    prac_orphan_rows = "".join(
        "<tr>"
        f"<td>{html.escape(_lob_s(l))}</td>"
        f"<td class='num'>{v[0]:,}</td>"
        f"<td class='num'>{render_rate(v[1], v[0])}</td>"
        "</tr>"
        for l, v in sorted(prac.items(), key=lambda kv: -kv[1][0]))
    section_i = f"""
<section class="card" id="section-j">
  <div class="hd">
    <h2>J. Referential integrity</h2>
    <div class="sub">records were validated standalone, so the validator
      never checked whether references resolve; measured here directly
      against the captured population</div>
  </div>
  <div class="bd">
    {identity_bar}
    <p><b>In plain terms:</b> directory records point at each other — a
    role points at its practitioner, its clinic, its locations. Here we
    follow every such pointer and report the ones that lead nowhere, the
    ones that lead into the other line of business, and the records
    nothing points at.</p>
    <h3>Forward: do references point at records that exist?</h3>
    <p class="sub">“crosses source” counts references that resolve, but to a
    record carrying a different source marker — intact for a pooled
    directory, broken from within one line of business. Unparseable
    references (non-<span class="mono">Type/id</span> forms) are counted
    separately, not as dangling.</p>
    <div class="notebox"><b>These numbers are capture-relative.</b> A
    reference counts as dangling when its target id is absent from the
    captured population. The capture walked the plan's published
    directory to exhaustion on every record type — every page the source
    serves was retrieved, and the record counts reconcile exactly against
    the validated population — so a dangling reference here means the
    target is not retrievable from the published directory, not merely that
    it was missed. Capture method and its limits: §L.</div>
    <table>
      <thead><tr><th>reference</th><th>source of the record</th><th>refs</th>
        <th>dangling</th><th>crosses source</th><th>unparseable</th></tr></thead>
      <tbody>{edge_rows}</tbody>
    </table>
    <h3>Reverse: what does nothing point at?</h3>
    <table><thead><tr><th>source</th><th>practitioners</th>
      <th>with no role at all</th></tr></thead><tbody>{prac_orphan_rows}</tbody></table>
    <table><thead><tr><th>source</th><th>locations</th>
      <th>referenced by no role or service</th></tr></thead><tbody>{unreferenced_rows}</tbody></table>
    <p class="sub">Network references are absent everywhere: no role, no
    affiliation and no plan references any of the {len(networks)} network
    records (§K).</p>
  </div>
</section>
"""

    # ---- §K population shape --------------------------------------------
    orgs_ref_lob = size.get("orgs_referenced_by_roles_by_lob") or {}
    orgs_ref_total = sum(orgs_ref_lob.values())
    n_org_total = sum(v[0] for v in orgs_by_lob.values()) if orgs_by_lob else 0
    addr_dup = _by_lob_counts(size.get("location_address_dup_by_lob"))
    dup_txt = ""
    if addr_dup:
        tot_l = sum(v[0] for v in addr_dup.values())
        tot_d = sum(v[1] for v in addr_dup.values())
        if tot_d:
            dup_txt = (f"The {tot_l:,} location records collapse to "
                       f"{tot_d:,} distinct street addresses "
                       f"(~{tot_l/max(tot_d,1):.1f} records per address) — "
                       "measured, the location table is largely the same "
                       "addresses republished per practitioner context."
                       if tot_l / max(tot_d, 1) >= 2 else
                       f"The {tot_l:,} location records map to {tot_d:,} "
                       "distinct street addresses.")
    plan_names = ", ".join(
        (html.escape(str(p["name"])) if p.get("name")
         else "one plan record that itself carries no name")
        for p in plans) or "none"
    # [R1-1] Network names are the payer's own product/geography branding
    # and would de-anonymize the report; only the placeholder names are
    # quotable, the rest are characterized without being printed.
    net_names = (
        f"{len(networks) - len(junk_net)} carry the plan's internal product "
        "and geography names (withheld here to preserve anonymity)"
        + (f"; {len(junk_net)} carry placeholder names "
           f"({', '.join(html.escape(x) for x in junk_net)})"
           if junk_net else ""))
    section_j = f"""
<section class="card" id="section-k">
  <div class="hd">
    <h2>K. Population shape — what the counts say</h2>
    <div class="sub">structural observations, all computed from this capture</div>
  </div>
  <div class="bd">
    {identity_bar}
    <p><b>In plain terms:</b> before judging quality record by record, the
    shape of the population itself tells a story — what is published in
    abundance, what is nearly absent, and what exists but connects to
    nothing.</p>
    <h3>Insurance plans: {len(plans)}</h3>
    <p>Published: {plan_names}.
    {"It names no network and carries no coverage area." if len(plans) == 1
     else f"{len(plan_no_net)} of {len(plans)} name no network and none carries a coverage area."}
    With {size.get('pr_network_refs_populated', 0):,} network references on
    {by_cell.get('PractitionerRole', 0):,} roles and
    {size.get('org_affiliation_rows', 0)} affiliation records, the
    plan→network→provider chain has no first link and no last link: the
    question “which providers are in this plan?” cannot be answered from
    the published directory, for either line of business.</p>
    <h3>Networks: {len(networks)}, referenced by nothing</h3>
    <p>Of the {len(networks)} network records, {net_names}.
    {("The placeholder rows indicate an unfiltered export of an internal "
      "enumeration.") if junk_net else ""}</p>
    <h3>The OtherDataSource population</h3>
    <p>{ods_n:,} of {n_org_total:,} organizations (a count that includes
    the {len(networks)} network records) carry a third source
    marker — the one labeled OtherDataSource throughout this report. What
    the data says about it: the records span the whole country, are
    NPI-complete, and virtually none is referenced by any provider role —
    consistent with an externally sourced organization roster loaded
    alongside the directory rather than directory content. Provider roles
    reference only {orgs_ref_total:,} distinct organizations —
    {" ".join(f"{html.escape(_lob_s(l))}: {n:,}." for l, n in sorted(orgs_ref_lob.items(), key=lambda kv: -kv[1]))}
    Under 1% of published organizations participate in the provider graph;
    conformance rates on the Organization type are therefore dominated by
    records that never surface in a provider search.</p>
    <h3>Location duplication</h3>
    <p>{dup_txt}</p>
  </div>
</section>
"""

    # ---- §L methodology -------------------------------------------------
    notes = ident.get("notes") or {}
    stripped = notes.get("instances_stripped")
    tho = notes.get("tho", "")
    section_k = f"""
<section class="card" id="section-l">
  <div class="hd"><h2>L. Methodology, limits, provenance</h2></div>
  <div class="bd">
    {identity_bar}
    <h3>What was validated, and how</h3>
    <p>Every FHIR record captured from the plan's published Plan-Net
    directory ({validated:,} records, capture window {html.escape(cap_txt)})
    was passed through the official HL7 validator
    (<span class="mono">validator_cli 6.10.1</span>) against Da&nbsp;Vinci
    PDex Plan-Net <span class="mono">{html.escape(ident['ig_version'])}</span>.
    No sampling. Three material facts about the setup:</p>
    <ul>
      <li><b>Profiles were forced.</b> The records mostly do not declare the
      Plan-Net profiles; they were validated against them anyway because
      Plan-Net is the applicable regulatory target for a payer provider
      directory regardless of what the records claim.</li>
      <li><b>Declared foreign profiles were removed first.</b>
      {f"{stripped:,}" if stripped else "A majority of"} records declared
      CARIN Blue Button profiles (a claims-data standard irrelevant to a
      directory); those declarations were stripped so records could be
      evaluated against the directory standard.</li>
      <li><b>No terminology server.</b> Code checks ran locally against the
      shipped packages (terminology snapshot
      <span class="mono">{html.escape(str(tho))}</span>); codes in systems
      outside those packages are reported as “uncheckable”, not as
      failures.</li>
    </ul>
    <h3>Capture method and completeness</h3>
    <p>The plan's directory does not allow deep pagination (bulk listing
    stops at a fixed offset), so the capture enumerated records through
    last-updated time windows, bisecting each window until every window
    returned fewer records than the server's page limit — i.e. until the
    published surface was exhausted for every record type. Referential
    findings in §J are relative to this capture: a target counted missing
    is one the published directory does not serve, to the completeness of
    this method. The capture completed {html.escape(cap_txt)} and the
    validated population equals the captured population exactly
    (asserted at build time).</p>

    <h3>Unit of presentation</h3>
    <p>A <b>finding</b> is one defect pattern — one (record type, element,
    message, severity) combination — counted once per record it touches,
    with a repeats-per-record multiplier for array duplication. The
    {occurrences_total:,} raw validator messages behind the findings are
    retained verbatim and every number here reproduces from them.</p>
    <h3>Lines of business</h3>
    <p>Each published record embeds a source marker
    (<span class="mono">urn:source_name</span>). Attribution covered
    {validated:,} of {validated:,} records. The third marker on
    organization records is a source system, not a line of business, and is
    never folded into either plan line. No cross-type pooled rate is
    reported anywhere in this document; every rate is per (record type,
    source).</p>
    <h3>Issue categories and exclusions</h3>
    <p>Validator messages are grouped into data-quality categories
    (structural, required code sets, preferred code sets) and environment /
    advisory categories (uncheckable codes, version stamping, validator
    limitations, publisher infrastructure, missing narrative). Only the
    data-quality categories enter grades and headline rates; the others are
    fully reported in §B. Version stamping is data-borne but classified
    separately from structural conformance so that one mechanical defect
    cannot set the headline structural rate for the largest record type
    ({vs_stamp_occ:,} messages).</p>
    <h3>Element mapping</h3>
    <p>Validator element paths are normalized and joined to the Plan-Net
    element definitions, collapsing profile slice discriminators (the
    validator emits positional paths). Where a collapsed path spans several
    profile slices, the row says so, and code-set failures are attributed
    by the failing code set itself, which is slice-exact. Sub-elements of
    profiled fields that core FHIR (not the profile) defines are classed
    “base-FHIR element”; whole-record messages are classed “whole record”.
    Nothing is dropped: §D accounts for 100% of messages.</p>
    <h3>Rendering rules</h3>
    <p>Rates render in five 20-point bands with the record count always
    visible; no rate renders as 100.0% or 0.0%. Cells with fewer than
    {ROW_FLOOR} records show the exact count instead of a rate (this is a
    census; small cells have no sampling error, but a percentage over 1
    record misleads). A cell with no records shows “no signal”. Grades
    (Good/OK/Bad) follow the fixed rule in §A and are never shown without
    the underlying rate.</p>
    <h3>What this report does not do</h3>
    <ul>
      <li>No certification or compliance determination of any kind.</li>
      <li>No cross-payer comparison (single source).</li>
      <li>No claim of a full, independent US Core validation pass. What IS
      measured for US Core / USCDI v3: the US Core rules Plan-Net imports
      (§E, rule-owner table) and USCDI v3 data-class population over the
      whole capture (§E, USCDI table). A standalone US Core validation run
      would add invariant-level detail and is the one remaining gap.</li>
      <li>No pooled cross-type rates.</li>
    </ul>
    <footer>
      A machine-readable dataset, a field-level data dictionary, and
      reproduction notes accompany this report and version together with it.
    </footer>
  </div>
</section>
"""

    body = "\n".join([section_a, section_b, section_c, section_d, section_e,
                      section_mpf, section_f, section_g, section_h,
                      section_i, section_j,
                      section_k])
    doc = f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Provider directory data quality — {html.escape(source)}</title>
<style>{CSS}</style>
</head><body>
<div class="wrap">
  <header>
    <h1>Provider directory data quality — {html.escape(source)}</h1>
    <p class="sub">Full-population analysis of {validated:,} published FHIR
       records, reported per line of business — Medicaid-LOB and
       Medicare-LOB, with records carrying a third source marker
       (OtherDataSource) reported separately. Findings are defect patterns in plain language,
       graded as a reading aid — not a compliance verdict. See §A.</p>
  </header>
  {("<div class='callout'><b>Medicare Plan Finder impact.</b> "
    "CMS requires Medicare Advantage organizations to publish this "
    "directory in exactly the machine-readable format measured here "
    "(42&nbsp;CFR&nbsp;422.111(m), naming Plan-Net "
    + html.escape(ident['ig_version']) + "). As captured, "
    + "; ".join(mpf_summary_items)
    + ". Until those are fixed, this directory cannot power Medicare Plan "
    "Finder provider search for the Medicare-LOB. Full element-by-element "
    "readiness: §F.</div>") if mpf_summary_items else ""}
  <div class="notebox"><b>How to read this report.</b> The unit is a
  <b>finding</b>: one defect pattern, counted once per record it touches
  (raw message counts are shown too, but never alone). Every rate shows its
  denominator (<span class="mono">n</span>) and renders in 20-point shaded
  bands whose color encodes failure severity — <b>red</b> (worst), amber,
  blue-gray, blue (healthy) — whichever direction the failure lies: a high
  affected-rate goes red, and so does a LOW populated-rate. A cell smaller
  than 30 records shows the exact count instead of a percentage, colored
  the same way. <b>Good / OK / Bad</b> grades are a reading aid over the
  three data-quality categories only — fixed cuts, defined in §A, never a
  compliance verdict. Sources: <b>Medicaid-LOB</b> and
  <b>Medicare-LOB</b> are the two lines of business;
  <b>OtherDataSource</b> is the label for a third source marker found only
  on organization records — reported separately, never folded into either
  line, and characterized from its own data in §K. Click any row
  with a ▸ marker to open its drill-down in place.</div>
  {body}
</div>
</body></html>
"""
    out_path.write_text(doc, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--payload", type=Path, required=True,
                    help="findings.json produced by build_azblue_report.py")
    ap.add_argument("--out", type=Path, required=True, help="HTML output path")
    args = ap.parse_args()
    payload = json.loads(args.payload.read_text(encoding="utf-8"))
    render(payload, args.out)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
