"""Collection-status dashboard — payer x resource coverage cross-tab.

Reflects the CURRENT database state: exact row counts per (payer, resource) plus
the measured coverage %% recorded in provenance (rows landed vs the server's total,
where the server exposes one). This is a COLLECTION-PROGRESS view, deliberately
NOT quality scoring — no composite quality numbers are computed here.

Emits three artifacts under output/:
  collection_status.json          the underlying data
  collection_status_widget.html   a CSS-variable fragment (for inline rendering)
  collection_status.html          a self-contained page (opens via file://)

Access tier is derived from each endpoint's auth strategy in the manifest, so it
stays correct as endpoints are added.
"""

from __future__ import annotations

import json
import html
from datetime import datetime, timezone

from .. import OUTPUT_DIR
from ..config import load_manifest
from ..db import safe_ident
from ..logging_setup import get_logger
from ..etl.loader import list_payer_schemas, pg_connection

log = get_logger(__name__)

# (table name, short column label) in Plan-Net display order.
RESOURCES = [
    ("organization", "Org"), ("practitioner", "Prac"), ("practitioner_role", "Role"),
    ("location", "Loc"), ("healthcare_service", "HCS"),
    ("organization_affiliation", "OrgAff"), ("insurance_plan", "Plan"),
    ("endpoint_resource", "Endpt"),
]
# resource_type (FHIR) -> table, to join provenance coverage.
_RT_TO_TABLE = {
    "Organization": "organization", "Practitioner": "practitioner",
    "PractitionerRole": "practitioner_role", "Location": "location",
    "HealthcareService": "healthcare_service",
    "OrganizationAffiliation": "organization_affiliation",
    "InsurancePlan": "insurance_plan", "Endpoint": "endpoint_resource",
}

DISCLAIMER = (
    "Personal, educational data-exploration project. Figures are point-in-time "
    "observations of public FHIR endpoints, may reflect transient conditions or "
    "collection tooling, and are not legal, compliance, or professional advice, "
    "nor an official assessment. Provided as-is, no warranty."
)


def _tier(strategy: str) -> str:
    if strategy == "none":
        return "open"
    if strategy == "healthsparq_public_token":
        return "public-token"
    return "gated"


def gather_status() -> dict:
    """Query the live DB into a serializable status structure."""
    manifest = load_manifest()
    name_by_key = {e.key: e.payer_name for e in manifest.endpoints}
    tier_by_key = {e.key: _tier(e.auth.strategy) for e in manifest.endpoints}

    conn = pg_connection()
    try:
        have = sorted(list_payer_schemas(conn))
        cur = conn.cursor()
        cov = {}
        cur.execute(
            "SELECT DISTINCT ON (payer_id, resource_type) payer_id, resource_type, "
            "notes->>'coverage_pct' FROM public.provenance "
            "ORDER BY payer_id, resource_type, started_at DESC"
        )
        for pid, rt, c in cur.fetchall():
            if rt in _RT_TO_TABLE and c is not None:
                try:
                    cov[(pid, _RT_TO_TABLE[rt])] = min(float(c), 100.0)
                except ValueError:
                    pass
        payers = []
        for s in have:
            union = " UNION ALL ".join(
                f"SELECT '{safe_ident(t)}', count(*) FROM {safe_ident(s)}.\"{safe_ident(t)}\""
                for t, _ in RESOURCES
            )
            cur.execute(union)
            rc = {r[0]: r[1] for r in cur.fetchall()}
            total = sum(rc.values())
            if total == 0:
                continue
            payers.append({
                "key": s, "name": name_by_key.get(s, s), "tier": tier_by_key.get(s, "open"),
                "total": total,
                "res": {lbl: {"n": rc[t], "cov": cov.get((s, t))} for t, lbl in RESOURCES},
            })
    finally:
        conn.close()

    payers.sort(key=lambda p: -p["total"])
    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "grand_total": sum(p["total"] for p in payers),
        "payer_count": len(payers),
        "payers": payers,
    }


def _ab(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1e6:.1f}M"
    if n >= 1000:
        return f"{n / 1000:.0f}k"
    return str(n) if n else ""


def _cell(info: dict) -> str:
    n, c = info["n"], info["cov"]
    base = "padding:3px;text-align:center;border:0.5px solid var(--border);font-size:11px"
    if n == 0:
        return f'<td style="{base};color:var(--text-muted)">·</td>'
    if c is None:
        bg, tx = "var(--surface-2)", "var(--text-secondary)"
    elif c >= 90:
        bg, tx = "var(--bg-success)", "var(--text-success)"
    elif c >= 55:
        bg, tx = "var(--bg-warning)", "var(--text-warning)"
    else:
        bg, tx = "var(--bg-danger)", "var(--text-danger)"
    pct = f'<br><span style="font-size:9px;opacity:.8">{c:.0f}%</span>' if c is not None else ""
    return f'<td style="{base};background:{bg};color:{tx}">{_ab(n)}{pct}</td>'


_TIER_DOT = {"open": "var(--text-accent)", "public-token": "var(--text-success)", "gated": "var(--text-muted)"}


def build_fragment(data: dict) -> str:
    """CSS-variable widget fragment (host provides surface/text/bg vars)."""
    labels = [lbl for _, lbl in RESOURCES]
    head = '<th style="padding:3px 6px;text-align:left;font-size:11px;color:var(--text-secondary)">Source</th>' \
        + "".join(f'<th style="padding:3px;font-size:11px;color:var(--text-secondary)">{l}</th>' for l in labels) \
        + '<th style="padding:3px 6px;text-align:right;font-size:11px;color:var(--text-secondary)">Total</th>'
    rows = ""
    for p in data["payers"]:
        dot = f'<span style="display:inline-block;width:7px;height:7px;border-radius:50%;background:{_TIER_DOT.get(p["tier"], "var(--text-muted)")};margin-right:5px"></span>'
        cells = "".join(_cell(p["res"][l]) for l in labels)
        rows += (
            f'<tr><td style="padding:3px 6px;font-size:11px;white-space:nowrap;overflow:hidden;'
            f'text-overflow:ellipsis;max-width:150px">{dot}{html.escape(p["name"])}</td>{cells}'
            f'<td style="padding:3px 6px;text-align:right;font-size:11px;font-weight:500">{_ab(p["total"])}</td></tr>'
        )
    g = data["grand_total"]
    cards = "".join(
        f'<div style="background:var(--surface-1);border-radius:var(--radius);padding:1rem">'
        f'<div style="font-size:13px;color:var(--text-secondary)">{lbl}</div>'
        f'<div style="font-size:24px;font-weight:500">{val}</div></div>'
        for lbl, val in [
            ("Records collected", f"{g / 1e6:.1f}M"),
            ("Payer directories", str(data["payer_count"])),
            ("All accessed", "no auth"),
        ]
    )
    legend = (
        '<span><span style="color:var(--text-success)">■</span> ≥90%</span>'
        '<span><span style="color:var(--text-warning)">■</span> 55-89%</span>'
        '<span><span style="color:var(--text-danger)">■</span> &lt;55%</span>'
        '<span><span style="color:var(--text-secondary)">■</span> server exposes no count</span>'
        '<span>· = not served</span>'
    )
    return (
        f'<h2 class="sr-only">Provider directory data-collection status: {g:,} records across '
        f'{data["payer_count"]} publicly-accessible payer FHIR directories.</h2>'
        '<div style="padding:1rem 0">'
        '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:1.25rem">'
        f'{cards}</div>'
        f'<div style="display:flex;gap:14px;flex-wrap:wrap;margin-bottom:1rem;font-size:12px;color:var(--text-secondary)">{legend}</div>'
        f'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;table-layout:fixed">'
        f'<thead><tr>{head}</tr></thead><tbody>{rows}</tbody></table></div>'
        f'<p style="font-size:11px;color:var(--text-muted);margin-top:1rem;line-height:1.6">{DISCLAIMER}</p>'
        '</div>'
    )


_STANDALONE_VARS = """:root{--surface-0:#faf9f5;--surface-1:#f1efe8;--surface-2:#fff;--border:#e5e3da;--radius:8px;
--text-primary:#1a1a18;--text-secondary:#5f5e5a;--text-muted:#8a8880;--text-accent:#185fa5;
--bg-success:#e1f5ee;--text-success:#0f6e56;--bg-warning:#faeeda;--text-warning:#854f0b;--bg-danger:#fcebeb;--text-danger:#a32d2d}
@media (prefers-color-scheme:dark){:root{--surface-0:#1a1a18;--surface-1:#26262a;--surface-2:#2c2c2a;--border:#3a3a37;
--text-primary:#e8e8e3;--text-secondary:#a8a8a0;--text-muted:#6f6f68;--text-accent:#85b7eb;
--bg-success:#0f6e56;--text-success:#9fe1cb;--bg-warning:#633806;--text-warning:#fac775;--bg-danger:#791f1f;--text-danger:#f09595}}
body{background:var(--surface-0);color:var(--text-primary);font-family:-apple-system,Segoe UI,Roboto,sans-serif;
max-width:1000px;margin:0 auto;padding:24px}.sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0)}
h1{font-size:20px;font-weight:500}"""


def build_standalone(data: dict) -> str:
    """Self-contained page for file:// viewing (defines the CSS variables)."""
    gen = data["generated_at"][:19].replace("T", " ")
    return (
        f'<!doctype html><html lang="en"><head><meta charset="utf-8">'
        f'<meta name="viewport" content="width=device-width, initial-scale=1">'
        f'<title>Provider directory collection status</title><style>{_STANDALONE_VARS}</style></head><body>'
        f'<h1>Provider directory collection status</h1>'
        f'<p style="font-size:13px;color:var(--text-secondary)">Generated {gen} UTC</p>'
        f'{build_fragment(data)}</body></html>'
    )


def run() -> dict:
    data = gather_status()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "collection_status.json").write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    (OUTPUT_DIR / "collection_status_widget.html").write_text(build_fragment(data), encoding="utf-8")
    (OUTPUT_DIR / "collection_status.html").write_text(build_standalone(data), encoding="utf-8")
    log.info("collection status: %d payers, %s rows -> output/collection_status.html",
             data["payer_count"], f"{data['grand_total']:,}")
    return data
