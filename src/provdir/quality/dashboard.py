"""Phase 6.5 / 7.4 — Static, self-contained dashboard.

Builds a folder of static HTML under output/site/ that opens directly via
file:// with no server. All chart data is baked in at build time; interactivity
(hover, zoom, weight-scheme toggle, navigation) is entirely client-side, served
by plotly.js from CDN. There is no Streamlit/Dash/Flask — by design.

Pages:
  index.html              national overview: composite ranking, dimension heatmap
  payers/<key>.html       per-payer drill-down: dimensions, completeness, integrity
  conformance.html        Phase 3 server-capability conformance across payers
  methodology.html        dimensions, weights, data sources, limitations

UX (national -> payer drill-down) implements the Phase 6.5 user journey.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import plotly.graph_objects as go
import plotly.io as pio

from .. import OUTPUT_DIR
from ..logging_setup import get_logger
from .scoring import DIMENSIONS, WEIGHT_SCHEMES

log = get_logger(__name__)

SITE_DIR = OUTPUT_DIR / "site"
QUALITY_DIR = OUTPUT_DIR / "quality"
CONF_DIR = OUTPUT_DIR / "conformance"
PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"

# --- design system ---------------------------------------------------------
PALETTE = {
    "bg": "#0f172a",
    "panel": "#1e293b",
    "panel2": "#243349",
    "text": "#e2e8f0",
    "muted": "#94a3b8",
    "accent": "#38bdf8",
    "good": "#22c55e",
    "mid": "#eab308",
    "bad": "#ef4444",
    "border": "#334155",
}
# Sequential, color-blind-safe scale for heatmaps (low->high quality).
HEAT_SCALE = [[0.0, "#7f1d1d"], [0.5, "#eab308"], [1.0, "#15803d"]]

DIM_LABELS = {
    "completeness": "Completeness",
    "conformance": "Conformance",
    "referential_integrity": "Ref. Integrity",
    "freshness": "Freshness",
    "uniqueness": "Uniqueness",
    "consistency": "Consistency",
}

BASE_CSS = f"""
:root {{
  --bg:{PALETTE['bg']}; --panel:{PALETTE['panel']}; --panel2:{PALETTE['panel2']};
  --text:{PALETTE['text']}; --muted:{PALETTE['muted']}; --accent:{PALETTE['accent']};
  --good:{PALETTE['good']}; --mid:{PALETTE['mid']}; --bad:{PALETTE['bad']}; --border:{PALETTE['border']};
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; font-family:-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;
  background:var(--bg); color:var(--text); line-height:1.5; }}
a {{ color:var(--accent); text-decoration:none; }}
a:hover {{ text-decoration:underline; }}
header.top {{ background:var(--panel); border-bottom:1px solid var(--border); padding:14px 28px;
  display:flex; align-items:center; gap:24px; position:sticky; top:0; z-index:10; }}
header.top h1 {{ font-size:17px; margin:0; font-weight:600; }}
header.top nav {{ display:flex; gap:18px; font-size:14px; }}
header.top nav a.active {{ color:var(--text); border-bottom:2px solid var(--accent); padding-bottom:2px; }}
.wrap {{ max-width:1200px; margin:0 auto; padding:24px 28px 64px; }}
.muted {{ color:var(--muted); }}
.cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:16px; margin:20px 0; }}
.card {{ background:var(--panel); border:1px solid var(--border); border-radius:10px; padding:16px 18px; }}
.card .v {{ font-size:28px; font-weight:700; }}
.card .l {{ font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.04em; }}
.panel {{ background:var(--panel); border:1px solid var(--border); border-radius:10px; padding:18px 20px; margin:18px 0; }}
.panel h2 {{ font-size:15px; margin:0 0 12px; font-weight:600; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ text-align:left; padding:8px 10px; border-bottom:1px solid var(--border); }}
th {{ color:var(--muted); font-weight:600; font-size:11px; text-transform:uppercase; letter-spacing:.04em; }}
td.num, th.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
.pill {{ display:inline-block; padding:2px 8px; border-radius:999px; font-size:12px; font-weight:600; }}
.controls {{ display:flex; gap:10px; align-items:center; margin-bottom:10px; font-size:13px; }}
select {{ background:var(--panel2); color:var(--text); border:1px solid var(--border); border-radius:6px; padding:5px 8px; }}
.note {{ font-size:12px; color:var(--muted); margin-top:8px; }}
.scorebadge {{ font-size:34px; font-weight:800; }}
"""


def _score_color(v: Optional[float]) -> str:
    if v is None:
        return PALETTE["muted"]
    if v >= 80:
        return PALETTE["good"]
    if v >= 55:
        return PALETTE["mid"]
    return PALETTE["bad"]


def _fmt(v: Optional[float]) -> str:
    return "—" if v is None else f"{v:.0f}"


def _fig_html(fig: go.Figure, height: int = 360) -> str:
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=PALETTE["text"], size=12),
        margin=dict(l=10, r=10, t=30, b=10),
        height=height,
    )
    return pio.to_html(
        fig, include_plotlyjs=False, full_html=False,
        config={"displayModeBar": False, "responsive": True},
    )


def _nav(active: str, depth: int) -> str:
    up = "../" * depth
    items = [("index.html", "Overview"), ("conformance.html", "Server Conformance"),
             ("methodology.html", "Methodology")]
    links = "".join(
        f'<a class="{ "active" if name==active else "" }" href="{up}{name}">{label}</a>'
        for name, label in items
    )
    return f'<nav>{links}</nav>'


def _page(title: str, body: str, active: str, depth: int = 0) -> str:
    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<script src="{PLOTLY_CDN}"></script>
<style>{BASE_CSS}</style>
</head><body>
<header class="top"><h1>CMS-9115-F · Provider Directory Quality</h1>{_nav(active, depth)}</header>
<div class="wrap">{body}</div>
</body></html>"""


# --- figures ---------------------------------------------------------------
def _composite_bar(payers: list[dict]) -> str:
    """Ranked composite scores with a client-side weight-scheme toggle."""
    order = sorted(payers, key=lambda p: (p.get("composite_default") or -1))
    names = [p["payer_name"] for p in order]
    scheme_keys = {"default": "composite_default", "equal": "composite_equal",
                   "conformance_focused": "composite_conformance_focused"}
    fig = go.Figure()
    for i, (scheme, col) in enumerate(scheme_keys.items()):
        vals = [p.get(col) for p in order]
        fig.add_bar(
            x=vals, y=names, orientation="h", name=scheme,
            visible=(i == 0),
            marker_color=[_score_color(v) for v in vals],
            text=[_fmt(v) for v in vals], textposition="outside",
            hovertemplate="%{y}: %{x:.1f}<extra></extra>",
        )
    buttons = []
    for i, scheme in enumerate(scheme_keys):
        vis = [j == i for j in range(len(scheme_keys))]
        buttons.append(dict(label=scheme, method="update",
                            args=[{"visible": vis}, {"title": f"Composite score — {scheme} weights"}]))
    fig.update_layout(
        title="Composite score — default weights",
        xaxis=dict(range=[0, 105], title="0–100"),
        updatemenus=[dict(type="dropdown", buttons=buttons, x=1.0, xanchor="right", y=1.18,
                          bgcolor=PALETTE["panel2"], font=dict(color=PALETTE["text"]))],
        showlegend=False,
    )
    return _fig_html(fig, height=max(260, 40 * len(names) + 120))


def _dimension_heatmap(payers: list[dict]) -> str:
    order = sorted(payers, key=lambda p: (p.get("composite_default") or -1), reverse=True)
    z = [[p.get(d) for d in DIMENSIONS] for p in order]
    text = [[_fmt(p.get(d)) for d in DIMENSIONS] for p in order]
    fig = go.Figure(go.Heatmap(
        z=z, x=[DIM_LABELS[d] for d in DIMENSIONS], y=[p["payer_name"] for p in order],
        zmin=0, zmax=100, colorscale=HEAT_SCALE,
        text=text, texttemplate="%{text}", textfont=dict(size=11),
        hovertemplate="%{y} · %{x}: %{z:.1f}<extra></extra>",
        colorbar=dict(title="0–100", outlinewidth=0),
    ))
    fig.update_layout(title="Quality dimensions by payer")
    return _fig_html(fig, height=max(260, 42 * len(order) + 120))


def _payer_dimension_bar(payer: dict) -> str:
    vals = [payer.get(d) for d in DIMENSIONS]
    fig = go.Figure(go.Bar(
        x=[DIM_LABELS[d] for d in DIMENSIONS], y=vals,
        marker_color=[_score_color(v) for v in vals],
        text=[_fmt(v) for v in vals], textposition="outside",
        hovertemplate="%{x}: %{y:.1f}<extra></extra>",
    ))
    fig.update_layout(title="Quality dimensions", yaxis=dict(range=[0, 105], title="0–100"))
    return _fig_html(fig, height=320)


def _completeness_bar(detail: dict) -> str:
    per = ((detail or {}).get("completeness") or {}).get("per_resource") or {}
    items = [(rt, r) for rt, r in per.items() if r.get("row_count", 0) > 0]
    items.sort(key=lambda kv: kv[1].get("completeness_score") or 0)
    if not items:
        return '<p class="muted">No resource data loaded.</p>'
    fig = go.Figure()
    fig.add_bar(x=[r["completeness_score"] for _, r in items], y=[rt for rt, _ in items],
                orientation="h", name="Completeness", marker_color=PALETTE["accent"],
                hovertemplate="%{y}: %{x:.1f}%<extra></extra>")
    fig.add_bar(x=[r["conformance_score"] for _, r in items], y=[rt for rt, _ in items],
                orientation="h", name="Req-element conformance", marker_color=PALETTE["good"],
                hovertemplate="%{y}: %{x:.1f}%<extra></extra>")
    fig.update_layout(title="Completeness & required-element conformance by resource",
                      barmode="group", xaxis=dict(range=[0, 105]),
                      legend=dict(orientation="h", y=1.12))
    return _fig_html(fig, height=max(260, 44 * len(items) + 120))


def _integrity_bar(detail: dict) -> str:
    checks = ((detail or {}).get("integrity") or {}).get("checks") or []
    checks = [c for c in checks if c.get("total")]
    if not checks:
        return '<p class="muted">No references with data to evaluate.</p>'
    checks.sort(key=lambda c: c.get("resolved_pct") or 0)
    fig = go.Figure(go.Bar(
        x=[c["resolved_pct"] for c in checks], y=[c["reference"] for c in checks],
        orientation="h", marker_color=[_score_color(c["resolved_pct"]) for c in checks],
        text=[f'{c["resolved"]}/{c["total"]}' for c in checks], textposition="outside",
        hovertemplate="%{y}: %{x:.1f}% resolved<extra></extra>",
    ))
    fig.update_layout(title="Referential integrity — % of references resolved", xaxis=dict(range=[0, 110]))
    return _fig_html(fig, height=max(260, 40 * len(checks) + 120))


# --- page builders ---------------------------------------------------------
def _overview_table(payers: list[dict]) -> str:
    head = "".join(f'<th class="num">{DIM_LABELS[d]}</th>' for d in DIMENSIONS)
    rows = []
    for p in sorted(payers, key=lambda x: (x.get("composite_default") or -1), reverse=True):
        cells = "".join(
            f'<td class="num" style="color:{_score_color(p.get(d))}">{_fmt(p.get(d))}</td>'
            for d in DIMENSIONS
        )
        comp = p.get("composite_default")
        rows.append(
            f'<tr><td><a href="payers/{p["key"]}.html">{p["payer_name"]}</a></td>'
            f'<td class="num">{p.get("total_rows", 0):,}</td>{cells}'
            f'<td class="num"><span class="pill" style="background:{_score_color(comp)};color:#0b1220">{_fmt(comp)}</span></td></tr>'
        )
    return f"""<table><thead><tr><th>Payer</th><th class="num">Rows</th>{head}<th class="num">Composite</th></tr></thead>
<tbody>{''.join(rows)}</tbody></table>"""


def build_dashboard(settings=None) -> Path:
    scores_path = QUALITY_DIR / "scores.json"
    if not scores_path.exists():
        raise FileNotFoundError("Run `provdir score` first to produce output/quality/scores.json")
    scores = json.loads(scores_path.read_text(encoding="utf-8"))
    payers = scores["payers"]
    gen = scores.get("generated_at", datetime.now(tz=timezone.utc).isoformat())

    SITE_DIR.mkdir(parents=True, exist_ok=True)
    (SITE_DIR / "payers").mkdir(exist_ok=True)

    total_rows = sum(p.get("total_rows", 0) for p in payers)
    comps = [p.get("composite_default") for p in payers if p.get("composite_default") is not None]
    avg_comp = round(sum(comps) / len(comps), 1) if comps else None

    # --- index ---
    cards = f"""
    <div class="cards">
      <div class="card"><div class="v">{len(payers)}</div><div class="l">Payers evaluated</div></div>
      <div class="card"><div class="v">{total_rows:,}</div><div class="l">Resources ingested</div></div>
      <div class="card"><div class="v" style="color:{_score_color(avg_comp)}">{_fmt(avg_comp)}</div><div class="l">Avg composite</div></div>
      <div class="card"><div class="v">{scores.get('dimensions') and len(DIMENSIONS)}</div><div class="l">Quality dimensions</div></div>
    </div>"""
    index_body = f"""
    <p class="muted">Data-quality evaluation of US payer Da Vinci PDex Plan-Net provider directories
    (CMS-9115-F). Generated {gen[:19].replace('T',' ')} UTC.</p>
    {cards}
    <div class="panel"><h2>Composite ranking</h2>
      <p class="note">Use the dropdown to switch between pre-generated weighting schemes (weights are fixed at build time).</p>
      {_composite_bar(payers)}</div>
    <div class="panel"><h2>Dimension heatmap</h2>{_dimension_heatmap(payers)}</div>
    <div class="panel"><h2>Scorecard</h2>{_overview_table(payers)}
      <p class="note">Click a payer to drill down. Scores are 0–100; green ≥80, amber ≥55, red below.</p></div>
    """
    (SITE_DIR / "index.html").write_text(_page("Provider Directory Quality — Overview", index_body, "index.html"), encoding="utf-8")

    # --- per-payer pages ---
    for p in payers:
        detail = _load_payer_detail(p["key"])
        conf = _load_conformance(p["key"])
        comp = p.get("composite_default")
        conf_panel = _conformance_panel(conf)
        orphan = (detail or {}).get("orphans") or {}
        orphan_rows = "".join(f"<tr><td>{k}</td><td class='num'>{v:,}</td></tr>" for k, v in orphan.items())
        body = f"""
        <p><a href="../index.html">← Overview</a></p>
        <h2 style="margin:6px 0 0">{p['payer_name']}</h2>
        <p class="muted">{p.get('total_rows',0):,} resources ingested · composite
          <span class="scorebadge" style="color:{_score_color(comp)}">{_fmt(comp)}</span></p>
        <div class="panel"><h2>Dimension scores</h2>{_payer_dimension_bar(p)}</div>
        <div class="panel"><h2>Completeness by resource</h2>{_completeness_bar(detail)}</div>
        <div class="panel"><h2>Referential integrity</h2>{_integrity_bar(detail)}
          <p class="note">Resolved = the referenced resource exists in the loaded data for this payer.</p></div>
        <div class="panel"><h2>Server capability conformance (Phase 3)</h2>{conf_panel}</div>
        <div class="panel"><h2>Orphans</h2><table><thead><tr><th>Check</th><th class="num">Count</th></tr></thead>
          <tbody>{orphan_rows or '<tr><td class="muted" colspan=2>n/a</td></tr>'}</tbody></table></div>
        """
        (SITE_DIR / "payers" / f"{p['key']}.html").write_text(
            _page(f"{p['payer_name']} — Quality", body, "index.html", depth=1), encoding="utf-8")

    # --- conformance page ---
    (SITE_DIR / "conformance.html").write_text(
        _page("Server Conformance", _conformance_page(), "conformance.html"), encoding="utf-8")

    # --- methodology page ---
    (SITE_DIR / "methodology.html").write_text(
        _page("Methodology", _methodology_body(), "methodology.html"), encoding="utf-8")

    log.info("dashboard built: %d payer pages -> %s", len(payers), SITE_DIR / "index.html")
    return SITE_DIR / "index.html"


def _load_payer_detail(key: str) -> Optional[dict]:
    f = QUALITY_DIR / f"{key}_conformance.json"
    if f.exists():
        rep = json.loads(f.read_text(encoding="utf-8"))
        return {
            "completeness": rep.get("completeness"),
            "integrity": rep.get("integrity"),
            "orphans": rep.get("orphans"),
        }
    return None


def _load_conformance(key: str) -> Optional[dict]:
    f = CONF_DIR / f"{key}.json"
    return json.loads(f.read_text(encoding="utf-8")) if f.exists() else None


def _conformance_panel(conf: Optional[dict]) -> str:
    if not conf:
        return '<p class="muted">No Phase 3 conformance report for this payer.</p>'
    s = conf.get("summary") or {}
    server = conf.get("server") or {}
    if not conf.get("metadata_available", True):
        meta = '<span class="pill" style="background:var(--mid);color:#0b1220">no CapabilityStatement (probe-only)</span>'
    else:
        meta = f'{server.get("software_name") or "?"} {server.get("software_version") or ""} · FHIR {server.get("fhir_version") or "?"}'
    return f"""<p class="muted">{meta}</p>
    <div class="cards">
      <div class="card"><div class="v">{_fmt(s.get('resource_declared_pct'))}</div><div class="l">Resources declared</div></div>
      <div class="card"><div class="v">{_fmt(s.get('shall_param_declared_pct'))}</div><div class="l">SHALL params declared</div></div>
      <div class="card"><div class="v">{_fmt(s.get('probe_pass_pct'))}</div><div class="l">Live probes passed</div></div>
    </div>"""


def _conformance_page() -> str:
    f = CONF_DIR / "summary.json"
    if not f.exists():
        return '<p class="muted">No conformance summary. Run <code>provdir conformance</code>.</p>'
    summary = json.loads(f.read_text(encoding="utf-8"))
    rows = []
    for e in summary.get("endpoints", []):
        rows.append(
            f'<tr><td>{e.get("payer_name")}</td>'
            f'<td class="num">{_fmt(e.get("resource_declared_pct"))}</td>'
            f'<td class="num">{_fmt(e.get("shall_param_declared_pct"))}</td>'
            f'<td class="num">{_fmt(e.get("required_interaction_pct"))}</td>'
            f'<td class="num">{_fmt(e.get("probe_pass_pct"))}</td>'
            f'<td class="num">{e.get("probes_passed",0)}/{e.get("probes_run",0)}</td></tr>'
        )
    return f"""<p class="muted">Plan-Net IG {summary.get('ig_version')} — server CapabilityStatement conformance &
    live capability probes (Phase 3).</p>
    <div class="panel"><table><thead><tr><th>Payer</th><th class="num">Resources declared %</th>
    <th class="num">SHALL params %</th><th class="num">Req interactions %</th>
    <th class="num">Probe pass %</th><th class="num">Probes</th></tr></thead>
    <tbody>{''.join(rows)}</tbody></table></div>"""


def _methodology_body() -> str:
    schemes = "".join(
        f"<tr><td>{s}</td>" + "".join(f'<td class="num">{w.get(d,0):.2f}</td>' for d in DIMENSIONS) + "</tr>"
        for s, w in WEIGHT_SCHEMES.items()
    )
    dim_head = "".join(f'<th class="num">{DIM_LABELS[d]}</th>' for d in DIMENSIONS)
    return f"""
    <div class="panel"><h2>Quality dimensions</h2>
    <ul>
      <li><b>Completeness</b> — mean population of required + must-support elements across loaded resources.</li>
      <li><b>Conformance</b> — population of IG <i>required</i> (SHALL / min=1) elements; &lt;100% is a violation.</li>
      <li><b>Referential integrity</b> — % of references (e.g. PractitionerRole.practitioner) whose target exists in the payer's loaded data.</li>
      <li><b>Freshness</b> — % of resources whose <code>meta.lastUpdated</code> is within 12 months.</li>
      <li><b>Uniqueness</b> — % of practitioner NPIs that are unique (duplicate detection).</li>
      <li><b>Consistency</b> — valid US state codes, ZIP format, and city/state completeness on Locations.</li>
    </ul></div>
    <div class="panel"><h2>Weight schemes (build-time)</h2>
    <table><thead><tr><th>Scheme</th>{dim_head}</tr></thead><tbody>{schemes}</tbody></table>
    <p class="note">Weights are fixed at build time; the overview chart pre-generates one view per scheme.
    Dimensions with no data for a payer are excluded and remaining weights renormalized.</p></div>
    <div class="panel"><h2>Data sources & limitations</h2>
    <ul>
      <li>Directories pulled live from payer Plan-Net FHIR endpoints; loaded to localhost Postgres
          (JSONB-hybrid, one schema per datasource).</li>
      <li>Servers that reject unfiltered searches are swept by state/name partitions; some reference resources
          (PractitionerRole/OrganizationAffiliation) on such servers can't be partitioned and are reported as not-ingested.</li>
      <li><b>Validated server-side defect:</b> Premera advertises a <code>next</code> pagination link that
          dead-ends on an empty page for non-Practitioner resources (page 1 = 50 of 94,993; page 2 = 0), so the
          full directory cannot be retrieved via standard FHIR paging. Reproduced 3/3.</li>
      <li><b>Completeness shown here is bounded by our own ingest caps</b> (dev <code>--max-pages</code> limits), not
          only by server behaviour. Several earlier "server cap" claims (e.g. Humana 8k) were our caps and were
          withdrawn on validation. Treat low completeness/integrity as <i>"not yet fully pulled"</i> unless a server
          limit is independently confirmed.</li>
      <li>Network adequacy (Phase 8) is out of scope here pending CMS reference data.</li>
    </ul></div>
    """
