#!/usr/bin/env python
"""Render output/quality/map_data.json into a self-contained HTML artifact.

The page is a dashboard, not a document: a US tile-grid choropleth over payer
directory geography, a metric selector, and per-state / per-payer detail. All
data is inlined, so the file opens with no network access.

Geography is a tile grid (one square per state) rather than real boundaries.
The artifact CSP blocks external TopoJSON, and hand-authored boundary paths
would mean inventing coordinates. A grid also makes RI and DC comparable to TX,
which a true-area choropleth actively works against.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# (col, row) on a 12x8 grid. Rows 0-7 top to bottom, cols 0-11 left to right.
TILE = {
    "AK": (0, 0), "ME": (11, 0),
    "VT": (10, 1), "NH": (11, 1),
    "WA": (1, 2), "ID": (2, 2), "MT": (3, 2), "ND": (4, 2), "MN": (5, 2),
    "IL": (6, 2), "WI": (7, 2), "MI": (8, 2), "NY": (9, 2), "MA": (10, 2),
    "RI": (11, 2),
    "OR": (1, 3), "NV": (2, 3), "WY": (3, 3), "SD": (4, 3), "IA": (5, 3),
    "IN": (6, 3), "OH": (7, 3), "PA": (8, 3), "NJ": (9, 3), "CT": (10, 3),
    "CA": (1, 4), "UT": (2, 4), "CO": (3, 4), "NE": (4, 4), "MO": (5, 4),
    "KY": (6, 4), "WV": (7, 4), "VA": (8, 4), "MD": (9, 4), "DE": (10, 4),
    "AZ": (2, 5), "NM": (3, 5), "KS": (4, 5), "AR": (5, 5), "TN": (6, 5),
    "NC": (7, 5), "SC": (8, 5), "DC": (9, 5),
    "OK": (4, 6), "LA": (5, 6), "MS": (6, 6), "AL": (7, 6), "GA": (8, 6),
    "HI": (0, 7), "TX": (4, 7), "FL": (9, 7),
    "AS": (0, 4), "GU": (0, 5), "MP": (0, 6),
    "VI": (11, 6), "PR": (11, 7),
}

CSS = """
:root{
  --ground:#FAFBFC; --surface:#FFFFFF; --raised:#F1F4F7;
  --ink:#0F1419; --ink-2:#3D4A56; --muted:#66788A;
  --line:#DCE3EA; --line-2:#C3CDD7;
  --accent:#0E7C86; --accent-soft:#E2F1F2; --accent-ink:#0A5A62;
  --good:#2F7D5B; --warn:#A9701C; --bad:#A93F3F;
  --ramp-0:#EDF1F4; --ramp-1:#C9E0E2; --ramp-2:#93C7CC; --ramp-3:#4FA5AE;
  --ramp-4:#1F838F; --ramp-5:#0B5F69;
  --shadow:0 1px 2px rgba(15,20,25,.06),0 4px 14px rgba(15,20,25,.05);
}
@media (prefers-color-scheme:dark){
  :root:not([data-theme="light"]){
    --ground:#0B1015; --surface:#131A21; --raised:#1B242D;
    --ink:#E6EDF3; --ink-2:#B4C2CE; --muted:#8497A6;
    --line:#243039; --line-2:#33434F;
    --accent:#3FB2BC; --accent-soft:#123037; --accent-ink:#7FD3DA;
    --good:#5FBE8E; --warn:#D9A248; --bad:#E0736F;
    --ramp-0:#1A232B; --ramp-1:#1E3E45; --ramp-2:#255C66; --ramp-3:#2E7C88;
    --ramp-4:#3FA0AC; --ramp-5:#63C4CE;
    --shadow:0 1px 2px rgba(0,0,0,.4),0 4px 14px rgba(0,0,0,.3);
  }
}
:root[data-theme="dark"]{
  --ground:#0B1015; --surface:#131A21; --raised:#1B242D;
  --ink:#E6EDF3; --ink-2:#B4C2CE; --muted:#8497A6;
  --line:#243039; --line-2:#33434F;
  --accent:#3FB2BC; --accent-soft:#123037; --accent-ink:#7FD3DA;
  --good:#5FBE8E; --warn:#D9A248; --bad:#E0736F;
  --ramp-0:#1A232B; --ramp-1:#1E3E45; --ramp-2:#255C66; --ramp-3:#2E7C88;
  --ramp-4:#3FA0AC; --ramp-5:#63C4CE;
  --shadow:0 1px 2px rgba(0,0,0,.4),0 4px 14px rgba(0,0,0,.3);
}

*{box-sizing:border-box}
body{
  margin:0; background:var(--ground); color:var(--ink);
  font-family:system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  font-size:15px; line-height:1.5; -webkit-font-smoothing:antialiased;
}
.mono,.num{font-family:ui-monospace,SFMono-Regular,"Cascadia Mono",Menlo,Consolas,monospace;
  font-variant-numeric:tabular-nums}
.wrap{max-width:1400px;margin:0 auto;padding:28px 24px 72px;display:flex;flex-direction:column;gap:22px}

header.top{display:flex;flex-direction:column;gap:10px;padding-bottom:18px;border-bottom:1px solid var(--line)}
h1{margin:0;font-size:26px;line-height:1.2;letter-spacing:-.02em;text-wrap:balance;font-weight:640}
.sub{margin:0;color:var(--muted);max-width:70ch}
.prov{display:flex;flex-wrap:wrap;gap:8px;margin-top:4px}
.chip{font-size:11.5px;letter-spacing:.04em;text-transform:uppercase;color:var(--ink-2);
  background:var(--raised);border:1px solid var(--line);border-radius:999px;padding:3px 10px}
.chip.flag{color:var(--warn);border-color:color-mix(in srgb,var(--warn) 40%,var(--line))}

.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
.stat{background:var(--surface);border:1px solid var(--line);border-radius:10px;padding:12px 14px;box-shadow:var(--shadow)}
.stat .k{font-size:11.5px;letter-spacing:.05em;text-transform:uppercase;color:var(--muted)}
.stat .v{font-size:22px;font-weight:640;letter-spacing:-.01em;margin-top:3px}

.controls{position:sticky;top:0;z-index:20;background:var(--ground);
  border-bottom:1px solid var(--line);padding:12px 0;display:flex;flex-wrap:wrap;gap:16px;align-items:flex-end}
.field{display:flex;flex-direction:column;gap:5px}
.field label{font-size:11.5px;letter-spacing:.05em;text-transform:uppercase;color:var(--muted)}
select,input[type=search]{font:inherit;font-size:14px;color:var(--ink);background:var(--surface);
  border:1px solid var(--line-2);border-radius:8px;padding:7px 10px;min-width:210px}
select:focus-visible,input:focus-visible,button:focus-visible,[tabindex]:focus-visible{
  outline:2px solid var(--accent);outline-offset:2px}

.grid{display:grid;grid-template-columns:minmax(0,1.35fr) minmax(300px,.9fr);gap:20px;align-items:start}
@media (max-width:1000px){.grid{grid-template-columns:1fr}}
.card{background:var(--surface);border:1px solid var(--line);border-radius:12px;box-shadow:var(--shadow)}
.card>h2{margin:0;padding:14px 16px;font-size:13px;letter-spacing:.05em;text-transform:uppercase;
  color:var(--ink-2);border-bottom:1px solid var(--line);font-weight:620}
.card>.body{padding:16px}

svg.map{width:100%;height:auto;display:block}
.tile{cursor:pointer;transition:opacity .12s}
.tile rect{stroke:var(--surface);stroke-width:1.5;rx:3}
.tile text{font-size:8.4px;font-weight:650;pointer-events:none;
  font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
.tile.dim{opacity:.28}
.tile.sel rect{stroke:var(--ink);stroke-width:2.4}
.tile:hover rect{stroke:var(--accent);stroke-width:2.4}

.legend{display:flex;align-items:center;gap:10px;margin-top:14px;flex-wrap:wrap}
.scale{display:flex;border-radius:4px;overflow:hidden;border:1px solid var(--line)}
.scale i{width:34px;height:12px;display:block}
.legend .lbl{font-size:12px;color:var(--muted)}

.detail .row{display:flex;justify-content:space-between;gap:14px;padding:7px 0;border-bottom:1px solid var(--line)}
.detail .row:last-child{border-bottom:0}
.detail .row .k{color:var(--muted);font-size:13.5px}
.empty{color:var(--muted);font-size:14px;padding:6px 0}
.plist{display:flex;flex-direction:column;gap:2px;max-height:420px;overflow-y:auto;margin-top:4px}
.pitem{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:7px 9px;
  border-radius:7px;cursor:pointer;border:1px solid transparent}
.pitem:hover{background:var(--raised);border-color:var(--line)}
.pitem .nm{font-size:13.5px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}

.bar{height:6px;border-radius:3px;background:var(--raised);overflow:hidden;min-width:60px;flex:0 0 60px}
.bar i{display:block;height:100%}

table{width:100%;border-collapse:collapse;font-size:13.5px}
.tablewrap{overflow-x:auto}
th{position:sticky;top:0;background:var(--surface);text-align:right;padding:9px 10px;
  font-size:11.5px;letter-spacing:.04em;text-transform:uppercase;color:var(--muted);
  border-bottom:1px solid var(--line-2);cursor:pointer;white-space:nowrap}
th:first-child,td:first-child{text-align:left;position:sticky;left:0;background:var(--surface)}
th.on{color:var(--accent)}
td{padding:8px 10px;text-align:right;border-bottom:1px solid var(--line);white-space:nowrap}
tbody tr{cursor:pointer}
tbody tr:hover td{background:var(--raised)}
tbody tr:hover td:first-child{background:var(--raised)}
.tag{font-size:10.5px;letter-spacing:.04em;text-transform:uppercase;padding:1.5px 6px;
  border-radius:4px;background:var(--accent-soft);color:var(--accent-ink);margin-left:7px}
.nd{color:var(--muted)}
footer{color:var(--muted);font-size:13px;border-top:1px solid var(--line);padding-top:16px;max-width:80ch}
footer p{margin:0 0 8px}
"""


def build(data: dict) -> str:
    payload = json.dumps(data, separators=(",", ":"))
    tiles = json.dumps(TILE, separators=(",", ":"))
    runs = data.get("runs", {})
    note = runs.get("conformance_note", "")
    partial = "of" in note  # "N of M instances (P%)" means the run was short
    gen = data.get("generated_at", "")[:19].replace("T", " ")

    flag = (f'<span class="chip flag">conformance sample {note}</span>'
            if partial else f'<span class="chip">conformance {note}</span>')

    return f"""<title>Provider directory conformance map</title>
<style>{CSS}</style>
<div class="wrap">
<header class="top">
  <h1>CMS-9115-F provider directory: conformance and coverage</h1>
  <p class="sub">Da Vinci PDex Plan-Net directories from 64 US payers, extracted to Postgres and
  validated against Plan-Net 1.1.0. Geography comes from <span class="mono">Location.address_state</span>;
  quality comes from the HL7 validator and from exhaustive binding and element-coverage sweeps.</p>
  <div class="prov">
    <span class="chip">generated {gen} UTC</span>
    {flag}
    <span class="chip">binding run {runs.get('binding')}</span>
    <span class="chip">coverage run {runs.get('coverage')}</span>
  </div>
</header>

<section class="stats" id="stats"></section>

<div class="controls">
  <div class="field">
    <label for="metric">Metric</label>
    <select id="metric">
      <option value="footprint">Directory footprint (locations)</option>
      <option value="payers">Payers publishing in state</option>
      <option value="conformance">Structural conformance</option>
      <option value="binding">Terminology binding conformance</option>
      <option value="uscdi">USCDI v3 element coverage</option>
      <option value="mpf">MA Plan Finder element coverage</option>
      <option value="plannet_ext">Plan-Net directory extensions</option>
    </select>
  </div>
  <div class="field">
    <label for="scope">Payers</label>
    <select id="scope">
      <option value="all">All payers</option>
      <option value="ma">Medicare Advantage only</option>
    </select>
  </div>
  <div class="field">
    <label for="focus">Focus a single payer</label>
    <select id="focus"><option value="">None</option></select>
  </div>
  <div class="field">
    <label for="q">Filter table</label>
    <input type="search" id="q" placeholder="payer name" />
  </div>
</div>

<div class="grid">
  <section class="card">
    <h2 id="maptitle">Directory footprint</h2>
    <div class="body">
      <svg class="map" id="map" viewBox="0 0 468 320" role="img" aria-label="US tile grid choropleth"></svg>
      <div class="legend">
        <span class="lbl" id="lo">low</span>
        <span class="scale"><i style="background:var(--ramp-0)"></i><i style="background:var(--ramp-1)"></i><i style="background:var(--ramp-2)"></i><i style="background:var(--ramp-3)"></i><i style="background:var(--ramp-4)"></i><i style="background:var(--ramp-5)"></i></span>
        <span class="lbl" id="hi">high</span>
        <span class="lbl" id="hint">Select a state for detail.</span>
      </div>
    </div>
  </section>

  <section class="card detail">
    <h2 id="dtitle">Detail</h2>
    <div class="body" id="detail"><p class="empty">Select a state on the map, or a payer in the table.</p></div>
  </section>
</div>

<section class="card">
  <h2>Payers</h2>
  <div class="tablewrap"><table id="tbl">
    <thead><tr>
      <th data-k="name">Payer</th><th data-k="states">States</th><th data-k="locations">Locations</th>
      <th data-k="practitioners">Practitioners</th><th data-k="roles">Roles</th>
      <th data-k="conformance">Structural</th><th data-k="binding">Binding</th>
      <th data-k="uscdi">USCDI v3</th><th data-k="mpf">MPF</th><th data-k="plannet_ext">Plan-Net ext</th>
    </tr></thead><tbody></tbody>
  </table></div>
</section>

<footer>
  <p><b>Reading the rates.</b> Structural conformance is the share of validated sample instances with
  zero errors against Plan-Net 1.1.0. Binding conformance is row-weighted over the full population,
  not a sample, and counts rows whose codes are members of the bound value set. Coverage rates
  average only measurable required elements; elements our mapping cannot measure are reported
  separately and never counted as zero.</p>
  <p><b>Caveats that change interpretation.</b> USCDI v3 has no official element-level mapping to
  Plan-Net, so that column is our inference and is labelled as such. MPF elements come from the CMS
  Technical Implementation Guide dated 18 Feb 2026, read via a DocumentCloud mirror because cms.gov
  returns 403. Aetna's PractitionerRole table stores stub JSON, so its JSONB-only metrics are
  sample-based. The map is a tile grid, not a projection: one square per state, so area carries no
  meaning.</p>
</footer>
</div>

<script>
const DATA = {payload};
const TILE = {tiles};
const P = DATA.payers;
const $ = s => document.querySelector(s);
const fmt = n => n == null ? '—' : n.toLocaleString('en-US');
const pct = v => v == null ? null : v * 100;
const pf = v => v == null ? '—' : (v * 100).toFixed(1) + '%';

function confRate(p) {{
  const c = p.conformance; if (!c || !c.validated) return null;
  return c.clean / c.validated;
}}
function bindRate(p) {{
  const b = p.bindings; if (!b) return null;
  let rows = 0, bad = 0;
  for (const k in b) {{ if (k === 'required') {{ rows += b[k].rows_with_codes; bad += b[k].rows_with_nonmember; }} }}
  if (!rows) return null;
  return 1 - bad / rows;
}}
function covRate(p, a) {{
  const c = p.coverage && p.coverage[a];
  return c && c.mean_rate != null ? c.mean_rate : null;
}}
function locOf(p) {{ return (p.resources && p.resources.Location) || 0; }}
function statesOf(p) {{ return Object.keys(p.states || {{}}).filter(s => s !== '__other__'); }}

const METRIC = {{
  footprint:   {{ label:'Directory footprint',              kind:'count', val:null }},
  payers:      {{ label:'Payers publishing in state',       kind:'count', val:null }},
  conformance: {{ label:'Structural conformance',           kind:'rate',  val:confRate }},
  binding:     {{ label:'Terminology binding conformance',  kind:'rate',  val:bindRate }},
  uscdi:       {{ label:'USCDI v3 element coverage',        kind:'rate',  val:p=>covRate(p,'uscdi') }},
  mpf:         {{ label:'MA Plan Finder element coverage',  kind:'rate',  val:p=>covRate(p,'mpf') }},
  plannet_ext: {{ label:'Plan-Net directory extensions',    kind:'rate',  val:p=>covRate(p,'plannet_ext') }},
}};

let metric = 'footprint', scope = 'all', focus = '', selState = '', sortK = 'locations', sortDir = -1;

function activePayers() {{
  let list = P;
  if (focus) list = list.filter(p => p.id === focus);
  else if (scope === 'ma') list = list.filter(p => p.is_ma);
  return list;
}}

// Per-state value for the chosen metric, over the active payer set.
function stateValues() {{
  const list = activePayers(), out = {{}};
  if (metric === 'footprint') {{
    for (const p of list) for (const [s, n] of Object.entries(p.states || {{}}))
      if (s !== '__other__') out[s] = (out[s] || 0) + n;
  }} else if (metric === 'payers') {{
    for (const p of list) for (const s of statesOf(p)) out[s] = (out[s] || 0) + 1;
  }} else {{
    // Weight each payer's rate by how much of its directory sits in that state,
    // so a payer with 40k locations in WA moves WA more than one with 12.
    const f = METRIC[metric].val, acc = {{}};
    for (const p of list) {{
      const r = f(p); if (r == null) continue;
      for (const [s, n] of Object.entries(p.states || {{}})) {{
        if (s === '__other__') continue;
        (acc[s] = acc[s] || [0, 0]);
        acc[s][0] += r * n; acc[s][1] += n;
      }}
    }}
    for (const s in acc) if (acc[s][1]) out[s] = acc[s][0] / acc[s][1];
  }}
  return out;
}}

function rampIndex(v, lo, hi, kind) {{
  if (v == null) return -1;
  if (kind === 'rate') {{
    // Fixed bands: a rate means the same thing on every metric and payer set.
    if (v >= .99) return 5; if (v >= .95) return 4; if (v >= .85) return 3;
    if (v >= .60) return 2; if (v >= .25) return 1; return 0;
  }}
  if (hi <= lo) return 3;
  const t = (Math.log10(v + 1) - Math.log10(lo + 1)) / (Math.log10(hi + 1) - Math.log10(lo + 1));
  return Math.max(0, Math.min(5, Math.floor(t * 5.999)));
}}

function drawMap() {{
  const vals = stateValues(), kind = METRIC[metric].kind;
  const nums = Object.values(vals).filter(v => v != null);
  const lo = nums.length ? Math.min(...nums) : 0, hi = nums.length ? Math.max(...nums) : 1;
  const W = 36, G = 3;
  let svg = '';
  for (const [st, [c, r]] of Object.entries(TILE)) {{
    const v = vals[st], i = rampIndex(v, lo, hi, kind);
    const fill = i < 0 ? 'var(--ramp-0)' : `var(--ramp-${{i}})`;
    const ink = i >= 3 ? '#fff' : 'var(--ink-2)';
    const cls = 'tile' + (v == null ? ' dim' : '') + (selState === st ? ' sel' : '');
    const x = c * (W + G), y = r * (W + G);
    const title = v == null ? `${{st}}: no data`
      : `${{st}}: ${{kind === 'rate' ? (v * 100).toFixed(1) + '%' : fmt(Math.round(v))}}`;
    svg += `<g class="${{cls}}" data-st="${{st}}" tabindex="0" role="button" aria-label="${{title}}">`
        +  `<title>${{title}}</title>`
        +  `<rect x="${{x}}" y="${{y}}" width="${{W}}" height="${{W}}" fill="${{fill}}"></rect>`
        +  `<text x="${{x + W / 2}}" y="${{y + W / 2 + 3}}" text-anchor="middle" fill="${{ink}}">${{st}}</text></g>`;
  }}
  $('#map').innerHTML = svg;
  $('#maptitle').textContent = METRIC[metric].label;
  $('#lo').textContent = kind === 'rate' ? '0%' : fmt(Math.round(lo));
  $('#hi').textContent = kind === 'rate' ? '100%' : fmt(Math.round(hi));
  for (const g of document.querySelectorAll('.tile')) {{
    g.addEventListener('click', () => selectState(g.dataset.st));
    g.addEventListener('keydown', e => {{ if (e.key === 'Enter' || e.key === ' ') {{ e.preventDefault(); selectState(g.dataset.st); }} }});
  }}
}}

function selectState(st) {{ selState = (selState === st ? '' : st); drawMap(); drawDetail(); }}

function rateBar(v) {{
  if (v == null) return '<span class="nd">—</span>';
  const col = v >= .95 ? 'var(--good)' : v >= .6 ? 'var(--warn)' : 'var(--bad)';
  return `<span class="bar"><i style="width:${{(v * 100).toFixed(1)}}%;background:${{col}}"></i></span>`
       + `<span class="num" style="margin-left:8px">${{(v * 100).toFixed(1)}}%</span>`;
}}

function drawDetail() {{
  const box = $('#detail');
  if (!selState) {{
    $('#dtitle').textContent = 'Detail';
    box.innerHTML = '<p class="empty">Select a state on the map, or a payer in the table.</p>';
    return;
  }}
  const list = activePayers().filter(p => (p.states || {{}})[selState])
    .sort((a, b) => b.states[selState] - a.states[selState]);
  const total = list.reduce((s, p) => s + p.states[selState], 0);
  $('#dtitle').textContent = selState;
  if (!list.length) {{ box.innerHTML = '<p class="empty">No payer in the current selection publishes locations here.</p>'; return; }}
  const f = METRIC[metric].kind === 'rate' ? METRIC[metric].val : null;
  box.innerHTML =
    `<div class="row"><span class="k">Payers publishing here</span><b class="num">${{list.length}}</b></div>`
  + `<div class="row"><span class="k">Locations</span><b class="num">${{fmt(total)}}</b></div>`
  + `<div class="plist">` + list.map(p => {{
      const r = f ? f(p) : null;
      const right = f ? (r == null ? '<span class="nd">no data</span>' : `<span class="num">${{(r * 100).toFixed(1)}}%</span>`)
                      : `<span class="num">${{fmt(p.states[selState])}}</span>`;
      return `<div class="pitem" data-p="${{p.id}}"><span class="nm">${{p.name}}`
           + (p.is_ma ? '<span class="tag">MA</span>' : '') + `</span>${{right}}</div>`;
    }}).join('') + `</div>`;
  for (const el of box.querySelectorAll('.pitem'))
    el.addEventListener('click', () => showPayer(el.dataset.p));
}}

function showPayer(id) {{
  const p = P.find(x => x.id === id); if (!p) return;
  $('#dtitle').textContent = p.name;
  const res = p.resources || {{}}, sts = statesOf(p);
  const top = Object.entries(p.states || {{}}).filter(([s]) => s !== '__other__')
    .sort((a, b) => b[1] - a[1]).slice(0, 8);
  const geo = p.geocoded || {{}};
  const rows = [
    ['Medicare Advantage', p.is_ma ? 'yes' : 'no'],
    ['States with locations', sts.length],
    ['Locations', fmt(res.Location)],
    ['Practitioners', fmt(res.Practitioner)],
    ['PractitionerRoles', fmt(res.PractitionerRole)],
    ['Organizations', fmt(res.Organization)],
    ['HealthcareServices', fmt(res.HealthcareService)],
    ['InsurancePlans', fmt(res.InsurancePlan)],
    ['Locations with lat/long', geo.locations ? `${{fmt(geo.with_latlon)}} (${{(100 * geo.with_latlon / geo.locations).toFixed(0)}}%)` : '—'],
  ].map(([k, v]) => `<div class="row"><span class="k">${{k}}</span><b class="num">${{v}}</b></div>`).join('');
  const rates = [
    ['Structural conformance', confRate(p)], ['Binding conformance', bindRate(p)],
    ['USCDI v3 coverage', covRate(p, 'uscdi')], ['MPF coverage', covRate(p, 'mpf')],
    ['Plan-Net extensions', covRate(p, 'plannet_ext')],
  ].map(([k, v]) => `<div class="row"><span class="k">${{k}}</span><span style="display:flex;align-items:center">${{rateBar(v)}}</span></div>`).join('');
  const c = p.conformance;
  const byres = c && c.by_resource ? Object.entries(c.by_resource).sort()
    .map(([r, o]) => `<div class="row"><span class="k">${{r}}</span><b class="num">${{o.clean}}/${{o.validated}}</b></div>`).join('') : '';
  $('#detail').innerHTML = rows + rates
    + (top.length ? `<div class="row" style="border:0;padding-top:12px"><span class="k">Top states by locations</span></div>`
        + top.map(([s, n]) => `<div class="row"><span class="k mono">${{s}}</span><b class="num">${{fmt(n)}}</b></div>`).join('') : '')
    + (byres ? `<div class="row" style="border:0;padding-top:12px"><span class="k">Clean instances by resource</span></div>` + byres : '');
}}

function rowOf(p) {{
  return {{
    id: p.id, name: p.name, is_ma: p.is_ma,
    states: statesOf(p).length, locations: locOf(p),
    practitioners: (p.resources || {{}}).Practitioner || 0,
    roles: (p.resources || {{}}).PractitionerRole || 0,
    conformance: confRate(p), binding: bindRate(p),
    uscdi: covRate(p, 'uscdi'), mpf: covRate(p, 'mpf'), plannet_ext: covRate(p, 'plannet_ext'),
  }};
}}

function drawTable() {{
  const q = $('#q').value.trim().toLowerCase();
  let rows = activePayers().map(rowOf);
  if (q) rows = rows.filter(r => r.name.toLowerCase().includes(q) || r.id.includes(q));
  rows.sort((a, b) => {{
    const x = a[sortK], y = b[sortK];
    if (x == null && y == null) return 0;
    if (x == null) return 1;          // no-data always sinks, either direction
    if (y == null) return -1;
    return (typeof x === 'string' ? x.localeCompare(y) : x - y) * sortDir;
  }});
  const rate = v => v == null ? '<span class="nd">—</span>' : `<span class="num">${{(v * 100).toFixed(1)}}%</span>`;
  $('#tbl tbody').innerHTML = rows.map(r =>
    `<tr data-p="${{r.id}}"><td>${{r.name}}${{r.is_ma ? '<span class="tag">MA</span>' : ''}}</td>`
    + `<td class="num">${{r.states}}</td><td class="num">${{fmt(r.locations)}}</td>`
    + `<td class="num">${{fmt(r.practitioners)}}</td><td class="num">${{fmt(r.roles)}}</td>`
    + `<td>${{rate(r.conformance)}}</td><td>${{rate(r.binding)}}</td><td>${{rate(r.uscdi)}}</td>`
    + `<td>${{rate(r.mpf)}}</td><td>${{rate(r.plannet_ext)}}</td></tr>`).join('');
  for (const th of document.querySelectorAll('#tbl th'))
    th.classList.toggle('on', th.dataset.k === sortK);
  for (const tr of document.querySelectorAll('#tbl tbody tr'))
    tr.addEventListener('click', () => showPayer(tr.dataset.p));
}}

function drawStats() {{
  const list = activePayers();
  const loc = list.reduce((s, p) => s + locOf(p), 0);
  const pr = list.reduce((s, p) => s + ((p.resources || {{}}).PractitionerRole || 0), 0);
  const sts = new Set(); for (const p of list) for (const s of statesOf(p)) sts.add(s);
  const rates = list.map(confRate).filter(v => v != null);
  const mean = rates.length ? rates.reduce((a, b) => a + b, 0) / rates.length : null;
  const cards = [
    ['Payers', list.length], ['States covered', sts.size],
    ['Locations', fmt(loc)], ['PractitionerRoles', fmt(pr)],
    ['Mean structural conformance', mean == null ? '—' : (mean * 100).toFixed(1) + '%'],
  ];
  $('#stats').innerHTML = cards.map(([k, v]) =>
    `<div class="stat"><div class="k">${{k}}</div><div class="v num">${{v}}</div></div>`).join('');
}}

function redraw() {{ drawStats(); drawMap(); drawDetail(); drawTable(); }}

$('#focus').innerHTML = '<option value="">None</option>' +
  [...P].sort((a, b) => a.name.localeCompare(b.name))
    .map(p => `<option value="${{p.id}}">${{p.name}}</option>`).join('');

$('#metric').addEventListener('change', e => {{
  metric = e.target.value;
  if (METRIC[metric].kind === 'rate' && ['locations','states','practitioners','roles'].includes(sortK)) {{
    sortK = metric === 'payers' ? 'locations' : metric; sortDir = -1;
  }}
  redraw();
}});
$('#scope').addEventListener('change', e => {{ scope = e.target.value; redraw(); }});
$('#focus').addEventListener('change', e => {{
  focus = e.target.value;
  if (focus) {{ $('#scope').disabled = true; showPayer(focus); }} else {{ $('#scope').disabled = false; }}
  drawStats(); drawMap(); drawTable();
}});
$('#q').addEventListener('input', drawTable);
for (const th of document.querySelectorAll('#tbl th')) th.addEventListener('click', () => {{
  const k = th.dataset.k;
  if (sortK === k) sortDir = -sortDir; else {{ sortK = k; sortDir = k === 'name' ? 1 : -1; }}
  drawTable();
}});

redraw();
</script>
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=str(REPO / "output" / "quality" / "map_data.json"))
    ap.add_argument("--out", default=str(REPO / "output" / "quality" / "directory_map.html"))
    a = ap.parse_args()
    data = json.loads(Path(a.data).read_text(encoding="utf-8"))
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(build(data), encoding="utf-8")
    print(f"wrote {out} ({out.stat().st_size:,} bytes, {len(data['payers'])} payers)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
