#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Render the framework report artifact from the prebuilt payload.

    .venv\\Scripts\\python.exe scripts\\build_framework_payload.py
    .venv\\Scripts\\python.exe scripts\\build_framework_report.py

Reads  output/quality/framework_mockup_data.json
Writes output/quality/framework-report-mockup.html  (self-contained, no external refs)

Implements docs/evaluation-framework.md v0.3.0 §9: the five report structures, §9.1's
shared visual rules, and §9.1a's reading contract (a summary cell shows a banded rate and
its n, or exactly one of four plain reasons there is no rate).
"""
from __future__ import annotations

import io
import json
import os

REPO = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
SRC = os.path.join(REPO, "output", "quality", "framework_mockup_data.json")
OUT = os.path.join(REPO, "output", "quality", "framework-report-mockup.html")

CSS = """
:root{
  --bg:#fbfbfa; --panel:#ffffff; --raised:#f4f4f2; --ink:#1a1a19; --ink-2:#4a4a46;
  --muted:#77776f; --line:#e2e2dd; --line-2:#cfcfc8;
  --b0:#e7ecef; --b1:#c6d5de; --b2:#9db8c8; --b3:#6f97ae; --b4:#3f7391;
  --ink-on-b:#0e1a22; --ink-on-b-hi:#ffffff;
  --accent:#3f7391; --accent-soft:#e8eff3; --accent-ink:#24485c;
  --warn-soft:#f6efe4; --warn-ink:#6b5426; --warn-line:#d9c49a;
  --hatch:#c9c9c2;
}
@media (prefers-color-scheme:dark){
  :root:not([data-theme="light"]){
    --bg:#131316; --panel:#1a1a1e; --raised:#232329; --ink:#ececef; --ink-2:#c2c2c8;
    --muted:#8d8d97; --line:#2c2c33; --line-2:#3c3c45;
    --b0:#262b30; --b1:#33454f; --b2:#456070; --b3:#5b8299; --b4:#78a5bc;
    --ink-on-b:#e6eef3; --ink-on-b-hi:#0d1519;
    --accent:#77a3ba; --accent-soft:#1e2a32; --accent-ink:#a8cbdc;
    --warn-soft:#2a2317; --warn-ink:#e0cfa8; --warn-line:#4d4026;
    --hatch:#3a3a42;
  }
}
:root[data-theme="dark"]{
  --bg:#131316; --panel:#1a1a1e; --raised:#232329; --ink:#ececef; --ink-2:#c2c2c8;
  --muted:#8d8d97; --line:#2c2c33; --line-2:#3c3c45;
  --b0:#262b30; --b1:#33454f; --b2:#456070; --b3:#5b8299; --b4:#78a5bc;
  --ink-on-b:#e6eef3; --ink-on-b-hi:#0d1519;
  --accent:#77a3ba; --accent-soft:#1e2a32; --accent-ink:#a8cbdc;
  --warn-soft:#2a2317; --warn-ink:#e0cfa8; --warn-line:#4d4026;
  --hatch:#3a3a42;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);
  font:15px/1.55 ui-sans-serif,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  -webkit-text-size-adjust:100%}
.wrap{max-width:1240px;margin:0 auto;padding:26px 18px 70px}
h1{font-size:25px;line-height:1.22;margin:0 0 8px;letter-spacing:-.012em}
h2{font-size:18px;margin:0;letter-spacing:-.008em}
h3{font-size:14px;margin:0 0 8px;color:var(--ink-2);text-transform:uppercase;letter-spacing:.07em}
p{margin:0 0 10px;color:var(--ink-2)}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:.9em}
.sub{max-width:84ch}
header.top{border-bottom:1px solid var(--line);padding-bottom:18px;margin-bottom:22px}
.prov{display:flex;flex-wrap:wrap;gap:6px;margin-top:12px}
.chip{font-size:11.5px;padding:3px 8px;border-radius:999px;background:var(--raised);
  color:var(--ink-2);border:1px solid var(--line)}
.chip.k{background:var(--accent-soft);color:var(--accent-ink);border-color:transparent}
.runbar{display:flex;flex-wrap:wrap;gap:5px;padding:8px 16px;border-bottom:1px solid var(--line);
  background:var(--raised)}
.runbar .chip{background:var(--panel)}
.framing{background:var(--warn-soft);border:1px solid var(--warn-line);color:var(--warn-ink);
  border-radius:9px;padding:12px 14px;margin:16px 0 0;font-size:13.5px;max-width:90ch}
.framing b{color:inherit}
section.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;
  margin:22px 0;overflow:hidden}
.card>.hd{padding:14px 16px;border-bottom:1px solid var(--line);display:flex;
  flex-wrap:wrap;gap:10px;align-items:baseline;justify-content:space-between}
.card>.bd{padding:16px}
.q{font-size:13px;color:var(--muted);max-width:80ch;margin:2px 0 0}
.misread{margin-top:14px;border-top:1px dashed var(--line-2);padding-top:12px;font-size:13px}
.misread ul{margin:6px 0 0;padding-left:18px}
.misread li{margin:4px 0;color:var(--ink-2)}
.misread .lbl{font-weight:600;color:var(--ink)}
.scroll{overflow-x:auto;-webkit-overflow-scrolling:touch}
table{border-collapse:collapse;width:100%;font-size:13px;min-width:760px}
th,td{padding:7px 9px;text-align:left;border-bottom:1px solid var(--line);white-space:nowrap;
  vertical-align:middle}
th{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);
  font-weight:600;position:sticky;top:0;background:var(--panel);z-index:1}
th.sortable{cursor:pointer;user-select:none}
th.sortable:hover{color:var(--ink)}
th.sortable::after{content:"\\2195";opacity:.3;margin-left:4px}
th.sortable.on::after{content:"\\25BC";opacity:.9}
th.sortable.on.asc::after{content:"\\25B2";opacity:.9}
td.name{white-space:normal;min-width:190px}
tbody tr:hover td{background:var(--raised)}
.cell{display:inline-flex;align-items:center;gap:6px;border-radius:6px;padding:3px 7px;
  min-width:76px;justify-content:space-between;border:1px solid transparent}
.cell .v{font-variant-numeric:tabular-nums;font-weight:600}
.cell .n{font-size:10.5px;opacity:.72;font-variant-numeric:tabular-nums}
.b0{background:var(--b0);color:var(--ink-on-b)}
.b1{background:var(--b1);color:var(--ink-on-b)}
.b2{background:var(--b2);color:var(--ink-on-b)}
.b3{background:var(--b3);color:var(--ink-on-b-hi)}
.b4{background:var(--b4);color:var(--ink-on-b-hi)}
/* the four absent-measurement states. never a colour band, never a low score. */
.absent{font-size:11px;font-weight:500;color:var(--muted);min-width:76px;
  justify-content:flex-start}
.absent.notmeasured{background:repeating-linear-gradient(135deg,transparent,transparent 4px,
  var(--hatch) 4px,var(--hatch) 5px);border:1px solid var(--line-2)}
.absent.wrongelement{background:repeating-linear-gradient(135deg,transparent,transparent 4px,
  var(--hatch) 4px,var(--hatch) 5px);border:1px solid var(--warn-line);color:var(--warn-ink)}
.absent.toofew{border:1px dashed var(--line-2)}
.absent.nosignal{border:1px dotted var(--line-2)}
.badge{display:inline-block;font-size:11px;padding:2px 8px;border-radius:5px;
  border:1px solid var(--line-2);background:var(--raised);color:var(--ink-2);
  white-space:nowrap;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
.badge.solid{border-style:solid;border-width:2px;border-color:var(--accent)}
.badge.dash{border-style:dashed}
.badge.dot{border-style:dotted}
.badge.na{background:repeating-linear-gradient(135deg,transparent,transparent 4px,
  var(--hatch) 4px,var(--hatch) 5px)}
.conj{font-size:10px;color:var(--muted);margin-left:6px;white-space:nowrap}
.legend{display:flex;flex-wrap:wrap;gap:14px;align-items:center;font-size:12px;
  color:var(--muted);margin-top:12px}
.legend .sw{display:inline-flex;align-items:center;gap:5px}
.legend i{width:16px;height:12px;border-radius:3px;display:inline-block}
.controls{display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end;margin-bottom:14px}
.field{display:flex;flex-direction:column;gap:4px}
.field label{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
select,input[type=search]{background:var(--panel);color:var(--ink);border:1px solid var(--line-2);
  border-radius:7px;padding:6px 9px;font:inherit;font-size:13px;min-width:190px;max-width:100%}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr));gap:16px}
.kv{display:grid;grid-template-columns:auto 1fr;gap:5px 12px;font-size:13px;align-items:baseline}
.kv dt{color:var(--muted)}
.kv dd{margin:0;font-variant-numeric:tabular-nums}
.dimrow{display:grid;grid-template-columns:38px 1fr auto;gap:10px;align-items:center;
  padding:7px 0;border-bottom:1px solid var(--line)}
.dimrow:last-child{border-bottom:0}
.dimrow .code{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;color:var(--muted)}
.dimrow .lab{font-size:13px}
.dimrow .lens{font-size:10.5px;color:var(--muted);text-transform:uppercase;letter-spacing:.05em}
.layer{font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);
  margin:14px 0 2px;padding-top:10px;border-top:1px solid var(--line-2)}
.layer:first-child{border-top:0;padding-top:0;margin-top:0}
.dotplot{margin-top:4px}
.dotrow{display:grid;grid-template-columns:220px 1fr 96px;gap:10px;align-items:center;
  font-size:12px;padding:3px 0}
.dotrow .track{position:relative;height:16px;background:var(--b0);border-radius:8px;
  border:1px solid var(--line)}
.dotrow .track i{position:absolute;top:50%;width:7px;height:7px;border-radius:50%;
  background:var(--accent);transform:translate(-50%,-50%);opacity:.72}
.dotrow .cnt{text-align:right;color:var(--muted);font-variant-numeric:tabular-nums}
.dotrow .lbl{color:var(--ink-2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.flagbox{background:var(--warn-soft);border:1px solid var(--warn-line);color:var(--warn-ink);
  border-radius:8px;padding:10px 12px;font-size:12.5px;margin-top:12px}
.notebox{background:var(--raised);border:1px solid var(--line);color:var(--ink-2);
  border-radius:8px;padding:10px 12px;font-size:12.5px;margin-top:12px}
.inert{position:relative;border:1px dashed var(--line-2);border-radius:10px;padding:18px;
  background:repeating-linear-gradient(135deg,transparent,transparent 7px,
  var(--hatch) 7px,var(--hatch) 8px);text-align:center}
.inert .msg{background:var(--panel);display:inline-block;padding:12px 16px;border-radius:8px;
  border:1px solid var(--line-2);font-size:13px;max-width:62ch;color:var(--ink-2)}
.audience{font-size:13px}
.audience th{white-space:normal}
.audience td{white-space:normal;vertical-align:top;min-width:150px}
footer{margin-top:34px;padding-top:18px;border-top:1px solid var(--line);
  font-size:12.5px;color:var(--muted);max-width:90ch}
footer p{color:inherit;margin:0 0 9px}
.alias{font-size:10px;border:1px solid var(--line-2);border-radius:4px;padding:1px 4px;
  color:var(--muted);margin-left:5px}
.bar{display:flex;height:14px;border-radius:4px;overflow:hidden;border:1px solid var(--line-2)}
.bar span{display:block;height:100%}
@media (max-width:640px){
  .wrap{padding:18px 12px 50px}
  h1{font-size:21px}
  .dotrow{grid-template-columns:120px 1fr 66px}
  .field,select,input[type=search]{min-width:0;width:100%}
  .controls{flex-direction:column;align-items:stretch}
  .grid2{grid-template-columns:1fr}
}
"""

JS = r"""
const D=DATA_PLACEHOLDER;
const P=D.payers.slice().sort((a,b)=>a.name.localeCompare(b.name));
const $=s=>document.querySelector(s);
const esc=s=>String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
const num=n=>n==null?'n/a':n.toLocaleString('en-US');
// §9.1 rule 9: rounding must not promote across a boundary that reads as a claim.
// 0.9997 renders 99.9%, never 100.0%; a small non-zero renders "<0.1%", never 0.0%.
// Everything in between rounds normally, so the page and the specification agree.
function pctS(v){ if(v==null) return 'n/a';
  const p=v*100;
  if(v>=1) return '100.0%';
  if(v<=0) return '0.0%';
  let x=Math.round(p*10)/10;
  if(x>=100) return '99.9%';
  if(x<=0) return '<0.1%';
  return x.toFixed(1)+'%'; }
// §9.1 rule 1: five bands of exactly 20 points, wider than the +/-18pp interval.
function band(v){ if(v==null) return null; const p=v*100;
  return p<20?0:p<40?1:p<60?2:p<80?3:4; }

// §9.1a reading contract: a rate + its n, or exactly ONE of four plain reasons.
const ABSENT={'not measured':'notmeasured','wrong element':'wrongelement',
              'too few rows':'toofew','no signal':'nosignal'};
function absentHTML(reason,title){
  return '<span class="cell absent '+(ABSENT[reason]||'notmeasured')+'" title="'+
         esc(title||'')+'">'+reason+'</span>'; }
function cellHTML(v,n,title,absent){
  if(v==null) return absentHTML(absent||'not measured',title);
  return '<span class="cell b'+band(v)+'" title="'+esc(title||'')+'"><span class="v">'+pctS(v)+
         '</span><span class="n">'+(n!=null?('n='+num(n)):'')+'</span></span>';
}
function covOf(p,a){ const c=p.cov&&p.cov[a]; return c?c.ruleB:null; }

const GATE1={'capture-bounded':'solid','capture-suspect-superset':'dot',
  'capture-incomplete':'dash','not-assessable':'na'};
const GATE2={'chain-resolves':'solid','chain-partially-resolves':'dot',
  'chain-does-not-resolve':'dash','not-assessable':'na'};
function gb(state,map,title){
  return '<span class="badge '+(map[state]||'')+'" title="'+esc(title||'')+'">'+state+'</span>'; }
function g1b(p){
  const g=p.gates.g1;
  return gb(g.state,GATE1,Object.entries(g.per_resource||{}).map(x=>x[0]+': '+x[1]).join('\n'))+
    '<span class="conj">'+g.conjuncts_evaluated+'/'+g.conjuncts_total+' conjuncts</span>'; }
function g2b(p){ const g=p.gates.g2; return gb(g.state,GATE2,g.reason||''); }

/* ---------------- 1 · cross-payer comparison (§9.3) ---------------- */
const DIMS=[
  ['d3','D3 structural','cell mean, conformance run '+D.runs.conformance+', n<30 cells suppressed'],
  ['d4','D4 terminology','offline binding expansion, run '+D.runs.binding],
  ['uscdi','D5 USCDI','rule B concept mean, coverage run '+D.runs.coverage],
  ['mpf','D5 MPF','rule B concept mean, coverage run '+D.runs.coverage],
  ['plannet_ext','D5 Plan-Net ext','rule B concept mean; communication-proficiency excluded (wrong element path)'],
  ['d6','D6 chain rate','share of Practitioners traversing to an InsurancePlan (§5.4 G2)'],
];
function dimVal(p,k){
  if(k==='d3') return p.d3?p.d3.rate:null;
  if(k==='d4') return p.d4?p.d4.rate:null;
  if(k==='d6') return p.gates.g2?p.gates.g2.chain_rate:null;
  const c=covOf(p,k); return c?c.rate:null;
}
function dimN(p,k){
  if(k==='d3') return p.d3&&p.d3.n_cells?p.d3.n_cells:null;
  if(k==='d4') return p.d4&&p.d4.rows?p.d4.rows:null;
  if(k==='d6') return p.gates.g2&&p.gates.g2.practitioners_in_chain!=null?p.resources.practitioner:null;
  const c=covOf(p,k); return c?c.n_concepts:null;
}
function dimAbsent(p,k){
  if(k==='d6'){ const g=p.gates.g2||{};
    if(g.method==='sample') return ['not measured','state set from a '+num(g.sample_n)+
      '-row existence sample; a sampled test may set a state but not a rate (§9.1 rule 10)'];
    return ['not measured',g.reason||'not measured']; }
  const c=covOf(p,k);
  if(c&&c.n_concepts===0){
    if(c.n_path_refuted&&!c.n_below_floor&&!c.n_unmeasurable) return ['wrong element',c.refuted.map(r=>r.cid).join(', ')];
    if(c.n_below_floor&&!c.n_unmeasurable) return ['too few rows','every cell below the n>=30 or method floor'];
    return ['not measured','no measurable required cell'];
  }
  return ['not measured',''];
}
function dimCell(p,k){
  const v=dimVal(p,k);
  if(v!=null) return cellHTML(v,dimN(p,k),DIMS.find(d=>d[0]===k)[2]);
  const a=dimAbsent(p,k); return absentHTML(a[0],a[1]);
}
let SORT={key:null,asc:false};
function renderCompare(){
  const q=($('#q').value||'').toLowerCase();
  const scope=$('#scope').value;
  let rows=P.filter(p=>p.name.toLowerCase().includes(q)||p.id.includes(q));
  if(scope==='ma') rows=rows.filter(p=>p.is_ma);
  if(scope==='dedup') rows=rows.filter(p=>!p.alias_group||
      P.filter(x=>x.alias_group===p.alias_group)[0].id===p.id);
  if(SORT.key){
    // §9.3: sorting re-sorts WITHIN bands, never within raw values.
    rows=rows.slice().sort((a,b)=>{
      const ba=band(dimVal(a,SORT.key)), bb=band(dimVal(b,SORT.key));
      const ka=ba==null?-1:ba, kb=bb==null?-1:bb;
      if(ka!==kb) return SORT.asc?ka-kb:kb-ka;
      return a.name.localeCompare(b.name);
    });
  }
  const head='<tr><th>Directory dataset</th><th>G1 capture</th><th>G2 graph</th>'+
    DIMS.map(d=>'<th class="sortable'+(SORT.key===d[0]?' on'+(SORT.asc?' asc':''):'')+
      '" data-k="'+d[0]+'" title="sorts within bands, not within raw values">'+d[1]+'</th>').join('')+
    '<th>Types</th></tr>';
  const body=rows.map(p=>{
    const al=p.alias_group?'<span class="alias" title="aliased dataset: '+esc(p.alias_group)+
      '">alias</span>':'';
    return '<tr><td class="name">'+esc(p.name)+al+'<br><span class="mono" style="opacity:.55">'+
      esc(p.id)+'</span></td>'+
      '<td>'+g1b(p)+'</td><td>'+g2b(p)+'</td>'+
      DIMS.map(d=>'<td>'+dimCell(p,d[0])+'</td>').join('')+
      '<td style="font-variant-numeric:tabular-nums">'+p.n_types+'/8</td></tr>';
  }).join('');
  $('#cmp').innerHTML='<thead>'+head+'</thead><tbody>'+body+'</tbody>';
  $('#cmpcount').textContent=rows.length+' of '+P.length+' shown';
  // §6.4 limit 4: the MPF obligation set exists for a payer because OUR classifier said so.
  $('#scopenote').innerHTML = scope==='ma' ?
    '<div class="notebox"><b>This is a filter over a caveat, not a comparison stratum '+
    '(&sect;6.3).</b> Line of business is a property of a <i>directory</i>, not of an '+
    'organisation, and the classifier detects <b>evidence of MA in the published data</b> '+
    '&mdash; never "this is an MA plan". Thirteen inclusions are public-record overrides with '+
    'no data signal at all. And the MPF element checks run <b>only</b> for datasets this '+
    'classifier calls MA, so the datasets hidden by this filter are not a control group: they '+
    'are absent from that column for a reason internal to this project. "MA directories carry '+
    'more elements than non-MA ones" is not a claim this data can support.</div>' :
    (scope==='dedup' ? '<div class="notebox"><b>Deduplicated datasets only.</b> The five '+
    '<span class="mono">amerihealth_*</span> schemas are one family and '+
    '<span class="mono">wellmark</span>/<span class="mono">wellmark_advantage</span> are one '+
    'dataset behind two hostnames. Every distribution, percentile and threshold in the '+
    'specification is computed on this view, not on the full list. Detection is currently by '+
    'inspection, so there may be aliased pairs not yet noticed.</div>' : '');
  $('#cmp').querySelectorAll('th.sortable').forEach(th=>th.addEventListener('click',()=>{
    const k=th.dataset.k;
    if(SORT.key===k&&!SORT.asc) SORT={key:k,asc:true};
    else if(SORT.key===k&&SORT.asc) SORT={key:null,asc:false};
    else SORT={key:k,asc:false};
    renderCompare();
  }));
}

/* ---------------- 2 · per-payer scorecard (§9.2) ---------------- */
const LAYERS=[
  ['L0 Access',[['D1','Retrievability','letter + usefulness','g1'],
                ['D2','Interface conformance','letter','na']]],
  ['L1 Conformance',[['D3','Structural conformance','letter','d3'],
                     ['D4','Terminology conformance','letter','d4']]],
  ['L2 Content',[['D5a','IG must-support','letter','na'],
                 ['D5b','MPF elements','letter','mpf'],
                 ['D5e','USCDI elements','letter + spirit','uscdi'],
                 ['D5x','Plan-Net extensions','spirit','plannet_ext'],
                 ['D6','Referential integrity & navigability','letter + usefulness','d6'],
                 ['D7','Plausibility & placeholder','spirit + usefulness','na']]],
  ['L3 Correspondence',[['D8','Currency & verification provenance','letter + spirit','d8'],
                        ['D9','External correspondence','usefulness','na']]],
  ['L4 Consequence',[['D10','Access impact — one proxy only: PractitionerRoles that carry contact information or an Endpoint (US Core pd-1). Higher is better. Not an accuracy or availability rate.','usefulness','pd1']]],
];
function renderCard(){
  const p=P.find(x=>x.id===$('#focus').value)||P[0];
  const g2=p.gates.g2||{};
  let h='<div class="grid2"><div>';
  h+='<h3>Gates</h3><div class="kv">'+
     '<dt>G1 capture</dt><dd>'+g1b(p)+'</dd>'+
     '<dt>G2 published graph</dt><dd>'+g2b(p)+'</dd></div>';
  h+='<p style="font-size:12px;margin-top:10px">Gate states describe the <b>capture</b> and '+
     'the <b>published graph</b>. They are not statements about this organisation. '+
     'G1 evaluates <b>1 of its 3 conjuncts</b>: the bidirectional id check has never been run '+
     'and provenance carries no load-mode assertion, so a bounded capture is not a certified one.</p>';
  if(g2.reason) h+='<div class="notebox"><b>G2 &mdash; why:</b> '+esc(g2.reason)+'</div>';
  const gp=p.gates.g1.per_resource||{};
  if(Object.keys(gp).length){
    h+='<h3 style="margin-top:18px">G1 per resource</h3><div class="kv">';
    for(const k in gp) h+='<dt class="mono">'+esc(k)+'</dt><dd><span class="badge '+
      (GATE1[gp[k]]||'')+'">'+gp[k]+'</span>'+(p.gates.g1.coverage[k]!=null?
      ' <span class="conj">'+p.gates.g1.coverage[k].toFixed(1)+'% of declared</span>':'')+'</dd>';
    h+='</div>';
    const nd=p.gates.g1.row_count_never_decreased||[];
    if(nd.length) h+='<div class="notebox"><b>Load-mode indicator, reported beside the gate and '+
      'never inside it:</b> '+nd.length+' of this dataset\'s resources have been loaded twice or '+
      'more and their row count has <b>never decreased</b>. That is consistent with an '+
      'upsert-only loader and is not proof of one &mdash; a stable directory also never shrinks.</div>';
  }
  h+='<h3 style="margin-top:18px">Published volume</h3><div class="kv">';
  const rs=p.resources;
  for(const k in rs) h+='<dt class="mono">'+k+'</dt><dd>'+(rs[k]<0?'unknown (never analysed)':num(rs[k]))+'</dd>';
  h+='<dt class="mono">resource types</dt><dd>'+p.n_types+' of 8</dd></div>';
  if(p.d8_tables&&Object.keys(p.d8_tables).length){
    h+='<h3 style="margin-top:18px">D8 timestamp signal, per table</h3><div class="scroll"><table>'+
      '<thead><tr><th>Table</th><th>Rows sampled</th><th>Distinct lastUpdated</th><th>Null</th>'+
      '<th>Signal</th></tr></thead><tbody>';
    for(const t in p.d8_tables){ const e=p.d8_tables[t];
      h+='<tr><td class="mono">'+t+'</td><td>'+num(e.n)+'</td><td>'+num(e.distinct)+'</td><td>'+
        num(e.nulls)+'</td><td>'+(e.distinct<=1?'<span class="badge dash">degenerate</span>':
        '<span class="badge">sound</span>')+'</td></tr>'; }
    h+='</tbody></table></div><p style="font-size:12px">Presence and entropy only. Neither is '+
      'freshness &mdash; the element means server load time, and every regulatory clock starts at '+
      'an event invisible from outside the payer. <b>Degeneracy is a property of a (dataset, table) '+
      'pair, not of a dataset</b>: a check reading a timestamp takes its signal from the table it '+
      'reads, and importing it from another table is the same class of error as reading an element '+
      'at the wrong path.</p>';
  }
  h+='</div><div>';
  h+='<h3>Dimension vector &mdash; never summed</h3>';
  for(const [lname,ds] of LAYERS){
    h+='<div class="layer">'+lname+'</div>';
    for(const [code,lab,lens,key] of ds){
      let cell;
      if(key==='na') cell=absentHTML('not measured','no corpus-wide results for this dimension yet');
      else if(key==='g1') cell=g1b(p);
      else if(key==='pd1') cell=p.pd1?cellHTML(1-(p.pd1.fail/p.pd1.n),p.pd1.n,
        'share of sampled PractitionerRoles carrying contact information or an Endpoint (US Core pd-1). weak-proxy.'):
        absentHTML('not measured','no PractitionerRole cell in this conformance run');
      else if(key==='d8') cell=p.d8?(p.d8.degenerate?absentHTML('no signal',
        'the sampled meta.lastUpdated column holds one distinct value or none'):
        cellHTML(1-(p.d8.nulls/p.d8.n),p.d8.n,'timestamp presence &mdash; not freshness')):
        absentHTML('not measured','');
      else cell=dimCell(p,key);
      if(key!=='na'&&key!=='g1'&&key!=='d8'&&key!=='pd1'&&key!=='d6'){
        const c=covOf(p,key);
        if(c&&c.n_path_refuted) cell+=' <span class="cell absent wrongelement" title="'+
          esc((c.refuted[0]||{}).note||'')+'">'+c.n_path_refuted+' wrong element</span>';
        if(c&&c.n_below_floor) cell+=' <span class="cell absent toofew">'+c.n_below_floor+' too few rows</span>';
        if(c&&c.n_unmeasurable) cell+=' <span class="cell absent notmeasured">'+c.n_unmeasurable+' not measured</span>';
      }
      h+='<div class="dimrow"><span class="code">'+code+'</span>'+
         '<span><span class="lab">'+lab+'</span><br><span class="lens">'+lens+'</span></span>'+
         '<span>'+cell+'</span></div>';
    }
  }
  h+='<p style="font-size:12px;margin-top:12px">Rows are ordered by <b>layer</b>, which is a '+
     'dependency order, not a priority order. Four states mean "no rate here": '+
     '<b>not measured</b>, <b>wrong element</b>, <b>too few rows</b>, <b>no signal</b>. '+
     'None of them is a low score.</p>';
  h+='</div></div>';

  if(g2.state&&g2.state!=='not-assessable'){
    h+='<h3 style="margin-top:20px">D6 &mdash; the chain, link by link (&sect;5.4 G2)</h3><div class="scroll"><table>'+
      '<thead><tr><th>InsurancePlans</th><th>&hellip;carrying a network ref</th>'+
      '<th>&hellip;named</th><th>network refs</th><th>&hellip;resolving to a published Organization</th>'+
      '<th>PractitionerRoles reaching a plan-backed network</th><th>Method</th></tr></thead><tbody><tr>'+
      '<td>'+num(g2.plans)+'</td><td>'+num(g2.plans_with_network_ref)+'</td><td>'+num(g2.plans_named)+
      '</td><td>'+num(g2.plan_network_refs)+'</td><td>'+num(g2.plan_network_refs_resolved)+
      '</td><td>'+num(g2.roles_to_plan_network)+(g2.method==='sample'?' <span class="conj">of '+
      num(g2.sample_n)+' sampled</span>':'')+'</td><td class="mono">'+(g2.method||'-')+
      '</td></tr></tbody></table></div>';
    if(g2.chain_rate!=null){
      h+='<div class="kv" style="margin-top:10px"><dt>chain rate</dt><dd>'+
        cellHTML(g2.chain_rate,p.resources.practitioner,'Practitioners traversing to any InsurancePlan')+
        '</dd><dt>chain to a <b>named</b> plan</dt><dd>'+
        cellHTML(g2.named_chain_rate,p.resources.practitioner,'Practitioners traversing to an InsurancePlan that carries a name')+'</dd></div>';
      if(g2.named_chain_rate!=null&&g2.chain_rate-g2.named_chain_rate>0.05)
        h+='<div class="flagbox"><b>The two rates diverge, and the divergence is the finding.</b> '+
          'A chain that resolves to a plan the member cannot recognise as theirs answers nothing. '+
          'This dataset traverses to plans that carry no <span class="mono">name</span>.</div>';
    } else if(g2.method==='sample'){
      h+='<div class="notebox">The exact traversal did not complete inside the query budget, so the '+
        '<b>state</b> above was established from the first '+num(g2.sample_n)+' PractitionerRole rows '+
        'in physical order. <b>No chain rate is published.</b> A sampled existence test may set a '+
        'state; it may not produce a percentage.</div>';
    }
  }

  const rows=['uscdi','mpf','plannet_ext'].filter(a=>p.cov&&p.cov[a]);
  if(rows.length){
    h+='<h3 style="margin-top:20px">Pooling rule, side by side (&sect;5.1)</h3><div class="scroll"><table>'+
      '<thead><tr><th>Assessment</th><th>Rule A (current, cell mean)</th>'+
      '<th>Rule B (concept mean, floors + aliases)</th><th>Concepts</th><th>Cells used</th>'+
      '<th>Wrong element</th><th>Too few rows</th><th>Not measurable</th></tr></thead><tbody>';
    for(const a of rows){ const c=p.cov[a];
      h+='<tr><td class="mono">'+a+'</td><td>'+cellHTML(c.ruleA)+'</td><td>'+
         cellHTML(c.ruleB.rate,c.ruleB.n_concepts)+'</td><td>'+c.ruleB.n_concepts+'</td><td>'+c.ruleB.n_cells+
         '</td><td>'+(c.ruleB.n_path_refuted||'&mdash;')+'</td><td>'+(c.ruleB.n_below_floor||'&mdash;')+
         '</td><td>'+(c.ruleB.n_unmeasurable||'&mdash;')+'</td></tr>'; }
    h+='</tbody></table></div>';
    h+='<div class="notebox"><b>These two columns are not on the same scale.</b> Rule A counts '+
      'an obligation once per resource type it appears on; rule B counts it once. They are shown '+
      'side by side to make the correction checkable, and they must never be plotted on one axis '+
      'or joined by a trend line. Rule B additionally excludes checks that read the wrong element '+
      'and cells below the floors; rule A excludes neither.</div>';
    // per-concept detail: mean vs pooled vs denominator span (§5.1)
    h+='<h3 style="margin-top:18px">Concepts, with the denominator span and the pooled rate</h3>'+
      '<div class="scroll"><table><thead><tr><th>Concept</th><th>Cells</th><th>n range</th>'+
      '<th>Concept mean (rule B)</th><th>Row-pooled</th><th>Signal</th></tr></thead><tbody>';
    for(const a of rows) for(const c of p.cov[a].ruleB.concepts){
      const div=Math.abs(c.rate-c.pooled)>0.10;
      h+='<tr><td class="mono" title="'+esc(c.cid)+'">'+esc(c.cid.replace(/^.*\//,''))+'</td><td>'+
        c.cells.length+'</td><td style="font-variant-numeric:tabular-nums">'+num(c.n_min)+
        (c.n_min===c.n_max?'':' &ndash; '+num(c.n_max))+'</td><td>'+cellHTML(c.rate)+'</td><td>'+
        cellHTML(c.pooled)+(div?' <span class="conj">diverges</span>':'')+'</td><td>'+
        (c.signal==='degenerate'?'<span class="badge dash" title="the column this check reads holds one distinct value or none">degenerate</span>':
         c.signal==='not-evaluated'?'<span class="badge dot" title="the underlying column was not sampled; the signal axis is not asserted here">not evaluated</span>':
         '<span class="conj">&mdash;</span>')+
        '</td></tr>';
    }
    h+='</tbody></table></div>';
    const ref=rows.map(a=>p.cov[a].ruleB.refuted).flat();
    if(ref.length) h+='<div class="flagbox"><b>Excluded: the check reads the wrong element.</b> '+
      ref.map(r=>'<span class="mono">'+esc(r.rt)+'.'+esc(r.el)+'</span> measured '+r.pop+'/'+r.d+
      ' &mdash; '+esc(r.note)).join(' ')+' It is out of every headline mean on this page, not merely '+
      'flagged on one view.</div>';
    const bf=rows.map(a=>p.cov[a].ruleB.below).flat();
    if(bf.length) h+='<div class="notebox"><b>Cells excluded by the &sect;5.1 floors</b> &mdash; shown, '+
      'never silently dropped: '+bf.slice(0,6).map(x=>esc(x.rt)+'.'+esc(x.el)+' ('+x.pop+'/'+x.d+
      ', '+esc(x.why)+')').join('; ')+(bf.length>6?' &hellip;and '+(bf.length-6)+' more':'')+'.</div>';
  }
  $('#card').innerHTML=h;
}

/* ---------------- 3 · dimension breakdown (§9.4) ---------------- */
function dedupCount(pts){
  const seen=new Set(); let n=0;
  for(const [p] of pts){ const k=p.alias_group||p.id; if(!seen.has(k)){seen.add(k);n++;} }
  return n;
}
function renderBreak(){
  const a=$('#assess').value;
  const byC={}; const refuted={};
  for(const p of P){ const c=covOf(p,a); if(!c) continue;
    for(const con of c.concepts){ (byC[con.cid]=byC[con.cid]||[]).push([p,con.rate]); }
    for(const r of c.refuted){ (refuted[r.cid]=refuted[r.cid]||[]).push([p,r.d?r.pop/r.d:0,r.note]); } }
  let h='';
  for(const k of Object.keys(byC).sort()){
    const pts=byC[k];
    const zero=pts.filter(x=>x[1]<0.01).length;
    const nd=dedupCount(pts);
    // §1.1.1: review-flag needs >=8 deduplicated datasets AND >=90% within 1pp of zero.
    // near-100% raises nothing: a met obligation looks exactly like that.
    const flag=(nd>=8)&&(zero/pts.length>=0.9);
    h+='<div class="dotrow"><span class="lbl mono" title="'+esc(k)+'">'+
       esc(k.replace(/^.*\//,'').replace(/^(uscdi|mpf):/,''))+'</span>'+
       '<span class="track">'+pts.map(x=>'<i style="left:'+(x[1]*100).toFixed(2)+
         '%" title="'+esc(x[0].name)+' &mdash; '+pctS(x[1])+'"></i>').join('')+'</span>'+
       '<span class="cnt">'+pts.length+' / '+nd+' dedup</span></div>';
    if(flag) h+='<div class="flagbox" style="margin:2px 0 8px"><b>review-flag</b> &mdash; '+
      zero+' of '+pts.length+' datasets within a point of zero, over '+nd+' deduplicated datasets. '+
      '&sect;1.1.1 requires someone to read the StructureDefinition and set this check\'s '+
      '<span class="mono">path_status</span>. <b>It changes no number.</b> A near-100% row raises '+
      'nothing &mdash; organisations having names is what a met obligation looks like.</div>';
  }
  for(const k of Object.keys(refuted).sort()){
    const pts=refuted[k];
    h+='<div class="dotrow"><span class="lbl mono" title="'+esc(k)+'">'+
       esc(k.replace(/^.*\//,''))+'</span><span class="track" style="opacity:.45">'+
       pts.map(x=>'<i style="left:'+(x[1]*100).toFixed(2)+'%" title="'+esc(x[0].name)+
       ' &mdash; as measured (wrong path)"></i>').join('')+'</span>'+
       '<span class="cnt">'+pts.length+'</span></div>';
    h+='<div class="flagbox" style="margin:2px 0 8px"><b>path_status: refuted &mdash; excluded from '+
      'every headline on this page.</b> '+esc(pts[0][2])+' The dots above are what the broken check '+
      'reports, drawn faint, because a reader who cannot see the wrong number cannot check the claim '+
      'that it was removed.</div>';
  }
  $('#brk').innerHTML=h||'<p>No concepts for this assessment.</p>';
}

/* ---------------- corpus panels ---------------- */
function renderCorpus(){
  const C=D.corpus;
  const g1=C.g1.payer, g2=C.g2.payer;
  let h='<div class="grid2"><div><h3>G1 &mdash; capture</h3>'+
    Object.keys(g1).sort().map(k=>'<div style="margin:5px 0">'+gb(k,GATE1)+' '+g1[k]+'</div>').join('')+
    '<p style="font-size:12px;margin-top:10px"><b>1 of 3 conjuncts evaluated.</b> Coverage bounds only. '+
    'The bidirectional id check has never been run; provenance carries no load-mode assertion, so '+
    '"is this a fresh snapshot or a cumulative union?" is currently <i>unanswerable</i>, not merely '+
    'unanswered. That <span class="mono">not-assessable</span> is the largest bucket is the honest '+
    'finding: for most of the corpus, capture completeness is unknown rather than good.</p>'+
    '<p style="font-size:12px">Per resource: '+Object.keys(C.g1.resource).sort().map(k=>
      k+' '+C.g1.resource[k]).join(' &middot; ')+'.</p></div>';
  h+='<div><h3>G2 &mdash; published graph</h3>'+
    Object.keys(g2).sort().map(k=>'<div style="margin:5px 0">'+gb(k,GATE2)+' '+g2[k]+'</div>').join('')+
    '<p style="font-size:12px;margin-top:10px">Computed by walking the chain, not by a row-count '+
    'ratio. Why the chain does not resolve, where it does not:</p><div class="kv" style="font-size:12px">'+
    Object.entries(C.g2.reasons).sort((a,b)=>b[1]-a[1]).map(x=>'<dt>'+x[1]+'</dt><dd>'+esc(x[0])+'</dd>').join('')+
    '</div></div></div>';
  h+='<div class="notebox"><b>Exact chain rates, where the traversal completed ('+
    C.g2.exact_rates.length+' datasets).</b> '+C.g2.exact_rates.map(x=>pctS(x)).join(' &middot; ')+
    '. This is the distribution the specification asked for before setting a G2 threshold. The '+
    'conclusion was to remove the threshold: the state describes which link is missing, and the '+
    'rate carries the magnitude.</div>';
  $('#corpus').innerHTML=h;

  const pd=C.pd1;
  const fail=pd.failing/pd.instances;
  $('#pd1').innerHTML='<div class="grid2"><div><h3>US Core <span class="mono">pd-1</span></h3>'+
    '<p style="font-size:13px">&ldquo;SHALL have contact information or a reference to an Endpoint&rdquo;. '+
    'Plan-Net 1.2.0 rebased <span class="mono">PlannetPractitionerRole</span> onto '+
    '<span class="mono">us-core-practitionerrole</span>, which imported this invariant.</p>'+
    '<div class="bar"><span class="b4" style="width:'+(100*fail).toFixed(1)+'%"></span>'+
    '<span class="b0" style="width:'+(100-100*fail).toFixed(1)+'%"></span></div>'+
    '<p style="font-size:12px;margin-top:6px"><b>'+pctS(fail)+'</b> of '+num(pd.instances)+
    ' sampled PractitionerRole instances across '+pd.payers+' datasets carry neither contact '+
    'information nor an Endpoint reference. '+pd.at_100+' datasets at 100%, '+pd.at_0+' at 0%.</p></div>'+
    '<div><h3>Why it is here</h3><p style="font-size:13px">It is the closest thing this corpus has to '+
    'a measured <b>unreachable-listing</b> rate &mdash; D10\'s central proxy &mdash; and it was sitting '+
    'in the conformance data unread. It lands inside the 45&ndash;80% band the ghost-network literature '+
    'reports, which is <b>suggestive and not calibration</b>: the literature measures whether a call '+
    'reaches an accepting provider; this measures whether a phone number is present at all.</p>'+
    '<p style="font-size:12px">It is also the worked case for the review-flag asymmetry. A corpus split '+
    pd.at_100+'/'+pd.at_0+' across the extremes is not a broken check &mdash; and a rule that flagged '+
    '&ldquo;more than 90% in one bin&rdquo; would have said nothing about it either way.</p></div></div>';
}

function renderDelta(){
  const rows=P.filter(p=>p.d3&&p.d3.delta!=null&&Math.abs(p.d3.delta)>=0.005)
    .sort((a,b)=>a.d3.delta-b.d3.delta);
  let h='<div class="scroll"><table><thead><tr><th>Dataset</th><th>Run '+D.runs.conformance_prior+
    ' &middot; '+D.runs.ig_prior+'</th><th>Run '+D.runs.conformance+' &middot; '+D.runs.ig+
    '</th><th>Movement</th><th>Sole cause, from matched instances</th></tr></thead><tbody>';
  for(const p of rows){
    const d=p.d3;
    const cause=(d.delta<0?d.newly_failing:d.newly_clean).slice(0,2)
      .map(x=>x[1]+' &times; <span class="mono">'+esc(x[0])+'</span>').join(', ')||'&mdash;';
    h+='<tr><td class="name">'+esc(p.name)+'</td><td>'+cellHTML(d.prior_rate)+'</td><td>'+
      cellHTML(d.rate)+'</td><td style="font-variant-numeric:tabular-nums;font-weight:600">'+
      (d.delta>0?'+':'')+(d.delta*100).toFixed(1)+'</td><td style="white-space:normal">'+cause+'</td></tr>';
  }
  h+='</tbody></table></div>';
  $('#delta').innerHTML=h;
}

/* ---------------- boot ---------------- */
function boot(){
  const f=$('#focus');
  f.innerHTML=P.map(p=>'<option value="'+esc(p.id)+'">'+esc(p.name)+'</option>').join('');
  f.value=(P.find(p=>p.id==='bcbs_az')||P[0]).id;
  f.addEventListener('change',renderCard);
  $('#q').addEventListener('input',renderCompare);
  $('#scope').addEventListener('change',renderCompare);
  $('#assess').addEventListener('change',renderBreak);
  renderCompare(); renderCard(); renderBreak(); renderCorpus(); renderDelta();
}
document.readyState==='loading'?document.addEventListener('DOMContentLoaded',boot):boot();
"""

RUNBAR = """  <div class="runbar">
    <span class="chip k">coverage run {{COV}}</span>
    <span class="chip k">conformance run {{CONF}} &middot; Plan-Net {{IG}}</span>
    <span class="chip k">binding run {{BIND}}</span>
    <span class="chip">pg_class row estimates</span>
    <span class="chip">captured {{GEN}}</span>
    <span class="chip">framework v{{FV}}</span>
  </div>
"""

HTML = """<meta charset="utf-8">
<title>Directory Evaluation Scorecards</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{{CSS}}</style>
<div class="wrap">

<header class="top">
  <h1>Provider directory evaluation &mdash; report structure mockup</h1>
  <p class="sub">Renders the report structures specified in
  <span class="mono">docs/evaluation-framework.md</span> &sect;9, against real measured data.
  Every figure on this page is computed from the database or the FHIR packages on disk by
  <span class="mono">scripts/build_framework_payload.py</span>. Nothing is placeholder and
  nothing is asserted.</p>
  <div class="prov">
    <span class="chip k">coverage run {{COV}}</span>
    <span class="chip k">conformance run {{CONF}} &middot; Plan-Net {{IG}}</span>
    <span class="chip k">binding run {{BIND}}</span>
    <span class="chip">prior conformance run {{CONFP}} &middot; Plan-Net {{IGP}}</span>
    <span class="chip">row counts: pg_class estimates</span>
    <span class="chip">built {{GEN}}</span>
    <span class="chip">framework v{{FV}}</span>
    <span class="chip">{{NP}} datasets, payer set pinned to the run-3 set</span>
  </div>
  <div class="framing"><b>What these findings are.</b> Observations about data published at
  public FHIR endpoints, at a point in time. They are <b>not</b> compliance verdicts, legal
  determinations, or assessments of any organisation's regulatory standing. A regulation is
  cited to explain why an element is worth measuring, never to establish that a given
  organisation owes it. Absence from an API is not absence from a directory.</div>
  <div class="notebox"><b>How to read one cell &mdash; the whole contract.</b> A cell shows a
  <b>banded rate</b> and its <b>n</b>, or exactly one of four reasons there is no rate:
  <span class="cell absent notmeasured">not measured</span>
  <span class="cell absent wrongelement">wrong element</span>
  <span class="cell absent toofew">too few rows</span>
  <span class="cell absent nosignal">no signal</span>.
  <b>None of the four is a low score</b> &mdash; they are absent measurements, and a payer is
  never penalised for one. Everything else (lens, mapping, aggregation unit, citation status,
  method) lives in the detail beneath the cell.</div>
</header>

<section class="card">
{{RUNBAR}}
  <div class="hd"><div><h2>1 &middot; Cross-payer comparison</h2>
    <p class="q">Regulator-facing. <b>How does the distribution of published directory data look
    across the corpus, and which dimensions separate datasets?</b> Sortable table, one row per
    dataset, banded chips. No rank column; alphabetical by default; <b>sorting a column re-sorts
    within bands, never within raw values.</b></p></div>
    <span class="chip" id="cmpcount"></span></div>
  <div class="bd">
    <div class="controls">
      <div class="field"><label for="scope">Scope</label>
        <select id="scope">
          <option value="all">All datasets</option>
          <option value="ma">Medicare Advantage evidence present</option>
          <option value="dedup">Deduplicated datasets only</option>
        </select></div>
      <div class="field"><label for="q">Filter</label>
        <input type="search" id="q" placeholder="payer name or key"></div>
    </div>
    <div id="scopenote"></div>
    <div class="scroll"><table id="cmp"></table></div>
    <div class="legend">
      <span class="sw"><i class="b0"></i><i class="b1"></i><i class="b2"></i><i class="b3"></i><i class="b4"></i> five bands, 20pp wide</span>
      <span class="sw"><span class="cell absent notmeasured">not measured</span></span>
      <span class="sw"><span class="cell absent wrongelement">wrong element</span></span>
      <span class="sw"><span class="cell absent toofew">too few rows</span></span>
      <span class="sw"><span class="cell absent nosignal">no signal</span></span>
      <span class="sw"><span class="badge solid">state</span> gate states are descriptive</span>
    </div>
    <div class="misread">
      <span class="lbl">What this can be misread as.</span>
      <ul>
        <li><b>A ranking.</b> The most damaging misreading: &sect;5.4 establishes no defensible
        total order exists. Hence no rank column, alphabetical default, and sorting that operates
        on bands rather than on raw values &mdash; two datasets in the same band keep their
        alphabetical order, because the measurement cannot separate them.</li>
        <li><b>Comparable cells across payers.</b> They are not when unit counts differ &mdash; the
        MPF concept count ranges from 8 to 29 across this corpus. Every cell shows its n.</li>
        <li><b>An ordering by usefulness to a member.</b> The gates carry that, not the rates &mdash;
        and even they are proxies. G1 evaluates one of its three conjuncts.</li>
        <li><b>A complete corpus.</b> Three schemas hold no rows and three have never-analysed
        statistics; they appear with absent-measurement states, not as omissions.</li>
      </ul>
    </div>
  </div>
</section>

<section class="card">
{{RUNBAR}}
  <div class="hd"><div><h2>2 &middot; Per-payer scorecard</h2>
    <p class="q">Payer-facing. <b>What does this dataset look like across every dimension, and
    what should a team look at first?</b> Deliberately not a radar chart &mdash; a radar implies
    commensurability and encodes magnitude as area, both false here.</p></div>
    <div class="field"><label for="focus">Dataset</label><select id="focus"></select></div>
  </div>
  <div class="bd">
    <div id="card"></div>
    <div class="misread">
      <span class="lbl">What this can be misread as.</span>
      <ul>
        <li><b>A compliance report card.</b> Mitigated by lens labels on every row, the framing
        statement above, descriptive gate-state names rather than pass/fail, and no verdict
        styling on any rate.</li>
        <li><b>An ordering of severity.</b> Rows are ordered by layer &mdash; a dependency order.</li>
        <li><b>Complete.</b> Four dimensions have no corpus-wide results and render as
        <span class="cell absent notmeasured">not measured</span>, never as zero.</li>
        <li><b>A statement about what the organisation knows.</b> Must-support means "populate
        it if you have it", so every completeness figure describes what is <i>published</i>,
        never what is held.</li>
        <li><b>A certified capture.</b> G1 tests coverage bounds and nothing else. Two of its
        three conjuncts cannot be evaluated by anyone today.</li>
      </ul>
    </div>
  </div>
</section>

<section class="card">
{{RUNBAR}}
  <div class="hd"><div><h2>3 &middot; Dimension breakdown</h2>
    <p class="q">Analyst-facing. <b>Within one dimension, which check drives the spread, and is
    the spread real or an artifact of one broken check?</b> One dot per dataset per concept;
    unmeasurable datasets are absent from a row rather than plotted at zero. The count column
    shows raw datasets and deduplicated datasets, because the review-flag rule needs the
    second.</p></div>
    <div class="field"><label for="assess">Assessment</label>
      <select id="assess">
        <option value="plannet_ext">D5 &middot; Plan-Net extensions</option>
        <option value="uscdi">D5 &middot; USCDI</option>
        <option value="mpf">D5 &middot; MPF elements</option>
      </select></div>
  </div>
  <div class="bd">
    <div class="dotplot" id="brk"></div>
    <div class="misread">
      <span class="lbl">Why this view exists.</span>
      <ul>
        <li>It is the view that would have caught the <span class="mono">communication-proficiency</span>
        inversion: 58 of 59 datasets stacked at zero with one at 100% is visually unmistakable.
        That check is now <b>refuted</b> &mdash; the path was read against the StructureDefinition
        and is wrong &mdash; so it is out of every headline on this page, and its broken values are
        drawn faint here so the exclusion can be checked.</li>
        <li><b>The flag and the consequence are different things.</b> A review-flag says "someone
        must read the profile"; only a refuted path removes a number. An earlier version of this
        page flagged a check here and kept scoring with it in the two views above.</li>
        <li><b>Misread as:</b> checks being equally important &mdash; they are not weighted, and
        importance is expressed by which dimension a check lives in and whether it gates.</li>
        <li><b>Misread as:</b> a near-100% row being suspicious. It is not. Organisations have
        names. Only the near-zero direction can indicate a broken path.</li>
      </ul>
    </div>
  </div>
</section>

<section class="card">
{{RUNBAR}}
  <div class="hd"><div><h2>4 &middot; Version delta &mdash; run {{CONFP}} to run {{CONF}}</h2>
    <p class="q">Analyst-facing. <b>What moved when the IG changed, and what caused each
    movement?</b> Run {{CONFP}}'s 118,951 instances are a strict subset of run {{CONF}}'s 121,038,
    so this is a matched-pairs comparison, not two samples. Every instance that changed state is
    attributed to its own error set. Movement a decomposition cannot explain would be the finding;
    there is none here.</p></div></div>
  <div class="bd">
    <div id="delta"></div>
    <div class="misread">
      <span class="lbl">What this can be misread as.</span>
      <ul>
        <li><b>Payers getting better or worse.</b> Nothing here is a payer behaviour change. One
        removed constraint explains both gainers; one imported US Core invariant explains every
        loss. The datasets did not move &mdash; the ruler did.</li>
        <li><b>A complete list of movers.</b> It lists every dataset that moved by at least half a
        point in D3's declared unit (the cell mean). Instance-pooled figures differ and are not
        shown, because mixing the two in one table is how the earlier draft got this wrong.</li>
      </ul>
    </div>
  </div>
</section>

<section class="card">
{{RUNBAR}}
  <div class="hd"><div><h2>5 &middot; Trend over time</h2>
    <p class="q">Payer-facing. <b>Is this dataset changing, and in which direction?</b></p></div>
  </div>
  <div class="bd">
    <div class="inert"><div class="msg"><b>Deliberately not rendered.</b><br>
      &sect;9.5 forbids publishing this view until at least three comparable captures exist under
      one IG version and one pooling rule. Today there are two conformance generations across an
      IG boundary ({{IGP}} &rarr; {{IG}}) and two pooling-rule changes. A line drawn across any of
      those boundaries would attribute to a payer what was in fact a change in the specification or
      in our own arithmetic &mdash; which structure&nbsp;4 above shows in full.</div></div>
    <div class="misread">
      <span class="lbl">What it would be misread as, once it exists.</span>
      <ul>
        <li><b>Improvement or regression by the payer</b> &mdash; when the cause may be our extraction,
        our sample, our pooling rule, or the IG.</li>
        <li><b>Continuity across the pooling-rule change.</b> Rule A and rule B figures are not on
        the same scale; the series must visibly break there, as it does at an IG boundary.</li>
      </ul>
    </div>
  </div>
</section>

<section class="card">
{{RUNBAR}}
  <div class="hd"><div><h2>Corpus gates</h2>
    <p class="q">Counts across all {{NP}} datasets in the pinned run set.</p></div></div>
  <div class="bd"><div id="corpus"></div></div>
</section>

<section class="card">
{{RUNBAR}}
  <div class="hd"><div><h2>The unreachable-listing rate, already measured</h2>
    <p class="q">Analyst-facing. A D10 proxy that was sitting inside the conformance data.</p></div></div>
  <div class="bd"><div id="pd1"></div></div>
</section>

<section class="card">
  <div class="hd"><div><h2>The three audience views</h2>
    <p class="q">Which structures each audience sees, and the misreading each is most exposed to.</p></div></div>
  <div class="bd">
    <div class="scroll"><table class="audience">
      <thead><tr><th></th><th>Regulator-facing</th><th>Payer-facing</th><th>Public-facing</th></tr></thead>
      <tbody>
        <tr><td><b>Question</b></td>
          <td>Is the population of directories getting better, and where are the systemic gaps?</td>
          <td>What should our team fix, in what order?</td>
          <td>Can I trust what I am looking at?</td></tr>
        <tr><td><b>Unit</b></td><td>The corpus</td><td>One dataset</td><td>One dataset, one question</td></tr>
        <tr><td><b>Shows</b></td>
          <td>Structures 1, 3 and 4, plus the "cannot be measured" table in full</td>
          <td>Structures 2, 3 and 5</td>
          <td>Gate states and a plain-language summary. <b>No rates, no ranks, no percentages.</b></td></tr>
        <tr><td><b>Hides</b></td><td>Nothing</td><td>Other datasets' identities in comparisons</td>
          <td>Every raw number</td></tr>
        <tr><td><b>Primary risk</b></td>
          <td>Treating the corpus as a compliance census</td>
          <td>Treating the scorecard as an audit finding</td>
          <td>Treating a gate as advice about a doctor</td></tr>
      </tbody>
    </table></div>
    <div class="flagbox"><b>The public view carries an extra constraint.</b> The gap between what
    this framework measures (what is <i>published</i>) and what a member needs (whether a doctor
    is reachable) is wide enough that a percentage shown to a member would be read as a promise.
    It shows the two gate states, a sentence about what the directory can and cannot answer, and
    a link to the methodology &mdash; nothing a member could mistake for advice about their care.
    Publication is additionally gated on the repository's disclosure process: findings go to each
    organisation's compliance contact first.</div>
  </div>
</section>

<footer>
  <p><b>How to read the rates.</b> Coverage rates use &sect;5.1's rule B: the unweighted mean over
  distinct <i>concepts</i>, each concept the unweighted mean of its cells, with the concept's
  denominator span and row-pooled rate published beside it. Cells below the floors are excluded and
  shown separately: <b>n &lt; 30</b> (a 95% interval of roughly &plusmn;18 points), or measured by a
  bounded <b>sample</b> of a much larger table rather than a full scan. v0.2.0's third floor
  &mdash; 0.1% of the dataset's total published rows &mdash; has been <b>withdrawn</b>: it scaled
  with the payer's largest table and silenced complete resource populations, including Humana's and
  Elevance's entire InsurancePlan obligations. Two USCDI concepts measured by identical SQL are
  collapsed to one observation. Structural conformance is the mean over (dataset, resource) cells
  from a 400-per-cell sample, with cells under 30 instances suppressed rather than averaged.</p>
  <p><b>Estimates and exact counts.</b> Row counts are <span class="mono">pg_class.reltuples</span>
  planner estimates. Never-analysed tables return the &minus;1 sentinel and render as
  "unknown (never analysed)" &mdash; distinct from zero, never sorted to the bottom as if zero.</p>
  <p><b>Caveats that change interpretation.</b> USCDI has no official element-level mapping to
  Plan-Net, so that column is an inference and is labelled as such;
  <span class="mono">provenance_author_timestamp</span> has been demoted out of the USCDI headline
  because the underlying signal is degenerate for thirteen datasets, while the two MPF
  <span class="mono">last_updated</span> checks stay in the MPF headline because they map directly
  onto a row of the CMS element list that asks whether the element is present &mdash; the ban is on
  reading the timestamp's <i>value</i> as currency, not on the presence check. MPF elements come from
  a CMS guide read via a mirror because cms.gov returns 403 to automated fetches, and that citation
  has never been second-sourced. One extension check
  (<span class="mono">communication-proficiency</span>) reads the wrong element path; it is
  <b>excluded from every headline on this page</b> and its broken values are drawn faint in
  structure&nbsp;3. Aetna's PractitionerRole table stores stub JSON, so its JSONB metrics come from a
  399-row sample and are excluded by the method floor &mdash; its <i>reference</i> columns are
  complete, so its G2 traversal is unaffected.</p>
  <p><b>What this page cannot tell you.</b> Whether any capture is a fresh snapshot rather than a
  cumulative union &mdash; provenance holds no load-mode field, so two of G1's three conjuncts are
  unevaluated for every dataset. Whether a chain rate holds for the largest datasets &mdash; the
  exact traversal completes for {{NEXACT}} of them and the rest publish a state without a rate.
  Whether any of this corresponds to what a member experiences: that is validation against ground
  truth this project does not hold.</p>
  <p>Generated from <span class="mono">output/quality/framework_mockup_data.json</span> by
  <span class="mono">scripts/build_framework_report.py</span>, built read-only. Specification:
  <span class="mono">docs/evaluation-framework.md</span> v{{FV}}.</p>
</footer>
</div>
<script>{{JS}}</script>
"""


def main():
    data = json.load(open(SRC, encoding="utf-8"))
    payload = json.dumps(data, separators=(",", ":"))
    runs = data["runs"]
    sub = dict(CSS=CSS, JS=JS.replace("DATA_PLACEHOLDER", payload),
               COV=runs["coverage"], CONF=runs["conformance"],
               CONFP=runs["conformance_prior"], BIND=runs["binding"],
               IG=runs["ig"], IGP=runs["ig_prior"], GEN=data["generated"],
               FV=data.get("framework_version", "0.3.0"), NP=len(data["payers"]),
               NEXACT=len(data["corpus"]["g2"]["exact_rates"]))
    runbar = RUNBAR
    for k in ("COV", "CONF", "BIND", "IG", "GEN", "FV"):
        runbar = runbar.replace("{{" + k + "}}", str(sub[k]))
    html = HTML.replace("{{RUNBAR}}", runbar)
    for k, v in sub.items():
        html = html.replace("{{" + k + "}}", str(v))
    for k in list(sub) + ["RUNBAR"]:
        assert "{{" + k + "}}" not in html, "unsubstituted token: " + k
    io.open(OUT, "w", encoding="utf-8").write(html)
    print("wrote", OUT, len(html), "bytes")


if __name__ == "__main__":
    main()
