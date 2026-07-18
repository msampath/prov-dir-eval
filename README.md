# prov-dir-eval — CMS-9115-F Provider Directory Evaluation

An independent, best-effort data-exploration project that connects to US payers'
**public Da Vinci PDex Plan-Net** FHIR provider directories (published under the CMS
Interoperability & Patient Access Final Rule, **CMS-9115-F**), ingests them into
localhost Postgres, and evaluates the data — retrievable completeness (coverage),
IG conformance, and referential integrity.

> **Disclaimer.** This is a personal / educational data-exploration project.
> Findings are point-in-time observations of public endpoints, may reflect transient
> conditions or our own tooling, and are **not** legal, compliance, or professional
> advice, nor an official assessment. Provided as-is, no warranty. Any assertion that
> a payer/vendor endpoint behaves a certain way is an *observation of that endpoint*,
> not a verdict about a party. Compliance determinations rest with the payer and CMS.

## What it does

- **Discovers and ingests** public Plan-Net directories across many payers and vendor
  platforms (1upHealth, HealthSparq/Kyruus, Innovaccer, Opala, MuleSoft, AWS API
  Gateway, Apigee, InterSystems HealthShare).
- **Measures coverage honestly** — every pull records `coverage_pct` = rows landed vs
  the server's own `_summary=count`, so completeness is *measured, not assumed*.
- **Bypasses server-side search limits** with an adaptive, count-guided partitioner and
  a reference-graph harvest (details below), reaching resources a bare paginated search
  can't.
- **Evaluates** IG conformance and referential integrity on the landed data.

Current snapshot: **~87M resources across 44 payer directories** (see
`/provdir-acquisition` dashboard / `provdir status-dashboard`). This grew from a
7-payer, ~5.9M-row MVP; the phased build (scaffolding → conformance → schema → ETL →
quality → score → dashboard) is complete, and the active focus has been maximizing
retrievable coverage before analysis.

## Architecture

```
src/provdir/
  config.py            # .env settings + endpoints.yaml manifest (typed)
  logging_setup.py     # JSON logs to output/logs/ + console, secret redaction
  http_client.py       # async httpx + tenacity retry + per-endpoint rate/timeout limits
  auth/                # auth strategies: none, client-id header, oauth2 client-creds,
                       #   api-key header, HealthSparq public-token, client-id/secret headers
  conformance/         # IG fetch, declared search-param matrix, live probes
  models.py            # Postgres schema (SQLAlchemy Core, JSONB-hybrid)
  etl/
    extract.py         # adaptive extractor (partition modes below) + id_chain/id_read/include
    pipeline.py        # run_etl / run_reference_harvest orchestration, status classification
    loader.py          # streaming COPY -> stage -> INSERT ... ON CONFLICT (upsert-optional)
    coverage.py        # `provdir coverage` scoreboard (landed vs server_total)
    specialties.py     # NUCC specialty ValueSet (671 codes) for the role sweep
  quality/
    collection_status.py  # acquisition dashboard (row counts + measured coverage)
    ...                   # completeness, integrity, scoring, evaluation dashboard
config/endpoints.yaml  # the endpoint manifest — source of truth for every payer
migrations/            # Alembic
output/, reference/    # generated artifacts + auto-fetched IG (git-ignored)
```

### Storage model — JSONB-hybrid, one schema per payer

Each payer gets its own Postgres schema (`humana.organization`, `medica.location`, …)
holding the 8 Plan-Net resource tables. Each table stores the full FHIR resource in a
`resource` JSONB column plus extracted/indexed columns (npi, address_state, lat/long,
`*_ref` reference columns) that back joins, quality checks, and integrity queries.
Cross-payer metadata (`public.provenance`, `public.data_quality_score`) lives in shared
`public`. A per-payer reload is an in-schema `TRUNCATE` — sources stay isolated.
References are stored but **not** enforced as DB foreign keys; integrity is *evaluated*,
not rejected.

Ingestion is **streaming and resumable**: the extractor streams resources to the loader
in ~5k-row batches (`COPY` → temp stage → `INSERT … ON CONFLICT (payer_id, id)`), committed
per batch, so a killed multi-hour pull keeps everything landed and resumes on re-run. The
conflict action is `DO NOTHING` by default (insert-only, additive); pass `--upsert` to use
`DO UPDATE` (refresh changed records in place).

## Extraction strategies

Many directories cap or reject an unfiltered search. The extractor selects a strategy per
resource from each endpoint's `quirks.adaptive` config:

| Mode | How it works | Used when |
|---|---|---|
| **bare** | plain paginated search | server pages fully |
| **prefix** | count-guided `name`/`family` prefix subdivision (`a`, `ab`, …) | a text param subdivides |
| **values** | fixed value list (e.g. `address-state`) or the specialty ValueSet | a coded/enum param |
| **daterange** | `_lastUpdated` time-window bisection until each bucket < cap | timestamps subdivide; bypasses page-offset ceilings |
| **id_chain** | `?<ref>=<id1>,<id2>,…` over ids we already hold (comma = FHIR OR, batched) | a resource is only reachable by reference (e.g. roles a bare search under-serves) |
| **id_read** | `GET {Type}/{id}` for ids referenced elsewhere (not paginated) | a resource has no working search partition |
| **include** (`provdir harvest`) | sweep roles by specialty with `_include` to pull roles + practitioners + locations + orgs + services in one pass | large directories with no usable search partition (Regence-class) |

Coverage for id_chain/id_read/include is bounded by the reference subgraph we hold (a
measured residual — e.g. a practitioner with no PractitionerRole is unreachable and
recorded as such), which is itself a data-fidelity signal.

**Access tiers** (derived from each endpoint's auth strategy): *open* (no auth),
*public-token* (HealthSparq mints a public token from a per-brand code), *gated*
(registration / OAuth / API key). The vast majority ingested here are open or
public-token.

## Setup

Prereqs: Python 3.11+, localhost Postgres 14+ (tested on 18), outbound HTTPS.

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev]"
copy .env.example .env          # then edit .env (POSTGRES_PASSWORD at minimum)
.venv\Scripts\python.exe -m alembic upgrade head
.venv\Scripts\python.exe -m provdir.cli db check
```

Secrets live **only** in `.env` (git-ignored); `output/` and `reference/` are git-ignored
and regenerable.

## Runbook

All commands are `python -m provdir.cli <command>` (or the `provdir` console script).

| Command | Output |
|---|---|
| `provdir smoke` | liveness of the endpoint set → `output/smoke_results.json` |
| `provdir conformance` | CapabilityStatement vs Plan-Net IG → `output/conformance/` |
| `provdir etl [--subset a,b] [--resources R] [--upsert] [--max-pages N]` | rows in Postgres + `output/etl_summary.json` |
| `provdir harvest --subset <payer>` | reference-graph (`_include`) role sweep for a big directory |
| `provdir coverage` | per-(payer, resource) landed-vs-server_total scoreboard |
| `provdir status-dashboard` | acquisition dashboard → `output/collection_status.{html,json,_widget.html}` |
| `provdir quality` | IG conformance + referential integrity per payer |
| `provdir score` | composite `data_quality_score` |
| `provdir dashboard` | evaluation site → `output/site/index.html` |
| `provdir db check` | server version + schema list |

Typical acquisition loop for one payer:

```powershell
.venv\Scripts\python.exe -m provdir.cli etl --subset banner            # bare/adaptive pull
.venv\Scripts\python.exe -m provdir.cli harvest --subset regence       # reference-graph harvest (big)
.venv\Scripts\python.exe -m provdir.cli coverage                       # check completeness
.venv\Scripts\python.exe -m provdir.cli status-dashboard               # visualize
```

Add credentialed endpoints by filling `.env` and widening the subset (e.g.
`--subset hcsc` once its keys are set).

## Known limitations (measured)

Each is an observed *endpoint* behavior, not a tool limit or a verdict about a party.
`coverage_pct` in `public.provenance` makes completeness explicit.

- **Search under-serving vs. real totals** — some servers' bare `PractitionerRole` search
  returns far fewer roles than exist; the reference-graph harvest / id_chain recovers them
  (e.g. Humana ~11M roles reached by chaining on practitioner ids). Others genuinely serve
  few roles because a large share of practitioners are published with no role at all — a
  content/referential observation, not a collection gap (verified server-side).
- **Page-offset ceilings** — a bare search can cap below the server's own count (e.g. an
  Organization search capping ~1.5M of ~1.86M). `_lastUpdated` daterange bisection recovers
  the remainder where timestamps subdivide; `name`/`address-state` filters that *time out*
  are unusable for partitioning on those servers.
- **Edge / bot protection** — some hosts are fronted by Akamai/Imperva; aggressive request
  rates trip IP-level blocks (observed on Humana under high concurrency), and some
  credentialed developer endpoints return edge errors to all clients (observed on HCSC).
- **Broken pagination** — a payer whose `next` link doesn't advance is effectively
  unretrievable via standard paging; reported to the operator and deprioritized.
- **Referential integrity** is only meaningful on complete per-payer data; capped or
  sampled ingests inflate dangling-reference counts.

## Versioning & operations

Secrets only in `.env` (git-ignored); logs are redacted. Per-payer reload = in-schema
`TRUNCATE`, never cross-schema. The DB is JSONB-heavy (~2 GB per ~1M resources); provision
disk accordingly. `VACUUM (ANALYZE)` after heavy upsert/re-pull activity to reclaim dead
tuples.

## License

Licensed under the **GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later)** —
see [`LICENSE`](LICENSE). The AGPL's network-use clause (§13) means that if you run a
modified version of this software as a network service, you must offer its source to the
users of that service.
