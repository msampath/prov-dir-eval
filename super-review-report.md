# Super-review — prov-dir-eval

**Date:** 2026-07-28
**Scope:** whole working tree of `main` at commit `542d065` (34 Python source files, ~2,600 statements, plus `config/endpoints.yaml`, which drives all runtime behaviour and was reviewed as source).
**Method:** 22 review agents (10 core lenses + data-integrity + api-contract + 9 full-file audit partitions + 6 solo auditors) → 215 deduped findings → adversarial validation by exactly two validators → tech-lead reproduction, fixes, and retest.

---

## 1. Files-read accounting (100%)

Every source file was read and evaluated. The engine's own bookkeeping reported three files as "unevaluated" (`extract.py`, `pipeline.py`, `dashboard.py`), but this is a double-counting artifact: each had a dedicated solo auditor that produced findings (9, 11 and 6 respectively), and the same field reported `evaluatedFiles: 55 of totalFiles: 35`. The 100% guarantee holds; no file was left unread.

## 2. Findings

Raw: **215 deduped** — 1 critical, 33 high, 81 medium, 91 low, 9 info.
Validated: the **115 critical/high/medium** findings were split by file and given to two adversarial validators.
Not validated: **91 low + 9 info** were deliberately left unverified and are listed as deferred rather than presented as confirmed.

**Validator outcome:** every finding examined was a factually accurate reading of the code — no fabrications. The value of validation was in re-grading, not rejection:

- **1 impact refuted.** `metrics.py:91` claimed the address score "rewards missing state over present-but-imperfect". Validator 1 showed `addr_complete_pct` already counts city AND state over the total, so the claimed inversion does not occur; only a reporting inconsistency remains.
- **1 finding rejected outright.** `extract.py:571` (daterange denominator scoped by `base_params`) is the project's documented, deliberate convention — `config.py:148-151` explicitly states the denominator is scoped to match the pull for an honest coverage %.
- **Several re-graded down.** The single "critical" was re-graded to **high** by me and independently by Validator 2: it is a performance cliff in an offline reporting command, not data loss or a security hole.
- **Five confirmed-but-latent** (real in code, unreachable with the current manifest) — see §6.

## 3. Confirmed and FIXED

| # | Severity | Location | Defect | Fix | Verified by |
|---|---|---|---|---|---|
| 1 | **high** | `etl/extract.py:104` | `ResourceSink._flush` had no lock. `id_chain`/`id_read` call `add()` from many concurrent tasks sharing **one** psycopg connection and **one** TEMP stage table, so a second flush could `TRUNCATE` the stage between the first flush's `COPY` and its `INSERT…SELECT` — silently discarding up to 5,000 resources while reporting success. Live on humana, kaiser, uhc, regence, medica, health_advantage_ar. | `asyncio.Lock` held across the buffer swap and the awaited flush, in `ResourceSink` and per-type in `MultiSink`. | new test asserting flushes never overlap |
| 2 | **high** | `etl/loader.py:155` | `ON CONFLICT … DO UPDATE` over an un-deduplicated batch raises Postgres `21000 CardinalityViolation` when one command hits the same key twice. Duplicates are routine: an `id_chain` sweep returns a role once per network it belongs to. The error then cascaded (no rollback → `InFailedSqlTransaction` → bare `gather` → whole run dead, no provenance). | `SELECT DISTINCT ON (payer_id, id) … ORDER BY payer_id, id` in the update branch only; `DO NOTHING` keeps the cheaper plain select. | 2 new tests (both branches' SQL) |
| 3 | **high** | `etl/extract.py:621` | A bare search refused with 401/403/429 fell through to the partition sweep, firing ~88 further requests at a host that had just refused us — the opposite of the project's stated network etiquette. | Return `method="blocked"` with an explanatory note; no sweep. | code read + `_is_retryable` tests |
| 4 | **high** | `etl/extract.py:432` | Per-bucket pagination failures were discarded (`pages, _ok, _, _added`). A mid-stream truncation does not raise, so it never reached `fetch_errors` either — meaning a sweep in which **every bucket truncated** was recorded `status="ok"`. For a project whose purpose is measuring directory completeness, truncated pulls were indistinguishable from complete ones. | Count into `stats["truncated_buckets"]`, fold into the note using the literal `"pagination stopped"` wording that `classify_status` matches on. | code read |
| 5 | **high** | `etl/pipeline.py:403` | Bare `asyncio.gather` — one unit's DB error killed the run before `etl_summary.json` and the per-payer results were written, discarding every other unit's work. | `return_exceptions=True`; exceptions logged and mapped to an `{"status":"error"}` result so the summary is still produced. | suite |
| 6 | **high** | `etl/pipeline.py:230` | No `conn.rollback()` anywhere in the codebase. A failed upsert left the connection in an aborted transaction; every later statement (including `_finalize`'s count) then failed, masking the real error. | `try/except` around `upsert_batch`/`commit` that rolls back before re-raising. | suite |
| 7 | **high** | `quality/evaluate.py:52` | Returned a hard-coded `100.0` when a resource declares no required elements. `rules.py` has **no** `required=True` checks for PractitionerRole, HealthcareService or OrganizationAffiliation — the largest tables in a Plan-Net directory — so each was handed a free 100 at the heaviest row-count weight, inflating the project's headline composite. | Return `None` so `wmean()` skips it and the score renormalises honestly. | new test |
| 8 | **high** | `quality/integrity.py:110` | `l.id = ANY(SELECT … FROM unnest(r.location_refs))` is a correlated sublink referencing both the outer and inner relation, so it cannot be hashed into an anti-join; it degrades to a nested loop rescanning `practitioner_role` for every unreferenced location. Effectively O(n×m) on Regence (18M) / Health Advantage (21M). | Flattened to a lateral `unnest` with a plain equality on `l.id`, which Postgres can plan as a hash anti-join. | reproduced by reasoning; both validators concur |
| 9 | **medium** | `logging_setup.py:36` | The redaction filter only rewrote `record.msg`, so anything passed as a `%`-arg was emitted verbatim — which is exactly where secrets are, since call sites log formatted exception text and `FhirError` embeds the full request URL. The module's defence-in-depth promise was vacuous. | Render `getMessage()` into `msg`, clear `args`, then redact. | new test (token in an arg is masked) |
| 10 | **medium** | `http_client.py:59` | One limiter per host, built from whichever endpoint happened to construct it first — silently discarding a co-hosted sibling's **stricter** politeness. Reproduced: `amerihealth_laex` got concurrency 8 instead of its declared 2. Three hosts affected. Made the effective request rate depend on scheduler order. | Resolve the strictest `max_concurrency` / `min_request_interval` across **all** manifest endpoints sharing the host. | new test, both build orders |
| 11 | **medium** | `quality/dashboard.py:361` | `software_name`/`software_version`/`fhir_version` are copied verbatim from a payer's `/metadata` and interpolated into HTML unescaped — untrusted third-party input in a generated page. (The sibling module already escapes.) | `html.escape` on all three. | code read |
| 12 | **medium** | `quality/collection_status.py:164` | A hard-coded "All accessed — no auth" card contradicted the tier dots on the same rows: 8 payers (>half the row count, incl. Regence 18M and Medica 13.8M) are `public-token` and mint a token per request. | Derive the access mix from the data. | suite |
| 13 | **medium** | `etl/pipeline.py:226` | `transform_resource` raises bare `ValueError`/`TypeError` on real malformed payer data (`position.latitude=""`, a null inside `address.line`); these escaped the `TransformError` guard and cost the whole in-flight 5,000-row batch instead of one row. | Catch `(ValueError, TypeError)` and count as a transform error. | new test |
| 14 | **low** | `quality/collection_status.py:145,150` | Two `E741` ambiguous variable names — the repo did not lint clean on `main`. | Renamed. | `ruff check src tests` clean |

**Also fixed from my own tech-lead review** (not agent-sourced): nothing further — my one independent finding (per-endpoint `user_agent` not reaching token-mint requests, `auth/strategies.py:108,156`) is **latent** and deferred; see §6.

### 3b. Second round — defects found in the FIXES themselves

The retest pass earned its keep: both validators independently found a bug in fix #3, and one found a regression fix #3 introduced. All were corrected and re-verified.

| Defect in the fix | Found by | Correction |
|---|---|---|
| **The 429 guard was dead code.** The `401/403/429` check sat in `except FhirError`, but a 429 never becomes one: `_request` calls `raise_for_status()` inside the retry loop and tenacity re-raises `httpx.HTTPStatusError`, which escapes before `get_json` can wrap it. Validator 2 reproduced it: 403 → 0 partition requests (fixed), **429 → 88 partition requests (unchanged)**. Rate limiting — the case that most needs backing off — was still unhandled. | both validators | Extracted `_REFUSED_STATUSES`/`_blocked()` and applied the guard in **both** exception handlers, keyed off `response.status_code` in the generic one. Pinned by a parametrised test over 401/403/429 asserting ≤4 total requests. |
| **Regression introduced by the fix.** The new `method="blocked"` had no case in `classify_status`, so a refused endpoint that already held rows from a prior run recorded `status="ok"` — strictly worse than the `"skipped"` it produced before. | Validator 2 | `blocked` (and the new `partition-failed`) now classify as `error`. Pinned by a test. |
| **Fix #4 covered 1 of 3 call sites.** `_daterange_sweep` and `extract_resource`'s partition loop still discarded the truncation note, leaving *every live daterange config* (humana, capital_blue, bcbs_az) able to truncate in every window and still report `ok`. | Validator 2 | Both remaining sites now count truncations and fold the `"pagination stopped"` wording into the note. |
| **All-partitions-failed was labelled `unsupported`**, which reads as "this resource isn't served" and classified as `skipped` — indistinguishable from a genuine outage. | Validator 2 (orig. finding) | Now `partition-failed` → classified `error`. |
| **The harvest path was left behind.** Fixes #1/#5/#6/#13 hardened `_extract_and_load`, but `_harvest_one` retained every one of those failure modes — no rollback, no `ValueError/TypeError` guard, a bare `await sink.close()`, connections closed only on the happy path, and it rebuilt the stats dict so `include_sweep`'s `pages`/`fetch_errors` were dropped (a partly-failed sweep recorded `ok`). `harvest` is the *more expensive* command. | Validator 1 | Rollback + broadened transform guard + guarded `sink.close()` + `finally`-closed connections + stats passed through intact. |

## 4. Verification gate

| Check | Result |
|---|---|
| `pytest -q` | **84 passed** (was 49) |
| `ruff check src tests` | **All checks passed** (was 2 errors on `main`) |
| Coverage | **41%** (was 36%) |
| Live payer HTTP | none issued — constraint honoured |
| DB mutations | none — all new tests use fakes/mocks |

New tests: `tests/test_review_fixes.py` (15) and `tests/test_http_and_extract_units.py` (20).

**Retest scorecard.** Both validators re-checked their own confirmed findings against the fixed code. Of the items I claimed to fix: **all passed on re-examination**, except the four defects listed in §3b, which were then corrected and re-verified. The remaining `fail` verdicts in their returns are the deferred set (§6) — correctly reported as unfixed, not as regressions.

**One caveat both validators raised and I am carrying forward honestly:** the `integrity.py` anti-join rewrite is reasoned, not measured. Neither they nor I could run `EXPLAIN` (no DB connections allowed in this review), so the plan change should be confirmed with one `EXPLAIN` on a small payer schema before it is trusted on Regence/Medica.

## 5. Coverage — **below target, stated plainly**

The skill's bar is >80% of testable logic. **This review reached 41%, and I did not meet the bar.** I chose to spend the available effort on confirming and fixing the high-severity defects rather than on bulk test authoring, and I am flagging the shortfall rather than letting it read as done.

Where the 41% sits, and the honest reason for each gap:

| Module | Cov | Why |
|---|---|---|
| `http_client.py` | 65% (was 33%) | raised this pass — UA override, quirks, retry classification |
| `loader.py` | 58% (was 28%) | raised this pass — both upsert branches' SQL |
| `logging_setup.py` | 77% | redaction now pinned |
| `transform.py` | 74% | pure, well covered |
| `extract.py` | 50% | large; the partition/daterange/include paths need a fake-client harness |
| `pipeline.py` | 22% | orchestration; needs a DB fake to test meaningfully |
| `cli.py`, `dashboard.py`, `smoke.py`, `probe.py`, `quality/runner.py`, `etl/coverage.py`, `inventory.py` | 0% | entrypoints and report renderers; each needs either a DB fake or a live-HTTP fake |

**Highest-ROI next step:** a shared fake-connection fixture plus a fake `FhirClient`. Those two harnesses would unlock `pipeline.py`, `extract.py`'s remaining strategies, and the four 0% runners — which is where the 80% actually lives. Padding line coverage on the renderers would raise the number without raising confidence, so I did not do it.

## 6. Confirmed but DEFERRED (not fixed)

**Latent** — real in code, unreachable with today's manifest:
- `auth/strategies.py:108,156` — the per-endpoint `user_agent` override does **not** reach token-mint requests, which post via the raw session client. All four UA endpoints are currently `strategy: none`, so nothing fires today; it will bite the first payer needing both a browser UA and OAuth. *(My own finding.)*
- `pipeline.py:386` / `:353` — `unconfirmed-auth` is never returned (no `unknown`-status endpoint declares secrets) and no endpoint is `blocked`, so both gaps are inert.
- `config.py:237/173/269` — no duplicate-key validator, unconstrained `resource_subset`, and `plannet_resources` defaulted to `[]` despite being declared required. All clean in the current manifest; the first is now pinned by a test.
- `extract.py:485` — `id_chain` ignores `base_params`; no endpoint combines them.

**Real and reachable, deliberately deferred** (each needs a decision or a larger change than a review should make unilaterally):
- **`uhc` / `uhc_optum` are the same endpoint** (identical `base_url`), both `status: known` — `--all-known` pulls the same server twice into two schemas and double-counts it in any roll-up. Confirmed by four lenses and both validators. Deleting a populated payer schema is destructive, so it needs the owner's call.
- **Analysis commands default to 7 MVP payers.** `coverage_report()` hardcodes `manifest.mvp()` and `score`/`coverage` expose no `--all-known`, so the progress meter and league table cover 7 of the ~55 loaded payers.
- `hcsc` and `bcbs_ks` are `status: known` despite documented edge blocks; `navitus` is `known` despite a comment saying it is out of scope. No endpoint anywhere uses `status: blocked`.
- **UA policy is a values decision, not a bug.** Four endpoints send a verbatim Chrome UA with no project token, and one manifest comment advises running from a different network to evade a Cloudflare block. Validator 1 correctly notes the repo states no explicit "no evasion" rule — so this needs the owner to set the policy, then the code follows.
- `health_advantage_ar` OrgAffiliation runs ~469k requests at concurrency 6 with no `min_request_interval`.
- Partition sweep budget is consumed by the first partition value (12 of 52 states never queried at `PARTITION_PAGE_CAP=40`); `max_pages` is ignored by `id_read`; all-partitions-failed is labelled `unsupported` (reads as "not served" rather than "outage"); `include_sweep` does not propagate truncation notes.
- Dead code: `http_client.iter_bundles/iter_resources/_next_link`, `loader.truncate/bulk_load`; stale docstrings in `loader.py` and README describing a drop-and-reload model that no longer exists.
- `.env.example` drift (missing `HEALTHPARTNERS_*`, `MIHIN_API_KEY`; several unreferenced keys; defaults disagreeing with `config.py`); `docs/` cited 11 times in the manifest but absent from the repo; README says 87M/44 payers (now ~118M/55).
- No migration path for the ~55 existing payer schemas (`create_all(checkfirst=True)` never emits `ALTER`), which is what makes the `raw_hash` and unused-index findings expensive to act on.
- `quality/runner.py` shares one connection across all payers with no rollback; `cli.py` returns 0 unconditionally for 5 subcommands; `probe.py:87` credits a SHALL param that was never actually sent.

**Not validated:** 91 low + 9 info findings.

## 7. Recommended follow-ups, in order

1. Decide `uhc`/`uhc_optum` (drop one after folding in its `OrganizationAffiliation` rows) — it currently corrupts any per-payer roll-up.
2. Add `--all-known`/`--subset` to `coverage` and `score` so the evaluation covers the data actually collected.
3. Build the fake-connection and fake-client fixtures, then take coverage to 80% where it is meaningful.
4. Set the User-Agent / blocked-host policy explicitly, and make `status: blocked` real for `hcsc` and `bcbs_ks`.
5. Fix the token-mint UA gap before onboarding a payer that needs both.
6. Give the partition sweep a per-value budget so late states are not starved.
