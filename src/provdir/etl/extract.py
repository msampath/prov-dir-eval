"""Phase 5.1/5.4 — Paginating extractor with partition fallback.

Primary strategy is a bare paginated search (``GET {Resource}?_count=N`` then
follow ``link[next]``). Real-world Plan-Net servers vary, so the extractor is
defensive:

* A server that rejects unfiltered searches (200 OperationOutcome, or 400/403)
  triggers a *partition* sweep — by ``address-state`` for place-bearing
  resources, else by ``name`` initial — de-duplicated by logical id.
* A 404 (or 4xx where no partition strategy exists) marks the resource
  ``unsupported`` for that payer rather than failing the run.
* Mid-stream pagination failures (e.g. Banner's auth-gated ``next`` links) stop
  pagination and keep what was collected, recording a partial note.

This keeps runs resumable in the logical sense (partitions are deterministic
keys, not ephemeral ``next`` URLs).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from ..config import Endpoint
from ..http_client import FhirClient, FhirError
from ..logging_setup import get_logger
from .specialties import INDIVIDUAL_GROUP_SPECIALTIES

log = get_logger(__name__)

# --- resume/checkpoint support (bare pagination only) ----------------------
# On resume, rewind this many pages before the checkpoint URL for offset-style
# servers, so a partially-committed final batch is safely re-walked (upsert
# dedupes the overlap). Non-offset (opaque/stateful) URLs are retried verbatim.
RESUME_REWIND_PAGES = 5
# Offset params whose value we can arithmetic-rewind. Elevance uses `_offset`;
# HAPI `_getpages` sessions expose `_getpagesoffset` (but those are stateful and
# usually not durably re-enterable — the retry-once-then-fresh path covers that).
_OFFSET_PARAMS = ("_offset", "_getpagesoffset")
# Emit a progress line every N pages so multi-hour bare pulls aren't silent.
HEARTBEAT_EVERY_PAGES = 50

# Named value lists a `values`-mode partition can reference via cfg.values_ref
# (keeps large IG code lists out of the yaml).
VALUE_LISTS = {"specialties": INDIVIDUAL_GROUP_SPECIALTIES}

US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
    "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
    "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC", "PR",
]
NAME_INITIALS = list("abcdefghijklmnopqrstuvwxyz0123456789")

# Even in a "full" run (max_pages=None), bound partition sweeps so a tiny-page,
# filter-only server (e.g. Excellus at 25 rows/page across 53 states) can't stall
# the pipeline. Bare (unfiltered) pagination remains unbounded in full runs.
PARTITION_PAGE_CAP = 40

# Resources that carry an address (so address-state partitioning is meaningful).
STATE_PARTITIONABLE = {"Location", "Organization", "Practitioner", "HealthcareService"}
# Resources with a name we can partition by initial.
NAME_PARTITIONABLE = {"Organization", "Practitioner", "Location", "HealthcareService", "InsurancePlan"}


def _is_bundle(payload: dict) -> bool:
    return payload.get("resourceType") == "Bundle"


def _oo_note(payload: dict) -> str:
    issue = (payload.get("issue") or [{}])[0]
    return (issue.get("details") or {}).get("text") or issue.get("diagnostics") or payload.get("resourceType")


# The host refused us outright (auth, WAF/bot-block, or rate limit). Falling
# through to the partition sweep would fire ~88 more requests at a server that
# just said no — the opposite of polite.
_REFUSED_STATUSES = (401, 403, 429)


def _blocked(stats: dict, status: int) -> dict:
    stats["method"] = "blocked"
    stats["note"] = f"bare search HTTP {status}; partition sweep skipped (host refused)"
    return stats


def _partitions(resource_type: str) -> tuple[Optional[str], list[str]]:
    if resource_type in STATE_PARTITIONABLE:
        return "address-state", US_STATES
    if resource_type in NAME_PARTITIONABLE:
        return "name", NAME_INITIALS
    return None, []


def _next_url(endpoint: Endpoint, bundle: dict) -> Optional[str]:
    for link in bundle.get("link", []) or []:
        if link.get("relation") == "next":
            url = link.get("url")
            rep = endpoint.quirks.next_link_replace
            if url and rep and len(rep) == 2:
                url = url.replace(rep[0], rep[1])
            return url
    return None


def _offset_of(url: Optional[str]) -> Optional[int]:
    """The numeric value of the first recognized offset param in `url`, else None."""
    if not url:
        return None
    for k, v in parse_qsl(urlsplit(url).query, keep_blank_values=True):
        if k in _OFFSET_PARAMS:
            try:
                return int(v)
            except (TypeError, ValueError):
                return None
    return None


def rewind_offset_url(
    url: str, rewind_pages: int = RESUME_REWIND_PAGES, page_size: Optional[int] = None
) -> Optional[str]:
    """Rewind an offset-paginated URL by `rewind_pages` pages.

    Returns the rewound URL when the URL carries a recognizable numeric offset
    param (offset floored at 0). Returns None when NO offset param is present —
    signalling the caller to retry the URL verbatim (opaque/stateful cursor).
    If an offset is present but the page size can't be determined (no arg, no
    `_count` in the URL), the URL is returned unchanged (exact retry).
    """
    parts = urlsplit(url)
    q = parse_qsl(parts.query, keep_blank_values=True)
    off_key = next((k for k, _ in q if k in _OFFSET_PARAMS), None)
    if off_key is None:
        return None
    size = page_size
    if not size:
        for k, v in q:
            if k == "_count":
                try:
                    size = int(v)
                except (TypeError, ValueError):
                    size = None
                break
    if not size:
        return url
    new_q = []
    for k, v in q:
        if k == off_key:
            try:
                cur = int(v)
            except (TypeError, ValueError):
                new_q.append((k, v))
                continue
            new_q.append((k, str(max(0, cur - rewind_pages * size))))
        else:
            new_q.append((k, v))
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(new_q), parts.fragment))


def bare_fingerprint(endpoint: Endpoint, resource_type: str, default_count: int) -> str:
    """Stable hash of the bare-pull shape, so a checkpoint is only reused if the
    URL/params/page-size that produced it still match. Mirrors the _count
    resolution in FhirClient._apply_quirks."""
    q = endpoint.quirks
    size = q.page_size_by_resource.get(resource_type) or q.page_size or default_count
    payload = json.dumps(
        {
            "base_url": endpoint.base_url,
            "method": "bare",
            "resource_type": resource_type,
            "base_params": q.base_params or {},
            "page_size": size,
        },
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


async def _try_resume(
    client: FhirClient, endpoint: Endpoint, resource_type: str, ckpt: dict
) -> Optional[tuple[dict, str]]:
    """Fetch the (possibly rewound) checkpoint URL once. Returns (bundle, url) to
    seed pagination, or None on any failure so the caller starts fresh. Kept
    separate from _paginate's first page so a dead cursor (404 on an expired
    _getpages session) can't be misread as 'resource unsupported'."""
    url = ckpt.get("resume_url")
    if not url:
        return None
    rewound = rewind_offset_url(url, RESUME_REWIND_PAGES, ckpt.get("page_size"))
    target = rewound if rewound is not None else url
    try:
        bundle = await client.get_json(target)
    except Exception as exc:  # noqa: BLE001 - any failure => fresh start
        log.info("extract %s/%s: resume fetch failed (%s: %s); starting fresh",
                 endpoint.key, resource_type, type(exc).__name__, exc)
        return None
    if not _is_bundle(bundle):
        log.info("extract %s/%s: resume URL returned non-bundle; starting fresh",
                 endpoint.key, resource_type)
        return None
    return bundle, target


class ResourceSink:
    """Streaming, incremental sink: buffers resources and flushes in batches.

    The flush coroutine persists each batch to Postgres and commits, so progress
    survives a reaped job (only the in-flight batch is lost) and re-runs resume
    via ON CONFLICT DO NOTHING. Memory is bounded to one batch.
    """

    def __init__(self, flush_coro, batch: int = 5000) -> None:
        self._flush_coro = flush_coro
        self._batch = batch
        self._buf: list[dict] = []
        # id_chain/id_read call add() from many concurrent tasks that share ONE
        # psycopg connection and ONE TEMP stage table. Without this lock a second
        # flush can start while the first is mid-TRUNCATE/COPY/INSERT and wipe the
        # staged rows, silently dropping a whole batch.
        self._lock = asyncio.Lock()
        self.streamed = 0   # resources handed to the sink (pre-DB dedup)
        self.inserted = 0   # NEW rows the DB actually inserted

    async def add(self, res: dict) -> None:
        self._buf.append(res)
        self.streamed += 1
        if len(self._buf) >= self._batch:
            await self._flush()

    async def _flush(self) -> None:
        async with self._lock:
            if self._buf:
                batch, self._buf = self._buf, []
                n = await self._flush_coro(batch)
                if isinstance(n, int):
                    self.inserted += n

    async def close(self) -> None:
        await self._flush()


class MultiSink:
    """Streaming sink that routes resources of DIFFERENT types to per-type
    flushers — for `_include` sweeps where one search returns a base resource
    plus its referenced resources (Practitioner, Location, Organization, ...).
    """

    def __init__(self, flushers: dict, batch: int = 5000) -> None:
        # flushers: {resourceType: async fn(list[dict]) -> int}
        self._flush = flushers
        self._batch = batch
        self._buf: dict[str, list[dict]] = {rt: [] for rt in flushers}
        # Same shared-connection/shared-stage-table race as ResourceSink; one lock
        # per resource type is enough because each type has its own connection.
        self._locks: dict[str, asyncio.Lock] = {rt: asyncio.Lock() for rt in flushers}
        self.inserted: dict[str, int] = {rt: 0 for rt in flushers}
        self.streamed: dict[str, int] = {rt: 0 for rt in flushers}

    async def add(self, res: dict) -> None:
        rt = res.get("resourceType")
        if rt not in self._buf:
            return  # a type we're not harvesting
        self._buf[rt].append(res)
        self.streamed[rt] += 1
        if len(self._buf[rt]) >= self._batch:
            await self._do_flush(rt)

    async def _do_flush(self, rt: str) -> None:
        async with self._locks[rt]:
            if self._buf[rt]:
                batch, self._buf[rt] = self._buf[rt], []
                n = await self._flush[rt](batch)
                if isinstance(n, int):
                    self.inserted[rt] += n

    async def close(self) -> None:
        for rt in list(self._buf):
            await self._do_flush(rt)


async def _paginate_all(
    client: FhirClient,
    endpoint: Endpoint,
    base_resource: str,
    params: dict,
    multisink: MultiSink,
    page_budget: Optional[int],
) -> int:
    """Paginate a search, routing ALL entries (base + _include'd) to a MultiSink."""
    params = {**(endpoint.quirks.base_params or {}), **params}
    bundle = await client.search_page(base_resource, params)
    if not _is_bundle(bundle):
        return 0
    pages = 0
    while True:
        pages += 1
        for entry in bundle.get("entry", []) or []:
            res = entry.get("resource") or {}
            if res.get("resourceType") and res.get("id"):
                await multisink.add(res)
        if page_budget is not None and pages >= page_budget:
            break
        nxt = _next_url(endpoint, bundle)
        if not nxt:
            break
        try:
            bundle = await client.get_json(nxt)
        except Exception:  # noqa: BLE001
            break
        if not _is_bundle(bundle):
            break
    return pages


async def include_sweep(
    client: FhirClient,
    endpoint: Endpoint,
    base_resource: str,
    cfg: dict,
    multisink: MultiSink,
    max_pages: Optional[int] = None,
) -> dict:
    """Enumerate `base_resource` by a value list (e.g. specialty ValueSet) with
    `_include` params, harvesting the base + all referenced resources in one
    sweep. Count-guided (skips empty values)."""
    param = cfg["param"]
    include = list(cfg["include"])
    vals = VALUE_LISTS[cfg["values_ref"]] if cfg.get("values_ref") else (cfg.get("values") or [])
    stats = {"method": f"include:{param}", "pages": 0, "buckets": 0,
             "count_queries": 0, "fetch_errors": 0, "empty": 0, "note": None}
    for val in vals:
        cnt = await _count_query(client, base_resource, {param: val})
        stats["count_queries"] += 1
        if cnt == 0:
            stats["empty"] += 1
            continue
        try:
            pages = await _paginate_all(
                client, endpoint, base_resource,
                {param: val, "_include": include}, multisink, max_pages,
            )
            stats["pages"] += pages
            stats["buckets"] += 1
        except Exception:  # noqa: BLE001 - one flaky bucket shouldn't kill the sweep
            stats["fetch_errors"] += 1
    if stats["fetch_errors"]:
        stats["note"] = f"{stats['fetch_errors']} bucket fetch errors"
    return stats


async def _paginate(
    client: FhirClient,
    endpoint: Endpoint,
    resource_type: str,
    params: dict,
    sink: ResourceSink,
    page_budget: Optional[int],
    progress: Optional[dict] = None,
    start: Optional[tuple[dict, str]] = None,
) -> tuple[int, bool, Optional[str], int]:
    """Stream resources from a paginated search into `sink`.

    Returns (pages_consumed, started_ok, note, added). started_ok=False means the
    first page was not a usable Bundle (filter-required OperationOutcome). The first
    page may raise FhirError (4xx/5xx) — the caller decides how to react.

    `progress` (bare pagination only) is a mutable dict shared with the sink's
    flush closure so each batch commit can checkpoint the page URL being consumed.
    It is thread-safe ONLY because bare pagination is single-task: while the flush
    runs in a worker thread, this coroutine is parked on `await sink.add`, so the
    dict is frozen. NEVER pass a shared `progress` from id_chain/id_read/MultiSink
    (many concurrent tasks, one sink). `start` seeds the first page from a resume
    checkpoint instead of issuing the page-1 search.
    """
    pages = 0
    note = None
    added = 0
    # Merge the endpoint's scope filter (e.g. _lastUpdated>=2026) into every
    # search; the partition/adaptive param wins on key conflicts.
    params = {**(endpoint.quirks.base_params or {}), **params}
    if start is not None:
        bundle, page_url = start
    else:
        bundle = await client.search_page(resource_type, params)
        page_url = None
    if not _is_bundle(bundle):
        return 0, False, _oo_note(bundle), 0
    while True:
        pages += 1
        if progress is not None:
            # page_url is the URL of the page about to be consumed -> a checkpoint
            # written mid-page names a page we can safely re-fetch on resume.
            progress["active"] = True
            progress["page_url"] = page_url
            progress["pages"] = progress.get("pages_base", 0) + pages
            if progress["pages"] % HEARTBEAT_EVERY_PAGES == 0:
                log.info("extract %-14s %-24s pages=%d added=%d", endpoint.key,
                         resource_type, progress["pages"], progress.get("added_base", 0) + added)
        for entry in bundle.get("entry", []) or []:
            res = entry.get("resource") or {}
            if res.get("id") and res.get("resourceType") == resource_type:
                await sink.add(res)
                added += 1
        if page_budget is not None and pages >= page_budget:
            # page1_only buckets (budget=1) truncate by design; anything else
            # stopping with a next link still present is OUR cap, not the server's.
            if page_budget > 1 and _next_url(endpoint, bundle):
                note = f"page budget reached at {pages} pages with more available (our cap)"
            break
        next_url = _next_url(endpoint, bundle)
        if not next_url:
            if progress is not None:
                progress["exhausted"] = True   # clean end -> flush/finalize deletes checkpoint
                progress["active"] = False
            break
        try:
            bundle = await client.get_json(next_url)
        except Exception as exc:  # noqa: BLE001 - keep partial on ANY mid-stream error
            note = f"pagination stopped at page {pages}: {type(exc).__name__}: {exc}"
            break
        if not _is_bundle(bundle):
            note = f"pagination stopped at page {pages}: non-bundle page"
            break
        page_url = next_url
    return pages, True, note, added


async def _count_query(
    client: FhirClient, resource_type: str, params: dict, apply_base: bool = True
) -> Optional[int]:
    """Fast Bundle.total via _summary=count (no entries). None if unavailable.

    apply_base merges the endpoint's scope filter so the coverage denominator is
    counted under the same scope we pull (honest %). Pass False for an unscoped
    context count (detects "not refreshed in 2026" false-empties).
    """
    base = (client.endpoint.quirks.base_params or {}) if apply_base else {}
    try:
        bundle = await client.get_json(resource_type, params={**base, **params, "_summary": "count"})
        if bundle.get("resourceType") == "Bundle":
            return bundle.get("total")
    except Exception:  # noqa: BLE001
        return None
    return None


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    if "T" in s:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    y, m, d = (int(x) for x in s.split("-"))
    return datetime(y, m, d, tzinfo=timezone.utc)


async def _daterange_sweep(
    client: FhirClient,
    endpoint: Endpoint,
    resource_type: str,
    param: str,
    cfg: dict,
    sink: ResourceSink,
    stats: dict,
    page_budget: Optional[int],
    bucket_max: int,
) -> None:
    """Recursively bisect a [start, end) time range on `param` (e.g. _lastUpdated).

    Counts each window via _summary=count; if > bucket_max, split at the midpoint;
    else fetch it. A window whose count is UNAVAILABLE (some servers time out
    counting wide ranges, e.g. Humana beyond ~1 year) is bisected blind down to
    `blind_split_floor_days` — fetching a decades-wide window bare would just
    re-trip the server's per-search cap. Closed/exhaustive over timestamped
    records. Caveat: if more than bucket_max records share an identical
    timestamp, a 1-second window can't split — those overflow to page-budget and
    are surfaced by coverage reconciliation.
    """
    from datetime import timedelta

    start = _parse_dt(cfg.get("start")) or datetime(2000, 1, 1, tzinfo=timezone.utc)
    end = _parse_dt(cfg.get("end")) or (datetime.now(timezone.utc) + timedelta(days=1))
    max_depth = int(cfg.get("max_depth", 48))
    min_window = timedelta(seconds=1)
    blind_floor = timedelta(days=float(cfg.get("blind_split_floor_days", 30)))

    stack = [(start, end, 0)]
    while stack:
        s, e, depth = stack.pop()
        rng = {param: [f"ge{_iso(s)}", f"lt{_iso(e)}"]}
        cnt = await _count_query(client, resource_type, rng)
        stats["count_queries"] += 1
        if cnt == 0:
            continue
        splittable = depth < max_depth and (e - s) > min_window
        oversized = cnt is not None and cnt > bucket_max
        blind = cnt is None and (e - s) > blind_floor
        if splittable and (oversized or blind):
            mid = s + (e - s) / 2
            stack.append((s, mid, depth + 1))
            stack.append((mid, e, depth + 1))
            stats["subdivided"] += 1
            if blind:
                stats["blind_splits"] += 1
            continue
        # Leaf window: its count contributes to the exhaustive window sum
        # (date windows partition the timestamped records without overlap).
        if cnt is not None:
            stats["counted_total"] += cnt
        else:
            stats["count_gaps"] += 1
        try:
            pages, _ok, w_note, _added = await _paginate(
                client, endpoint, resource_type, rng, sink, page_budget)
            stats["pages"] += pages
            stats["buckets"] += 1
            # Same trap as the prefix/values branch: a window that dead-ends
            # mid-pagination doesn't raise, so without this every window could
            # truncate and the run would still be recorded "ok".
            if w_note:
                stats["truncated_buckets"] = stats.get("truncated_buckets", 0) + 1
                stats.setdefault("truncated_sample", w_note)
        except Exception:  # noqa: BLE001
            stats["fetch_errors"] += 1


async def adaptive_extract(
    client: FhirClient,
    endpoint: Endpoint,
    resource_type: str,
    cfg: dict,
    sink: ResourceSink,
    max_pages: Optional[int] = None,
) -> dict:
    """Count-guided partitioning to bypass per-search caps / broken pagination.

    `cfg`: {param, mode("prefix"|"values"), values?, bucket_max, page1_only}.
    Recursively subdivides a string `param` by prefix (a-z0-9) until each bucket's
    `_summary=count` is <= bucket_max, then fetches that bucket. With page1_only it
    takes only page 1 (for servers whose `next` links are broken but small buckets
    return complete on page 1, e.g. Premera).
    """
    param = cfg["param"]
    mode = cfg.get("mode", "prefix")
    bucket_max = int(cfg.get("bucket_max", 1000))
    page1_only = bool(cfg.get("page1_only", False))
    max_depth = int(cfg.get("max_depth", 6))
    page_budget = 1 if page1_only else max_pages
    stats = {"method": f"adaptive:{param}", "pages": 0, "buckets": 0, "count_queries": 0,
             "subdivided": 0, "blind_splits": 0, "fetch_errors": 0, "counted_total": 0,
             "count_gaps": 0, "truncated_buckets": 0, "note": None}

    if mode == "daterange":
        # Closed-key partitioning: bisect a time range on a date param (e.g.
        # _lastUpdated). Exhaustive over records that HAVE the timestamp — it can't
        # miss "names we don't know about" the way a prefix sweep can.
        await _daterange_sweep(client, endpoint, resource_type, param, cfg, sink, stats,
                               page_budget, bucket_max)
    else:
        # Work stack of (param) values to evaluate (prefix subdivision / fixed list).
        if mode == "values":
            vals = VALUE_LISTS[cfg["values_ref"]] if cfg.get("values_ref") else (cfg.get("values") or US_STATES)
            stack = list(vals)
        else:
            stack = list(NAME_INITIALS)
        while stack:
            val = stack.pop()
            cnt = await _count_query(client, resource_type, {param: val})
            stats["count_queries"] += 1
            if cnt == 0:
                continue
            # Subdivide oversized prefix buckets (only meaningful in prefix mode).
            if cnt is not None and cnt > bucket_max and mode == "prefix" and len(val) < max_depth:
                stack.extend(val + ch for ch in NAME_INITIALS)
                stats["subdivided"] += 1
                continue
            if cnt is not None:
                stats["counted_total"] += cnt
            else:
                stats["count_gaps"] += 1
            # Fetch this bucket (small enough, at max depth, or count unavailable).
            try:
                pages, _ok, b_note, _added = await _paginate(
                    client, endpoint, resource_type, {param: val}, sink, page_budget
                )
                stats["pages"] += pages
                stats["buckets"] += 1
                # A bucket that dead-ends mid-pagination does NOT raise, so it
                # never reaches fetch_errors. Count it, or a sweep in which every
                # bucket truncated is reported as a clean "ok" run.
                if b_note:
                    stats["truncated_buckets"] += 1
                    stats.setdefault("truncated_sample", b_note)
            except Exception:  # noqa: BLE001 - one flaky bucket shouldn't kill the sweep
                stats["fetch_errors"] += 1

    # Server total for coverage reconciliation (computed against the DB row
    # count by the caller). Bare count where the server allows it; otherwise an
    # exhaustive partition sum can stand in — but ONLY for daterange (windows
    # partition timestamped records) or an explicit match-all filter. Prefix leaf
    # sums undercount the true total (the a-z0-9 charset residual) and must
    # never be used as the denominator, or coverage would overstate itself.
    bare_total = await _count_query(client, resource_type, {})
    stats["server_total"], stats["server_total_source"] = resolve_server_total(
        bare_total, mode, bool(cfg.get("match_all")),
        stats["counted_total"], stats["count_gaps"],
    )
    parts = []
    if stats["fetch_errors"]:
        parts.append(f"{stats['fetch_errors']} bucket fetch errors")
    if stats["truncated_buckets"]:
        # Keep the literal "pagination stopped" wording — classify_status matches
        # on it to mark the run partial rather than ok.
        parts.append(
            f"{stats['truncated_buckets']} of {stats['buckets']} buckets truncated "
            f"(pagination stopped): {stats.get('truncated_sample', '')}"
        )
    if parts:
        stats["note"] = "; ".join(parts)
    return stats


async def id_chain_extract(
    client: FhirClient,
    endpoint: Endpoint,
    resource_type: str,
    cfg: dict,
    sink: ResourceSink,
    ids: list[str],
    max_pages: Optional[int] = None,
) -> dict:
    """Harvest a filter-only reference resource by chaining on a known id.

    Some servers (Excellus) reject every broad/match-all search for
    PractitionerRole/OrganizationAffiliation but answer
    ``{Resource}?{chain_param}=<id>`` one id at a time. We source the ids from an
    already-loaded resource table (e.g. every Practitioner id) and issue one
    chained search each, concurrently. Coverage is bounded by the source set —
    roles for practitioners we never loaded are unreachable (a measured residual).

    cfg: {mode: id_chain, chain_param, source_table[, chain_concurrency]}.
    """
    chain_param = cfg["chain_param"]
    concurrency = int(cfg.get("chain_concurrency", 8))
    # ids per query: comma-joined values are a FHIR OR match, so a batch of N
    # collapses N one-id searches into one request (~Nx fewer requests). Default 1
    # keeps the original one-per-id behaviour. Only raise for servers that both
    # honour comma-OR on the param AND rate-limit request volume (e.g. Humana).
    batch = max(1, int(cfg.get("chain_batch", 1)))
    # Large _count keeps a fat OR-batch to few pages (fewer requests = gentler on
    # rate-limited edges). Servers that cap _count just return their own max.
    page_size = cfg.get("chain_count")
    base_params = {"_count": str(page_size)} if page_size else {}
    sem = asyncio.Semaphore(concurrency)
    stats = {"method": f"id_chain:{chain_param}" + (f"x{batch}" if batch > 1 else ""),
             "pages": 0, "queries": 0, "fetch_errors": 0, "source_ids": len(ids),
             "note": None, "server_total": None, "server_total_source": None}

    async def _one(id_group: list[str]) -> None:
        async with sem:
            try:
                bundle = await client.search_page(resource_type, {**base_params, chain_param: ",".join(id_group)})
                pages = 0
                while isinstance(bundle, dict) and bundle.get("resourceType") == "Bundle":
                    pages += 1
                    for entry in bundle.get("entry", []) or []:
                        res = entry.get("resource") or {}
                        if res.get("resourceType") == resource_type and res.get("id"):
                            await sink.add(res)
                    nxt = _next_url(endpoint, bundle)
                    if not nxt or (max_pages is not None and pages >= max_pages):
                        break
                    bundle = await client.get_json(nxt)
                stats["pages"] += pages
                stats["queries"] += 1
            except Exception:  # noqa: BLE001 - one bad group shouldn't kill the sweep
                stats["fetch_errors"] += 1

    # Chunk ids into OR-batches, then bound in-flight query tasks.
    groups = [ids[i:i + batch] for i in range(0, len(ids), batch)]
    for i in range(0, len(groups), 500):
        await asyncio.gather(*(_one(g) for g in groups[i:i + 500]))
    if stats["fetch_errors"]:
        stats["note"] = (f"{stats['fetch_errors']} chain-query errors of "
                         f"{len(groups)} groups ({len(ids)} ids)")
    return stats


async def id_read_extract(
    client: FhirClient,
    endpoint: Endpoint,
    resource_type: str,
    cfg: dict,
    sink: ResourceSink,
    ids: list[str],
    max_pages: Optional[int] = None,
) -> dict:
    """Reference-graph harvest: fetch a resource by id via ``GET {Type}/{id}``.

    For resources with NO working bulk-search partition (e.g. Regence
    Practitioner / Location / HealthcareService), the single-record READ endpoint
    still works and is NOT paginated — so it sidesteps the search pagination cap
    entirely. Ids come from references already collected in other tables
    (role.practitioner, *.location, ...). Coverage is bounded by the referenced
    subgraph, which for a provider directory is nearly all of it.
    """
    concurrency = int(cfg.get("read_concurrency", 8))
    sem = asyncio.Semaphore(concurrency)
    stats = {"method": "id_read", "reads": 0, "hits": 0, "not_found": 0,
             "fetch_errors": 0, "source_ids": len(ids), "note": None,
             "server_total": None, "server_total_source": None}

    async def _one(rid: str) -> None:
        async with sem:
            stats["reads"] += 1
            try:
                res = await client.get_json(f"{resource_type}/{rid}")
                if isinstance(res, dict) and res.get("resourceType") == resource_type and res.get("id"):
                    await sink.add(res)
                    stats["hits"] += 1
            except FhirError as exc:
                if exc.status == 404:
                    stats["not_found"] += 1  # stale reference; not an error
                else:
                    stats["fetch_errors"] += 1
            except Exception:  # noqa: BLE001 - one bad id shouldn't kill the harvest
                stats["fetch_errors"] += 1

    for i in range(0, len(ids), 500):
        await asyncio.gather(*(_one(r) for r in ids[i:i + 500]))
    if stats["fetch_errors"]:
        stats["note"] = f"{stats['fetch_errors']} read errors of {len(ids)} ids"
    return stats


def resolve_server_total(
    bare_total: Optional[int],
    mode: str,
    match_all: bool,
    counted_total: int,
    count_gaps: int,
) -> tuple[Optional[int], Optional[str]]:
    """Pick the coverage denominator: bare count, else an exhaustive bucket sum."""
    if isinstance(bare_total, int):
        return bare_total, "bare"
    if counted_total and not count_gaps:
        if mode == "daterange":
            return counted_total, "daterange_window_sum"
        if match_all:
            return counted_total, "match_all_count"
    return None, None


async def extract_resource(
    client: FhirClient,
    endpoint: Endpoint,
    resource_type: str,
    sink: ResourceSink,
    max_pages: Optional[int] = None,
    resume_ckpt: Optional[dict] = None,
    progress: Optional[dict] = None,
) -> dict:
    """Stream all resources of a type into `sink`. Returns stats only.

    stats.method is one of: adaptive:<param> | bare | partition:<param> |
    unsupported | needs-partition. Never raises for HTTP status conditions;
    unexpected errors still propagate. The sink persists incrementally (per batch),
    so progress survives a reaped job and re-runs resume.

    `resume_ckpt`/`progress` enable bare-pagination checkpointing (see _paginate).
    They apply only to the bare path; adaptive/partition paths ignore them.
    """
    stats: dict = {"method": None, "pages": 0, "partitions": 0, "note": None}

    # 0. Forced adaptive partitioning (configured per endpoint to bypass caps).
    adaptive_cfg = (endpoint.quirks.adaptive or {}).get(resource_type)
    if adaptive_cfg:
        stats = await adaptive_extract(client, endpoint, resource_type, adaptive_cfg, sink, max_pages)
        return stats

    # 1. Bare paginated search.
    try:
        start = None
        if resume_ckpt and resume_ckpt.get("resume_url"):
            start = await _try_resume(client, endpoint, resource_type, resume_ckpt)
            if start is not None:
                # Carry forward cumulative page/row counts ONLY when the resume
                # actually took, so a failed resume reports honest page-1 numbers.
                if progress is not None:
                    progress["pages_base"] = resume_ckpt.get("pages_done") or 0
                    progress["added_base"] = resume_ckpt.get("rows_added") or 0
                stats["resumed_from_page"] = resume_ckpt.get("pages_done")
                stats["resumed_offset"] = _offset_of(start[1])
        pages, ok, note, _added = await _paginate(
            client, endpoint, resource_type, {}, sink, max_pages,
            progress=progress, start=start,
        )
        stats["pages"] += pages
        if ok:
            stats["method"] = "bare"
            stats["note"] = note
            bare_total = await _count_query(client, resource_type, {})
            stats["server_total"] = bare_total
            stats["server_total_source"] = "bare" if isinstance(bare_total, int) else None
            return stats
        bare_reason = note  # 200 OperationOutcome => requires a filter
    except FhirError as exc:
        if exc.status == 404:
            stats["method"] = "unsupported"
            stats["note"] = "bare search 404 (resource not served)"
            return stats
        if exc.status in _REFUSED_STATUSES:
            return _blocked(stats, exc.status)
        bare_reason = f"HTTP {exc.status}"
    except Exception as exc:  # noqa: BLE001
        # 5xx (after retries) or a timeout on the bare page — e.g. bcbs_mn 500s
        # on Practitioner?_count=1000. Fall through to the smaller-page partition
        # sweep, which the server can serve.
        status = getattr(getattr(exc, "response", None), "status_code", None)
        if status == 404:
            stats["method"] = "unsupported"
            stats["note"] = "bare search 404 (resource not served)"
            return stats
        # 429 arrives HERE, not as a FhirError: _request calls raise_for_status()
        # inside the retry loop and tenacity re-raises httpx.HTTPStatusError, which
        # escapes before get_json can wrap it. Checking only the FhirError branch
        # would leave rate limiting — the case that most needs backing off —
        # falling through to the ~88-request partition sweep.
        if status in _REFUSED_STATUSES:
            return _blocked(stats, status)
        bare_reason = f"{type(exc).__name__}: {exc}"

    # Bare failed; we're leaving the checkpointed path. Clear the shared progress
    # so partition-sweep flushes (which pass progress=None) can't have a stale
    # active=True + bare page_url written as a bogus checkpoint by the sink.
    if progress is not None:
        progress["active"] = False
        progress.pop("exhausted", None)

    # 2. Partition fallback (filter-required servers). Try the primary partition
    # param; if it yields nothing and another strategy exists, try that too
    # (e.g. some servers don't support address-state search on Organization).
    strategies: list[tuple[str, list[str]]] = []
    param, values = _partitions(resource_type)
    if param:
        strategies.append((param, values))
    if resource_type in NAME_PARTITIONABLE and param != "name":
        strategies.append(("name", NAME_INITIALS))

    if not strategies:
        stats["method"] = "needs-partition"
        stats["note"] = f"bare search rejected ({bare_reason}); no partition strategy for {resource_type}"
        log.warning("extract %s/%s: %s", endpoint.key, resource_type, stats["note"])
        return stats

    # Bound the partition sweep even in full runs (bare path stays unbounded).
    budget_left = max_pages if max_pages is not None else PARTITION_PAGE_CAP
    used = 0
    errored = 0
    truncated = 0
    truncated_sample = None
    method_used = None
    for s_param, s_values in strategies:
        added_strategy = 0
        for val in s_values:
            if budget_left is not None and budget_left <= 0:
                break
            try:
                p, p_ok, p_note, added = await _paginate(
                    client, endpoint, resource_type, {s_param: val}, sink, budget_left
                )
            except Exception:  # noqa: BLE001 - one flaky partition (5xx/timeout) shouldn't kill the sweep
                errored += 1
                continue
            stats["pages"] += p
            added_strategy += added
            if p_note:
                truncated += 1
                if not truncated_sample:
                    truncated_sample = p_note
            if budget_left is not None:
                budget_left -= p
            if p_ok:
                used += 1
        if added_strategy > 0:
            method_used = s_param
            break  # this strategy produced data; don't run the next one

    stats["partitions"] = used
    stats["fetch_errors"] = errored
    stats["method"] = f"partition:{method_used}" if method_used else (
        "unsupported" if errored else f"partition:{strategies[0][0]}"
    )
    if method_used is None and errored:
        # "unsupported" would read as "this resource isn't served"; every
        # partition failing is an outage/refusal, which classify_status must not
        # fold into "skipped".
        stats["method"] = "partition-failed"
        stats["note"] = f"bare and all {errored} partitions failed for {resource_type}"
    elif budget_left is not None and budget_left <= 0:
        cap = max_pages if max_pages is not None else PARTITION_PAGE_CAP
        prefix = f"{stats['note']}; " if stats.get("note") else ""
        stats["note"] = f"{prefix}partition sweep page budget exhausted (cap={cap}; our cap, not the server's)"
    if truncated:
        prefix = f"{stats['note']}; " if stats.get("note") else ""
        stats["note"] = (f"{prefix}{truncated} partitions truncated "
                         f"(pagination stopped): {truncated_sample}")
    bare_total = await _count_query(client, resource_type, {})
    stats["server_total"] = bare_total
    stats["server_total_source"] = "bare" if isinstance(bare_total, int) else None
    return stats
