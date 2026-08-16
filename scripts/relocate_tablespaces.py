#!/usr/bin/env python
"""Move large payer schemas off the default tablespace, which is out of space.

F: holds `data_directory` (F:/PostgreSQL/18/data) AND the repo AND the 118,951
file validator sample. `default_tablespace` was empty, so every table without an
explicit tablespace landed there; pg_default reached 646GB and F: hit zero, which
killed the HCSC load mid-write at 13.6M of 18.6M PractitionerRole rows.

Split by access pattern rather than dumping everything in one place:

  heap + TOAST -> bulk_heap (E:)   ~475GB, USB HDD, 4.6TB free
  indexes      -> hot_idx   (M:)   ~20GB, SATA SSD, the tablespace already
                                   named for this and holding other indexes

TOAST is most of the bytes here (the JSONB `resource` column) and moves
automatically with its table. Indexes have their own reltablespace and must be
moved separately -- which is the opening to put them somewhere faster, since
they are what queries actually seek into while the heap is mostly scanned or
fetched by ctid.

ALTER ... SET TABLESPACE writes the new copy before freeing the old, so the
destination needs the space, not the source. That is what makes this work at all
with F: at zero.

Each statement runs in its own transaction under autocommit, so an interruption
leaves earlier moves done rather than rolling back hours of rewriting. Re-running
skips anything already relocated.
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from provdir.etl.loader import pg_connection  # noqa: E402

HEAP_TS = "bulk_heap"
IDX_TS = "hot_idx"
MIN_ROWS = 10_000_000


def volume_of(conn, tablespace: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname = %s",
                    (tablespace,))
        row = cur.fetchone()
    return row[0] if row and row[0] else None


def free_gb(path: str | None) -> float | None:
    if not path:
        return None
    try:
        return shutil.disk_usage(path).free / 1e9
    except OSError:
        return None


def big_payers(conn, min_rows: int) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT n.nspname
            FROM pg_class cl
            JOIN pg_namespace n ON n.oid = cl.relnamespace
            LEFT JOIN pg_stat_all_tables st ON st.relid = cl.oid
            WHERE cl.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog','information_schema','public')
              AND n.nspname NOT LIKE 'pg_%%'
            GROUP BY 1
            HAVING SUM(GREATEST(COALESCE(st.n_live_tup,0), COALESCE(cl.reltuples,0))) > %s
            ORDER BY 1
        """, (min_rows,))
        return [r[0] for r in cur.fetchall()]


def pending(conn, schemas: list[str]) -> list[tuple[str, str, str, int]]:
    """(kind, schema, name, bytes) still sitting on pg_default, biggest first.

    pg_relation_size + the TOAST total is what actually moves with an
    ALTER TABLE; pg_total_relation_size would also count indexes, which move
    separately and to a different tablespace.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cl.relkind, n.nspname, cl.relname,
                   pg_relation_size(cl.oid)
                     + CASE WHEN cl.reltoastrelid <> 0
                            THEN pg_total_relation_size(cl.reltoastrelid) ELSE 0 END
            FROM pg_class cl
            JOIN pg_namespace n ON n.oid = cl.relnamespace
            WHERE cl.reltablespace = 0
              AND cl.relkind IN ('r','i')
              AND n.nspname = ANY(%s)
            ORDER BY 4 DESC
        """, (schemas,))
        return [(k, s, r, int(b or 0)) for k, s, r, b in cur.fetchall()]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-rows", type=int, default=MIN_ROWS)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", help="restrict to one schema")
    a = ap.parse_args()

    with pg_connection() as conn:
        conn.autocommit = True
        heap_vol, idx_vol = volume_of(conn, HEAP_TS), volume_of(conn, IDX_TS)
        print(f"{HEAP_TS} -> {heap_vol} ({free_gb(heap_vol):,.0f} GB free)")
        print(f"{IDX_TS}   -> {idx_vol} ({free_gb(idx_vol):,.0f} GB free)")

        schemas = [a.only] if a.only else big_payers(conn, a.min_rows)
        print(f"\n{len(schemas)} schema(s) over {a.min_rows:,} rows: {', '.join(schemas)}")

        items = pending(conn, schemas)
        tot = sum(b for _, _, _, b in items)
        tables = [i for i in items if i[0] == "r"]
        idxs = [i for i in items if i[0] == "i"]
        print(f"{len(tables)} table(s) + {len(idxs)} index(es) still on pg_default, "
              f"{tot/1e9:,.1f} GB\n")
        if a.dry_run:
            for kind, sch, name, b in items:
                dest = HEAP_TS if kind == "r" else IDX_TS
                print(f"  WOULD MOVE {'table' if kind=='r' else 'index'} "
                      f"{sch}.{name} ({b/1e9:.1f} GB) -> {dest}")
            return 0

        moved = 0
        t_start = time.time()
        for i, (kind, sch, name, b) in enumerate(items, 1):
            dest = HEAP_TS if kind == "r" else IDX_TS
            what = "TABLE" if kind == "r" else "INDEX"
            t0 = time.time()
            print(f"[{i}/{len(items)}] {what} {sch}.{name} {b/1e9:6.1f} GB -> {dest} ...",
                  end="", flush=True)
            try:
                with conn.cursor() as cur:
                    cur.execute(f'ALTER {what} "{sch}"."{name}" SET TABLESPACE {dest}')
            except Exception as e:
                print(f" FAILED: {type(e).__name__}: {str(e)[:120]}")
                continue
            moved += b
            dt = time.time() - t0
            src_free = free_gb(r"F:\\")
            print(f" done in {dt/60:5.1f}m  (F: now {src_free:,.0f} GB free, "
                  f"{moved/1e9:,.0f} of {tot/1e9:,.0f} GB moved)")

        print(f"\nmoved {moved/1e9:,.1f} GB in {(time.time()-t_start)/3600:.1f} h")
        print(f"F: free now {free_gb(r'F:\\'):,.0f} GB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
