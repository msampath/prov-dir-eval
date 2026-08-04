"""Phase 5.3 — Load: per-datasource schema, monthly drop-and-reload via COPY.

Each payer's resource tables live in their own Postgres schema (named after the
endpoint key). A connection sets ``search_path`` to that schema so unqualified
table names resolve there, while the shared ``provenance`` / ``data_quality_score``
tables resolve from ``public``.

Reload = TRUNCATE the resource table in the payer schema, then stream rows with
psycopg3 binary COPY. JSONB values are wrapped for the binary protocol;
arrays/timestamps adapt natively.
"""

from __future__ import annotations

import re
from typing import Iterable, Optional

import psycopg
from psycopg.types.json import Jsonb
from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from ..config import get_settings
from ..logging_setup import get_logger

log = get_logger(__name__)

_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def schema_for(payer_key: str) -> str:
    """Postgres schema name for a payer key (keys are already snake_case slugs)."""
    s = payer_key.strip().lower().replace("-", "_")
    if not _IDENT_RE.match(s):
        raise ValueError(f"payer key {payer_key!r} is not a safe schema identifier")
    return s


def set_search_path(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'SET search_path TO "{schema}", public')


def pg_connection(schema: Optional[str] = None) -> psycopg.Connection:
    s = get_settings()
    conn = psycopg.connect(
        host=s.postgres_host,
        port=s.postgres_port,
        dbname=s.postgres_db,
        user=s.postgres_user,
        password=s.postgres_password,
        autocommit=False,
    )
    if schema:
        set_search_path(conn, schema)
    return conn


def ensure_payer_schema(schema: str) -> None:
    """Create the payer schema and its 8 resource tables (idempotent)."""
    from ..db import get_engine
    from ..models import create_resource_schema

    create_resource_schema(get_engine(), schema)


def _copy_type(column) -> str:
    t = column.type
    if isinstance(t, JSONB):
        return "jsonb"
    if isinstance(t, ARRAY):
        return "text[]"
    cls = type(t).__name__
    return {
        "Text": "text",
        "String": "text",
        "DateTime": "timestamptz",
        "Integer": "int4",
        "BigInteger": "int8",
        "Double": "float8",
    }.get(cls, "text")


def _copy_columns(table: Table) -> list[str]:
    skip = {"ingested_at", "run_id"}
    return [c.name for c in table.columns if c.name not in skip]


def _adapt(value, type_name: str):
    if value is None:
        return None
    if type_name == "jsonb":
        return Jsonb(value)
    return value


def truncate(conn: psycopg.Connection, table: Table) -> None:
    """TRUNCATE the resource table in the current search_path (payer) schema."""
    with conn.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE "{table.name}"')


def bulk_load(conn: psycopg.Connection, table: Table, rows: Iterable[dict]) -> int:
    cols = _copy_columns(table)
    types = [_copy_type(table.c[name]) for name in cols]
    col_list = ", ".join(f'"{c}"' for c in cols)
    n = 0
    with conn.cursor() as cur:
        with cur.copy(f'COPY "{table.name}" ({col_list}) FROM STDIN (FORMAT BINARY)') as copy:
            copy.set_types(types)
            for row in rows:
                copy.write_row([_adapt(row.get(name), types[i]) for i, name in enumerate(cols)])
                n += 1
    return n


_STAGE = "_provdir_stage"


def prepare_stage(conn: psycopg.Connection, table: Table) -> None:
    """Create a per-connection TEMP staging table mirroring the target.

    Used by the streaming/incremental loader: each batch is COPY'd into the stage
    then merged with ON CONFLICT DO NOTHING, so progress commits per batch and a
    re-run resumes (idempotent) instead of restarting.
    """
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS {_STAGE}')
        cur.execute(f'CREATE TEMP TABLE {_STAGE} (LIKE "{table.name}" INCLUDING DEFAULTS)')
    conn.commit()


def upsert_batch(conn: psycopg.Connection, table: Table, rows: list[dict], update: bool = False) -> int:
    """COPY rows into the stage, then INSERT ... ON CONFLICT (payer_id,id).

    Default (``update=False``): ``DO NOTHING`` — existing ids are skipped, so a
    re-run only inserts NEW rows (resumable, non-destructive).

    ``update=True``: ``DO UPDATE`` — on a (payer_id, id) conflict every non-key
    column (``resource``, ``raw_hash``, ``meta_last_updated``, denormalized ref
    columns, …) is refreshed from the incoming row, so records that CHANGED on
    the server are updated in place instead of left stale.

    Returns cur.rowcount (rows inserted; in update mode also counts rows updated).
    Does NOT commit — the caller commits per batch.
    """
    if not rows:
        return 0
    cols = _copy_columns(table)
    types = [_copy_type(table.c[name]) for name in cols]
    col_list = ", ".join(f'"{c}"' for c in cols)
    if update:
        set_cols = [c for c in cols if c not in ("payer_id", "id")]
        set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in set_cols)
        conflict = f"ON CONFLICT (payer_id, id) DO UPDATE SET {set_clause}"
        # DO UPDATE raises 21000 CardinalityViolation if one command touches the
        # same conflict key twice, and a batch legitimately can: an id_chain sweep
        # returns a role once per network it belongs to, and prefix/state
        # partitions overlap. DO NOTHING tolerates duplicates, so only the update
        # branch needs the staged rows collapsed to one row per key.
        select_from_stage = f"SELECT DISTINCT ON (payer_id, id) {col_list} FROM {_STAGE} ORDER BY payer_id, id"
    else:
        conflict = "ON CONFLICT (payer_id, id) DO NOTHING"
        select_from_stage = f"SELECT {col_list} FROM {_STAGE}"
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {_STAGE}")
        with cur.copy(f'COPY {_STAGE} ({col_list}) FROM STDIN (FORMAT BINARY)') as copy:
            copy.set_types(types)
            for row in rows:
                copy.write_row([_adapt(row.get(name), types[i]) for i, name in enumerate(cols)])
        cur.execute(
            f'INSERT INTO "{table.name}" ({col_list}) '
            f'{select_from_stage} {conflict}'
        )
        return cur.rowcount


def list_payer_schemas(conn: psycopg.Connection) -> set[str]:
    """Schemas that hold a resource table => one per loaded datasource."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT table_schema FROM information_schema.tables "
            "WHERE table_name = 'organization' "
            "AND table_schema NOT IN ('public','pg_catalog','information_schema')"
        )
        return {r[0] for r in cur.fetchall()}


def count_rows(conn: psycopg.Connection, table: Table) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM "{table.name}"')
        return cur.fetchone()[0]


def latest_prior_count(conn: psycopg.Connection, payer_id: str, resource_type: str) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT resource_count FROM public.provenance "
            "WHERE payer_id = %s AND resource_type = %s AND status IN ('ok','partial') "
            "ORDER BY started_at DESC LIMIT 1",
            (payer_id, resource_type),
        )
        row = cur.fetchone()
        return row[0] if row else None


def load_checkpoint(conn: psycopg.Connection, payer_id: str, resource_type: str) -> Optional[dict]:
    """Read the bare-pagination checkpoint for (payer, resource), or None.

    public.-qualified so it works regardless of the connection's search_path.
    Does NOT commit — the caller owns the transaction.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT resume_url, pages_done, rows_added, page_size, params_fingerprint, updated_at "
            "FROM public.extract_checkpoint WHERE payer_id = %s AND resource_type = %s",
            (payer_id, resource_type),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "resume_url": row[0], "pages_done": row[1], "rows_added": row[2],
        "page_size": row[3], "params_fingerprint": row[4], "updated_at": row[5],
    }


def upsert_checkpoint(
    conn: psycopg.Connection, payer_id: str, resource_type: str, resume_url: Optional[str],
    pages_done: int, rows_added: int, page_size: Optional[int], params_fingerprint: str,
) -> None:
    """Insert/update the checkpoint. Does NOT commit (caller owns the txn).

    updated_at is set explicitly on BOTH insert and conflict — the server_default
    only fires on insert, so a bare EXCLUDED update would freeze the timestamp and
    break the TTL freshness check.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO public.extract_checkpoint "
            "(payer_id, resource_type, resume_url, pages_done, rows_added, "
            " page_size, params_fingerprint, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, now()) "
            "ON CONFLICT (payer_id, resource_type) DO UPDATE SET "
            "resume_url = EXCLUDED.resume_url, pages_done = EXCLUDED.pages_done, "
            "rows_added = EXCLUDED.rows_added, page_size = EXCLUDED.page_size, "
            "params_fingerprint = EXCLUDED.params_fingerprint, updated_at = now()",
            (payer_id, resource_type, resume_url, pages_done, rows_added,
             page_size, params_fingerprint),
        )


def delete_checkpoint(conn: psycopg.Connection, payer_id: str, resource_type: str) -> None:
    """Remove the checkpoint (no-op if absent). Does NOT commit."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM public.extract_checkpoint WHERE payer_id = %s AND resource_type = %s",
            (payer_id, resource_type),
        )


def insert_provenance(conn: psycopg.Connection, record: dict) -> int:
    cols = [
        "payer_id", "source_base_url", "resource_type", "started_at", "finished_at",
        "status", "page_count", "resource_count", "error_count", "prior_count",
        "pct_change", "notes",
    ]
    placeholders = ", ".join(["%s"] * len(cols))
    values = []
    for c in cols:
        v = record.get(c)
        values.append(Jsonb(v) if c == "notes" and v is not None else v)
    with conn.cursor() as cur:
        cur.execute(
            f'INSERT INTO public.provenance ({", ".join(cols)}) VALUES ({placeholders}) RETURNING run_id',
            values,
        )
        return cur.fetchone()[0]
