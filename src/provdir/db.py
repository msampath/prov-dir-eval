"""Database access: SQLAlchemy engine + connectivity check.

Schema DDL lives in Alembic migrations (Phase 4). This module owns the engine
factory and a `check` helper used by `python -m provdir.db check`.
"""

from __future__ import annotations

import re
import sys
from functools import lru_cache
from typing import Optional

from sqlalchemy import Engine, create_engine, text

from .config import Settings, get_settings
from .logging_setup import get_logger

log = get_logger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def safe_ident(name: str) -> str:
    """Validate a SQL identifier (schema/table/column) before it is interpolated
    into a query string. Every such identifier in this codebase comes from the
    internal manifest or the fixed model definitions — never external/user input —
    so this asserts that invariant and makes string-built DDL/DML injection-proof
    (data *values* are always passed as bound parameters, never interpolated).
    """
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


@lru_cache
def get_engine() -> Engine:
    settings = get_settings()
    if not settings.postgres_password:
        log.warning(
            "POSTGRES_PASSWORD is empty; set it in .env before running DB operations."
        )
    return create_engine(settings.sqlalchemy_url, pool_pre_ping=True, future=True)


def check_connection(settings: Optional[Settings] = None) -> dict:
    """Return server version + a list of provdir tables, raising on failure."""
    settings = settings or get_settings()
    engine = create_engine(settings.sqlalchemy_url, future=True)
    info: dict = {"url": settings.sqlalchemy_url_redacted}
    with engine.connect() as conn:
        info["server_version"] = conn.execute(text("SHOW server_version")).scalar_one()
        info["current_database"] = conn.execute(text("SELECT current_database()")).scalar_one()
        info["current_user"] = conn.execute(text("SELECT current_user")).scalar_one()
        tables = conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' ORDER BY table_name"
            )
        ).scalars().all()
        info["public_tables"] = list(tables)
        payer_schemas = conn.execute(
            text(
                "SELECT table_schema, count(*) FROM information_schema.tables "
                "WHERE table_schema NOT IN ('public','pg_catalog','information_schema') "
                "GROUP BY table_schema ORDER BY table_schema"
            )
        ).all()
        info["payer_schemas"] = {s: int(n) for s, n in payer_schemas}
    engine.dispose()
    return info


def _cli(argv: list[str]) -> int:
    from .logging_setup import setup_logging

    setup_logging(run_name="db")
    cmd = argv[0] if argv else "check"
    if cmd != "check":
        print(f"Unknown db command: {cmd!r}. Try: check", file=sys.stderr)
        return 2
    try:
        info = check_connection()
    except Exception as exc:  # noqa: BLE001 - surface any connection error clearly
        print(f"DB connection FAILED: {exc}", file=sys.stderr)
        print(f"  url: {get_settings().sqlalchemy_url_redacted}", file=sys.stderr)
        return 1
    print("DB connection OK")
    print(f"  url             : {info['url']}")
    print(f"  server_version  : {info['server_version']}")
    print(f"  current_database: {info['current_database']}")
    print(f"  current_user    : {info['current_user']}")
    print(f"  public tables ({len(info['public_tables'])}): {', '.join(info['public_tables']) or '(none yet)'}")
    schemas = info["payer_schemas"]
    print(f"  payer schemas ({len(schemas)}): "
          + (", ".join(f"{s}({n})" for s, n in schemas.items()) or "(none yet)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli(sys.argv[1:]))
