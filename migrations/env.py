"""Alembic environment — drives migrations from provdir.models metadata.

The DB URL is taken from Settings (.env), never from alembic.ini, so secrets
stay out of version control.
"""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

from provdir.config import get_settings
from provdir.models import shared_metadata as target_metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Build the URL from settings directly. Do NOT route it through alembic.ini /
# ConfigParser: the URL-encoded password can contain '%', which ConfigParser
# misreads as interpolation syntax.
DB_URL = get_settings().sqlalchemy_url


def run_migrations_offline() -> None:
    context.configure(
        url=DB_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = create_engine(DB_URL, poolclass=pool.NullPool, future=True)
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
