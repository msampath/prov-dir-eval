"""extract_checkpoint: durable bare-pagination cursor for `provdir etl --resume`

Adds only the shared `public.extract_checkpoint` table. The `last_seen_at` column
on per-payer resource tables is NOT here — those schemas are created at runtime by
provdir.models.create_resource_schema, which self-heals the column via
ALTER TABLE ... ADD COLUMN IF NOT EXISTS.

Revision ID: 0002_extract_checkpoint
Revises: 0001_initial
Create Date: 2026-08-02
"""
from typing import Sequence, Union

from alembic import op

from provdir.models import extract_checkpoint

revision: str = "0002_extract_checkpoint"
down_revision: Union[str, None] = "0001_initial"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # checkfirst: a fresh DB runs 0001 first, whose shared_metadata.create_all
    # already includes this table (it's registered on shared_metadata). On an
    # existing DB the table is missing and gets created here.
    extract_checkpoint.create(op.get_bind(), checkfirst=True)


def downgrade() -> None:
    extract_checkpoint.drop(op.get_bind(), checkfirst=True)
