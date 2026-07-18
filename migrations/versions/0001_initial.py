"""initial shared schema (provenance + data_quality_score)

Per-payer resource schemas (one per datasource) are created at runtime by the
ETL (see provdir.models.create_resource_schema); this migration owns only the
shared cross-payer tables in `public`.

Revision ID: 0001_initial
Revises:
Create Date: 2026-06-14
"""
from typing import Sequence, Union

from alembic import op

from provdir.models import shared_metadata

revision: str = "0001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    shared_metadata.create_all(bind=bind)
    op.create_index("ix_prov_payer_resource", "provenance", ["payer_id", "resource_type"])


def downgrade() -> None:
    op.drop_index("ix_prov_payer_resource", table_name="provenance")
    shared_metadata.drop_all(bind=op.get_bind())
