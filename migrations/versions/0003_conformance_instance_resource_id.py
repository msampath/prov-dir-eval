"""conformance_instance.resource_id: trace a finding back to the row it came from

`public.conformance_instance`'s primary key is
`(run_id, payer_id, cell, instance)`, where `instance` is a FILENAME
(`<payer>__<cell>__<n>.json`). That is enough to join a finding to the file the
validator read and nothing else — the file name carries no FHIR id, and until
the sampler gained an ORDER BY it did not even name the same row twice.

So a D3 finding could not be looked up in the payer's own data. This adds the
resource id the sampler now records alongside every instance it writes.

Nullable, and deliberately not backfilled: runs 1–9 have no id sidecar, and
inventing ids for them would be worse than a NULL that says "this run predates
the column". Every conformance table is created by `scripts/validate_conformance.py`'s
own DDL rather than by `provdir.models`, so this migration uses IF EXISTS /
IF NOT EXISTS — a database that has never run a conformance job simply has
nothing to alter, and the script's self-healing ALTER covers the reverse order.

Revision ID: 0003_conf_inst_resource_id
Revises: 0002_extract_checkpoint
Create Date: 2026-08-26
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0003_conf_inst_resource_id"
down_revision: Union[str, None] = "0002_extract_checkpoint"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE IF EXISTS public.conformance_instance "
        "ADD COLUMN IF NOT EXISTS resource_id text"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE IF EXISTS public.conformance_instance "
        "DROP COLUMN IF EXISTS resource_id"
    )
