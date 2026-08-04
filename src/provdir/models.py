"""Phase 4 — Postgres schema (JSONB-hybrid), one schema per datasource.

Each payer/datasource gets its OWN Postgres schema (named after the endpoint
key, e.g. ``humana``, ``bcbs_az``) holding the 8 Plan-Net resource tables. This
makes per-payer reload a clean ``DROP SCHEMA``/``TRUNCATE`` and isolates sources
for long-term operation. Cross-payer metadata (``provenance``,
``data_quality_score``) lives in the shared ``public`` schema.

Resource tables keep a ``payer_id`` column for traceability even though the
schema already identifies the payer. Queries set ``search_path`` to the payer
schema so unqualified table names resolve correctly; ``public`` stays on the
path for the shared tables.

Tables are SQLAlchemy Core (no ORM): the same metadata drives migrations,
per-schema creation (via schema_translate_map), bulk loads, and quality queries.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Double,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

# Resource tables: schema=None so schema_translate_map can place them per payer.
resource_metadata = MetaData()
# Shared cross-payer metadata: lives in public.
shared_metadata = MetaData()


def _common_columns() -> list[Column]:
    return [
        Column("payer_id", Text, primary_key=True),
        Column("id", Text, primary_key=True),
        Column("source_base_url", Text, nullable=False),
        Column("resource", JSONB, nullable=False),
        Column("raw_hash", Text, nullable=False),
        Column("meta_last_updated", DateTime(timezone=True)),
        Column("ingested_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
        # Stamped by transform on every (re)upsert so a monthly re-pull can tell
        # still-present rows from upstream-deleted ghosts (last_seen_at < run start).
        # Only refreshed under --upsert; DO NOTHING skips existing rows entirely.
        Column("last_seen_at", DateTime(timezone=True)),
    ]


organization = Table(
    "organization",
    resource_metadata,
    *_common_columns(),
    Column("name", Text),
    Column("alias", ARRAY(Text)),
    Column("type_codes", ARRAY(Text)),
    Column("is_network", Integer, index=True),
    Column("npi", Text, index=True),
    Column("active", Integer),
    Column("part_of_ref", Text),
)

practitioner = Table(
    "practitioner",
    resource_metadata,
    *_common_columns(),
    Column("family", Text, index=True),
    Column("given", Text),
    Column("name_text", Text),
    Column("npi", Text, index=True),
    Column("active", Integer),
    Column("qualification_codes", ARRAY(Text)),
)

practitioner_role = Table(
    "practitioner_role",
    resource_metadata,
    *_common_columns(),
    Column("practitioner_ref", Text, index=True),
    Column("organization_ref", Text, index=True),
    Column("location_refs", ARRAY(Text)),
    Column("healthcare_service_refs", ARRAY(Text)),
    Column("network_refs", ARRAY(Text)),
    Column("specialty_codes", ARRAY(Text)),
    Column("role_codes", ARRAY(Text)),
    Column("active", Integer),
)

location = Table(
    "location",
    resource_metadata,
    *_common_columns(),
    Column("name", Text),
    Column("status", Text),
    Column("address_line", Text),
    Column("address_city", Text),
    Column("address_state", Text, index=True),
    Column("address_postalcode", Text, index=True),
    Column("address_country", Text),
    Column("latitude", Double),
    Column("longitude", Double),
    Column("managing_organization_ref", Text, index=True),
    Column("type_codes", ARRAY(Text)),
)

healthcare_service = Table(
    "healthcare_service",
    resource_metadata,
    *_common_columns(),
    Column("name", Text),
    Column("provided_by_ref", Text, index=True),
    Column("location_refs", ARRAY(Text)),
    Column("type_codes", ARRAY(Text)),
    Column("specialty_codes", ARRAY(Text)),
    Column("category_codes", ARRAY(Text)),
    Column("active", Integer),
)

organization_affiliation = Table(
    "organization_affiliation",
    resource_metadata,
    *_common_columns(),
    Column("organization_ref", Text, index=True),
    Column("participating_organization_ref", Text, index=True),
    Column("network_refs", ARRAY(Text)),
    Column("location_refs", ARRAY(Text)),
    Column("healthcare_service_refs", ARRAY(Text)),
    Column("specialty_codes", ARRAY(Text)),
    Column("role_codes", ARRAY(Text)),
    Column("active", Integer),
)

insurance_plan = Table(
    "insurance_plan",
    resource_metadata,
    *_common_columns(),
    Column("name", Text),
    Column("type_codes", ARRAY(Text)),
    Column("owned_by_ref", Text, index=True),
    Column("administered_by_ref", Text),
    Column("coverage_area_refs", ARRAY(Text)),
    Column("network_refs", ARRAY(Text)),
    Column("plan_type_codes", ARRAY(Text)),
)

endpoint_resource = Table(
    "endpoint_resource",
    resource_metadata,
    *_common_columns(),
    Column("name", Text),
    Column("status", Text),
    Column("connection_type", Text),
    Column("address", Text),
    Column("managing_organization_ref", Text),
    Column("payload_types", ARRAY(Text)),
)

# --- shared (public) tables ------------------------------------------------
provenance = Table(
    "provenance",
    shared_metadata,
    Column("run_id", BigInteger, primary_key=True, autoincrement=True),
    Column("payer_id", Text, nullable=False),
    Column("source_base_url", Text),
    Column("resource_type", Text, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("status", Text, nullable=False),
    Column("page_count", Integer, default=0),
    Column("resource_count", Integer, default=0),
    Column("error_count", Integer, default=0),
    Column("prior_count", Integer),
    Column("pct_change", Double),
    Column("notes", JSONB),
)

data_quality_score = Table(
    "data_quality_score",
    shared_metadata,
    Column("payer_id", Text, primary_key=True),
    Column("scored_at", DateTime(timezone=True), primary_key=True),
    Column("weight_scheme", String(64), primary_key=True),
    Column("completeness", Double),
    Column("conformance", Double),
    Column("referential_integrity", Double),
    Column("freshness", Double),
    Column("uniqueness", Double),
    Column("consistency", Double),
    Column("composite", Double),
    Column("detail", JSONB),
)

# Durable bare-pagination cursor for `provdir etl --resume`. One row per
# (payer, resource) being pulled; written in the SAME transaction as each 5000-row
# batch commit so it never points past durable data. Deleted on clean exhaustion.
extract_checkpoint = Table(
    "extract_checkpoint",
    shared_metadata,
    Column("payer_id", Text, primary_key=True),
    Column("resource_type", Text, primary_key=True),
    # URL of the page being CONSUMED at commit time (not the next link): resuming
    # re-fetches at most one already-seen page, which ON CONFLICT dedupes.
    Column("resume_url", Text),
    Column("pages_done", Integer, nullable=False, server_default="0"),   # cumulative across resumes
    Column("rows_added", BigInteger, nullable=False, server_default="0"),
    Column("page_size", Integer),               # _count in effect (drives offset rewind)
    Column("params_fingerprint", Text),         # guards resuming into a different method/params
    Column("updated_at", DateTime(timezone=True), server_default=func.now(), nullable=False),
)

# resource_type (FHIR) -> table mapping used by the ETL loader.
RESOURCE_TABLES: dict[str, Table] = {
    "Organization": organization,
    "Practitioner": practitioner,
    "PractitionerRole": practitioner_role,
    "Location": location,
    "HealthcareService": healthcare_service,
    "OrganizationAffiliation": organization_affiliation,
    "InsurancePlan": insurance_plan,
    "Endpoint": endpoint_resource,
}

RESOURCE_TABLE_NAMES = [t.name for t in RESOURCE_TABLES.values()]

# Injection-safety invariant: these table/column names are interpolated into SQL
# strings across the ETL and quality modules. Assert at import that each is a strict
# identifier, so string-built DDL/DML over model identifiers is provably safe (all
# data *values* are passed as bound parameters, never interpolated).
import re as _re  # noqa: E402

_SAFE_ID = _re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
for _t in resource_metadata.tables.values():
    if not _SAFE_ID.match(_t.name):
        raise ValueError(f"unsafe table identifier: {_t.name!r}")
    for _c in _t.columns:
        if not _SAFE_ID.match(_c.name):
            raise ValueError(f"unsafe column identifier: {_c.name!r}")


def create_resource_schema(engine, schema: str) -> None:
    """Create `schema` and its 8 resource tables (idempotent)."""
    from sqlalchemy import text

    if not _SAFE_ID.match(schema):
        raise ValueError(f"unsafe schema name: {schema!r}")
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        translated = conn.execution_options(schema_translate_map={None: schema})
        resource_metadata.create_all(bind=translated, checkfirst=True)
        # Self-heal columns introduced after a schema was first created (per-payer
        # resource tables are not alembic-managed). Nullable ADD COLUMN is a
        # metadata-only, instant DDL even on multi-million-row tables.
        for _t in resource_metadata.tables.values():
            conn.execute(text(
                f'ALTER TABLE "{schema}"."{_t.name}" '
                'ADD COLUMN IF NOT EXISTS last_seen_at timestamptz'
            ))


def create_shared_tables(engine) -> None:
    shared_metadata.create_all(bind=engine, checkfirst=True)
