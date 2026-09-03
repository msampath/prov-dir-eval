"""Cross-check a payer's Practitioner/Organization NPIs against a bulk NPPES
NPI Files snapshot (https://download.cms.gov/nppes/NPI_Files.html).

Streams the (multi-GB) NPPES monthly file plus any weekly deltas straight out
of their zips, keeping only rows whose NPI appears in the target payer's
schema, and loads the result into public.nppes_verify for comparison queries
(deactivation status, entity type, registry practice state, etc. against the
payer's own captured records). Weekly deltas are applied after the monthly
base, latest file wins per NPI, so the result reflects NPPES state as of the
last delta file's coverage end date.

Usage:
    python scripts/nppes_verify.py --schema bcbs_az_new \
        --monthly data/nppes/NPPES_Data_Dissemination_August_2026_V2.zip \
        --weekly data/nppes/NPPES_Data_Dissemination_081026_081626_Weekly_V2.zip \
        --weekly data/nppes/NPPES_Data_Dissemination_081726_082326_Weekly_V2.zip
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import time
import zipfile
from pathlib import Path

import psycopg

from provdir.config import get_settings

# npidata_pfile column indices (0-based) we keep; see the file's own
# _fileheader.csv for the full ~330-column layout.
COLS = [0, 1, 4, 5, 6, 10, 28, 30, 31, 32, 36, 37, 38, 39, 40, 47, 329]
COL_NAMES = [
    "npi", "entity_type_code", "org_name", "last_name", "first_name", "credential",
    "addr_line1", "city", "state", "postal", "enum_date", "last_update_date",
    "deactivation_reason", "deactivation_date", "reactivation_date",
    "taxonomy_code_1", "certification_date",
]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.nppes_verify (
  npi text PRIMARY KEY,
  entity_type_code text, org_name text, last_name text, first_name text,
  credential text, addr_line1 text, city text, state text, postal text,
  enum_date text, last_update_date text, deactivation_reason text,
  deactivation_date text, reactivation_date text, taxonomy_code_1 text,
  certification_date text
)
"""


def db_conn():
    s = get_settings()
    return psycopg.connect(s.sqlalchemy_url.replace("postgresql+psycopg://", "postgresql://"))


def target_npis(conn, schema: str) -> set[str]:
    cur = conn.cursor()
    npis: set[str] = set()
    for table in ("practitioner", "organization"):
        cur.execute(f'SELECT DISTINCT npi FROM "{schema}"."{table}" WHERE npi IS NOT NULL')
        npis.update(row[0] for row in cur.fetchall())
    return npis


def npidata_member_name(zf: zipfile.ZipFile) -> str:
    for name in zf.namelist():
        if re.match(r"npidata_pfile_.*\.csv$", name) and "fileheader" not in name:
            return name
    raise ValueError("no npidata_pfile_*.csv member found in zip")


def filter_zip(zip_path: Path, targets: set[str], out_path: Path) -> tuple[int, int]:
    zf = zipfile.ZipFile(zip_path)
    member = npidata_member_name(zf)
    start = time.time()
    n_read = n_matched = 0
    with zf.open(member) as raw, open(out_path, "w", newline="", encoding="utf-8") as out:
        text = io.TextIOWrapper(raw, encoding="latin-1")
        reader = csv.reader(text)
        header = next(reader)
        writer = csv.writer(out)
        writer.writerow([header[i] for i in COLS])
        for row in reader:
            n_read += 1
            if row[0] in targets:
                writer.writerow([row[i] for i in COLS])
                n_matched += 1
            if n_read % 1_000_000 == 0:
                print(f"  read {n_read:,} matched {n_matched:,} elapsed {time.time()-start:.0f}s", flush=True)
    print(f"{zip_path.name}: read {n_read:,} matched {n_matched:,} -> {out_path}", flush=True)
    return n_read, n_matched


def load_csv(conn, csv_path: Path) -> None:
    cols = ",".join(COL_NAMES)
    update_clause = ",".join(f"{c}=EXCLUDED.{c}" for c in COL_NAMES if c != "npi")
    cur = conn.cursor()
    cur.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _nppes_stage (
          npi text, entity_type_code text, org_name text, last_name text, first_name text,
          credential text, addr_line1 text, city text, state text, postal text,
          enum_date text, last_update_date text, deactivation_reason text,
          deactivation_date text, reactivation_date text, taxonomy_code_1 text,
          certification_date text
        ) ON COMMIT PRESERVE ROWS
    """)
    cur.execute("TRUNCATE _nppes_stage")
    with open(csv_path, "r", encoding="utf-8") as f:
        with cur.copy(f"COPY _nppes_stage ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)") as copy:
            while data := f.read(1024 * 1024):
                copy.write(data)
    cur.execute(f"""
        INSERT INTO public.nppes_verify ({cols})
        SELECT {cols} FROM _nppes_stage
        ON CONFLICT (npi) DO UPDATE SET {update_clause}
    """)
    conn.commit()
    cur.execute("SELECT COUNT(*) FROM public.nppes_verify")
    print(f"{csv_path.name} loaded -> {cur.fetchone()[0]:,} rows in public.nppes_verify", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--schema", required=True, help="payer schema to cross-check, e.g. bcbs_az_new")
    ap.add_argument("--monthly", required=True, type=Path, help="NPPES monthly dissemination zip")
    ap.add_argument("--weekly", action="append", type=Path, default=[],
                     help="NPPES weekly delta zip, applied in the order given (repeatable)")
    ap.add_argument("--out-dir", type=Path, default=Path("output/nppes_verify"),
                     help="where to write the filtered CSVs (default: output/nppes_verify/)")
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    conn = db_conn()
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    cur.execute("TRUNCATE public.nppes_verify")
    conn.commit()

    targets = target_npis(conn, args.schema)
    print(f"target NPIs from {args.schema}: {len(targets):,}", flush=True)

    for zip_path in [args.monthly, *args.weekly]:
        out_path = args.out_dir / f"{zip_path.stem}.csv"
        filter_zip(zip_path, targets, out_path)
        load_csv(conn, out_path)

    print("done", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
