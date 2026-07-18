"""prov-dir-eval: CMS-9115-F provider directory connection & data-quality evaluation."""

__version__ = "0.1.0"

# Repository root resolved relative to this file: src/provdir/__init__.py -> repo root.
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"
REFERENCE_DIR = REPO_ROOT / "reference"
OUTPUT_DIR = REPO_ROOT / "output"
SQL_DIR = REPO_ROOT / "sql"

__all__ = ["__version__", "REPO_ROOT", "CONFIG_DIR", "REFERENCE_DIR", "OUTPUT_DIR", "SQL_DIR"]
