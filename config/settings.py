"""
Centralized configuration for AdFunnel ETL.

Why this file exists:
Hardcoding paths/URLs across extract.py, transform.py, load.py is how
pipelines become unmaintainable. One source of truth means when we
move from local DuckDB to a real warehouse later, we change ONE file.
"""
import os
from pathlib import Path

# --- Project root (absolute, so this works regardless of where script is run from) ---
BASE_DIR = Path(__file__).resolve().parent.parent

# --- Directories ---
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
WAREHOUSE_DIR = BASE_DIR / "data" / "warehouse"
LOG_DIR = BASE_DIR / "logs"

# --- Ensure directories exist at import time ---
# Why: fails fast and loud at startup rather than a confusing
# FileNotFoundError three functions deep into a pipeline run.
for _dir in [RAW_DATA_DIR, WAREHOUSE_DIR, LOG_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)

# --- Mock API config ---
MOCK_API_BASE_URL = os.getenv("MOCK_API_BASE_URL", "http://127.0.0.1:8000")
MOCK_API_TIMEOUT_SECONDS = 10

# --- DuckDB ---
DUCKDB_PATH = WAREHOUSE_DIR / "adfunnel.duckdb"

# --- Retry policy (used by Sprint 1's extract.py) ---
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2