"""
Transform stage: loads raw JSON files (landed by extract.py) into
DuckDB raw tables.

Why this is called "transform" even though it looks like a simple
load: JSON -> relational table IS a transformation (flattening
nested structures, type coercion, schema enforcement). The
SQL-based metric computation lives in load.py, deliberately
separated from this file.
"""
import json
import logging
from pathlib import Path

import duckdb
import pandas as pd

from config.settings import DUCKDB_PATH, RAW_DATA_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("adfunnel.transform")

SCHEMA_SQL_PATH = Path(__file__).parent / "sql" / "schema.sql"


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Single shared connection factory.
    Why centralize this: every module that touches DuckDB (transform,
    load, validate, dashboard) should connect the same way, to the
    same file path. Duplicated connection logic is how you end up
    with two modules accidentally writing to two different DB files.
    """
    return duckdb.connect(str(DUCKDB_PATH))


def ensure_schema(con: duckdb.DuckDBPyConnection):
    """Applies schema.sql — safe to run repeatedly (CREATE TABLE IF NOT EXISTS)."""
    with open(SCHEMA_SQL_PATH) as f:
        con.execute(f.read())
    logger.info("Schema ensured (tables created if not already present)")


def _latest_file(prefix: str) -> Path:
    """
    Finds the most recently landed raw file matching a prefix
    (e.g. 'campaigns_20260706T120000Z.json').

    Why 'latest' instead of processing every file ever landed:
    In a real pipeline, you'd track processed files in a manifest
    table to avoid reprocessing. For this weekend project, we keep
    it simple: always transform the newest extraction. (This is a
    known simplification — worth calling out explicitly in your
    README as a "next steps" item, which is itself a good interview
    signal: knowing your own shortcuts.)
    """
    files = sorted(RAW_DATA_DIR.glob(f"{prefix}_*.json"))
    if not files:
        raise FileNotFoundError(f"No raw files found matching '{prefix}_*.json' in {RAW_DATA_DIR}")
    return files[-1]


def load_campaigns_to_duckdb(con: duckdb.DuckDBPyConnection):
    filepath = _latest_file("campaigns")
    with open(filepath) as f:
        campaigns = json.load(f)

    df = pd.DataFrame(campaigns)
    df["created_time"] = pd.to_datetime(df["created_time"])

    # Why DELETE + INSERT instead of raw INSERT:
    # Re-running transform.py on the same extraction shouldn't create
    # duplicate campaign rows. This is a manual upsert pattern for a
    # table keyed on a simple primary key (campaign_id).
    con.execute("DELETE FROM raw_campaigns WHERE campaign_id IN (SELECT campaign_id FROM df)")
    con.execute("""
        INSERT INTO raw_campaigns
            (campaign_id, campaign_name, objective, status, daily_budget, created_time)
        SELECT campaign_id, campaign_name, objective, status, daily_budget, created_time
        FROM df
    """)
    logger.info(f"Loaded {len(df)} campaigns from {filepath.name} into raw_campaigns")


def load_insights_to_duckdb(con: duckdb.DuckDBPyConnection):
    filepath = _latest_file("insights")
    with open(filepath) as f:
        insights = json.load(f)

    df = pd.DataFrame(insights)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Upsert pattern keyed on composite (campaign_id, date)
    con.execute("""
    DELETE FROM raw_insights
    WHERE EXISTS (
        SELECT 1 FROM df 
        WHERE raw_insights.campaign_id = df.campaign_id 
        AND raw_insights.date = df.date
    )
    """)
    con.execute("""
        INSERT INTO raw_insights (campaign_id, date, impressions, clicks, spend, leads)
        SELECT campaign_id, date, impressions, clicks, spend, leads
        FROM df
    """)
    logger.info(f"Loaded {len(df)} insight rows from {filepath.name} into raw_insights")


def run_transform():
    logger.info("=== Starting transform (JSON -> DuckDB raw tables) ===")
    con = get_connection()
    try:
        ensure_schema(con)
        load_campaigns_to_duckdb(con)
        load_insights_to_duckdb(con)
    finally:
        con.close()  # always release the DuckDB file lock
    logger.info("=== Transform complete ===")


if __name__ == "__main__":
    run_transform()