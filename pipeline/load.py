"""
Load stage: runs the SQL metric computation (metrics.sql) to populate
campaign_daily_metrics from the raw tables.

Why this is a separate file from transform.py:
transform.py = "get data INTO the warehouse, structurally correct"
load.py      = "derive business metrics FROM the warehouse"
Keeping these separate means if someone asks "where's the CTR
formula defined," there's exactly one place to look — not buried
inside a JSON-parsing function.
"""
import logging
from pathlib import Path

from pipeline.transform import get_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("adfunnel.load")

METRICS_SQL_PATH = Path(__file__).parent / "sql" / "metrics.sql"


def _extract_upsert_query(sql_file_contents: str) -> str:
    """
    metrics.sql contains the live upsert query plus commented-out
    reference queries (2 and 3) for documentation purposes.
    We only execute the first INSERT statement (Query 1) here —
    queries 2/3 are meant to be run ad-hoc or by the dashboard.
    """
    # Grab everything up to (and including) the closing semicolon
    # of the first INSERT statement.
    end_idx = sql_file_contents.find(";") + 1
    return sql_file_contents[:end_idx]


def run_load():
    logger.info("=== Starting load (computing CTR/CPC/CPA into campaign_daily_metrics) ===")
    con = get_connection()
    try:
        with open(METRICS_SQL_PATH) as f:
            full_sql = f.read()

        upsert_query = _extract_upsert_query(full_sql)
        con.execute(upsert_query)

        row_count = con.execute("SELECT COUNT(*) FROM campaign_daily_metrics").fetchone()[0]
        logger.info(f"campaign_daily_metrics now has {row_count} rows")

        # Sanity peek — a habit worth keeping: always eyeball a few
        # rows after a load, don't just trust "it ran without error."
        sample = con.execute("""
            SELECT campaign_name, date, impressions, clicks, ctr, cpc, cpa
            FROM campaign_daily_metrics
            ORDER BY date DESC
            LIMIT 5
        """).fetchdf()
        logger.info(f"Sample rows:\n{sample}")

    finally:
        con.close()
    logger.info("=== Load complete ===")


if __name__ == "__main__":
    run_load()