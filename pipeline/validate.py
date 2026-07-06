"""
Two-checkpoint validation layer:

CHECKPOINT A (record-level, pre-load): validate_raw_records()
    Runs Pydantic models against raw JSON before it's loaded into
    DuckDB. Bad records are quarantined (logged + excluded), not
    silently dropped, and not allowed to crash the whole batch.

CHECKPOINT B (set-level, post-load): run_quality_checks()
    Runs SQL checks from quality_checks.sql against the warehouse
    after transform+load. Results are stored in a validation_results
    table — an audit trail, exactly like a real dbt test run history.
"""
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone

from pydantic import ValidationError

from pipeline.schemas import CampaignRecord, InsightRecord
from pipeline.transform import get_connection
from config.settings import RAW_DATA_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("adfunnel.validate")

QUALITY_CHECKS_SQL_PATH = Path(__file__).parent / "sql" / "quality_checks.sql"


class DataQualityError(Exception):
    """
    Raised when a CRITICAL check fails — one severe enough that
    downstream consumption (dashboard, reporting) should be blocked.
    Not every failed check should raise this (e.g. a single
    quarantined record shouldn't halt the whole pipeline) — only
    checks that indicate systemic warehouse corruption.
    """
    pass


# ---------------------------------------------------------------
# CHECKPOINT A: Record-level validation (pre-load)
# ---------------------------------------------------------------

def _latest_file(prefix: str) -> Path:
    files = sorted(RAW_DATA_DIR.glob(f"{prefix}_*.json"))
    if not files:
        raise FileNotFoundError(f"No raw files found matching '{prefix}_*.json'")
    return files[-1]


def validate_raw_records() -> dict:
    """
    Validates every raw campaign + insight record against Pydantic
    schemas. Returns a summary dict rather than raising on the first
    bad record — we want to know ALL problems in one pass, not
    whack-a-mole through them one at a time on repeated runs.
    """
    results = {"campaigns": {"valid": 0, "invalid": 0, "errors": []},
               "insights": {"valid": 0, "invalid": 0, "errors": []}}

    # --- Campaigns ---
    with open(_latest_file("campaigns")) as f:
        campaigns = json.load(f)

    for record in campaigns:
        try:
            CampaignRecord(**record)
            results["campaigns"]["valid"] += 1
        except ValidationError as e:
            results["campaigns"]["invalid"] += 1
            results["campaigns"]["errors"].append(
                {"campaign_id": record.get("campaign_id", "UNKNOWN"), "error": str(e)}
            )

    # --- Insights ---
    with open(_latest_file("insights")) as f:
        insights = json.load(f)

    for record in insights:
        try:
            InsightRecord(**record)
            results["insights"]["valid"] += 1
        except ValidationError as e:
            results["insights"]["invalid"] += 1
            results["insights"]["errors"].append(
                {
                    "campaign_id": record.get("campaign_id", "UNKNOWN"),
                    "date": record.get("date", "UNKNOWN"),
                    "error": str(e),
                }
            )

    # Log a clear summary — this is what you'd screenshot for a
    # "data quality report" in a real job.
    logger.info(
        f"Campaign validation: {results['campaigns']['valid']} valid, "
        f"{results['campaigns']['invalid']} invalid"
    )
    logger.info(
        f"Insight validation: {results['insights']['valid']} valid, "
        f"{results['insights']['invalid']} invalid"
    )

    if results["campaigns"]["invalid"] > 0:
        for err in results["campaigns"]["errors"]:
            logger.warning(f"Invalid campaign {err['campaign_id']}: {err['error']}")

    if results["insights"]["invalid"] > 0:
        for err in results["insights"]["errors"]:
            logger.warning(
                f"Invalid insight {err['campaign_id']} on {err['date']}: {err['error']}"
            )

    # Why we raise ONLY on a high invalid RATE, not on any single
    # failure: a handful of bad records is normal data-quality noise
    # (worth logging, quarantining, monitoring) — but if more than
    # 5% of a batch is invalid, that's a signal something upstream
    # is systemically broken (e.g. the mock API's schema changed),
    # and we should stop the pipeline rather than load garbage.
    total_insights = results["insights"]["valid"] + results["insights"]["invalid"]
    if total_insights > 0:
        invalid_rate = results["insights"]["invalid"] / total_insights
        if invalid_rate > 0.05:
            raise DataQualityError(
                f"Insight invalid rate {invalid_rate:.1%} exceeds 5% threshold — "
                f"halting pipeline before load."
            )

    return results


# ---------------------------------------------------------------
# CHECKPOINT B: Warehouse-level SQL quality checks (post-load)
# ---------------------------------------------------------------

def _parse_named_queries(sql_text: str) -> dict[str, str]:
    """
    Splits quality_checks.sql into individual named queries using
    the '-- name: xyz' comment convention.
    Why this pattern instead of one file per check: keeping all
    checks in one .sql file makes them easy to review together
    (exactly how you'd review a dbt schema.yml of tests), while
    still letting us execute + report on them individually.
    """
    blocks = re.split(r"--\s*name:\s*(\w+)", sql_text)
    # blocks[0] is preamble/comments before first name; skip it
    queries = {}
    for i in range(1, len(blocks), 2):
        name = blocks[i].strip()
        query = blocks[i + 1].strip().rstrip(";")
        queries[name] = query
    return queries


# Checks that should HALT the pipeline (block dashboard consumption)
# vs. checks that should WARN only. This distinction matters: not
# every failed check is equally severe.
CRITICAL_CHECKS = {"no_duplicate_campaign_date", "minimum_row_count", "no_negative_metrics"}


def run_quality_checks() -> dict:
    """
    Executes every named check in quality_checks.sql against the
    live warehouse, logs a pass/fail per check, persists results to
    a validation_results audit table, and raises DataQualityError if
    any CRITICAL check fails.
    """
    con = get_connection()
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS validation_results (
                check_name   VARCHAR,
                status       VARCHAR,
                failed_rows  INTEGER,
                checked_at   TIMESTAMP DEFAULT current_timestamp
            )
        """)

        with open(QUALITY_CHECKS_SQL_PATH) as f:
            sql_text = f.read()

        checks = _parse_named_queries(sql_text)
        summary = {}
        critical_failures = []

        for name, query in checks.items():
            failing_rows = con.execute(query).fetchdf()
            passed = len(failing_rows) == 0
            status = "PASS" if passed else "FAIL"
            summary[name] = {"status": status, "failed_rows": len(failing_rows)}

            con.execute(
                "INSERT INTO validation_results (check_name, status, failed_rows) VALUES (?, ?, ?)",
                [name, status, len(failing_rows)],
            )

            if passed:
                logger.info(f"[PASS] {name}")
            else:
                logger.warning(f"[FAIL] {name} — {len(failing_rows)} offending row(s)")
                logger.warning(f"  Sample:\n{failing_rows.head(3)}")
                if name in CRITICAL_CHECKS:
                    critical_failures.append(name)

        if critical_failures:
            raise DataQualityError(
                f"Critical data quality checks failed: {critical_failures}. "
                f"Blocking downstream consumption until resolved."
            )

        return summary
    finally:
        con.close()


def run_validation():
    """Entry point: runs both checkpoints in sequence."""
    logger.info("=== Starting validation ===")
    record_results = validate_raw_records()
    quality_results = run_quality_checks()
    logger.info("=== Validation complete — all critical checks passed ===")
    return {"record_validation": record_results, "quality_checks": quality_results}


if __name__ == "__main__":
    run_validation()