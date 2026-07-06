"""
Tests for transform + load: proves DuckDB schema is correct, upserts
don't duplicate, and CTR/CPC/CPA formulas handle zero-division safely.
"""
import duckdb
import pytest


@pytest.fixture
def con():
    """
    Why an in-memory DuckDB for tests instead of the real warehouse file:
    Tests should never depend on (or pollute) real pipeline state.
    An in-memory DB gives us a clean, disposable warehouse per test.
    """
    connection = duckdb.connect(":memory:")
    with open("pipeline/sql/schema.sql") as f:
        connection.execute(f.read())
    yield connection
    connection.close()


def test_ctr_cpc_cpa_handle_zero_division(con):
    """
    The single most important correctness test in this whole project:
    a campaign with 0 clicks or 0 impressions must NOT crash the
    pipeline or produce Infinity — it must produce NULL.
    """
    con.execute("""
        INSERT INTO raw_campaigns (campaign_id, campaign_name, objective, status, daily_budget, created_time)
        VALUES ('cmp_test', 'Test Campaign', 'CONVERSIONS', 'ACTIVE', 100.0, '2026-01-01')
    """)
    con.execute("""
        INSERT INTO raw_insights (campaign_id, date, impressions, clicks, spend, leads)
        VALUES ('cmp_test', '2026-06-01', 0, 0, 0.0, 0)
    """)

    with open("pipeline/sql/metrics.sql") as f:
        full_sql = f.read()
    upsert_query = full_sql[: full_sql.find(";") + 1]
    con.execute(upsert_query)

    result = con.execute("""
        SELECT ctr, cpc, cpa FROM campaign_daily_metrics WHERE campaign_id = 'cmp_test'
    """).fetchone()

    ctr, cpc, cpa = result
    assert ctr is None  # NOT Infinity, NOT a crash
    assert cpc is None
    assert cpa is None


def test_upsert_is_idempotent(con):
    """
    Running the same insert twice should NOT duplicate rows —
    proves the ON CONFLICT DO UPDATE clause works as intended.
    """
    con.execute("""
        INSERT INTO raw_campaigns (campaign_id, campaign_name, objective, status, daily_budget, created_time)
        VALUES ('cmp_dup', 'Dup Campaign', 'TRAFFIC', 'ACTIVE', 50.0, '2026-01-01')
    """)
    con.execute("""
        INSERT INTO raw_insights (campaign_id, date, impressions, clicks, spend, leads)
        VALUES ('cmp_dup', '2026-06-01', 1000, 20, 40.0, 2)
    """)

    with open("pipeline/sql/metrics.sql") as f:
        full_sql = f.read()
    upsert_query = full_sql[: full_sql.find(";") + 1]

    con.execute(upsert_query)  # first run
    con.execute(upsert_query)  # second run — should UPDATE, not duplicate

    count = con.execute(
        "SELECT COUNT(*) FROM campaign_daily_metrics WHERE campaign_id = 'cmp_dup'"
    ).fetchone()[0]
    assert count == 1