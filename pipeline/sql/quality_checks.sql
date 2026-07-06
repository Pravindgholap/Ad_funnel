-- ============================================================
-- WAREHOUSE-LEVEL DATA QUALITY CHECKS
-- These run AFTER load.py, against campaign_daily_metrics.
-- Each check returns 0 rows if PASSING, >0 rows if FAILING.
-- Why this convention: it lets validate.py treat every check
-- identically — "did this query return any rows? if yes, fail."
-- This uniform pattern is exactly how dbt tests and Great
-- Expectations checks are structured in real production stacks.
-- ============================================================

-- CHECK 1: No duplicate (campaign_id, date) pairs.
-- Even though the PRIMARY KEY should prevent this at the DB level,
-- we check explicitly anyway — defense in depth. A schema
-- constraint can be silently bypassed by DELETE+INSERT race
-- conditions or manual edits; this check would catch it either way.
-- name: no_duplicate_campaign_date
SELECT campaign_id, date, COUNT(*) as cnt
FROM campaign_daily_metrics
GROUP BY campaign_id, date
HAVING COUNT(*) > 1;

-- CHECK 2: No negative metric values (spend, ctr, cpc, cpa should
-- never be negative — a negative cost is a sign of a corrupted
-- upstream record, not a real business scenario).
-- name: no_negative_metrics
SELECT campaign_id, date, spend, ctr, cpc, cpa
FROM campaign_daily_metrics
WHERE spend < 0 OR ctr < 0 OR cpc < 0 OR cpa < 0;

-- CHECK 3: CTR must be between 0 and 1 (it's a proportion).
-- A CTR > 100% means clicks exceeded impressions, which should
-- already be impossible thanks to schemas.py — but if it ever
-- shows up here, it means data got into the warehouse WITHOUT
-- going through Pydantic validation (e.g. a manual insert, or a
-- bug in the pipeline order). This check is a tripwire for exactly
-- that scenario.
-- name: ctr_within_valid_range
SELECT campaign_id, date, ctr
FROM campaign_daily_metrics
WHERE ctr IS NOT NULL AND (ctr < 0 OR ctr > 1);

-- CHECK 4: Freshness — every campaign that is ACTIVE should have
-- data for the most recent date present in the warehouse.
-- Why this matters in a REAL job: this is the single most common
-- alert a data engineer configures in production — "did today's
-- data actually land, or did the pipeline silently stop running."
-- name: no_stale_active_campaigns
WITH max_date AS (
    SELECT MAX(date) AS latest_date FROM campaign_daily_metrics
)
SELECT c.campaign_id, c.campaign_name, c.status
FROM raw_campaigns c
CROSS JOIN max_date
WHERE c.status = 'ACTIVE'
  AND NOT EXISTS (
      SELECT 1 FROM campaign_daily_metrics m
      WHERE m.campaign_id = c.campaign_id
        AND m.date = max_date.latest_date
  );

-- CHECK 5: Row count sanity — expect at least 1 row per campaign
-- per day loaded (basic non-empty check, catches a totally empty
-- or truncated load).
-- name: minimum_row_count
SELECT COUNT(*) as row_count
FROM campaign_daily_metrics
HAVING COUNT(*) = 0;