-- ============================================================
-- CORE METRIC DEFINITIONS (for reference / interview talking points)
-- ============================================================
-- CTR (Click-Through Rate) = clicks / impressions
--   -> measures ad creative/targeting relevance
-- CPC (Cost Per Click)      = spend / clicks
--   -> measures cost efficiency of driving traffic
-- CPA (Cost Per Acquisition)= spend / leads
--   -> measures cost efficiency of the full funnel (impression -> lead)
--
-- All three are DIVISION operations, which means ALL THREE can
-- divide by zero (a campaign with 0 impressions, 0 clicks, or 0
-- leads on a given day is completely normal — e.g. a paused
-- campaign). NULLIF is used everywhere below to turn 0 into NULL
-- instead of crashing or returning Infinity. This single detail
-- is the most common bug I've seen junior engineers ship in
-- marketing analytics SQL.
-- ============================================================


-- Query 1: Daily grain, per-campaign metrics (this is what populates
-- campaign_daily_metrics)
INSERT INTO campaign_daily_metrics
    (campaign_id, campaign_name, objective, date,
     impressions, clicks, spend, leads, ctr, cpc, cpa)
SELECT
    i.campaign_id,
    c.campaign_name,
    c.objective,
    i.date,
    i.impressions,
    i.clicks,
    i.spend,
    i.leads,
    ROUND(i.clicks::DOUBLE / NULLIF(i.impressions, 0), 4)  AS ctr,
    ROUND(i.spend / NULLIF(i.clicks, 0), 2)                AS cpc,
    ROUND(i.spend / NULLIF(i.leads, 0), 2)                 AS cpa
FROM raw_insights i
JOIN raw_campaigns c ON c.campaign_id = i.campaign_id
ON CONFLICT (campaign_id, date) DO UPDATE SET
    impressions = EXCLUDED.impressions,
    clicks      = EXCLUDED.clicks,
    spend       = EXCLUDED.spend,
    leads       = EXCLUDED.leads,
    ctr         = EXCLUDED.ctr,
    cpc         = EXCLUDED.cpc,
    cpa         = EXCLUDED.cpa;
-- ON CONFLICT ... DO UPDATE ("upsert") is what makes this pipeline
-- IDEMPOTENT — re-running it on the same data won't duplicate rows,
-- it just refreshes them. Idempotency is a top-tier interview
-- keyword for data engineering; it's the difference between a
-- pipeline you can safely re-run after a failure and one that
-- corrupts your warehouse on every retry.


-- Query 2: Campaign-level rollup (aggregated across all days) —
-- this is the query you'd screen-share when asked "show me overall
-- campaign performance"
-- SELECT
--     campaign_id,
--     campaign_name,
--     objective,
--     SUM(impressions)                                   AS total_impressions,
--     SUM(clicks)                                         AS total_clicks,
--     ROUND(SUM(spend), 2)                                AS total_spend,
--     SUM(leads)                                          AS total_leads,
--     ROUND(SUM(clicks)::DOUBLE / NULLIF(SUM(impressions), 0), 4) AS ctr,
--     ROUND(SUM(spend) / NULLIF(SUM(clicks), 0), 2)               AS cpc,
--     ROUND(SUM(spend) / NULLIF(SUM(leads), 0), 2)                AS cpa
-- FROM campaign_daily_metrics
-- GROUP BY campaign_id, campaign_name, objective
-- ORDER BY total_spend DESC;
--
-- WHY WE AGGREGATE RAW SUMS FIRST, THEN DIVIDE — NOT AVERAGE THE
-- DAILY RATIOS:
-- AVG(daily_ctr) is a classic marketing-analytics bug. A campaign
-- with 10 impressions/5 clicks (CTR=50%) on a slow day and
-- 100,000 impressions/2,000 clicks (CTR=2%) on a big day does NOT
-- have an average CTR of 26%. The correct overall CTR is
-- total_clicks / total_impressions, which weights each day by its
-- actual volume. This is called "Simpson's Paradox" risk in
-- aggregation, and catching it is a strong signal of SQL maturity.


-- Query 3: Funnel view — top-of-funnel to bottom, by objective
-- (this is your "marketing funnel understanding" proof point)
-- SELECT
--     objective,
--     SUM(impressions)                                    AS impressions,
--     SUM(clicks)                                          AS clicks,
--     SUM(leads)                                           AS leads,
--     ROUND(SUM(clicks)::DOUBLE / NULLIF(SUM(impressions),0) * 100, 2) AS click_rate_pct,
--     ROUND(SUM(leads)::DOUBLE / NULLIF(SUM(clicks),0) * 100, 2)       AS lead_conversion_pct,
--     ROUND(SUM(leads)::DOUBLE / NULLIF(SUM(impressions),0) * 100, 4)  AS overall_funnel_conversion_pct
-- FROM campaign_daily_metrics
-- GROUP BY objective
-- ORDER BY impressions DESC;