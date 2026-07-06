-- Schema for AdFunnel ETL warehouse.
-- Why explicit DDL instead of letting pandas auto-infer types:
-- Auto-inferred schemas silently drift (e.g. a column that's all
-- integers today becomes float the moment one NULL sneaks in).
-- Explicit DDL is a contract — the same discipline a real
-- warehouse team enforces before letting data land in prod tables.

CREATE TABLE IF NOT EXISTS raw_campaigns (
    campaign_id     VARCHAR PRIMARY KEY,
    campaign_name   VARCHAR NOT NULL,
    objective       VARCHAR NOT NULL,
    status          VARCHAR NOT NULL,
    daily_budget    DOUBLE,
    created_time    TIMESTAMP,
    loaded_at       TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS raw_insights (
    campaign_id     VARCHAR NOT NULL,
    date            DATE NOT NULL,
    impressions     BIGINT,
    clicks          BIGINT,
    spend           DOUBLE,
    leads           BIGINT,
    loaded_at       TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (campaign_id, date)
    -- Why a composite PK: one campaign has exactly one row per day.
    -- This is our first line of defense against duplicate loads —
    -- DuckDB will reject a re-insert of the same (campaign_id, date).
);

-- Analytics layer: pre-aggregated metrics, refreshed by load.py.
-- Why a separate table instead of computing metrics on the fly
-- in every dashboard query: materializing the metrics layer is
-- the "T" in ETL — the dashboard should query pre-computed,
-- validated numbers, not re-derive business logic every page load.
CREATE TABLE IF NOT EXISTS campaign_daily_metrics (
    campaign_id     VARCHAR NOT NULL,
    campaign_name   VARCHAR,
    objective       VARCHAR,
    date            DATE NOT NULL,
    impressions     BIGINT,
    clicks          BIGINT,
    spend           DOUBLE,
    leads           BIGINT,
    ctr             DOUBLE,   -- click-through rate
    cpc             DOUBLE,   -- cost per click
    cpa             DOUBLE,   -- cost per acquisition (per lead)
    computed_at     TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (campaign_id, date)
);