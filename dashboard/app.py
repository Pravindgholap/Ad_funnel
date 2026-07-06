"""
AdFunnel ETL Dashboard.

Why this dashboard queries DuckDB in READ-ONLY mode:
The orchestrator (pipeline/orchestrator.py) may be writing to the
same .duckdb file — either manually or via the scheduler. DuckDB
only allows ONE read-write connection to a file at a time, but
allows MULTIPLE read-only connections concurrently. Opening this
dashboard read-only means you can have the scheduler running in
one terminal and the dashboard open in a browser tab simultaneously
without lock contention. This is a real production consideration,
not just a nice-to-have — I've seen a dashboard crash a pipeline
(and vice versa) over exactly this kind of lock conflict.
"""
import sys
from pathlib import Path

# Why this is necessary: Streamlit's `streamlit run` adds only the
# script's OWN directory (dashboard/) to sys.path, not the project
# root. Without this, `from config.settings import ...` and
# `from pipeline...` fail with ModuleNotFoundError, even though the
# exact same imports work fine under pytest or `python -m`, which
# handle sys.path differently. This is a Streamlit-specific quirk,
# not a general Python packaging issue — and it must run BEFORE
# any project-local imports below.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from datetime import datetime

from config.settings import DUCKDB_PATH

st.set_page_config(page_title="AdFunnel ETL Dashboard", layout="wide")


@st.cache_resource
def get_read_only_connection():
    """
    @st.cache_resource (not @st.cache_data) because a DB connection
    is a stateful resource, not serializable data — caching it as a
    resource means Streamlit reuses the same connection across
    reruns instead of opening a new one on every button click.
    """
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


@st.cache_data(ttl=30)
def load_campaign_metrics() -> pd.DataFrame:
    """
    ttl=30: cache expires every 30 seconds. Why cache at all instead
    of querying fresh every rerun: Streamlit reruns the ENTIRE script
    on every widget interaction (e.g. moving a date slider). Without
    caching, you'd re-hit DuckDB dozens of times per second of
    interaction. ttl=30 balances freshness (if the scheduler just
    ran, you'll see new data within 30s) against query load.
    """
    con = get_read_only_connection()
    return con.execute("""
        SELECT campaign_id, campaign_name, objective, date,
               impressions, clicks, spend, leads, ctr, cpc, cpa
        FROM campaign_daily_metrics
        ORDER BY date
    """).fetchdf()


@st.cache_data(ttl=30)
def load_pipeline_runs() -> pd.DataFrame:
    con = get_read_only_connection()
    try:
        return con.execute("""
            SELECT run_id, started_at, finished_at, status, trigger_type
            FROM pipeline_runs ORDER BY started_at DESC LIMIT 20
        """).fetchdf()
    except duckdb.CatalogException:
        # Table won't exist if orchestrator has never run yet —
        # fail gracefully rather than crashing the whole dashboard.
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_validation_results() -> pd.DataFrame:
    con = get_read_only_connection()
    try:
        return con.execute("""
            SELECT check_name, status, failed_rows, checked_at
            FROM validation_results
            ORDER BY checked_at DESC LIMIT 20
        """).fetchdf()
    except duckdb.CatalogException:
        return pd.DataFrame()


# =================================================================
# HEADER
# =================================================================
st.title("📊 AdFunnel ETL — Campaign Performance Dashboard")
st.caption(
    "Simulated Meta Ads pipeline: mock API → DuckDB → validated metrics. "
    f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
)

try:
    df = load_campaign_metrics()
except Exception as e:
    st.error(
        f"Could not read from warehouse at {DUCKDB_PATH}. "
        f"Have you run `python -m pipeline.orchestrator` at least once? "
        f"Error: {e}"
    )
    st.stop()  # halt rendering rest of dashboard — no point showing empty charts

if df.empty:
    st.warning("campaign_daily_metrics is empty. Run the pipeline first.")
    st.stop()

df["date"] = pd.to_datetime(df["date"])

# =================================================================
# SIDEBAR FILTERS
# =================================================================
st.sidebar.header("Filters")

date_range = st.sidebar.date_input(
    "Date range",
    value=(df["date"].min(), df["date"].max()),
    min_value=df["date"].min(),
    max_value=df["date"].max(),
)
objectives = st.sidebar.multiselect(
    "Objective", options=sorted(df["objective"].unique()), default=list(df["objective"].unique())
)

# Guard against a user selecting only a single date (date_input can
# return a single value instead of a tuple mid-selection) — without
# this check, unpacking date_range below would throw.
if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range[0]

filtered = df[
    (df["date"] >= pd.Timestamp(start_date))
    & (df["date"] <= pd.Timestamp(end_date))
    & (df["objective"].isin(objectives))
]

if filtered.empty:
    st.warning("No data matches the current filters.")
    st.stop()

# =================================================================
# TOP-LEVEL KPIs
# Why sum-then-divide here too (not average of daily ratios):
# same Simpson's Paradox risk flagged in Sprint 2's metrics.sql —
# the dashboard must obey the same aggregation rule as the warehouse
# layer, or the two will disagree and undermine trust in the numbers.
# =================================================================
total_impressions = filtered["impressions"].sum()
total_clicks = filtered["clicks"].sum()
total_spend = filtered["spend"].sum()
total_leads = filtered["leads"].sum()

blended_ctr = total_clicks / total_impressions if total_impressions else None
blended_cpc = total_spend / total_clicks if total_clicks else None
blended_cpa = total_spend / total_leads if total_leads else None

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Impressions", f"{total_impressions:,.0f}")
col2.metric("Clicks", f"{total_clicks:,.0f}")
col3.metric("Spend", f"${total_spend:,.2f}")
col4.metric("Leads", f"{total_leads:,.0f}")
col5.metric(
    "Blended CTR",
    f"{blended_ctr:.2%}" if blended_ctr is not None else "N/A",
)

col6, col7 = st.columns(2)
col6.metric("Blended CPC", f"${blended_cpc:.2f}" if blended_cpc is not None else "N/A")
col7.metric("Blended CPA", f"${blended_cpa:.2f}" if blended_cpa is not None else "N/A")

st.divider()

# =================================================================
# TREND CHART: daily spend + leads over time
# =================================================================
st.subheader("Daily Trend")
daily = filtered.groupby("date", as_index=False).agg(
    impressions=("impressions", "sum"),
    clicks=("clicks", "sum"),
    spend=("spend", "sum"),
    leads=("leads", "sum"),
)
daily["ctr"] = daily["clicks"] / daily["impressions"].replace(0, pd.NA)

fig_trend = px.line(
    daily, x="date", y=["spend", "leads"],
    title="Spend vs. Leads Over Time",
    labels={"value": "Value", "variable": "Metric"},
)
st.plotly_chart(fig_trend, use_container_width=True)

# =================================================================
# FUNNEL VIEW — by objective (top-of-funnel to bottom)
# This is the direct visual proof of "marketing funnel understanding"
# from the JD — impressions -> clicks -> leads, by campaign objective.
# =================================================================
st.subheader("Funnel by Objective")
funnel_df = filtered.groupby("objective", as_index=False).agg(
    impressions=("impressions", "sum"),
    clicks=("clicks", "sum"),
    leads=("leads", "sum"),
)

funnel_long = funnel_df.melt(
    id_vars="objective", value_vars=["impressions", "clicks", "leads"],
    var_name="stage", value_name="count"
)
# Enforce funnel stage order — without this, plotly may sort
# alphabetically (clicks, impressions, leads) which visually breaks
# the funnel narrative.
stage_order = ["impressions", "clicks", "leads"]
funnel_long["stage"] = pd.Categorical(funnel_long["stage"], categories=stage_order, ordered=True)
funnel_long = funnel_long.sort_values("stage")

fig_funnel = px.funnel(
    funnel_long, x="count", y="stage", color="objective",
    title="Impressions → Clicks → Leads by Objective",
)
st.plotly_chart(fig_funnel, use_container_width=True)

# =================================================================
# CAMPAIGN-LEVEL TABLE (sum-then-divide rollup, matches metrics.sql
# Query 2 exactly — dashboard and warehouse SQL must agree)
# =================================================================
st.subheader("Campaign Performance")
campaign_rollup = filtered.groupby(
    ["campaign_id", "campaign_name", "objective"], as_index=False
).agg(
    impressions=("impressions", "sum"),
    clicks=("clicks", "sum"),
    spend=("spend", "sum"),
    leads=("leads", "sum"),
)
campaign_rollup["ctr"] = (campaign_rollup["clicks"] / campaign_rollup["impressions"]).round(4)
campaign_rollup["cpc"] = (campaign_rollup["spend"] / campaign_rollup["clicks"]).round(2)
campaign_rollup["cpa"] = (campaign_rollup["spend"] / campaign_rollup["leads"]).round(2)
campaign_rollup = campaign_rollup.sort_values("spend", ascending=False)

st.dataframe(
    campaign_rollup,
    use_container_width=True,
    column_config={
        "ctr": st.column_config.NumberColumn("CTR", format="%.2%%"),
        "cpc": st.column_config.NumberColumn("CPC", format="$%.2f"),
        "cpa": st.column_config.NumberColumn("CPA", format="$%.2f"),
        "spend": st.column_config.NumberColumn("Spend", format="$%.2f"),
    },
)

st.divider()

# =================================================================
# PIPELINE HEALTH PANEL
# Why this section exists at all: a Data Engineer's dashboard
# should expose pipeline health, not just business metrics. This
# directly answers "Monitor pipelines and resolve data issues"
# from the JD — the SAME dashboard that shows CTR/CPA also shows
# whether that data can be trusted.
# =================================================================
st.subheader("🔧 Pipeline Health")

health_col1, health_col2 = st.columns(2)

with health_col1:
    st.markdown("**Recent Pipeline Runs**")
    runs_df = load_pipeline_runs()
    if runs_df.empty:
        st.info("No pipeline runs recorded yet.")
    else:
        def _status_emoji(s):
            return {"SUCCESS": "✅", "FAILED": "❌", "RUNNING": "🔄"}.get(s, "❓")
        runs_df["status_display"] = runs_df["status"].apply(
            lambda s: f"{_status_emoji(s)} {s}"
        )
        st.dataframe(
            runs_df[["run_id", "started_at", "status_display", "trigger_type"]],
            use_container_width=True, hide_index=True,
        )

with health_col2:
    st.markdown("**Latest Data Quality Checks**")
    validation_df = load_validation_results()
    if validation_df.empty:
        st.info("No validation results recorded yet.")
    else:
        latest_check_time = validation_df["checked_at"].max()
        latest = validation_df[validation_df["checked_at"] == latest_check_time]
        for _, row in latest.iterrows():
            icon = "✅" if row["status"] == "PASS" else "⚠️"
            st.write(f"{icon} **{row['check_name']}** — {row['status']} ({row['failed_rows']} failed rows)")

st.caption(
    "Built with Python, FastAPI (mock Meta Ads API), DuckDB, and Streamlit. "
    "Source: github.com/Pravindgholap/adfunnel-etl"
)