# AdFunnel ETL

A weekend-built, end-to-end ETL pipeline simulating Meta Ads data ingestion,
transformation, validation, orchestration, and dashboarding.

## Architecture

Mock Meta Ads API (FastAPI)
    → Extract (Python, pagination + retry/backoff)
    → Transform (JSON → DuckDB raw tables)
    → Load (SQL: CTR/CPC/CPA computation, idempotent upserts)
    → Validate (Pydantic record checks + SQL data quality checks)
    → Orchestrate (task-level retry, dependency-aware skip logic, run history)
    → Dashboard (Streamlit: business metrics + pipeline health)

## Stack
- **Mock API**: FastAPI — simulates pagination, rate limiting (429), and
  transient failures (500) to force realistic error handling
- **Storage/Compute**: DuckDB
- **Orchestration**: APScheduler, implementing Airflow's core primitives
  (DAG task dependency, retry, skip-on-upstream-failure, run history)
- **Validation**: Pydantic (record-level) + raw SQL (set-level: duplicates,
  freshness, funnel-logic violations)
- **Dashboard**: Streamlit + Plotly

## Key Engineering Decisions
- **Idempotent upserts** (`ON CONFLICT DO UPDATE`) — safe to re-run the
  pipeline on the same data without duplicating rows
- **Sum-then-divide, never average-of-ratios** for CTR/CPC/CPA — avoids a
  Simpson's Paradox bug common in marketing analytics SQL
- **Retryable vs. permanent errors are explicitly separated** — 429/500/
  timeouts retry with backoff; 404s fail immediately
- **Task-level dependency skip logic** — a failed `extract` correctly skips
  `transform`/`load`/`validate` rather than running them against stale data
- **Data quality checks are severity-tiered** — critical checks (duplicates,
  empty table) halt the pipeline; non-critical checks (staleness) warn only

## Setup
\`\`\`bash
python -m venv venv
venv\Scripts\Activate.ps1      # Windows PowerShell
pip install -r requirements.txt
\`\`\`

## Run
\`\`\`bash
# Terminal 1: mock API
uvicorn mock_api.server:app --reload --port 8000

# Terminal 2: run the full pipeline once
python -m pipeline.orchestrator

# Terminal 2: or run it on a schedule
python -m pipeline.orchestrator schedule

# Terminal 3: dashboard
streamlit run dashboard/app.py
\`\`\`

## Testing
\`\`\`bash
pytest tests/ -v
\`\`\`

## Known Simplifications (honest scope notes)
- Extraction always processes the LATEST landed raw file, rather than
  tracking a processed-files manifest — fine for a single-pipeline demo,
  would need a manifest table at production scale to avoid reprocessing.
- Mock API's failure rates (10% on /insights, every 5th request on
  rate-limit) are fixed constants for demo purposes, not configurable.
- No cloud warehouse integration (BigQuery) — DuckDB was chosen deliberately
  for a lightweight, file-based weekend build. The SQL patterns
  (idempotent upserts, sum-then-divide aggregation) transfer directly.

## Live Demo
https://pravindgholap-ad-funnel-dashboardapp-gpjuk7.streamlit.app/

Note: the hosted version is bootstrapped from a committed seed snapshot
(data/seed/) since Streamlit Cloud doesn't run companion background
processes (the mock API server, scheduler). The full live pipeline —
mock API, retries, orchestrated scheduling, validation — runs locally
via the Setup/Run instructions below.
