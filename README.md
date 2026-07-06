# AdFunnel ETL

A weekend-built ETL pipeline simulating Meta Ads data ingestion, transformation,
and marketing funnel analytics — built to demonstrate Data Engineer L2 competencies:
API ingestion (pagination/retries), SQL-based metric computation (CTR/CPC/CPA),
orchestration, and data quality validation.

## Stack
- **Mock API**: FastAPI (simulates Meta Ads Graph API behavior)
- **Storage/Compute**: DuckDB
- **Orchestration**: APScheduler (Airflow-pattern, lightweight)
- **Dashboard**: Streamlit

## Setup
\`\`\`bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
\`\`\`

## Run
\`\`\`bash
./run.sh                  # starts mock API on :8000
pytest tests/              # run tests
\`\`\`

