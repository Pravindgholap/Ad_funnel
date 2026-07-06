-- Tracks every pipeline run and every task within that run.
-- Why we need this at all: "Familiarity with workflow orchestration"
-- isn't just about RUNNING tasks in order — it's about being able
-- to answer "did last night's run succeed? which task failed? how
-- long did it take?" without digging through log files. This is
-- exactly what Airflow's metadata database does under the hood.

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          VARCHAR PRIMARY KEY,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    status          VARCHAR,       -- RUNNING, SUCCESS, FAILED
    trigger_type    VARCHAR        -- MANUAL or SCHEDULED
);

CREATE TABLE IF NOT EXISTS task_runs (
    task_run_id     VARCHAR PRIMARY KEY,
    run_id          VARCHAR NOT NULL,
    task_name       VARCHAR NOT NULL,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    status          VARCHAR,        -- SUCCESS, FAILED, SKIPPED
    attempt_number  INTEGER,
    error_message   VARCHAR
);