"""
Orchestrator: chains extract -> transform -> load -> validate into a
single DAG-like run, with task-level retry, failure isolation, run
history persistence, and both manual + scheduled execution.

This is deliberately built to mirror Airflow's mental model without
the operational overhead of running an actual Airflow instance for
a weekend project:

  Airflow concept              -> Our equivalent
  ------------------------------------------------------------
  DAG                          -> PIPELINE_TASKS list (ordered)
  Task                         -> Task dataclass (name, fn, retries)
  Task instance / TaskInstance -> a row in task_runs table
  DagRun                       -> a row in pipeline_runs table
  Task-level retries           -> @retry-wrapped task execution
  Upstream failure behavior    -> depends_on_previous flag (see below)
  Scheduler                    -> APScheduler cron trigger
  Manual trigger ("Trigger DAG")-> run_pipeline(trigger_type="MANUAL")

Why this matters for the interview:
If asked "have you used Airflow," you can honestly say "I built a
lightweight orchestrator that implements Airflow's core primitives
task dependency, retry, run history, scheduled + manual triggers
to understand the concepts deeply before learning Airflow's DSL
specifically." That's a strong, honest answer for an L2 role.
"""
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from pipeline.extract import run_extraction
from pipeline.transform import run_transform, get_connection
from pipeline.load import run_load
from pipeline.validate import run_validation, DataQualityError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("adfunnel.orchestrator")

ORCH_SCHEMA_PATH = Path(__file__).parent / "sql" / "orchestration_schema.sql"


@dataclass
class Task:
    """
    One node in our DAG.

    depends_on_previous: if True, this task only runs if the
    previous task in the list SUCCEEDED. This mirrors Airflow's
    default `trigger_rule='all_success'` behavior — you don't want
    load.py running against raw data that transform.py failed to
    write, for instance.

    max_attempts: task-level retry count. Note this is SEPARATE
    from extract.py's own HTTP-level retries (Sprint 1) — this is
    a coarser retry for the whole task (e.g. re-running all of
    extraction if it fails for a reason unrelated to a single HTTP
    call, like a disk-full error writing raw JSON).
    """
    name: str
    fn: Callable[[], None]
    depends_on_previous: bool = True
    max_attempts: int = 2


# The DAG: an ordered list of tasks. Order = dependency order.
# Why extract->transform->load->validate specifically:
# validate MUST run last because it checks campaign_daily_metrics,
# which only exists after load. Running validate earlier would be
# checking a table that doesn't reflect the current run yet.
PIPELINE_TASKS = [
    Task(name="extract", fn=run_extraction, depends_on_previous=False, max_attempts=2),
    Task(name="transform", fn=run_transform, depends_on_previous=True, max_attempts=2),
    Task(name="load", fn=run_load, depends_on_previous=True, max_attempts=2),
    Task(name="validate", fn=run_validation, depends_on_previous=True, max_attempts=1),
    # max_attempts=1 for validate: retrying a FAILED data quality
    # check doesn't fix bad data — re-running it just wastes time.
    # Contrast with extract/transform/load, where a transient
    # failure (disk hiccup, momentary API flakiness) genuinely can
    # succeed on retry.
]


def _ensure_orchestration_schema(con):
    with open(ORCH_SCHEMA_PATH) as f:
        con.execute(f.read())


def _run_task_with_retry(task: Task, run_id: str, con) -> bool:
    """
    Executes a single task with retry, logging each attempt to
    task_runs. Returns True on success, False on final failure.

    Why we catch Exception broadly here (not just specific types):
    at the orchestration layer, we genuinely don't care WHAT failed
    inside a task — extract.py, transform.py etc already handle
    their own specific error types internally. The orchestrator's
    job is simply: did this task succeed, yes or no, and how many
    times did we try.
    """
    for attempt in range(1, task.max_attempts + 1):
        task_run_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        logger.info(f"[{task.name}] Attempt {attempt}/{task.max_attempts} starting...")

        try:
            task.fn()
            finished_at = datetime.now(timezone.utc)
            con.execute(
                """INSERT INTO task_runs
                   (task_run_id, run_id, task_name, started_at, finished_at,
                    status, attempt_number, error_message)
                   VALUES (?, ?, ?, ?, ?, 'SUCCESS', ?, NULL)""",
                [task_run_id, run_id, task.name, started_at, finished_at, attempt],
            )
            logger.info(f"[{task.name}] SUCCESS on attempt {attempt}")
            return True

        except Exception as e:
            finished_at = datetime.now(timezone.utc)
            error_msg = str(e)[:500]  # truncate — don't blow up the DB column on huge tracebacks
            con.execute(
                """INSERT INTO task_runs
                   (task_run_id, run_id, task_name, started_at, finished_at,
                    status, attempt_number, error_message)
                   VALUES (?, ?, ?, ?, ?, 'FAILED', ?, ?)""",
                [task_run_id, run_id, task.name, started_at, finished_at, attempt, error_msg],
            )
            logger.error(f"[{task.name}] FAILED on attempt {attempt}: {error_msg}")

            if attempt < task.max_attempts:
                wait_seconds = 3 * attempt  # simple linear backoff between task-level retries
                logger.info(f"[{task.name}] Retrying in {wait_seconds}s...")
                time.sleep(wait_seconds)

    return False  # all attempts exhausted


def run_pipeline(trigger_type: str = "MANUAL") -> str:
    """
    Runs the full DAG once, top to bottom, respecting
    depends_on_previous semantics. Returns the run_id for lookup.

    Why we don't just wrap this whole thing in one big try/except:
    we want granular, per-task visibility into WHERE a run failed,
    not just "the pipeline broke somewhere." This is the entire
    point of orchestration tooling versus a single monolithic script.
    """
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    logger.info(f"=== Pipeline run {run_id} starting (trigger={trigger_type}) ===")

    con = get_connection()
    _ensure_orchestration_schema(con)

    con.execute(
        "INSERT INTO pipeline_runs (run_id, started_at, status, trigger_type) VALUES (?, ?, 'RUNNING', ?)",
        [run_id, started_at, trigger_type],
    )
    con.close()  # release lock while tasks run (they open their own connections)

    previous_task_succeeded = True
    overall_success = True

    for task in PIPELINE_TASKS:
        if task.depends_on_previous and not previous_task_succeeded:
            # Mirrors Airflow's default trigger_rule=all_success:
            # skip downstream tasks rather than run them against a
            # known-broken upstream state.
            con = get_connection()
            con.execute(
                """INSERT INTO task_runs
                   (task_run_id, run_id, task_name, started_at, finished_at,
                    status, attempt_number, error_message)
                   VALUES (?, ?, ?, ?, ?, 'SKIPPED', 0, 'Upstream task failed')""",
                [str(uuid.uuid4()), run_id, task.name, datetime.now(timezone.utc),
                 datetime.now(timezone.utc)],
            )
            con.close()
            logger.warning(f"[{task.name}] SKIPPED — upstream dependency failed")
            overall_success = False
            continue

        con = get_connection()
        success = _run_task_with_retry(task, run_id, con)
        con.close()

        previous_task_succeeded = success
        if not success:
            overall_success = False

    finished_at = datetime.now(timezone.utc)
    final_status = "SUCCESS" if overall_success else "FAILED"

    con = get_connection()
    con.execute(
        "UPDATE pipeline_runs SET finished_at = ?, status = ? WHERE run_id = ?",
        [finished_at, final_status, run_id],
    )
    con.close()

    duration = (finished_at - started_at).total_seconds()
    logger.info(f"=== Pipeline run {run_id} finished: {final_status} ({duration:.1f}s) ===")
    return run_id


def print_run_history(limit: int = 10):
    """
    Utility to inspect recent pipeline runs — this is your
    'Airflow UI DAG runs list' equivalent, just via terminal.
    """
    con = get_connection()
    df = con.execute(
        """SELECT run_id, started_at, finished_at, status, trigger_type
           FROM pipeline_runs ORDER BY started_at DESC LIMIT ?""",
        [limit],
    ).fetchdf()
    con.close()
    print(df.to_string(index=False))


def print_task_history_for_run(run_id: str):
    """Drill-down equivalent of clicking into a specific DAG run in Airflow's UI."""
    con = get_connection()
    df = con.execute(
        """SELECT task_name, status, attempt_number, started_at, finished_at, error_message
           FROM task_runs WHERE run_id = ? ORDER BY started_at""",
        [run_id],
    ).fetchdf()
    con.close()
    print(df.to_string(index=False))


# ---------------------------------------------------------------
# Scheduling: mimics an Airflow DAG schedule_interval.
# ---------------------------------------------------------------

def start_scheduler(cron_expression: str = "*/10 * * * *"):
    """
    Runs the pipeline on a recurring schedule using cron syntax —
    the same scheduling language Airflow uses for schedule_interval.

    Default: every 10 minutes (deliberately short for demo purposes;
    a real Meta Ads pipeline would likely run hourly or daily,
    e.g. '0 * * * *' for hourly).

    Why BlockingScheduler and not a bare while+sleep loop:
    APScheduler handles misfire policy, timezone-aware cron
    parsing, and won't drift the way a naive `time.sleep(600)` loop
    does over long uptimes. This is a lightweight but legitimate
    orchestration tool, not a toy — it's a real option teams use
    when Airflow is overkill for their scale.
    """
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_pipeline,
        trigger=CronTrigger.from_crontab(cron_expression),
        kwargs={"trigger_type": "SCHEDULED"},
        id="adfunnel_etl_job",
        max_instances=1,  # CRITICAL: never let two pipeline runs overlap
        # Why max_instances=1 matters: if a run takes longer than the
        # schedule interval, APScheduler would otherwise start a
        # second overlapping run against the same DuckDB file,
        # causing lock contention or race conditions on upserts.
        # This is a real Airflow gotcha too (`catchup` + concurrency
        # settings exist for exactly this reason).
    )
    logger.info(f"Scheduler started. Cron: '{cron_expression}'. Press Ctrl+C to stop.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "schedule":
        # e.g. python -m pipeline.orchestrator schedule
        start_scheduler()
    elif len(sys.argv) > 1 and sys.argv[1] == "history":
        print_run_history()
    else:
        # Manual one-off trigger: python -m pipeline.orchestrator
        run_id = run_pipeline(trigger_type="MANUAL")
        print(f"\nRun ID: {run_id}\n")
        print_task_history_for_run(run_id)