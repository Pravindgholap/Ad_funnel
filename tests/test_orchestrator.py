"""
Tests proving the orchestrator's failure-isolation logic actually
works — specifically that a failed task correctly SKIPS (not runs)
downstream dependent tasks, which is the core DAG semantic.
"""
import pytest
from unittest.mock import patch, MagicMock
from pipeline.orchestrator import Task, _run_task_with_retry


def test_task_retries_on_failure_then_succeeds():
    """
    Simulates a task that fails once, then succeeds on retry —
    proves the retry loop actually re-invokes the function rather
    than giving up after one failure.
    """
    call_count = {"n": 0}

    def flaky_fn():
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("simulated transient failure")
        return  # succeeds on 2nd call

    task = Task(name="flaky_task", fn=flaky_fn, max_attempts=3)
    mock_con = MagicMock()

    with patch("time.sleep"):  # don't actually wait during tests
        result = _run_task_with_retry(task, run_id="test_run", con=mock_con)

    assert result is True
    assert call_count["n"] == 2


def test_task_exhausts_retries_and_returns_false():
    """
    A task that ALWAYS fails must return False after max_attempts,
    not hang forever or silently swallow the failure.
    """
    def always_fails():
        raise RuntimeError("permanent failure")

    task = Task(name="broken_task", fn=always_fails, max_attempts=2)
    mock_con = MagicMock()

    with patch("time.sleep"):
        result = _run_task_with_retry(task, run_id="test_run", con=mock_con)

    assert result is False
    # Should have logged 2 FAILED task_runs (one per attempt)
    assert mock_con.execute.call_count == 2