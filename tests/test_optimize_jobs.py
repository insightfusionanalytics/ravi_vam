"""
Integration tests for the optimizer's process management (app/optimize/
jobs.py) -- start/status/cancel/resume against REAL subprocesses running
scripts/run_optimization.py, not mocks. Each test points at its own
isolated scratch SQLite file via RAVI_VAM_OPTUNA_DB (inherited by the
spawned subprocess through normal environment inheritance) so nothing here
touches data/optuna/studies.db, the real study database.

These are slower than the rest of the suite (spawning real Python
processes that run real, if tiny, backtests) -- keep n_trials small.
"""

import os
import time

import pytest

STRATEGY_ID = "step1_upro_4state"
MODE = "confirmed_locked"


@pytest.fixture
def isolated_optuna_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test_studies.db"
    monkeypatch.setenv("RAVI_VAM_OPTUNA_DB", str(db_path))
    yield db_path


def _wait_until(predicate, timeout=30, interval=0.2):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_status_before_any_run_reports_not_exists(isolated_optuna_db):
    from app.optimize.jobs import get_status

    status = get_status(STRATEGY_ID, MODE)
    assert status.exists is False
    assert status.running is False


def test_start_launches_a_running_process_and_it_completes(isolated_optuna_db):
    from app.optimize.jobs import get_status, start_job

    status = start_job(STRATEGY_ID, MODE, n_trials=3)
    assert status.exists is True
    assert status.pid is not None
    # small n_trials should finish quickly; poll until the pid is gone
    finished = _wait_until(lambda: not get_status(STRATEGY_ID, MODE).running, timeout=20)
    assert finished, "run didn't finish within the timeout"

    final = get_status(STRATEGY_ID, MODE)
    assert final.total_trials == 3
    assert final.n_complete == 3  # no pruning possible before n_startup_trials=5
    assert final.best_value is not None
    assert final.best_params is not None


def test_resume_adds_to_existing_trials_not_replacing_them(isolated_optuna_db):
    from app.optimize.jobs import get_status, start_job

    start_job(STRATEGY_ID, MODE, n_trials=3)
    assert _wait_until(lambda: not get_status(STRATEGY_ID, MODE).running, timeout=20)
    after_first = get_status(STRATEGY_ID, MODE)
    assert after_first.total_trials == 3

    start_job(STRATEGY_ID, MODE, n_trials=2)
    assert _wait_until(lambda: not get_status(STRATEGY_ID, MODE).running, timeout=20)
    after_second = get_status(STRATEGY_ID, MODE)
    assert after_second.total_trials == 5, "resuming should add to the existing study, not start over"


def test_start_refuses_duplicate_while_already_running(isolated_optuna_db):
    from app.optimize.jobs import cancel_job, get_status, start_job

    status = start_job(STRATEGY_ID, MODE, n_trials=500)
    assert status.running is True

    with pytest.raises(RuntimeError, match="already running"):
        start_job(STRATEGY_ID, MODE, n_trials=5)

    # cleanup: don't leave a 500-trial run alive past this test
    cancel_job(STRATEGY_ID, MODE)
    assert _wait_until(lambda: not get_status(STRATEGY_ID, MODE).running, timeout=35)


def test_cancel_stops_a_running_job_and_keeps_completed_trials(isolated_optuna_db):
    from app.optimize.jobs import cancel_job, get_status, start_job

    status = start_job(STRATEGY_ID, MODE, n_trials=500)
    assert status.running is True

    # let a couple of trials complete so there's something to "keep"
    _wait_until(lambda: get_status(STRATEGY_ID, MODE).total_trials >= 2, timeout=10)

    result = cancel_job(STRATEGY_ID, MODE)
    assert result.running is False, "cancel should wait for the process to actually stop before returning"
    assert result.cancel_requested is True
    assert 0 < result.total_trials < 500, (
        f"expected cancel to stop well short of the full 500 trials, got {result.total_trials}"
    )

    # the study itself must still be there with its trials intact -- cancel
    # is not the same as deleting the run
    final = get_status(STRATEGY_ID, MODE)
    assert final.total_trials == result.total_trials


def test_is_pid_alive():
    from app.optimize.jobs import is_pid_alive

    assert is_pid_alive(os.getpid()) is True
    assert is_pid_alive(None) is False
    # a pid essentially guaranteed not to exist
    assert is_pid_alive(2**30) is False
