"""
Process management for optimization runs: start (as a detached
subprocess), check status, and cancel -- either cleanly (a flag the
runner checks between trials) or forcibly (SIGTERM, if it doesn't respond
in time). This is the layer app/optimize/local_api.py calls.

Deliberately not distributed/multi-worker: only one runner process per
(strategy, mode) study is allowed at a time (see start_job), to keep the
operational model simple -- "which process do I cancel" always has one
answer. Optuna itself supports several processes optimizing the same
study concurrently, that's just not what this needs for a locally-run,
one-operator tool.
"""

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import optuna

from app.optimize.storage import default_storage_url, study_exists, study_name as _study_name, load_study

REPO_ROOT = Path(__file__).resolve().parents[2]


def _log_dir() -> Path:
    """Resolved at call time (not a module constant) so it follows
    RAVI_VAM_OPTUNA_DB the same way storage.default_storage_url() does --
    otherwise the test suite's isolated runs would still scatter real log
    files into data/optuna/logs/ even while using a scratch study DB."""
    db_url = default_storage_url()
    db_path = Path(db_url.removeprefix("sqlite:///"))
    log_dir = db_path.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir

CANCEL_GRACE_PERIOD_SECONDS = 30
CANCEL_POLL_INTERVAL_SECONDS = 1


def is_pid_alive(pid: int | None) -> bool:
    """os.kill(pid, 0) alone isn't enough: a child that has exited but
    hasn't been reaped is a zombie, and zombies still answer kill(pid, 0)
    as "exists". Since this process IS the parent of any pid it just
    started via start_job, a non-blocking waitpid first reaps it if it's
    finished -- without this, a completed run looks "running" forever to
    every caller (found the hard way: it hung both cancel_job's grace-
    period poll and the test suite's wait loops indefinitely).

    A pid started by a *different, now-gone* process (e.g. checking a
    study after this API server itself restarted) isn't our child at all,
    so waitpid raises ChildProcessError -- caught and falls through to the
    plain kill(pid, 0) check, which is the best available signal at that
    point (that process's own parent, whoever it is, is responsible for
    reaping it)."""
    if pid is None:
        return False
    try:
        reaped_pid, _ = os.waitpid(pid, os.WNOHANG)
        if reaped_pid == pid:
            return False  # just reaped it -- it had already exited
    except ChildProcessError:
        pass  # not our child -- fall through to a plain liveness check
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False
    except OSError:
        return False


@dataclass
class JobStatus:
    strategy_id: str
    mode: str
    study_name: str
    exists: bool
    running: bool
    pid: int | None = None
    total_trials: int = 0
    n_complete: int = 0
    n_pruned: int = 0
    n_rejected: int = 0
    best_value: float | None = None
    best_params: dict | None = None
    cancel_requested: bool = False
    last_heartbeat: float | None = None


def get_status(strategy_id: str, mode: str) -> JobStatus:
    name = _study_name(strategy_id, mode)
    if not study_exists(strategy_id, mode):
        return JobStatus(strategy_id=strategy_id, mode=mode, study_name=name, exists=False, running=False)

    study = load_study(strategy_id, mode)
    trials = study.trials
    n_complete = sum(1 for t in trials if t.state == optuna.trial.TrialState.COMPLETE)
    n_pruned = sum(1 for t in trials if t.state == optuna.trial.TrialState.PRUNED)
    # COMPLETE trials that hit a hard constraint score REJECTED_VALUE rather
    # than being marked PRUNED/FAIL -- count them separately here so a
    # status view doesn't read "172 complete" as "172 usable results."
    from app.optimize.objective import REJECTED_VALUE
    n_rejected = sum(1 for t in trials if t.state == optuna.trial.TrialState.COMPLETE and t.value == REJECTED_VALUE)

    pid = study.user_attrs.get("pid")
    running = is_pid_alive(pid)

    best_value, best_params = None, None
    usable = [t for t in trials if t.state == optuna.trial.TrialState.COMPLETE and t.value != REJECTED_VALUE]
    if usable:
        best = max(usable, key=lambda t: t.value)
        best_value, best_params = best.value, best.params

    return JobStatus(
        strategy_id=strategy_id,
        mode=mode,
        study_name=name,
        exists=True,
        running=running,
        pid=pid,
        total_trials=len(trials),
        n_complete=n_complete,
        n_pruned=n_pruned,
        n_rejected=n_rejected,
        best_value=best_value,
        best_params=best_params,
        cancel_requested=bool(study.user_attrs.get("cancel_requested", False)),
        last_heartbeat=study.user_attrs.get("last_heartbeat"),
    )


def start_job(strategy_id: str, mode: str, n_trials: int) -> JobStatus:
    """Launches scripts/run_optimization.py as a detached subprocess.
    Refuses to start a second runner against the same study while one is
    already alive -- check status first if unsure."""
    status = get_status(strategy_id, mode)
    if status.running:
        raise RuntimeError(
            f"a runner for {status.study_name} is already running (pid {status.pid}) -- "
            f"cancel it first, or just let it keep going"
        )

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    log_path = _log_dir() / f"{_study_name(strategy_id, mode)}__{timestamp}.log"
    log_file = open(log_path, "w")

    python = sys.executable
    script = REPO_ROOT / "scripts" / "run_optimization.py"
    subprocess.Popen(
        [python, str(script), strategy_id, mode, str(n_trials)],
        cwd=str(REPO_ROOT),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,  # detach from this process's session/group
        close_fds=True,
    )
    # The child will set its own pid as a user_attr once it starts up
    # (scripts/run_optimization.py); give it a moment to do so before
    # returning status, otherwise the immediate response would show
    # running=False despite the process having just launched.
    for _ in range(20):
        time.sleep(0.1)
        status = get_status(strategy_id, mode)
        if status.running:
            break
    return status


def cancel_job(strategy_id: str, mode: str) -> JobStatus:
    """Sets the cancel flag (checked between trials -- stops within a
    trial or two, keeping every completed trial). Falls back to SIGTERM
    if the process hasn't stopped on its own within
    CANCEL_GRACE_PERIOD_SECONDS -- e.g. if it's stuck deep inside a single
    slow engine call."""
    if not study_exists(strategy_id, mode):
        raise ValueError(f"no study for {strategy_id}/{mode}")

    study = load_study(strategy_id, mode)
    status = get_status(strategy_id, mode)
    if not status.running:
        study.set_user_attr("cancel_requested", True)  # harmless if nothing's running
        return status

    study.set_user_attr("cancel_requested", True)
    pid = status.pid

    deadline = time.time() + CANCEL_GRACE_PERIOD_SECONDS
    while time.time() < deadline:
        if not is_pid_alive(pid):
            return get_status(strategy_id, mode)
        time.sleep(CANCEL_POLL_INTERVAL_SECONDS)

    if is_pid_alive(pid):
        os.kill(pid, signal.SIGTERM)
        time.sleep(1)

    return get_status(strategy_id, mode)
