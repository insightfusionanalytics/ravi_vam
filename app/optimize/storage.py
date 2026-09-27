"""
Shared Optuna storage for the parameter optimizer -- a single SQLite file
so a study's progress and completed trials survive a runner process being
killed or restarted. Resuming a study just means creating/loading the same
study name against this storage and calling study.optimize() again;
Optuna already has every prior completed trial on disk, so it picks up
where it left off rather than starting over.

This lives entirely outside the deployed app: never mounted into
app/main.py, never shipped by deploy.sh, never reachable from the
production domain. Per the chosen design (2026-09-24), heavy optimization
runs happen locally -- Ravi never triggers a run himself, he only ever
sees finished results that get explicitly published (Step 8/9). Keeping
this module import-isolated from app.main is what makes that boundary
real rather than just a UI convention.
"""

import os
from pathlib import Path

import optuna

STORAGE_DIR = Path(__file__).resolve().parents[2] / "data" / "optuna"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)
STORAGE_PATH = STORAGE_DIR / "studies.db"

VALID_MODES = ("confirmed_locked", "free_everything")


def default_storage_url() -> str:
    """Resolved fresh on every call, deliberately NOT cached as a module
    constant -- a module-level constant would be frozen at first import,
    which would silently ignore RAVI_VAM_OPTUNA_DB if anything imported
    this module before a test (or a caller) set the env var. RAVI_VAM_OPTUNA_DB
    overrides the storage file; the test suite uses it to point an entire
    process tree (parent + any subprocess it launches, via inherited
    environment) at an isolated scratch database instead of the real one."""
    override = os.environ.get("RAVI_VAM_OPTUNA_DB")
    return f"sqlite:///{override or STORAGE_PATH}"


def get_storage(url: str | None = None) -> optuna.storages.RDBStorage:
    return optuna.storages.RDBStorage(
        url=url or default_storage_url(),
        engine_kwargs={"connect_args": {"timeout": 30}},
    )


def study_name(strategy_id: str, mode: str) -> str:
    """One study per (strategy, mode) pair, e.g.
    'step2_upro_tqqq_6state__confirmed_locked'. Deterministic, so starting
    the same combination again resumes it instead of creating a duplicate
    study that would split the trial history in two."""
    if mode not in VALID_MODES:
        raise ValueError(f"mode must be one of {VALID_MODES}, got {mode!r}")
    return f"{strategy_id}__{mode}"


def create_or_load_study(strategy_id: str, mode: str, storage_url: str | None = None) -> optuna.Study:
    """TPESampler is re-instantiated fresh on every call (including a
    resume after a restart) -- Optuna doesn't persist a sampler's internal
    RNG stream across processes. This is fine functionally: TPE builds its
    proposal distribution from the trial history already in storage, not
    from hidden sampler state, so a resumed run still continues sensibly.
    It just won't be bit-for-bit identical to an uninterrupted run with the
    same seed, which doesn't matter here since nothing downstream depends
    on exact trial-for-trial reproducibility, only on the final
    distribution of good trials."""
    return optuna.create_study(
        study_name=study_name(strategy_id, mode),
        storage=get_storage(storage_url),
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=42),
        pruner=optuna.pruners.MedianPruner(n_startup_trials=5, n_warmup_steps=0),
        load_if_exists=True,
    )


def load_study(strategy_id: str, mode: str, storage_url: str | None = None) -> optuna.Study:
    """Read-only-ish access (status/cancel endpoints) -- doesn't create
    anything if the study doesn't exist yet."""
    return optuna.load_study(
        study_name=study_name(strategy_id, mode),
        storage=get_storage(storage_url),
    )


def study_exists(strategy_id: str, mode: str, storage_url: str | None = None) -> bool:
    try:
        load_study(strategy_id, mode, storage_url)
        return True
    except KeyError:
        return False
