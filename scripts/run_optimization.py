#!/usr/bin/env python3
"""
Runs (or resumes) an Optuna study for one strategy/mode combination.

This is the actual worker: app/optimize/jobs.py launches it as a detached
subprocess so a study survives its parent (a local API server, or your own
terminal) being killed or restarted. Optuna's SQLite storage
(app/optimize/storage.py) persists every completed trial as it happens, so
re-running this script for the same study just continues adding trials on
top of what's already there -- it never re-does finished work.

Only Steps 1-4 are supported: they're the strategies Ravi actually asked
for, confirmed on the client's own numbers, AND the only ones with
startDate/endDate support -- required for the fold-based validation this
optimizer relies on (see app/optimize/folds.py). v3/v5/v5b are internal
R&D, never requested, and can't be walk-forward validated without adding
date-range support to them first -- deliberately out of scope here.

Usage:
  scripts/run_optimization.py <strategy_id> <mode> <n_trials>

  strategy_id: step1_upro_4state | step2_upro_tqqq_6state |
               step3_spxu_predatory_short | step4_svix_safety_valve
  mode:        confirmed_locked | free_everything
  n_trials:    additional trials to run this invocation (on top of
               whatever's already in the study, if resuming)
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import optuna

from app.optimize.objective import make_objective
from app.optimize.storage import create_or_load_study

ENGINE_MODULES = {
    "step1_upro_4state": "app.engines.step1",
    "step2_upro_tqqq_6state": "app.engines.step2",
    "step3_spxu_predatory_short": "app.engines.step3",
    "step4_svix_safety_valve": "app.engines.step4",
}


def _import_engine(dotted_path: str):
    import importlib
    return importlib.import_module(dotted_path)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("strategy_id", choices=sorted(ENGINE_MODULES.keys()))
    parser.add_argument("mode", choices=["confirmed_locked", "free_everything"])
    parser.add_argument("n_trials", type=int)
    args = parser.parse_args()

    strategy_config = json.loads((REPO_ROOT / "strategies" / f"{args.strategy_id}.json").read_text())
    engine_module = _import_engine(ENGINE_MODULES[args.strategy_id])

    study = create_or_load_study(args.strategy_id, args.mode)
    # Record our own PID so a caller that wants to force-kill a stuck run
    # (the SIGTERM fallback in app/optimize/jobs.py) knows which process.
    study.set_user_attr("pid", os.getpid())
    study.set_user_attr("cancel_requested", False)
    study.set_user_attr("last_heartbeat", time.time())

    def on_trial_complete(study: optuna.Study, trial: optuna.trial.FrozenTrial):
        study.set_user_attr("last_heartbeat", time.time())
        if study.user_attrs.get("cancel_requested"):
            print(f"cancel requested -- stopping after trial {trial.number}", flush=True)
            study.stop()

    print(f"study '{study.study_name}': {len(study.trials)} trials already present, running up to {args.n_trials} more", flush=True)

    objective = make_objective(engine_module, strategy_config, args.mode)
    study.optimize(objective, n_trials=args.n_trials, callbacks=[on_trial_complete])

    n_complete = len([t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE])
    print(f"stopped: {len(study.trials)} total trials in study ({n_complete} completed)", flush=True)
    if study.best_trial is not None:
        print(f"best value so far: {study.best_value}", flush=True)
        print(f"best params so far: {study.best_params}", flush=True)


if __name__ == "__main__":
    main()
