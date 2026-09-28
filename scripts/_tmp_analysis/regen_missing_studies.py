"""Regenerates the 4 studies that were never persisted (martin reward pass,
and the 3 whipsaw-fix experiments), this time saving to data/optuna/studies.db
so they're captured for good. Same seed=42, same n_trials as originally run --
deterministic, dev-period only, no sealed holdout touched."""
import json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import optuna
from optuna.samplers import TPESampler
from optuna.pruners import MedianPruner

from app.optimize.objective import make_objective
from app.optimize.search_space import tunable_param_keys

STORAGE_URL = "sqlite:////Users/chirag/ravi_vam/data/optuna/studies.db"

def run_study(study_name, engine_module, strategy_config, mode, reward, n_trials, seed=42):
    storage = optuna.storages.RDBStorage(url=STORAGE_URL)
    try:
        optuna.delete_study(study_name=study_name, storage=storage)
    except KeyError:
        pass
    study = optuna.create_study(
        study_name=study_name, storage=storage, direction="maximize",
        sampler=TPESampler(seed=seed), pruner=MedianPruner(),
    )
    objective = make_objective(engine_module, strategy_config, mode, reward=reward)
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
    print(f"{study_name}: {len(study.trials)} trials saved")

if __name__ == "__main__":
    PROJECT_ROOT = Path("/Users/chirag/ravi_vam")

    # 1. Martin-reward pass for step2 (same search space as confirmed_locked)
    from app.engines import step2
    step2_config = json.loads((PROJECT_ROOT / "strategies" / "step2_upro_tqqq_6state.json").read_text())
    run_study("step2_upro_tqqq_6state__martin", step2, step2_config, "confirmed_locked", "martin", 200)

    # 2. Kill-switch fix
    from app.engines import step2_killfix_variant
    killfix_config = json.loads((PROJECT_ROOT / "strategies" / "experiments" / "step2_killfix_variant.json").read_text())
    run_study("step2_killfix_variant__confirmed_locked", step2_killfix_variant, killfix_config, "confirmed_locked", "calmar", 150)

    # 3. Re-entry boolean fix
    from app.engines import step2_reentry_variant
    reentry_config = json.loads((PROJECT_ROOT / "strategies" / "experiments" / "step2_reentry_variant.json").read_text())
    run_study("step2_reentry_variant__confirmed_locked", step2_reentry_variant, reentry_config, "confirmed_locked", "calmar", 100)

    # 4. Continuous re-entry fix
    from app.engines import step2_continuous_reentry_variant
    creentry_config = json.loads((PROJECT_ROOT / "strategies" / "experiments" / "step2_continuous_reentry_variant.json").read_text())
    run_study("step2_continuous_reentry_variant__confirmed_locked", step2_continuous_reentry_variant, creentry_config, "confirmed_locked", "calmar", 150)
