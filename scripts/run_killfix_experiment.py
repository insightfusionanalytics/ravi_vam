"""
Runs the real Optuna search (same objective, guardrails, 4-fold dev scoring,
and region-based selection as every other pass in OPTIMIZATION_REPORT.md)
against the kill-switch whipsaw fix in app/engines/step2_killfix_variant.py.

Only killBufferBps and killConfirmDays are tunable -- every other parameter
is held fixed at the already-recommended "confirmed values locked" tuned
config, so this isolates the new mechanism's effect instead of re-mixing it
with a fresh full search. See strategies/experiments/step2_killfix_variant.json
for the tier assignments.

Dev period only (2011-2021, via app.optimize.folds.DEV_FOLDS). Does NOT touch
the sealed holdout -- this is the "worth taking further?" check that should
happen before spending that one remaining disclosed look.
"""

import json
import sys
from pathlib import Path

import numpy as np
import optuna
from optuna.samplers import TPESampler
from optuna.pruners import MedianPruner

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.engines import step2, step2_killfix_variant
from app.optimize.folds import DEV_FOLDS
from app.optimize.objective import make_objective, run_fold, _fold_score
from app.optimize.search_space import tunable_param_keys
from app.optimize.selection import select_robust_region

CONFIG_PATH = PROJECT_ROOT / "strategies" / "experiments" / "step2_killfix_variant.json"
N_TRIALS = 150
SEED = 42


def main():
    strategy_config = json.loads(CONFIG_PATH.read_text())
    mode = "confirmed_locked"

    tunable = tunable_param_keys(strategy_config, mode)
    print(f"Tunable params for this experiment: {tunable}")
    assert set(tunable) == {"killBufferBps", "killConfirmDays"}, \
        f"expected only the two new params tunable, got {tunable}"

    objective = make_objective(step2_killfix_variant, strategy_config, mode, reward="calmar")

    study = optuna.create_study(
        direction="maximize",
        sampler=TPESampler(seed=SEED),
        pruner=MedianPruner(),
    )
    study.optimize(objective, n_trials=N_TRIALS, show_progress_bar=False)

    completed = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    rejected = [t for t in study.trials if t.value == -1000.0]
    pruned = [t for t in study.trials if t.state == optuna.trial.TrialState.PRUNED]
    print(f"\n{len(study.trials)} trials: {len(completed)} completed, {len(rejected)} rejected by guardrails, {len(pruned)} pruned")

    search_space = {
        key: {"min": strategy_config["params"][key]["min"], "max": strategy_config["params"][key]["max"]}
        for key in tunable
    }
    region = select_robust_region(study, search_space)

    print(f"\nBest single trial: value={region.best_trial_value:.4f} params={region.best_trial_params}")
    print(f"Region pick ({region.method}): {region.region_size}/{region.region_out_of_top} trials in cluster")
    print(f"Region params: {region.region_params}")
    print(f"Region agrees with best trial: {region.agrees_with_best_trial}")
    for key, spread in region.param_spread.items():
        print(f"  {key}: median={spread['median']:.1f} std={spread['std']:.2f} (n={spread['n']})")

    # --- Compare region pick vs baseline (killBufferBps=0, killConfirmDays=1 == today's behavior) on the same dev folds ---
    baseline_params = {k: p["default"] for k, p in strategy_config["params"].items() if p.get("type") == "range" and p.get("tier") != "input"}
    variant_params = dict(baseline_params)
    variant_params.update(region.region_params)

    print(f"\n{'='*70}\nDev-fold comparison: today's behavior vs. region-picked fix\n{'='*70}")
    print(f"{'fold':>20s} {'baseline calmar':>16s} {'variant calmar':>16s} {'baseline maxdd':>15s} {'variant maxdd':>15s}")
    base_scores, var_scores = [], []
    for fold_start, fold_end in DEV_FOLDS:
        rb = run_fold(step2_killfix_variant, baseline_params, fold_start, fold_end)
        rv = run_fold(step2_killfix_variant, variant_params, fold_start, fold_end)
        mb, mv = rb["metrics"], rv["metrics"]
        base_scores.append(mb["calmar"])
        var_scores.append(mv["calmar"])
        print(f"{fold_start}/{fold_end:>9s} {mb['calmar']:16.3f} {mv['calmar']:16.3f} {mb['max_drawdown_pct']:15.2f} {mv['max_drawdown_pct']:15.2f}")

    print(f"\nMedian Calmar: baseline={np.median(base_scores):.3f} variant={np.median(var_scores):.3f}")

    # Full continuous dev period (2011-2021) for headline CAGR/MaxDD/whipsaw comparison
    full_dev = {"startDate": "2011-01-01", "endDate": "2021-12-31", "fullHistory": True}
    rb_full = step2_killfix_variant.run({**baseline_params, **full_dev})
    rv_full = step2_killfix_variant.run({**variant_params, **full_dev})
    print(f"\nFull dev period (2011-2021), continuous:")
    print(f"  baseline: CAGR {rb_full['metrics']['cagr_pct']:.2f}%  Calmar {rb_full['metrics']['calmar']:.3f}  MaxDD {rb_full['metrics']['max_drawdown_pct']:.2f}%  Trades {rb_full['metrics']['total_trades']}")
    print(f"  variant:  CAGR {rv_full['metrics']['cagr_pct']:.2f}%  Calmar {rv_full['metrics']['calmar']:.3f}  MaxDD {rv_full['metrics']['max_drawdown_pct']:.2f}%  Trades {rv_full['metrics']['total_trades']}")

    out = {
        "region_params": region.region_params,
        "region_method": region.method,
        "region_size": region.region_size,
        "region_out_of_top": region.region_out_of_top,
        "agrees_with_best_trial": region.agrees_with_best_trial,
        "best_trial_params": region.best_trial_params,
        "best_trial_value": region.best_trial_value,
        "baseline_dev_fold_calmars": base_scores,
        "variant_dev_fold_calmars": var_scores,
        "baseline_full_dev": rb_full["metrics"],
        "variant_full_dev": rv_full["metrics"],
        "n_completed": len(completed),
        "n_rejected": len(rejected),
        "n_pruned": len(pruned),
    }
    out_path = PROJECT_ROOT / "scripts" / "_tmp_analysis" / "killfix_experiment_result.json"
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
