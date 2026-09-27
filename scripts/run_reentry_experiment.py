"""
Real Optuna search (same objective, guardrails, 4-fold dev scoring, and
region-based selection as every other pass) against the re-entry-lag fix in
app/engines/step2_reentry_variant.py. Dev period only -- does not touch the
sealed holdout.
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

from app.engines import step2_reentry_variant
from app.optimize.folds import DEV_FOLDS
from app.optimize.objective import make_objective, run_fold
from app.optimize.search_space import tunable_param_keys
from app.optimize.selection import select_robust_region

CONFIG_PATH = PROJECT_ROOT / "strategies" / "experiments" / "step2_reentry_variant.json"
N_TRIALS = 100
SEED = 42


def main():
    strategy_config = json.loads(CONFIG_PATH.read_text())
    mode = "confirmed_locked"

    tunable = tunable_param_keys(strategy_config, mode)
    print(f"Tunable params for this experiment: {tunable}")
    assert tunable == ["reentryMinConditions"], f"expected only reentryMinConditions tunable, got {tunable}"

    objective = make_objective(step2_reentry_variant, strategy_config, mode, reward="calmar")

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

    # tiny discrete space (2,3,4) -- just show every value's score directly
    by_value = {}
    for t in completed:
        v = t.params["reentryMinConditions"]
        by_value.setdefault(v, []).append(t.value)
    print("\nScore by reentryMinConditions value (all completed trials, since space is tiny):")
    for v in sorted(by_value):
        scores = by_value[v]
        print(f"  {v}: n={len(scores)}  mean={np.mean(scores):.4f}  min={min(scores):.4f}  max={max(scores):.4f}")

    search_space = {"reentryMinConditions": {"min": 2, "max": 4}}
    region = select_robust_region(study, search_space)
    print(f"\nBest single trial: value={region.best_trial_value:.4f} params={region.best_trial_params}")
    print(f"Region pick ({region.method}): {region.region_size}/{region.region_out_of_top} trials in cluster -> {region.region_params}")

    baseline_params = {k: p["default"] for k, p in strategy_config["params"].items() if p.get("type") == "range" and p.get("tier") != "input"}
    variant_params = dict(baseline_params)
    variant_params.update(region.region_params)

    print(f"\n{'='*70}\nDev-fold comparison: today's behavior vs. region-picked re-entry fix\n{'='*70}")
    print(f"{'fold':>22s} {'base CAGR':>10s} {'fix CAGR':>9s} {'base Calmar':>12s} {'fix Calmar':>11s} {'base MaxDD':>11s} {'fix MaxDD':>10s}")
    base_scores, var_scores = [], []
    for fold_start, fold_end in DEV_FOLDS:
        rb = run_fold(step2_reentry_variant, baseline_params, fold_start, fold_end)
        rv = run_fold(step2_reentry_variant, variant_params, fold_start, fold_end)
        mb, mv = rb["metrics"], rv["metrics"]
        base_scores.append(mb["calmar"])
        var_scores.append(mv["calmar"])
        print(f"{fold_start}/{fold_end:>10s} {mb['cagr_pct']:10.2f} {mv['cagr_pct']:9.2f} {mb['calmar']:12.3f} {mv['calmar']:11.3f} {mb['max_drawdown_pct']:11.2f} {mv['max_drawdown_pct']:10.2f}")

    print(f"\nMedian Calmar: baseline={np.median(base_scores):.3f} variant={np.median(var_scores):.3f}")

    full_dev = {"startDate": "2011-01-01", "endDate": "2021-12-31", "fullHistory": True}
    rb_full = step2_reentry_variant.run({**baseline_params, **full_dev})
    rv_full = step2_reentry_variant.run({**variant_params, **full_dev})
    print(f"\nFull dev period (2011-2021), continuous:")
    print(f"  baseline: CAGR {rb_full['metrics']['cagr_pct']:.2f}%  Calmar {rb_full['metrics']['calmar']:.3f}  MaxDD {rb_full['metrics']['max_drawdown_pct']:.2f}%  Trades {rb_full['metrics']['total_trades']}")
    print(f"  variant:  CAGR {rv_full['metrics']['cagr_pct']:.2f}%  Calmar {rv_full['metrics']['calmar']:.3f}  MaxDD {rv_full['metrics']['max_drawdown_pct']:.2f}%  Trades {rv_full['metrics']['total_trades']}")

    out = {
        "region_params": region.region_params,
        "score_by_value": {str(k): {"mean": float(np.mean(v)), "n": len(v)} for k, v in by_value.items()},
        "baseline_full_dev": rb_full["metrics"],
        "variant_full_dev": rv_full["metrics"],
        "n_completed": len(completed), "n_rejected": len(rejected), "n_pruned": len(pruned),
    }
    out_path = PROJECT_ROOT / "scripts" / "_tmp_analysis" / "reentry_experiment_result.json"
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
