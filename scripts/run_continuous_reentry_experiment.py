"""
Real Optuna search against the continuous re-entry tolerance fix in
app/engines/step2_continuous_reentry_variant.py. Same objective, guardrails,
4-fold dev scoring, and region-based selection as every other pass. Dev
period only -- does not touch the sealed holdout.
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

from app.engines import step2_continuous_reentry_variant as variant_mod
from app.optimize.folds import DEV_FOLDS
from app.optimize.objective import make_objective, run_fold
from app.optimize.search_space import tunable_param_keys
from app.optimize.selection import select_robust_region

CONFIG_PATH = PROJECT_ROOT / "strategies" / "experiments" / "step2_continuous_reentry_variant.json"
N_TRIALS = 150
SEED = 42


def main():
    strategy_config = json.loads(CONFIG_PATH.read_text())
    mode = "confirmed_locked"

    tunable = tunable_param_keys(strategy_config, mode)
    print(f"Tunable params: {tunable}")
    assert tunable == ["reentryToleranceBps"]

    objective = make_objective(variant_mod, strategy_config, mode, reward="calmar")

    study = optuna.create_study(direction="maximize", sampler=TPESampler(seed=SEED), pruner=MedianPruner())
    study.optimize(objective, n_trials=N_TRIALS, show_progress_bar=False)

    completed = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    rejected = [t for t in study.trials if t.value == -1000.0]
    pruned = [t for t in study.trials if t.state == optuna.trial.TrialState.PRUNED]
    print(f"\n{len(study.trials)} trials: {len(completed)} completed, {len(rejected)} rejected by guardrails, {len(pruned)} pruned")

    by_value = {}
    for t in completed:
        v = t.params["reentryToleranceBps"]
        by_value.setdefault(v, []).append(t.value)
    print("\nScore by reentryToleranceBps (bps):")
    for v in sorted(by_value):
        scores = by_value[v]
        print(f"  {v:4d}bps: n={len(scores):3d}  mean={np.mean(scores):.4f}")

    search_space = {"reentryToleranceBps": {"min": 0, "max": 500}}
    region = select_robust_region(study, search_space)
    print(f"\nBest single trial: value={region.best_trial_value:.4f} params={region.best_trial_params}")
    print(f"Region pick ({region.method}): {region.region_size}/{region.region_out_of_top} trials in cluster -> {region.region_params}")
    for key, spread in region.param_spread.items():
        print(f"  {key}: median={spread['median']:.1f} std={spread['std']:.2f} (n={spread['n']})")

    baseline_params = {k: p["default"] for k, p in strategy_config["params"].items() if p.get("type") == "range" and p.get("tier") != "input"}
    variant_params = dict(baseline_params)
    variant_params.update(region.region_params)

    print(f"\n{'='*70}\nDev-fold comparison: today's behavior vs. region-picked continuous fix\n{'='*70}")
    print(f"{'fold':>22s} {'base CAGR':>10s} {'fix CAGR':>9s} {'base Calmar':>12s} {'fix Calmar':>11s} {'base MaxDD':>11s} {'fix MaxDD':>10s} {'base trades':>12s} {'fix trades':>11s}")
    base_scores, var_scores = [], []
    for fold_start, fold_end in DEV_FOLDS:
        rb = run_fold(variant_mod, baseline_params, fold_start, fold_end)
        rv = run_fold(variant_mod, variant_params, fold_start, fold_end)
        mb, mv = rb["metrics"], rv["metrics"]
        base_scores.append(mb["calmar"])
        var_scores.append(mv["calmar"])
        print(f"{fold_start}/{fold_end:>10s} {mb['cagr_pct']:10.2f} {mv['cagr_pct']:9.2f} {mb['calmar']:12.3f} {mv['calmar']:11.3f} {mb['max_drawdown_pct']:11.2f} {mv['max_drawdown_pct']:10.2f} {mb['total_trades']:12d} {mv['total_trades']:11d}")

    b_med, v_med = np.median(base_scores), np.median(var_scores)
    b_iqr = np.percentile(base_scores,75)-np.percentile(base_scores,25)
    v_iqr = np.percentile(var_scores,75)-np.percentile(var_scores,25)
    print(f"\nBaseline: median={b_med:.3f} iqr={b_iqr:.3f} objective={b_med-0.5*b_iqr:.4f}")
    print(f"Variant:  median={v_med:.3f} iqr={v_iqr:.3f} objective={v_med-0.5*v_iqr:.4f}")

    full_dev = {"startDate": "2011-01-01", "endDate": "2021-12-31", "fullHistory": True}
    rb_full = variant_mod.run({**baseline_params, **full_dev})
    rv_full = variant_mod.run({**variant_params, **full_dev})
    print(f"\nFull dev period (2011-2021), continuous:")
    print(f"  baseline: CAGR {rb_full['metrics']['cagr_pct']:.2f}%  Calmar {rb_full['metrics']['calmar']:.3f}  MaxDD {rb_full['metrics']['max_drawdown_pct']:.2f}%  Trades {rb_full['metrics']['total_trades']}")
    print(f"  variant:  CAGR {rv_full['metrics']['cagr_pct']:.2f}%  Calmar {rv_full['metrics']['calmar']:.3f}  MaxDD {rv_full['metrics']['max_drawdown_pct']:.2f}%  Trades {rv_full['metrics']['total_trades']}")

    out = {
        "region_params": region.region_params, "region_size": region.region_size, "region_out_of_top": region.region_out_of_top,
        "agrees_with_best_trial": region.agrees_with_best_trial,
        "baseline_full_dev": rb_full["metrics"], "variant_full_dev": rv_full["metrics"],
        "n_completed": len(completed), "n_rejected": len(rejected), "n_pruned": len(pruned),
    }
    out_path = PROJECT_ROOT / "scripts" / "_tmp_analysis" / "continuous_reentry_experiment_result.json"
    out_path.parent.mkdir(exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
