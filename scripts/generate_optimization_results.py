"""
Generates frontend/optimize/results.json -- the data behind the
client-facing optimization results page (frontend/optimize/index.html).

Every number here is computed fresh from the live engines, not copied by
hand from OPTIMIZATION_REPORT.md -- the winning parameter sets themselves
are fixed (they came from the actual 200-trial searches already run and
recorded in that report), but their metrics are always recomputed live so
this can never silently drift out of sync with the engines the way the
dashboard's own precomputed snapshots once did (see
generate_precomputed_snapshots.py's docstring for that history).

Run this after any change to app/engines/step1.py or step2.py, and after
any change to the winning parameter sets below (e.g. if the optimizer is
re-run and finds a different candidate).
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUT_PATH = PROJECT_ROOT / "frontend" / "optimize" / "results.json"

DEV_PERIOD = ("2011-01-01", "2021-12-31")
HOLDOUT_PERIOD = ("2022-02-01", "2025-12-30")

# The winning parameter sets found by the 200-trial searches recorded in
# OPTIMIZATION_REPORT.md. Fixed here deliberately -- re-deriving them
# would mean re-running a full search, which is a separate, deliberate
# action (scripts/run_optimization.py), not something this report
# generator should do silently.
STRATEGIES = [
    {
        "id": "step2_upro_tqqq_6state",
        "engine": "app.engines.step2",
        "display_name": "UPRO + TQQQ (flagship strategy)",
        "recommended": True,
        "candidates": [
            {
                "label": "Tuned, confirmed values locked",
                "key": "confirmed_locked",
                "params": {"cooldown": 2, "defSell": 75, "rsiPeriod": 24, "rsiTrim": 50, "uproSplit": 10},
                "verdict": "adopt",
                "verdict_note": "Real, sealed-data-tested improvement in return and Sharpe. Comes with a larger drawdown -- that trade-off is a preference call, not a technical one.",
            },
            {
                "label": "Tuned, confirmed values also opened up",
                "key": "free_everything",
                "params": {"cooldown": 4, "defSell": 55, "rsiOB": 65, "rsiPeriod": 12, "rsiRe": 57, "rsiTrim": 20, "smaDef": 50, "smaKill": 290, "uproSplit": 20, "vixThreshold": 45},
                "verdict": "reject",
                "verdict_note": "Scored far higher in the search, then did worse than doing nothing on sealed data -- a clean example of overfitting to 2011-2021. Not recommended.",
            },
            {
                "label": "Tuned for shorter/shallower drawdowns instead (Martin ratio)",
                "key": "martin",
                "params": {"cooldown": 3, "defSell": 75, "rsiPeriod": 22, "rsiTrim": 45, "uproSplit": 25},
                "verdict": "reject",
                "verdict_note": "Tried specifically to find a version with the return gain but less drawdown. Worse on every measure on sealed data -- the drawdown trade-off looks like a real, structural feature of this strategy's parameter space, not an artifact of how the search was scored.",
            },
            # --- Whipsaw-fix attempts (post-adoption investigation) ---
            # All three stack on top of the adopted "confirmed values locked"
            # config (cooldown/defSell/rsiPeriod/rsiTrim/uproSplit below) and
            # isolate one additional mechanism change. Each uses a different
            # engine module (app/engines/step2_*_variant.py), not the
            # confirmed step2 engine -- see "engine" override, handled in
            # main() below.
            {
                "label": "Fix attempt: kill-switch buffer + confirmation",
                "key": "killfix",
                "engine": "app.engines.step2_killfix_variant",
                "params": {"cooldown": 2, "defSell": 75, "rsiPeriod": 24, "rsiTrim": 50, "uproSplit": 10, "killBufferBps": 75, "killConfirmDays": 4},
                "verdict": "reject",
                "verdict_note": "Properly optimized (0 guardrail rejections, every dev fold improved). Whipsaws genuinely dropped on sealed data (false alarms 4→2), but the slower trigger also absorbed more of a real decline (SVB, March 2023) -- CAGR came out slightly worse (21.24% → 20.61%), not better. Mechanism verified working as designed; net effect negative.",
            },
            {
                "label": "Fix attempt: re-entry loosened (3-of-4 conditions)",
                "key": "reentry_bool",
                "engine": "app.engines.step2_reentry_variant",
                "params": {"cooldown": 2, "defSell": 75, "rsiPeriod": 24, "rsiTrim": 50, "uproSplit": 10, "reentryMinConditions": 3},
                "verdict": "reject",
                "verdict_note": "Dev-period signal was weak and mixed (one fold worse, trades up 40%, improvement driven by reduced cross-fold variance rather than higher typical return) -- did not clear the bar to justify a sealed-holdout check. Not adopted on dev evidence alone.",
                "skip_holdout": True,
            },
            {
                "label": "Fix attempt: re-entry loosened (continuous tolerance)",
                "key": "reentry_continuous",
                "engine": "app.engines.step2_continuous_reentry_variant",
                "params": {"cooldown": 2, "defSell": 75, "rsiPeriod": 24, "rsiTrim": 50, "uproSplit": 10, "reentryToleranceBps": 250},
                "verdict": "reject",
                "verdict_note": "A properly-built continuous generalization of the boolean version (verified identical to today's rule at zero tolerance), but the robust, density-picked region scored worse than baseline on dev (0.756 vs. 0.905), deeper drawdown, 35% more trades. Not adopted on dev evidence alone.",
                "skip_holdout": True,
            },
        ],
    },
    {
        "id": "step1_upro_4state",
        "engine": "app.engines.step1",
        "display_name": "UPRO-Only",
        "recommended": False,
        "candidates": [
            {
                "label": "Tuned, confirmed values locked",
                "key": "confirmed_locked",
                "params": {"rsiPeriod": 12, "uproSplit": 60},
                "verdict": "reject",
                "verdict_note": "Underperforms today's settings and buy-and-hold SPY on sealed data. Drawdown improved substantially, so this may be worth a conversation if minimizing pain matters more than maximizing return -- but it isn't a straightforward upgrade.",
            },
            {
                "label": "Tuned, confirmed values also opened up",
                "key": "free_everything",
                "params": {"rsiOB": 71, "rsiPeriod": 8, "rsiRe": 44, "smaDef": 70, "smaKill": 240, "uproSplit": 52, "vixThreshold": 26},
                "verdict": "reject",
                "verdict_note": "Same pattern as the Ravi's-numbers version, slightly more so.",
            },
        ],
    },
    {
        "id": "step3_spxu_predatory_short",
        "engine": "app.engines.step3",
        "display_name": "SPXU Hedge Overlay",
        "recommended": False,
        "candidates": [
            {
                "label": "Tuned, confirmed values locked",
                "key": "confirmed_locked",
                "params": {"exitVix": 37},
                "verdict": "reject",
                "verdict_note": "This overlay already loses money at today's settings (disclosed, on purpose). Tuning made it lose more on sealed data, not less. Recommendation: leave exactly as-is.",
            },
            {
                "label": "Tuned, confirmed values also opened up",
                "key": "free_everything",
                "params": {"exitVix": 39, "spxuAllocation": 82, "vixEntry": 30},
                "verdict": "reject",
                "verdict_note": "Worse still -- more than double the baseline's drawdown.",
            },
        ],
    },
]


def _run(engine_module, params: dict, start: str, end: str) -> dict:
    result = engine_module.run({**params, "fullHistory": True, "startDate": start, "endDate": end})
    return result["metrics"]


def _run_with_curve(engine_module, params: dict, start: str, end: str) -> tuple[dict, list[dict]]:
    """Same as _run, but also returns a lightweight (date, portfolio_value)
    series for charting -- not the full daily_log (which carries ~20
    extra diagnostic fields per day that the results page doesn't need)."""
    result = engine_module.run({**params, "fullHistory": True, "startDate": start, "endDate": end})
    curve = [{"date": row["date"], "value": row["portfolio_value"]} for row in result["daily_log"]]
    return result["metrics"], curve


def main() -> None:
    import importlib
    from app.config import ensure_data_available

    ensure_data_available()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    output = {"dev_period": DEV_PERIOD, "holdout_period": HOLDOUT_PERIOD, "strategies": []}

    for spec in STRATEGIES:
        print(f"Computing {spec['display_name']}...")
        strategy_json = json.loads((PROJECT_ROOT / "strategies" / f"{spec['id']}.json").read_text())
        default_params = {
            k: p["default"] for k, p in strategy_json["params"].items()
            if p.get("type") == "range" and p.get("tier") != "input"
        }
        engine_module = importlib.import_module(spec["engine"])

        baseline_dev = _run(engine_module, default_params, *DEV_PERIOD)
        baseline_holdout, baseline_curve = _run_with_curve(engine_module, default_params, *HOLDOUT_PERIOD)

        candidates_out = []
        for cand in spec["candidates"]:
            # A candidate may run through a different engine module than its
            # strategy's confirmed one (the whipsaw-fix attempts each isolate
            # one mechanism change in their own module) -- default_params still
            # comes from the strategy's own JSON, since these fixes stack on
            # top of the same confirmed/tuned param set either way.
            cand_engine = importlib.import_module(cand["engine"]) if cand.get("engine") else engine_module
            full_params = {**default_params, **cand["params"]}
            dev = _run(cand_engine, full_params, *DEV_PERIOD)
            entry = {
                "label": cand["label"],
                "key": cand["key"],
                "params": cand["params"],
                "verdict": cand["verdict"],
                "verdict_note": cand["verdict_note"],
                "dev": dev,
            }
            if cand.get("skip_holdout"):
                entry["holdout"] = None
                entry["holdout_curve"] = None
                entry["holdout_skipped"] = True
            else:
                holdout, holdout_curve = _run_with_curve(cand_engine, full_params, *HOLDOUT_PERIOD)
                entry["holdout"] = holdout
                entry["holdout_curve"] = holdout_curve
            candidates_out.append(entry)

        output["strategies"].append({
            "id": spec["id"],
            "display_name": spec["display_name"],
            "recommended": spec["recommended"],
            "default_params": default_params,
            "baseline": {"dev": baseline_dev, "holdout": baseline_holdout, "holdout_curve": baseline_curve},
            "candidates": candidates_out,
        })

    with open(OUT_PATH, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"\nWrote {OUT_PATH} ({OUT_PATH.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
