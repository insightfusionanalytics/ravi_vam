"""Exports every trial from every optimization study (original passes +
whipsaw-fix experiments) into one unified CSV: params, the search's own
reward-function value (fold-level Calmar/Martin, whatever that study used),
and a freshly recomputed full-dev-period CAGR/Calmar/MaxDD/trades for every
single trial, not just the winners."""
import csv
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, "/Users/chirag/ravi_vam")

import optuna

from app.engines import step1, step2, step3
from app.engines import step2_killfix_variant, step2_reentry_variant, step2_continuous_reentry_variant

PROJECT_ROOT = Path("/Users/chirag/ravi_vam")
STORAGE_URL = "sqlite:////Users/chirag/ravi_vam/data/optuna/studies.db"
DEV_PERIOD = ("2011-01-01", "2021-12-31")

STUDIES = [
    ("step2_upro_tqqq_6state__confirmed_locked", step2, "strategies/step2_upro_tqqq_6state.json", "calmar"),
    ("step2_upro_tqqq_6state__free_everything", step2, "strategies/step2_upro_tqqq_6state.json", "calmar"),
    ("step2_upro_tqqq_6state__martin", step2, "strategies/step2_upro_tqqq_6state.json", "martin"),
    ("step1_upro_4state__confirmed_locked", step1, "strategies/step1_upro_4state.json", "calmar"),
    ("step1_upro_4state__free_everything", step1, "strategies/step1_upro_4state.json", "calmar"),
    ("step3_spxu_predatory_short__confirmed_locked", step3, "strategies/step3_spxu_predatory_short.json", "calmar"),
    ("step3_spxu_predatory_short__free_everything", step3, "strategies/step3_spxu_predatory_short.json", "calmar"),
    ("step2_killfix_variant__confirmed_locked", step2_killfix_variant, "strategies/experiments/step2_killfix_variant.json", "calmar"),
    ("step2_reentry_variant__confirmed_locked", step2_reentry_variant, "strategies/experiments/step2_reentry_variant.json", "calmar"),
    ("step2_continuous_reentry_variant__confirmed_locked", step2_continuous_reentry_variant, "strategies/experiments/step2_continuous_reentry_variant.json", "calmar"),
]

ALL_PARAM_KEYS = [
    "vixThreshold", "smaKill", "smaDef", "confirmDays", "rsiOB", "rsiRe",
    "cooldown", "defSell", "rsiPeriod", "rsiTrim", "uproSplit",
    "exitVix", "vixEntry", "spxuAllocation",
    "killBufferBps", "killConfirmDays", "reentryMinConditions", "reentryToleranceBps",
]

storage = optuna.storages.RDBStorage(url=STORAGE_URL)

rows = []
t_start = time.time()
for study_name, engine_module, config_rel, reward_name in STUDIES:
    strategy_config = json.loads((PROJECT_ROOT / config_rel).read_text())
    default_params = {
        k: p["default"] for k, p in strategy_config["params"].items()
        if p.get("type") == "range" and p.get("tier") != "input"
    }
    study = optuna.load_study(study_name=study_name, storage=storage)
    print(f"{study_name}: {len(study.trials)} trials, reward={reward_name}")

    for t in study.trials:
        state = str(t.state).replace("TrialState.", "")
        attrs = t.user_attrs
        fold_median = attrs.get("fold_median_calmar", attrs.get("fold_median_score"))
        fold_iqr = attrs.get("fold_iqr_calmar", attrs.get("fold_iqr_score"))
        fold_list = attrs.get("fold_calmars", attrs.get("fold_scores"))
        rejected_reason = attrs.get("rejected_reason")
        is_rejected = (t.value == -1000.0)

        full_params = {**default_params, **t.params}
        run_params = {**full_params, "fullHistory": True, "startDate": DEV_PERIOD[0], "endDate": DEV_PERIOD[1]}
        try:
            result = engine_module.run(run_params)
            m = result["metrics"]
            cagr, calmar, maxdd, trades = m["cagr_pct"], m["calmar"], m["max_drawdown_pct"], m["total_trades"]
        except Exception as e:
            cagr = calmar = maxdd = trades = None

        row = {
            "study": study_name,
            "reward_function": reward_name,
            "trial_number": t.number,
            "state": "REJECTED_BY_GUARDRAIL" if is_rejected else state,
            "rejected_reason": rejected_reason or "",
            "search_objective_value": t.value if t.value is not None else "",
            "fold_median_calmar": fold_median if fold_median is not None else "",
            "fold_iqr_calmar": fold_iqr if fold_iqr is not None else "",
            "fold_calmars": json.dumps(fold_list) if fold_list is not None else "",
            "full_dev_cagr_pct": cagr,
            "full_dev_calmar": calmar,
            "full_dev_max_drawdown_pct": maxdd,
            "full_dev_trades": trades,
        }
        for k in ALL_PARAM_KEYS:
            row[k] = t.params.get(k, "")
        rows.append(row)

    elapsed = time.time() - t_start
    print(f"  running total: {len(rows)} rows, {elapsed:.0f}s elapsed")

out_path = PROJECT_ROOT / "scripts" / "_tmp_analysis" / "all_trials_export.csv"
fieldnames = ["study", "reward_function", "trial_number", "state", "rejected_reason",
              "search_objective_value", "fold_median_calmar", "fold_iqr_calmar", "fold_calmars",
              "full_dev_cagr_pct", "full_dev_calmar", "full_dev_max_drawdown_pct", "full_dev_trades"] + ALL_PARAM_KEYS
with open(out_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"\nWrote {out_path} ({len(rows)} rows, {time.time()-t_start:.0f}s total)")
