"""Clean, self-contained CSV: Original vs Tuned, every strategy that has an
adopted-or-considered tuned candidate, across all three precise periods
(Full History / Search / Sealed) -- full parameter set on every row, so
anyone can independently evaluate or reproduce a number without needing to
run anything themselves."""
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, "/Users/chirag/ravi_vam")

from app.engines import step1, step2, step3

PROJECT_ROOT = Path("/Users/chirag/ravi_vam")

PERIODS = [
    ("Full History", "2011-01-01", "2025-12-30"),
    ("Search (dev period)", "2011-01-01", "2021-12-31"),
    ("Sealed (holdout)", "2022-02-01", "2025-12-30"),
]

STRATEGIES = [
    {
        "name": "UPRO + TQQQ (flagship)", "id": "step2_upro_tqqq_6state", "engine": step2,
        "config_path": "strategies/step2_upro_tqqq_6state.json",
        "tuned_overrides": {"cooldown": 2, "defSell": 75, "rsiPeriod": 24, "rsiTrim": 50, "uproSplit": 10},
        "verdict": "ADOPTED",
    },
    {
        "name": "UPRO-Only", "id": "step1_upro_4state", "engine": step1,
        "config_path": "strategies/step1_upro_4state.json",
        "tuned_overrides": {"rsiPeriod": 12, "uproSplit": 60},
        "verdict": "REJECTED",
    },
    {
        "name": "SPXU Hedge Overlay", "id": "step3_spxu_predatory_short", "engine": step3,
        "config_path": "strategies/step3_spxu_predatory_short.json",
        "tuned_overrides": {"exitVix": 37},
        "verdict": "REJECTED",
    },
]

ALL_PARAM_KEYS = ["vixThreshold", "smaKill", "smaDef", "confirmDays", "rsiOB", "rsiRe",
                  "cooldown", "defSell", "rsiPeriod", "rsiTrim", "uproSplit",
                  "exitVix", "vixEntry", "spxuAllocation"]

rows = []
for spec in STRATEGIES:
    config = json.loads((PROJECT_ROOT / spec["config_path"]).read_text())
    default_params = {
        k: p["default"] for k, p in config["params"].items()
        if p.get("type") == "range" and p.get("tier") != "input"
    }
    tuned_params = {**default_params, **spec["tuned_overrides"]}

    for config_label, params in [("Original", default_params), ("Tuned", tuned_params)]:
        for period_label, start, end in PERIODS:
            run_params = {**params, "fullHistory": True, "startDate": start, "endDate": end}
            result = spec["engine"].run(run_params)
            m = result["metrics"]
            row = {
                "strategy": spec["name"],
                "config": config_label,
                "verdict": spec["verdict"] if config_label == "Tuned" else "",
                "period": period_label,
                "period_start": start,
                "period_end": end,
                "cagr_pct": m["cagr_pct"],
                "sharpe": m["sharpe"],
                "sortino": m.get("sortino", ""),
                "calmar": m["calmar"],
                "max_drawdown_pct": m["max_drawdown_pct"],
                "total_trades": m["total_trades"],
                "final_value": m["final_value"],
                "total_return_pct": m["total_return_pct"],
            }
            for k in ALL_PARAM_KEYS:
                row[k] = params.get(k, "")
            rows.append(row)
        print(f"{spec['name']} / {config_label}: done")

out_path = PROJECT_ROOT / "scripts" / "_tmp_analysis" / "tuned_vs_original_export.csv"
fieldnames = ["strategy", "config", "verdict", "period", "period_start", "period_end",
              "cagr_pct", "sharpe", "sortino", "calmar", "max_drawdown_pct",
              "total_trades", "final_value", "total_return_pct"] + ALL_PARAM_KEYS
with open(out_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
print(f"\nWrote {out_path} ({len(rows)} rows)")
