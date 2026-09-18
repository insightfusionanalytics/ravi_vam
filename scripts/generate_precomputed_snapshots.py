"""
Regenerates the static "instant paint" snapshots the dashboard loads before
any live computation happens (frontend/dashboard/precomputed/*.json).

These files exist so the dashboard renders instantly even during a Render
cold start, instead of showing a spinner. The tradeoff: if they're never
regenerated, they silently go stale -- which is exactly what happened here.
The Sep 9 snapshots were still being served as "the" default result on
2026-09-18, showing the double-split-adjustment bug and pre-60-day-rule
numbers to anyone who opened the dashboard without manually clicking
Run Backtest.

Run this after any change to app/engines/*.py, and it now also runs as
part of deploy.sh so this can't drift silently again.
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUT_DIR = PROJECT_ROOT / "frontend" / "dashboard" / "precomputed"

# (strategy_id, module_path, function_name)
SNAPSHOT_TARGETS = [
    ("step1_upro_4state", "app.engines.step1", "run"),
    ("step2_upro_tqqq_6state", "app.engines.step2", "run"),
    ("v3_7state_optimized", "app.engines.v5", "run_v3"),
    ("v5_leveraged", "app.engines.v5", "run_v5"),
    ("v5b_nonleveraged", "app.engines.v5", "run_v5b"),
]


def main() -> None:
    from app.config import ACTIVE_DATA_SOURCE, ensure_data_available
    import importlib

    ensure_data_available()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for strategy_id, module_path, func_name in SNAPSHOT_TARGETS:
        print(f"Generating {strategy_id}...")
        mod = importlib.import_module(module_path)
        run_fn = getattr(mod, func_name)
        result = run_fn({}, 100_000.0)

        from app.config import ACTIVE_DATA_SOURCE as current_source
        result["data_source"] = current_source

        out_path = OUT_DIR / f"{strategy_id}.json"
        with open(out_path, "w") as f:
            json.dump(result, f, default=str)
        print(f"  wrote {out_path} ({out_path.stat().st_size / 1024:.0f} KB)")

    print("Done. Commit these files so the dashboard's default view matches the live engines.")


if __name__ == "__main__":
    main()
