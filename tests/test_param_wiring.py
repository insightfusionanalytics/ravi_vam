"""
Guards against the exact bug found 2026-09-24 while planning the parameter
optimizer: several sliders on the dashboard (smaKill, smaDef, rsiPeriod,
uproSplit, defSell, rsiTrim, cooldown) were declared in the strategy JSON
and honored by the browser's JS engine, but silently ignored by the Python
engines -- moving the slider changed the label, not the backtest. An
optimizer pointed at a dead param would run hundreds of trials that are all
secretly identical and confidently report a meaningless "winner."

For every tunable ("confirmed" or "free" tier) param declared in step1 and
step2's strategy JSON, this nudges it one step off its default and asserts
the result actually changes. It does not assert the change is *correct* --
only that the param is wired to something. Correctness is covered by
test_regression_benchmarks.py (defaults) and the client-confirmed cases
elsewhere (e.g. test_60day_rule.py).
"""

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _tunable_params(strategy_file: str) -> list[tuple[str, dict]]:
    config = json.loads((REPO_ROOT / "strategies" / strategy_file).read_text())
    return [
        (key, p)
        for key, p in config["params"].items()
        if p.get("type") == "range" and p.get("tier") in ("confirmed", "free")
    ]


def _nudged_value(param: dict):
    """One step off default, clamped into [min, max] -- never equal to default."""
    default, step, lo, hi = param["default"], param["step"], param["min"], param["max"]
    up = min(default + step, hi)
    down = max(default - step, lo)
    if up != default:
        return up
    if down != default:
        return down
    pytest.fail(f"param has no room to move: {param}")


STEP1_PARAMS = _tunable_params("step1_upro_4state.json")
STEP2_PARAMS = _tunable_params("step2_upro_tqqq_6state.json")


def _result_fingerprint(result: dict) -> tuple:
    m = result["metrics"]
    return (m["total_trades"], m["final_value"], m["cagr_pct"], m["max_drawdown_pct"])


@pytest.mark.parametrize("param_key,param", STEP1_PARAMS, ids=[k for k, _ in STEP1_PARAMS])
def test_step1_param_moves_the_result(param_key, param):
    from app.engines import step1

    baseline = _result_fingerprint(step1.run({}))
    nudged = _result_fingerprint(step1.run({param_key: _nudged_value(param)}))
    assert baseline != nudged, (
        f"step1 param '{param_key}' moved one step off its default "
        f"({param['default']} -> {_nudged_value(param)}) but the backtest result "
        f"didn't change at all -- this param is declared as tunable but is not "
        f"actually wired into the engine."
    )


@pytest.mark.parametrize("param_key,param", STEP2_PARAMS, ids=[k for k, _ in STEP2_PARAMS])
def test_step2_param_moves_the_result(param_key, param):
    from app.engines import step2

    baseline = _result_fingerprint(step2.run({}))
    nudged = _result_fingerprint(step2.run({param_key: _nudged_value(param)}))
    assert baseline != nudged, (
        f"step2 param '{param_key}' moved one step off its default "
        f"({param['default']} -> {_nudged_value(param)}) but the backtest result "
        f"didn't change at all -- this param is declared as tunable but is not "
        f"actually wired into the engine."
    )
