"""
Locks in today's (2026-09-18, post split-adjustment-bug-fix) default-param
metrics for every strategy, within a tolerance. This is the direct fix for
the exact failure mode found in audit_import itself during the earlier
reconciliation phase: its own math audit reported Step 1 max drawdown as
"~38%" in April, then a May rerun of the same window silently produced
-48.43% with no re-audit against the new number -- nobody could tell
whether that was an intentional fix or a regression, because nothing was
locked in. If a future change moves any of these numbers outside its
tolerance, that's a signal to consciously decide whether the change was
intentional, not a hint to update the test blindly.
"""

import pytest

TOLERANCE_PCT_POINTS = 1.0


EXPECTED = {
    "step1_result": {"cagr_pct": 17.87, "max_drawdown_pct": -51.58, "total_trades": 78},
    "step2_result": {"cagr_pct": 19.74, "max_drawdown_pct": -53.88, "total_trades": 176},
    "step3_result": {"cagr_pct": -5.19, "max_drawdown_pct": -48.36, "total_trades": 48},
    "step4_result": {"cagr_pct": 1.23, "max_drawdown_pct": -2.7, "total_trades": 6},
}


@pytest.mark.parametrize("result_fixture,expected", EXPECTED.items())
def test_default_metrics_match_locked_baseline(result_fixture, expected, request):
    result = request.getfixturevalue(result_fixture)
    m = result["metrics"]
    for key, expected_val in expected.items():
        actual_val = m[key]
        if key == "total_trades":
            assert actual_val == expected_val, (
                f"{result_fixture}.{key}: expected exactly {expected_val}, got {actual_val}. "
                f"Trade count changing means the state machine's behavior changed -- "
                f"confirm this was intentional before updating the baseline."
            )
        else:
            assert abs(actual_val - expected_val) <= TOLERANCE_PCT_POINTS, (
                f"{result_fixture}.{key}: expected {expected_val} +/- {TOLERANCE_PCT_POINTS}, "
                f"got {actual_val}. If this change was intentional (e.g. a confirmed bug fix "
                f"or new data), update EXPECTED here deliberately -- don't let it drift silently."
            )
