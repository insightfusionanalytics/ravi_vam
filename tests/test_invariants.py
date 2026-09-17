"""
Generic sanity invariants that must hold for every engine's output,
regardless of strategy-specific logic. These are cheap, structural checks
that catch entire classes of bugs (NaN leaking through, dates out of order,
negative portfolio values, allocations that don't add up) without needing
to know anything about a specific strategy's rules.
"""

import math

import pytest

from conftest import ALL_ENGINE_RESULT_FIXTURES


@pytest.mark.parametrize("result_fixture", ALL_ENGINE_RESULT_FIXTURES)
def test_daily_log_is_non_empty(result_fixture, request):
    result = request.getfixturevalue(result_fixture)
    assert len(result["daily_log"]) > 0


@pytest.mark.parametrize("result_fixture", ALL_ENGINE_RESULT_FIXTURES)
def test_dates_strictly_increasing_no_duplicates(result_fixture, request):
    result = request.getfixturevalue(result_fixture)
    dates = [row["date"] for row in result["daily_log"]]
    assert dates == sorted(dates), "daily_log dates are not in order"
    assert len(dates) == len(set(dates)), "daily_log has duplicate dates"


@pytest.mark.parametrize("result_fixture", ALL_ENGINE_RESULT_FIXTURES)
def test_portfolio_value_never_negative_or_nan(result_fixture, request):
    result = request.getfixturevalue(result_fixture)
    for row in result["daily_log"]:
        pv = row["portfolio_value"]
        assert pv is not None and not (isinstance(pv, float) and math.isnan(pv)), (
            f"NaN/None portfolio_value on {row['date']}"
        )
        assert pv >= 0, f"Negative portfolio_value on {row['date']}: {pv}"


@pytest.mark.parametrize("result_fixture", ["step1_result", "step2_result", "step3_result"])
def test_allocation_percentages_sum_to_100(result_fixture, request):
    """Whatever the strategy's own allocation fields are, they should always
    sum to ~100% of the portfolio (cash + invested = everything)."""
    result = request.getfixturevalue(result_fixture)
    alloc_keys = [
        k for k in result["daily_log"][0].keys()
        if k.endswith("_allocation_pct")
    ]
    assert alloc_keys, f"No *_allocation_pct fields found for {result_fixture}"
    for row in result["daily_log"]:
        total = sum(row[k] for k in alloc_keys)
        assert abs(total - 100) < 0.5, (
            f"Allocations sum to {total:.2f}% (expected ~100%) on {row['date']} "
            f"for {result_fixture}: {[row[k] for k in alloc_keys]}"
        )


@pytest.mark.parametrize("result_fixture", ALL_ENGINE_RESULT_FIXTURES)
def test_metrics_has_no_nan(result_fixture, request):
    result = request.getfixturevalue(result_fixture)
    for key, val in result["metrics"].items():
        if isinstance(val, float):
            assert not math.isnan(val), f"NaN in metrics['{key}'] for {result_fixture}"


@pytest.mark.parametrize("result_fixture", ALL_ENGINE_RESULT_FIXTURES)
def test_final_value_matches_last_daily_log_row(result_fixture, request):
    """The headline 'final_value' metric should exactly match the last
    portfolio_value in the daily log -- catches metrics being computed from
    a different (e.g. stale, truncated) dataset than what's actually shown."""
    result = request.getfixturevalue(result_fixture)
    last_pv = result["daily_log"][-1]["portfolio_value"]
    assert abs(result["metrics"]["final_value"] - last_pv) < 0.01
