"""
Trade-level bookkeeping checks: does each trade's own numbers add up
internally? These are independent of whether the *strategy* made a good
decision -- they only check that the arithmetic recording that decision
is self-consistent (shares_after = shares_before + shares_delta, cash
moves by exactly the trade value plus costs, etc).
"""

import pytest


def test_step1_trade_bookkeeping(step1_result):
    for t in step1_result["trades"]:
        # Fields are independently rounded to 4dp in the engine's output, so
        # two of them combined can be off by up to ~2e-4 purely from rounding.
        assert abs(t["shares_after"] - (t["shares_before"] + t["shares_delta"])) < 2e-3, t

        pv_before = t["cash_before"] + t["shares_before"] * t["exec_price"]
        assert abs(pv_before - t["portfolio_value_before_trade"]) < 0.5, (
            f"pv_before mismatch on {t['execution_date']}: "
            f"computed {pv_before:.2f} vs recorded {t['portfolio_value_before_trade']:.2f}"
        )

        expected_cash_after = (
            t["portfolio_value_before_trade"]
            - t["shares_after"] * t["exec_price"]
            - t["commission_dollars"]
            - t["slippage_dollars"]
        )
        assert abs(expected_cash_after - t["cash_after"]) < 0.5, (
            f"cash_after mismatch on {t['execution_date']}: "
            f"expected {expected_cash_after:.2f} vs recorded {t['cash_after']:.2f}"
        )


def test_step2_trade_value_matches_shares_and_price(step2_result):
    for t in step2_result["trades"]:
        expected = abs(t["shares_delta"] * t["exec_price"])
        assert abs(expected - t["trade_value_dollars"]) < 0.5, (
            f"{t['instrument']} trade on {t['execution_date']}: "
            f"shares_delta*price={expected:.2f} vs recorded trade_value={t['trade_value_dollars']:.2f}"
        )


def test_step3_trade_bookkeeping(step3_result):
    for t in step3_result["trades"]:
        assert abs(t["shares_after"] - (t["shares_before"] + t["shares_delta"])) < 1e-4, t
        pv_before = t["cash_before"] + t["shares_before"] * t["exec_price"]
        assert abs(pv_before - t["portfolio_value_before_trade"]) < 0.5, (
            f"pv_before mismatch on {t['execution_date']}: "
            f"computed {pv_before:.2f} vs recorded {t['portfolio_value_before_trade']:.2f}"
        )


def test_step4_trade_value_matches_shares_and_price(step4_result):
    for t in step4_result["trades"]:
        expected = abs(t["shares_delta"] * t["exec_price"])
        assert abs(expected - t["trade_value_dollars"]) < 0.5, (
            f"{t['action']} on {t['execution_date']}: "
            f"shares_delta*price={expected:.2f} vs recorded trade_value={t['trade_value_dollars']:.2f}"
        )


@pytest.mark.parametrize("result_fixture", ["step1_result", "step2_result", "step3_result", "step4_result"])
def test_no_trades_with_zero_or_negative_price(result_fixture, request):
    result = request.getfixturevalue(result_fixture)
    for t in result["trades"]:
        assert t["exec_price"] > 0, f"Non-positive exec_price on {t['execution_date']}: {t}"
