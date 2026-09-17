"""
Look-ahead bias checks: every trade must execute strictly after the day its
signal fired (T+1 execution, per the client's confirmed spec: "signal at
4 PM close, order placed next day"). If a trade's signal_date and
execution_date were ever equal or reversed, the backtest would be trading
on information it couldn't have had yet -- the single most fundamental way
a backtest can lie about how well a strategy would really have done.
"""

import pandas as pd
import pytest


@pytest.mark.parametrize("result_fixture", ["step1_result", "step2_result", "step3_result", "step4_result"])
def test_signal_date_strictly_before_execution_date(result_fixture, request):
    result = request.getfixturevalue(result_fixture)
    for t in result["trades"]:
        sig = pd.Timestamp(t["signal_date"])
        exe = pd.Timestamp(t["execution_date"])
        assert sig < exe, (
            f"Look-ahead bias: signal_date {t['signal_date']} is not before "
            f"execution_date {t['execution_date']} for trade {t}"
        )
