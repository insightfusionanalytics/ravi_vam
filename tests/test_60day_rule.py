"""
Regression-locks the client-confirmed 60-day re-entry immunity rule.

The client validated this fix on 2018/2020 crash-and-recovery data: giving a
fresh CASH -> BULL_100 re-entry 60 days of immunity from the SMA-50
defensive trim measurably improved CAGR (his numbers: 20.3% -> 23%,
20.4% -> 22.6%). This test reproduces that exact scenario -- the 2020 COVID
crash and recovery -- so if a future change to the state machine silently
breaks or weakens the rule, this test fails instead of the regression
sitting undetected until someone re-derives it by hand again.
"""

import pandas as pd
import pytest


@pytest.fixture
def step1_2020_window():
    """Truncate Step 1's data to end 2020-09-30, matching the exact window
    the client's own validation used. Ends well before any of the app's
    later data-quality issues (e.g. the 2022 split-adjustment bug), so this
    test is unaffected by unrelated bugs."""
    from app.engines import step1

    original_loader = step1._mod.load_step1_data

    def truncated_loader():
        df = original_loader()
        return df[df.index <= pd.Timestamp("2020-09-30")]

    step1._mod.load_step1_data = truncated_loader
    yield step1
    step1._mod.load_step1_data = original_loader


def test_immunity_improves_cagr_in_2020_window(step1_2020_window):
    result_off = step1_2020_window.run({"reentryImmunityDays": 0})
    result_on = step1_2020_window.run({"reentryImmunityDays": 60})

    cagr_off = result_off["metrics"]["cagr_pct"]
    cagr_on = result_on["metrics"]["cagr_pct"]

    assert cagr_on > cagr_off, (
        f"60-day immunity should improve CAGR over this exact window (client-validated "
        f"finding), got {cagr_off}% without vs {cagr_on}% with"
    )
    # Client's own validated range was ~20.3% -> 23%. Allow slack for any
    # unrelated future data/param changes, but the shape of the finding
    # (both numbers land in a similar ballpark) should hold.
    assert 15 < cagr_off < 25
    assert 20 < cagr_on < 28


def test_immunity_suppresses_the_documented_whipsaw_trim(step1_2020_window):
    """The specific whipsaw this rule fixes: a defensive trim firing within
    60 days of a fresh re-entry. Assert no such trim appears when immunity
    is on, and that it *would* have appeared with immunity off (otherwise
    this test would trivially pass for the wrong reason)."""
    result_off = step1_2020_window.run({"reentryImmunityDays": 0})
    result_on = step1_2020_window.run({"reentryImmunityDays": 60})

    def defensive_trims_within_60d_of_reentry(trades):
        reentry_dates = [
            pd.Timestamp(t["execution_date"]) for t in trades
            if t["state_from"] == "CASH" and t["state_to"] == "BULL_100"
        ]
        violations = []
        for t in trades:
            if t["state_to"] != "DEFENSIVE":
                continue
            trim_date = pd.Timestamp(t["execution_date"])
            for rd in reentry_dates:
                if rd < trim_date <= rd + pd.Timedelta(days=60):
                    violations.append(t["execution_date"])
        return violations

    assert defensive_trims_within_60d_of_reentry(result_off["trades"]), (
        "Expected this scenario to reproduce the whipsaw with immunity off -- "
        "if it doesn't, this test isn't actually testing the documented bug anymore"
    )
    assert not defensive_trims_within_60d_of_reentry(result_on["trades"]), (
        "60-day immunity failed to suppress a defensive trim within 60 days of re-entry"
    )
