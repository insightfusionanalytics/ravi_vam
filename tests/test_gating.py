"""
Step 3 and Step 4 are overlays that must only ever act during Step 2's CASH
periods (per the client's confirmed spec: "activates ONLY when Step 2 is in
CASH"). These tests assert that gate can never be silently broken -- e.g. by
a future refactor that decouples the two engines' date ranges.
"""

import pandas as pd

from app.engines.step4 import SVIX_LAUNCH_DATE


def test_step3_only_holds_spxu_during_step2_cash(step3_result):
    for row in step3_result["daily_log"]:
        if row["state"] == "CASH_SPXU":
            assert row["step2_state"] == "CASH", (
                f"Step 3 holding SPXU on {row['date']} while Step 2 state was "
                f"'{row['step2_state']}', not CASH"
            )


def test_step3_trades_are_spxu_only(step3_result):
    for t in step3_result["trades"]:
        assert t["instrument"] == "SPXU"


def test_step4_never_holds_svix_before_launch_date(step4_result):
    launch = pd.Timestamp(SVIX_LAUNCH_DATE)
    for row in step4_result["daily_log"]:
        if row["state"] in ("CASH_SVIX_INIT", "CASH_SVIX_PANIC"):
            assert pd.Timestamp(row["date"]) >= launch, (
                f"Step 4 holding SVIX on {row['date']}, before it existed ({SVIX_LAUNCH_DATE})"
            )


def test_step4_only_holds_svix_during_step2_cash(step4_result):
    for row in step4_result["daily_log"]:
        if row["state"] in ("CASH_SVIX_INIT", "CASH_SVIX_PANIC"):
            assert row["step2_state"] == "CASH", (
                f"Step 4 holding SVIX on {row['date']} while Step 2 state was "
                f"'{row['step2_state']}', not CASH"
            )


def test_step4_trades_are_svix_only(step4_result):
    for t in step4_result["trades"]:
        assert t["instrument"] == "SVIX"


def test_step4_allocation_never_exceeds_panic_cap(step4_result):
    for row in step4_result["daily_log"]:
        assert row["svix_allocation_pct"] <= 31, (
            f"SVIX allocation {row['svix_allocation_pct']}% on {row['date']} "
            f"exceeds the documented 30% panic cap"
        )
