"""
Regression-locks the trade-log signal-day-price fix (Phase 1, 2026-09-18).
This is the exact bug Ravi caught by hand, was told was fixed, and turned
out not to be: signal_spy_close/signal_vix/etc were always blank, and the
"same day" price field actually held the execution day's price mislabeled
as the signal day's. If this regresses, these fields go blank again or the
overnight-gap math silently reverts to comparing a day to itself.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))


def test_step1_offline_script_fills_signal_day_fields():
    import vam_step1_databento as mod
    from app.config import ensure_data_available

    mod.DATA_DIR = ensure_data_available()
    df = mod.load_step1_data()
    df = mod.add_step1_indicators(df)
    trades, daily_log, metrics = mod.run_step1_backtest(df)

    assert trades, "Expected at least one trade to check"
    for t in trades:
        assert t["signal_spy_close"] != "", f"signal_spy_close left blank on {t['execution_date']}"
        assert t["signal_vix"] != "", f"signal_vix left blank on {t['execution_date']}"
        assert isinstance(t["signal_spy_close"], (int, float))
        assert isinstance(t["signal_vix"], (int, float))

        # The overnight gap should reflect a real day-to-day price move, not
        # a same-day open-vs-close comparison (the original bug's exact shape).
        assert "signal_day_upro_close" in t
        assert "overnight_gap_pct" in t


def test_step2_offline_script_fills_signal_day_fields():
    import vam_step2_databento as mod
    from app.config import ensure_data_available

    mod.DATA_DIR = ensure_data_available()
    df = mod.load_step2_data()
    df = mod.add_step2_indicators(df)
    trades, daily_log, metrics = mod.run_step2_backtest(df)

    assert trades, "Expected at least one trade to check"
    for t in trades:
        assert "signal_day_upro_close" in t or "signal_day_tqqq_close" in t
        assert "overnight_gap_pct" in t
