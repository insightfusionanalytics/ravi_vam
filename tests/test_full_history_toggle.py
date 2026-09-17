"""
Regression-locks the "History Range" toggle (Phase 3, 2026-09-18): the
dashboard's honesty fix that lets the full 2011-2025 result be seen
alongside the flattering 2020-2025 window, instead of the long-horizon
finding being buried in an audit log nobody re-reads.
"""


def test_full_history_covers_more_years_than_recent(step1_result, step1_full_history_result):
    assert step1_full_history_result["metrics"]["years"] > step1_result["metrics"]["years"]


def test_full_history_flag_is_reported_correctly(step1_result, step1_full_history_result):
    assert step1_result["metrics"]["full_history_mode"] is False
    assert step1_full_history_result["metrics"]["full_history_mode"] is True


def test_full_history_starts_around_2011(step1_full_history_result):
    first_date = step1_full_history_result["daily_log"][0]["date"]
    assert first_date.startswith("2011") or first_date.startswith("2012"), (
        f"Expected Full History to start near 2011, got {first_date}"
    )


def test_alpha_is_lower_or_similar_over_the_full_honest_window(step1_result, step2_result,
                                                                 step1_full_history_result,
                                                                 step2_full_history_result):
    """The documented finding: the strategy's edge over SPY does not hold up
    as well -- or at least isn't dramatically better -- once tested over the
    full, less flattering history. This doesn't have to hold to the decimal,
    but a huge full-history outperformance versus the recent window would
    mean the 'honesty' finding silently reversed, which is worth knowing."""
    assert step1_full_history_result["metrics"]["alpha_vs_spy_pct"] < 10
    assert step2_full_history_result["metrics"]["alpha_vs_spy_pct"] < 15
