"""
Tests the fold-scoring objective (app/optimize/objective.py) against a fake
engine with controllable per-fold metrics -- fast, and isolates the
scoring/constraint/pruning logic from real backtests (those are covered by
the regression suite and a slower end-to-end smoke test elsewhere).
"""

import optuna
import pytest

from app.optimize.folds import DEV_FOLDS
from app.optimize.objective import (
    MAX_CASH_FRACTION_OF_BASELINE,
    MAX_DRAWDOWN_FLOOR_PCT,
    MIN_DEFENSIVE_FRACTION_OF_BASELINE,
    MIN_TRADES_PER_FOLD,
    REJECTED_VALUE,
    _fold_score,
    _ulcer_index,
    make_objective,
)

STRATEGY_CONFIG = {
    "params": {
        "vixThreshold": {"type": "range", "default": 30, "min": 20, "max": 45, "step": 1, "tier": "confirmed"},
        "confirmDays": {"type": "range", "default": 2, "min": 0, "max": 5, "step": 1, "tier": "free"},
        "capital": {"type": "range", "default": 100000, "min": 10000, "max": 1000000, "step": 10000, "tier": "input"},
    }
}
# No "id" key on STRATEGY_CONFIG above -- deliberately: DEFENSIVE_STATES is
# keyed by strategy id, so a config with no id makes the defensive-floor
# constraint a no-op, which is what the tests in this file that aren't
# specifically about that constraint want (they're testing other things).
# make_objective() always calls compute_baseline_defensive_fractions() once
# up front, which costs exactly 4 engine calls (one per dev fold) before
# any trial runs -- every call-count assertion below adds 4 to account for
# it.
BASELINE_CALLS = 4


class FakeEngine:
    """Returns metrics from a caller-supplied function of (params, fold_index)
    instead of running a real backtest."""

    def __init__(self, metrics_fn):
        self.metrics_fn = metrics_fn
        self.calls = []

    def run(self, params):
        fold_index = next(
            i for i, (s, e) in enumerate(DEV_FOLDS)
            if params.get("startDate") == s and params.get("endDate") == e
        )
        self.calls.append((dict(params), fold_index))
        return {"metrics": self.metrics_fn(params, fold_index)}


def _good_metrics(calmar):
    return {"calmar": calmar, "max_drawdown_pct": -20.0, "total_trades": 50}


def test_objective_calls_all_four_folds_when_no_pruner():
    engine = FakeEngine(lambda params, i: _good_metrics(1.0))
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, STRATEGY_CONFIG, "confirmed_locked"), n_trials=3)
    assert len(engine.calls) == BASELINE_CALLS + 3 * 4, "expected exactly 4 engine calls (one per fold) per trial, plus the one-time baseline"
    assert {fold_i for _, fold_i in engine.calls} == {0, 1, 2, 3}


def test_objective_uses_median_minus_half_iqr():
    # calmars [1.0, 2.0, 3.0, 4.0] -> median=2.5, iqr=(3.25-1.75)=1.5 -> 2.5-0.75=1.75
    fold_calmars = [1.0, 2.0, 3.0, 4.0]
    engine = FakeEngine(lambda params, i: _good_metrics(fold_calmars[i]))
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, STRATEGY_CONFIG, "confirmed_locked"), n_trials=1)
    assert study.trials[0].value == pytest.approx(1.75, abs=1e-6)


def test_objective_rejects_on_drawdown_floor():
    bad_dd = MAX_DRAWDOWN_FLOOR_PCT - 5.0

    def metrics_fn(params, i):
        if i == 1:
            return {"calmar": 5.0, "max_drawdown_pct": bad_dd, "total_trades": 50}
        return _good_metrics(1.0)

    engine = FakeEngine(metrics_fn)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, STRATEGY_CONFIG, "confirmed_locked"), n_trials=1)
    assert study.trials[0].value == REJECTED_VALUE
    # Must stop at the failing fold, not keep calling the engine needlessly
    assert len(engine.calls) == BASELINE_CALLS + 2


def test_objective_rejects_on_min_trades():
    # The floor is relative to the baseline's own trade count (min(10,
    # baseline) -- see objective.py's min-trades note), so the baseline
    # calls (the first 4, made by compute_baselines before any trial runs)
    # must report a healthy trade count, and only the actual trial's fold-2
    # call reports too few -- otherwise a low baseline would just lower the
    # floor to match and nothing would get rejected.
    def metrics_fn(params, i):
        is_baseline_call = len(engine.calls) <= BASELINE_CALLS
        if i == 2 and not is_baseline_call:
            return {"calmar": 5.0, "max_drawdown_pct": -20.0, "total_trades": MIN_TRADES_PER_FOLD - 1}
        return _good_metrics(1.0)

    engine = FakeEngine(metrics_fn)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, STRATEGY_CONFIG, "confirmed_locked"), n_trials=1)
    assert study.trials[0].value == REJECTED_VALUE
    assert len(engine.calls) == BASELINE_CALLS + 3


DEFENSIVE_STRATEGY_CONFIG = {
    "id": "step2_upro_tqqq_6state",  # matches a real DEFENSIVE_STATES entry
    "params": {
        "confirmDays": {"type": "range", "default": 2, "min": 0, "max": 15, "step": 1, "tier": "free"},
    }
}


def _log_with_defensive_fraction(fraction: float, n: int = 100) -> list[dict]:
    n_defensive = round(n * fraction)
    return [{"state": "DEF_SPY"}] * n_defensive + [{"state": "BULL_FULL"}] * (n - n_defensive)


class DailyLogFakeEngine:
    """Like FakeEngine, but also returns a daily_log so the defensive-floor
    constraint has something to check. baseline_fraction is what the first
    4 calls report (make_objective() always calls
    compute_baseline_defensive_fractions() -- 4 calls, one per dev fold --
    before returning the objective, so those are unambiguously the first 4
    calls this engine ever sees, regardless of what params they carry);
    trial_fraction is what every call after that reports."""

    def __init__(self, baseline_fraction: float, trial_fraction: float):
        self.baseline_fraction = baseline_fraction
        self.trial_fraction = trial_fraction
        self.calls_seen = 0

    def run(self, params):
        self.calls_seen += 1
        fraction = self.baseline_fraction if self.calls_seen <= 4 else self.trial_fraction
        return {
            "metrics": {"calmar": 5.0, "max_drawdown_pct": -20.0, "total_trades": 50},
            "daily_log": _log_with_defensive_fraction(fraction),
        }


def test_objective_rejects_when_defensive_usage_drops_far_below_baseline():
    """The exact scenario that motivated this constraint: a param set that
    drives defensive-state usage down to a small fraction of what the
    strategy's own default produces (here: 20% of baseline, well under the
    40% floor) should be rejected outright."""
    engine = DailyLogFakeEngine(baseline_fraction=0.08, trial_fraction=0.08 * 0.2)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, DEFENSIVE_STRATEGY_CONFIG, "confirmed_locked"), n_trials=1)
    assert study.trials[0].value == REJECTED_VALUE
    assert study.trials[0].user_attrs["rejected_reason"] == "defensive_state_floor"


def test_objective_accepts_defensive_usage_comfortably_above_baseline_floor():
    """Sanity check the constraint isn't so tight it rejects normal
    variation: 80% of baseline usage (well above the 40% floor) should be
    fine."""
    engine = DailyLogFakeEngine(baseline_fraction=0.08, trial_fraction=0.08 * 0.8)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, DEFENSIVE_STRATEGY_CONFIG, "confirmed_locked"), n_trials=1)
    assert study.trials[0].value != REJECTED_VALUE


class TradesFakeEngine:
    """Like FakeEngine, but reports a controllable total_trades: the first
    4 calls (the baseline, from compute_baselines) report
    baseline_trades; every call after that reports trial_trades."""

    def __init__(self, baseline_trades: int, trial_trades: int):
        self.baseline_trades = baseline_trades
        self.trial_trades = trial_trades
        self.calls_seen = 0

    def run(self, params):
        self.calls_seen += 1
        trades = self.baseline_trades if self.calls_seen <= 4 else self.trial_trades
        return {
            "metrics": {"calmar": 5.0, "max_drawdown_pct": -20.0, "total_trades": trades},
            "daily_log": [],
        }


def test_objective_rejects_min_trades_when_below_baseline_even_if_baseline_is_low():
    """This is the exact bug found running Step 3 (a rare-event overlay
    whose confirmed default produces as few as 0-4 trades in some dev
    folds): a flat floor of 10 rejected literally every trial, including
    the confirmed baseline itself. The floor must scale down with a low
    baseline -- but a trial trading even LESS than that already-low
    baseline should still be rejected."""
    engine = TradesFakeEngine(baseline_trades=2, trial_trades=1)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, STRATEGY_CONFIG, "confirmed_locked"), n_trials=1)
    assert study.trials[0].value == REJECTED_VALUE
    assert study.trials[0].user_attrs["rejected_reason"] == "min_trades"


def test_objective_accepts_zero_trades_when_baseline_is_also_zero():
    """The specific case that broke Step 3's fold 0: the confirmed default
    itself produced 0 trades there (no qualifying panic happened in that
    window) -- a trial that also produces 0 trades in that fold is the
    strategy correctly doing nothing, not a broken param set, and must not
    be rejected."""
    engine = TradesFakeEngine(baseline_trades=0, trial_trades=0)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, STRATEGY_CONFIG, "confirmed_locked"), n_trials=1)
    assert study.trials[0].value != REJECTED_VALUE


def test_objective_still_enforces_flat_floor_when_baseline_is_healthy():
    """Step 1/Step 2 trade often enough that the relative floor should
    behave just like the old flat floor of 10 -- a trial with fewer than
    10 trades gets rejected even though the baseline easily clears 10."""
    engine = TradesFakeEngine(baseline_trades=50, trial_trades=MIN_TRADES_PER_FOLD - 1)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, STRATEGY_CONFIG, "confirmed_locked"), n_trials=1)
    assert study.trials[0].value == REJECTED_VALUE
    assert study.trials[0].user_attrs["rejected_reason"] == "min_trades"


def test_objective_skips_defensive_floor_for_unregistered_strategy():
    """A strategy id with no DEFENSIVE_STATES entry (e.g. v3/v5/v5b, or in
    this test a config with no id at all) must not be affected by this
    constraint -- confirms the earlier fake-engine tests above, which rely
    on exactly this behavior, aren't accidentally relying on a fluke."""
    engine = DailyLogFakeEngine(baseline_fraction=0.5, trial_fraction=0.0)  # would fail the floor if it applied
    config_no_id = {"params": DEFENSIVE_STRATEGY_CONFIG["params"]}
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, config_no_id, "confirmed_locked"), n_trials=1)
    assert study.trials[0].value != REJECTED_VALUE


def test_ulcer_index_zero_for_pure_uptrend():
    """No drawdown anywhere -- portfolio value only ever rises -- should
    score a perfect 0."""
    daily_log = [{"portfolio_value": v} for v in [100, 110, 120, 130, 140]]
    assert _ulcer_index(daily_log) == pytest.approx(0.0)


def test_ulcer_index_penalizes_duration_not_just_depth():
    """Two series with the identical single worst drawdown (-20%), but one
    recovers immediately and the other stays underwater for most of the
    period -- Ulcer Index must be higher for the one that stays down
    longer, which is exactly what max_drawdown_pct alone can't see."""
    quick_recovery = [100, 80, 100, 100, 100, 100, 100, 100, 100, 100]
    slow_recovery = [100, 80, 80, 80, 80, 80, 80, 80, 80, 100]
    ui_quick = _ulcer_index([{"portfolio_value": v} for v in quick_recovery])
    ui_slow = _ulcer_index([{"portfolio_value": v} for v in slow_recovery])
    assert ui_slow > ui_quick


def test_fold_score_dispatches_to_the_right_metric():
    metrics = {"calmar": 1.5, "cagr_pct": 20.0, "sharpe": 0.8, "max_drawdown_pct": -20.0, "total_trades": 50}
    daily_log = [{"portfolio_value": v} for v in [100, 90, 100, 110]]  # some real drawdown, ulcer > 0

    assert _fold_score(metrics, daily_log, "calmar") == 1.5
    assert _fold_score(metrics, daily_log, "sharpe") == 0.8
    assert _fold_score(metrics, daily_log, "cagr") == 20.0
    ui = _ulcer_index(daily_log)
    assert _fold_score(metrics, daily_log, "martin") == pytest.approx(20.0 / ui)


def test_fold_score_martin_is_zero_when_no_drawdown_at_all():
    """Avoid a division by zero when a candidate has literally no
    drawdown in a fold -- score 0 rather than raising or returning inf."""
    metrics = {"cagr_pct": 5.0}
    daily_log = [{"portfolio_value": v} for v in [100, 110, 120]]
    assert _fold_score(metrics, daily_log, "martin") == 0.0


def test_fold_score_rejects_unknown_reward():
    with pytest.raises(ValueError):
        _fold_score({}, [], "not_a_real_reward")


def test_make_objective_accepts_alternate_reward():
    """End-to-end: passing reward='sharpe' should score trials on sharpe,
    not calmar -- confirms the parameter actually threads through, not
    just that the dispatcher function works in isolation."""
    def metrics_fn(params, i):
        return {"calmar": 1.0, "sharpe": 9.0, "max_drawdown_pct": -20.0, "total_trades": 50}

    engine = FakeEngine(metrics_fn)
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, STRATEGY_CONFIG, "confirmed_locked", reward="sharpe"), n_trials=1)
    assert study.trials[0].value == pytest.approx(9.0)


def _log_with_cash_fraction(fraction: float, n: int = 100) -> list[dict]:
    n_cash = round(n * fraction)
    return [{"state": "CASH"}] * n_cash + [{"state": "BULL_FULL"}] * (n - n_cash)


class CashFakeEngine:
    """Like DailyLogFakeEngine, but for the excess-cash ceiling: the first
    4 calls (baseline) report baseline_fraction time in CASH; every call
    after that reports trial_fraction."""

    def __init__(self, baseline_fraction: float, trial_fraction: float):
        self.baseline_fraction = baseline_fraction
        self.trial_fraction = trial_fraction
        self.calls_seen = 0

    def run(self, params):
        self.calls_seen += 1
        fraction = self.baseline_fraction if self.calls_seen <= 4 else self.trial_fraction
        return {
            "metrics": {"calmar": 5.0, "max_drawdown_pct": -20.0, "total_trades": 50},
            "daily_log": _log_with_cash_fraction(fraction),
        }


def test_objective_rejects_when_sitting_out_far_more_than_baseline():
    """The mirror-image risk to the defensive-floor case: a candidate that
    spends much more time in CASH than the strategy's own confirmed
    default -- avoiding participation rather than avoiding losses -- must
    be rejected even though its raw score might look attractive (e.g.
    under a Sharpe-style reward that rewards low volatility)."""
    engine = CashFakeEngine(baseline_fraction=0.15, trial_fraction=0.15 * (MAX_CASH_FRACTION_OF_BASELINE + 1))
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, DEFENSIVE_STRATEGY_CONFIG, "confirmed_locked"), n_trials=1)
    assert study.trials[0].value == REJECTED_VALUE
    assert study.trials[0].user_attrs["rejected_reason"] == "excess_cash"


def test_objective_accepts_cash_usage_within_the_ceiling():
    engine = CashFakeEngine(baseline_fraction=0.15, trial_fraction=0.15 * (MAX_CASH_FRACTION_OF_BASELINE - 0.2))
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(make_objective(engine, DEFENSIVE_STRATEGY_CONFIG, "confirmed_locked"), n_trials=1)
    assert study.trials[0].value != REJECTED_VALUE


def test_objective_prunes_bad_trial_after_first_fold():
    """With a MedianPruner, a trial whose fold-0 calmar is far below the
    running median from prior trials should stop after fold 0 instead of
    running all 4 folds -- this is the throughput win the plan counts on.

    This drives optuna's pruning machinery directly (rather than through
    make_objective + FakeEngine) so each trial's fold-0 calmar can be
    controlled by trial number -- first 5 trials score well (establishing
    a high running median), the 6th scores badly in fold 0 only."""
    strategy_config = STRATEGY_CONFIG

    def make_variable_objective():
        from app.optimize.search_space import suggest_params
        import numpy as np

        def objective(trial):
            params = suggest_params(trial, strategy_config, "confirmed_locked")
            calmar_fold0 = 0.1 if trial.number == 5 else 5.0
            calls = 0
            fold_calmars = []
            for step in range(4):
                calls += 1
                calmar = calmar_fold0 if step == 0 else 5.0
                fold_calmars.append(calmar)
                trial.report(calmar, step=step)
                if trial.should_prune():
                    trial.set_user_attr("calls_made", calls)
                    raise optuna.TrialPruned()
            trial.set_user_attr("calls_made", calls)
            return float(np.median(fold_calmars))
        return objective

    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.RandomSampler(seed=1),
        pruner=optuna.pruners.MedianPruner(n_startup_trials=3, n_warmup_steps=0, interval_steps=1),
    )
    study.optimize(make_variable_objective(), n_trials=6)

    pruned_trial = study.trials[5]
    assert pruned_trial.state == optuna.trial.TrialState.PRUNED
    assert pruned_trial.user_attrs["calls_made"] == 1, (
        "a trial far below the running median after fold 0 should be pruned "
        "immediately, not run through all 4 folds"
    )
