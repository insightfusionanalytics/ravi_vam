"""
The Optuna objective: scores one candidate parameter set across the four
development folds (app/optimize/folds.py), never the sealed holdout.

Default reward = median(fold Calmar) - 0.5 * IQR(fold Calmar). Calmar
(CAGR / |max drawdown|) was chosen over raw CAGR (which just picks max
leverage forever) and over Sharpe (unreliable on fat-tailed leveraged-ETF
daily returns) -- see the plan. Median instead of mean across folds so one
lucky fold can't carry a param set; the IQR penalty pushes the search away
from param sets that only work in some folds and fall apart in others.
Alternative rewards ("martin", "sharpe", "cagr") are selectable via
make_objective's reward= argument -- see REWARD_FUNCTIONS below.

Hard constraints reject a candidate outright (large negative return, not a
score of merely "bad") rather than letting the sampler quietly explore
territory that isn't holdable in the first place:
  - max drawdown worse than -65% in any fold (not survivable)
  - fewer trades in a fold than the strategy's OWN confirmed/default params
    produce there, up to a cap of 10 (too few to be statistically
    meaningful) -- see the min-trades note below for why this is relative,
    not a flat number
  - defensive-state usage below 40% of what the strategy's OWN confirmed/
    default params produce in that same fold (see DEFENSIVE_STATES below)
  - time spent in CASH more than 75% above what the strategy's OWN
    confirmed/default params produce in that same fold (see CASH_STATES
    below) -- the mirror-image risk to the one above: a reward function
    that isn't Calmar (Sharpe in particular) can flatter a strategy that
    just sits out of the market most of the time, since near-zero returns
    with near-zero volatility can look deceptively good on a ratio. This
    isn't "avoiding losses," it's avoiding participation -- caught the
    same way as the defensive-floor issue, by bounding how far a candidate
    may drift from the strategy's own normal behavior rather than trusting
    the reward number alone.

Min-trades note (2026-09-24): Step 3 is a rare-event overlay -- it only
trades when Step 2 is in CASH *and* SPY is below its 200-day average *and*
VIX has spiked, a compound condition that doesn't fire often. Running
Ravi's own confirmed default params through the dev folds produced 0, 4,
2, and 22 trades across the four folds respectively -- a flat floor of 10
rejected literally every trial in every fold, including the confirmed
baseline itself, because the strategy is *supposed* to sit idle most of
the time. The floor is now min(10, baseline trades in this fold) -- never
demands more activity than the strategy's own confirmed behavior, while
still catching a param combination that trades so rarely a single lucky
trade would dominate the fold's score (Step 1/Step 2 trade often enough
that this is functionally still a flat floor of 10 for them).

That third constraint exists because of a real finding (2026-09-24): an
early 200-trial pass let Optuna push Step 2's "confirmDays" (how many
consecutive weak days before the defensive-trim rule fires) up to 14,
which real SPY/QQQ streaks almost never sustain -- the strategy wasn't
tuning its defensive trigger, it was quietly disabling it (measured:
defensive-state days dropped from 7.4% at the confirmed default to 3.6%),
and it scored well purely because 2011-2021 happened to reward staying
invested. confirmDays is now a *fixed* param (see search_space.py) so it
can't do that directly -- but this constraint is the general backstop, so
some *other* free param combination can't quietly find the same loophole
later. The floor is relative to each engine's own default behavior, per
fold, rather than one fixed global percentage: a fixed number turned out
to be fragile (Step 1's own healthy default sits at 4.5% defensive-state
usage, uncomfortably close to Step 2's 3.6% degenerate case -- one global
threshold can't cleanly separate "normal" from "gutted" across engines
with different natural baselines).

Pruning: after each fold, trial.report() lets an optuna MedianPruner kill
a trial early if it's already worse than the running median at that fold
-- roughly halves total engine calls across a study without touching which
param set eventually wins (pruned trials never get selected; they just
stop wasting time proving it).
"""

import numpy as np
import optuna

from app.optimize.folds import DEV_FOLDS
from app.optimize.search_space import suggest_params

MAX_DRAWDOWN_FLOOR_PCT = -65.0
MIN_TRADES_PER_FOLD = 10
REJECTED_VALUE = -1000.0

# Which daily_log "state" values count as "the strategy being defensive",
# per strategy id. A strategy id with no entry here simply skips the
# defensive-floor constraint (nothing to compare against).
DEFENSIVE_STATES = {
    "step1_upro_4state": {"DEFENSIVE"},
    "step2_upro_tqqq_6state": {"DEF_SPY", "DEF_QQQ", "DEF_BOTH"},
    "step2_killfix_variant": {"DEF_SPY", "DEF_QQQ", "DEF_BOTH"},
    "step2_reentry_variant": {"DEF_SPY", "DEF_QQQ", "DEF_BOTH"},
    "step2_continuous_reentry_variant": {"DEF_SPY", "DEF_QQQ", "DEF_BOTH"},
}
MIN_DEFENSIVE_FRACTION_OF_BASELINE = 0.4

# Which daily_log "state" values count as "sitting out of the market
# entirely", per strategy id.
CASH_STATES = {
    "step1_upro_4state": {"CASH"},
    "step2_upro_tqqq_6state": {"CASH"},
    "step2_killfix_variant": {"CASH"},
    "step2_reentry_variant": {"CASH"},
    "step2_continuous_reentry_variant": {"CASH"},
}
MAX_CASH_FRACTION_OF_BASELINE = 1.75


def run_fold(engine_module, params: dict, fold_start: str, fold_end: str) -> dict:
    """Run one engine call restricted to a single fold's date range. Always
    uses the full-history dataset (folds live inside 2011-2021), sliced
    after indicator warmup by the engine's existing startDate/endDate
    handling -- no look-ahead leakage across the fold boundary. Returns the
    full result (metrics + daily_log), not just metrics -- the defensive-
    floor constraint needs the daily_log's state column."""
    fold_params = dict(params)
    fold_params["startDate"] = fold_start
    fold_params["endDate"] = fold_end
    fold_params["fullHistory"] = True
    return engine_module.run(fold_params)


def _state_fraction(daily_log: list[dict], states: set[str] | None) -> float | None:
    """None means 'not defined for this strategy, skip the check' -- not
    the same as 0.0 (which means 'defined, and it never happened')."""
    if states is None:
        return None
    if not daily_log:
        return 0.0
    n = sum(1 for row in daily_log if row["state"] in states)
    return n / len(daily_log)


def defensive_state_fraction(daily_log: list[dict], strategy_id: str | None) -> float | None:
    return _state_fraction(daily_log, DEFENSIVE_STATES.get(strategy_id))


def cash_state_fraction(daily_log: list[dict], strategy_id: str | None) -> float | None:
    return _state_fraction(daily_log, CASH_STATES.get(strategy_id))


def _ulcer_index(daily_log: list[dict]) -> float:
    """RMS of the percentage drawdown from the running peak, across the
    whole period. Unlike max_drawdown_pct (which only reports the single
    worst point), this penalizes BOTH how deep a drawdown gets AND how
    long the portfolio stays underwater -- two candidates with the same
    max drawdown can have very different Ulcer Indexes if one recovers in
    a month and the other takes two years."""
    if not daily_log:
        return 0.0
    values = np.array([row["portfolio_value"] for row in daily_log], dtype=float)
    running_max = np.maximum.accumulate(values)
    drawdown_pct = np.where(running_max > 0, (values - running_max) / running_max * 100, 0.0)
    return float(np.sqrt(np.mean(drawdown_pct ** 2)))


def _fold_score(metrics: dict, daily_log: list[dict], reward: str) -> float:
    """The per-fold number that gets combined (median - 0.5*IQR) across
    folds into a trial's final objective value. Selectable so the same
    search infrastructure can ask 'what if we optimized for X instead' --
    see make_objective's reward= argument."""
    if reward == "calmar":
        return metrics["calmar"]
    if reward == "martin":
        ui = _ulcer_index(daily_log)
        return metrics["cagr_pct"] / ui if ui > 0 else 0.0
    if reward == "sharpe":
        return metrics["sharpe"]
    if reward == "cagr":
        return metrics["cagr_pct"]
    raise ValueError(f"unknown reward: {reward!r} (expected calmar/martin/sharpe/cagr)")


def compute_baselines(engine_module, strategy_config: dict) -> tuple[list[float | None], list[float | None], list[int]]:
    """Runs the strategy's own confirmed/default params through every dev
    fold once -- the per-fold reference for 'how much defensive-state
    time', 'how much cash time', and 'how many trades' this strategy
    normally produces here. Cheap (4 extra engine calls) and only needs to
    happen once per study, not once per trial."""
    default_params = {
        key: p["default"] for key, p in strategy_config["params"].items()
        if p.get("type") == "range" and p.get("tier") != "input"
    }
    strategy_id = strategy_config.get("id")
    defensive_fractions, cash_fractions, trade_counts = [], [], []
    for fold_start, fold_end in DEV_FOLDS:
        result = run_fold(engine_module, default_params, fold_start, fold_end)
        daily_log = result.get("daily_log", [])
        defensive_fractions.append(defensive_state_fraction(daily_log, strategy_id))
        cash_fractions.append(cash_state_fraction(daily_log, strategy_id))
        trade_counts.append(result["metrics"]["total_trades"])
    return defensive_fractions, cash_fractions, trade_counts


def fold_passes_constraints(
    metrics: dict,
    daily_log: list[dict],
    strategy_id: str | None,
    baseline_defensive_fraction: float | None,
    baseline_cash_fraction: float | None,
    baseline_trades: int,
) -> tuple[bool, str | None]:
    if metrics["max_drawdown_pct"] < MAX_DRAWDOWN_FLOOR_PCT:
        return False, "max_drawdown"
    effective_min_trades = min(MIN_TRADES_PER_FOLD, baseline_trades)
    if metrics["total_trades"] < effective_min_trades:
        return False, "min_trades"
    def_frac = defensive_state_fraction(daily_log, strategy_id)
    if def_frac is not None and baseline_defensive_fraction is not None and baseline_defensive_fraction > 0:
        if def_frac < MIN_DEFENSIVE_FRACTION_OF_BASELINE * baseline_defensive_fraction:
            return False, "defensive_state_floor"
    cash_frac = cash_state_fraction(daily_log, strategy_id)
    if cash_frac is not None and baseline_cash_fraction is not None and baseline_cash_fraction > 0:
        if cash_frac > MAX_CASH_FRACTION_OF_BASELINE * baseline_cash_fraction:
            return False, "excess_cash"
    return True, None


def make_objective(engine_module, strategy_config: dict, mode: str, reward: str = "calmar"):
    """mode: 'confirmed_locked' or 'free_everything' -- see search_space.py.
    reward: 'calmar' (default), 'martin', 'sharpe', or 'cagr' -- see
    _fold_score. The hard constraints (drawdown floor, min-trades,
    defensive floor, excess-cash ceiling) apply the same way regardless of
    which reward is chosen -- switching the reward doesn't switch off the
    guardrails."""
    strategy_id = strategy_config.get("id")
    baseline_defensive, baseline_cash, baseline_trade_counts = compute_baselines(engine_module, strategy_config)

    def objective(trial: optuna.Trial) -> float:
        params = suggest_params(trial, strategy_config, mode)

        fold_scores = []
        for step, (fold_start, fold_end) in enumerate(DEV_FOLDS):
            result = run_fold(engine_module, params, fold_start, fold_end)
            metrics = result["metrics"]
            daily_log = result.get("daily_log", [])
            ok, reason = fold_passes_constraints(
                metrics, daily_log, strategy_id,
                baseline_defensive[step], baseline_cash[step], baseline_trade_counts[step],
            )
            if not ok:
                trial.set_user_attr("rejected_fold", step)
                trial.set_user_attr("rejected_reason", reason)
                return REJECTED_VALUE

            score = _fold_score(metrics, daily_log, reward)
            fold_scores.append(score)
            trial.report(score, step=step)
            if trial.should_prune():
                trial.set_user_attr("fold_scores", fold_scores)
                raise optuna.TrialPruned()

        arr = np.array(fold_scores)
        median = float(np.median(arr))
        iqr = float(np.percentile(arr, 75) - np.percentile(arr, 25))
        trial.set_user_attr("fold_scores", fold_scores)
        trial.set_user_attr("fold_median_score", median)
        trial.set_user_attr("fold_iqr_score", iqr)
        return median - 0.5 * iqr

    return objective
