"""
Turns a strategy's params JSON into an Optuna search space, respecting the
confirmed/free/input tiering added in strategies/*.json (2026-09-24).

  confirmed -- a number Ravi actually gave (e.g. the 200-day trend line,
               VIX=30). Tunable only in "free_everything" mode; held fixed
               at its JSON default in "confirmed_locked" mode.
  free      -- never specified by Ravi, an implementation detail. Always
               tunable.
  fixed     -- never tunable, in EITHER mode, even though it's not one of
               Ravi's numbers either. Reserved for params where letting an
               optimizer search freely turned out to find a loophole
               rather than a real improvement (found 2026-09-24:
               Step 2's confirmDays, pushed to 14, which real market
               streaks almost never sustain -- the search wasn't tuning
               the defensive-trim trigger, it was quietly disabling it).
               Always held at its JSON default.
  input     -- account size (capital). Never part of the search space or
               the params dict passed to an engine's run() -- it's a
               separate function argument, not a strategy setting.

Every range param in strategies/step1_upro_4state.json and
strategies/step2_upro_tqqq_6state.json has integer default/min/max/step
(verified 2026-09-24), so this always uses suggest_int. A strategy with a
genuinely fractional param would need this extended -- deliberately not
handled speculatively here.
"""

import optuna


def tunable_param_keys(strategy_config: dict, mode: str) -> list[str]:
    """mode: 'confirmed_locked' or 'free_everything'."""
    if mode not in ("confirmed_locked", "free_everything"):
        raise ValueError(f"unknown mode: {mode!r}")
    keys = []
    for key, p in strategy_config["params"].items():
        if p.get("type") != "range":
            continue
        tier = p.get("tier")
        if tier in ("input", "fixed"):
            continue
        if tier == "confirmed" and mode == "confirmed_locked":
            continue
        if tier in ("confirmed", "free"):
            keys.append(key)
    return keys


def suggest_params(trial: optuna.Trial, strategy_config: dict, mode: str) -> dict:
    """A concrete params dict for one trial: Optuna-suggested values for
    tunable params, the JSON default for every other range param.
    Non-range params (labels/units on non-slider fields, if any) and the
    'input' tier (capital) are never included -- capital isn't part of the
    params dict contract, it's run()'s separate initial_capital argument."""
    tunable = set(tunable_param_keys(strategy_config, mode))
    result = {}
    for key, p in strategy_config["params"].items():
        if p.get("type") != "range" or p.get("tier") == "input":
            continue
        if key in tunable:
            result[key] = trial.suggest_int(key, int(p["min"]), int(p["max"]), step=int(p.get("step", 1)))
        else:
            result[key] = p["default"]
    return result
