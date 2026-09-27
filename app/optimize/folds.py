"""
Date ranges for honest parameter search, per the optimization plan
(/Users/chirag/.claude/plans/crystalline-beaming-castle.md, Step 7).

Three layers, all fixed here -- not adjustable by a study, a user, or a
reward function. Letting the split itself be tuned is exactly how
flattering backtest numbers get manufactured.

  1. DEV_FOLDS -- four contiguous, non-overlapping ~2.75-year blocks
     spanning 2011-2021. A study is scored on the median across these four,
     not the whole period at once, so a param set that only works in one
     lucky stretch loses to one that works everywhere.
  2. HOLDOUT -- 2022-02-01 to 2025-12-30. Never touched during search. A
     one-month embargo after DEV_FOLDS ends so nothing bleeds across the
     boundary (indicator warmup, rolling windows). Run exactly once per
     strategy, at the very end (Step 7) -- re-running it repeatedly would
     turn it into more tuning data.
  3. FULL_HISTORY_END -- 2025-12-30, the last date the merged 2011-2025
     dataset actually covers (Polygon 2011-2019 + DataBento 2020-2025).
"""

DEV_FOLDS = [
    ("2011-01-01", "2013-09-30"),
    ("2013-10-01", "2016-06-30"),
    ("2016-07-01", "2019-03-31"),
    ("2019-04-01", "2021-12-31"),
]

HOLDOUT = ("2022-02-01", "2025-12-30")

FULL_HISTORY_END = "2025-12-30"
