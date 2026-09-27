"""
consecutive_streak() was rewritten from a per-row Python loop to a
vectorized groupby-cumsum (2026-09-24, part of making the engine fast
enough for an optimizer to call hundreds of times). This locks the new
version against a brute-force reference implementation of the *original*
row-by-row semantics, on both a hand-picked case and randomized series,
so a future change to either copy (scripts/vam_step1_databento.py,
scripts/vam_step2_databento.py) can't silently drift from the original
meaning: "count of consecutive True values ending at and including this
row, reset to 0 the moment the condition is False."
"""

import random
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import vam_step1_databento as step1_mod  # noqa: E402  (path insert above)
import vam_step2_databento as step2_mod  # noqa: E402


def _reference_streak(condition: pd.Series) -> pd.Series:
    """The original row-by-row implementation, kept only as a test oracle."""
    cond_int = condition.astype(int)
    streak = cond_int.copy()
    for i in range(1, len(streak)):
        if cond_int.iloc[i] == 1:
            streak.iloc[i] = streak.iloc[i - 1] + 1
        else:
            streak.iloc[i] = 0
    return streak


HAND_PICKED = [
    [True, True, False, True, True, True, False, False, True],
    [False, False, False],
    [True, True, True, True],
    [True],
    [False],
]


@pytest.mark.parametrize("values", HAND_PICKED)
@pytest.mark.parametrize("mod", [step1_mod, step2_mod], ids=["step1_script", "step2_script"])
def test_matches_reference_hand_picked(mod, values):
    condition = pd.Series(values)
    expected = _reference_streak(condition)
    actual = mod.consecutive_streak(condition)
    assert list(actual) == list(expected)


@pytest.mark.parametrize("seed", range(20))
@pytest.mark.parametrize("mod", [step1_mod, step2_mod], ids=["step1_script", "step2_script"])
def test_matches_reference_randomized(mod, seed):
    rng = random.Random(seed)
    values = [rng.random() < 0.5 for _ in range(500)]
    condition = pd.Series(values)
    expected = _reference_streak(condition)
    actual = mod.consecutive_streak(condition)
    assert list(actual) == list(expected)
