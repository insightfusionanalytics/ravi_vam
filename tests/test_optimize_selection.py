"""
Tests select_robust_region against synthetic studies with known structure,
so the core claim -- "pick the dense region, not the lone spike" -- is
verified mechanically rather than just argued for.
"""

import optuna
import pytest

from app.optimize.selection import select_robust_region


def _add_trial(study, x_value, y_value, score):
    study.add_trial(
        optuna.trial.create_trial(
            params={"x": x_value, "y": y_value},
            distributions={
                "x": optuna.distributions.IntDistribution(0, 100),
                "y": optuna.distributions.IntDistribution(0, 100),
            },
            value=score,
        )
    )


SEARCH_SPACE = {"x": {"min": 0, "max": 100}, "y": {"min": 0, "max": 100}}


def test_picks_dense_region_over_lone_spike():
    """A cluster of 20 trials scoring ~8-9 around (50, 50), plus a single
    isolated trial at (95, 5) scoring 10 (the highest of all). The naive
    argmax would pick the isolated spike; the region-based selector should
    pick from the cluster instead."""
    study = optuna.create_study(direction="maximize")
    import random
    rng = random.Random(0)
    for _ in range(20):
        x = 50 + rng.randint(-5, 5)
        y = 50 + rng.randint(-5, 5)
        score = 8.0 + rng.uniform(0, 1.0)
        _add_trial(study, x, y, score)
    # a handful of clearly worse trials scattered elsewhere, so top_frac
    # actually has to discriminate
    for _ in range(30):
        x = rng.randint(0, 100)
        y = rng.randint(0, 100)
        score = rng.uniform(0, 3.0)
        _add_trial(study, x, y, score)
    # the lone spike: best raw score, but has no neighbors
    _add_trial(study, 95, 5, 10.0)

    result = select_robust_region(study, SEARCH_SPACE, top_frac=0.2, min_top=15, neighbor_radius=0.15)

    assert result.best_trial_params == {"x": 95, "y": 5}, "sanity check: the spike really is the raw best trial"
    assert abs(result.region_params["x"] - 50) <= 10, f"expected region near x=50, got {result.region_params}"
    assert abs(result.region_params["y"] - 50) <= 10, f"expected region near y=50, got {result.region_params}"
    assert not result.agrees_with_best_trial, "region pick should differ from the isolated spike"
    assert result.region_size >= 5, "the dense cluster should have several neighbors, not just itself"


def test_agrees_with_best_trial_when_best_trial_is_in_the_dense_region():
    """If the single best-scoring trial IS inside the dense region (the
    normal, non-adversarial case), the region pick and the raw best trial
    should land in the same place -- this isn't meant to always override
    the best trial, only to resist being fooled by an isolated spike."""
    study = optuna.create_study(direction="maximize")
    import random
    rng = random.Random(1)
    best_score = -1
    best_xy = None
    for _ in range(25):
        x = 50 + rng.randint(-5, 5)
        y = 50 + rng.randint(-5, 5)
        score = 8.0 + rng.uniform(0, 1.0)
        if score > best_score:
            best_score, best_xy = score, (x, y)
        _add_trial(study, x, y, score)

    result = select_robust_region(study, SEARCH_SPACE, top_frac=0.3, min_top=10, neighbor_radius=0.2)
    assert abs(result.region_params["x"] - 50) <= 10
    assert abs(result.region_params["y"] - 50) <= 10


def test_falls_back_gracefully_with_too_few_trials():
    study = optuna.create_study(direction="maximize")
    _add_trial(study, 10, 20, 5.0)
    _add_trial(study, 90, 80, 9.0)

    result = select_robust_region(study, SEARCH_SPACE, min_top=15)
    assert result.method == "best_trial_fallback"
    assert result.region_params == {"x": 90, "y": 80}


def test_raises_on_no_completed_trials():
    study = optuna.create_study(direction="maximize")
    with pytest.raises(ValueError):
        select_robust_region(study, SEARCH_SPACE)
