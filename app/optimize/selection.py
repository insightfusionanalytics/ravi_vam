"""
Picks a parameter set from a completed study by finding the densest region
of good-scoring trials, instead of taking the single best trial
(argmax) at face value.

Why: across N noisy trials, the single best score is a biased estimator of
how good that param set actually is -- a lone spike surrounded by
mediocre neighbors is much more likely to be luck than a genuine effect,
because noise doesn't independently favor a whole cluster of neighboring
points at once. A dense region of nearby good scores is much harder to
produce by chance. This is standard practice in trading-strategy
optimization specifically (Pardo's walk-forward framework calls it picking
from the "center of a stable performance plateau").

Caveat this module exists to handle: the landscape can be multimodal --
two separate good regions with a bad valley between them. Blindly
averaging all "good" trials could land in that valley, worse than either
peak. So this does an actual density step (count neighbors within a
normalized radius) rather than a plain mean/median over the whole top-N.

This is a diagnostic, not a guarantee -- the chosen region still has to be
checked on the sealed holdout (Step 7) like any other candidate. What this
buys is a candidate that's less likely to be pure search-noise before that
check ever happens.
"""

from dataclasses import dataclass, field

import numpy as np
import optuna


@dataclass
class RegionSelection:
    method: str
    region_params: dict
    region_size: int
    region_out_of_top: int
    best_trial_params: dict
    best_trial_value: float
    best_trial_number: int
    region_center_trial_number: int | None = None
    region_center_trial_value: float | None = None
    param_spread: dict = field(default_factory=dict)
    agrees_with_best_trial: bool = False
    reason: str = ""


def _normalized_param_matrix(trials: list[optuna.trial.FrozenTrial], tunable_keys: list[str], search_space: dict) -> np.ndarray:
    """(n_trials, n_dims), each dimension min-max normalized to [0, 1]
    using its declared search range so no single wide-range parameter
    dominates the distance metric."""
    mat = np.zeros((len(trials), len(tunable_keys)))
    for j, key in enumerate(tunable_keys):
        lo, hi = search_space[key]["min"], search_space[key]["max"]
        span = (hi - lo) or 1
        for i, t in enumerate(trials):
            mat[i, j] = (t.params[key] - lo) / span
    return mat


def select_robust_region(
    study: optuna.Study,
    search_space: dict,
    top_frac: float = 0.15,
    min_top: int = 15,
    neighbor_radius: float = 0.25,
) -> RegionSelection:
    """search_space: {param_key: {"min": ..., "max": ...}} for exactly the
    tunable params of this study (e.g. from search_space.tunable_param_keys
    plus the strategy JSON's min/max for each).

    top_frac/min_top: how many of the best-scoring completed trials count
    as "good" before looking for a dense region among them.
    neighbor_radius: normalized-distance cutoff (in [0,1]-scaled param
    space) for two trials to count as neighbors. 0.25 is deliberately
    generous -- shrink it for a stricter, tighter notion of "the same
    region."
    """
    completed = [t for t in study.trials if t.state == optuna.trial.TrialState.COMPLETE]
    if not completed:
        raise ValueError("no completed trials to select from")

    completed_sorted = sorted(completed, key=lambda t: t.value, reverse=True)
    best = completed_sorted[0]

    tunable_keys = sorted(search_space.keys())
    n_top = min(max(min_top, int(len(completed_sorted) * top_frac)), len(completed_sorted))
    top_trials = completed_sorted[:n_top]

    if len(tunable_keys) == 0 or n_top < 3:
        return RegionSelection(
            method="best_trial_fallback",
            reason="fewer than 3 top trials or 0 tunable params -- not enough to assess a region",
            region_params=dict(best.params),
            region_size=1,
            region_out_of_top=n_top,
            best_trial_params=dict(best.params),
            best_trial_value=best.value,
            best_trial_number=best.number,
            agrees_with_best_trial=True,
        )

    mat = _normalized_param_matrix(top_trials, tunable_keys, search_space)
    n = len(top_trials)
    dists = np.sqrt(((mat[:, None, :] - mat[None, :, :]) ** 2).sum(axis=2))
    neighbor_counts = (dists <= neighbor_radius).sum(axis=1) - 1  # exclude self

    densest_idx = int(np.argmax(neighbor_counts))
    densest_trial = top_trials[densest_idx]
    neighbor_mask = dists[densest_idx] <= neighbor_radius
    neighbor_trials = [top_trials[i] for i in range(n) if neighbor_mask[i]]

    param_spread = {}
    region_params = {}
    for key in tunable_keys:
        vals = [t.params[key] for t in neighbor_trials]
        median = float(np.median(vals))
        param_spread[key] = {"median": median, "std": float(np.std(vals)), "n": len(vals)}
        region_params[key] = int(round(median))

    agrees = all(region_params[k] == best.params[k] for k in tunable_keys)

    return RegionSelection(
        method="density_region",
        region_params=region_params,
        region_size=len(neighbor_trials),
        region_out_of_top=n_top,
        region_center_trial_number=densest_trial.number,
        region_center_trial_value=densest_trial.value,
        param_spread=param_spread,
        best_trial_params=dict(best.params),
        best_trial_value=best.value,
        best_trial_number=best.number,
        agrees_with_best_trial=agrees,
    )
