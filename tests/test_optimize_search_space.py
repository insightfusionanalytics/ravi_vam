"""
Verifies the optimizer's search space respects the confirmed/free/input
tiering added to strategies/*.json in Step 0 -- this is the thing that
makes "confirmed_locked" mode actually mean "Ravi's numbers stay put."
"""

import json
from pathlib import Path

import optuna
import pytest

from app.optimize.search_space import suggest_params, tunable_param_keys

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load(strategy_file: str) -> dict:
    return json.loads((REPO_ROOT / "strategies" / strategy_file).read_text())


STEP1 = _load("step1_upro_4state.json")
STEP2 = _load("step2_upro_tqqq_6state.json")


@pytest.mark.parametrize("config,expected_confirmed", [
    (STEP1, {"vixThreshold", "smaKill", "smaDef", "rsiOB", "rsiRe"}),
    (STEP2, {"vixThreshold", "smaKill", "smaDef", "rsiOB", "rsiRe"}),
], ids=["step1", "step2"])
def test_confirmed_locked_excludes_confirmed_tier(config, expected_confirmed):
    tunable = set(tunable_param_keys(config, "confirmed_locked"))
    assert tunable.isdisjoint(expected_confirmed), (
        f"confirmed_locked mode must never tune Ravi's confirmed values, "
        f"but found {tunable & expected_confirmed} in the search space"
    )


@pytest.mark.parametrize("config", [STEP1, STEP2], ids=["step1", "step2"])
def test_free_everything_includes_confirmed_tier(config):
    tunable = set(tunable_param_keys(config, "free_everything"))
    confirmed_keys = {k for k, p in config["params"].items() if p.get("tier") == "confirmed"}
    assert confirmed_keys <= tunable


@pytest.mark.parametrize("config", [STEP1, STEP2], ids=["step1", "step2"])
def test_capital_never_in_search_space(config):
    for mode in ("confirmed_locked", "free_everything"):
        assert "capital" not in tunable_param_keys(config, mode)


@pytest.mark.parametrize("config", [STEP1, STEP2], ids=["step1", "step2"])
def test_confirmed_locked_suggested_params_match_json_defaults(config):
    """The whole point of the confirmed tier: a confirmed_locked trial's
    params for those keys must equal Ravi's numbers, regardless of what
    the sampler would otherwise pick."""
    study = optuna.create_study(sampler=optuna.samplers.RandomSampler(seed=1))
    confirmed_keys = {k for k, p in config["params"].items() if p.get("tier") == "confirmed"}

    def objective(trial):
        params = suggest_params(trial, config, "confirmed_locked")
        for key in confirmed_keys:
            assert params[key] == config["params"][key]["default"]
        return 0.0

    study.optimize(objective, n_trials=10)


@pytest.mark.parametrize("config", [STEP1, STEP2], ids=["step1", "step2"])
def test_suggested_params_stay_within_declared_range(config):
    study = optuna.create_study(sampler=optuna.samplers.RandomSampler(seed=2))

    def objective(trial):
        params = suggest_params(trial, config, "free_everything")
        for key, p in config["params"].items():
            if p.get("tier") == "input":
                continue
            assert p["min"] <= params[key] <= p["max"], f"{key}={params[key]} outside [{p['min']}, {p['max']}]"
        return 0.0

    study.optimize(objective, n_trials=10)


def test_capital_absent_from_suggested_params():
    study = optuna.create_study(sampler=optuna.samplers.RandomSampler(seed=3))

    def objective(trial):
        params = suggest_params(trial, STEP1, "free_everything")
        assert "capital" not in params
        return 0.0

    study.optimize(objective, n_trials=5)


@pytest.mark.parametrize("config", [STEP1, STEP2], ids=["step1", "step2"])
def test_fixed_tier_never_tunable_in_either_mode(config):
    """confirmDays is 'fixed' as of 2026-09-24 (see its tier_note): freeing
    it let an optimizer disable the defensive-trim rule outright rather
    than genuinely tune it. It must be excluded from the search space in
    BOTH modes -- unlike 'confirmed', which is only locked in
    confirmed_locked mode."""
    fixed_keys = {k for k, p in config["params"].items() if p.get("tier") == "fixed"}
    assert "confirmDays" in fixed_keys, "sanity check: this test assumes confirmDays is tagged fixed"
    for mode in ("confirmed_locked", "free_everything"):
        tunable = set(tunable_param_keys(config, mode))
        assert tunable.isdisjoint(fixed_keys), f"fixed-tier params must never be tunable, found {tunable & fixed_keys} in {mode} mode"


@pytest.mark.parametrize("config", [STEP1, STEP2], ids=["step1", "step2"])
def test_fixed_tier_suggested_params_always_equal_default(config):
    fixed_keys = {k for k, p in config["params"].items() if p.get("tier") == "fixed"}
    for mode in ("confirmed_locked", "free_everything"):
        study = optuna.create_study(sampler=optuna.samplers.RandomSampler(seed=4))

        def objective(trial, mode=mode):
            params = suggest_params(trial, config, mode)
            for key in fixed_keys:
                assert params[key] == config["params"][key]["default"]
            return 0.0

        study.optimize(objective, n_trials=10)
