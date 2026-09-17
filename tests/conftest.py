"""
Shared fixtures for the backtest engine test suite.

Each engine is run once per test session (not once per test) since a full
backtest takes real time — dozens of tests can then check different
properties of the same result without re-running pandas over the whole
dataset each time.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture(scope="session")
def step1_result():
    from app.engines import step1
    return step1.run({})


@pytest.fixture(scope="session")
def step2_result():
    from app.engines import step2
    return step2.run({})


@pytest.fixture(scope="session")
def step3_result():
    from app.engines import step3
    return step3.run({})


@pytest.fixture(scope="session")
def step4_result():
    from app.engines import step4
    return step4.run({})


@pytest.fixture(scope="session")
def step1_full_history_result():
    from app.engines import step1
    return step1.run({"fullHistory": True})


@pytest.fixture(scope="session")
def step2_full_history_result():
    from app.engines import step2
    return step2.run({"fullHistory": True})


ALL_ENGINE_RESULT_FIXTURES = [
    "step1_result",
    "step2_result",
    "step3_result",
    "step4_result",
]
