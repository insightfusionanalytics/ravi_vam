"""
Strategy config loader — discovers all strategy JSON files in strategies/.
No hardcoded strategy list: add a new .json file to register a new strategy.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STRATEGIES_DIR = PROJECT_ROOT / "strategies"
RESULTS_DIR = PROJECT_ROOT / "results"


def load_all_strategies() -> dict:
    """Glob strategies/*.json and return {id: config} dict."""
    strategies = {}
    for path in sorted(STRATEGIES_DIR.glob("*.json")):
        # Skip existing YAML-derived files that are not our new configs
        try:
            data = json.loads(path.read_text())
            if "id" in data and "params" in data:
                strategies[data["id"]] = data
        except (json.JSONDecodeError, KeyError):
            continue
    return strategies


def get_strategy(strategy_id: str) -> dict | None:
    strategies = load_all_strategies()
    return strategies.get(strategy_id)


def _is_valid_csv(path: Path) -> bool:
    """Return True if the CSV exists and isn't a zero-filled OneDrive placeholder."""
    if not path.exists():
        return False
    try:
        with open(path, "rb") as f:
            sample = f.read(200)
        return any(b != 0 for b in sample)
    except OSError:
        return False


def find_data_dir() -> Path | None:
    """
    Locate the DataBento data directory.
    Priority: RAVI_DATA_DIR env var → local data/ → well-known IFA OneDrive path.
    Ignores OneDrive placeholder files (zero-filled stubs).
    """
    spy_rel = Path("databento") / "equities" / "SPY_daily.csv"

    # 1. Environment variable override
    env_path = os.environ.get("RAVI_DATA_DIR")
    if env_path:
        p = Path(env_path)
        if _is_valid_csv(p / spy_rel):
            return p

    # 2. Sibling 'data/' next to project root (yfinance cache lives here)
    local_data = PROJECT_ROOT / "data"
    if _is_valid_csv(local_data / spy_rel):
        return local_data

    # 3. Well-known IFA Framework path (OneDrive) — only if files are real
    ifa_path = (
        Path.home()
        / "Library/Group Containers"
        / "UBF8T346G9.OneDriveSyncClientSuite"
        / "OneDrive.noindex/OneDrive"
        / "Insight fusion Analytics/Coders/Anmol/IFA Perfect"
        / "Prompting General Framework/IFA AI Framework"
        / "projects/algo_trading/_engine/code/data"
    )
    if _is_valid_csv(ifa_path / spy_rel):
        return ifa_path

    # 4. No valid data found — return local data/ path so fetcher can populate it
    return PROJECT_ROOT / "data"


def ensure_data_available() -> Path:
    """
    Ensure market data CSVs exist locally.
    Priority: DataBento CSVs first. Falls back to yfinance download if missing.
    Updates ACTIVE_DATA_SOURCE so the API/UI can inform the user.
    Returns the data directory.
    """
    global ACTIVE_DATA_SOURCE
    import logging
    logger = logging.getLogger(__name__)

    data_dir = find_data_dir()
    spy_path = data_dir / "databento" / "equities" / "SPY_daily.csv"

    yahoo_marker = data_dir / "databento" / "equities" / ".yfinance_source"

    if _is_valid_csv(spy_path) and not yahoo_marker.exists():
        ACTIVE_DATA_SOURCE = "databento"
        logger.info("Using DataBento data from %s", data_dir)
        return data_dir

    if _is_valid_csv(spy_path) and yahoo_marker.exists():
        # Data exists but was previously downloaded from yfinance
        ACTIVE_DATA_SOURCE = "yahoo_fallback"
        logger.warning("DATA SOURCE: Yahoo Finance (cached) — DataBento CSVs were not originally available.")
        return data_dir

    # DataBento not available — fall back to yfinance
    logger.warning(
        "DataBento data not found or invalid at %s — falling back to Yahoo Finance (yfinance).",
        data_dir,
    )
    from app.data_fetcher import ensure_equity_data, ensure_vix_data

    ensure_equity_data(data_dir)
    ensure_vix_data(data_dir)

    # Write marker so future runs know this is yfinance data
    yahoo_marker.write_text("Downloaded via yfinance fallback. Delete this file and provide real DataBento CSVs to switch.\n")

    ACTIVE_DATA_SOURCE = "yahoo_fallback"
    logger.warning("DATA SOURCE: Yahoo Finance (yfinance) — DataBento CSVs were not available.")

    return data_dir


DATA_DIR = find_data_dir()

# Track which data source is active — updated by ensure_data_available()
# Possible values: "databento", "yahoo_fallback", "unknown"
ACTIVE_DATA_SOURCE: str = "unknown"


def get_precomputed_csv_path(strategy_id: str) -> Path | None:
    """Return the path to the pre-computed portfolio values CSV for a strategy."""
    mapping = {
        "step1_upro_4state": RESULTS_DIR / "step1_databento_portfolio_values.csv",
        "step2_upro_tqqq_6state": RESULTS_DIR / "step1_step2_databento_final" / "step2_databento_portfolio_values.csv",
        # v3/v5/v5b don't have row-by-row CSVs, only summary JSONs
    }
    path = mapping.get(strategy_id)
    if path and path.exists():
        return path
    return None


def get_precomputed_metrics_path(strategy_id: str) -> Path | None:
    """Return the path to pre-computed metrics JSON for a strategy."""
    mapping = {
        "step1_upro_4state": RESULTS_DIR / "step1_databento_metrics.json",
        "step2_upro_tqqq_6state": RESULTS_DIR / "step2_databento_metrics.json",
        "v3_7state_optimized": RESULTS_DIR / "ravi_vam_v3_databento_results.json",
        "v5_leveraged": RESULTS_DIR / "ravi_vam_v5_databento_results.json",
        "v5b_nonleveraged": RESULTS_DIR / "ravi_vam_v5_databento_results.json",
    }
    path = mapping.get(strategy_id)
    if path and path.exists():
        return path
    return None
