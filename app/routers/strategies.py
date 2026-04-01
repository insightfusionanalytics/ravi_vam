"""
Strategy list, detail, and precomputed data endpoints.
"""

import json
from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse

from app.config import load_all_strategies, get_precomputed_csv_path, get_precomputed_metrics_path

router = APIRouter()


@router.get("/data-source")
def get_data_source():
    """
    Return the active data source so the frontend can display it.
    Possible values: 'databento', 'yahoo_fallback', 'unknown' (not yet resolved).
    """
    from app.config import ACTIVE_DATA_SOURCE
    return {
        "source": ACTIVE_DATA_SOURCE,
        "label": {
            "databento": "DataBento",
            "yahoo_fallback": "Yahoo Finance (fallback)",
            "unknown": "Not yet determined",
        }.get(ACTIVE_DATA_SOURCE, ACTIVE_DATA_SOURCE),
        "is_fallback": ACTIVE_DATA_SOURCE == "yahoo_fallback",
    }


@router.get("/strategies")
def list_strategies():
    """Return all strategy configs (name, desc, params schema, benchmark results)."""
    return load_all_strategies()


@router.get("/strategies/{strategy_id}")
def get_strategy(strategy_id: str):
    """Return a single strategy config."""
    strategies = load_all_strategies()
    if strategy_id not in strategies:
        raise HTTPException(status_code=404, detail=f"Strategy '{strategy_id}' not found")
    return strategies[strategy_id]


@router.get("/strategies/{strategy_id}/precomputed", response_class=PlainTextResponse)
def get_precomputed(strategy_id: str):
    """
    Return the pre-computed portfolio values CSV as plain text.
    The frontend parseCSV() function processes it directly.
    """
    csv_path = get_precomputed_csv_path(strategy_id)
    if csv_path:
        return csv_path.read_text()

    # For v3/v5/v5b — no row-by-row CSV, return empty to signal API-mode
    raise HTTPException(
        status_code=404,
        detail=f"No pre-computed CSV for '{strategy_id}'. Use POST /api/backtest instead."
    )


@router.get("/strategies/{strategy_id}/metrics")
def get_precomputed_metrics(strategy_id: str):
    """Return pre-computed performance metrics for a strategy."""
    metrics_path = get_precomputed_metrics_path(strategy_id)
    if not metrics_path:
        raise HTTPException(status_code=404, detail=f"No metrics found for '{strategy_id}'")

    data = json.loads(metrics_path.read_text())

    # Normalize v5/v5b nested structure
    if strategy_id == "v5_leveraged" and "v5_leveraged" in data:
        return data["v5_leveraged"]["performance"]
    if strategy_id == "v5b_nonleveraged" and "v5b_non_leveraged" in data:
        return data["v5b_non_leveraged"]["performance"]
    if strategy_id == "v3_7state_optimized" and "performance" in data:
        return data["performance"]

    return data
