"""
POST /api/backtest — runs a Python backtest engine with caller-supplied params.

Step1 / Step2 strategies: live recalc happens in browser JS. This endpoint
exists for server-side verification or when JS fallback is not available.

v3 / v5 / v5b: These require Python-side computation (ATR, drawdown circuit
breaker) and are the primary users of this endpoint.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


class BacktestRequest(BaseModel):
    strategy_id: str
    params: dict = {}
    initial_capital: float = 100_000.0


def _get_engine(strategy_id: str):
    """Lazy import engines to avoid loading scipy/pandas at startup."""
    if strategy_id == "step1_upro_4state":
        from app.engines.step1 import run
        return run
    elif strategy_id == "step2_upro_tqqq_6state":
        from app.engines.step2 import run
        return run
    elif strategy_id in ("v3_7state_optimized", "v5_leveraged", "v5b_nonleveraged"):
        from app.engines import v5 as v5_module
        fns = {
            "v3_7state_optimized": v5_module.run_v3,
            "v5_leveraged": v5_module.run_v5,
            "v5b_nonleveraged": v5_module.run_v5b,
        }
        return fns[strategy_id]
    return None


@router.post("/backtest")
def run_backtest(req: BacktestRequest):
    """
    Run a backtest with the given strategy and parameters.
    Returns: { daily_log: [...], trades: [...], metrics: {...} }
    """
    engine_fn = _get_engine(req.strategy_id)
    if engine_fn is None:
        raise HTTPException(status_code=404, detail=f"Unknown strategy: {req.strategy_id}")

    try:
        result = engine_fn(params=req.params, initial_capital=req.initial_capital)
        # Attach data source info so the frontend can inform the user
        from app.config import ACTIVE_DATA_SOURCE
        result["data_source"] = ACTIVE_DATA_SOURCE
        return result
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Data files not found. Set RAVI_DATA_DIR env var to point to the DataBento data directory. Error: {e}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Backtest failed: {e}")
