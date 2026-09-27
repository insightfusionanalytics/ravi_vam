"""
A small, local-only API for starting/checking/cancelling optimization
runs. This is the "backtest runner which is able to start/cancel
backtests by API" Ravi asked for -- but per the chosen design (2026-09-24:
"heavy runs happen locally, only finished results get published"), it is
a SEPARATE app from app/main.py, run on your own machine, never deployed
to the production VPS and never reachable from the live domain. Ravi
views finished results (Step 8/9's report and dashboard integration); he
never gets a "start" button.

Run it yourself with:
  .venv/bin/uvicorn app.optimize.local_api:app --port 8010 --reload

Then e.g.:
  curl -X POST 'http://127.0.0.1:8010/studies/step2_upro_tqqq_6state/confirmed_locked/start?n_trials=200'
  curl 'http://127.0.0.1:8010/studies/step2_upro_tqqq_6state/confirmed_locked'
  curl -X POST 'http://127.0.0.1:8010/studies/step2_upro_tqqq_6state/confirmed_locked/cancel'
"""

from dataclasses import asdict

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.optimize.jobs import cancel_job, get_status, start_job
from app.optimize.storage import VALID_MODES

SUPPORTED_STRATEGIES = (
    "step1_upro_4state",
    "step2_upro_tqqq_6state",
    "step3_spxu_predatory_short",
    "step4_svix_safety_valve",
)

app = FastAPI(title="ravi_vam optimizer (local only)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost", "http://127.0.0.1", "http://localhost:8000", "http://127.0.0.1:8000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _validate(strategy_id: str, mode: str):
    if strategy_id not in SUPPORTED_STRATEGIES:
        raise HTTPException(status_code=404, detail=f"unsupported strategy: {strategy_id}. Supported: {SUPPORTED_STRATEGIES}")
    if mode not in VALID_MODES:
        raise HTTPException(status_code=400, detail=f"mode must be one of {VALID_MODES}")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/studies")
def list_studies():
    return [
        asdict(get_status(strategy_id, mode))
        for strategy_id in SUPPORTED_STRATEGIES
        for mode in VALID_MODES
    ]


@app.get("/studies/{strategy_id}/{mode}")
def study_status(strategy_id: str, mode: str):
    _validate(strategy_id, mode)
    return asdict(get_status(strategy_id, mode))


@app.post("/studies/{strategy_id}/{mode}/start")
def study_start(strategy_id: str, mode: str, n_trials: int = 200):
    _validate(strategy_id, mode)
    if n_trials < 1 or n_trials > 5000:
        raise HTTPException(status_code=400, detail="n_trials must be between 1 and 5000")
    try:
        status = start_job(strategy_id, mode, n_trials)
    except RuntimeError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return asdict(status)


@app.post("/studies/{strategy_id}/{mode}/cancel")
def study_cancel(strategy_id: str, mode: str):
    _validate(strategy_id, mode)
    try:
        status = cancel_job(strategy_id, mode)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return asdict(status)
