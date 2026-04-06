"""
Ravi VAM Strategy Platform — FastAPI Backend
Serves the frontend at / and the API at /api/*
"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.routers import strategies, backtest
from app.config import ensure_data_available

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FRONTEND_DIR = PROJECT_ROOT / "frontend"

app = FastAPI(
    title="Ravi VAM Strategy Platform",
    description="Config-driven trading strategy research platform",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routers
app.include_router(strategies.router, prefix="/api", tags=["strategies"])
app.include_router(backtest.router, prefix="/api", tags=["backtest"])

@app.on_event("startup")
async def startup_resolve_data_source():
    """Resolve data source in background so server starts immediately."""
    import asyncio
    import logging
    logger = logging.getLogger("app.startup")

    async def _resolve():
        try:
            # Run blocking yfinance downloads in thread pool so they don't block the event loop
            await asyncio.get_event_loop().run_in_executor(None, ensure_data_available)
            from app.config import ACTIVE_DATA_SOURCE
            logger.info("Data source resolved: %s", ACTIVE_DATA_SOURCE)
        except Exception as e:
            logger.warning("Could not resolve data source at startup: %s", e)

    # Fire and forget — server starts accepting requests immediately
    asyncio.create_task(_resolve())


# Serve frontend at root (must be last — catches all unmatched routes)
if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
