"""FastAPI surface for the order book collector.

Endpoints (interactive docs at /docs):

* ``POST /collection/start`` – start collecting one symbol
* ``POST /collection/stop``  – stop collecting
* ``GET  /status``           – idle / syncing / live plus diagnostics
* ``GET  /orderbook``        – top-5 bids/asks + last update id (only while live)
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .collector import Collector
from .config import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

settings = Settings()
collector = Collector(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log = logging.getLogger("collector")
    log.info("collector up, source=%s %s", settings.source_http_base, settings.source_ws_base)
    yield
    await collector.stop()


app = FastAPI(
    title="Order Book Collector",
    description=(
        "Collects one trading pair from a Binance-Spot-compatible source "
        "(HTTP depth snapshot + WebSocket incremental updates) and serves the "
        "top of book. The book is only served while the stream is synced; "
        "during reconnects, gaps or snapshot delays the state is `syncing` "
        "and no stale book is exposed."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


class StartRequest(BaseModel):
    symbol: str = Field(..., examples=["BTCUSDT"], description="Trading pair to collect")


@app.post(
    "/collection/start",
    summary="Start (or restart) collection for a single symbol",
)
async def start_collection(req: StartRequest):
    await collector.start(req.symbol)
    return await collector.status()


@app.post("/collection/stop", summary="Stop collection")
async def stop_collection():
    await collector.stop()
    return await collector.status()


@app.get("/status", summary="Collector state: idle / syncing / live")
async def get_status():
    return await collector.status()


@app.get(
    "/orderbook",
    summary="Top-5 bids/asks and last update id",
    responses={
        200: {"description": "Consistent snapshot of the top of book"},
        409: {"description": "Not live (idle or syncing); no book is served"},
    },
)
async def get_orderbook():
    book = await collector.orderbook()
    if book is None:
        status = await collector.status()
        raise HTTPException(
            status_code=409,
            detail={
                "message": "order book is not served while the collector is not live",
                "state": status["state"],
            },
        )
    return book


@app.get("/", include_in_schema=False)
async def root():
    return {"service": "order-book-collector", "docs": "/docs"}
