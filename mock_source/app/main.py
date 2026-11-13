"""Mock Binance Spot source: REST snapshots + WebSocket depth stream + chaos controls."""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from .market import MAX_LEVELS, MockMarket

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

markets: dict[str, MockMarket] = {}


def get_market(symbol: str) -> MockMarket:
    symbol = symbol.upper()
    if symbol not in markets:
        market = MockMarket(symbol)
        markets[symbol] = market
        market.start()
    return markets[symbol]


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_market("BTCUSDT")  # pre-warm the default pair
    yield
    for market in markets.values():
        await market.stop()


app = FastAPI(
    title="Mock Binance Spot Source",
    description=(
        "Binance-Spot-compatible depth source for local testing. "
        "Streams incremental updates at `ws://<host>/ws/<symbol>@depth`, serves "
        "snapshots at `GET /depth`, and exposes `/control/*` endpoints to inject "
        "snapshot delays, sequence gaps, disconnects and silent streams."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ------------------------------------------------------------- market data
@app.get("/depth", summary="Depth snapshot (Binance format, max 10 levels per side)")
async def depth(symbol: str = "BTCUSDT", limit: int = MAX_LEVELS):
    return await get_market(symbol).snapshot(limit)


@app.websocket("/ws/{stream}")
async def ws_endpoint(websocket: WebSocket, stream: str):
    # stream format: <symbol>@depth, e.g. btcusdt@depth
    symbol = stream.split("@")[0]
    market = get_market(symbol)
    await websocket.accept()
    market.connections.add(websocket)
    try:
        while True:
            await websocket.receive_text()  # clients send nothing; detects close
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        market.connections.discard(websocket)


# ------------------------------------------------------------------ chaos
class SnapshotDelayRequest(BaseModel):
    ms: int = Field(..., ge=0, description="Delay before /depth responds")


@app.post("/control/snapshot-delay", summary="Delay every subsequent /depth response")
async def set_snapshot_delay(req: SnapshotDelayRequest, symbol: str = "BTCUSDT"):
    get_market(symbol).snapshot_delay_ms = req.ms
    return {"symbol": symbol.upper(), "snapshot_delay_ms": req.ms}


@app.post("/control/disconnect", summary="Close all current WebSocket connections")
async def disconnect(symbol: str = "BTCUSDT"):
    closed = await get_market(symbol).inject_disconnect()
    return {"symbol": symbol.upper(), "closed_connections": closed}


class GapRequest(BaseModel):
    updates: int = Field(5, ge=1, description="How many sequence ids to skip")


@app.post("/control/gap", summary="Skip sequence ids so the next update cannot connect")
async def gap(req: GapRequest, symbol: str = "BTCUSDT"):
    get_market(symbol).inject_gap(req.updates)
    return {"symbol": symbol.upper(), "skipped_updates": req.updates}


class PauseRequest(BaseModel):
    seconds: float = Field(..., gt=0, description="Silence the stream without closing it")


@app.post("/control/pause", summary="Stop streaming for N seconds (connection stays open)")
async def pause(req: PauseRequest, symbol: str = "BTCUSDT"):
    get_market(symbol).inject_pause(req.seconds)
    return {"symbol": symbol.upper(), "paused_seconds": req.seconds}


# ------------------------------------------------------------------ introspection
@app.get("/market/state", summary="Internal source state (for debugging)")
async def market_state(symbol: str = "BTCUSDT"):
    m = get_market(symbol)
    return {
        "symbol": m.symbol,
        "update_id": m.update_id,
        "bid_levels": len(m.bids),
        "ask_levels": len(m.asks),
        "connections": len(m.connections),
        "snapshot_delay_ms": m.snapshot_delay_ms,
        "paused": time.monotonic() < m.pause_until,
        "events_sent": m.events_sent,
        "gaps_injected": m.gaps_injected,
        "disconnects_injected": m.disconnects_injected,
        "pauses_injected": m.pauses_injected,
    }


@app.get("/", include_in_schema=False)
async def root():
    return {"service": "mock-binance-spot-source", "docs": "/docs"}
