"""Mock Binance-Spot-like market data source.

Maintains a synthetic order book per symbol (at most 10 levels per side),
streams Binance-style incremental depth updates over WebSocket and serves
REST depth snapshots. Chaos hooks: snapshot delay, sequence gaps, dropped
connections and silent streams.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from decimal import Decimal
from operator import itemgetter
from typing import Any, Optional

log = logging.getLogger("mock-source")

MAX_LEVELS = 10
ZERO = "0.00000000"


def fmt(value: Decimal) -> str:
    return format(value, "f")


class MockMarket:
    """Synthetic market for one symbol."""

    def __init__(self, symbol: str, tick_ms: int = 100) -> None:
        self.symbol = symbol.upper()
        self.tick_ms = tick_ms
        self.update_id = 1000
        self.mid = Decimal("67000.00")
        self.tick_size = Decimal("0.10")
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        # chaos knobs
        self.snapshot_delay_ms = 0
        self.pause_until = 0.0  # monotonic deadline; stream stays silent until then
        # diagnostics
        self.connections: set[Any] = set()
        self.events_sent = 0
        self.gaps_injected = 0
        self.disconnects_injected = 0
        self.pauses_injected = 0
        self._task: Optional[asyncio.Task] = None
        self._rng = random.Random(f"mock-{self.symbol}")
        self._init_book()

    def _init_book(self) -> None:
        for i in range(1, MAX_LEVELS + 1):
            offset = self.tick_size * i
            self.bids[self.mid - offset] = self._qty()
            self.asks[self.mid + offset] = self._qty()

    def _qty(self) -> Decimal:
        # 0.00001 .. 5.00000 with 8 decimal places
        return Decimal(self._rng.randrange(1, 500000)) / Decimal(100000)

    # ------------------------------------------------------------------ engine
    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name=f"market-{self.symbol}")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self.tick_ms / 1000)
            if time.monotonic() < self.pause_until:
                continue  # silent stream ("断流"): connection stays open, no data
            event = self._next_event()
            self.events_sent += 1
            await self._broadcast(event)

    def _next_event(self) -> dict:
        self.update_id += 1
        return {
            "e": "depthUpdate",
            "E": int(time.time() * 1000),
            "s": self.symbol,
            "U": self.update_id,
            "u": self.update_id,
            "b": self._mutate(self.bids, is_bid=True),
            "a": self._mutate(self.asks, is_bid=False),
        }

    def _mutate(self, side: dict[Decimal, Decimal], is_bid: bool) -> list[list[str]]:
        changes: list[list[str]] = []
        for _ in range(self._rng.randint(1, 3)):
            roll = self._rng.random()
            if roll < 0.15 and len(side) > MAX_LEVELS - 3:
                # remove a level -> absolute quantity zero
                price = self._rng.choice(list(side.keys()))
                del side[price]
                changes.append([fmt(price), ZERO])
            elif roll < 0.45 and len(side) < MAX_LEVELS:
                # add a level inside the 10-level band around mid
                price = self._fresh_price(side, is_bid)
                if price is None:
                    continue
                qty = self._qty()
                side[price] = qty
                changes.append([fmt(price), fmt(qty)])
            else:
                # update a level -> absolute quantity
                price = self._rng.choice(list(side.keys()))
                qty = self._qty()
                side[price] = qty
                changes.append([fmt(price), fmt(qty)])
        return changes

    def _fresh_price(self, side: dict[Decimal, Decimal], is_bid: bool) -> Optional[Decimal]:
        for _ in range(20):
            i = self._rng.randint(1, MAX_LEVELS + 2)
            price = self.mid - self.tick_size * i if is_bid else self.mid + self.tick_size * i
            if price not in side:
                return price
        return None

    async def _broadcast(self, event: dict) -> None:
        if not self.connections:
            return
        payload = json.dumps(event)
        stale = []
        for ws in list(self.connections):
            try:
                await ws.send_text(payload)
            except Exception:
                stale.append(ws)
        for ws in stale:
            self.connections.discard(ws)

    # ---------------------------------------------------------------- snapshot
    async def snapshot(self, limit: int) -> dict:
        """REST depth snapshot; honours the configured snapshot delay."""
        if self.snapshot_delay_ms:
            await asyncio.sleep(self.snapshot_delay_ms / 1000)
        limit = max(1, min(limit, MAX_LEVELS))
        bids = sorted(self.bids.items(), key=itemgetter(0), reverse=True)[:limit]
        asks = sorted(self.asks.items(), key=itemgetter(0))[:limit]
        return {
            "lastUpdateId": self.update_id,
            "bids": [[fmt(p), fmt(q)] for p, q in bids],
            "asks": [[fmt(p), fmt(q)] for p, q in asks],
        }

    # ------------------------------------------------------------------- chaos
    def inject_gap(self, updates: int) -> None:
        """Skip ``updates`` sequence ids so the next event no longer connects."""
        self.update_id += max(1, updates)
        self.gaps_injected += 1

    async def inject_disconnect(self) -> int:
        """Close every current WebSocket connection."""
        conns = list(self.connections)
        for ws in conns:
            try:
                await ws.close(code=1011, reason="injected disconnect")
            except Exception:
                pass
        self.disconnects_injected += 1
        return len(conns)

    def inject_pause(self, seconds: float) -> None:
        """Stop broadcasting for ``seconds`` without closing connections."""
        self.pause_until = time.monotonic() + seconds
        self.pauses_injected += 1
