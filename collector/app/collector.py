"""Single-symbol order book collector.

State machine
-------------

``idle``
    Not collecting; the order book API is unavailable.
``syncing``
    (Re)connecting the WebSocket, fetching the REST snapshot and buffering
    incremental events. The order book API is unavailable and the previous
    book is never served.
``live``
    The snapshot has been joined with the event stream (the first event
    covering ``snapshot.lastUpdateId + 1`` has been applied). The order book
    API serves the top of book.

Any disconnect, sequence gap or silent stream moves the collector back to
``syncing`` immediately and the book is discarded. A snapshot that belongs
to a superseded connection generation can never revive the book.

Synchronisation procedure (Binance Spot semantics)
--------------------------------------------------

1. Open the WebSocket and buffer every incoming depth event.
2. Request the HTTP depth snapshot (``lastUpdateId``) in parallel; events
   keep being buffered while the snapshot is in flight.
3. When the snapshot arrives, load it, then replay the buffer:
   drop events with ``u <= lastUpdateId``; the first event with
   ``U <= lastUpdateId + 1 <= u`` is applied; afterwards every event must
   keep covering ``current + 1``.
4. Only after that first covering event is applied does the collector
   become ``live``.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Optional

import httpx

try:  # websockets >= 14
    from websockets.asyncio.client import connect as ws_connect
except ImportError:  # pragma: no cover - older websockets
    from websockets import connect as ws_connect  # type: ignore

from .book import OrderBook
from .config import Settings

log = logging.getLogger("collector")


class SequenceGapError(RuntimeError):
    """An event did not cover ``current + 1``; the book must be resynced."""


class StaleStreamError(RuntimeError):
    """No data arrived within the stale timeout; treat the stream as dead."""


class StaleSnapshotError(RuntimeError):
    """A snapshot from an old connection generation was delivered."""


class Collector:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = asyncio.Lock()
        self._state = "idle"
        self._symbol: Optional[str] = None
        self._book = OrderBook()
        self._ws_connected = False
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        # Monotonic connection generation; guards against late snapshots
        # from previous connections.
        self._generation = 0
        self._snapshot_tasks: set[asyncio.Task] = set()
        self._last_error: Optional[str] = None
        self._counters = {
            "events_applied": 0,
            "events_ignored": 0,
            "gaps": 0,
            "reconnects": 0,
            "stale_timeouts": 0,
            "stale_snapshots": 0,
            "snapshot_fetches": 0,
        }

    # ------------------------------------------------------------------- API
    async def start(self, symbol: str) -> None:
        """Start (or restart) collection for exactly one symbol."""
        await self.stop()
        self._symbol = symbol.upper()
        self._stop = asyncio.Event()
        self._last_error = None
        async with self._lock:
            self._state = "syncing"
            self._book = OrderBook()
        self._task = asyncio.create_task(
            self._run(), name=f"collector-{self._symbol}"
        )

    async def stop(self) -> None:
        self._stop.set()
        for task in list(self._snapshot_tasks):
            task.cancel()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        async with self._lock:
            self._state = "idle"
            self._book = OrderBook()
            self._ws_connected = False

    async def status(self) -> dict[str, Any]:
        async with self._lock:
            return {
                "symbol": self._symbol,
                "state": self._state,
                "serving": self._state == "live",
                "ws_connected": self._ws_connected,
                "last_update_id": self._book.last_update_id,
                "bid_levels": len(self._book.bids),
                "ask_levels": len(self._book.asks),
                "counters": dict(self._counters),
                "last_error": self._last_error,
            }

    async def orderbook(self) -> Optional[dict[str, Any]]:
        """Top of book, or None while not live.

        State, last update id and levels are read under a single lock
        acquisition, so one query always observes one consistent state.
        """
        async with self._lock:
            if self._state != "live":
                return None
            bids, asks = self._book.top(self.settings.serve_depth)
            return {
                "symbol": self._symbol,
                "state": self._state,
                "lastUpdateId": self._book.last_update_id,
                "bids": bids,
                "asks": asks,
            }

    # --------------------------------------------------------------- run loop
    async def _run(self) -> None:
        assert self._symbol is not None
        while not self._stop.is_set():
            self._generation += 1
            gen = self._generation
            await self._mark_syncing()
            try:
                await self._stream(gen)
            except asyncio.CancelledError:
                raise
            except SequenceGapError as exc:
                self._counters["gaps"] += 1
                self._last_error = f"sequence gap: {exc}"
                log.warning("gen %d: %s", gen, self._last_error)
            except StaleSnapshotError as exc:
                self._last_error = str(exc)
                log.warning("gen %d: %s", gen, self._last_error)
            except Exception as exc:  # connection drop, http error, stale stream...
                if isinstance(exc, StaleStreamError):
                    self._counters["stale_timeouts"] += 1
                self._last_error = f"{type(exc).__name__}: {exc}"
                log.warning("gen %d: stream ended: %s", gen, self._last_error)
            # The old book must stop being served the moment we lose sync.
            await self._mark_syncing()
            if not self._stop.is_set():
                self._counters["reconnects"] += 1
                try:
                    await asyncio.wait_for(
                        self._stop.wait(),
                        timeout=self.settings.reconnect_delay_seconds,
                    )
                except asyncio.TimeoutError:
                    pass

    async def _mark_syncing(self) -> None:
        async with self._lock:
            self._state = "syncing"
            self._book = OrderBook()  # never serve a stale book
            self._ws_connected = False

    async def _mark_live(self) -> None:
        async with self._lock:
            if self._state == "syncing":
                self._state = "live"
                log.info("gen %d: live at update id %d", self._generation, self._book.last_update_id)

    # -------------------------------------------------------------- streaming
    async def _stream(self, gen: int) -> None:
        symbol = self._symbol
        url = f"{self.settings.source_ws_base}/ws/{symbol.lower()}@depth"
        log.info("gen %d: connecting %s", gen, url)
        async with ws_connect(url, max_queue=10000) as ws:
            async with self._lock:
                self._ws_connected = True
            snapshot_ready = self._launch_snapshot_fetch(gen)
            recv_task = asyncio.create_task(ws.recv())
            buffered: list[dict] = []
            have_snapshot = False
            try:
                while not self._stop.is_set():
                    wait_for = (
                        {recv_task} if have_snapshot else {recv_task, snapshot_ready}
                    )
                    done, _ = await asyncio.wait(
                        wait_for,
                        timeout=self.settings.stale_timeout_seconds,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if not done:
                        raise StaleStreamError(
                            f"no depth data for "
                            f"{self.settings.stale_timeout_seconds}s"
                        )
                    if not have_snapshot and snapshot_ready in done:
                        snapshot = snapshot_ready.result()  # may raise (HTTP error)
                        if snapshot is None:
                            # Late snapshot from a previous connection: it must
                            # not restore querying. Abandon this connection.
                            raise StaleSnapshotError(
                                "snapshot from an old connection arrived; discarded"
                            )
                        have_snapshot = True
                        live = await self._load_snapshot(snapshot, buffered)
                        buffered.clear()
                        if live:
                            await self._mark_live()
                    if recv_task in done:
                        raw = recv_task.result()  # raises ConnectionClosed on drop
                        recv_task = asyncio.create_task(ws.recv())
                        event = self._parse_event(raw)
                        if event is None:
                            continue
                        if not have_snapshot:
                            buffered.append(event)
                        else:
                            result = await self._apply(event)
                            if result == "gap":
                                raise SequenceGapError(
                                    f"U={event['U']} u={event['u']} "
                                    f"last={self._book.last_update_id}"
                                )
                            if result == "applied":
                                await self._mark_live()
            finally:
                recv_task.cancel()
                # The snapshot fetch is deliberately NOT cancelled here: if it
                # completes after this connection is gone, the generation
                # check in _launch_snapshot_fetch discards it.
                if snapshot_ready.done():
                    if not snapshot_ready.cancelled():
                        snapshot_ready.exception()  # mark retrieved
                else:
                    snapshot_ready.add_done_callback(
                        lambda f: None if f.cancelled() else f.exception()
                    )
                async with self._lock:
                    self._ws_connected = False

    def _parse_event(self, raw) -> Optional[dict]:
        try:
            data = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return None
        if not isinstance(data, dict) or data.get("e") != "depthUpdate":
            return None
        if data.get("s") not in (None, self._symbol):
            return None
        return data

    # --------------------------------------------------------------- snapshot
    def _launch_snapshot_fetch(self, gen: int) -> "asyncio.Future[Optional[dict]]":
        """Fetch the REST snapshot in the background.

        The returned future resolves to the snapshot dict, or to ``None``
        when the fetch belongs to a superseded connection generation — a
        late snapshot from an old connection is discarded and never touches
        the book.
        """
        loop = asyncio.get_running_loop()
        ready: asyncio.Future = loop.create_future()
        task = asyncio.create_task(self._fetch_snapshot(), name=f"snapshot-gen{gen}")
        self._snapshot_tasks.add(task)

        def _done(done_task: asyncio.Task) -> None:
            self._snapshot_tasks.discard(done_task)
            if done_task.cancelled():
                if not ready.done():
                    ready.cancel()
                return
            exc = done_task.exception()
            if gen != self._generation:
                self._counters["stale_snapshots"] += 1
                log.warning(
                    "discarding late snapshot from generation %d (current %d)",
                    gen,
                    self._generation,
                )
                if not ready.done():
                    ready.set_result(None)
                return
            if exc is not None:
                if not ready.done():
                    ready.set_exception(exc)
            elif not ready.done():
                ready.set_result(done_task.result())

        task.add_done_callback(_done)
        return ready

    async def _fetch_snapshot(self) -> dict:
        self._counters["snapshot_fetches"] += 1
        async with httpx.AsyncClient(
            timeout=self.settings.snapshot_http_timeout_seconds
        ) as client:
            resp = await client.get(
                f"{self.settings.source_http_base}/depth",
                params={"symbol": self._symbol, "limit": self.settings.book_limit},
            )
            resp.raise_for_status()
            data = resp.json()
        return {
            "lastUpdateId": int(data["lastUpdateId"]),
            "bids": data["bids"],
            "asks": data["asks"],
        }

    async def _load_snapshot(self, snapshot: dict, buffered: list[dict]) -> bool:
        """Load the snapshot and replay buffered events.

        Returns True once an event covering ``lastUpdateId + 1`` has been
        applied, i.e. the collector may go live.
        """
        async with self._lock:
            self._book.load_snapshot(
                snapshot["lastUpdateId"], snapshot["bids"], snapshot["asks"]
            )
            live = False
            for event in buffered:
                result = self._book.apply_event(
                    event["U"], event["u"], event["b"], event["a"]
                )
                if result == "gap":
                    raise SequenceGapError(
                        f"buffered event U={event['U']} u={event['u']} does "
                        f"not cover {self._book.last_update_id + 1}"
                    )
                if result == "applied":
                    self._counters["events_applied"] += 1
                    live = True
                else:
                    self._counters["events_ignored"] += 1
            return live

    async def _apply(self, event: dict) -> str:
        async with self._lock:
            result = self._book.apply_event(
                event["U"], event["u"], event["b"], event["a"]
            )
            if result == "applied":
                self._counters["events_applied"] += 1
            elif result == "ignored":
                self._counters["events_ignored"] += 1
            return result
