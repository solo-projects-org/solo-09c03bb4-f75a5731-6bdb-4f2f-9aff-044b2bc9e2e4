"""Unit tests for the collector's late-snapshot generation guard."""
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collector"))

from app.collector import Collector  # noqa: E402
from app.config import Settings  # noqa: E402


def _settings() -> Settings:
    return Settings(
        source_http_base="http://127.0.0.1:1",
        source_ws_base="ws://127.0.0.1:1",
    )


def test_late_snapshot_from_old_generation_is_discarded():
    async def scenario():
        collector = Collector(_settings())
        snapshot = {"lastUpdateId": 42, "bids": [["1.0", "1.0"]], "asks": []}

        async def fake_fetch():
            await asyncio.sleep(0.01)
            return snapshot

        collector._fetch_snapshot = fake_fetch  # type: ignore[assignment]

        # Simulate: connection gen 1 started a fetch, then the connection
        # died and the collector moved on to gen 2 before the fetch finished.
        collector._generation = 2
        stale_future = collector._launch_snapshot_fetch(gen=1)
        assert await stale_future is None  # discarded, never delivered
        assert collector._counters["stale_snapshots"] == 1

        # A snapshot for the current generation is accepted.
        fresh_future = collector._launch_snapshot_fetch(gen=2)
        assert await fresh_future == snapshot
        assert collector._counters["stale_snapshots"] == 1

    asyncio.run(scenario())


def test_snapshot_fetch_failure_propagates():
    async def scenario():
        collector = Collector(_settings())

        async def failing_fetch():
            raise RuntimeError("boom")

        collector._fetch_snapshot = failing_fetch  # type: ignore[assignment]
        collector._generation = 1
        future = collector._launch_snapshot_fetch(gen=1)
        try:
            await future
        except RuntimeError as exc:
            assert "boom" in str(exc)
        else:  # pragma: no cover
            raise AssertionError("expected the fetch error to propagate")

    asyncio.run(scenario())
