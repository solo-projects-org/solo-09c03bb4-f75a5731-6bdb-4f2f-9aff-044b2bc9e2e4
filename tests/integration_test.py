"""End-to-end test against a running collector + mock source.

Usage:
    python3 tests/integration_test.py [collector_base] [source_base]

Defaults: collector http://localhost:8000, source http://localhost:9000
"""
from __future__ import annotations

import sys
import time

import httpx

COLLECTOR = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
SOURCE = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:9000"
SYMBOL = "BTCUSDT"

client = httpx.Client(timeout=10.0)


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ok: {msg}")


def wait_for(predicate, timeout: float, interval: float = 0.1, desc: str = ""):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError(f"timeout waiting for: {desc}")


def status() -> dict:
    return client.get(f"{COLLECTOR}/status").json()


def state() -> str:
    return status()["state"]


def wait_live(timeout: float = 20.0) -> dict:
    return wait_for(lambda: state() == "live" and status(), timeout, desc="state == live")


def wait_syncing(timeout: float = 10.0) -> dict:
    return wait_for(lambda: state() == "syncing" and status(), timeout, desc="state == syncing")


def assert_book_shape(book: dict) -> None:
    check(book["symbol"] == SYMBOL, "book symbol matches")
    check(book["state"] == "live", "book served only while live")
    check(isinstance(book["lastUpdateId"], int) and book["lastUpdateId"] > 0, "lastUpdateId present")
    for side_name in ("bids", "asks"):
        side = book[side_name]
        check(len(side) == 5, f"{side_name} has exactly 5 levels")
        for price, qty in side:
            check(isinstance(price, str) and isinstance(qty, str), "price/qty are decimal strings")
            float(price), float(qty)  # parseable
    bid_prices = [float(p) for p, _ in book["bids"]]
    ask_prices = [float(p) for p, _ in book["asks"]]
    check(bid_prices == sorted(bid_prices, reverse=True), "bids sorted desc")
    check(ask_prices == sorted(ask_prices), "asks sorted asc")
    check(bid_prices[-1] < ask_prices[0], "best bid below best ask")


def assert_book_unavailable() -> None:
    resp = client.get(f"{COLLECTOR}/orderbook")
    check(resp.status_code == 409, "orderbook -> 409 while not live")
    check(resp.json()["detail"]["state"] in ("syncing", "idle"), "detail reports the state")


def main() -> None:
    print("== 0. reset: stop collection, clear snapshot delay ==")
    client.post(f"{COLLECTOR}/collection/stop")
    client.post(f"{SOURCE}/control/snapshot-delay", json={"ms": 0})

    print("== 1. start collection, reaches live ==")
    resp = client.post(f"{COLLECTOR}/collection/start", json={"symbol": SYMBOL})
    assert resp.status_code == 200, resp.text
    check(resp.json()["state"] == "syncing", "state is syncing right after start")
    assert_book_unavailable()
    st = wait_live()
    check(st["ws_connected"] is True, "websocket connected")
    check(st["counters"]["events_applied"] > 0, "events applied")
    book = client.get(f"{COLLECTOR}/orderbook").json()
    assert_book_shape(book)
    first_id = book["lastUpdateId"]
    time.sleep(0.5)
    book2 = client.get(f"{COLLECTOR}/orderbook").json()
    check(book2["lastUpdateId"] > first_id, "lastUpdateId advances with the stream")

    print("== 2. sequence gap -> syncing -> resynced ==")
    gaps_before = status()["counters"]["gaps"]
    client.post(f"{SOURCE}/control/gap", json={"updates": 5})
    st = wait_syncing()
    assert_book_unavailable()
    st = wait_live()
    check(st["counters"]["gaps"] == gaps_before + 1, "gap was detected and counted")
    assert_book_shape(client.get(f"{COLLECTOR}/orderbook").json())

    print("== 3. disconnect -> syncing -> resynced ==")
    rec_before = status()["counters"]["reconnects"]
    client.post(f"{SOURCE}/control/disconnect")
    wait_syncing()
    assert_book_unavailable()
    st = wait_live()
    check(st["counters"]["reconnects"] > rec_before, "reconnect counted")
    assert_book_shape(client.get(f"{COLLECTOR}/orderbook").json())

    print("== 4. silent stream (pause) -> stale timeout -> syncing -> live ==")
    stale_before = status()["counters"]["stale_timeouts"]
    client.post(f"{SOURCE}/control/pause", json={"seconds": 6})
    st = wait_syncing(timeout=10)
    check(st["counters"]["stale_timeouts"] == stale_before + 1, "stale stream detected")
    assert_book_unavailable()
    wait_live(timeout=20)
    assert_book_shape(client.get(f"{COLLECTOR}/orderbook").json())

    print("== 5. snapshot delay + disconnect: late snapshot must not revive the book ==")
    client.post(f"{SOURCE}/control/snapshot-delay", json={"ms": 4000})
    client.post(f"{COLLECTOR}/collection/start", json={"symbol": SYMBOL})
    time.sleep(1.0)  # snapshot fetch for gen N is in flight (delayed 4s)
    check(state() == "syncing", "syncing while snapshot is delayed")
    assert_book_unavailable()
    stale_before = status()["counters"]["stale_snapshots"]
    client.post(f"{SOURCE}/control/disconnect")  # kill gen N mid-fetch
    time.sleep(4.5)  # gen N's late snapshot arrives now; gen N+1 fetch still pending
    check(state() == "syncing", "still syncing: late snapshot did not restore querying")
    assert_book_unavailable()
    st = wait_live(timeout=20)
    check(
        st["counters"]["stale_snapshots"] >= stale_before + 1,
        "late snapshot from the old connection was discarded",
    )
    assert_book_shape(client.get(f"{COLLECTOR}/orderbook").json())
    client.post(f"{SOURCE}/control/snapshot-delay", json={"ms": 0})

    print("== 6. stop -> idle, book unavailable ==")
    client.post(f"{COLLECTOR}/collection/stop")
    check(state() == "idle", "state is idle after stop")
    assert_book_unavailable()

    print("\nALL INTEGRATION CHECKS PASSED")


if __name__ == "__main__":
    main()
