"""Unit tests for the order book merge rules (Binance Spot semantics)."""
import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "collector"))

from app.book import OrderBook  # noqa: E402


def make_book():
    book = OrderBook()
    book.load_snapshot(
        100,
        [["67000.00", "1.50000000"], ["66999.90", "2.00000000"]],
        [["67000.10", "0.50000000"], ["67000.20", "3.25000000"]],
    )
    return book


def test_snapshot_loads_exact_decimals():
    book = make_book()
    assert book.last_update_id == 100
    assert book.bids[Decimal("67000.00")] == Decimal("1.50000000")
    bids, asks = book.top(5)
    assert bids[0] == ["67000.00", "1.50000000"]
    assert asks[0] == ["67000.10", "0.50000000"]


def test_event_with_u_not_greater_than_current_is_ignored():
    book = make_book()
    assert book.apply_event(95, 100, [["1", "1"]], []) == "ignored"
    assert book.apply_event(90, 99, [["1", "1"]], []) == "ignored"
    assert book.last_update_id == 100
    assert Decimal("1") not in book.bids


def test_event_covering_next_id_is_applied():
    book = make_book()
    # U <= 101 <= u
    assert book.apply_event(101, 101, [["67000.00", "9.00000000"]], []) == "applied"
    assert book.bids[Decimal("67000.00")] == Decimal("9.00000000")
    assert book.last_update_id == 101


def test_overlapping_event_is_applied():
    book = make_book()
    # U < current + 1 <= u: partially overlapping, still connects.
    assert book.apply_event(98, 105, [], [["67000.10", "1.00000000"]]) == "applied"
    assert book.last_update_id == 105
    assert book.asks[Decimal("67000.10")] == Decimal("1.00000000")


def test_event_not_covering_next_id_is_a_gap():
    book = make_book()
    assert book.apply_event(102, 103, [], []) == "gap"
    # book untouched, caller must resync
    assert book.last_update_id == 100


def test_zero_quantity_removes_level():
    book = make_book()
    assert book.apply_event(101, 101, [["67000.00", "0.00000000"]], []) == "applied"
    assert Decimal("67000.00") not in book.bids


def test_quantities_are_absolute_not_deltas():
    book = make_book()
    book.apply_event(101, 101, [["67000.00", "2.00000000"]], [])
    book.apply_event(102, 102, [["67000.00", "3.00000000"]], [])
    assert book.bids[Decimal("67000.00")] == Decimal("3.00000000")


def test_top_five_ordering_and_exact_decimal_strings():
    book = OrderBook()
    book.load_snapshot(
        7,
        [[p, "1.00000000"] for p in ["0.00000120", "2.5", "10", "9.99999999", "3", "1"]],
        [[p, "2.00000000"] for p in ["11", "10.5", "12", "13", "14", "15"]],
    )
    bids, asks = book.top(5)
    assert [p for p, _ in bids] == ["10", "9.99999999", "3", "2.5", "1"]
    assert [p for p, _ in asks] == ["10.5", "11", "12", "13", "14"]
    # exact decimal rendering: no scientific notation, trailing zeros kept
    book.apply_event(8, 8, [["0.00000120", "0.00000001"]], [])
    bids, _ = book.top(6)
    assert ["0.00000120", "0.00000001"] in bids
