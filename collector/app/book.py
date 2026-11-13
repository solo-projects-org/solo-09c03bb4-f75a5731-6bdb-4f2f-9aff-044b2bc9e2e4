"""Order book with Binance Spot snapshot + incremental-update semantics.

All prices and quantities are kept as ``Decimal`` so the exact decimal
representation sent by the source is preserved end to end.
"""
from __future__ import annotations

from decimal import Decimal
from operator import itemgetter
from typing import Iterable


def _dec(value) -> Decimal:
    return Decimal(str(value))


class OrderBook:
    """Mutable order book for one symbol.

    The methods are plain synchronous code; the collector calls them while
    holding its lock, so a query always observes one consistent state.
    """

    def __init__(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.last_update_id: int = 0

    # ---------------------------------------------------------------- snapshot
    def load_snapshot(
        self,
        last_update_id: int,
        bids: Iterable[Iterable[str]],
        asks: Iterable[Iterable[str]],
    ) -> None:
        self.bids = {_dec(p): _dec(q) for p, q in bids}
        self.asks = {_dec(p): _dec(q) for p, q in asks}
        self.last_update_id = int(last_update_id)

    # ------------------------------------------------------------- increments
    def apply_event(self, U: int, u: int, bids, asks) -> str:
        """Apply one incremental depth update.

        Returns one of:

        * ``"ignored"``  – ``u`` is not greater than the current id, the
          event carries nothing new;
        * ``"applied"``  – the event covers ``current + 1``
          (``U <= current + 1 <= u``) and was merged;
        * ``"gap"``      – the event does not cover ``current + 1``; the
          book is unusable and must be resynchronised.
        """
        U = int(U)
        u = int(u)
        if u <= self.last_update_id:
            return "ignored"
        if not (U <= self.last_update_id + 1 <= u):
            return "gap"
        for price, qty in bids:
            self._set_level(self.bids, price, qty)
        for price, qty in asks:
            self._set_level(self.asks, price, qty)
        self.last_update_id = u
        return "applied"

    @staticmethod
    def _set_level(side: dict[Decimal, Decimal], price, qty) -> None:
        price = _dec(price)
        qty = _dec(qty)
        if qty == 0:
            # Zero quantity removes the price level.
            side.pop(price, None)
        else:
            # Quantities are absolute, not deltas.
            side[price] = qty

    # ------------------------------------------------------------------- read
    def top(self, depth: int = 5) -> tuple[list[list[str]], list[list[str]]]:
        """Best ``depth`` bids (desc) and asks (asc) as exact decimal strings."""
        bids = sorted(self.bids.items(), key=itemgetter(0), reverse=True)[:depth]
        asks = sorted(self.asks.items(), key=itemgetter(0))[:depth]
        return (
            [[format(p, "f"), format(q, "f")] for p, q in bids],
            [[format(p, "f"), format(q, "f")] for p, q in asks],
        )
