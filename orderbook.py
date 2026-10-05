"""In-memory, full-depth CLOB books updated only by websocket events."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any


@dataclass(slots=True)
class OrderBook:
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    ready: bool = False
    updated_at: datetime | None = None

    @staticmethod
    def _levels(levels: Any) -> dict[Decimal, Decimal]:
        result: dict[Decimal, Decimal] = {}
        for level in levels or ():
            price = Decimal(str(level.price))
            size = Decimal(str(level.size))
            if size > 0:
                result[price] = size
        return result

    def apply(self, event: Any, token_ids: set[str]) -> set[str]:
        """Apply a full book or incremental change; return changed token IDs."""
        event_type = getattr(event, "type", None)
        event_type = str(getattr(event_type, "value", event_type)).lower()
        payload = getattr(event, "payload", None)
        if payload is None:
            return set()

        changed: set[str] = set()
        now = datetime.now(UTC)
        if event_type == "book":
            token_id = str(payload.token_id)
            if token_id not in token_ids:
                return changed
            self.bids = self._levels(payload.bids)
            self.asks = self._levels(payload.asks)
            self.ready = True
            self.updated_at = now
            return {token_id}

        if event_type != "price_change":
            return changed

        for item in payload.price_changes:
            token_id = str(item.token_id)
            if token_id not in token_ids:
                continue
            self.apply_change(item, now)
            changed.add(token_id)
        return changed

    def apply_change(self, item: Any, at: datetime | None = None) -> None:
        side = getattr(item.side, "value", item.side)
        levels = self.bids if str(side).upper().endswith("BUY") else self.asks
        price = Decimal(str(item.price))
        size = Decimal(str(item.size))
        if size <= 0:
            levels.pop(price, None)
        else:
            levels[price] = size
        self.updated_at = at or datetime.now(UTC)

    def best(self, *, ask: bool) -> tuple[Decimal, Decimal] | None:
        levels = self.asks if ask else self.bids
        if not levels:
            return None
        price = min(levels) if ask else max(levels)
        return price, levels[price]

    def depth(self, *, ask: bool) -> tuple[tuple[Decimal, Decimal], ...]:
        levels = self.asks if ask else self.bids
        ordered = sorted(levels.items(), reverse=not ask)
        return tuple((price, size) for price, size in ordered if size > 0)
