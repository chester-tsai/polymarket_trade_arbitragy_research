"""Resolve the rolling BTC 5m/15m market pairs through the public SDK."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from market_models import MarketPair, MarketWindow, as_decimal, as_utc


SERIES_SLUGS = {"5m": "btc-up-or-down-5m", "15m": "btc-up-or-down-15m"}
LOOKBACK = timedelta(minutes=20)
PAIR_LOOKAHEAD = timedelta(minutes=60)
DISCOVERY_PAGE_SIZE = 40
MAX_PAIRS_PER_SUBSCRIPTION = 4
DISCOVERY_RETRY_SECONDS = 5


def _metadata_strike(event: Any) -> Decimal | None:
    metadata = getattr(event, "metadata", None)
    if not isinstance(metadata, dict):
        return None
    return as_decimal(metadata.get("priceToBeat") or metadata.get("price_to_beat"))


def event_window(event: Any, duration: str) -> MarketWindow | None:
    if not event.markets:
        return None
    market = event.markets[0]
    end_at = as_utc(market.state.end_date)
    if (
        end_at is None
        or market.outcomes.yes.token_id is None
        or market.outcomes.no.token_id is None
    ):
        return None

    schedule = getattr(event, "schedule", None)
    start_at = as_utc(schedule.start_time) if schedule is not None else None
    if start_at is None:
        minutes = 5 if duration == "5m" else 15
        start_at = end_at - timedelta(minutes=minutes)

    trading = getattr(market, "trading", None)
    min_order_size = as_decimal(getattr(trading, "minimum_order_size", None))
    if min_order_size is None or min_order_size <= 0:
        min_order_size = Decimal(5)
    return MarketWindow(
        duration=duration,
        slug=market.slug or event.slug or "unknown",
        question=market.question or event.title or "BTC Up or Down",
        start_at=start_at,
        end_at=end_at,
        up_token=str(market.outcomes.yes.token_id),
        down_token=str(market.outcomes.no.token_id),
        min_order_size=min_order_size,
        gamma_strike=_metadata_strike(event),
    )


class MarketDiscovery:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.series_ids: dict[str, str] = {}

    async def initialize(self) -> None:
        async def resolve(duration: str, slug: str) -> tuple[str, str]:
            series = [item async for item in self.client.list_series(slug=slug).iter_items()]
            if not series:
                raise ValueError(f"找不到 Polymarket series：{slug}")
            return duration, str(series[0].id)

        resolved = await asyncio.gather(
            *(resolve(duration, slug) for duration, slug in SERIES_SLUGS.items())
        )
        self.series_ids = dict(resolved)

    async def _windows(self, duration: str, now: datetime) -> list[MarketWindow]:
        events = self.client.list_events(
            series_ids=[self.series_ids[duration]],
            closed=False,
            start_time_min=now - LOOKBACK,
            order="startTime",
            ascending=True,
            page_size=DISCOVERY_PAGE_SIZE,
        )
        result: list[MarketWindow] = []
        async for event in events.iter_items():
            window = event_window(event, duration)
            if (
                window is not None
                and window.end_at > now
                and window.start_at < now + PAIR_LOOKAHEAD
            ):
                result.append(window)
        return result

    async def find_pairs(self) -> list[MarketPair]:
        now = datetime.now(UTC)
        five_windows, fifteen_windows = await asyncio.gather(
            self._windows("5m", now),
            self._windows("15m", now),
        )
        fifteen_by_end = {window.end_at: window for window in fifteen_windows}
        pairs = [
            MarketPair(five=five, fifteen=fifteen_by_end[five.end_at])
            for five in five_windows
            if five.end_at in fifteen_by_end
        ]
        pairs.sort(key=lambda pair: pair.end_at)
        return pairs[:MAX_PAIRS_PER_SUBSCRIPTION]
