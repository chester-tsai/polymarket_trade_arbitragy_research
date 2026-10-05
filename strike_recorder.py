"""Opening-strike capture using the reference project's Chainlink TWAP feed."""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from market_models import MarketWindow, OpeningStrike
from rtds_feed import PriceFeed


RTDS_TOPIC = "crypto_prices_twap_sixty"
CAPTURE_BUFFER_SECONDS = 3.0
MAX_START_SKEW_SECONDS = Decimal("6")
TICK_BUFFER_SIZE = 2400


class OpeningStrikeRecorder:
    """Capture the 60s Chainlink TWAP around market start boundaries.

    Gamma's eventMetadata.priceToBeat is preferred whenever present.
    For a live market where Gamma has not published it yet, the reference
    project's TWAP websocket is sampled around the scheduled start time.
    A process started after that boundary cannot reconstruct a missing strike;
    such a pair stays unconfirmed instead of using spot or an inferred value.
    """

    def __init__(self) -> None:
        self.strikes: dict[str, OpeningStrike] = {}
        self.updates: asyncio.Queue[str] = asyncio.Queue()
        self._ticks: deque[Any] = deque(maxlen=TICK_BUFFER_SIZE)
        self._registered: set[str] = set()
        self._capture_tasks: set[asyncio.Task[None]] = set()
        self._feed: Any = None
        self._feed_task: asyncio.Task[None] | None = None
        self.feed_error: str | None = None

    async def start(self) -> None:
        self._feed = PriceFeed(topic=RTDS_TOPIC)
        await self._feed.connect()
        self._feed_task = asyncio.create_task(self._consume())

    async def _consume(self) -> None:
        while True:
            try:
                if self._feed is None:
                    self._feed = PriceFeed(topic=RTDS_TOPIC)
                    await self._feed.connect()
                async for tick in self._feed.listen():
                    self._ticks.append(tick)
                raise ConnectionError("Chainlink TWAP websocket ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.feed_error = f"{type(exc).__name__}: {exc}"
                self.updates.put_nowait("__feed_error__")
                if self._feed is not None:
                    try:
                        await self._feed.close()
                    except Exception:
                        pass
                self._feed = None
                await asyncio.sleep(5)
                self.feed_error = None

    def register(self, window: MarketWindow) -> None:
        if window.slug in self._registered:
            return
        self._registered.add(window.slug)

        if window.gamma_strike is not None:
            now = datetime.now(UTC)
            self._publish(
                window.slug,
                OpeningStrike(
                    price=window.gamma_strike,
                    source="Gamma eventMetadata.priceToBeat",
                    source_timestamp=window.start_at,
                    captured_at=now,
                    skew_seconds=Decimal(0),
                    official=True,
                ),
            )
            return

        now = datetime.now(UTC)
        if window.start_at <= now:
            # A late start can use only a tick actually observed by this process.
            tick = self._nearest_tick(window.start_at)
            if tick is not None:
                self._publish(window.slug, self._from_tick(tick, window.start_at))
            elif (now - window.start_at).total_seconds() <= float(MAX_START_SKEW_SECONDS):
                task = asyncio.create_task(self._capture_after_start(window))
                self._capture_tasks.add(task)
                task.add_done_callback(self._capture_tasks.discard)
            return

        task = asyncio.create_task(self._capture_after_start(window))
        self._capture_tasks.add(task)
        task.add_done_callback(self._capture_tasks.discard)

    async def _capture_after_start(self, window: MarketWindow) -> None:
        capture_at = window.start_at + timedelta(seconds=CAPTURE_BUFFER_SECONDS)
        delay = (capture_at - datetime.now(UTC)).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)

        tick = self._nearest_tick(window.start_at)
        if tick is None:
            # One boundary handoff allowance for RTDS delivery lag; this is
            # not a polling loop and does not run the arbitrage scanner.
            await asyncio.sleep(1.0)
            tick = self._nearest_tick(window.start_at)
        if tick is None:
            return
        self._publish(window.slug, self._from_tick(tick, window.start_at))

    def _nearest_tick(self, boundary: datetime) -> Any | None:
        if not self._ticks:
            return None
        tick = min(self._ticks, key=lambda item: abs((item.timestamp - boundary).total_seconds()))
        skew = abs((tick.timestamp - boundary).total_seconds())
        return tick if skew <= float(MAX_START_SKEW_SECONDS) else None

    @staticmethod
    def _from_tick(tick: Any, boundary: datetime) -> OpeningStrike:
        return OpeningStrike(
            price=Decimal(str(tick.price)),
            source=f"RTDS {RTDS_TOPIC}",
            source_timestamp=tick.timestamp,
            captured_at=datetime.now(UTC),
            skew_seconds=Decimal(str(abs((tick.timestamp - boundary).total_seconds()))),
            official=False,
        )

    def _publish(self, slug: str, strike: OpeningStrike) -> None:
        self.strikes[slug] = strike
        self.updates.put_nowait(slug)

    async def close(self) -> None:
        for task in tuple(self._capture_tasks):
            task.cancel()
        if self._capture_tasks:
            await asyncio.gather(*self._capture_tasks, return_exceptions=True)
        if self._feed_task is not None:
            self._feed_task.cancel()
            await asyncio.gather(self._feed_task, return_exceptions=True)
        if self._feed is not None:
            await self._feed.close()
