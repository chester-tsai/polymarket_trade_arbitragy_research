"""Non-blocking gzip-compressed JSONL recording of top-five CLOB book updates."""

from __future__ import annotations

import asyncio
import gzip
import heapq
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from market_models import MarketWindow, OpeningStrike
from orderbook import OrderBook


class OrderBookHistoryWriter:
    """Queue top-five book states and append them from a background worker.

    One compact row is queued per changed token, not a repeated copy of all
    four books. The queue put never waits, and disk I/O/JSON encoding run in a
    worker thread so they cannot pause WebSocket event consumption.
    """

    def __init__(self, directory: Path, *, max_queue_size: int = 20_000) -> None:
        self.directory = directory
        self._queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(
            maxsize=max_queue_size
        )
        self._task: asyncio.Task[None] | None = None
        self.dropped_records = 0
        self.failed_records = 0
        self._overflow_reported = False
        self._last_write_error: str | None = None

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("order book history writer already started")
        self._task = asyncio.create_task(self._run(), name="orderbook-history-writer")

    def record(
        self,
        window: MarketWindow,
        outcome: str,
        token_id: str,
        book: OrderBook,
        strike: OpeningStrike | None,
        event_received_at: datetime,
        event_type: str,
    ) -> None:
        if self._task is None or not book.ready:
            return

        bids = heapq.nlargest(5, book.bids.items())
        asks = heapq.nsmallest(5, book.asks.items())
        row: dict[str, Any] = {
            "event_received_at_utc": event_received_at.isoformat(),
            "event_type": event_type,
            "market_duration": window.duration,
            "market_slug": window.slug,
            "market_start_utc": window.start_at.isoformat(),
            "market_expiry_utc": window.end_at.isoformat(),
            "outcome": outcome,
            "token_id": token_id,
            "book_updated_at_utc": (
                book.updated_at.isoformat() if book.updated_at is not None else None
            ),
            "bids": [[str(price), str(size)] for price, size in bids],
            "asks": [[str(price), str(size)] for price, size in asks],
            "opening_strike": self._strike_record(strike),
        }
        try:
            self._queue.put_nowait(row)
        except asyncio.QueueFull:
            self.dropped_records += 1
            if not self._overflow_reported:
                self._overflow_reported = True
                print(
                    "ERROR: orderbook history queue is full; some history rows are being dropped.",
                    flush=True,
                )

    @staticmethod
    def _strike_record(strike: OpeningStrike | None) -> dict[str, object] | None:
        if strike is None:
            return None
        return {
            "price": str(strike.price),
            "source": strike.source,
            "official": strike.official,
            "source_timestamp_utc": strike.source_timestamp.isoformat(),
            "captured_at_utc": strike.captured_at.isoformat(),
            "boundary_skew_seconds": str(strike.skew_seconds),
        }

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            first = await self._queue.get()
            if first is None:
                self._queue.task_done()
                return

            batch = [first]
            should_stop = False
            flush_deadline = loop.time() + 0.25
            while len(batch) < 250:
                remaining = flush_deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    row = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                except TimeoutError:
                    break
                if row is None:
                    self._queue.task_done()
                    should_stop = True
                    break
                batch.append(row)

            try:
                await asyncio.to_thread(self._append_batch, batch)
            except Exception as exc:  # noqa: BLE001 - report storage errors without stopping the feed
                self.failed_records += len(batch)
                message = f"{type(exc).__name__}: {exc}"
                if message != self._last_write_error:
                    print(f"ERROR: orderbook history write failed: {message}", flush=True)
                    self._last_write_error = message
            else:
                self._last_write_error = None
            finally:
                for _ in batch:
                    self._queue.task_done()

            if should_stop:
                return

    def _append_batch(self, rows: list[dict[str, Any]]) -> None:
        by_day: dict[str, list[str]] = {}
        for row in rows:
            day = str(row["event_received_at_utc"])[:10]
            by_day.setdefault(day, []).append(
                json.dumps(row, ensure_ascii=False, separators=(",", ":"))
            )

        for day, lines in by_day.items():
            path = self.directory / f"orderbook_history_{day}.jsonl.gz"
            payload = ("\n".join(lines) + "\n").encode("utf-8")
            # Each flushed batch is a separate gzip member. Standard gzip readers
            # transparently concatenate members, while a process interruption
            # cannot leave the entire day's history as one unfinished stream.
            with gzip.open(path, "ab", compresslevel=6) as stream:
                stream.write(payload)

    async def close(self) -> None:
        task = self._task
        if task is None:
            return
        self._task = None
        if not task.done():
            await self._queue.put(None)
        await task
        if self.dropped_records:
            print(
                f"ERROR: orderbook history dropped {self.dropped_records} rows because its queue filled.",
                flush=True,
            )
        if self.failed_records:
            print(
                f"ERROR: orderbook history could not write {self.failed_records} rows.",
                flush=True,
            )
