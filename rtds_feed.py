"""Minimal public Polymarket RTDS client for BTC Chainlink TWAP ticks."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime

import certifi
import websockets
from websockets.asyncio.client import ClientConnection


logger = logging.getLogger(__name__)
RTDS_URI = "wss://ws-live-data.polymarket.com"
DEFAULT_CRYPTO_SYMBOL = "btc/usd"
DEFAULT_CRYPTO_TOPIC = "crypto_prices_twap_sixty"


@dataclass(frozen=True)
class PriceTick:
    """One timestamped reference-price update from Polymarket RTDS."""

    symbol: str
    price: float
    timestamp: datetime
    server_timestamp: datetime


class PriceFeed:
    """Subscribe to RTDS and reconnect after interrupted WebSocket sessions."""

    def __init__(
        self,
        symbol: str = DEFAULT_CRYPTO_SYMBOL,
        topic: str = DEFAULT_CRYPTO_TOPIC,
        max_reconnect_attempts: int = 3,
        reconnect_delay_seconds: float = 5.0,
    ) -> None:
        self._symbol = symbol
        self._topic = topic
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_delay_seconds = reconnect_delay_seconds
        self._ws: ClientConnection | None = None

    async def connect(self) -> None:
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        self._ws = await websockets.connect(RTDS_URI, ssl=ssl_context)
        filters = json.dumps({"symbol": self._symbol}, separators=(",", ":"))
        await self._ws.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "subscriptions": [
                        {"topic": self._topic, "type": "*", "filters": filters}
                    ],
                }
            )
        )

    async def _reconnect(self) -> None:
        last_error: Exception | None = None
        for attempt in range(1, self._max_reconnect_attempts + 1):
            await asyncio.sleep(self._reconnect_delay_seconds)
            try:
                await self.connect()
                logger.info("RTDS feed reconnected on attempt %s", attempt)
                return
            except Exception as exc:  # noqa: BLE001 - retry network errors
                last_error = exc
                logger.warning("RTDS reconnect attempt %s failed: %s", attempt, exc)
        raise ConnectionError(
            f"RTDS failed to reconnect after {self._max_reconnect_attempts} attempts"
        ) from last_error

    async def listen(self) -> AsyncIterator[PriceTick]:
        if self._ws is None:
            raise RuntimeError("call connect() before listen()")

        while True:
            try:
                async for raw in self._ws:
                    if not raw:
                        continue
                    message = json.loads(raw)
                    if (
                        message.get("topic") != self._topic
                        or message.get("type") != "update"
                    ):
                        continue
                    payload = message["payload"]
                    yield PriceTick(
                        symbol=payload["symbol"],
                        price=payload["value"],
                        timestamp=datetime.fromtimestamp(
                            payload["timestamp"] / 1000, tz=UTC
                        ),
                        server_timestamp=datetime.fromtimestamp(
                            message["timestamp"] / 1000, tz=UTC
                        ),
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - reconnect after stream errors
                logger.warning("RTDS connection ended: %s", exc)
            await self._reconnect()

    async def close(self) -> None:
        if self._ws is not None:
            await self._ws.close()
            self._ws = None
