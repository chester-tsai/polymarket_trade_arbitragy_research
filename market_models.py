"""Small immutable models shared by the live monitor."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


def as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def as_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


@dataclass(frozen=True, slots=True)
class OpeningStrike:
    price: Decimal
    source: str
    source_timestamp: datetime
    captured_at: datetime
    skew_seconds: Decimal
    official: bool = False


@dataclass(frozen=True, slots=True)
class MarketWindow:
    duration: str
    slug: str
    question: str
    start_at: datetime
    end_at: datetime
    up_token: str
    down_token: str
    min_order_size: Decimal
    gamma_strike: Decimal | None = None

    @property
    def identity(self) -> str:
        return self.slug


@dataclass(frozen=True, slots=True)
class MarketPair:
    five: MarketWindow
    fifteen: MarketWindow

    def __post_init__(self) -> None:
        if self.five.duration != "5m" or self.fifteen.duration != "15m":
            raise ValueError("MarketPair must contain a 5m window and a 15m window")
        if as_utc(self.five.end_at) != as_utc(self.fifteen.end_at):
            raise ValueError("5m and 15m markets must have the exact same expiry")

    @property
    def end_at(self) -> datetime:
        return self.five.end_at

    @property
    def token_labels(self) -> dict[str, str]:
        return {
            self.five.up_token: "5m Up",
            self.five.down_token: "5m Down",
            self.fifteen.up_token: "15m Up",
            self.fifteen.down_token: "15m Down",
        }

    @property
    def windows(self) -> tuple[MarketWindow, MarketWindow]:
        return self.five, self.fifteen
