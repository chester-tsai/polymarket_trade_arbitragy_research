"""Fee-aware, visible-depth evaluation for the two strike-dominance cases."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import os
from typing import Mapping

from market_models import MarketPair, OpeningStrike
from orderbook import OrderBook


ONE = Decimal("1")
FEE_COEFFICIENT = Decimal(os.environ.get("POLYMARKET_FEE_COEFFICIENT", "0.07"))


def taker_fee_per_share(price: Decimal) -> Decimal:
    """Requested fee model: 0.07 * p * (1-p), charged on each bought leg."""
    return FEE_COEFFICIENT * price * (ONE - price)


@dataclass(frozen=True, slots=True)
class FillSlice:
    shares: Decimal
    first_price: Decimal
    second_price: Decimal
    first_fee_per_share: Decimal
    second_fee_per_share: Decimal

    @property
    def raw_cost(self) -> Decimal:
        return self.shares * (self.first_price + self.second_price)

    @property
    def fees(self) -> Decimal:
        return self.shares * (self.first_fee_per_share + self.second_fee_per_share)

    @property
    def total_cost(self) -> Decimal:
        return self.raw_cost + self.fees

    @property
    def net_profit(self) -> Decimal:
        # The dominance portfolio pays at least $1 per matched share at settlement.
        return self.shares - self.total_cost

    @property
    def all_in_cost_per_share(self) -> Decimal:
        return (
            self.first_price
            + self.second_price
            + self.first_fee_per_share
            + self.second_fee_per_share
        )


@dataclass(frozen=True, slots=True)
class ArbitrageAnalysis:
    case: str
    strategy: str
    condition: str
    first_label: str
    second_label: str
    first_token: str
    second_token: str
    k5: OpeningStrike
    k15: OpeningStrike
    fills: tuple[FillSlice, ...]
    minimum_order_size: Decimal
    first_best_bid: tuple[Decimal, Decimal] | None
    second_best_bid: tuple[Decimal, Decimal] | None
    first_best_ask: tuple[Decimal, Decimal] | None
    second_best_ask: tuple[Decimal, Decimal] | None
    first_book_updated_at: datetime | None
    second_book_updated_at: datetime | None

    @property
    def profitable_shares(self) -> Decimal:
        return sum((fill.shares for fill in self.fills), Decimal(0))

    @property
    def raw_cost(self) -> Decimal:
        return self.first_leg_cost + self.second_leg_cost

    @property
    def first_leg_cost(self) -> Decimal:
        return sum((fill.shares * fill.first_price for fill in self.fills), Decimal(0))

    @property
    def second_leg_cost(self) -> Decimal:
        return sum((fill.shares * fill.second_price for fill in self.fills), Decimal(0))

    @property
    def first_leg_fees(self) -> Decimal:
        return sum(
            (fill.shares * fill.first_fee_per_share for fill in self.fills), Decimal(0)
        )

    @property
    def second_leg_fees(self) -> Decimal:
        return sum(
            (fill.shares * fill.second_fee_per_share for fill in self.fills), Decimal(0)
        )

    @property
    def fees(self) -> Decimal:
        return sum((fill.fees for fill in self.fills), Decimal(0))

    @property
    def total_cost(self) -> Decimal:
        return self.raw_cost + self.fees

    @property
    def guaranteed_payout_floor(self) -> Decimal:
        return self.profitable_shares

    @property
    def net_profit_floor(self) -> Decimal:
        return self.guaranteed_payout_floor - self.total_cost

    @property
    def average_all_in_cost(self) -> Decimal | None:
        if self.profitable_shares <= 0:
            return None
        return self.total_cost / self.profitable_shares

    @property
    def tradable_size(self) -> bool:
        return self.profitable_shares >= self.minimum_order_size

    @property
    def profitable(self) -> bool:
        return self.tradable_size and self.net_profit_floor > 0


class ArbitrageEngine:
    """Evaluates only the case whose opening strikes establish dominance.

    The depth walk pairs equal share quantities from each leg. It stops at
    the first unprofitable marginal level; ask prices and all-in costs are
    increasing across the book, so deeper levels cannot become profitable
    again after that point.
    """

    def __init__(self, fee_coefficient: Decimal = FEE_COEFFICIENT) -> None:
        self.fee_coefficient = fee_coefficient

    def _fee(self, price: Decimal) -> Decimal:
        return self.fee_coefficient * price * (ONE - price)

    def evaluate(
        self,
        pair: MarketPair,
        books: Mapping[str, OrderBook],
        strikes: Mapping[str, OpeningStrike | None],
    ) -> ArbitrageAnalysis | None:
        k5 = strikes.get(pair.five.slug)
        k15 = strikes.get(pair.fifteen.slug)
        if k5 is None or k15 is None:
            return None

        if k15.price < k5.price:
            case = "Case A"
            strategy = "15m Up + 5m Down"
            condition = "K15 < K5"
            first_label, second_label = "15m Up", "5m Down"
            first_token, second_token = pair.fifteen.up_token, pair.five.down_token
        elif k5.price < k15.price:
            case = "Case B"
            strategy = "5m Up + 15m Down"
            condition = "K5 < K15"
            first_label, second_label = "5m Up", "15m Down"
            first_token, second_token = pair.five.up_token, pair.fifteen.down_token
        else:
            return None

        first_book = books[first_token]
        second_book = books[second_token]
        first_asks = first_book.depth(ask=True)
        second_asks = second_book.depth(ask=True)
        fills = self._walk_profitable_depth(first_asks, second_asks)
        return ArbitrageAnalysis(
            case=case,
            strategy=strategy,
            condition=condition,
            first_label=first_label,
            second_label=second_label,
            first_token=first_token,
            second_token=second_token,
            k5=k5,
            k15=k15,
            fills=tuple(fills),
            minimum_order_size=max(pair.five.min_order_size, pair.fifteen.min_order_size),
            first_best_bid=first_book.best(ask=False),
            second_best_bid=second_book.best(ask=False),
            first_best_ask=first_book.best(ask=True),
            second_best_ask=second_book.best(ask=True),
            first_book_updated_at=first_book.updated_at,
            second_book_updated_at=second_book.updated_at,
        )

    def _walk_profitable_depth(
        self,
        first_asks: tuple[tuple[Decimal, Decimal], ...],
        second_asks: tuple[tuple[Decimal, Decimal], ...],
    ) -> list[FillSlice]:
        result: list[FillSlice] = []
        if not first_asks or not second_asks:
            return result

        i = j = 0
        first_left = first_asks[0][1]
        second_left = second_asks[0][1]
        while i < len(first_asks) and j < len(second_asks):
            first_price = first_asks[i][0]
            second_price = second_asks[j][0]
            if not (Decimal(0) < first_price < ONE and Decimal(0) < second_price < ONE):
                break
            first_fee = self._fee(first_price)
            second_fee = self._fee(second_price)
            all_in = first_price + second_price + first_fee + second_fee
            if all_in >= ONE:
                break

            quantity = min(first_left, second_left)
            if quantity <= 0:
                break
            result.append(
                FillSlice(
                    shares=quantity,
                    first_price=first_price,
                    second_price=second_price,
                    first_fee_per_share=first_fee,
                    second_fee_per_share=second_fee,
                )
            )
            first_left -= quantity
            second_left -= quantity
            if first_left == 0:
                i += 1
                if i < len(first_asks):
                    first_left = first_asks[i][1]
            if second_left == 0:
                j += 1
                if j < len(second_asks):
                    second_left = second_asks[j][1]
        return result
