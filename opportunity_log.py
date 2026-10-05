"""Write detailed, fee-aware opportunities once per entry event."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from arbitrage_engine import ArbitrageAnalysis, FEE_COEFFICIENT
from market_models import MarketPair, OpeningStrike


def _strike_record(strike: OpeningStrike) -> dict[str, object]:
    return {
        "price": str(strike.price),
        "source": strike.source,
        "source_timestamp_utc": strike.source_timestamp.isoformat(),
        "captured_at_utc": strike.captured_at.isoformat(),
        "boundary_skew_seconds": str(strike.skew_seconds),
        "official_gamma_value": strike.official,
    }


def _best_record(value: tuple[Decimal, Decimal] | None) -> dict[str, str] | None:
    if value is None:
        return None
    return {"price": str(value[0]), "shares": str(value[1])}


def _make_record(pair: MarketPair, result: ArbitrageAnalysis) -> dict[str, object]:
    first_cost = result.first_leg_cost
    second_cost = result.second_leg_cost
    first_fee = result.first_leg_fees
    second_fee = result.second_leg_fees
    return {
        "detected_at_utc": datetime.now(UTC).isoformat(),
        "window_start_utc": pair.fifteen.start_at.isoformat(),
        "window_end_utc": pair.end_at.isoformat(),
        "five_min_market": pair.five.slug,
        "fifteen_min_market": pair.fifteen.slug,
        "five_min_up_token": pair.five.up_token,
        "five_min_down_token": pair.five.down_token,
        "fifteen_min_up_token": pair.fifteen.up_token,
        "fifteen_min_down_token": pair.fifteen.down_token,
        "status": "positive_visible_depth_after_fees",
        "strike_order": result.condition,
        "case": result.case,
        "strategy": result.strategy,
        "K5": _strike_record(result.k5),
        "K15": _strike_record(result.k15),
        "fee_model": {
            "formula_per_share_per_leg": f"{FEE_COEFFICIENT}*p*(1-p)",
            "coefficient": str(FEE_COEFFICIENT),
            "taker_fee_assumed": True,
        },
        "entry_legs": [
            {
                "outcome": result.first_label,
                "token_id": result.first_token,
                "book_updated_at_utc": (
                    result.first_book_updated_at.isoformat()
                    if result.first_book_updated_at is not None
                    else None
                ),
                "best_ask": _best_record(result.first_best_ask),
                "best_bid": _best_record(result.first_best_bid),
                "buy_shares": str(result.profitable_shares),
                "buy_cost_before_fee": str(first_cost),
                "fee": str(first_fee),
            },
            {
                "outcome": result.second_label,
                "token_id": result.second_token,
                "book_updated_at_utc": (
                    result.second_book_updated_at.isoformat()
                    if result.second_book_updated_at is not None
                    else None
                ),
                "best_ask": _best_record(result.second_best_ask),
                "best_bid": _best_record(result.second_best_bid),
                "buy_shares": str(result.profitable_shares),
                "buy_cost_before_fee": str(second_cost),
                "fee": str(second_fee),
            },
        ],
        "profitable_matched_shares": str(result.profitable_shares),
        "minimum_order_size": str(result.minimum_order_size),
        "visible_ask_depth_used": [
            {
                "shares": str(fill.shares),
                "first_leg_ask": str(fill.first_price),
                "second_leg_ask": str(fill.second_price),
                "first_leg_fee_per_share": str(fill.first_fee_per_share),
                "second_leg_fee_per_share": str(fill.second_fee_per_share),
                "all_in_cost_per_pair": str(fill.all_in_cost_per_share),
                "net_edge_per_pair": str(Decimal(1) - fill.all_in_cost_per_share),
            }
            for fill in result.fills
        ],
        "total_cost_before_fees": str(first_cost + second_cost),
        "total_fees": str(result.fees),
        "total_cost_after_fees": str(result.total_cost),
        "minimum_settlement_payout": str(result.guaranteed_payout_floor),
        "net_profit_floor_after_fees": str(result.net_profit_floor),
        "weighted_average_all_in_cost_per_pair": (
            str(result.average_all_in_cost) if result.average_all_in_cost is not None else None
        ),
        "actual_orders_placed": False,
    }


class OpportunityLogger:
    """Append JSONL only when a profitable, minimum-size entry appears.

    Ordinary websocket quote changes do not create records. File appends run
    in a worker thread so disk latency does not block book-event handling.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._active: set[tuple[str, str]] = set()
        self._writes: dict[asyncio.Task[None], tuple[str, str]] = {}
        self.last_error: str | None = None

    async def update(
        self, pair: MarketPair, result: ArbitrageAnalysis | None
    ) -> bool:
        key = (pair.end_at.isoformat(), result.strategy) if result is not None else None
        is_profitable = result is not None and result.profitable

        for active_key in tuple(self._active):
            if active_key[0] == pair.end_at.isoformat() and (
                not is_profitable or active_key != key
            ):
                self._active.discard(active_key)

        if not is_profitable or result is None or key in self._active:
            return False

        record = _make_record(pair, result)
        self._active.add(key)
        task = asyncio.create_task(asyncio.to_thread(self._append, record))
        self._writes[task] = key
        task.add_done_callback(self._write_finished)
        return True

    def _append(self, record: dict[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")
            stream.flush()

    def _write_finished(self, task: asyncio.Task[None]) -> None:
        key = self._writes.pop(task, None)
        try:
            task.result()
        except Exception as exc:  # noqa: BLE001 - report filesystem errors in the live UI
            self.last_error = f"{type(exc).__name__}: {exc}"
            if key is not None:
                self._active.discard(key)
            print(f"ERROR: 套利機會紀錄寫入失敗：{self.last_error}", flush=True)
        else:
            self.last_error = None

    async def close(self) -> None:
        if self._writes:
            await asyncio.gather(*tuple(self._writes), return_exceptions=True)
