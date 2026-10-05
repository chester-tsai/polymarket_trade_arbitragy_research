"""Fee-aware Polymarket BTC 5m/15m strike-dominance monitor."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Any

from polymarket import AsyncPublicClient
from polymarket.streams import MarketSpec

from arbitrage_engine import (
    ArbitrageAnalysis,
    ArbitrageEngine,
    taker_fee_per_share,
)
from market_discovery import DISCOVERY_RETRY_SECONDS, MarketDiscovery
from market_history import OrderBookHistoryWriter
from market_models import MarketPair, MarketWindow
from opportunity_log import OpportunityLogger
from orderbook import OrderBook
from strike_recorder import OpeningStrikeRecorder


ANALYSIS_FILL_LINES = 12
_OPPORTUNITY_PRINT_LOCK = Lock()
_BACKGROUND_REPORTS: set[asyncio.Task[None]] = set()


def _all_tokens(pairs: list[MarketPair]) -> list[str]:
    return list(
        dict.fromkeys(
            token
            for pair in pairs
            for token in (
                pair.five.up_token,
                pair.five.down_token,
                pair.fifteen.up_token,
                pair.fifteen.down_token,
            )
        )
    )


def _apply_event(event: Any, books: dict[str, OrderBook]) -> set[str]:
    event_type = getattr(event, "type", None)
    event_type = str(getattr(event_type, "value", event_type)).lower()
    payload = getattr(event, "payload", None)
    if payload is None:
        return set()

    if event_type == "book":
        token_id = str(payload.token_id)
        book = books.get(token_id)
        return book.apply(event, {token_id}) if book is not None else set()

    if event_type == "price_change":
        changed: set[str] = set()
        received_at = datetime.now(UTC)
        for update in payload.price_changes:
            token_id = str(update.token_id)
            book = books.get(token_id)
            if book is not None:
                book.apply_change(update, received_at)
                changed.add(token_id)
        return changed
    return set()


def _pair_books_ready(pair: MarketPair, books: dict[str, OrderBook]) -> bool:
    token_ids = (
        pair.five.up_token,
        pair.five.down_token,
        pair.fifteen.up_token,
        pair.fifteen.down_token,
    )
    return all(books[token_id].ready for token_id in token_ids)


def _best_quote_line(
    label: str,
    ask: tuple[Decimal, Decimal] | None,
    bid: tuple[Decimal, Decimal] | None,
) -> str:
    ask_text = "—" if ask is None else f"USD {ask[0]:.3f}×{ask[1]:.2f}"
    bid_text = "—" if bid is None else f"USD {bid[0]:.3f}×{bid[1]:.2f}"
    if ask is None:
        return f"{label}: ask={ask_text}, bid={bid_text}"
    fee = taker_fee_per_share(ask[0])
    return (
        f"{label}: ask={ask_text}, fee/share=USD {fee:.5f}, "
        f"ask+fee=USD {ask[0] + fee:.5f}, bid={bid_text}"
    )


def _print_opportunity_report(pair: MarketPair, analysis: ArbitrageAnalysis) -> None:
    """Print a detailed report only for a newly detected tradable opportunity."""
    print("\n" + "=" * 78)
    print("套利機會：扣除兩腿手續費後仍有正淨利（paper analysis，未送單）")
    print(f"偵測時間：{datetime.now(UTC).isoformat()}")
    print(f"共同到期：{pair.end_at.isoformat()} UTC")
    print(f"Case：{analysis.case}；條件：{analysis.condition}；策略：{analysis.strategy}")
    print(
        f"K15=USD {analysis.k15.price:,.4f} [{analysis.k15.source}], "
        f"K5=USD {analysis.k5.price:,.4f} [{analysis.k5.source}], "
        f"K5-K15=USD {analysis.k5.price - analysis.k15.price:+,.4f}"
    )

    for label, ask, bid in (
        (analysis.first_label, analysis.first_best_ask, analysis.first_best_bid),
        (analysis.second_label, analysis.second_best_ask, analysis.second_best_bid),
    ):
        print(_best_quote_line(label, ask, bid))

    bid_pair_size = None
    if analysis.first_best_bid is not None and analysis.second_best_bid is not None:
        bid_pair_size = min(analysis.first_best_bid[1], analysis.second_best_bid[1])
    bid_size_text = "未知" if bid_pair_size is None else f"{bid_pair_size:.4f}"
    print(
        f"可獲利 ask 深度：{analysis.profitable_shares:.4f} 股配對；"
        f"兩腿 best-bid 配對量：{bid_size_text} 股；"
        f"最小下單量：{analysis.minimum_order_size} 股"
    )

    print("可獲利 ask 深度配對：")
    for fill in analysis.fills[:ANALYSIS_FILL_LINES]:
        print(
            f"  {fill.shares:.4f} 股：{analysis.first_label} {fill.first_price:.3f} + "
            f"{analysis.second_label} {fill.second_price:.3f}；"
            f"fees={fill.first_fee_per_share + fill.second_fee_per_share:.5f}/組；"
            f"all-in={fill.all_in_cost_per_share:.5f}/組；"
            f"net={Decimal(1) - fill.all_in_cost_per_share:+.5f}/組"
        )
    if len(analysis.fills) > ANALYSIS_FILL_LINES:
        print(f"  其餘 {len(analysis.fills) - ANALYSIS_FILL_LINES} 個深度檔位已完整寫入 JSONL")

    print(
        f"買入成本：USD {analysis.raw_cost:.5f}；手續費：USD {analysis.fees:.5f}；"
        f"總成本：USD {analysis.total_cost:.5f}；"
        f"保證 payout floor：USD {analysis.guaranteed_payout_floor:.5f}；"
        f"扣費後淨利下限：USD {analysis.net_profit_floor:+.5f}"
    )
    print("終端狀態 payoff ≥ USD 1.00/組；本程式只監看及記錄，不會送出訂單。")
    print("機會詳細資料寫入排程：opportunities.jsonl")
    print("=" * 78, flush=True)


def _print_opportunity(pair: MarketPair, analysis: ArbitrageAnalysis) -> None:
    with _OPPORTUNITY_PRINT_LOCK:
        _print_opportunity_report(pair, analysis)


def _report_finished(task: asyncio.Task[None]) -> None:
    _BACKGROUND_REPORTS.discard(task)
    try:
        task.result()
    except Exception as exc:
        print(f"ERROR: 套利機會終端輸出失敗：{type(exc).__name__}: {exc}", flush=True)


def _schedule_opportunity_report(pair: MarketPair, analysis: ArbitrageAnalysis) -> None:
    task = asyncio.create_task(asyncio.to_thread(_print_opportunity, pair, analysis))
    _BACKGROUND_REPORTS.add(task)
    task.add_done_callback(_report_finished)


async def _evaluate_and_log(
    pair: MarketPair,
    books: dict[str, OrderBook],
    recorder: OpeningStrikeRecorder,
    engine: ArbitrageEngine,
    logger: OpportunityLogger,
) -> ArbitrageAnalysis | None:
    if not _pair_books_ready(pair, books):
        await logger.update(pair, None)
        return None
    result = engine.evaluate(pair, books, recorder.strikes)
    is_new_opportunity = await logger.update(pair, result)
    if is_new_opportunity and result is not None:
        _schedule_opportunity_report(pair, result)
    return result


async def _watch_pairs(
    client: AsyncPublicClient,
    pairs: list[MarketPair],
    recorder: OpeningStrikeRecorder,
    engine: ArbitrageEngine,
    opportunity_logger: OpportunityLogger,
    history_writer: OrderBookHistoryWriter,
) -> None:
    for pair in pairs:
        for window in pair.windows:
            recorder.register(window)

    token_ids = _all_tokens(pairs)
    token_books = {token_id: OrderBook() for token_id in token_ids}
    token_pairs: dict[str, list[MarketPair]] = {token_id: [] for token_id in token_ids}
    token_windows: dict[str, tuple[MarketWindow, str]] = {}
    pair_by_slug: dict[str, list[MarketPair]] = {}
    for pair in pairs:
        for token_id in pair.token_labels:
            token_pairs[token_id].append(pair)
        for window in pair.windows:
            pair_by_slug.setdefault(window.slug, []).append(pair)
            token_windows[window.up_token] = (window, "Up")
            token_windows[window.down_token] = (window, "Down")

    current_pair = pairs[0]
    remaining = max(0.0, (current_pair.end_at - datetime.now(UTC)).total_seconds())

    async with await client.subscribe(MarketSpec(token_ids=token_ids)) as stream:
        async def consume_books() -> None:
            async for event in stream:
                changed_tokens = _apply_event(event, token_books)
                if not changed_tokens:
                    continue
                event_type = getattr(event, "type", None)
                event_type = str(getattr(event_type, "value", event_type)).lower()
                event_received_at = datetime.now(UTC)
                strikes = recorder.strikes
                for token_id in changed_tokens:
                    metadata = token_windows.get(token_id)
                    if metadata is None:
                        continue
                    window, outcome = metadata
                    history_writer.record(
                        window=window,
                        outcome=outcome,
                        token_id=token_id,
                        book=token_books[token_id],
                        strike=strikes.get(window.slug),
                        event_received_at=event_received_at,
                        event_type=event_type,
                    )
                affected_pairs = {
                    pair
                    for token_id in changed_tokens
                    for pair in token_pairs.get(token_id, ())
                }
                for pair in affected_pairs:
                    await _evaluate_and_log(
                        pair, token_books, recorder, engine, opportunity_logger
                    )

        async def consume_strikes() -> None:
            while True:
                slug = await recorder.updates.get()
                if slug == "__feed_error__":
                    raise ConnectionError(recorder.feed_error or "Chainlink TWAP feed disconnected")
                for pair in pair_by_slug.get(slug, ()):
                    await _evaluate_and_log(
                        pair, token_books, recorder, engine, opportunity_logger
                    )

        consumers = {
            asyncio.create_task(consume_books()),
            asyncio.create_task(consume_strikes()),
        }
        try:
            await asyncio.wait_for(asyncio.gather(*consumers), timeout=remaining)
        except asyncio.TimeoutError:
            pass
        finally:
            for task in consumers:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*consumers, return_exceptions=True)


async def run() -> None:
    logger = OpportunityLogger(Path(__file__).resolve().with_name("opportunities.jsonl"))
    history_writer = OrderBookHistoryWriter(Path(__file__).resolve().parent)
    history_writer.start()
    engine = ArbitrageEngine()
    recorder = OpeningStrikeRecorder()

    try:
        while True:
            try:
                await recorder.start()
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"ERROR: Chainlink TWAP feed 連線失敗：{exc}; {DISCOVERY_RETRY_SECONDS}s 後重試。", flush=True)
                await recorder.close()
                await asyncio.sleep(DISCOVERY_RETRY_SECONDS)
        async with AsyncPublicClient() as client:
            discovery = MarketDiscovery(client)
            while not discovery.series_ids:
                try:
                    await discovery.initialize()
                except Exception as exc:
                    print(f"ERROR: Series discovery 失敗：{exc}; {DISCOVERY_RETRY_SECONDS}s 後重試。", flush=True)
                    await asyncio.sleep(DISCOVERY_RETRY_SECONDS)

            while True:
                try:
                    pairs = await discovery.find_pairs()
                    if not pairs:
                        await asyncio.sleep(DISCOVERY_RETRY_SECONDS)
                        continue
                    await _watch_pairs(
                        client, pairs, recorder, engine, logger, history_writer
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    print(f"ERROR: Market stream/discovery 斷線：{exc}; 重新連線中。", flush=True)
                    await asyncio.sleep(DISCOVERY_RETRY_SECONDS)
    finally:
        await recorder.close()
        await logger.close()
        await history_writer.close()
        if _BACKGROUND_REPORTS:
            await asyncio.gather(*tuple(_BACKGROUND_REPORTS), return_exceptions=True)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
