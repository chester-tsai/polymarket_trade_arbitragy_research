# BTC 5m/15m Strike Dominance Monitor

This is a live, read-only Polymarket monitor. It keeps the public 5m and 15m
order books in memory, records top-five book updates, and evaluates a
marketable two-leg entry against every new book update. It does not require a
wallet and does not place orders.

## Run in VS Code

Use Python 3.11 or newer. On a new Windows computer, from the project folder:

    py -m venv .venv
    & .\.venv\Scripts\python.exe -m pip install -r requirements.txt
    & .\.venv\Scripts\python.exe .\main.py

In VS Code, select `.venv` as the interpreter and run **BTC Strike Dominance
Demo** from Run and Debug.

Dependencies are listed in `requirements.txt`. Runtime logs and collected
order-book history are local files and are excluded from Git.

## Strike ordering and the two cases

The program pairs 5m and 15m markets only when their scheduled end times match.
It gets an official priceToBeat from Gamma when present. Otherwise it reuses
polymarket_trader/price_feed.py from the referenced trader project and
captures the Chainlink 60-second TWAP at each market's scheduled opening
boundary. It subscribes to upcoming pairs in advance so both strikes can be
captured before the shared expiry.

- **Case A — K15 < K5:** buy 15m Up and 5m Down. Payouts by settlement price:
  below K15 = $1/share; between strikes = $2/share; at or above K5 = $1/share.
- **Case B — K5 < K15:** buy 5m Up and 15m Down. Payouts:
  below K5 = $1/share; between strikes = $2/share; at or above K15 = $1/share.
- **K15 = K5:** neither case has strike dominance.

If the process starts after a market opened and no official strike is available,
that pair is not called an arbitrage. The monitor waits for a pair whose opening
strikes it can capture. The opportunity record includes each strike's source,
source timestamp, and capture skew.

## Fee and size calculation

For each ask level in each entry leg, fee per share is calculated as requested:

    fee(p) = 0.07 * p * (1 - p)
    all-in pair cost = ask_1 + ask_2 + fee(ask_1) + fee(ask_2)

The default coefficient is the literal 0.07. To override it for a different
fee schedule, set POLYMARKET_FEE_COEFFICIENT before launch; for example, in
PowerShell use $env:POLYMARKET_FEE_COEFFICIENT="0.08".

The engine walks both ask books together, matching equal share quantities by
price level. It stops when the marginal all-in cost reaches $1. It reports the
profitable matched share count, each level used, total buy cost, fees, minimum
settlement payout, and net profit floor after fees. The reported quantity can
exceed either best-bid level because it uses cumulative ask depth; bid depth is
shown separately as an immediate-exit reference.

A strategy is recorded only when both strikes are known, the ask depth reaches
the market minimum order size, and total net profit after the configured fees is
positive. Ordinary quote changes do not create log lines. New opportunities
are appended to opportunities.jsonl; each record includes strikes, both legs,
fee breakdown, used ask levels, payout floor, and net profit. actual_orders_placed
is always false.

## Order book history

While the monitor is running, each changed token book is recorded with its top
five bids and asks, market/token identifiers, event timestamp, and opening
strike when available. Daily files use gzip-compressed JSONL:
`orderbook_history_YYYY-MM-DD.jsonl.gz`. Compression runs in the existing
background writer, in batches, so the WebSocket event loop does not wait for
disk writes or compression. Each flushed batch is a standard gzip member; tools
that support concatenated gzip members can read the whole daily file. In Python,
use `gzip.open(path, "rt", encoding="utf-8")` to read its JSONL rows. The
`bids` and `asks` fields are best-to-worst arrays of `[price, shares]`.
No second market connection is opened. If storage falls behind far enough to
fill the bounded queue, an error is printed and affected rows are counted as
dropped rather than blocking price processing.

The Chainlink TWAP feed is implemented locally in `rtds_feed.py`, using the
public Polymarket RTDS WebSocket. The market books use the public SDK WebSocket.
The program does not require the reference trader project, a wallet, or its
database modules.
