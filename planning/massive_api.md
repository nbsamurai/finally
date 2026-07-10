# Massive API Reference (formerly Polygon.io)

Research reference for the Massive REST API as used in FinAlly's real-market-data path. Massive.com is the October 30, 2025 rebrand of Polygon.io — same accounts, same API keys, same endpoints; only the default base URL and package name changed. This document verifies and supersedes the equivalent notes in `planning/archive/MASSIVE_API.md`.

## Overview

- **Company / product**: Massive (massive.com) — real-time and historical market data for stocks, options, forex, crypto, indices, futures. Formerly branded Polygon.io.
- **Base URL**: `https://api.massive.com` (the legacy `https://api.polygon.io` host is still accepted for backward compatibility)
- **Python package**: [`massive`](https://pypi.org/project/massive/) on PyPI — "Official Massive (formerly Polygon.io) REST and Websocket client". Current version at time of writing: 2.8.0. Install with `pip install -U massive` or `uv add massive`.
- **Min Python version**: 3.9 (tested through 3.14)
- **Repo**: [`massive-com/client-python`](https://github.com/massive-com/client-python) (MIT licensed)
- **Docs**: [massive.com/docs](https://massive.com/docs)
- **Auth**: API key, sent as `Authorization: Bearer <API_KEY>`. The `RESTClient` adds this header automatically — you never construct it by hand.

```python
from massive import RESTClient

# Reads MASSIVE_API_KEY from the environment automatically
client = RESTClient()

# Or pass explicitly
client = RESTClient(api_key="your_key_here")
```

`RESTClient` is a **synchronous** client. There is a separate `WebSocketClient` for streaming, but FinAlly does not use it (see "Why polling, not WebSocket" below).

## Rate Limits

| Tier | Limit |
|------|-------|
| Free | 5 requests/minute |
| Paid (all tiers) | Unlimited requests (production guidance: stay well under abuse thresholds) |

Source: [Massive knowledge base — "What is the request limit for Massive's RESTful APIs?"](https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis) and [Massive pricing](https://massive.com/pricing). Paid plans start at $199/month.

Because FinAlly polls on a timer rather than holding a streaming connection, the rate limit directly sets the poll interval:

| Tier | Poll interval used by FinAlly |
|------|-------------------------------|
| Free | 15s (well under the 5/min = 1 per 12s ceiling, with margin) |
| Paid | 2–5s |

## Why Polling, Not WebSocket

Massive offers a WebSocket API for tick-level streaming, but FinAlly's `MarketDataSource` interface (see `market_interface.md`) is poll-shaped by design — the simulator also produces discrete "ticks" on a timer, and both implementations write into the same `PriceCache` on their own schedule. Polling with `get_snapshot_all()` also has a decisive practical advantage: **one REST call returns snapshots for every watched ticker**, which is what keeps the free tier (5 req/min) viable regardless of watchlist size. A WebSocket subscription would need per-symbol subscription management for no benefit at this project's scale.

## Endpoints Used in FinAlly

### 1. Full Market Snapshot — All Tickers (primary endpoint)

The endpoint the poller calls every cycle. Returns current-state snapshots for an arbitrary list of tickers in a single call.

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers`

**Query parameters**:
| Param | Type | Notes |
|---|---|---|
| `tickers` | string | Case-sensitive, comma-separated list, e.g. `AAPL,TSLA,GOOG` |
| `include_otc` | boolean | Include OTC securities; defaults to `false` |

**Raw REST response** (field names as they appear on the wire — note the abbreviated camelCase, this is the raw JSON shape, *not* what the Python client exposes):

```json
{
  "status": "OK",
  "count": 1,
  "tickers": [
    {
      "ticker": "AAPL",
      "day": {
        "o": 129.61, "h": 130.15, "l": 125.07, "c": 125.07,
        "v": 111237700, "vw": 127.35
      },
      "prevDay": { "c": 129.61 },
      "lastTrade": { "p": 125.07, "s": 100, "t": 1675190399000000000 },
      "lastQuote": { "P": 125.08, "p": 125.06, "S": 1000, "s": 500 },
      "min": { "o": 125.0, "h": 125.1, "l": 124.9, "c": 125.07, "v": 5000 },
      "todaysChange": -4.54,
      "todaysChangePerc": -3.50,
      "updated": 1675190399000000000
    }
  ]
}
```

Note `lastTrade.t` and `updated` are Unix **nanoseconds** in the raw REST payload; the Python client normalizes this (see below).

**Python client** — this is what FinAlly actually calls:

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient()

snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
)

for snap in snapshots:
    print(f"{snap.ticker}: ${snap.last_trade.price}")
    print(f"  Day change: {snap.day.change_percent}%")
    print(f"  Day OHLC: O={snap.day.open} H={snap.day.high} L={snap.day.low} C={snap.day.close}")
    print(f"  Volume: {snap.day.volume}")
```

The client maps the raw JSON onto typed, snake_case model objects (`snap.last_trade.price`, `snap.day.previous_close`, etc.) and converts the trade timestamp to a field measured in **milliseconds** (`snap.last_trade.timestamp`) — divide by 1000 to get Unix seconds, which is what `PriceCache.update()` expects.

**Key fields extracted by FinAlly's poller**:
- `snap.last_trade.price` — current price for trading and display
- `snap.last_trade.timestamp` — when the trade occurred (ms → convert to seconds)
- `snap.day.previous_close` / `snap.day.change_percent` — for day-change display, if surfaced later

### 2. Single Ticker Snapshot

Detailed data for one ticker — useful if the chart-detail view ever wants finer data (bid/ask spread, minute bar) than the watchlist poll provides. Not currently called by FinAlly's poller, which always uses the batched all-tickers form, but documented here since the frontend spec calls for a "detailed chart" on ticker click.

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers/{stocksTicker}`

**Python client**:
```python
snapshot = client.get_snapshot_ticker(
    market_type=SnapshotMarketType.STOCKS,
    ticker="AAPL",
)

print(f"Price: ${snapshot.last_trade.price}")
print(f"Bid/Ask: ${snapshot.last_quote.bid_price} / ${snapshot.last_quote.ask_price}")
print(f"Day range: ${snapshot.day.low} - ${snapshot.day.high}")
```

Response shape matches the per-ticker object from the all-tickers endpoint, plus `min` (latest minute bar) and, on Business plans only, `fmv` (proprietary fair market value).

### 3. Previous Close

Previous trading day's OHLCV for a ticker. Useful for computing day-change percentages and for picking realistic simulator seed prices.

**REST**: `GET /v2/aggs/ticker/{stocksTicker}/prev`

**Query parameters**: `adjusted` (boolean, default `true`) — set `false` to exclude split adjustments.

**Response**:
```json
{
  "status": "OK",
  "ticker": "AAPL",
  "adjusted": true,
  "queryCount": 1,
  "resultsCount": 1,
  "results": [
    { "T": "AAPL", "o": 115.55, "h": 117.59, "l": 114.13, "c": 115.97, "v": 131704427, "vw": 116.3058, "t": 1605042000000 }
  ]
}
```

**Python client**:
```python
prev = client.get_previous_close_agg(ticker="AAPL")

for agg in prev:
    print(f"Previous close: ${agg.close}")
    print(f"OHLC: O={agg.open} H={agg.high} L={agg.low} C={agg.close}")
    print(f"Volume: {agg.volume}")
```

### 4. Aggregates (Custom Bars)

Historical OHLCV bars over a date range and arbitrary granularity. Not needed for the live poll loop, but the natural source for a historical price-history feature if FinAlly ever adds one (e.g. backfilling a chart before the SSE stream has accumulated enough points).

**REST**: `GET /v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{from}/{to}`

**Parameters**:
| Param | Required | Notes |
|---|---|---|
| `multiplier` | yes | Size of the timespan window, e.g. `1` |
| `timespan` | yes | `minute`, `hour`, `day`, `week`, `month`, `quarter`, `year` |
| `from` / `to` | yes | `YYYY-MM-DD` or millisecond epoch timestamp |
| `adjusted` | no | Split-adjust results, default `true` |
| `sort` | no | `asc` or `desc` |
| `limit` | no | Max base aggregates returned; max/default `50000`/`5000` |

**Python client**:
```python
aggs = []
for a in client.list_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="day",
    from_="2024-01-01",
    to="2024-01-31",
    limit=50000,
):
    aggs.append(a)

for a in aggs:
    print(f"Date: {a.timestamp}, O={a.open} H={a.high} L={a.low} C={a.close} V={a.volume}")
```

`list_aggs` is a paginating generator — it transparently follows `next_url` cursors, so the loop above may issue multiple HTTP requests under the hood if the range exceeds `limit`.

### 5. Last Trade / Last Quote

Single-field lookups, mostly superseded by the snapshot endpoints for FinAlly's purposes but worth knowing:

```python
trade = client.get_last_trade(ticker="AAPL")
print(f"Last trade: ${trade.price} x {trade.size}")

quote = client.get_last_quote(ticker="AAPL")
print(f"Bid: ${quote.bid} x {quote.bid_size}")
print(f"Ask: ${quote.ask} x {quote.ask_size}")
```

## How FinAlly Uses the API

`backend/app/market/massive_client.py` implements `MassiveDataSource`, one of the two `MarketDataSource` implementations (see `market_interface.md`). The poll loop:

1. On `start(tickers)`, construct `RESTClient(api_key=...)` and run one immediate poll so the cache isn't empty while waiting for the first interval.
2. Every `poll_interval` seconds, call `get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=self._tickers)` for the full watched-ticker set in one request.
3. For each returned snapshot, extract `last_trade.price` and `last_trade.timestamp` (ms → s) and write into the shared `PriceCache`.
4. `add_ticker` / `remove_ticker` just mutate the in-memory ticker list — the next poll cycle picks up the change; no separate subscription call is needed since the whole list is re-fetched each cycle.

```python
class MassiveDataSource(MarketDataSource):
    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            # RESTClient is synchronous — run in a thread so it doesn't block the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            for snap in snapshots:
                price = snap.last_trade.price
                timestamp = snap.last_trade.timestamp / 1000.0  # ms -> s
                self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Swallow and retry next interval — see Error Handling below.

    def _fetch_snapshots(self) -> list:
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

## Error Handling

The client raises exceptions for HTTP-level failures. FinAlly's poller wraps each cycle in a broad `try/except`, logs, and lets the next scheduled poll retry — a single failed cycle never crashes the background task or blocks the SSE stream (stale cache data just keeps being served until the next successful poll).

| Status | Meaning | FinAlly's handling |
|---|---|---|
| 401 | Invalid API key | Logged; retried next cycle (will keep failing until key is fixed) |
| 403 | Plan doesn't include this endpoint | Logged; retried next cycle |
| 429 | Rate limit exceeded (free tier: 5 req/min) | Logged; the poll interval (15s) is already sized to avoid this in steady state |
| 5xx | Server error | Logged; retried next cycle |

## Notes and Gotchas

- The all-tickers snapshot endpoint returning **every requested ticker in one call** is the load-bearing property for staying inside the free tier's 5 req/min limit — never switch to per-ticker calls in a loop.
- Raw REST timestamps are Unix **nanoseconds**; the Python client's model objects report **milliseconds**; FinAlly's `PriceCache` wants Unix **seconds** — always convert (`/ 1000.0` from the client's `timestamp` field).
- Outside market hours, `last_trade.price` reflects the last traded price (which may be from after-hours or the prior session) — the snapshot endpoint does not distinguish "market closed" from "stale price" in a field the free tier response surfaces; if that distinction matters later, compare `updated`/`last_trade.timestamp` against a market-calendar check.
- `prevDay` (`day.previous_close` in the client model) resets at market open; during pre-market it still reflects the prior completed session.
- Ticker symbols are case-sensitive in both the REST query string and client calls — always uppercase before sending (FinAlly does this at the watchlist validation boundary, not inside the Massive client).

## Sources

- [massive-com/client-python (GitHub)](https://github.com/massive-com/client-python)
- [massive · PyPI](https://pypi.org/project/massive/)
- [Massive REST API docs](https://massive.com/docs)
- [Full Market Snapshot](https://massive.com/docs/rest/stocks/snapshots/full-market-snapshot.md)
- [Single Ticker Snapshot](https://massive.com/docs/rest/stocks/snapshots/single-ticker-snapshot.md)
- [Previous Day Bar](https://massive.com/docs/rest/stocks/aggregates/previous-day-bar.md)
- [Custom Bars](https://massive.com/docs/rest/stocks/aggregates/custom-bars.md)
- [Massive knowledge base — request limits](https://massive.com/knowledge-base/article/what-is-the-request-limit-for-massives-restful-apis)
- [Massive pricing](https://massive.com/pricing)
