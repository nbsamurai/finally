# Market Data Backend — Design Document

Consolidated design reference for FinAlly's market data subsystem: the unified `MarketDataSource` interface, the GBM simulator, the Massive (Polygon.io) REST client, the shared price cache, and the SSE streaming endpoint that serves them to the frontend. This document reflects the subsystem as **implemented and tested** in `backend/app/market/` (see `MARKET_DATA_SUMMARY.md` for test/coverage stats). It consolidates and supersedes the three standalone documents `market_interface.md`, `market_simulator.md`, and `massive_api.md` — read those only if you need more narrative detail on one piece; this document is the single place with the full picture plus the integration pieces (app lifespan, downstream consumers) that don't live in any one module.

---

## 1. Goals and Constraints

From `PLAN.md` §6:

- Stream live prices to the frontend over SSE at ~500ms cadence.
- Support two interchangeable data sources selected by an environment variable: a built-in simulator (default, zero config) and a real-data poller against the Massive API (Polygon.io rebrand).
- Downstream code (SSE endpoint, portfolio valuation, trade execution) must be **agnostic** to which source is running — no `if using_massive` branches outside one factory function.
- No external dependency required to run the app out of the box — the simulator has to work with nothing but Python.

## 2. Architecture

```
                    ┌─────────────────────────────┐
                    │      MarketDataSource (ABC)  │
                    │  start / stop / add_ticker /  │
                    │  remove_ticker / get_tickers  │
                    └───────────────┬───────────────┘
                     ┌──────────────┴──────────────┐
                     │                              │
        ┌────────────▼────────────┐   ┌─────────────▼─────────────┐
        │   SimulatorDataSource    │   │      MassiveDataSource     │
        │   (GBM, 500ms ticks,     │   │  (REST poll, 15s/2-5s,     │
        │    no API key needed)    │   │   MASSIVE_API_KEY set)     │
        └────────────┬────────────┘   └─────────────┬─────────────┘
                     │        writes                 │  writes
                     └───────────────┬────────────────┘
                                     ▼
                          ┌─────────────────────┐
                          │      PriceCache      │
                          │ thread-safe, in-mem  │
                          │  single source of    │
                          │       truth          │
                          └──────────┬───────────┘
                                     │ reads
                ┌────────────────────┼────────────────────┐
                ▼                    ▼                     ▼
   GET /api/stream/prices    Trade execution        Portfolio valuation
        (SSE, 500ms poll)    (fill price lookup)    (unrealized P&L)
```

Both sources are **producers**: they push updates into `PriceCache` on their own schedule and never return prices synchronously to a caller. Everything else is a **consumer**: it reads the cache and never talks to a concrete source. This is what lets a 500ms simulator and a 15s Massive poller sit behind the identical interface — the cadence mismatch is invisible to every reader.

Selection between the two implementations happens in exactly one place — `factory.py` — driven by whether `MASSIVE_API_KEY` is set (`PLAN.md` §5).

## 3. Core Data Model — `PriceUpdate`

`backend/app/market/models.py`

```python
from dataclasses import dataclass, field
import time


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

Design notes:

- `frozen=True, slots=True` — immutable and memory-tight, since a new instance is constructed on **every tick for every ticker** (500ms × N tickers under the simulator). Slots avoid per-instance `__dict__` overhead at that allocation rate.
- `change`, `change_percent`, `direction` are **computed properties**, not stored fields — the cache never has to keep derived fields in sync with `price`/`previous_price` after construction.
- **Important semantic caveat**: `previous_price` is the *immediately preceding tick's* price, not a session/day-open reference. `change`/`change_percent` are tick-over-tick, not daily change. With the simulator's `dt ≈ 8.5e-8`, tick-over-tick moves are sub-cent — meaningful for the flash animation's up/down direction, **not** meaningful as a displayed "% change" number. See §9 (Open Design Questions) for the day-change gap; this document does not resolve it — it's flagged here so nothing downstream assumes `change_percent` is a daily figure.
- `to_dict()` is the only serialization boundary — it's what the SSE endpoint (§7) sends over the wire, field names included.

## 4. Price Cache — `PriceCache`

`backend/app/market/cache.py` — the single point of truth every producer writes to and every consumer reads from.

```python
import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time — the
    factory only ever constructs one).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # monotonically increasing, bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price  # first update: direction="flat"

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        with self._lock:
            return dict(self._prices)  # shallow copy — safe snapshot for callers

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        return self._version
```

Two details worth understanding before touching this file:

- **`version` counter** — bumped on every `update()` call (i.e. per-ticker, not per-batch). The SSE endpoint polls this integer instead of diffing price dicts; when it hasn't moved since the last check, the endpoint skips serialization entirely. This is what makes the SSE loop cheap when the Massive source is idle between 15-second polls.
- **Lock granularity** — one `Lock` for the whole dict. At FinAlly's scale (≤ tens of tickers, exactly one writer at a time, sub-millisecond critical sections) sharding locks per-ticker would add complexity for no measurable benefit. The lock exists for writer/reader safety, not writer/writer — the factory guarantees only one source is ever active.
- **Prices are rounded to 2 decimals on write**, not on read/serialize — every consumer sees the same rounded value, avoiding float-formatting drift between the cache, the SSE payload, and trade execution.

## 5. Abstract Interface — `MarketDataSource`

`backend/app/market/interface.py`

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source directly for prices —
    it reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        # ... app runs ...
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        # ... app shutting down ...
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing price updates for the given tickers.
        Must be called exactly once. Calling start() twice is undefined behavior."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources. Safe to call multiple times."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set. Also removes it from the PriceCache."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

Deliberately five lifecycle/set-membership methods and **no `get_price()`** — reading is the cache's job (§4), not the source's. This keeps the interface small enough that adding a third implementation later (e.g. a recorded-replay source for deterministic E2E tests) requires implementing exactly these five methods and nothing else.

## 6. Factory — Environment-Driven Selection

`backend/app/market/factory.py` — the **only** place in the codebase that reads `MASSIVE_API_KEY`.

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Create the appropriate market data source based on environment variables.

    - MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    - Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers).
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        logger.info("Market data source: GBM Simulator")
        return SimulatorDataSource(price_cache=price_cache)
```

Every other module — including the FastAPI app entrypoint, tests, and the demo script — is written against `MarketDataSource`, never against `SimulatorDataSource` or `MassiveDataSource` directly. This is what makes the strategy pattern actually pay off: swapping implementations never touches more than this one function.

## 7. Simulator — Geometric Brownian Motion

`backend/app/market/simulator.py` + `seed_prices.py`. Used when `MASSIVE_API_KEY` is unset (the default, per `PLAN.md` §5).

### 7.1 The GBM update equation

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

- `S(t)` — current price
- `mu` — annualized drift (expected return), e.g. `0.05` for 5%/year
- `sigma` — annualized volatility, e.g. `0.20` for 20%/year
- `dt` — time step as a fraction of a trading year
- `Z` — a (possibly correlated) draw from the standard normal distribution

GBM is the standard stochastic process behind Black-Scholes: it can never produce a negative price (the update is multiplicative through `exp()`), and it produces the lognormal-return distribution real equities approximately exhibit.

**Time step sizing** is derived from real trading-calendar constants, not picked arbitrarily:

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800 (252 trading days x 6.5h x 3600s)
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR    # ~8.48e-8, for 500ms ticks
```

This calibration is why compounded ticks look like realistic intraday noise (sub-cent moves most ticks, occasional larger swings) rather than a jumpy random walk — `dt` reflects the true ratio of one 500ms tick to a full trading year.

### 7.2 Correlated moves via Cholesky decomposition

Real equities don't move independently — a tech-sector rally lifts the whole basket. FinAlly reproduces this by Cholesky-decomposing a sector correlation matrix and applying it to independent normal draws every tick:

```
Given correlation matrix C:  L = cholesky(C)
Independent draws:            Z_independent ~ N(0,1)^n
Correlated draws:             Z_correlated = L @ Z_independent
```

Cholesky decomposition is guaranteed to succeed whenever the input matrix is a valid correlation matrix (values in `[-1, 1]`, 1s on the diagonal, symmetric) — which is always true here since the matrix is built entirely from the fixed constants below.

```python
# seed_prices.py
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR = 0.6      # tech stocks move together
INTRA_FINANCE_CORR = 0.5   # finance stocks move together
CROSS_GROUP_CORR = 0.3     # between sectors, or either ticker unknown
TSLA_CORR = 0.3            # TSLA is carved out — correlates weakly with everything
```

```python
@staticmethod
def _pairwise_correlation(t1: str, t2: str) -> float:
    tech = CORRELATION_GROUPS["tech"]
    finance = CORRELATION_GROUPS["finance"]

    if t1 == "TSLA" or t2 == "TSLA":
        return TSLA_CORR
    if t1 in tech and t2 in tech:
        return INTRA_TECH_CORR
    if t1 in finance and t2 in finance:
        return INTRA_FINANCE_CORR
    return CROSS_GROUP_CORR
```

TSLA is checked *before* the tech-group test even though it's economically a tech name — it's deliberately carved out so it never inherits the 0.6 intra-tech correlation, giving the demo a ticker that visibly diverges from the pack.

### 7.3 Random shock events

Independent of the GBM step, each ticker has a small per-tick chance of a sudden 2–5% jump:

```python
if random.random() < event_probability:  # default 0.001 (0.1%) per tick per ticker
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_sign = random.choice([-1, 1])
    price *= 1 + shock_magnitude * shock_sign
```

At 2 ticks/sec across 10 tickers, expected frequency is roughly one visible event across the whole watchlist every ~50 seconds — enough drama for a demo without every ticker jumping constantly.

### 7.4 Seed prices and per-ticker parameters

`seed_prices.py` is intentionally logic-free — pure constant lookup tables — so tuning the market's "personality" never touches `simulator.py`:

```python
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00, "TSLA": 250.00,
    "NVDA": 800.00, "META": 500.00, "JPM": 195.00, "V": 280.00, "NFLX": 600.00,
}

TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},  # high volatility
    "NVDA":  {"sigma": 0.40, "mu": 0.08},  # high volatility, strong upward drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},  # low volatility (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},  # low volatility (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}  # for tickers outside the seed set
```

**Runtime-added tickers** (a user or the LLM adds something outside the default 10, per `PLAN.md` §8/§9) fall back to `DEFAULT_PARAMS` and a random seed in `$50–$300`:

```python
self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))
```

This is a deliberate simplification: the simulator never rejects an unknown ticker, it just gives it "average" market behavior rather than inventing per-ticker realism. Any stricter validation belongs at the watchlist API boundary (§8 in `PLAN.md`), not here.

### 7.5 `GBMSimulator` — the stepping engine

```python
import math
import random

import numpy as np

from .seed_prices import DEFAULT_PARAMS, SEED_PRICES, TICKER_PARAMS, CORRELATION_GROUPS, \
    INTRA_TECH_CORR, INTRA_FINANCE_CORR, CROSS_GROUP_CORR, TSLA_CORR


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR

    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT, event_probability: float = 0.001):
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.
        Hot path — called every 500ms. Keep it fast."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu, sigma = params["mu"], params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Rebuilds the correlation matrix — O(n^2) to build + O(n^3) to decompose,
        but n stays well under 50, so this is unmeasurable in practice."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add without rebuilding Cholesky — used for batch init in __init__."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]
        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

Note the split between `_add_ticker_internal` (no Cholesky rebuild) and public `add_ticker` (rebuilds): `__init__` batches every starting ticker through the internal method and rebuilds Cholesky exactly once at the end, rather than once per ticker during construction. Correlation only needs recomputing when the *set* of tracked tickers changes — `step()` always reuses the cached matrix, keeping the hot path to one matrix-vector multiply plus per-ticker scalar math.

### 7.6 `SimulatorDataSource` — the `MarketDataSource` wrapper

```python
import asyncio
import logging

from .cache import PriceCache
from .interface import MarketDataSource
from .simulator import GBMSimulator  # class defined above, same module

logger = logging.getLogger(__name__)


class SimulatorDataSource(MarketDataSource):
    """Runs a background asyncio task that calls GBMSimulator.step() every
    `update_interval` seconds and writes results to the PriceCache."""

    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5, event_probability: float = 0.001):
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        for ticker in tickers:  # seed the cache before the first scheduled tick
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)  # seed immediately

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")
            await asyncio.sleep(self._interval)
```

`_run_loop` wraps each step in `try/except Exception` so one bad tick logs and skips rather than killing the background task permanently — the loop always re-arms via `asyncio.sleep` regardless of whether the step succeeded.

**Seed-before-first-tick** is the pattern to notice: `start()` writes every ticker's seed price into the cache *synchronously*, before scheduling the periodic task. Without this, a client connecting to the SSE stream in the window between `start()` returning and the first tick firing (up to 500ms later) would see an empty cache.

## 8. Massive API — Real Market Data

`backend/app/market/massive_client.py`. Used when `MASSIVE_API_KEY` is set. Massive is the October 2025 rebrand of Polygon.io — same accounts, same API keys, same endpoints, new default host.

### 8.1 Client library

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key="your_key_here")  # or reads MASSIVE_API_KEY from env if omitted
```

`RESTClient` is **synchronous** — FinAlly offloads every call to a thread with `asyncio.to_thread` so it never blocks the event loop. A `WebSocketClient` exists for tick-level streaming but is intentionally unused: FinAlly's `MarketDataSource` interface is poll-shaped by design (the simulator also produces discrete ticks on a timer), and polling `get_snapshot_all()` has a decisive practical advantage — **one REST call returns snapshots for every watched ticker**, which is what keeps the free tier (5 req/min) viable regardless of watchlist size.

### 8.2 Rate limits → poll interval

| Tier | Rate limit | Poll interval used by FinAlly |
|---|---|---|
| Free | 5 requests/minute | 15s (default; well under the 1-per-12s ceiling) |
| Paid ($199+/mo) | unlimited (soft abuse threshold) | 2–5s |

### 8.3 The endpoint

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT`

Raw wire response (abbreviated camelCase, nanosecond timestamps):

```json
{
  "status": "OK",
  "tickers": [
    {
      "ticker": "AAPL",
      "day": { "o": 129.61, "h": 130.15, "l": 125.07, "c": 125.07, "v": 111237700 },
      "prevDay": { "c": 129.61 },
      "lastTrade": { "p": 125.07, "s": 100, "t": 1675190399000000000 },
      "todaysChangePerc": -3.50
    }
  ]
}
```

The Python client normalizes this to typed, snake_case models with **millisecond** timestamps (`snap.last_trade.timestamp`), not the raw nanoseconds:

```python
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
)

for snap in snapshots:
    print(f"{snap.ticker}: ${snap.last_trade.price}")
```

`PriceCache.update()` expects Unix **seconds**, so the poller always divides the client's millisecond timestamp by 1000.

### 8.4 `MassiveDataSource`

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.
    """

    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)

        await self._poll_once()  # immediate first poll so the cache isn't empty

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval", len(tickers), self._interval)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval. First poll already happened in start()."""
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            # RESTClient is synchronous — run in a thread so it doesn't block the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    timestamp = snap.last_trade.timestamp / 1000.0  # ms -> s
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s", getattr(snap, "ticker", "???"), e)
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Swallow and retry next interval — see error handling table below.

    def _fetch_snapshots(self) -> list:
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

`add_ticker`/`remove_ticker` just mutate `self._tickers` — there is no separate subscribe/unsubscribe call, because the *whole set* is re-fetched every poll cycle regardless of what changed. This is simpler than incremental subscription management and costs nothing extra since the endpoint is already "all tickers in one call."

### 8.5 Error handling

The poller wraps each cycle in a broad `try/except` so a single failed cycle never crashes the background task or blocks the SSE stream — stale cache data keeps being served until the next successful poll.

| Status | Meaning | Handling |
|---|---|---|
| 401 | Invalid API key | Logged; retried next cycle (keeps failing until the key is fixed) |
| 403 | Plan doesn't include this endpoint | Logged; retried next cycle |
| 429 | Rate limit exceeded | Logged; the 15s default interval is sized to avoid this in steady state |
| 5xx | Server error | Logged; retried next cycle |
| Per-snapshot `AttributeError`/`TypeError` | Unexpected shape for one ticker in an otherwise-good response | That ticker is skipped (`logger.warning`); the rest of the batch still updates the cache |

### 8.6 Gotchas worth remembering

- The all-tickers snapshot endpoint returning **every requested ticker in one call** is load-bearing for staying under the free tier's 5 req/min — never refactor this into a per-ticker loop.
- Timestamp units differ at three layers: raw REST wire = nanoseconds, Python client model = milliseconds, `PriceCache` = seconds. Always divide the client's `timestamp` field by 1000.
- Ticker symbols are case-sensitive to the API — always uppercase before sending. `MassiveDataSource.add_ticker`/`remove_ticker` do this; the simulator (§7) currently does **not** uppercase, which is a known inconsistency (§9).
- Outside market hours, `last_trade.price` is simply the last traded price (possibly from the prior session) — the snapshot response doesn't flag "market closed" vs. "stale" on the free tier.

## 9. Unknown/Invalid Tickers — Current Behavior (Known Gap)

This isn't fully resolved by the code and is called out explicitly so the Backend agent building the watchlist API doesn't get surprised:

| | Simulator | Massive |
|---|---|---|
| Unknown ticker added | Gets a **random price** `uniform(50, 300)` and `DEFAULT_PARAMS` — "works" but the price is fake | Added to `_tickers`; snapshot response simply omits it; `cache.get_price()` returns `None` forever |
| Case handling | Does **not** uppercase — `add_ticker("aapl")` creates a second, separate fake ticker from `AAPL` | Uppercases in `add_ticker`/`remove_ticker` |
| Validation | None | None |

Consequence: the exact same `POST /api/watchlist {"ticker": "ZZZZ"}` behaves completely differently depending on which source is active. This layer deliberately does **not** enforce ticker validity — per `market_interface.md`'s design decision, that responsibility belongs at the watchlist API boundary (`PLAN.md` §8), in one shared validation function used by both the manual REST path and the LLM's `watchlist_changes` path. Two concrete fixes worth making at that boundary layer (not in this subsystem):

1. Normalize case in exactly one place before either source ever sees a ticker.
2. Decide whether watchlist adds are restricted to a known universe or fully open; if open, define what `GET /api/watchlist` returns for a ticker with no price yet (`null`? omit the field? omit the row?).

## 10. SSE Streaming Endpoint

`backend/app/market/stream.py` — `GET /api/stream/prices`.

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Factory pattern lets us inject the PriceCache without globals."""

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"  # EventSource auto-reconnects after 1s on drop

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {ticker: update.to_dict() for ticker, update in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### 10.1 Wire format (frontend contract)

Every event is a single **batched JSON object keyed by ticker**, sent as a default (`message`) SSE event — not one event per ticker:

```
retry: 1000

data: {"AAPL": {"ticker": "AAPL", "price": 190.55, "previous_price": 190.50, "timestamp": 1751900000.123, "change": 0.05, "change_percent": 0.026, "direction": "up"}, "GOOGL": {...}, ...}

```

Frontend integration:

```javascript
const es = new EventSource("/api/stream/prices");
es.onmessage = (event) => {
  const prices = JSON.parse(event.data);  // { "AAPL": {...}, "GOOGL": {...}, ... }
  for (const [ticker, update] of Object.entries(prices)) {
    // apply flash animation based on update.direction, update the sparkline, etc.
  }
};
// No addEventListener('price', ...) — there is no named event type, use onmessage.
```

`EventSource` handles reconnection automatically; the `retry: 1000` directive tells the browser to wait 1 second before retrying after a drop.

### 10.2 Change detection via `version`

The endpoint polls `price_cache.version` every `interval` (500ms) rather than diffing dict contents. When nothing has changed since the last check (e.g., between two Massive polls up to 15 seconds apart), it sends **nothing** — no empty keepalive frame. This is efficient but means a stalled producer (simulator loop crashed, Massive poller stuck) is silent on the wire with no heartbeat ping; a proxy sitting in front of the app could time out an idle connection. Not currently mitigated — worth a periodic SSE comment-ping (`: keepalive\n\n`) if this becomes a problem in a proxied deployment.

## 11. Integration — Wiring Into the FastAPI App

No `main.py`/app factory exists yet in `backend/` (the market data subsystem is a self-contained package with no owner for the app lifespan). This section is a **design sketch**, not implemented code, for whoever builds the entrypoint — the shape below is required by the contracts already established in §§4–10.

```python
# backend/app/main.py (sketch — not yet implemented)
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .market import PriceCache, create_market_data_source, create_stream_router

DEFAULT_WATCHLIST = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "NFLX"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    price_cache = PriceCache()
    source = create_market_data_source(price_cache)
    await source.start(DEFAULT_WATCHLIST)  # or the persisted watchlist from SQLite, once DB exists

    app.state.price_cache = price_cache
    app.state.market_source = source

    yield

    await source.stop()


app = FastAPI(lifespan=lifespan)
app.include_router(create_stream_router(app.state.price_cache))
```

Key constraints this sketch must satisfy, all established above:

- `PriceCache` is a **singleton** — exactly one instance for the process lifetime, created before `source.start()` is called.
- `source.start()` is called **exactly once**, in the lifespan startup, and `source.stop()` in shutdown — `interface.py`'s docstring states calling `start()` twice is undefined behavior.
- The watchlist REST endpoints (`POST /api/watchlist`, `DELETE /api/watchlist/{ticker}`, not yet built) call `app.state.market_source.add_ticker()` / `remove_ticker()` — never reach into `SimulatorDataSource`/`MassiveDataSource` directly, so the route handlers stay source-agnostic too.

### 11.1 Downstream consumer patterns

**Trade execution** (fill price, resolves `PLAN.md` §13 Open Question 2 — fill = the cache value read server-side at the moment the trade is processed, not whatever the client last rendered):

```python
price = price_cache.get_price(ticker)
if price is None:
    raise ValueError(f"No price available for {ticker}")
# ... proceed with trade at `price` ...
```

**Portfolio valuation** (unrealized P&L per position):

```python
for position in positions:
    current_price = price_cache.get_price(position.ticker)
    if current_price is not None:
        market_value = position.quantity * current_price
        unrealized_pnl = market_value - (position.quantity * position.avg_cost)
```

Both call sites must handle `None` (ticker not yet priced — see §9) rather than assuming every position has a live price.

## 12. File Structure

```
backend/
  app/
    market/
      __init__.py         # public exports: PriceCache, PriceUpdate, MarketDataSource,
                           #   create_market_data_source, create_stream_router
      models.py            # PriceUpdate dataclass
      interface.py          # MarketDataSource ABC
      cache.py              # PriceCache
      factory.py            # create_market_data_source()
      seed_prices.py         # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, correlation constants
      simulator.py            # GBMSimulator + SimulatorDataSource
      massive_client.py        # MassiveDataSource
      stream.py                 # create_stream_router() — SSE endpoint factory
    main.py                # NOT YET BUILT — app entrypoint/lifespan, see §11
  tests/
    market/                # 73 tests, 84% coverage
      test_models.py         # 11 tests — models.py: 100%
      test_cache.py           # 13 tests — cache.py: 100%
      test_simulator.py        # 17 tests — simulator.py: 98%
      test_simulator_source.py  # 10 tests — SimulatorDataSource integration
      test_factory.py           # 7 tests — factory.py: 100%
      test_massive.py            # 13 tests — massive_client.py: 56% (API methods mocked)
  market_data_demo.py     # Rich terminal dashboard — `uv run market_data_demo.py`
```

Public import surface, from `app/market/__init__.py`:

```python
from app.market import PriceCache, PriceUpdate, MarketDataSource, create_market_data_source, create_stream_router
```

## 13. Lifecycle Summary

1. **App startup**: construct one `PriceCache`; call `create_market_data_source(price_cache)`; `await source.start(initial_tickers)` with the seeded (or persisted) watchlist.
2. **Watchlist changes** (manual REST or LLM-driven, `PLAN.md` §8/§9): `await source.add_ticker(t)` / `await source.remove_ticker(t)` — both implementations keep this cheap and idempotent.
3. **SSE streaming**: `create_stream_router(price_cache)` mounts `GET /api/stream/prices`; polls the cache every 500ms regardless of which source is feeding it, gated by the `version` counter.
4. **Trade execution**: read the fill price with `price_cache.get_price(ticker)` at the moment the request is processed.
5. **App shutdown**: `await source.stop()` cancels the background task cleanly; safe to call even if `start()` was never called or was already stopped.

## 14. Testing Strategy

Already implemented (`backend/tests/market/`, 73 tests / 84% coverage — see §12 for the per-module breakdown). Notable coverage choices:

- **`test_simulator.py`** verifies GBM math directly (drift/diffusion formula, `dt` sizing), correlation lookup (`_pairwise_correlation` for tech/finance/TSLA/cross-group), event injection probability, and add/remove-ticker Cholesky rebuilds.
- **`test_massive.py`** mocks `RESTClient`/`get_snapshot_all` entirely — the real Massive API surface is never called in CI. This keeps tests free and deterministic but means the actual response-model shape (`snap.last_trade.price` etc.) is **unverified against the live API** — flagged as a P2 gap in `REVIEW.md` §1.4. A manual smoke test with a real `MASSIVE_API_KEY` (or a recorded-response integration test) is recommended before relying on the Massive path in a live demo.
- **`test_cache.py`** covers thread-safety implicitly (single-lock correctness) and the `version` counter's monotonic-increment behavior, since the SSE endpoint's efficiency depends on it.

Run the suite:

```bash
cd backend
uv run --extra dev pytest -v
uv run --extra dev pytest --cov=app
```

## 15. Design Decisions Worth Remembering

- **Strategy pattern, not scattered feature flags** — `MASSIVE_API_KEY` is read in exactly one function (`factory.py`). Every other module, including tests, targets the abstract `MarketDataSource`.
- **Push, not pull** — sources write to the cache on their own cadence; nothing downstream drives timing. This is what lets a 500ms simulator and a 15s Massive poller share one interface without either faking the other's cadence.
- **Cache is the only synchronization point** — the two source implementations never talk to each other and never run simultaneously (factory picks exactly one), so `PriceCache`'s lock only ever has one writer at a time in practice.
- **No ticker-validity enforcement in this layer** — intentional; validation belongs at the watchlist API boundary (§9 above), not inside either data source.
- **SSE batches all tickers per event, gated by a version counter** — not one SSE event per ticker; the frontend must use `onmessage`, not a named event listener.
- **Prices never go negative** — GBM's multiplicative `exp()` update guarantees this regardless of how extreme drift/diffusion/shock terms get in a single step.

## 16. Open Items Carried Into Downstream Work

Not resolved by this subsystem — tracked here so the agents building the watchlist API, frontend, and Docker/deployment layers don't have to rediscover them (full detail in `REVIEW.md`):

1. **No day-open reference price exists anywhere in the cache.** `change`/`change_percent` on `PriceUpdate` are tick-over-tick, not daily — `PLAN.md` §2/§10 require a "daily change %" the current data model cannot produce. Needs a per-ticker "open" or "session start" price stored somewhere (cache extension or `/api/watchlist` response field) before the frontend can render it correctly.
2. **Unknown-ticker behavior diverges by source** and neither path validates or case-normalizes consistently (§9 above).
3. **No app entrypoint/lifespan exists yet** (§11 is a sketch, not code) — whoever builds `main.py` owns enforcing the "`start()` exactly once" contract and choosing where the seed/persisted watchlist comes from.
4. **Massive real-API response shape is untested** against the live service (§14) — verify before a demo that depends on it.
5. **No SSE heartbeat/keepalive** when the cache is idle (§10.2) — fine for the simulator (never idle), a possible proxy-timeout risk for Massive's up-to-15s idle windows.
