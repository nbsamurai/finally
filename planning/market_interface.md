# Market Data Interface Design

Unified Python interface for market data in FinAlly. Two implementations — the GBM simulator and the Massive API poller (see `massive_api.md`) — sit behind one abstract interface, `MarketDataSource`. All downstream code (SSE streaming, portfolio valuation, trade execution) is source-agnostic: it reads from a shared `PriceCache` and never knows or cares which implementation is running.

This document reflects the interface as actually implemented in `backend/app/market/` (status: complete, tested — see `MARKET_DATA_SUMMARY.md`). It supersedes the design sketch in `planning/archive/MARKET_INTERFACE.md`.

## Architecture

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Massive REST poller (when MASSIVE_API_KEY is set)
        │
        ▼
   PriceCache (thread-safe, in-memory, single source of truth)
        │
        ├──→ SSE stream endpoint (/api/stream/prices)
        ├──→ Portfolio valuation
        └──→ Trade execution (fill price)
```

Neither implementation returns prices synchronously to a caller. Both **push** updates into the shared `PriceCache` on their own schedule (500ms for the simulator, 15s/2-5s for Massive depending on tier). Everything downstream reads the cache; nothing downstream calls the data source for a price directly. This is what makes the two sources interchangeable despite wildly different update cadences and network behavior.

## Core Data Model

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

`PriceUpdate` is `frozen`/`slots` — immutable and memory-tight, since one gets constructed on every tick for every ticker (500ms × N tickers in the simulator case). `change`, `change_percent`, and `direction` are computed properties rather than stored fields, so the cache never has to keep them in sync with `price`/`previous_price`.

This is the only structure that leaves the market data layer. `to_dict()` is what the SSE endpoint serializes.

## Abstract Interface

`backend/app/market/interface.py`

```python
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

Five methods, all of them lifecycle/set-membership operations — deliberately no `get_price()` on this interface. Reading is the cache's job, not the source's.

## Price Cache

`backend/app/market/cache.py` — the single point of truth both sources write to and everything else reads from.

```python
import time
from threading import Lock
from .models import PriceUpdate

class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price  # first update: direction = "flat"

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

Two details worth calling out:

- **`version` counter** — bumped on every `update()`. The SSE endpoint polls this instead of diffing price dicts; when `version` hasn't changed since the last check, it skips serialization entirely. Cheap change detection without comparing dict contents.
- **Lock granularity** — a single `Lock` guards the whole dict. At FinAlly's scale (≤ tens of tickers, one writer at a time, sub-millisecond critical sections) this is simpler and fast enough; there's no reason to shard locks per-ticker.

## Factory Function

`backend/app/market/factory.py` — the one place that reads `MASSIVE_API_KEY` and decides which implementation to construct.

```python
import os
from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """MASSIVE_API_KEY set and non-empty → MassiveDataSource (real market data)
    Otherwise → SimulatorDataSource (GBM simulation)

    Returns an unstarted source. Caller must await source.start(tickers)."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()

    if api_key:
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    else:
        return SimulatorDataSource(price_cache=price_cache)
```

This is the entire env-variable-driven branch point described in `PLAN.md` §6. Nothing else in the codebase checks `MASSIVE_API_KEY` — every other module is written against `MarketDataSource`, never against a concrete implementation.

## Massive Implementation

`backend/app/market/massive_client.py` — full detail and API research in `massive_api.md`. Summary of the shape:

```python
class MassiveDataSource(MarketDataSource):
    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0):
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

    async def _poll_once(self) -> None:
        # RESTClient is synchronous — offload to a thread so it never blocks the event loop
        snapshots = await asyncio.to_thread(self._fetch_snapshots)
        for snap in snapshots:
            self._cache.update(
                ticker=snap.ticker,
                price=snap.last_trade.price,
                timestamp=snap.last_trade.timestamp / 1000.0,  # ms -> s
            )
```

`add_ticker`/`remove_ticker` just mutate `self._tickers`; the next poll cycle re-fetches the whole set (one call covers all tickers regardless of how many changed), so there's no separate subscribe/unsubscribe call to make.

## Simulator Implementation

`backend/app/market/simulator.py` — full GBM design in `market_simulator.md`. Summary of the shape:

```python
class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5, event_probability: float = 0.001):
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        for ticker in tickers:  # seed the cache immediately, don't wait for first tick
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

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

Both `start()` methods share a pattern worth naming: **seed the cache before the first scheduled tick**. Without this, a client connecting to the SSE stream in the window between `start()` returning and the first tick firing would see an empty cache. The simulator seeds synchronously inline; Massive does it via an `await self._poll_once()` before scheduling the loop.

## SSE Integration

`backend/app/market/stream.py` reads the cache on a 500ms poll using the version counter to avoid redundant serialization:

```python
async def _generate_events(price_cache: PriceCache, request: Request, interval: float = 0.5):
    yield "retry: 1000\n\n"  # tells EventSource to reconnect after 1s on drop
    last_version = -1
    while True:
        if await request.is_disconnected():
            break
        current_version = price_cache.version
        if current_version != last_version:
            last_version = current_version
            prices = price_cache.get_all()
            if prices:
                data = {ticker: update.to_dict() for ticker, update in prices.items()}
                yield f"data: {json.dumps(data)}\n\n"
        await asyncio.sleep(interval)
```

This is why the version counter exists: without it, every 500ms tick would re-serialize and re-send the full price dict even when nothing changed (e.g., between Massive polls, where the cache is static for up to 15 seconds at a time). With it, the endpoint sends nothing until `PriceCache.update()` has actually been called since the last check.

## File Structure

```
backend/
  app/
    market/
      __init__.py         # exports: PriceCache, PriceUpdate, MarketDataSource, create_market_data_source, create_stream_router
      models.py            # PriceUpdate dataclass
      interface.py          # MarketDataSource ABC
      cache.py              # PriceCache
      factory.py            # create_market_data_source()
      massive_client.py     # MassiveDataSource (see massive_api.md)
      simulator.py           # GBMSimulator + SimulatorDataSource (see market_simulator.md)
      seed_prices.py         # SEED_PRICES, TICKER_PARAMS, correlation constants
      stream.py              # create_stream_router() — FastAPI SSE endpoint factory
  tests/
    market/                 # 73 tests, 84% coverage across all modules above
```

## Lifecycle

1. **App startup**: create one `PriceCache`, call `create_market_data_source(price_cache)`, then `await source.start(initial_tickers)` (the seeded watchlist).
2. **Watchlist changes** (manual or LLM-driven, per `PLAN.md` §8/§9): call `source.add_ticker()` / `source.remove_ticker()` — both implementations keep this cheap and idempotent.
3. **SSE streaming**: `create_stream_router(price_cache)` mounts `GET /api/stream/prices`, polling `PriceCache` every 500ms regardless of which source is feeding it.
4. **Trade execution**: reads the fill price with `price_cache.get(ticker)` at the moment the trade request is processed — server-side, not whatever price the client last rendered (resolves Open Question 2 in `PLAN.md` §13).
5. **App shutdown**: `await source.stop()` cancels the background task cleanly; safe to call even if `start()` was never called or already stopped.

## Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source

cache = PriceCache()
source = create_market_data_source(cache)  # reads MASSIVE_API_KEY
await source.start(["AAPL", "GOOGL", "MSFT", ...])

update = cache.get("AAPL")          # PriceUpdate or None
price = cache.get_price("AAPL")     # float or None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")

await source.stop()
```

## Design Decisions Worth Remembering

- **Strategy pattern, not feature flags scattered through the codebase** — the only place `MASSIVE_API_KEY` is read is `factory.py`. Every other module, including tests, is written against the abstract `MarketDataSource` interface.
- **Push, not pull** — sources write to the cache on their own cadence; nothing downstream drives the timing. This is what lets a 500ms simulator and a 15s Massive poller share one interface without either implementation faking the other's cadence.
- **Cache is the only synchronization point** — the two source implementations never talk to each other and never run simultaneously (the factory picks exactly one), so `PriceCache`'s lock only ever has one writer at a time in practice; it's there for writer/reader safety, not writer/writer.
- **No ticker-validity enforcement in this layer** — `add_ticker`/`remove_ticker` accept anything; the simulator falls back to `DEFAULT_PARAMS` and a random seed price for unknown tickers (see `market_simulator.md`), and Massive will simply get an empty/error snapshot back for a bad symbol on the next poll. Ticker validation, if any is added, belongs at the watchlist API boundary (§8 in `PLAN.md`), not here — this addresses Open Question 1 in `PLAN.md` §13 for the simulator side; the Massive side has no such restriction since real tickers are whatever the exchange lists.
