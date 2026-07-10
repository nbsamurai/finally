# Market Simulator Design

Approach and code structure for simulating realistic, correlated stock prices when no `MASSIVE_API_KEY` is configured — the default mode for most users per `PLAN.md` §6. This document reflects the simulator as actually implemented in `backend/app/market/simulator.py` and `seed_prices.py` (status: complete, tested — 17 unit tests + 10 integration tests, 98% coverage). It supersedes the design sketch in `planning/archive/MARKET_SIMULATOR.md`.

## Overview

The simulator uses **Geometric Brownian Motion (GBM)** — the standard stochastic process underlying Black-Scholes option pricing. Prices evolve continuously with random noise, can never go negative (the update is multiplicative via `exp()`), and produce the lognormal return distribution real markets approximately exhibit.

`SimulatorDataSource` (a `MarketDataSource` implementation, see `market_interface.md`) drives a `GBMSimulator` core on a 500ms asyncio loop, writing each tick's prices into the shared `PriceCache`.

## GBM Math

At each time step, a stock price evolves as:

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

Where:
- `S(t)` = current price
- `mu` = annualized drift (expected return), e.g. `0.05` (5%/year)
- `sigma` = annualized volatility, e.g. `0.20` (20%/year)
- `dt` = time step expressed as a fraction of a trading year
- `Z` = a (possibly correlated) standard normal random draw

**Time step sizing** — `dt` is derived from real trading-calendar constants, not an arbitrary small number:

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800 (252 trading days × 6.5h × 3600s)
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR    # ≈ 8.48e-8, for 500ms ticks
```

This tiny `dt` is what makes ticks look like real intraday noise (sub-cent moves most ticks) rather than jumpy — the moves compound correctly over a simulated trading day because `dt` is calibrated to the actual wall-clock-to-trading-year ratio, not a round number picked for convenience.

## Correlated Moves

Real stocks don't move independently — a rally in one large tech name tends to lift the sector. FinAlly reproduces this with a **Cholesky decomposition** of a sector-based correlation matrix, applied to independent normal draws every tick:

```
Given correlation matrix C:  L = cholesky(C)
Independent draws:            Z_independent ~ N(0,1)^n
Correlated draws:             Z_correlated = L @ Z_independent
```

Cholesky decomposition is the standard way to turn independent noise into noise with a specified covariance structure, and it's guaranteed to succeed (produce a valid `L`) whenever the input matrix is a valid (positive semi-definite) correlation matrix — which a matrix built from correlations in `[-1, 1]` with 1s on the diagonal always is here.

### Correlation groups (`seed_prices.py`)

```python
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR = 0.6      # tech stocks move together
INTRA_FINANCE_CORR = 0.5   # finance stocks move together
CROSS_GROUP_CORR = 0.3     # between sectors, or either ticker unknown
TSLA_CORR = 0.3            # TSLA is carved out — correlates weakly with everything
```

Pairwise correlation lookup (`GBMSimulator._pairwise_correlation`):

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

TSLA sits in the tech ticker universe but is deliberately checked *before* the tech-group test, so it never inherits the 0.6 intra-tech correlation — it "does its own thing," which is both realistic (TSLA's historical correlation to the broader tech basket is lower and noisier than, say, AAPL/MSFT) and good for the demo (a ticker that visibly diverges from the pack is more interesting to watch).

## Random Events

Each ticker has a small independent chance per tick of a sudden 2–5% shock, layered on top of the GBM step:

```python
if random.random() < event_probability:  # default 0.001 (0.1%) per tick per ticker
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_sign = random.choice([-1, 1])
    price *= 1 + shock_magnitude * shock_sign
```

At 2 ticks/sec (500ms interval) with 10 watched tickers, expected frequency is roughly one event across the whole watchlist every ~50 seconds — frequent enough to give the dashboard visible "drama" during a demo without every ticker visibly jumping every few seconds.

## Seed Prices and Per-Ticker Parameters

`seed_prices.py` holds three flat, constant lookup tables — deliberately kept separate from `simulator.py`'s logic so tuning the market "personality" never requires touching the stepping code.

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

DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}  # for tickers not in TICKER_PARAMS
```

**Tickers added at runtime that aren't in `SEED_PRICES`/`TICKER_PARAMS`** (e.g., a user or the LLM adds a ticker outside the default watchlist, per `PLAN.md` Open Question 1) fall back to `DEFAULT_PARAMS` for volatility/drift and a random seed price in `$50–$300`:

```python
self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))
```

This is a deliberate simplification, not an oversight: the simulator never rejects an unknown ticker, it just gives it "average" market behavior. Any stricter ticker-universe validation belongs at the watchlist API boundary, not in the simulator.

## Implementation

`backend/app/market/simulator.py` — the core stepping engine, `GBMSimulator`, and its `MarketDataSource` wrapper, `SimulatorDataSource`.

```python
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
        """Rebuilds the correlation matrix — O(n^2), but n stays well under 50."""
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
```

Note `_add_ticker_internal` (no Cholesky rebuild) vs the public `add_ticker` (rebuilds): `__init__` calls the internal version in a loop over the starting ticker list and rebuilds Cholesky exactly once at the end, rather than once per ticker during batch construction.

### `SimulatorDataSource` — the `MarketDataSource` wrapper

```python
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
        for ticker in tickers:  # seed the cache before the first tick fires
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

`_run_loop` wraps each step in `try/except Exception` so a single bad tick (e.g., a transient numpy issue) logs and skips rather than killing the background task — the loop always re-arms via `asyncio.sleep` in the `finally`-equivalent position after the try/except.

## File Structure

```
backend/
  app/
    market/
      simulator.py       # GBMSimulator + SimulatorDataSource
      seed_prices.py      # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, correlation constants — pure data, no logic
  tests/
    market/
      test_simulator.py         # 17 tests — GBM math, correlation, events, add/remove ticker — 98% coverage
      test_simulator_source.py  # 10 tests — SimulatorDataSource integration (start/stop/cache writes)
```

`seed_prices.py` is intentionally logic-free — only constant dicts and sets — so that re-tuning the market's "personality" (which tickers are volatile, which sectors correlate) never requires touching `GBMSimulator`.

## Behavior Notes

- Prices can never go negative — GBM's update is `price *= exp(...)`, and `exp()` is always strictly positive regardless of how extreme the drift/diffusion/shock terms get in a single step.
- The tiny `dt` (~8.5e-8) produces sub-cent moves per individual tick; realistic-looking intraday ranges emerge from compounding many ticks, not from any single large jump (outside of the rare random-event shocks).
- Correlation only needs recomputing (Cholesky rebuild) when the *set* of tracked tickers changes, not on every tick — `step()` reuses the cached `self._cholesky` matrix every call, keeping the hot path to one matrix-vector multiply plus per-ticker scalar math.
- Rebuilding Cholesky is O(n²) to build the correlation matrix plus O(n³) for the decomposition itself, but `n` (number of watched tickers) stays under 50 in any realistic FinAlly session, so this is unmeasurable in practice even on every add/remove.
- A demo of the simulator's live output is available via `backend/market_data_demo.py` (`uv run market_data_demo.py`) — a Rich terminal dashboard showing all watched tickers with sparklines, direction arrows, and an event log, useful for visually sanity-checking parameter changes without running the full stack.
