# FinAlly — Comprehensive Project Review

*Reviewer pass on 2026-07-07. Scope: PLAN.md as the shared agent contract, plus the one component actually built so far — the backend market-data subsystem in `backend/app/market/`. Frontend, database, LLM chat, Docker, and scripts are not yet built.*

This review **builds on** the self-review already embedded in `PLAN.md §13` (Open Questions 1–9 and the Simplification Opportunities). It does not repeat those items except where the shipped code now answers, contradicts, or changes the priority of one. New findings are the focus.

---

## 0. Executive Summary

The market-data subsystem is well-built: clean strategy pattern, thread-safe cache, a correct GBM simulator, good test coverage (73 tests, 84%). It matches the plan's architecture faithfully. The issues below are almost all in the **contract boundaries** the *next* agents will build against — the places where PLAN.md is silent or slightly inconsistent, and where the code has quietly made a decision the plan hasn't ratified.

Top priorities, in order:

1. **"Daily change %" has no data source** (blocks watchlist + frontend). — P0
2. **SSE payload shape is undocumented in PLAN.md** and differs from the plan's per-event wording. — P0
3. **Unknown/invalid tickers behave completely differently** between simulator and Massive, and nothing validates symbols. — P1
4. **Missing repo scaffolding the plan/README already promise**: `.env.example`, `db/.gitkeep`, dotenv loading. — P1
5. **No application entrypoint yet** — the market router exists but nothing constructs the FastAPI app, lifespan, cache, or wires the source. — P1 (expected, but must be owned).

---

## 1. Market-Data Subsystem vs. PLAN.md — Correctness & Completeness

### What matches the plan well
- **Strategy/ABC pattern** (`interface.py`) with `SimulatorDataSource` and `MassiveDataSource` both writing to a shared `PriceCache` — exactly as §6 describes; downstream code is source-agnostic.
- **GBM math** (`simulator.py`) is correct: `S(t+dt) = S·exp((μ − ½σ²)dt + σ√dt·Z)`, with `dt` derived from trading-seconds-per-year. Cholesky-correlated draws implement §6's "correlated moves." Random 2–5% shock events at ~0.1%/tick match §6's "occasional events."
- **PriceCache** is genuinely thread-safe (single `Lock`, snapshot copies on read) and exposes a `version` counter that the SSE loop uses for change detection — a clean design.
- **Factory** selects source purely on `MASSIVE_API_KEY` non-empty, matching §5 behavior.
- **500ms cadence**, seed prices, per-ticker σ/μ, and the 10 default tickers all match §6/§7.

### Gaps between what the plan promises and what the code delivers

**1.1 — "Daily change %" is not derivable from anything the backend produces. (P0)**
- §2 and §10 both require the watchlist to show **daily change %**. §10 positions table needs **% change**.
- `PriceUpdate.change` / `change_percent` (`models.py`) are computed **tick-over-tick** — `previous_price` in `cache.py` is literally *the immediately preceding tick's* price. With `dt≈8.5e-8`, per-tick moves are sub-cent, so `change_percent` is a tiny, meaningless-for-display number, not a session/day delta.
- Nothing stores a **reference/open price** per ticker. The simulator seeds the cache once (previous_price == price → `direction="flat"`), then every subsequent update overwrites `previous_price` with the last tick.
- Consequence: the frontend has enough to do the **flash animation** (up/down direction is correct tick-to-tick) but **cannot compute the "daily change %"** the UI is specified to show. This will silently produce ~0.00% everywhere or force the frontend to invent its own baseline from the first SSE frame after page load (which is "change since you opened the tab," not "daily change").
- **Recommendation:** Decide and document the semantics. Cheapest option consistent with §7: treat the **seed price** (or first price of the process/session) as the day's open, store it per ticker, and either (a) add `open_price`/`day_change_percent` to `PriceUpdate.to_dict()`, or (b) expose it via the `/api/watchlist` GET response. Pin this in PLAN.md §6 so the frontend and backend agents agree.

**1.2 — SSE wire format is undocumented and inconsistent with §6 wording. (P0)**
- §6 says: "*Each SSE event contains ticker, price, previous price, timestamp, and change direction*" — reads as one event **per ticker**.
- The code (`stream.py`) actually emits **one event containing all tickers as a JSON object**: `data: {"AAPL": {...}, "GOOGL": {...}}`, on the default (`message`) event type, gated on `version` change, every ~0.5s.
- Neither shape is wrong, but the **frontend contract must be exact**. As written, a Frontend agent reading §6 could build an `onmessage` handler expecting a single-ticker payload and break.
- Also undocumented: there is **no named event type** (frontend must use `onmessage`, not `addEventListener('price', …)`); the stream sends a `retry: 1000` directive; and it sends **no heartbeat/keepalive** when `version` is unchanged (fine while the simulator runs, but if the sim loop stalls the connection goes silent with no comment ping — proxies may time it out).
- **Recommendation:** Add an explicit "SSE payload schema" block to §6 showing the exact batched object, the field names from `to_dict()` (`ticker, price, previous_price, timestamp, change, change_percent, direction`), the event type (default), and the reconnect directive.

**1.3 — Unknown/invalid tickers diverge by source, with no validation anywhere. (P1)**
- This partially **answers PLAN §13 Open Question #1** — but reveals the answer is *inconsistent*:
  - **Simulator** (`simulator.py::_add_ticker_internal`): an unknown ticker gets a **random price `uniform(50, 300)`** and `DEFAULT_PARAMS`. It *works* but invents a fake company at a fake price.
  - **Massive** (`massive_client.py`): an unknown ticker is added to `_tickers`, the snapshot call simply never returns it, and it **stays absent from the cache forever** — `cache.get_price(ticker)` returns `None`.
- So the same `POST /api/watchlist {ticker:"ZZZZ"}` produces a live fake price under the simulator and a permanent "no price" hole under Massive.
- There is **zero symbol validation** on either path. Garbage, lowercase, empty, or duplicate-with-different-case symbols are accepted (Massive upper-cases; the simulator does **not** — `add_ticker("aapl")` becomes a *second, separate* fake ticker).
- Downstream risk: portfolio valuation and P&L will hit `None` prices for Massive-side unknowns → NaN/`TypeError` unless every consumer null-checks. The watchlist GET (§8, "with latest prices") must define what it returns when price is `None`.
- **Recommendations:**
  - Normalize ticker case in **one** place (the simulator does not upper-case; Massive does — inconsistent).
  - Decide the validation policy (§13 Simplification "one validation path" — endorse it) and state whether add is restricted to a known universe or open. If open, define the "no price yet" contract for every consumer.
  - Fix the simulator's non-upper-casing so `add_ticker` de-dupes correctly.

**1.4 — Massive API response shape is unverified (real-data path). (P2)**
- `_fetch_snapshots` calls `self._client.get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=…)` and reads `snap.last_trade.price` / `snap.last_trade.timestamp`. The summary notes `massive_client.py` is only 56% covered (API methods mocked). If the real `massive` package's model attributes differ, this fails only at runtime with a real key. It is guarded (`try/except`, no re-raise) so it degrades to "no prices" rather than crashing — but that's indistinguishable from 1.3's silent hole.
- **Recommendation:** Note in the plan that the Massive path is untested against the live API; add one integration smoke test or a documented manual verification step before relying on it in a demo.

---

## 2. Missing Scaffolding the Plan/README Already Promise (P1)

These are concrete, already-referenced artifacts that don't exist. They will bite the very next agent.

- **`.env.example` does not exist.** PLAN §4 ("`.env.example` committed") and README ("`cp .env.example .env`") both reference it. The `.gitignore` correctly ignores `.env`. Create `.env.example` with the three documented vars.
- **`db/` directory + `.gitkeep` do not exist.** §4 specifies `db/.gitkeep` in the repo and that `finally.db` is gitignored. Neither is present, and `.gitignore` only ignores `db.sqlite3*` — **`db/finally.db` is NOT currently gitignored** (the pattern doesn't match). Add `db/.gitkeep` and a `db/finally.db` (or `db/*.db`) ignore rule.
- **No dotenv loading.** `factory.py` reads `os.environ` directly, and the cerebras skill states the key "must be set in the .env file **and loaded in** as an environment variable." Nothing in the backend loads `.env`. Under Docker `--env-file` this is fine, but **local `uv run` dev and the demo won't pick up `.env`**. Either add `python-dotenv` + a single `load_dotenv()` at app startup, or document that local runs must `export`/`--env-file` manually. (litellm/pydantic are also not yet in `pyproject.toml`, but that's expected — LLM not built.)

---

## 3. No Application Entrypoint Yet (P1, expected)

There is **no `main.py`/app factory** anywhere in `backend/`. `create_stream_router` exists but nothing:
- constructs the FastAPI `app`,
- creates the singleton `PriceCache`,
- calls `create_market_data_source(cache)` and `await source.start(seed_tickers)` in a **lifespan** handler,
- mounts the stream router, the (unbuilt) REST routers, and the static Next.js export,
- implements `GET /api/health` (§8).

This is expected given only market data is done, but it's the critical integration seam. **Recommendation:** the plan should explicitly assign ownership of the app entrypoint + lifespan (who starts/stops the background task, and where the shared `PriceCache` lives) so the Backend/DB/LLM agents don't each invent their own wiring. The `interface.py` docstring says `start()` "must be called exactly once" — the lifespan owner must enforce that.

---

## 4. Status of PLAN §13's Own Open Questions (cross-check against code)

The plan already lists 9 open questions. Quick status now that code is in:

- **Q1 (dynamic tickers):** *Partially answered by code* but inconsistently — see §1.3 above. Still needs a ratified policy.
- **Q2 (trade fill price):** Still open, but the machinery exists: `cache.get_price(ticker)` at request time is the obvious server-side source of truth. Recommend stating "fill = `PriceCache` value read server-side at execution," and note the cache rounds to 2 decimals.
- **Q3 (chat history growth), Q4 (snapshot retention), Q5 (LLM error shape), Q6 (`LLM_MOCK` contract), Q7 (public-deploy cost surface), Q9 (test DB isolation):** all still fully open — no code touches these areas yet. Q5 and Q6 are the ones that will block the LLM and Test agents respectively; resolve those before those agents start.
- **Q8 (`backend/db/` vs top-level `db/` name clash):** still open and worth the rename; note that **neither directory exists yet**, so renaming now is free.
- **Charting canvas-vs-SVG** and **shared ticker-validation** simplifications: endorse both; the validation one is now more urgent given §1.3.

---

## 5. Smaller Notes / Nits

- **`change_percent` naming:** `to_dict()` emits `change_percent` but §6's field list only mentions "change direction." Minor, but align the doc's field list with the actual serialized keys.
- **SSE `version` granularity:** `version` bumps on *every* per-ticker `update()`, so with 10 tickers it increments ~10×/cycle; the 0.5s reader coalesces them into one snapshot send. Correct and efficient — just note it so no one "optimizes" the counter into a per-ticker diff expecting deltas.
- **Simulator `remove_ticker` then re-add** returns a *new random* seed price (original seed only applies to the known 10). Fine, but a UX surprise if a user removes and re-adds AAPL and it's still seeded — actually AAPL re-seeds from `SEED_PRICES`, so only *unknown* tickers get a fresh random. Worth a one-line note.
- **No market-hours / closed-market handling** in either source. Simulator runs 24/7 (fine). Massive returns stale `last_trade` off-hours — acceptable for a demo, but the "flash on change" will be quiet. Not a defect; just set expectations.
- **Skill name:** PLAN §9 says "cerebras-inference skill"; the SKILL.md frontmatter name is `cerebras-inference` — consistent. Good.

---

## 6. Prioritized Recommendations

| # | Priority | Action | Owner |
|---|---|---|---|
| 1 | P0 | Define a day-open reference price and expose `day_change_percent` (via SSE payload or `/api/watchlist`); document in §6. | Backend/Market |
| 2 | P0 | Document the exact SSE batched-object payload schema + event type + reconnect in §6. | Backend/Market + Frontend |
| 3 | P1 | Ratify unknown-ticker policy; add one shared ticker-normalize+validate function; fix simulator case-folding; define the "no price yet" contract. | Backend |
| 4 | P1 | Create `.env.example`; add `db/.gitkeep` and a `db/*.db` gitignore rule; add dotenv loading (or document manual env). | Backend/Docker |
| 5 | P1 | Assign ownership of the FastAPI app entrypoint + lifespan (single `PriceCache`, `start()`/`stop()`, `/api/health`). | Backend |
| 6 | P1 | Resolve §13 Q5 (LLM execution-result shape) and Q6 (`LLM_MOCK` contract) before LLM/Test agents start. | LLM + Test |
| 7 | P2 | Verify Massive real-API model shape or flag it as untested; add a smoke test. | Market |
| 8 | P2 | Adopt §13 simplifications: pick Lightweight Charts (canvas) definitively; rename `backend/db/` while it's still free. | Frontend / Backend |
