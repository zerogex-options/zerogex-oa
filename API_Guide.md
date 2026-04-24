# ZeroGEX API Endpoints Reference

Complete reference for all currently available API endpoints.

Base URL: `http://your-server:8000`

---

## Health & Status

### GET /api/health
Check API and database health.

---

## GEX (Gamma Exposure)

### GET /api/gex/summary
Get latest GEX summary with key metrics.

**Parameters:**
- `symbol` (optional): default `SPY`

### GET /api/gex/by-strike
Get GEX breakdown by individual strikes.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `200`, default `50`

### GET /api/gex/historical
Get historical GEX data.

**Parameters:**
- `symbol` (optional): default `SPY`
- `start_date` (optional): ISO format datetime/date
- `end_date` (optional): ISO format datetime/date
- `window_units` (optional): max `90`, default `90`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`

### GET /api/gex/heatmap
Get GEX heatmap matrix (strike × time).

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `5min`
- `window_units` (optional): max `90`, default `60`

---

## Options Flow

### GET /api/flow/by-type
Get option flow by type (calls vs puts) across the full selected interval (time-series rows).

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`
- `window_units` (optional): max `90`, default `60`

### GET /api/flow/by-strike
Get option flow by strike level.

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`
- `window_units` (optional): max `90`, default `60`
- `limit` (optional): max `50000`, default `1000`

### GET /api/flow/smart-money
Get unusual activity / smart money flow.

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`
- `window_units` (optional): max `90`, default `60`
- `limit` (optional): max `50000`, default `50`

---

## Market Data

### GET /api/market/quote
Get latest underlying quote.

**Parameters:**
- `symbol` (optional): default `SPY`

### GET /api/market/previous-close
Get previous trading day close.

**Parameters:**
- `symbol` (optional): default `SPY`

### GET /api/market/session-closes
Get the two most recently completed regular session closes (4:00 PM ET bars).

- `current_session_close` — last completed 4pm ET close. During market hours on a given day (before 4pm ET), this is the previous day's close; during after-hours or the following pre-market, it is that day's close.
- `prior_session_close` — the session close immediately before `current_session_close`.

**Parameters:**
- `symbol` (optional): default `SPY`

**Example response:**
```json
{
  "symbol": "SPY",
  "current_session_close": 676.04,
  "current_session_close_ts": "2026-03-11T21:00:00Z",
  "prior_session_close": 675.73,
  "prior_session_close_ts": "2026-03-10T21:00:00Z"
}
```

### GET /api/market/historical
Get historical underlying quotes.

**Parameters:**
- `symbol` (optional): default `SPY`
- `start_date` (optional): ISO format datetime/date
- `end_date` (optional): ISO format datetime/date
- `window_units` (optional): max `90`, default `90`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`

---

## Max Pain

### GET /api/max-pain/timeseries
Get max pain over time (aggregated by timeframe).

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `5min`
- `window_units` (optional): min `1`, max `90`, default `90`

### GET /api/max-pain/current
Get current max pain with current underlying price, difference (`max_pain - underlying_price`), and per-expiration strike payout/notional grids.

**Parameters:**
- `symbol` (optional): default `SPY`
- `strike_limit` (optional): min `10`, max `1000`, default `200`

---

## Technicals

### GET /api/technicals/vwap-deviation
Get VWAP deviation for mean reversion monitoring.

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`
- `window_units` (optional): max `90`, default `20`

### GET /api/technicals/opening-range
Get opening range breakout status.

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`
- `window_units` (optional): max `90`, default `20`

### GET /api/technicals/gamma-levels
Get gamma exposure levels (support/resistance zones).

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `100`, default `20`

### GET /api/technicals/dealer-hedging
Get dealer hedging pressure signals.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `100`, default `20`

### GET /api/technicals/volume-spikes
Get unusual volume spike events.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `100`, default `20`

### GET /api/technicals/momentum-divergence
Get momentum divergence signals.

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`
- `window_units` (optional): max `90`, default `20`

---

## Signals

Signal endpoints surface the Market State Index composite, Advanced Signals
(triggered events with hysteresis), and Basic Signals (continuous
directional reads). Full per-endpoint field semantics, ranges, trader
interpretation, and page-design notes live in Swagger (`/docs`) — this
section is a path quick-reference.

Endpoints below are listed in alphabetical order — matching the Swagger
UI at `/docs`, which uses `operationsSorter: "alpha"` for the same
purpose.

### Composite & trades

- `GET /api/signals/score` — latest MSI composite score, regime label, component breakdown.
- `GET /api/signals/score-history` — time series of composite scores + contributions.
- `GET /api/signals/trades-history` — realized trade ideas with P&L / hit rate.
- `GET /api/signals/trades-live` — open trade ideas derived from current signal state.

### Advanced Signals (7, triggered + hysteresis)

- `GET /api/signals/advanced/0dte-position-imbalance`
- `GET /api/signals/advanced/confluence-matrix` — N×N pairwise agreement over rolling lookback.
- `GET /api/signals/advanced/eod-pressure`
- `GET /api/signals/advanced/gamma-vwap-confluence`
- `GET /api/signals/advanced/range-break-imminence` — regime-switch (chop vs break) detector; emits `imminence` 0–100 and `label` (Range Fade / Weak Range / Break Watch / Breakout Mode).
- `GET /api/signals/advanced/squeeze-setup`
- `GET /api/signals/advanced/trap-detection`
- `GET /api/signals/advanced/vol-expansion`

### Basic Signals (6, continuous directional reads, weight=0)

- `GET /api/signals/basic` — bundle: latest snapshot of all six in one response.
- `GET /api/signals/basic/confluence-matrix` — 6×6 pairwise agreement over rolling lookback.
- `GET /api/signals/basic/dealer-delta-pressure` — estimated dealer net-delta imbalance (DNI).
- `GET /api/signals/basic/gex-gradient` — dealer gamma asymmetry above vs below spot.
- `GET /api/signals/basic/positioning-trap` — squeeze/flush risk from one-way crowding.
- `GET /api/signals/basic/skew-delta` — short-dated OTM put-vs-call IV deviation (fear gauge).
- `GET /api/signals/basic/tape-flow-bias` — signed option-tape premium imbalance.
- `GET /api/signals/basic/vanna-charm-flow` — second-order greek dealer-hedging pressure.

### Cross-cutting

- `GET /api/signals/{signal_name}/events` — per-signal time-series with direction-flip
  detection and forward realized returns. Accepts any of the 13 advanced/basic names.

**Common response shape (per-signal):**
- `underlying`, `timestamp` (ISO-8601 UTC).
- `clamped_score` ∈ `[-1, +1]`; `score` = `clamped_score × 100` ∈ `[-100, +100]`.
- `direction` ∈ `"bullish" | "bearish" | "neutral"`.
- `context_values` — signal-specific inputs/derived fields.
- `score_history` — up to 90 recent `{score, timestamp}` points, oldest→newest.

Returns `404` when a signal has no row yet for the symbol. Weight is `0.0`
for all Advanced and Basic Signals (they do not contribute to the MSI).

---

## Interactive API Docs

### GET /docs
Swagger UI.

### GET /redoc
ReDoc UI.

### GET /openapi.json
OpenAPI schema JSON.
