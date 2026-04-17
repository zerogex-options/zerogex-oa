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

### GET /api/technicals/eod-pressure
Latest end-of-day pressure score, combining charm-at-spot flow, gamma-gated pin gravity, and a calendar amplifier (OpEx / quad-witching). Gated off before 14:30 ET; returns `score == 0` with `time_ramp == 0` outside the window.

**Parameters:**
- `symbol` (optional): default `SPY`

**Response (top-level fields):**
- `score` (-100..+100): composite EOD pressure (positive = bullish close)
- `direction`: `"bullish"` / `"bearish"` / `"neutral"`
- `charm_at_spot`: signed charm aggregate within ±1% of spot
- `pin_target`: heavy-GEX strike (max_pain fallback)
- `pin_distance_pct`: `(pin - spot) / spot`
- `gamma_regime`: `"positive"` / `"negative"`
- `time_ramp` (0..1): time-to-close scale
- `calendar_flags`: `{opex, quad_witching}`

---

## Interactive API Docs

### GET /docs
Swagger UI.

### GET /redoc
ReDoc UI.

### GET /openapi.json
OpenAPI schema JSON.
