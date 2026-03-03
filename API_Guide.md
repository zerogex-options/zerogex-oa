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
- `limit` (optional): max `1000`, default `90`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`

### GET /api/gex/heatmap
Get GEX heatmap matrix (strike × time).

**Parameters:**
- `symbol` (optional): default `SPY`
- `window_minutes` (optional): max `7200`, default `60`
- `interval_minutes` (optional): max `1440`, default `5`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `5min`

---

## Options Flow

### GET /api/flow/by-type
Get option flow by type (calls vs puts).

**Parameters:**
- `symbol` (optional): default `SPY`
- `window_minutes` (optional): max `1440`, default `60`

### GET /api/flow/by-strike
Get option flow by strike level.

**Parameters:**
- `symbol` (optional): default `SPY`
- `window_minutes` (optional): max `1440`, default `60`
- `limit` (optional): max `100`, default `20`

### GET /api/flow/smart-money
Get unusual activity / smart money flow.

**Parameters:**
- `symbol` (optional): default `SPY`
- `window_minutes` (optional): max `1440`, default `60`
- `limit` (optional): max `50`, default `10`

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

### GET /api/market/historical
Get historical underlying quotes.

**Parameters:**
- `symbol` (optional): default `SPY`
- `start_date` (optional): ISO format datetime/date
- `end_date` (optional): ISO format datetime/date
- `limit` (optional): max `1000`, default `90`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`

---

## Max Pain

### GET /api/max-pain/timeseries
Get max pain over time (aggregated by timeframe).

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `5min`
- `limit` (optional): min `1`, max `500`, default `90`

### GET /api/max-pain/current
Get current max pain plus strike-by-strike payout notional.

**Parameters:**
- `symbol` (optional): default `SPY`
- `strike_limit` (optional): min `10`, max `1000`, default `200`

---

## Day Trading Signals

### GET /api/trading/vwap-deviation
Get VWAP deviation for mean reversion monitoring.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `100`, default `20`

### GET /api/trading/opening-range
Get opening range breakout status.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `100`, default `20`

### GET /api/trading/gamma-levels
Get gamma exposure levels (support/resistance zones).

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `100`, default `20`

### GET /api/trading/dealer-hedging
Get dealer hedging pressure signals.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `100`, default `20`

### GET /api/trading/volume-spikes
Get unusual volume spike events.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `100`, default `20`

### GET /api/trading/momentum-divergence
Get momentum divergence signals.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `100`, default `20`

---

## Interactive API Docs

### GET /docs
Swagger UI.

### GET /redoc
ReDoc UI.

### GET /openapi.json
OpenAPI schema JSON.
