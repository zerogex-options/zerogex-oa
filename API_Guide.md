# ZeroGEX API Endpoints Reference

Complete reference for all currently available API endpoints.

Base URL: `http://your-server:8000`

---

## Authentication

**Every caller must send its own key.** Use the Bearer scheme on every
request:

```
Authorization: Bearer <your-key>
```

`X-API-Key: <your-key>` is also accepted (read directly from request
headers) for backward compatibility with callers that haven't migrated
to Bearer yet. New integrations should use Bearer — it is the only
scheme advertised in the OpenAPI spec, the only one shown in Swagger's
Authorize modal, and the only one not subject to reverse-proxy header
rewrites at any layer.

Requests with an invalid or missing key return `401 Unauthorized` with
`WWW-Authenticate: Bearer`.

Two key types are supported, validated against the same headers:

- **Per-user keys** *(primary)* — long-lived keys issued via the admin
  CLI and stored hashed (SHA-256) in the `api_keys` table. Each request
  authenticates as a specific `user_id`, and individual keys can be
  revoked without affecting others. Every human or integration that
  hits the API directly should have its own key. The website's
  Next.js server holds its own key (`user_id=zerogex-web`) and sends
  it on every API call.
- **Shared static key** *(break-glass)* — set via the `API_KEY` env
  var on the server. No per-user attribution. Every successful match
  is logged at WARNING with the caller's IP so stragglers can be
  identified. Kept only for ops emergencies and bootstrap; will be
  removed from `.env` once every caller has its own per-user key.

### Swagger UI

Open `https://api.zerogex.io/docs`, click **Authorize** in the top right,
paste your per-user key into the `HTTPBearer` field, click Authorize,
then "Try it out" any endpoint. The key is sent on every subsequent
request from that browser tab.

### Provisioning per-user keys

Run the admin CLI from the server (uses the same DB credentials as the
API). The raw key is printed exactly once — copy it then.

```bash
# Easiest: via Make
make api-keys-create USER=alice@example.com NAME=alice-laptop
make api-keys-list
make api-keys-list USER=alice@example.com
make api-keys-revoke ID=7

# Or directly
python -m src.api.admin_keys create alice@example.com --name "alice-laptop"
python -m src.api.admin_keys list [--user-id alice@example.com]
python -m src.api.admin_keys revoke 7
```

Revocations take effect within the cache TTL (default 60s, controlled by
`API_KEY_CACHE_TTL_SECONDS`). Restart the API to invalidate immediately.

When neither `API_KEY` is set nor any keys exist in the `api_keys` table,
authentication is disabled — appropriate only for local development/CI.

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

### GET /api/technicals
Combined per 5-minute bar timeseries of VWAP deviation, opening-range
breakout, unusual volume spikes (all classifications), and momentum
divergence — plus the underlying close — for the most recent session.

Session window depends on `symbols.asset_type`:
- `INDEX` → 09:30–16:00 ET (cash session only)
- otherwise (ETF, EQUITY) → 04:00–20:00 ET (extended hours)

Each bar is a 5-minute bucket; `timestamp` is the start of the bucket
(e.g. `10:30` → `10:30:00–10:34:59`). The bar aggregates whichever
1-minute underlying bars have landed in the bucket: `close` is the
latest 1-minute close, volumes are summed, `high`/`low` use max/min.
While the 5-minute window is still active the bar updates as new
1-minute bars arrive; once the window closes the bar becomes
immutable.

Cash indices use a proxy ETF's volume for VWAP and volume-spike stats
(SPX→SPY, NDX→QQQ, RUT→IWM, DJX→DIA); the active proxy is reported in
the response's `volume_proxy` field. Bars before 09:30 ET return null
opening-range fields (the ORB hasn't been established yet).

Dealer hedging is intentionally excluded — its underlying view is a
point-in-time snapshot, not a timeseries.

**Parameters:**
- `symbol` (optional): default `SPY`
- `intervals` (optional): trailing N 5-minute bars (1–192, where 192
  bars × 5 min = 16h covers the full extended ETF session). Omit for
  the full session. Tail anchors on the most recent existing bar —
  safe for live mid-session polling.

**Response shape:**
```json
{
  "symbol": "SPY",
  "asset_type": "ETF",
  "session_date": "2026-05-08",
  "session_start_et": "2026-05-08T04:00:00-04:00",
  "session_end_et": "2026-05-08T20:00:00-04:00",
  "volume_proxy": null,
  "bars": [
    {
      "time_et": "2026-05-08T04:00:00-04:00",
      "timestamp": "2026-05-08T08:00:00+00:00",
      "close": 737.62,
      "volume": 12500,
      "vwap_deviation": { "vwap": ..., "vwap_deviation_pct": ..., "vwap_position": ... },
      "opening_range": { "orb_high": null, "orb_low": null, ... },
      "volume_spike": { "current_volume": ..., "volume_sigma": ..., "volume_class": ... },
      "momentum_divergence": { "chg_5m": ..., "opt_flow": ..., "divergence_signal": ... }
    }
  ]
}
```

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
Get current dealer hedging pressure (point-in-time snapshot).
Returns at most one row per symbol — this is not a timeseries.

**Parameters:**
- `symbol` (optional): default `SPY`

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
- `GET /api/signals/action` — Playbook Engine Action Card: single decisive trade
  instruction (or `STAND_DOWN`) fusing MSI regime + advanced/basic signals + live
  levels. See `docs/playbook_catalog.md` for the pattern catalog and Action Card
  schema. PR-2 ships the engine plus one canonical pattern (`call_wall_fade`);
  remaining patterns land in PR-3+.
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
- `score_history` — up to 90 recent `{score, timestamp}` points, newest→oldest.

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
