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

### End-user attribution (website-proxied requests)

The website's Next.js server calls this API with a single shared per-user
key (`user_id=zerogex-web`) on behalf of every logged-in human, so the
caller identity alone can't say *which* end-user a request is for. An
optional second factor closes that gap: the website mints a short-lived
signed token naming the end-user and sends it in an extra header
**in addition to** (never replacing) the existing `Authorization: Bearer`:

```
X-End-User-Token: <JWT>
```

**Token contract (the verifier is strict):**

- Standard JWT, **`alg=HS256` only**. Any other algorithm — including
  `none` — is rejected (algorithm-confusion guard).
- Header: `{"alg":"HS256","typ":"JWT"}`.
- Claims: `sub` (required) — the website's stable, opaque internal
  account id, non-empty after trimming, ≤ 256 chars (it lands in audit
  logs, so it must not be email/PII); `exp` (required, epoch seconds —
  a missing `exp` is rejected); `iat` (recommended, epoch seconds).
- Signature: HMAC-SHA256 over `base64url(header).base64url(payload)`
  using the raw secret string's UTF-8 bytes as the key (the secret is
  **not** base64-decoded first), base64url without padding — exactly
  what any standard JWT library produces by default.

**Secret & tunables (server env):**

- `END_USER_TOKEN_SECRET` — shared high-entropy string, byte-identical
  to the website's `ZEROGEX_END_USER_TOKEN_SECRET`. **Unset ⇒ attribution
  disabled ⇒ callers authenticate exactly as before.** Generate with
  `python3 -c 'import secrets; print(secrets.token_urlsafe(48))'`.
- `END_USER_TOKEN_LEEWAY_SECONDS` (default 60) — clock-skew tolerance.
- `END_USER_TOKEN_MAX_AGE_SECONDS` (default 900) — the honored lifetime
  is hard-capped at this many seconds from `iat`, regardless of `exp`.

**Fail-open / purely additive.** Verification is pure crypto: it never
touches the DB and never raises into the request path. No token, no
secret configured, or any invalid/expired/forged token simply means
"no end-user" — the request still authenticates as the caller and
returns its normal `200`. A bad token never turns a `200` into a `4xx`.

**Consuming the identity.** Handlers can depend on the resolved identity:

```python
from src.api.identity import RequestIdentity, current_identity
from fastapi import Depends

@app.get("/api/example")
async def example(identity: RequestIdentity = Depends(current_identity)):
    # identity.caller_kind in {"static","db","anonymous"}
    # identity.caller_user_id, identity.end_user_id, identity.end_user_source
    # identity.subject -> "end_user:<id>" | "caller:<id>" | "caller_kind:<k>"
    ...
```

Every request also emits one structured line on the `src.api.audit`
logger: `api_request method=… path=… status=… caller_kind=…
caller_user_id=… end_user_id=… duration_ms=…`.

**Identity-keyed rate limiting.** A global dependency (`src.api.ratelimit`)
can throttle per end-user (falling back to caller, then client IP — see
`rate_limit_key`). It is **off** by default; `END_USER_RATE_LIMIT_ENABLED=1`
turns on **log-only** mode (counts and logs `WOULD-BLOCK`, never rejects),
and additionally `END_USER_RATE_LIMIT_ENFORCE=1` returns `429` with
`Retry-After` over the limit (`END_USER_RATE_LIMIT_REQUESTS`,
`END_USER_RATE_LIMIT_WINDOW_SECONDS`). The counter is an in-memory
fixed-window map, so the limit is **per worker** — adequate for a
smoke/observability rollout; the multi-worker scale-out path is
`slowapi` + Redis with the same `rate_limit_key` derivation ported
unchanged.

---

## Scopes, tiers & what's redistributable

Every endpoint declares a capability **scope** (`src/api/scopes.py`); each
key is provisioned with a **tier** bundle that grants a set of scopes.
Enforcement is opt-in (`API_SCOPE_ENFORCEMENT`) and a wildcard `*` key
always passes, so these declarations are inert until keys are backfilled.

| Scope | Covers | Redistributable? |
| --- | --- | --- |
| `gex` | GEX summary / by-strike / profile, walls, flip term-structure & surface, vol & premium surface, replay, and **`/api/v1/levels`** | ✅ derived |
| `flow` | options-flow aggregates, forced flow | ✅ derived |
| `maxpain` | max-pain analytics | ✅ derived |
| `technicals` | VWAP / ORB / volume / momentum | ✅ derived |
| `signals` | signal engine, backtest, scorecard, forecast, TradeWorkz | ✅ derived (premium) |
| `market_raw` | raw per-contract quotes & underlying OHLC (`/api/market/*`, `/api/option/*`) | ❌ **withheld** |

Tier bundles (the unit of commercial packaging):

- **`analytics`** — `gex` + `flow` + `maxpain` + `technicals`: the clean
  derived product for external / B2B2C consumers. **No raw data.**
- **`signals`** — `analytics` + `signals`.
- **`full`** — everything *including* `market_raw`; the internal website
  backend only, never resold.

`market_raw` is isolated precisely so it can be granted to the internal
BFF and **withheld from every external customer** — the derived scopes are
broadly redistributable, raw upstream market data is not. A third-party
charting integration is issued an **`analytics`-tier key**.

## Data freshness & update cadence

The derived analytics (GEX, walls, flip, max pain, per-strike profile) are
recomputed on the **~60-second analytics cycle** and served from a short
TTL cache (GEX summary ~1.5 s; by-strike / profile ~5 s). This is
**snapshot-poll, not a stream** — clients re-issue the GET at whatever
cadence they want (1–5 s is effectively free against the cache). The only
realtime push channel, `WS /ws`, carries **underlying quotes only**; it
does not carry GEX / wall / flip.

Levels and summary responses carry the snapshot time so a consumer can
reason about staleness; `/api/v1/levels` additionally returns
`age_seconds`. A future delayed-vs-live tier split (free = delayed / EOD,
paid = realtime) is a timestamp gate on these fields, not a streaming
change.

> **Authoritative source.** This guide is the curated derived/charting
> surface. The live, complete, always-current endpoint list is the
> OpenAPI schema at `/openapi.json` (rendered at `/docs`); when the two
> disagree, the schema wins.

---

## Health & Status

### GET /api/health
Check API and database health.

---

## Consolidated Levels (v1) — recommended for charting integrations

Scope: `gex` (the `analytics` tier). Derived, redistributable.

### GET /api/v1/levels/{symbol}

The stable, **versioned** contract that bundles the headline
dealer-positioning levels and the per-strike gamma profile into one
response — the surface third-party charting integrations (a TradingView
Charting Library widget, a NinjaScript indicator, embeddable partner
widgets) should build on. The field names here are a committed contract;
the internal `/api/gex/*` models can change without breaking integrations.
It is a thin re-shape of `/api/gex/summary` + an across-expiration
aggregate of `/api/gex/by-strike`, so a consumer needs one call, not two.

**Path parameters:**
- `symbol` — underlying, e.g. `SPY`, `SPX`, `QQQ` (1–16 chars, `[A-Za-z0-9.^-]`).

**Query parameters:**
- `strikes` (optional): number of strikes nearest to spot to include in the
  gamma profile, aggregated across expirations. Min `1`, max `200`,
  default `40`.

**Returns `404`** when no snapshot exists for the symbol yet.

**Example response:**
```json
{
  "symbol": "SPY",
  "spot": 676.04,
  "as_of": "2026-07-06T19:30:00Z",
  "age_seconds": 42,
  "net_gex_at_spot": -1200000000.0,
  "levels": {
    "gamma_flip": 675.0,
    "call_wall": 680.0,
    "put_wall": 670.0,
    "max_pain": 676.0
  },
  "profile": [
    {"strike": 670.0, "net_gex": -500000000.0, "call_gex": 100000000.0, "put_gex": -600000000.0},
    {"strike": 676.0, "net_gex":          0.0, "call_gex": 200000000.0, "put_gex": -200000000.0},
    {"strike": 680.0, "net_gex":  400000000.0, "call_gex": 500000000.0, "put_gex": -100000000.0}
  ]
}
```

- `levels.*` draw as horizontal lines. Any field may be `null` when the
  engine can't resolve it (e.g. an unresolved gamma flip on a thin chain)
  — hide the line, don't render a `0`.
- `profile` is ascending by strike (histogram order). `net_gex` is dollar
  gamma per 1% move, calls positive / puts negative, and
  `net_gex == call_gex + put_gex` by construction.
- `as_of` / `age_seconds` describe snapshot freshness — see *Data
  freshness & update cadence* above.

---

## GEX (Gamma Exposure)

Scope: `gex` (the `analytics` tier).

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

Scope: `flow` (the `analytics` tier).

### GET /api/flow/by-contract
Per-contract option flow in 5-minute buckets with session-cumulative values.

**Parameters:**
- `symbol` (optional): default `SPY`
- `session` (optional): `current` | `prior`, default `current`
- `intervals` (optional): trailing N 5-minute buckets, `1`–`390`; omit for the full session

### GET /api/flow/series
Server-accumulated flow series — one row per 5-minute bar (cumulative call/put premium, volume, position, net volume, put/call ratio). Rows are newest→oldest.

**Parameters:**
- `symbol` (required): `[A-Z.]{1,10}`
- `session` (optional): `current` | `prior`, default `current`
- `strikes` (optional): comma-separated strikes to include; omit for all
- `expirations` (optional): comma-separated `YYYY-MM-DD`; omit for all
- `intervals` (optional): trailing N 5-minute bars, `1`–`390`

### GET /api/flow/contracts
Distinct strikes and expirations that traded in the resolved session (powers the Flow-page filter chips).

**Parameters:**
- `symbol` (required): `[A-Z.]{1,10}`
- `session` (optional): `current` | `prior`, default `current`

### GET /api/flow/smart-money
Unusual-activity / smart-money flow — 1-minute intervals (session 07:15–16:15 ET).

**Parameters:**
- `symbol` (optional): default `SPY`
- `session` (optional): `current` | `prior`, default `current`
- `limit` (optional): max `50`, default `50`

### GET /api/flow/buying-pressure
Underlying buying/selling pressure.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): `1`–`500`, default `20`

---

## Market Data

Scope: `market_raw` — **raw upstream data, not redistributable.** These
endpoints are for the internal `full`-tier BFF only and are excluded from
the `analytics` tier issued to external customers.

### GET /api/market/quote
Get latest underlying quote (the live tick).

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

### GET /api/market/session-levels
Get pre-market and previous-session high/low levels — the chart's PM High/Low and Prev High/Low overlays.

Non-index symbols only (ETFs/equities such as SPY, QQQ). Cash indexes have no pre-market print and return `is_index: true` with null levels (HTTP 200, not 404).

- `premarket_high` / `premarket_low` — high/low of `trading_date`'s 04:00–09:30 ET pre-market session. Live-updating while the pre-market is in progress; final after the 09:30 open.
- `prev_session_high` / `prev_session_low` — high/low of `prev_session_date`'s regular session (09:30–16:00 ET; 13:00 close on NYSE half-days), including the closing auction print.
- Levels roll at the start of each new pre-market session (04:00 ET), not at the close.
- `source` — provenance: `captured` (the session-levels capture job), `live` (on-the-fly 1-min-bar aggregate fallback), or `captured+live` (pre-market union of both while the session is in progress).

**Parameters:**
- `symbol` (optional): default `SPY`

**Example response:**
```json
{
  "symbol": "SPY",
  "is_index": false,
  "trading_date": "2026-07-06",
  "premarket_high": 625.4,
  "premarket_low": 622.15,
  "prev_session_date": "2026-07-03",
  "prev_session_high": 626.28,
  "prev_session_low": 621.4,
  "source": "captured",
  "updated_at": "2026-07-06T13:55:04.412331+00:00"
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

### GET /api/market/open-interest
Current open interest per option contract from the most recent chain snapshot.

**Parameters:**
- `underlying` (optional): default `SPY`

### GET /api/option/quote
Most recent quote for a single option contract.

**Parameters:**
- `underlying` (optional): default `SPY`
- `strike` (optional): strike price
- `expiration` (optional): `YYYY-MM-DD`
- `type` (optional): `C` (call) or `P` (put)

---

## Max Pain

Scope: `maxpain` (the `analytics` tier).

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

Scope: `technicals` (the `analytics` tier).

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

Scope: `signals` (the `signals` tier — premium; not in the base `analytics`
bundle).

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
