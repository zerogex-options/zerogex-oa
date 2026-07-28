# Endpoint Inventory — T0 Baseline

Snapshot of every HTTP route exposed by `zerogex-oa` as of `release@bd80bdd`.
This is the ground truth that ADR-0002 (envelope) and ADR-0003 (versioning)
migrate away from.

**Total count:** ~100 routes across 9 tag groups + admin + websocket.

---

## Conventions used in this document

| Column | Meaning |
|---|---|
| **Method / Path** | HTTP verb and route pattern as registered with FastAPI |
| **Scope** | Auth scope enforced (see `src/api/scopes.py`); `—` means no scope guard |
| **Response** | Pydantic response model or `raw dict` / `raw list` when no `response_model=` set |
| **Freshness handling** | How the endpoint currently exposes data currency (as of T0). "Implicit" = payload has a timestamp but no age/session/status labels. "None" = no time signal at all. |
| **File:line** | Source location of the handler |

The **Freshness handling** column is the T0 gap analysis for Tier 2 migration —
every "Implicit" and "None" row needs the envelope from ADR-0002.

---

## Health

| Method / Path | Scope | Response | Freshness handling | File:line |
|---|---|---|---|---|
| `GET /api/health` | — | `HealthStatus` | Partial: `last_data_update` + `data_age_seconds` present, but no session state, no per-payload `carry_forward`, no `stale_after`. HTTP 503 on degraded. | `src/api/main.py:470` |

**Migration note.** `HealthStatus` is the closest existing thing to a freshness
envelope in the codebase. It has the right *idea* (age in seconds, database
connectivity flag) but lacks session state and applies only to the health
endpoint. The v1 envelope generalizes this pattern to every market-sensitive
route.

---

## GEX (Gamma Exposure)

All routes carry `_scope_gex` (basic + pro tiers).

| Method / Path | Response | Freshness handling | File:line |
|---|---|---|---|
| `GET /api/gex/summary` | `GEXSummary` | Implicit (payload `timestamp` only) | `main.py:516` |
| `GET /api/gex/by-strike` | `List[GEXByStrike]` | Implicit | `main.py:526` |
| `GET /api/gex/profile` | `GEXProfile` | Implicit | `main.py:554` |
| `GET /api/gex/historical-context` | `GEXHistoricalContext` | Implicit (`timestamp` + `tracking_started_at`) | `main.py:572` |
| `GET /api/gex/historical` | `List[GEXSummary]` | Implicit (per-row `timestamp`) | `main.py:603` |
| `GET /api/gex/heatmap` | raw list | None | `main.py:633` |
| `GET /api/gex/expirations` | `List[date]` | None | `main.py:645` |
| `GET /api/gex/strike-profile-timeseries` | `List[StrikeProfileBucket]` | Implicit (per-bucket `timestamp`) | `main.py:667` |
| `GET /api/gex/flip-term-structure` | `FlipTermStructureResponse` | Implicit | `routers/gex_flip_horizon.py:249` |
| `GET /api/gex/flip-surface` | `FlipSurfaceResponse` | Implicit | `routers/gex_flip_horizon.py:413` |
| `GET /api/gex/vol_surface` | `VolSurfaceResponse` | Implicit | `routers/vol_surface.py:253` |
| `GET /api/gex/premium_surface` | `PremiumSurfaceResponse` | Implicit (Beta tag — contract may change) | `routers/premium_surface.py:153` |

**Nullable-analytics site (Johnnie's July 17 observation).**
`GEXSummary` currently exposes: `gamma_flip`, `gamma_flip_raw`, `gamma_flip_span_used`,
`flip_distance`, `local_gex`, `convexity_risk`, `max_pain`, `call_wall`, `put_wall`
as `Optional[Decimal]`. When any is `None`, the response gives the consumer no
way to distinguish "no crossing in searched range", "insufficient data",
"quality filter", or "not applicable to this symbol/timeframe". This is
exactly the case ADR-0002's `FieldStatus` reason-code addresses.

---

## Options Flow

All routes carry `_scope_flow`.

| Method / Path | Response | Freshness handling | File:line |
|---|---|---|---|
| `GET /api/flow/by-contract` | `List[FlowPoint]` | Implicit | `main.py:753` |
| `GET /api/flow/series` | `List[FlowSeriesPoint]` (via raw `JSONResponse`) | Implicit; `is_synthetic` flag distinguishes carry-forward | `main.py:899` |
| `GET /api/flow/contracts` | `FlowContractsResponse` | None | `main.py:966` |
| `GET /api/flow/smart-money` | `List[SmartMoneyFlowPoint]` | Implicit | `main.py:996` |
| `GET /api/flow/buying-pressure` | `List[FlowBuyingPressurePoint]` | Implicit | `main.py:1020` |

**Timestamp inconsistency.** `/api/flow/series` uses hand-rolled
`"%Y-%m-%dT%H:%M:%SZ"` strings; every other flow route uses Pydantic's
default `isoformat()`. Envelope migration will normalize on
RFC 3339 UTC with fractional-second precision.

**Precedent for status flagging.** `FlowSeriesPoint.is_synthetic: bool`
already flags carry-forward rows — proof the team recognizes this need.
Envelope generalizes it via `sanity_flags: [SYNTHETIC_BAR]`.

---

## Market Data

Underlying/option quote surfaces. All carry `_scope_market_raw`.

| Method / Path | Response | Freshness handling | File:line |
|---|---|---|---|
| `GET /api/market/quote` | `UnderlyingQuote` (excludes null) | **Best in class**: `session` field with values `open`/`pre-market`/`after-hours`/`closed`; index→future display swap flagged via `display_source` | `main.py:1201` |
| `GET /api/market/session-closes` | `SessionCloses` | Implicit | `main.py:1271` |
| `GET /api/market/session-levels` | `SessionLevels` | Implicit | `main.py:1292` |
| `GET /api/market/historical` | `List[UnderlyingQuote]` | Per-row `session`; futures opt-in via `allow_futures` | `main.py:1325` |
| `GET /api/market/volatility` | `VolatilityIndexResponse` (defaults to VIX; accepts `?ticker=VXN`) | Implicit | `routers/volatility_gauge.py:255` |
| `GET /api/option/quote` | `OptionQuote` | Implicit | `main.py:1380` |
| `GET /api/option/contract` | `List[OptionContractRow]` | Implicit | `routers/option_contract.py:64` |
| `GET /api/market/open-interest` | `OpenInterestResponse` | None (snapshot type; no `as_of`) | `main.py:1409` |

**`session` is the pattern to generalize.** `/api/market/quote` already
emits `session` with a rich state machine including soft-close windows
(`_SoftCloseTracker`, `has_todays_close_landed`). Envelope's
`freshness.market_session` reuses this exact vocabulary.

**⚠ `/api/market/vix` is intentionally gone.** Superseded by
`/api/market/volatility`. See `route-drift.md`. This is the pilot case for
ADR-0004's deprecation policy.

---

## Max Pain

Scope: `_scope_maxpain`.

| Method / Path | Response | Freshness handling | File:line |
|---|---|---|---|
| `GET /api/max-pain/timeseries` | `List[MaxPainTimeseriesPoint]` | Implicit | `main.py:1434` |
| `GET /api/max-pain/current` | `MaxPainCurrent` | Implicit (OI updates once daily at settlement — should carry a `data_cadence: daily`) | `main.py:1451` |

---

## Technicals

Scope: `_scope_technicals`.

| Method / Path | Response | Freshness handling | File:line |
|---|---|---|---|
| `GET /api/technicals` | raw dict | Implicit; carries `session_start_et` / `session_end_et` + `volume_proxy` | `main.py:1486` |
| `GET /api/technicals/vwap-deviation` | raw | None | `main.py:1567` |
| `GET /api/technicals/opening-range` | raw | None | `main.py:1578` |
| `GET /api/technicals/dealer-hedging` | raw | None (point-in-time snapshot) | `main.py:1589` |
| `GET /api/technicals/volume-spikes` | raw | None | `main.py:1607` |
| `GET /api/technicals/momentum-divergence` | `List[MomentumDivergencePoint]` | Implicit | `main.py:1616` |

**Most `response_model`-less endpoints are here.** Migration to the envelope
will force explicit schemas on all of these, which is a good thing.

---

## Trade Signals (`/api/signals/*`)

Scope: `_scope_signals` (basic + pro tiers). This is the "premium" surface.

| Method / Path | Response | File:line |
|---|---|---|
| `GET /api/signals/trades-history` | raw | `routers/trade_signals.py:94` |
| `GET /api/signals/trades-live` | raw | `:157` |
| `GET /api/signals/score` | raw | `:202` |
| `GET /api/signals/action` | raw | `:296` |
| `GET /api/signals/action/recent` | raw | `:392` |
| `GET /api/signals/action/{card_id}` | raw | `:443` |
| `GET /api/signals/score-history` | raw | `:465` |
| `GET /api/signals/advanced/vol-expansion` | raw | `:518` |
| `GET /api/signals/advanced/eod-pressure` | raw | `:584` |
| `GET /api/signals/advanced/squeeze-setup` | raw | `:662` |
| `GET /api/signals/advanced/trap-detection` | raw | `:731` |
| `GET /api/signals/advanced/0dte-position-imbalance` | raw | `:826` |
| `GET /api/signals/advanced/gamma-vwap-confluence` | raw | `:892` |
| `GET /api/signals/advanced/range-break-imminence` | raw | `:971` |
| `GET /api/signals/advanced/market-pressure` | raw | `:1052` |
| `GET /api/signals/basic` | raw | `:1177` |
| `GET /api/signals/basic/tape-flow-bias` | raw | `:1256` |
| `GET /api/signals/basic/skew-delta` | raw | `:1333` |
| `GET /api/signals/basic/vanna-charm-flow` | raw | `:1408` |
| `GET /api/signals/basic/dealer-delta-pressure` | raw | `:1497` |
| `GET /api/signals/basic/gex-gradient` | raw | `:1581` |
| `GET /api/signals/basic/positioning-trap` | raw | `:1673` |
| `GET /api/signals/{signal_name}/events` | raw | `:1788` |
| `GET /api/signals/advanced/confluence-matrix` | raw | `:1926` |
| `GET /api/signals/basic/confluence-matrix` | raw | `:1995` |
| `GET /api/signals/trade-bias` | raw | `routers/trade_bias.py:67` |
| `GET /api/signals/trade-bias-history` | raw | `routers/trade_bias.py:91` |

**Freshness handling: none across the entire signals surface.**
Freshness handling: none on all `raw`-returning routes.

**Score-interpretation site (Johnnie's July 17 observation).**
The composite score endpoints (`/api/signals/score`, `/api/signals/basic`,
and every `advanced/*` and `basic/*` per-component route) produce the numbers
that Johnnie saw diverge across SPX/SPY/QQQ. Envelope must attach
`score_metadata` (range, cadence, kind, component_weights, symbol_comparable)
to each of these responses.

---

## Levels (v1 pilot)

⚠ **Only router already using `/v1/` prefix.** Under `_scope_gex`.

| Method / Path | Response | File:line |
|---|---|---|
| `GET /api/v1/levels/{symbol}` | `LevelsResponse` | `routers/levels.py:113` |

**Precedent note.** This router was mounted at `/api/v1/levels` because it
was designed as the stable external contract that third-party integrations
(TradingView widget, NinjaScript indicator) bind against. That precedent
supports ADR-0003's hard-cut to `/v1/` — the concept already exists in the
codebase; we're generalizing the pattern.

---

## Forecast

Under `_scope_signals`.

| Method / Path | Response | File:line |
|---|---|---|
| `GET /api/forecast/available-dates` | raw | `routers/forecast.py:146` |
| `GET /api/forecast/{forecast_date}` | raw | `:179` |
| `GET /api/forecast` | raw | `:202` |
| `GET /api/forecast/history/recent` | raw | `:220` |
| `GET /api/forecast/stats/rolling` | raw | `:265` |

---

## Scorecard

Under `_scope_signals`.

| Method / Path | Response | File:line |
|---|---|---|
| `GET /api/scorecard/daily` | raw | `routers/scorecard.py:110` |

---

## Replay

Under `_scope_gex`.

| Method / Path | Response | File:line |
|---|---|---|
| `GET /api/replay/sessions` | raw | `routers/replay.py:120` |
| `GET /api/replay/frame` | raw | `:150` |
| `GET /api/replay/range` | raw | `:181` |
| `GET /api/replay/diff` | raw | `:247` |
| `POST /api/replay/clip` | `503` (feature-gated stub) | `:314` |

---

## Backtest (Beta)

Under `_scope_signals`. Tagged `Beta`.

| Method / Path | Response | File:line |
|---|---|---|
| `GET /api/backtest/meta` | raw | `routers/backtest.py:42` |
| `GET /api/backtest/insights/patterns` | raw | `:48` |
| `POST /api/backtest/runs` | raw (202) | `:68` |
| `GET /api/backtest/runs` | raw | `:96` |
| `GET /api/backtest/runs/{run_id}` | raw | `:106` |
| `POST /api/backtest/runs/{run_id}/share` | raw | `:116` |
| `GET /api/backtest/runs/shared/{share_token}` | raw | `:130` |
| `GET /api/backtest/runs/shared/{share_token}/equity` | raw | `:139` |
| `GET /api/backtest/runs/{run_id}/trades` | raw | `:148` |
| `GET /api/backtest/runs/{run_id}/trades.csv` | raw (text/csv) | `:190` |
| `GET /api/backtest/runs/{run_id}/equity` | raw | `:212` |
| `POST /api/backtest/configs` | raw (201) | `:238` |
| `GET /api/backtest/configs` | raw | `:261` |
| `GET /api/backtest/configs/shared/{share_token}` | raw | `:268` |
| `GET /api/backtest/configs/{config_id}` | raw | `:277` |
| `POST /api/backtest/sweeps` | raw (202) | `:342` |
| `GET /api/backtest/sweeps` | raw | `:377` |
| `GET /api/backtest/sweeps/{sweep_id}` | raw | `:387` |

**Beta contract note.** The `Beta` tag docs on `main.py:325` explicitly say
"contract and behaviour of these endpoints may change without notice."
Under ADR-0004 that verbal disclaimer becomes a machine-readable
`stability: "beta"` field in the envelope, plus a `Sunset` header if any
route is scheduled to break.

---

## Forced Flow

Under `_scope_flow`.

| Method / Path | Response | File:line |
|---|---|---|
| `GET /api/forced-flow/curve` | `CurveResponse` | `routers/forced_flow.py:290` |
| `GET /api/forced-flow/charm-decay` | `CharmDecayResponse` | `:356` |
| `GET /api/forced-flow/vanna-ladder` | `VannaLadderResponse` | `:387` |
| `GET /api/forced-flow/surface` | `SurfaceResponse` | `:425` |
| `GET /api/forced-flow/scenario` | `ScenarioResponse` | `:472` |
| `GET /api/forced-flow/levels` | `LevelsResponse` | `:517` |
| `GET /api/forced-flow/backtest` | `BacktestResponse` | `:582` |

---

## TradeWorkz

Under `_scope_signals` (with additional `require_admin` on `/admin/*` subpaths).
`POST` routes are the only substantive mutations in the entire API.

| Method / Path | File:line |
|---|---|
| `GET /api/tradeworkz/summary` | `routers/tradeworkz.py:93` |
| `GET /api/tradeworkz/bots` | `:177` |
| `GET /api/tradeworkz/leaderboard` | `:278` |
| `GET /api/tradeworkz/bots/{bot_id}` | `:344` |
| `GET /api/tradeworkz/bots/{bot_id}/trades` | `:403` |
| `GET /api/tradeworkz/bots/{bot_id}/equity-curve` | `:424` |
| `GET /api/tradeworkz/equity-curves` | `:444` |
| `GET /api/tradeworkz/bots/{bot_id}/metrics` | `:486` |
| `GET /api/tradeworkz/me/follows` | `:512` |
| `POST /api/tradeworkz/bots/{bot_id}/follow` | `:532` |
| `GET /api/tradeworkz/me/feed` | `:637` |
| `POST` (5 admin routes) | `:684, :756, :797, :820, :1176, :1664` |
| `GET` (4 admin routes) | `:849, :954, :1295, :1594` |

---

## Tools

Under `_scope_market_raw`.

| Method / Path | Response | File:line |
|---|---|---|
| `GET /api/tools/option-calculator` | `OptionCalculatorResponse` | `routers/option_calculator.py:117` |

---

## Admin

Not scope-guarded — gated by `X-Admin-Token` header via `require_admin`.
**Excluded from envelope migration** (internal-only; different contract).

| Method / Path | File:line |
|---|---|
| `POST /api/admin/api-keys/provision` | `routers/admin_api_keys.py:91` |
| `POST /api/admin/api-keys/revoke-all` | `:119` |
| `GET /api/admin/api-keys` | `:131` |
| `GET /api/admin/x-post/symbols` | `routers/admin_xpost.py:63` |
| `GET /api/admin/x-post/latest` | `:70` |
| `POST /api/admin/x-post/regenerate` | `:104` |

---

## WebSocket

| Method / Path | Location |
|---|---|
| `WS /ws` (per-symbol quote fan-out) | `routers/websockets.py`, registered via `ws_router.register(app, ...)` in `main.py:1643` |

Envelope work does not apply to the WebSocket surface directly, but the
per-message payload should carry the same `freshness` block. Deferred to
Tier 2 sub-workstream.

---

## Cross-cutting observations

### 1. Existing infrastructure that helps

The existing middleware stack already gives us most of the observability
plumbing the envelope work needs:

- **`RequestIdMiddleware`** (`middleware.py:46`) already generates a UUID4
  hex per request, sets it into `request_id_var` for structured logging,
  and echoes it back via `X-Request-Id`. ADR-0002's `meta.request_id`
  reads from the same source; no new middleware required.
- **`AuditLogMiddleware`** (`middleware.py:76`) captures duration via
  `time.perf_counter()`. Adding an `X-Server-Processing-Ms` response header
  is a ~5-line extension.
- **`handle_api_errors`** decorator (`errors.py:30`) standardizes 500s.
  A single change here — wrap the raised `HTTPException` body in the
  envelope shape — migrates every decorated endpoint's error path in one shot.
- **`get_market_session`** (`main.py:1099`) already implements the full
  RTH/pre/AH/closed state machine including soft-close windows and
  `close_data_available` gating. ADR-0002's `freshness.market_session`
  wraps this function; no re-implementation.
- **`response_model_exclude_none=True`** is used on `/api/market/quote` —
  the pattern for omitting null fields already exists.

### 2. Existing gaps the envelope closes

- **No response-level `age_seconds`** anywhere except `/api/health`. Every
  other endpoint's currency is inferred from `timestamp` by the consumer.
- **No response-level `data_status` vocabulary.** `is_synthetic` on
  `FlowSeriesPoint` is the only precedent, and it's per-row not per-response.
- **No per-field null reason codes.** `GEXSummary`'s null cluster
  (`gamma_flip` etc.) is the exact case Johnnie flagged on 2026-07-17.
- **No `X-Server-Processing-Ms` header.** Consumers can't distinguish API
  latency from downstream processing.
- **No API version discovery endpoint.** `FastAPI(version="1.0.0")` is
  visible in `/openapi.json` but not machine-readable elsewhere; there is
  no capability matrix, no changelog pointer, no build SHA.
- **No `Deprecation` / `Sunset` headers.** Precisely the gap that let
  `/api/market/vix` disappear without notice.

### 3. Timestamp field zoo (needs unification)

Across the codebase we see:
- `timestamp` (top level, `datetime`, most common)
- `time_window_start`, `time_window_end` (`OptionFlow`)
- `interval_timestamp` (`OptionFlow`)
- `bar_start`, `bar_end` (`FlowSeriesPoint`; hand-formatted `Z` suffix)
- `tracking_started_at` (`GEXHistoricalContext`)
- `session_start_et` / `session_end_et` (`/api/technicals`, string)
- `session_date` (implicit in various)

Every one of these becomes a specific, named field in the v1 envelope
(`source_ts`, `exchange_ts`, `calc_ts`, `server_ts`) with documented
semantics. Ambiguous names like `timestamp` are retired.

### 4. `response_model` coverage is uneven

Roughly a third of GET routes have no `response_model=` set and return
raw dicts or `List[dict]`. That prevents FastAPI from generating an
accurate OpenAPI schema for them, which blocks ADR-0003's spec-as-source-
of-truth. Tier 2 must add a Pydantic model to every route as it migrates.

### 5. `POST` mutations are rare and mostly internal

Only routes that mutate state: `POST /api/tradeworkz/*` (follow/unfollow,
admin actions), `POST /api/admin/*` (key provisioning, x-post regeneration),
`POST /api/backtest/{runs,configs,sweeps}` (async job submission),
`POST /api/replay/clip` (feature-gated stub).

Public-facing API is ~95% GET. Simplifies the envelope contract: focus
first on GET responses; POST responses (mostly job-id ack shapes) can
follow the same envelope but need a smaller cross-cut.

---

## What's next in T0

- [types.md](./types.md) — companion inventory of every response Pydantic
  model referenced above, with field-level analysis.
- [route-drift.md](./route-drift.md) — the specific case of
  `/api/market/vix` and any others like it.

## What's next after T0

Tier 1 uses this inventory to:
1. Pick the pilot endpoint for envelope migration (recommended:
   `/api/gex/summary` — most-called, highest ambiguity, contains the
   nullable-analytics case Johnnie flagged).
2. Generate the OpenAPI spec baseline against these routes so we can diff
   forward.
3. Wire the envelope into the shared response layer once, so Tier 2's
   sweep can be mostly mechanical.
