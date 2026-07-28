# Response Type Inventory — T0 Baseline

Every response Pydantic model exported from `src/api/models.py` plus the
router-local models, catalogued for the Tier 1 envelope migration. Snapshot
at `release@bd80bdd`.

Companion to [endpoints.md](./endpoints.md).

---

## Scope of this document

- **In scope:** Pydantic response models used with `response_model=` on
  routes returning market data, plus the enums that give them semantics.
- **Out of scope:** query-parameter models, request bodies (there are
  effectively none — the API is read-only except for a few admin/POST
  routes), internal SQL row types.
- **Explicitly documented:** every field-level ambiguity that Tier 1
  needs to fix, especially the nullable-analytics case Johnnie flagged
  and the timestamp-naming zoo.

---

## Pydantic version and style baseline

- **Pydantic 2.5+** (per `pyproject.toml`).
- Every model in `src/api/models.py` uses the **v1-compatible `class Config`**
  inner-class style with `json_encoders` for `Decimal` and `datetime`
  serialization. Pydantic 2 still supports this via a deprecation shim;
  it will need migrating to `model_config = ConfigDict(...)` at some
  point but that's independent of the envelope work.
- `from_attributes = True` is set widely (formerly `orm_mode`) so
  models can be constructed from asyncpg `Record` objects.
- Nullable is expressed as `Optional[X] = None` — no discriminated
  unions, no `None`-as-sentinel-vs-missing distinction.

**Envelope migration decision.** Tier 1 introduces the envelope as new
Pydantic 2 models using `model_config = ConfigDict(...)` idiom, but does
NOT rewrite the existing 40+ payload models to v2 style — that's a
mechanical cleanup independent of the contract change. The envelope
wraps the existing models unchanged.

---

## Category A — Freshness / status models

The models that already do part of what the v1 envelope will formalize.

### `HealthStatus` (`models.py:599`)

```python
class HealthStatus(BaseModel):
    status: str = Field(..., description="healthy, degraded, or unhealthy")
    database_connected: bool
    last_data_update: Optional[datetime] = None
    data_age_seconds: Optional[int] = None
```

**Analysis.** This is the closest existing thing to a freshness envelope.
It gets three things right (age in seconds, timestamped last-known-good,
categorical status) and three wrong for our purposes:

1. `status` is a plain `str` — no enum, no closed vocabulary. In Tier 1
   this becomes `DataStatus` enum.
2. No session state, no `stale_after`, no `expected_cadence` — the
   payload can't tell you *why* it's stale.
3. Scope-limited to health. Every other endpoint reinvents the wheel
   (or, more often, doesn't).

**Migration path.** `HealthStatus` gets absorbed into the general
envelope's `meta` block. `/api/health` becomes the reference example
of the envelope in ADR-0002.

### `FlowSeriesPoint.is_synthetic: bool` (`models.py:319`)

**Not a model, but a precedent.** The `is_synthetic` flag on carry-forward
bars is the only in-response "this row isn't fresh" signal in the codebase.
Tier 1 generalizes it into the `SanityFlag` enum's `SYNTHETIC_BAR` /
`CARRY_FORWARD` values.

### `UnderlyingQuote.session: Optional[str]` (`models.py:233`)

**Also a precedent.** Emits one of `open` / `pre-market` / `after-hours` /
`closed`, populated from `get_market_session()`. This is the source of
truth for market-session labeling and becomes `freshness.market_session`
in ADR-0002 verbatim.

---

## Category B — Nullable-analytics models (Johnnie's July 17 case)

### `GEXSummary` (`models.py:12`)

The response shape from `/api/gex/summary`. Reproduced here with annotations:

```python
class GEXSummary(BaseModel):
    timestamp: datetime
    symbol: str
    spot_price: Decimal
    total_call_gex: Decimal          # always populated
    total_put_gex: Decimal           # always populated
    net_gex: Decimal                 # always populated
    net_gex_at_spot: Optional[Decimal] = None       # ← ambiguous null
    gamma_flip: Optional[Decimal] = None            # ← Johnnie's null
    gamma_flip_raw: Optional[Decimal] = None        # ← Johnnie's non-null
    gamma_flip_span_used: Optional[Decimal] = None  # ← Johnnie's null
    flip_distance: Optional[Decimal] = None         # ← Johnnie's null
    local_gex: Optional[Decimal] = None             # ← ambiguous null
    convexity_risk: Optional[Decimal] = None        # ← Johnnie's null
    max_pain: Optional[Decimal] = None              # ← ambiguous null
    call_wall: Optional[Decimal] = None             # ← ambiguous null
    put_wall: Optional[Decimal] = None              # ← ambiguous null
    total_call_oi: Optional[int] = None             # ← ambiguous null
    total_put_oi: Optional[int] = None              # ← ambiguous null
    put_call_ratio: Optional[Decimal] = None        # ← ambiguous null
```

**Every one of the `Optional`s above is a case Johnnie's `FieldStatus`
proposal covers.** In the July 17 QQQ observation:

- `gamma_flip` was null while `gamma_flip_raw` was numeric.
- The docstring on `gamma_flip_raw` (lines 21-29) explains that
  `gamma_flip` applies a horizon-occupancy ramp that can drop the
  crossing when the profile is one-signed / degraded — which is
  precisely the failure mode Johnnie couldn't distinguish from
  "no data" or "quality filter."

**Migration path.** In v1, each of the fields above is emitted as either
a scalar or a `FieldValue` discriminated union:

```python
class FieldValue(BaseModel):
    value: Optional[Decimal] = None
    status: FieldStatus  # valid | no_crossing_in_range | insufficient_data | filtered | not_applicable
    detail: Optional[FieldDetail] = None  # optional context per status
```

`FieldDetail` for `gamma_flip` would carry the searched span (which
`gamma_flip_span_used` currently exposes as a sibling field). The v1
shape folds that context into the value's own status block instead of
requiring the consumer to correlate two sibling nulls.

**Backwards compatibility.** The legacy `/legacy/gex/summary` route keeps
the raw-Optional shape unchanged for the 90-day deprecation window
(ADR-0003 §Migration).

### Other models with the same pattern

Every model listed in Category F below that contains `Optional[Decimal]`
or `Optional[float]` fields carries the same ambiguity. Tier 2's endpoint
sweep applies the `FieldValue` treatment to any nullable field whose null
state has semantic meaning (i.e. isn't just "this row doesn't have this
field kind" — which is a different case entirely).

Tier 1's ADR-0002 defines the taxonomy; Tier 2 does the field-by-field
audit.

---

## Category C — Signal / score models (Johnnie's July 17 score-vs-label case)

### `TradeSignalResponse` (`models.py:655`) + related enums

```python
class SignalDirection(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"

class SignalStrength(str, Enum):
    HIGH = "high"; MEDIUM = "medium"; LOW = "low"

class SignalComponent(BaseModel):
    name: str
    weight: int
    score: int
    description: str
    value: Optional[float] = None
    applicable: bool = True

class TradeSignalResponse(BaseModel):
    symbol: str
    timeframe: Timeframe
    timestamp: datetime
    current_price: float
    composite_score: int
    max_possible_score: int
    normalized_score: float
    direction: SignalDirection
    strength: SignalStrength
    estimated_win_pct: float
    components: List[SignalComponent]
    trade_idea: TradeIdea
    # + several optional context fields (net_gex, gamma_flip, vwap, etc.)
```

**Analysis.** This model has more of the right ingredients than most:

- `components: List[SignalComponent]` — each has `name`, `weight`, `score`,
  `description`, `value`, `applicable`. So the component-level detail
  Johnnie wants ("which components pushed toward the label") is already
  in the payload for this route.
- `composite_score` + `max_possible_score` + `normalized_score` — three
  numbers instead of one, which is already better than the "composite
  ~52 with no context" case Johnnie described.
- `direction` and `strength` are closed enums — good.

**Gaps.** No `score_metadata` block explaining:
- Whether the score is descriptive, predictive, or experimental.
- Whether it's comparable across symbols (Johnnie's SPX vs SPY vs QQQ point).
- Expected update cadence.
- The **mapping** from `composite_score` bands + `direction` + `strength`
  to the derived label the frontend displays (`mixed`, `constructive`, etc.).
- `applicable` on components is present but not used to explain a null
  composite.

**Migration path.** Tier 1 adds a `ScoreMetadata` model, embedded in
`interpretation.score_metadata` of the v1 envelope. Populated per signal
endpoint (values differ by endpoint — the trade-bias score is descriptive,
some advanced signals are experimental). Tier 3 documents the label-mapping
rules in the signal-semantics guide.

### `PositionOptimizerSignalResponse` (`models.py:738`) + related

Larger, more elaborate model but same shape and same gap: composite
scores present, no `score_metadata`, no comparability flag.

**Migration:** identical pattern, same `ScoreMetadata` type reused.

---

## Category D — Timestamp naming zoo

Fields observed carrying time information (representative list, not
exhaustive):

| Field name | Type | Model(s) | Semantic meaning |
|---|---|---|---|
| `timestamp` | `datetime` | Most models | ambiguous — sometimes source, sometimes calc |
| `time_window_start` / `time_window_end` | `datetime` | `OptionFlow` | Aggregation window bounds |
| `interval_timestamp` | `datetime` | `OptionFlow` | Bar-start; overlaps `time_window_start` |
| `bar_start` / `bar_end` | `str` (hand-formatted `Z`) | `FlowSeriesPoint` | Bar bounds; string not datetime |
| `tracking_started_at` | `datetime` | `GEXHistoricalContext` | First observation date |
| `current_session_close_ts` | `datetime` | `SessionCloses` | Close bar timestamp |
| `prior_session_close_ts` | `datetime` | `SessionCloses` | Prior close bar timestamp |
| `updated_at` | `datetime` | `SessionLevels`, `OpenInterestRecord` | Row mtime |
| `signal_timestamp` | `datetime` | `PositionOptimizerSignalResponse` | When the signal fired |
| `session_start_et` / `session_end_et` | `str` | `/api/technicals` (raw dict) | Session bounds |
| `trading_date` / `prev_session_date` | `date` | `SessionLevels` | ET calendar day |
| `session_date` | implicit | Various | ET calendar day |
| `last_data_update` | `datetime` | `HealthStatus` | Last known good |
| `data_age_seconds` | `int` | `HealthStatus` | Derived from now - last_data_update |

**Timezone handling.** Inconsistent:
- Most `datetime` fields carry ET timezone (`_ET`, `US/Eastern`).
- `FlowSeriesPoint` hand-formats UTC with trailing `Z`.
- `session_start_et` is a string with "et" suffix; the actual value uses
  ET time zone.

**Serialization format.**
- Default: Pydantic `isoformat()` — variable precision, may or may not
  include microseconds, timezone offset written as `+00:00` or similar.
- One-off: `/api/flow/series` uses `"%Y-%m-%dT%H:%M:%SZ"` (no fractional
  seconds, `Z` suffix).

**Migration decision (ADR-0002).** The v1 envelope's timestamp fields
are all RFC 3339 UTC with **millisecond precision** written with the
`Z` suffix (never `+00:00`), and every temporal field has an unambiguous
name from the closed set:

- `source_ts` — vendor's exchange/source publication time
- `exchange_ts` — exchange-side matching engine time (when different)
- `vendor_receipt_ts` — TradeStation's receipt time
- `calc_ts` — when *we* computed this
- `server_ts` — when the API assembled the response
- `event_ts` — when a discrete event fired (used on signal fire records)
- `as_of_date` — ET calendar day for daily-cadence data

Legacy fields (`timestamp`, `bar_start`, etc.) keep their names inside
the `data` payload for the 90-day compat window but new v1 endpoints
place authoritative timestamps in the `meta.freshness` block.

---

## Category E — Unit and precision conventions

### Money and Greeks: `Decimal`

Every monetary value and Greek exposure uses `Decimal` internally with a
`json_encoders` cast to `float` on serialization. Fine for numerical
correctness (asyncpg returns `Decimal` from numeric columns), fine for
JSON (float on wire).

**Documentation gap.** The units of the wire values are only in
docstrings (`"γ × OI × 100 × S² × 0.01"`, `"$ per 1% spot move"`, etc.).
The v1 envelope doesn't need per-field unit metadata — that's overkill —
but the OpenAPI spec must include units in the field descriptions
(ADR-0003 §Style Guide) so consumers don't guess.

### Volumes, OI: `int`

Uniform.

### Percentages / ratios: `Decimal` or `float`

Mixed. `put_call_ratio` is `Decimal` on `GEXSummary` (line 49) but `float`
on `FlowSeriesPoint` (line 339). Not user-visible in payload but the
schema-diff tool will complain. Tier 2 normalizes to `float` (percentages
never need arbitrary-precision).

### Percentiles: `float`

Uniform (`percentile: Optional[float]` on `GEXHistoricalWindow`).

---

## Category F — Full model catalog (payload types)

Grouped by domain. Line refs are `src/api/models.py` unless noted.

**GEX**
- `GEXSummary` (:12) — `/api/gex/summary`
- `GEXByStrike` (:59) — `/api/gex/by-strike`
- `GEXProfilePoint` (:85) + `GEXProfile` (:93) — `/api/gex/profile`
- `GEXHistoricalWindow` (:122) + `GEXHistoricalMetric` (:165) + `GEXHistoricalContext` (:172) — `/api/gex/historical-context`

**Flow**
- `OptionFlow` (:201) — internal, unused by public routes
- `FlowCallPutTotals` (:259) + `FlowBucketResponse` (:264) + `FlowMapBucketResponse` (:278) — unused by public routes
- `FlowPoint` (:292) — `/api/flow/by-contract`
- `FlowSeriesPoint` (:319) + `FlowContractsResponse` (:345) — `/api/flow/series`, `/api/flow/contracts`
- `SmartMoneyFlowPoint` (:352) — `/api/flow/smart-money`
- `MomentumDivergencePoint` (:370) — `/api/technicals/momentum-divergence`
- `FlowBuyingPressurePoint` (:379) — `/api/flow/buying-pressure`

**Market data**
- `UnderlyingQuote` (:223) — `/api/market/quote`, `/api/market/historical`
- `PreviousClose` (:390) — unused
- `SessionCloses` (:403) — `/api/market/session-closes`
- `SessionLevels` (:418) — `/api/market/session-levels`
- `OptionQuote` (:478) — `/api/option/quote`
- `OpenInterestRecord` (:566) + `OpenInterestResponse` (:585) — `/api/market/open-interest`
- `StrikeProfileStrike` (:497) + `StrikeProfileBucket` (:522) — `/api/gex/strike-profile-timeseries`

**Max pain**
- `MaxPainPoint` (:448) + `MaxPainExpiration` (:456) + `MaxPainCurrent` (:463) — `/api/max-pain/current`
- `MaxPainTimeseriesPoint` (:472) — `/api/max-pain/timeseries`

**Health**
- `HealthStatus` (:599) — `/api/health`

**Signal engine**
- Enums: `SignalDirection` (:611), `SignalStrength` (:617), `TradeType` (:623), `Timeframe` (:632)
- `SignalComponent` (:638), `TradeIdea` (:647), `TradeSignalResponse` (:655)
- Position optimizer: `PositionOptimizerDirection` (:685), `PositionOptimizerCandidateComponent` (:691),
  `PositionOptimizerSizingProfile` (:700), `PositionOptimizerCandidate` (:708),
  `PositionOptimizerSignalResponse` (:738)

**Router-local models** (defined inside the router file, not `models.py`):
- `LevelsResponse` — `routers/levels.py`
- `VolatilityIndexResponse` — `routers/volatility_gauge.py:217`
- `FlipTermStructureResponse`, `FlipSurfaceResponse` — `routers/gex_flip_horizon.py`
- `VolSurfaceResponse` — `routers/vol_surface.py`
- `PremiumSurfaceResponse` — `routers/premium_surface.py`
- `OptionContractRow` — `routers/option_contract.py`
- `OptionCalculatorResponse` — `routers/option_calculator.py`
- `CurveResponse`, `CharmDecayResponse`, `VannaLadderResponse`, `SurfaceResponse`,
  `ScenarioResponse`, `LevelsResponse` (different from levels router's),
  `BacktestResponse` — `routers/forced_flow.py`

Router-local models are ~20 additional Pydantic types. They should be
audited into `models.py` (or a per-domain module) during Tier 1 to make
the OpenAPI generation coherent.

---

## Category G — Fields with existing status/kind vocabulary

Fields that already emit closed vocabularies. These are the seed for the
`DataStatus` enum in ADR-0002.

| Field | Model | Values | Notes |
|---|---|---|---|
| `session` | `UnderlyingQuote` | `open`, `pre-market`, `after-hours`, `closed` | Becomes `freshness.market_session` |
| `status` | `HealthStatus` | `healthy`, `degraded`, `unhealthy` (informal) | Absorbed into `meta.data_status` |
| `is_synthetic` | `FlowSeriesPoint` | `bool` | Becomes `SanityFlag.CARRY_FORWARD` |
| `applicable` | `SignalComponent` | `bool` | Becomes `FieldStatus.NOT_APPLICABLE` for null values |
| `direction` | Signal responses | `bullish`, `bearish`, `neutral` | Stays; extended vocabulary in `interpretation.labels` |
| `strength` | Signal responses | `high`, `medium`, `low` | Stays |
| `sentiment` | `OptionFlow` | `str` (open) | Not currently a closed enum; either close it in Tier 2 or drop it |
| `divergence_signal` | `MomentumDivergencePoint` | `str` (open) | Same |
| `momentum` | `FlowBuyingPressurePoint` | `str` (open) | Same |
| `hedge_pressure` (in dealer-hedging response) | raw dict | `Heavy Buy-Hedging Risk` / `Heavy Sell-Hedging Risk` / `Balanced Hedging` (with emoji prefixes) | Close as enum, drop emoji |
| `source` | `SessionLevels` | `captured`, `live`, `captured+live` | Becomes a `SanityFlag` composite or a `provenance` field in `meta` |
| `display_source` | `UnderlyingQuote` | `futures` or None | Provenance signal — keep or absorb into `meta.provenance` |
| `regime` | `GEXHistoricalWindow` | `record_high`, `extreme_high`, `elevated`, `normal`, `low`, `extreme_low`, `record_low`, `unknown` | Stays; documented in signal-semantics guide |

---

## Summary of Tier 1 impact on models

- **New envelope types (create):** `MetaBlock`, `FreshnessBlock`,
  `DataStatus`, `SanityFlag`, `FieldStatus`, `FieldValue<T>`,
  `ScoreMetadata`, `ProvenanceBlock`, `WarningEntry`,
  `EndpointStability`. All new; no legacy interference.
- **Existing models (unchanged for v1 launch):** the 40+ payload
  models. They become `.data` inside the envelope.
- **Existing models (v1-touched):** `HealthStatus` (folded into meta),
  `GEXSummary`'s nullable cluster (per-field `FieldValue<Decimal>`
  treatment), `TradeSignalResponse`'s signal shape (adds
  `score_metadata`).
- **Deferred:** Pydantic v1→v2 `ConfigDict` migration on legacy models.
  Independent of envelope; do after Tier 2 lands.
- **Router-local models:** consolidate into `models.py` or a
  `models/` package during Tier 1 so OpenAPI generation is coherent.

Full envelope shapes and JSON examples are in ADR-0002.
