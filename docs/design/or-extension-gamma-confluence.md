# Opening-range extension × gamma confluence — Phase 1 repository assessment

**Question:** a trader (Jim, `zerogex.io Mail — Re: Friday`, 2026-09-07) proposes a
fixed intraday level system projected away from the opening range, with the
observation *"the further from the opening price, the stronger the rubber band
effect when it reverts"*, plus a second claim that levels which are **not**
respected signal continuation. Can ZeroGEX gamma structure tell us **which**
opening-range extensions matter, and is there enough measurable edge to justify
one or more TradeWorkz bots?

**Answer: the research is buildable almost entirely from parts that already
exist, and it should be a new `research/` package rather than a new framework.**
The point-in-time discipline the question demands is already implemented and
already tested here — `StepSeries` (`src/analytics/wall_breaks.py:141`) is a
right-continuous as-of lookup written for exactly this hazard, and
`research/wall_break_odds` is a complete, working template for "label events,
featurise strictly at the event, screen, walk forward, report the nulls". Four
things are genuinely missing and are the real work: (1) the OR ladder itself,
(2) a **synchronous** historical futures-basis read, (3) point-in-time GEX
**rank** reconstruction, and (4) a futures execution model, which TradeWorkz has
never needed because every bot it has ever shipped trades option legs.

**Status:** assessment only. No implementation code written. Every claim carries
a `file:line` into this repo, current as of the commit that adds this document.

---

## 0. Executive summary of what already exists

| The request asks for | Exists? | Where |
|---|---|---|
| Point-in-time gamma joins with no forward-fill | **Yes** | `StepSeries.at()` `src/analytics/wall_breaks.py:141-181` — last value at-or-before, never the next |
| First-touch / re-arm / cooldown event semantics | **Yes** | `EventConfig` `src/analytics/wall_breaks.py:80-121`, `extract_wall_tests` `:294` |
| Read-only research DB access | **Yes** | `research_connection` `research/mm_attributed_gex/sources.py:84` — sets session `READ ONLY`, raises statement timeout, `DatabaseUnavailable` |
| Minute bars for cash **and** ES/NQ | **Yes** | `load_bars` `research/msi_regime_excursion/sources.py:132` — `underlying_quotes` for cash, `futures_quotes` for futures |
| MFE / MAE / forward excursion at horizons | **Yes** | `compute_excursion` `research/msi_regime_excursion/excursion.py:246`; horizons 5/15/30/60 + rest-of-session |
| Session-clustered bootstrap, Wilson CI, BH-FDR | **Yes** | `research/msi_regime_excursion/stats.py:330`, `:498`, `:539` |
| Chronological walk-forward / OOS split | **Yes** | `session_walk_forward` `research/wall_break_odds/model.py:310` (splits on **session**, never row index) |
| Survival curve + log-rank (for time-to-target) | **Yes** | `kaplan_meier` / `logrank` `src/analytics/wall_breaks.py:421`, `:527` |
| Futures ↔ index price-axis mapping | **Yes, but async** | `resolve_basis` `src/jobs/futures_projection.py:460`, `FuturesBasis` `:364`, SQL in `get_futures_basis_samples` `src/api/database.py:6324` |
| Ranked GEX levels ("GEX #4") | **Computed, not stored** | `compute_wall_ladder` `src/analytics/walls.py:165` → `C1..Cn` / `P1..Pn` |
| VWAP | **Yes** | `underlying_vwap_deviation` view; CTE fallback `src/signals/unified_signal_engine.py:300-330` |
| Opening range | **Yes, but fixed at 30 min** | view `opening_range_breakout` `setup/database/schema.sql:1194`; reader `src/api/queries/technicals.py:356` |
| Bot interface + signal object | **Yes** | `BaseBot` `src/tradeworkz/bots/base.py:147`; `TradeSignal` `src/tradeworkz/models.py:27` |
| "Registered but never provisioned" bot state | **Yes** | `CANDIDATE_SPECS` `src/tradeworkz/registry.py:522`; `DEFAULT_ROSTER` `:826` is currently **empty** |
| Bot promotion gate / backtest | **Yes** | `src/tradeworkz/backtest.py` (`make tradeworkz-backtest`) — replays real `open_criteria` against `build_snapshot(as_of=t)` |
| Trade audit blob | **Yes** | `TradeSignal.components_at_entry` `src/tradeworkz/models.py:50` → `tw_trades` |
| Extension ladder (±50 % … ±1000 % of R) | **No** | new |
| Synchronous historical basis read | **No** | `resolve_basis` is `async` and needs `DatabaseManager` |
| Point-in-time GEX **rank** history | **No** | must be recomputed per snapshot from `gex_by_strike` |
| Futures execution model (tick value, RT commission) | **No** | see §6 |
| Parquet output | **No** | `pyarrow` is not a dependency; repo convention is JSONL + `.meta.json` |

---

## 1. The look-ahead requirement is harder than stated — and the repo can prove it

The brief's central rule is `gamma_snapshot_used.timestamp < T`. **That rule as
written is not sufficient**, and this repo contains the evidence.

`gex_summary.timestamp` is **not** the instant the level became visible. It is
the option-chain data instant: the analytics engine selects the newest
`option_chains` snapshot and carries its timestamp through
(`src/analytics/main_engine.py:3953` — `latest_timestamp = snapshot["timestamp"]`,
persisted as `gex_summary.timestamp` at `:3124`). The engine then *computes*, and
only then writes. `created_at TIMESTAMPTZ DEFAULT NOW()` on the same row
(`setup/database/schema.sql`, `gex_summary`) is the write instant.

Michael's own reply in the source email quantifies the rest of the gap:

> "our server recomputes the levels once a minute and the indicator pulls the
> newest copy every 30 seconds, so a line is normally about a minute behind."

So there are three distinct clocks, and the study has to name which one it uses:

| Clock | Column | Meaning | Honest for research? |
|---|---|---|---|
| Data instant | `gex_summary.timestamp` | the chain the level was computed *from* | **Optimistic** — level did not exist yet |
| Publish instant | `gex_summary.created_at` | when the row hit the database | Correct for "the platform knew it" |
| Visible instant | `created_at + poll lag` | when a user's chart could draw it | Correct for "the trader could act on it" |

**Proposal:** an explicit `availability_clock` config with three settings
(`data` / `published` / `visible`), defaulting to `visible`, and
`client_poll_lag_seconds` defaulting to 30 (the indicator's clamp, documented in
the same email). `gamma_min_lead_seconds` is then measured against the chosen
clock, not against `timestamp`. Reporting `0 / 30 / 60 / 120 / 180` against the
`data` clock alone would systematically over-credit confluence, and the size of
that error is roughly the whole lead-time range being tested — i.e. it could
manufacture the entire result.

Caveat to carry: `created_at` is the row-write time, so for any **backfilled**
rows it is the backfill time, not the historical publish time. The dataset
builder must detect this (`created_at` clustered far from `timestamp`) and fail
closed for those sessions rather than silently using a nonsense lead.

### The second-order look-ahead: re-centering levels

The email documents this precisely — Call Wall is "the biggest call gamma strike
**above spot**", so when price goes through a strike "that strike flips to Put
Wall and the Call Wall re-points to the next one up". `compute_wall_ladder`
(`src/analytics/walls.py:165`) implements exactly that spot-relative filter
(`lambda s: s >= spot_price` / `s <= spot_price` at `:249-250`).

Consequence for this study: a wall or GEX rank observed at the touch is
**partly caused by the touch**. The harness must therefore evaluate confluence
using the ladder as it stood at `T − lead`, computed against the **spot at
`T − lead`** — not the ladder recomputed at the touch, and not the ladder from
`T − lead` re-ranked against the touch spot. Test #4 in the brief ("a level that
becomes GEX #1 AFTER price arrives does NOT count") is exactly this, and it is
the single test most likely to fail on a naive implementation.

---

## 2. Historical data: what is actually available, and for how long

This is the finding that should shape the study design, because **the two halves
of the gamma picture have very different history depths.**

| Table | Contents | Retention |
|---|---|---|
| `gex_summary` | walls + strengths, `gamma_flip_point`, `gamma_flip_raw`, `flip_distance`, `total_net_gex`, `net_gex_at_spot`, `local_gex`, `convexity_risk`, `max_pain`, `pin_strike`/`pin_score`/`pin_confidence`, `max_gamma_strike` — 1 row/min/symbol | **RETENTION-EXEMPT** (`Makefile` `DB_MAINTAIN_TABLES` note: "removed 2026-08-25") |
| `underlying_quotes` | minute OHLC + up/down volume, cash symbols | **RETENTION-EXEMPT** (same note) |
| `gex_by_strike` | per `(underlying, timestamp, strike, expiration)`: `net_gex`, `call_gamma`, `put_gamma`, OI, vanna, charm | **PRUNED** at `DATA_RETENTION_DAYS` (`src/config.py:600`, default 90; `.env.example:685` sets **60**) |
| `futures_quotes` | ES/NQ minute OHLC, keyed by **cash index** | Pruned by the ingester at `FUTURES_BARS_RETENTION_DAYS` ?: `DATA_RETENTION_DAYS` (`src/ingestion/futures_underlying_ingester.py:477-493`) |
| `trade_bias_scores` | `bias_code`, `direction`, `market_state`, `confidence` | not pruned by `db-prune` |
| `gex_regime_session` | per-session FIRMING / CAPPING / FRAGILE_BID / DETERIORATING / QUIET | not pruned |

**Measured 2026-09-08 — the "long history" was wrong.** The retention *rule*
above is right, but the retention exemption only dates from 2026-08-25, so it
preserves history forward from then and does not restore what was already
pruned. `make orgc-coverage` against production returns:

| symbol | gamma frames | bars | **usable** | ranked (GEX) | gamma window |
|---|---:|---:|---:|---:|---|
| SPY | 59 | 59 | **59** | 51 | 2026-06-29 .. 09-05 |
| QQQ | 59 | 59 | **59** | 51 | 2026-06-29 .. 09-05 |
| SPX | 49 | 59 | **49** | 42 | 2026-06-29 .. 09-04 |
| NDX | 31 | 52 | **31** | 31 | 2026-07-24 .. 09-04 |
| ES | 49 | 52 | **49** | 42 | (SPX book) |
| NQ | 31 | 52 | **31** | 31 | (NDX book) |

So the ceiling is **~59 sessions**, not years — and **NQ, the symbol the idea
came from, is the thinnest at 31**. Gamma frames bind everywhere; bars are never
the constraint. For scale, `research/wall_break_odds` ran 48 SPX sessions and
declined to fit a model below 200 events. Sessions, not touches, are the
independent unit here, because the cohort comparison resamples whole sessions.

**The publish clock is usable, and the lag is bigger than assumed.** Zero NULL
`created_at`, zero backfilled rows, one negative-lag row (QQQ) across ~146k
rows. Lag runs p50 29–41 s, p95 61–71 s, with maxima of 83 s (SPX), 96 s (NDX),
651 s (QQQ), 1618 s (SPY). Two consequences:

* The `visible` clock sits **~60–70 s behind the `data` clock at the median**
  — which independently reproduces what the source email told the customer
  ("a line is normally about a minute behind"). That gap is larger than half
  the 0–180 s lead sweep, so the clock choice changes the answer, not the
  decimal place.
* The original `max_publish_lag_seconds = 1200` was a blind guess and was
  wrong: it would have discarded SPY's 1618 s frame as "backfilled" when it is
  a slow publish that the `visible` clock already handles correctly. Raised to
  6 h, chosen off this distribution, and rejection moved from the SESSION to
  the FRAME — an isolated bad clock now costs one frame (the step function
  holds the previous value across it) and a session fails closed only when
  over 5% of its frames are unusable. Under the old rule SPY and QQQ each lost
  a whole session to a single row.

**The asymmetry that still matters.** Walls, flip, max pain and pin come from
`gex_summary`. **Ranked GEX levels do not exist as stored
data at all** — they are computed on demand by `compute_wall_ladder` from
`gex_by_strike`, which is the one pruned table. So:

* the *wall / flip / max-pain / pin* confluence arm can run over the full
  `gex_summary` window (59 / 49 / 31 sessions above);
* the *"GEX #4" ranked-strike* arm — which is half of what the email is actually
  excited about ("Call Wall, Max Pain, Pin Strike and GEX 1 were all sitting on
  29530") — is capped at ~60–90 days.

Design consequence: ranked-GEX must be an **optional, availability-flagged level
source**, not a hard requirement. A session with no `gex_by_strike` coverage
records `gex_rank_available = false` and contributes to every cohort that does
not need ranks, instead of dropping out and silently shrinking the whole study.

**Other hard limits:**

* **Strike coverage is a band.** Ingestion keeps strikes within
  `INGEST_STRIKE_PCT_RANGE` (3.0 %, `.env.example:1056`) up to
  `INGEST_STRIKE_COUNT_MAX` (40, `:1064`) per expiration. An OR extension at
  −300 % of R on a wide-range day can easily sit outside ±3 % of spot, where
  **no gamma level can exist by construction**. That is a censoring mechanism
  aligned with the very extension distances the reversion hypothesis is most
  interested in, and it must be recorded per event (`strike_band_covered`),
  not ignored. Otherwise "far extensions have no confluence" is an artefact of
  the ingester, reported as a finding.
* **Ingested option chains are SPY, SPX (SPXW), QQQ, NDX (NDXP)** only
  (`docs/runbooks/ingestion_symbol_gap.md:62`). **NQ and ES have no option
  chain of their own** — their levels *are* NDX/SPX levels projected. MNQ/MES
  are not in `_DEFAULT_FUTURES_UNDERLYINGS` (`src/symbols.py:168`) at all.
* **Snapshot cadence is 60 s** (`ANALYTICS_INTERVAL`, `src/config.py:1806`).
  `gamma_min_lead_seconds=30` is therefore *sub-cadence* — it will behave almost
  identically to 0 for most events. Worth keeping in the sweep as a control, but
  the informative values are 0 / 60 / 120 / 180.

---

## 3. Futures mapping — reuse, do not reinvent (and one adapter is required)

The brief is right that the mapping must be the production one. It is
`src/jobs/futures_projection.py`, and it is already correct for historical use:

* `FuturesBasis` (`:364`) with `project()` / `unproject()` / `offset_at()`, and
  `round_to_tick` to the future's own tick (`src/symbols.py:245`, NQ = 0.25).
* `resolve_basis(db, symbol, at=...)` (`:460`) — the `at` parameter exists
  **specifically** for this, and its docstring already states the failure mode
  the brief is worried about: *"projecting a frame from three months ago with
  today's ratio offsets every level by however much it has moved since. That
  silent offset is invisible on a chart and corrupts a backtest."*
* The rule is levels project, dollars do not (`PRICE_FIELDS` `:94`). `net_gex`,
  wall strengths, OI must **not** be rescaled.
* Sanity bound: a ratio >3 % from 1.0 is rejected as a bad feed (`:73`),
  falling back to cost-of-carry with `source="carry"`.

**The one gap:** `resolve_basis` is `async` and requires an object exposing
`get_futures_basis_samples` — that is `DatabaseManager` (asyncpg,
`src/api/database.py:6324`). The research packages are **synchronous psycopg2**
(`research_connection`). So a thin sync adapter is needed that runs the *same*
SQL and hands rows to the *same* `resolve_basis` logic. I propose extracting
nothing from production; instead the research package provides a tiny shim class
exposing `get_futures_basis_samples` over a psycopg2 cursor and driving
`resolve_basis` through `asyncio.run`. That keeps the ratio math, the median,
the sanity bound, the staleness labelling and the carry fallback as the single
production implementation, with the research side owning only the transport.

**A second correctness point the brief did not raise:** basis is measured, so it
moves. Projecting *once per session* would smear the ladder. The harness should
resolve the basis **per gamma snapshot** (or at minimum per event), and persist
`basis_ratio`, `basis_source` (`measured` / `measured_stale` / `carry`) and
`basis_observed_at` on every row. A run where most events carry
`basis_source="carry"` is not a run whose NQ-axis levels can be quoted.

**Recommended framing for NQ:** do the price/OR work on the **NQ axis** (from
`futures_quotes`, which is what a trader actually sees) and carry NDX gamma
levels onto it. Do *not* project spot. That mirrors production exactly
(`SPOT_FIELDS`, `:361`).

MNQ/MES: same price series as NQ/ES, different contract multiplier. I propose
handling them as a **research-config multiplier table**, not by editing
`src/symbols.py` — a production symbol-map change is out of scope for a research
harness and would alter live display behaviour.

---

## 4. Opening range: what exists and why it is not enough

There is an existing opening range — the `opening_range_breakout` SQL view
(`setup/database/schema.sql:1194`) read by `get_opening_range_breakout`
(`src/api/queries/technicals.py:356`). It is:

* **fixed at 30 minutes** (`EXTRACT(MINUTE ...) BETWEEN 30 AND 59`, hardcoded);
* **not parameterisable** to 5 / 15;
* computed *live* per request, with no extension ladder;
* cash-symbol only (`underlying_quotes`).

And `OpeningRangeHunter` (`src/tradeworkz/bots/opening_range_hunter.py`) does
**not** implement a fixed OR — its own comment says so: *"Not a hard 30-min
window without a separate persistence layer, so we use the running session_high
/ session_low as the working proxy."* That is a genuinely different (and
look-ahead-adjacent) object; it is not reusable here.

**Proposal:** reuse the *conventions* — ET session, 09:30 anchor, the
`orb_high` / `orb_low` / `orb_range` naming, the `PRICE_FIELDS` entries that
already project `opening_range` / `orb_range` / `distance_above_orb_high` onto
the futures axis (`src/jobs/futures_projection.py:192-205`) — and implement the
parameterised OR + ladder as a pure function in the research package, freezing
`(ORH, ORL, R)` at the close of the OR window and never recomputing. Session
handling comes from `src/market_calendar.py` (`ET`, `NYSE_HOLIDAYS`,
half-days) so DST and holidays are the production answer, not a new one.

---

## 5. The statistical layer is already built

Nothing new is needed here, and the repo's existing standards are *stricter*
than the brief asks for:

* `session_block_bootstrap_diff` (`research/msi_regime_excursion/stats.py:330`)
  — the brief asks for bootstrap CIs; the repo already knows that rows sharing a
  session are not independent. `research/wall_break_odds/README.md` documents the
  measured false-positive rate: a naive z-test runs **0.110** at α=0.05 on
  session-clustered data versus **0.055** for the clustered bootstrap.
* `wilson_ci` (`:498`) for proportions at small N — directly the
  reversal-first / continuation-first rate.
* `benjamini_hochberg` (`:539`) — mandatory given the cohort × parameter grid
  this brief specifies; without it the 11 cohorts × 5 lead times × 5 confluence
  buckets × 3 OR windows is a multiple-comparisons machine.
* `mann_whitney_u` / `cliffs_delta` (`:214`, `:238`) for MFE/MAE, which are
  heavy-tailed and should not be compared by means alone.
* `session_walk_forward` (`research/wall_break_odds/model.py:310`) splits on
  **session boundaries**, satisfying "no random shuffle across time".
* `kaplan_meier` + `logrank` (`src/analytics/wall_breaks.py:421`, `:527`) — the
  right tool for `time_to_previous_extension`, which is censored at the bell in
  exactly the way `wall_break_odds` already handles.

The repo also has a strong, enforced culture of reporting nulls
(`wall_break_odds/README.md`: *"No feature survived — including the one I called
a null too early"*). I intend to match that register.

---

## 6. Execution: the real architectural gap

**TradeWorkz cannot trade futures.** Every bot emits `List[Leg]` where a `Leg` is
`(option_symbol, side, option_type, strike, expiration)`
(`src/tradeworkz/models.py:15-25`). Fills are option bid/ask plus a slippage
fraction and a per-contract commission (`leg_fill_price`
`src/signals/execution.py:41`; `commission_per_contract: float = 0.65`
`src/backtesting/models.py:73`). There is **no** point multiplier, tick value,
round-turn commission or exchange-fee model anywhere in `src/tradeworkz/`,
`src/backtesting/` or `src/signals/execution.py` — I grepped for
`multiplier` / `point_value` / `tick_value` / `contract_size` and the only hits
are ML sizing multipliers.

This forks the project, and the fork should be an explicit decision rather than
something I quietly pick:

* **Path A — research on NQ, bots on NDX/QQQ options.** The price phenomenon is
  measured on the NQ axis (where Jim sees it); a promoted bot expresses it in
  NDX or QQQ options, which is the only thing the current engine can fill. Zero
  new execution machinery; the bots slot straight into `CANDIDATE_SPECS`. Cost:
  a 0DTE option overlay has theta and spread costs that can eat a small
  point-edge entirely — the same failure that shelved `PutWallMagnetReversal`
  (`src/tradeworkz/registry.py:57-67` note: *"that is BEFORE 0DTE theta on a
  ~65 %-timeout distribution, which almost certainly turns it net-negative"*).
* **Path B — add a futures execution model.** A small, self-contained
  `FuturesFillModel` (tick size from `resolve_futures_tick`, point value,
  round-turn commission + exchange/NFA fees, configurable slippage in ticks,
  no fills at impossible prices) used **only** by the research trade simulator.
  Honest for NQ, but it is new execution code and the bots still could not go
  live without a futures broker path that does not exist.

**Recommendation: do both, in that order.** Phase 4 builds Path B's fill model
for the *research* P&L only (so "is this economically tradeable after costs?" is
answered on the instrument the effect was measured on), and Phase 5's TradeWorkz
candidates take Path A so they integrate with the real engine, audit trail and
promotion gate. Phase 6 reports both, and flags clearly if the effect survives
in points but dies under option carry.

---

## 7. Proposed build

### New package: `research/or_gamma_confluence/`

Mirrors `research/wall_break_odds/` exactly — stdlib-only (that package and
`msi_regime_excursion` take no third-party imports; only `mm_attributed_gex`
uses numpy), read-only DB, file outputs, `python -m research.<pkg>.cli`.

| File | Purpose |
|---|---|
| `__init__.py` | package docstring; the research-never-imported-by-src rule |
| `config.py` | **single** frozen `ResearchConfig` dataclass — every knob in the brief: `opening_range_minutes`, `extension_mode` (A/B), `extension_step`, `max_extension`, `confluence_distance_*`, `gamma_min_lead_seconds`, `availability_clock`, `client_poll_lag_seconds`, `touch_tolerance`, `rearm_minutes`, `trend_filter`, `regime_filter`, `min_extension_for_reversion`, `min_failed_extensions_for_continuation`, confirmation/stop/target rules, cost assumptions. Carries a `fingerprint()` hash used as the cache key (brief's test #12) |
| `ranges.py` | pure: OR from bars, frozen at window close; Mode A (boundary ± k·R) and Mode B (open ± k·R) ladders; `Extension` dataclass with number, price, side |
| `levels.py` | point-in-time gamma level set at an instant: `gex_summary` fields + `compute_wall_ladder` recomputed from `gex_by_strike` **at the lead-time spot**; per-source availability flags; `LevelSnapshot` carrying source ts, `created_at`, age, clock used |
| `basis.py` | sync adapter over `resolve_basis` (§3); per-snapshot ratio; `basis_source` recorded |
| `events.py` | first-touch detection on the ladder, re-arm/cooldown, `prev-before-next` outcome labelling; built on `StepSeries` and the `EventConfig` idiom |
| `features.py` | everything the brief lists — OR geometry, time-of-day, session high/low, realized range, ATR, VWAP + distance, `consecutive_extensions_broken`, `prior_extension_respect_score`, confluence fields (`nearest_gamma_*`, `gamma_confluence*`), regime, trend. Hard cutoff at the event timestamp, in the `wall_break_odds/features.py` style |
| `trend.py` | modular trend-filter interface: HMA, EMA slope, VWAP slope, `trade_bias_scores`, `gex_regime_session`. A research dimension, not a truth |
| `outcomes.py` | reversal-first / continuation-first; MFE/MAE at 1/3/5/10/15/30/EOD via `compute_excursion`; time-to-target; censoring at the bell |
| `dataset.py` | per-session assembly, backward-only trailing stats, JSONL + `.meta.json` provenance |
| `sources.py` | read-only SELECTs; `futures_quotes` for NQ/ES via the `msi_regime_excursion` reader |
| `cohorts.py` | the 11 cohorts, as declarative predicates over event rows |
| `stats.py` | thin re-export of `msi_regime_excursion.stats` + `wall_breaks` survival — no new statistics |
| `simulate.py` | Phase 4: explicit entry/stop/target rules, `FuturesFillModel` (tick, point value, RT commission, fees, slippage), gross vs net |
| `report.py` | Markdown answering the brief's 10 questions in plain English, incl. failures; JSON summary; CSV export |
| `selftest.py` | seeded synthetic sessions, no DB — asserts the anti-look-ahead properties |
| `cli.py` | `selftest`, `build-dataset`, `analyze`, `sweep`, `simulate`, `report` |
| `README.md` | status, method, results, and every null |

### Production files touched

Deliberately minimal. The research package reads production; production does not
read it.

| File | Change | Why |
|---|---|---|
| `Makefile` | ~8 new `.PHONY` targets (`orgc-selftest`, `orgc-dataset`, `orgc-analyze`, `orgc-sweep`, `orgc-simulate`, `orgc-report`) | matches the `mmgex-*` convention at `Makefile:1204+` |
| `docs/design/or-extension-gamma-confluence.md` | this document, extended with method + results | repo convention |

### Phase 5 — TradeWorkz candidates (option-expressed, disabled)

| File | Content |
|---|---|
| `src/tradeworkz/bots/or_gamma_reversion.py` | fade extreme OR extension with pre-existing confluence + containment regime |
| `src/tradeworkz/bots/or_gamma_continuation.py` | trade the ladder when successive extensions are not respected |
| `src/tradeworkz/bots/or_gamma_confluence.py` | strict multi-structure version |
| `src/tradeworkz/registry.py` | register the 3 classes in `STRATEGY_CLASSES`; add specs to **`CANDIDATE_SPECS` only** — registered, backtestable by id, **never provisioned**, never in `DEFAULT_ROSTER` |

Each emits a normal `TradeSignal` with `rationale` plus machine-readable
`reason_codes` inside `components_at_entry` (`OR_EXTREME_-300`,
`PUT_WALL_CONFLUENCE`, `GEX_RANK_4_CONFLUENCE`, `POSITIVE_GEX`,
`REVERSAL_CONFIRMED`, …), alongside the gamma snapshot timestamp, its age, the
clock used, the basis ratio and source, and the OR parameters — so the existing
audit UI answers "what did ZeroGEX know at that exact moment?" with no schema
change.

**Bot 4 (regime switcher) is deferred.** It is a router over Bots 1–2, and it
should not exist until 1 and 2 have separately earned their keep.

### Tests (`tests/test_or_gamma_confluence.py`, + a lookahead-specific module)

One test per numbered requirement in the brief, plus three the brief did not
ask for that this repo's structure makes necessary:

13. the **publish-clock** test — a snapshot whose `timestamp` precedes the touch
    but whose `created_at` does not is rejected under `availability_clock=published`;
14. the **re-centering** test — the ladder used for confluence is ranked against
    the lead-time spot, not the touch spot;
15. the **basis-anchor** test — a historical event resolves basis at its own
    timestamp; `at=None` (today's ratio) produces a measurably different level
    and is never used.

---

## 8. Honest limitations to state up front

1. **Sample size is the binding constraint everywhere, not just on the
   ranked-GEX arm.** Measured: 59 sessions for SPY/QQQ, 49 for SPX/ES, **31 for
   NDX/NQ**. `wall_break_odds` set its model floor at 200 events and declined
   to fit below it; the same discipline applies here, and on NQ specifically
   the honest expectation is that most cohorts will not separate.
2. **The ±3 % strike band censors exactly the far extensions the reversion
   hypothesis cares most about.** This must be reported as coverage, not
   silently absorbed.
3. **Dealer sign is modelled, not observed** — walls come from the
   call-positive / put-negative OI convention. Carried forward verbatim from
   `wall_break_odds/README.md`; `research/mm_attributed_gex` is the work on that.
4. **Correlation is not dealer causality.** A wall near a reverting extension is
   a co-location, not a mechanism.
5. **This session has no database access.** Phases 1–2 (build + synthetic
   selftest + unit tests) are fully executable here; Phase 3 onward needs a run
   against the production analytics DB.
6. **Jim's observation may simply be right for a reason unrelated to gamma** —
   extension distance alone (cohort 2 vs 3) is the control that decides it, and
   it is the comparison most likely to kill the interesting version of the story.

---

## 9. Decisions taken (2026-09-08)

1. **Execution fork (§6)** — Path A + B as recommended. Phase 4 builds a
   futures fill model for the RESEARCH P&L only, so "is this tradeable after
   costs?" is answered on the instrument the effect was measured on; Phase 5's
   TradeWorkz candidates express the signal in NDX/QQQ options so they reach
   the real engine, audit trail and promotion gate. Phase 6 reports both, and
   flags it explicitly if the effect survives in points but dies under option
   carry.
2. **Availability clock** — default `visible` (`created_at` + 30 s poll lag),
   with `published` and `data` available as robustness rows. Implemented as
   `ResearchConfig.availability_clock`.
3. **Symbols** — all six: SPY, QQQ, SPX, NDX, ES, NQ. The four-way structure
   is a genuine replication design rather than six separate studies: SPY/SPX
   share the S&P book and QQQ/NDX the Nasdaq one, while ES/NQ are the same two
   books on a futures axis. A feature that is real should hold its sign across
   the pairs; `wall_break_odds` used exactly this shape to kill fourteen
   candidates.
4. **History window** — not knowable from the code, so it is now a command:
   `make orgc-coverage` (`cli coverage`) reports per symbol and per table the
   earliest/latest row and the session count, and separately measures whether
   `created_at` is a usable publish clock. **Run it before anything else** —
   it bounds the whole study and validates decision 2.

## 10. Phase 2 status — built

`research/or_gamma_confluence/` is complete, tested (53 tests) and verified end
to end on synthetic data. It has NOT been run against production; this session
had no database access, so Phase 3 onward is pending.

Two defects were found and fixed by the tests while building, both of the kind
that would have produced a confident wrong answer rather than a crash:

* `consecutive_extensions_broken` scored a rung as *respected* using the touch
  bar's own far extreme — which is where price came FROM on its way to the
  level — so any rung reached by a large bar counted as respected, and the
  continuation hypothesis would have had almost no sample.
* `gex_ranks_available` was computed and then never set on the snapshot, so
  every row claimed ranked levels were unavailable and the ranked-confluence
  arm would have silently reported an empty cohort.

One data-quality hazard was found that the Phase 1 assessment missed, and it
bears directly on a **5-minute** opening range: SPX and NDX print a stale
opening-rotation value at 09:30 (constituents have not opened), which
TradeStation folds into the bar's OHLC. On a gap day that phantom value IS the
bar's high or low, and on a 5-minute range that one bar is 20% of the sample —
frequently setting `ORH` or `ORL` outright and putting the entire ladder in the
wrong place. `src/tools/cash_index_open_repair.py` is production's rule for it;
the harness applies it defensively at read time rather than assuming
`make cash-index-open-repair` has been run over history. It is a no-op for
SPY/QQQ (real traded opens) and for ES/NQ (futures print continuously).

## 11. Correction — which axes are re-cohortable

An earlier draft of §7 said the lead-time and confluence-distance axes could
both be re-cohorted from a single build. Only the second is true, and the
distinction is not cosmetic: `gamma_min_lead_seconds` is passed to
`GammaTimeline.as_of` inside `dataset.build_session`, so it selects a
*different gamma snapshot per touch* and is baked into the stored confluence
distances. Sweeping it requires a rebuild per value.

| axis | changes | swept by |
|---|---|---|
| `opening_range_minutes` | the range, so the ladder, so the events | `sweep` (rebuild) |
| `extension_step` | the ladder, so the events | `sweep` (rebuild) |
| `gamma_min_lead_seconds` | which snapshot each touch sees | `sweep --lead-grid` (rebuild) |
| `confluence_distance` | only how stored distances are bucketed | `analyze` (no rebuild) |

The consequence is that the brief's required lead-time sensitivity check
(0 / 30 / 60 / 120 / 180 s) had never actually run: it was assumed to fall out
of `analyze`, and it does not. `make orgc-sweep-lead` runs it as five
rebuilds.
