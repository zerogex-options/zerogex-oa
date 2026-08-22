# Market-Maker Attributed GEX — experiment design

**Status:** research-only. Nothing in this document describes a production feature.
No production metric, table, endpoint or dashboard is changed by it.

**Question:** does SPX gamma positioning reconstructed from Cboe exchange-classified
Market Maker Open/Close activity carry materially more information than ZeroGEX's
current dealer-gamma methodology?

**Terminology (non-negotiable).** The metric is **Market-Maker Attributed GEX**.
Acceptable synonyms: *Exchange-Classified MM GEX*, *Reconstructed Market Maker
Positioning*, *Participant-Attributed Gamma*. It must **not** be called "true dealer
GEX" — exchange tags improve *attribution*, but the resulting position is still a
reconstruction resting on the assumptions listed in §3. The report renderer enforces
this in its prose and a test pins it.

---

## 1. What already exists in ZeroGEX

| Concern | Where | Used how |
|---|---|---|
| SPX chain ingestion | `src/ingestion/main_engine.py` (TradeStation) → `option_chains` | untouched |
| Chain storage | `option_chains`, `option_chains_latest`, `option_chains_archive` (retention-exempt) | read-only source of IV / gamma / OI |
| IV + Greeks | `src/ingestion/iv_calculator.py`, `greeks_calculator.py`, `src/greeks_fd.py` | reused via the chain |
| BS gamma kernel | `AnalyticsEngine._calculate_bs_gamma` — vectorised over a spot grid | **reused verbatim** |
| Dollar-GEX convention | `γ × qty × 100 × S² × 0.01` ("$ per 1% move") | **reused verbatim** |
| Dealer positioning model | `_calculate_gex_by_strike` / `_gamma_exposure_profile`: calls **+**, puts **−** | **this is the variable under test** |
| Spot-shift profile | `_gamma_exposure_profile` — re-prices every contract across `±span`, sticky-strike IV, DTE horizon-occupancy ramp | **reused verbatim** |
| Gamma Flip | `_resolve_gamma_flip` → span ladder + `_find_structural_interior_crossing` (interior margin, structural p90 floor, max-distance gate, linear interpolation, honest `None`) | **reused verbatim** |
| Dealer Gamma @ Spot | `_net_gex_at_spot` — samples the *same* curve at spot | **reused verbatim** |
| Call/Put Wall | `src/analytics/walls.py::compute_call_put_walls_with_strength` | reused unchanged (Definition A) + an MM-natural variant (Definition B) |
| Composite Score (MSI) | `src/signals/scoring_engine.py` → `signal_scores` | read-only comparand |
| Settlement / DTE | `src/market_calendar.py` — SPX 3rd-Friday AM (09:30 SOQ) vs SPXW PM (16:00) | **reused verbatim** |
| Backtest patterns | `src/tools/put_wall_magnet_backtest.py`, `src/backtesting/msi_regime_sweep.py` | pattern followed; read-only DB study |
| Replay | `/api/replay` over `gex_summary` + `gex_by_strike` | the persisted comparand |

**The enabling fact:** `AnalyticsEngine.__init__` opens no database connection and
starts no loop. It resolves the symbol, the risk-free rate and the per-symbol
dividend yield from config, and nothing else. So the experiment can construct one and
call the production gamma/flip primitives directly, with the operator's real `r` and
`q`, and zero production side effects.

---

## 2. What the experiment changes — exactly one thing

Production computes, per contract, across a hypothetical-spot grid:

```
contribution_i(S) = sign(type_i) · w_dte(T_i) · γ(K_i,T_i,σ_i; S) · OI_i · 100 · S² · 0.01
```

`sign = +1` for calls, `−1` for puts. ZeroGEX documents this openly as a *modeled*
convention: dealers long the calls customers overwrite, short the puts customers buy.
It is a proxy for something public open interest cannot reveal.

MM-attributed computes:

```
contribution_i(S) = MM_net_i · w_dte(T_i) · γ(K_i,T_i,σ_i; S) · 100 · S² · 0.01
```

No option-type sign — the sign comes from the observed position. A long option
position has positive gamma whether it is a call or a put.

### The sign-encoding identity

Black-Scholes gamma is identical for a call and a put at the same `(K, T, σ)`.
So a signed quantity can be fed to the **unmodified** production kernel by encoding
its sign as the option type:

| MM net position | synthetic row | kernel's `sign(type)` | contribution |
|---|---|---|---|
| `+N` (MM long) | option_type `C`, `open_interest = N` | `+1` | `+N·γ·…` ✓ |
| `−N` (MM short) | option_type `P`, `open_interest = N` | `−1` | `−N·γ·…` ✓ |

This is an identity, not an approximation — verified to bit-equality against a
contract-by-contract manual sum in `tests/test_mm_attributed_gex.py`. It is what lets
the flip resolver, the DTE ramp, the structural gates, the interpolation and the
numerical-stability guards run over MM positioning without one line of production
code being touched or duplicated. The synthetic row carries the contract's real
`option_symbol`, so AM-settled SPX vs PM-settled SPXW still resolves correctly.

The contract's true option type stays on the row as `mm_option_type` and is what the
strike-structure layer splits on.

---

## 3. Methodology: how MM inventory is reconstructed

### 3.1 The recursion

Per option series `(symbol, expiration, strike, call/put)`, walking buckets in time order:

```
MM_long [t] = MM_long [t-1] + MM_opening_buys  − MM_closing_sells
MM_short[t] = MM_short[t-1] + MM_opening_sells − MM_closing_buys
MM_net      = MM_long − MM_short
```

Implemented literally — but with one property made explicit, because it changes how
the result should be read.

### 3.2 The net position is invariant to open/close misclassification

Expand the recursion:

```
net = (OB − CS) − (OS − CB) = (OB + CB) − (OS + CS) = all_buys − all_sells
```

The open/close designation **cancels exactly**. This matters because the
position-effect flag is the least trustworthy field in an Open-Close feed *for the
Market Maker category specifically*: the designation originates from the clearing
position effect the originating firm marks, and market makers do not mark it with the
discipline a customer account does.

Consequences:

* `mm_net_contracts` — the only number the gamma calculation consumes — depends on
  correct **buy/sell** classification and on **complete history**, not on the
  open/close flag.
* The `long` / `short` decomposition — reported as a diagnostic — *does* depend on it,
  and should be read with that in mind.

Both estimators are computed and compared (`net_contracts` vs `net_flow_contracts`).
They can only differ by volume whose position effect was `UNKNOWN`, so any gap
directly measures the feed's effect coverage. `--net-flow-estimator` runs the whole
study on the open/close-agnostic quantity as a labelled robustness arm.

### 3.3 Left-censoring — the requirement that decides whether any of this is usable

If the Open-Close history starts after a contract was already trading, the
reconstructed *level* is missing an unknown pre-window position. This is **never**
treated as zero. Instead:

1. The series is flagged `left_censored` with a reason code.
2. A **lower bound** on the missing inventory is derived from floor breaches. If
   running long inventory reaches −50, at least 50 contracts must have existed before
   the window opened; the deepest breach over the series' life is the tightest such
   bound (`implied_prior_long` / `implied_prior_short`).
3. The confidence layer scores it, and the dataset builder either excludes it from the
   headline numbers (`clean_only`, the default) or routes it to a separately labelled
   partial-data arm (`--include-censored`).

Censoring verdicts, strongest evidence first:

| Reason | Censored? | Basis |
|---|---|---|
| `closing_volume_exceeded_observed_opens` | yes | direct proof inventory pre-existed |
| `observed_from_listing_date` | no | listing date (from ZeroGEX's own chain history) falls inside the window |
| `listed_before_data_window` | yes | listing date precedes the data |
| `first_trade_inside_window` | no | first trade falls at least `censor_buffer_sessions` inside the window |
| `first_trade_at_window_edge` | yes | first trade at the window's edge — the window itself may be why we saw no earlier trades |
| `window_too_short_to_establish_origin` | yes | window shorter than the buffer; nothing can be established |

Listing dates come from `MIN(timestamp)` per series in `option_chains`. That is a
*lower bound* on the true listing date — the contract cannot have been quoted to
ZeroGEX before ZeroGEX first saw it — which errs in the conservative direction: it can
mark a genuinely clean series censored, never the reverse.

### 3.4 Expiration

SPX is European-style and cash-settled: no early assignment, so a series simply ceases
at its settlement instant and its inventory retires. The settlement instant comes from
ZeroGEX's own `settlement_close_time_for_contract` — 09:30 ET SOQ for SPX 3rd-Friday
monthlies, 16:00 ET for SPXW and everything else — so the experiment cannot drift from
the convention the live gamma engine uses.

### 3.5 Causality

The replay only ever consumes records stamped at or before the snapshot instant. For a
session-summary feed, buckets are stamped at **16:00 ET on the trading date** — the
instant the day's aggregate flow is fully known — so an end-of-day file can never
inform a mid-session snapshot of the same day. Without this the forward tests would be
scoring readings against information they already contained.

### 3.6 Explicit assumptions

1. Cboe's participant tag identifies the *originating* account type; a market maker
   trading through another firm's booking may not be tagged `MARKET_MAKER`.
2. `MARKET_MAKER` is treated as the whole liquidity-provider population. Widening it
   (`MARKET_MAKER_LIKE`) is a separate labelled arm, never the headline.
3. Cboe C1 is one exchange. SPX is Cboe-proprietary and predominantly C1, but any
   activity off C1 is not in the reconstruction. The MM inventory is therefore a
   **C1-attributed** inventory.
4. Implied vol is held at its snapshot value across the spot shift (sticky-strike) —
   the same simplification production makes.
5. Gamma comes from ZeroGEX's own chain, so both methodologies see identical market
   data. A series with no quote is reported as unpriceable, not dropped silently.
6. Contract multiplier 100 is inherited from production, not re-derived.

### 3.7 Independent validation

The reconstruction is checked against facts ZeroGEX already stores, so a broken column
mapping shows up as a number rather than as a plausible-looking result.

**Open-interest identity.** Across *all* participant categories in one session:

```
ΔOI = Σ (opening_buys + opening_sells)/2 − Σ (closing_buys + closing_sells)/2
```

The halving is the point: one contract opened creates one unit of open interest but
appears twice in the feed. ZeroGEX stores end-of-session OI per contract, so this is
directly testable and it validates the *whole* file at once — column mapping,
participant coverage, open/close semantics and units all have to be right for it to
hold.

**Zero-sum.** Options are in zero net supply; participant net positions must sum to
zero per series. A residual measures how much of the market the feed's categories fail
to cover.

Both are diagnostic. A reconstruction that fails them is reported as failing, never
silently repaired.

---

## 4. Data requirements — exactly what to obtain

**No Cboe Open-Close file was present in the repository or environment when this was
built.** Nothing here hard-codes a column name; the mapping is a declarative
`ColumnProfile`, and `inspect-cboe` proposes one from a real header for a human to
confirm. An unconfirmed profile refuses to load (`ProfileNotConfirmed`) so a guess can
never quietly become a research result.

### 4.1 Required per row

| Concept | Required | Notes |
|---|---|---|
| Underlying symbol | yes* | *or a single-underlying file + `default_symbol` |
| Option root | strongly preferred | `SPX` vs `SPXW` decides AM vs PM settlement; without it a shared 3rd-Friday gets the wrong `T` |
| Trade date | **yes** | ET session date |
| Interval end | preferred | intraday bucket end; absent ⇒ treated as an end-of-day summary stamped 16:00 ET |
| Expiration | **yes** | |
| Strike | **yes** | flag `strike_scale=0.001` if published in thousandths |
| Call / Put | **yes** | `C`/`P` or `CALL`/`PUT` |

### 4.2 Required volume columns

At minimum, four Market Maker buckets:

```
market_maker × { BUY, SELL } × { OPEN, CLOSE }
```

Both **BUY and SELL are mandatory** — the net position is `buys − sells`, so a
one-sided feed cannot produce an inventory. OPEN/CLOSE are needed for the long/short
decomposition and for the open-interest identity; if the feed carries no position
effect, the net-flow estimator still works but §3.7's strongest check does not.

**Strongly recommended:** the same buy/sell/open/close grid for `customer`,
`professional_customer`, `broker_dealer` and `firm`. Without every category, the
open-interest identity and the zero-sum check cannot be evaluated, and the experiment
loses its only independent validation of the file.

### 4.3 History depth — the single most important requirement

The experiment is only meaningful for series whose inventory can be built from a known
zero. **Obtain Open-Close history that begins at or before each target contract's
listing date.** Concretely, for the priority universe:

| Universe | Listing lead time | History needed before the study window |
|---|---|---|
| 0DTE (SPXW dailies) | listed ~1–2 weeks ahead | ≥ 3 weeks |
| Weeklies | listed ~4–8 weeks ahead | ≥ 2–3 months |
| Near-term (≤45 DTE) | varies | ≥ 4 months |

A study window with less lead-in will report `INCONCLUSIVE_DATA` rather than a
verdict — by design, because at low coverage the experiment measures the history, not
the methodology.

### 4.4 Cadence

Intraday (30-minute or finer) is preferred: it lets the reconstruction move within a
session and makes the 5/15/30/60-minute forward tests meaningful. End-of-day summaries
work — the pipeline handles them and stamps them causally — but then every intraday
snapshot in a session carries the *previous* session's inventory, which weakens the
short-horizon tests substantially. Say which cadence the delivered files use; nothing
needs to change in the code either way.

### 4.5 What to check on delivery

```bash
python -m research.mm_attributed_gex.cli inspect-cboe <file> --save profile.json
```

Review every mapping against Cboe's field documentation, set `"confirmed": true`, then:

```bash
python -m research.mm_attributed_gex.cli check-load <files> --profile profile.json
python -m research.mm_attributed_gex.cli reconstruct <files> --profile profile.json
```

`reconstruct` prints the clean-vs-censored split and the open-interest reconciliation
verdict. If the reconciliation says `inconsistent`, stop and fix the mapping before
running anything else.

---

## 5. Architecture

```
research/mm_attributed_gex/
    schema.py       normalized ParticipantActivity (exchange-agnostic)
    cboe/
        profiles.py declarative column mapping (+ JSON round-trip)
        loader.py   csv / csv.gz / zip / parquet -> ParticipantActivity (streaming)
        inspect.py  read a real header, PROPOSE a mapping, name what it could not map
    inventory.py    ParticipantActivity -> MM long/short/net, censoring, expiration
    confidence.py   how complete/trustworthy is a reconstructed inventory
    reconcile.py    open-interest identity + zero-sum checks vs ZeroGEX data
    gex.py          MM inventory -> gamma@spot / flip / net GEX (production kernels)
    walls.py        MM inventory -> strike structure (definitions A and B, nodes)
    sources.py      READ-ONLY production database access
    dataset.py      side-by-side existing-vs-MM research dataset (two-pass replay)
    outcomes.py     forward market outcomes each reading is scored against
    stats.py        CI / Welch / HAC OLS / logit / bootstrap / permutation / walk-forward
    backtest.py     the experiment battery
    report.py       verdict logic + markdown/JSON report
    selftest.py     synthetic end-to-end plumbing check (NOT a research result)
    cli.py          python -m research.mm_attributed_gex.cli ...
```

The dependency arrow points one way: research imports from `src`, `src` imports
nothing from research. Cboe file parsing is not coupled to the GEX engine — a second
exchange's Open-Close feed means one more loader emitting `ParticipantActivity`, and
no change to any other layer.

**Performance.** Loading is streaming (a full-chain 1-minute day never materializes).
Inventory folding is one pass, `O(records)`, with dict lookups on a tuple key. The
replay is `O(records + snapshots)` with no per-snapshot rescan. Bar lookups are binary
searches. The gamma hot path is production's vectorised NumPy kernel. Nothing is
`O(N²)` across the chain.

---

## 6. Metrics produced

Per snapshot, per expiration universe (`0dte`, `nearest`, `weekly`, `near_term`, `all`):

| Experimental | Production counterpart | Notes |
|---|---|---|
| `mm_attributed_gamma_at_spot` | `net_gex_at_spot` | same curve-sampling method |
| `mm_attributed_gamma_flip` | `gamma_flip_point` | same span ladder + structural gates |
| `mm_attributed_gamma_flip_raw` | `gamma_flip_raw` | un-DTE-weighted nearest crossing |
| `mm_attributed_net_gex` | `total_net_gex` | unweighted per-strike sum, both sides |
| `mm_attributed_call_wall` / `put_wall` (A) | `call_wall` / `put_wall` | production helper, MM inputs |
| `mm_attributed_b_call_wall` / `b_put_wall` (B) | — | MM-natural definition |
| `mm_accelerant_up` / `down` | — | most-negative MM gamma; **not** a wall |

Horizon weighting is testable both ways: `apply_horizon_weighting=True` applies
ZeroGEX's existing horizon-occupancy ramp to MM positioning;
`--raw-positioning` gives raw MM positioning at full weight.

### Wall definitions

**A — existing definition, MM inputs.** `src/analytics/walls.py` called unchanged, with
`call_gamma = Σ_calls γ·MM_net` and `put_gamma = −Σ_puts γ·MM_net` so that
`call_gamma − put_gamma` equals MM net gamma at the strike. Same ranking, same
tie-breaks, same strength scale.

**B — MM-natural.** Production splits on option type because, under the modeled
convention, option type *is* the sign. With observed attribution that link is broken,
so B ranks on total MM net dollar gamma at the strike (calls and puts combined):
largest *positive* MM gamma above/below spot is the dampening wall; most *negative* is
reported separately as an accelerant, under its own name.

When MM is net long calls above spot and net short puts below — the positioning the
production convention assumes — A and B agree. Divergence is itself a measurement.

### Diagnostics on every row

`number_of_contracts`, `number_of_cleanly_reconstructed_contracts`,
`number_of_unpriceable_contracts`, `percent_of_gamma_universe_reconstructed`,
`clean_gamma_share`, `contribution_{0dte,weekly,monthly,leaps}`,
`inventory_confidence` (+ band), `mean_position_confidence`, `data_completeness`,
`mm_positive_gamma_share`, `mm_negative_gamma_share`, `mm_concentration_hhi`,
`estimator_disagreement_contracts`, `existing_recompute_parity_at_spot`.

That last one is an integrity check: when a persisted `gex_summary` row exists, the
harness *also* recomputes the production reading from the same chain and records the
relative gap. Near zero proves the research path reproduces production.

### Inventory confidence

Per series, five conjunctive factors multiplied (a product, not an average — a perfect
continuity score must not rescue an unknown starting position):

| Factor | Protects against |
|---|---|
| `origin` | inventory starting from an unknown pre-window position |
| `continuity` | sessions missing from the middle of the contract's life |
| `effect_coverage` | volume with an unknown open/close designation |
| `breach` | closing volume exceeding observed opens |
| `maturity` | a contract observed for too little of its life |

Per snapshot, the number that gates interpretation is the **|dollar-gamma|-weighted**
mean confidence, plus `percent_of_gamma_universe_reconstructed` against ZeroGEX's full
chain. Weighting by gamma rather than contract count is the point: 2,000 cleanly
reconstructed far-OTM series carrying no gamma do not make a snapshot trustworthy.

---

## 7. Evaluation

Four families, each run for both methodologies on identical rows.

1. **Gamma regime** — does `gamma@spot > 0` sort subsequent realized volatility,
   intraday range, mean reversion, trend persistence, VWAP reversion and large
   directional moves? Reported as effect sizes per measure and horizon, with the
   winner named only when the sample supports a call.
2. **Gamma flip** — above / below / crossing / at each flip, per horizon; plus how
   often the two flips put spot on the same side and how far apart they sit.
3. **Walls** — rejection, pinning, stalling and break acceleration at each wall, for
   the existing wall and both MM definitions.
4. **Incremental value** — baseline (existing ZeroGEX variables) vs baseline + MM
   variables, on identical rows: Δ adjusted R² with an F-test, HAC t-statistics,
   Δ AUC / Brier / log-loss / calibration for large-move classification, and
   walk-forward out-of-sample versions of each.

Plus **confluence**: four buckets by the sign pair of the two gamma@spot readings. If
agreement carries more information than either signal alone, the honest answer to the
central question is "useful as confirmation", and the verdict logic can reach it.

### Statistical guardrails

* **Overlap.** Forward windows overlap heavily at minute resolution; ordinary standard
  errors would overstate significance by roughly `sqrt(overlap)`. Every regression
  carries Newey-West HAC standard errors sized to the overlap; every bootstrap
  resamples contiguous blocks; walk-forward uses an embargo between train and test.
* **Multiplicity.** Dozens of tests run. P-values go through Benjamini-Hochberg as a
  family and the corrected count is what the report reads.
* **Power.** Below 200 scored observations a comparison is labelled
  `insufficient_sample` and no conclusion is drawn.
* **Confounders.** Every headline test re-runs inside control splits: 0DTE-dominant vs
  not, front DTE bands, time of day, VIX regime, OPEX, month-end, gamma concentration,
  and a high-inventory-confidence arm.

### Verdict logic (`report.decide`)

Applied mechanically, first gate wins. The first two fire **before** any effect is
examined:

1. `n_scored < 200` → `INCONCLUSIVE_SAMPLE`
2. gamma coverage < 20% or mean confidence < 0.35 → `INCONCLUSIVE_DATA`
3. in-sample **and** out-of-sample incremental gain at a majority of horizons → `YES`
   (scoped to `USEFUL_FOR_0DTE` if the effect lives only in 0DTE-dominant snapshots)
4. subset-only effect → `USEFUL_FOR_0DTE` / `USEFUL_IN_REGIMES`
5. agreement beats either alone → `USEFUL_AS_CONFIRMATION`
6. otherwise → `NO`

Gate 2 exists specifically so an incomplete Open-Close history reports as
"inconclusive — data" rather than as "no". A negative result is a first-class outcome
and the wording says so.

---

## 8. Running it

> Step-by-step operator's guide, including where to buy the data and how to read
> the report: [`docs/runbooks/mm_attributed_gex_how_to_run.md`](../runbooks/mm_attributed_gex_how_to_run.md).

```bash
# 1. Discover the real schema and confirm the mapping.
python -m research.mm_attributed_gex.cli inspect-cboe data/cboe_oc_20260601.csv \
    --save research_output/cboe_profile.json
#    ... review every line, then set "confirmed": true in that file.

# 2. Sanity-check the parse and the reconstruction.
python -m research.mm_attributed_gex.cli check-load  data/cboe/ --profile research_output/cboe_profile.json
python -m research.mm_attributed_gex.cli reconstruct data/cboe/ --profile research_output/cboe_profile.json

# 3. Build the side-by-side dataset (read-only against the production DB).
python -m research.mm_attributed_gex.cli build-dataset data/cboe/ \
    --profile research_output/cboe_profile.json \
    --start 2026-05-01T13:30:00Z --end 2026-06-30T20:00:00Z \
    --out research_output/mm_dataset.jsonl

# 4. Run the battery and render the report.
python -m research.mm_attributed_gex.cli backtest research_output/mm_dataset.jsonl \
    --out research_output/mm_report.md
```

Useful arms: `--include-censored` (partial-data), `--raw-positioning` (no horizon
weighting), `--net-flow-estimator` (open/close-agnostic quantity),
`--headline-universe 0dte`.

`pipeline-check` runs every layer on synthetic data. It validates plumbing only; its
inputs are invented, and it says so in its output.

---

## 9. Scope

**In:** SPX, Cboe C1, Market Maker activity, historical/sample Open-Close data, 0DTE,
weeklies, near-term expirations, comparison against the existing model.

**Out:** buying any dataset, a paid live Cboe feed, SPY/QQQ/NDX, dashboard changes,
customer exposure, replacing any production metric, or describing this as true dealer
positioning.

---

## 10. Findings to date

**None. No Cboe Open-Close data has been supplied, so no comparison has been run and
no result exists.** This section will hold the answer once real files are available;
until then it deliberately holds nothing, because a fabricated or extrapolated figure
here would be worse than an empty section.

### Verified end to end against production infrastructure (2026-08-22)

Every step ran on the live host against the production database — read-only — using
synthetic Market Maker flow anchored to real SPX contracts (`make mmgex-sample-anchored`).
The flow was invented, so no number below is a finding about the market; what it
establishes is that the pipeline works on real infrastructure:

| Step | Result |
|---|---|
| `check-load` | 107,264 records from 5 files, 0 skipped, 0 errors |
| `reconstruct` | 90 series; 38 clean / 52 left-censored; zero-sum residual 0.0 |
| `build-dataset` | 209 snapshots in 225s (~0.9/s); `data_completeness` 1.0, 0 session gaps |
| `backtest` | 3,531 bars scored; verdict `INCONCLUSIVE_DATA` |

The verdict is the important line. Coverage came out at 0.4% of the production gamma
universe — the anchor deliberately samples a narrow slice (15 strikes in a ±3% band,
3 expirations) so a rehearsal does not hammer the database — and the completeness gate
fired *before* any effect was examined, which is the behavior §7 specifies. An
under-reconstructed run cannot be mistaken for a negative result.

Two things that run showed which are worth carrying into a real study:

* **Left-censoring is the binding constraint, and it is visible immediately.**  Those
  were real SPX contracts expiring 8/21–8/25, listed weeks before the 5-session window
  opened; the listing-date lookup found them in the chain with a first appearance
  before the window start and correctly classified 52 of 90 as
  `listed_before_data_window`.  Five sessions of Open-Close against contracts listed
  weeks earlier yields a minority of usable series — which is §4.3's history-depth
  requirement, observed rather than argued.
* **Cost.**  ~0.9 snapshots/second at `--step-minutes 15`, plus a one-off
  ~40s listing-date scan.  A two-month study is a multi-hour run; start coarse.

What has been established, without market data:

* The experiment's gamma math is **bit-identical** to production's. The sign-encoding
  identity of §2 was verified numerically against a contract-by-contract manual sum
  (relative difference exactly zero), so any difference the study eventually reports
  is attributable to positioning, not to arithmetic.
* Every layer runs end to end (`make mmgex-pipeline-check`), on synthetic inputs whose
  flow is drawn independently of the price path. That validates plumbing only; the
  check labels itself `synthetic: true` and its numbers are properties of the
  generator, not of the market.
* The verdict gates fire in the right order. On a 120-snapshot synthetic run the
  harness returned `INCONCLUSIVE_SAMPLE` rather than a verdict — which is the intended
  behavior and the reason a thin or incomplete run cannot be mistaken for a negative
  result.

**Blocking dependency:** Cboe C1 SPX Open-Close files meeting §4, with enough history
depth (§4.3) that 0DTE / weekly / near-term contracts can be reconstructed from their
listing dates. Without that depth the study will correctly return `INCONCLUSIVE_DATA`
instead of an answer.

## 11. Known limitations

* **Single venue.** SPX is Cboe-proprietary and predominantly C1, but the
  reconstruction is C1-attributed, not market-wide.
* **Participant tag ≠ economic role.** A market maker booking through another firm may
  not carry the `MARKET_MAKER` tag; the tag identifies the originating account type.
* **Position-effect reliability.** Documented in §3.2 and worked around: the headline
  quantity is invariant to it, and the net-flow arm removes the dependency entirely.
* **Level vs change.** Left-censoring degrades the reconstructed *level* far more than
  the *changes*. A future arm could test MM gamma **deltas** even on censored series,
  where the unknown constant cancels. Not built — it would be a second experiment, and
  the brief asked for the level comparison first.
* **Session-summary cadence.** With end-of-day files every intraday snapshot in a
  session carries the previous session's inventory, which materially weakens the
  5–30 minute tests. Intraday files are worth the ask.
