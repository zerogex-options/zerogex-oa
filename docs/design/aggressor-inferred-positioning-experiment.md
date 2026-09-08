# Aggressor-Inferred vs Attributed Positioning — the three-arm experiment

**Status:** research-only. Nothing in this document describes a production feature.
No production metric, table, endpoint, dashboard or calculation is changed by it.

**Result to date:** **none.** No Cboe participant-attributed file has been supplied,
so no A / B / C comparison and no attribution result exists. Every number this
pipeline has produced so far came from synthetic inputs and is labelled as such.
See §12 for exactly what is needed to run the study.

This extends the existing Market-Maker Attributed GEX framework
([`market-maker-attributed-gex.md`](market-maker-attributed-gex.md)) with a third
positioning arm and a second, more direct experiment. Read that document first; this
one only describes what is new.

---

## 1. The question, and why it needs a third arm

The existing framework asks whether SPX positioning reconstructed from Cboe
exchange-classified Market Maker activity carries more information than ZeroGEX's
production call-positive / put-negative convention. That is a comparison of a
**convention** against an **attribution**.

Between the two sits an inference that a number of positioning products make, and
that ZeroGEX's own product surfaces describe: use the tape's aggressor classification
(buyer- or seller-initiated, from execution price against the NBBO) and *assume*
that the passive side of every print was a market maker:

```
buyer-initiated print   ->  a customer bought, so a market maker SOLD
seller-initiated print  ->  a customer sold,   so a market maker BOUGHT
```

Aggressor classification establishes which side *initiated* a trade. It does not
establish *who* was on either side. A trade at the bid can be a customer selling to a
market maker or a market maker selling to a customer; the location of the print
relative to the quote cannot distinguish those cases. That is the criticism this
experiment is built to measure rather than argue with.

So there are three arms, and two questions:

| Arm | What it is | What it is built from |
|---|---|---|
| **A — Production Modeled GEX** | the live methodology | open interest, the call+/put− convention |
| **B — Aggressor-Inferred MM GEX** | the assumption under test | ZeroGEX's tape classification + the passive-side-is-MM assumption |
| **C — Market-Maker Attributed GEX** | the exchange's classification | Cboe Open-Close Market Maker buy/sell volume |

1. **Does the aggressor assumption identify market-maker activity?** Compare B and C
   directly, bucket by bucket (§4). This puts a percentage on the criticism.
2. **Which positioning arm explains subsequent SPX behaviour best?** Run A, B and C
   through the same market-outcome battery on identical rows (§5).

Those can have different answers. C can identify market-maker activity far better
than B and still add nothing to the production model's read of what price does
next. Both outcomes are results.

### Terminology — mandatory

| Arm | Name | Never call it |
|---|---|---|
| A | **Production Modeled GEX** | measured dealer positioning |
| B | **Aggressor-Inferred MM GEX** / *Aggressor-Assumption GEX* | observed dealer flow, attributed dealer flow, dealer positioning |
| C | **Market-Maker Attributed GEX** / *Exchange-Classified MM GEX* | true dealer GEX, actual dealer book, observed dealer inventory |

Six categories that every report keeps apart: observed market data; aggressor-classified
trade direction; aggressor-inferred MM positioning; exchange-classified MM activity;
reconstructed MM inventory; production modeled dealer positioning. The report
renderer states them and a test checks that it does.

---

## 2. Phase 0 — audit of what existed (2026-09-07)

### 2.1 Already built and reused unchanged

| Concern | Where | Reused how |
|---|---|---|
| Cboe Open-Close ingestion, column profiles, human-confirmed mapping | `research/mm_attributed_gex/cboe/` | unchanged; Model C |
| Participant-normalised records | `schema.py::ParticipantActivity` | unchanged |
| MM inventory recursion, left-censoring, expiration, reconciliation, confidence | `inventory.py`, `reconcile.py`, `confidence.py` | unchanged; Model C |
| Sign-encoding identity onto the production gamma kernel | `gex.py` | **reused for B** — a signed quantity of any origin is priced identically |
| Production kernels: BS gamma, spot-shift profile, DTE ramp, flip resolver, walls | `src/analytics/main_engine.py`, `src/analytics/walls.py` | reused verbatim via `gex.py` and `walls.py` |
| Two-pass causal replay, dataset rows, forward outcomes, statistics, two-arm battery, verdict, CLI | `dataset.py`, `outcomes.py`, `stats.py`, `backtest.py`, `report.py`, `cli.py` | extended, not replaced |
| Tests | `tests/test_mm_attributed_*.py` | all 177 still pass unchanged |

### 2.2 The production aggressor classifier

`src/ingestion/main_engine.py::IngestionEngine._classify_volume_chunk`. Each option
snapshot's volume delta is classified against the prior-tick NBBO (with a staleness
guard that falls back to the contemporaneous quote, and a mid band of a configured
fraction of the half-spread): a print at or near the ask is **buyer-initiated**
(`ask_volume`), at or near the bid **seller-initiated** (`bid_volume`), anything
inside the band, the 09:30 opening auction, or a print with no usable quote is
**unclassified** (`mid_volume`). Locked markets classify by which side the print
crossed; a genuinely crossed quote falls back to nearest-neighbour and increments a
data-quality counter.

The three counters are **session-cumulative** from the 09:30 ET cash open (the
accumulator is keyed by cash-session date, the same keying `flow_contract_facts`
uses), monotonic within the session, and persisted on every `option_chains` minute
row. `ask_volume + mid_volume + bid_volume == volume` holds per row.

### 2.3 What historical data actually exists for Model B

| Store | Granularity | Buy/sell semantics | Unclassified share | Retention |
|---|---|---|---|---|
| `option_chains` | per contract, per minute (bucket START stamped) | session-cumulative ask / mid / bid counters | **preserved** | `DATA_RETENTION_DAYS` (default 90) |
| `option_chains_archive` | per contract, per minute | **no volume columns at all** | — | durable |
| `flow_contract_facts` | per contract, per minute (sparse) | per-bucket deltas; mid volume **redistributed pro-rata** into buy/sell | **lost** | operational |
| `flow_by_contract` | per contract, 5-minute | day-to-date cumulative net (buys − sells), extrapolated | lost | operational |
| `flow_series_5min` | per symbol, 5-minute | aggregate only | — | operational |

Not stored anywhere: individual prints, per-trade classification, trade size, or
any participant field. The finest reconstructable unit is one contract-minute.

**Consequences.**

* Model B can be rebuilt **contract by contract at one-minute granularity**, with the
  unclassified share intact, from `option_chains` — for sessions still inside the
  chain's retention window. That is the honest source and the default.
* Beyond retention the only per-contract record is `flow_contract_facts`, whose
  buy/sell split invented a side for the mid volume. It is supported as a labelled
  **extrapolated** fallback; sessions built from it fail the aggressor data gate
  unless the operator opts in, and the label travels into every report.
* A per-trade test of "was the passive side of *this* print a market maker" is
  **not possible** with ZeroGEX's data even if a Cboe trade-by-trade file were
  supplied: ZeroGEX has no prints to match. The finest common granularity is the
  exchange interval (1-minute at best), and the design stops there rather than
  pretending otherwise.
* Every `option_chains` row is stamped with the START of its minute and its
  counters cover every snapshot inside that minute, so the row is fully known one
  minute later. Model B carries that *known-at* instant, not the row stamp.

---

## 3. The arms — exact definitions

Everything below is priced with the **unmodified production kernels** through the
sign-encoding identity in `gex.py`: a signed quantity is encoded as a synthetic call
(positive) or put (negative) row carrying `|quantity|`, and the same
`γ · q · 100 · S² · 0.01` dollar convention, DTE horizon-occupancy ramp, spot-shift
profile, span ladder, structural crossing gates, interpolation and wall helper run
over it. The only thing that differs between arms is the quantity.

### A — Production Modeled GEX

`+open_interest` for every call, `−open_interest` for every put, from the chain
snapshot at the timestamp. Read from `gex_summary` when a persisted row exists and
recomputed from the same chain as an integrity check (`existing_recompute_parity_at_spot`).

### B1 — Aggressor-Inferred MM GEX, flow since open (`aggressor_mm_flow_*`)

Every cash session starts from **zero**. For each `option_chains` minute row of each
contract, inside `[09:30 ET, 16:15 ET)`:

```
buyer_initiated  = ask_volume − LAG(ask_volume)     (first row of the session: the counter itself)
seller_initiated = bid_volume − LAG(bid_volume)
unclassified     = mid_volume − LAG(mid_volume)

assumed_mm_delta = +seller_initiated − buyer_initiated      # aggressor.assumed_mm_delta
unclassified     -> no signed change, counted as unknown
```

accumulated per series `(underlying, expiration, strike, call/put)`. **This is not an
inventory level.** It is the inferred *change* in MM option quantity since the cash
open, and the option type never enters the sign: a long put and a long call both
carry positive gamma; the assumed MM buy/sell side decides the sign.

### B2 — Aggressor-Inferred MM GEX, production-anchored (`production_anchored_aggressor_*`)

For every chain contract with a usable implied vol at the snapshot:

```
quantity = (+OI for a call | −OI for a put) + B1 signed change for that series
```

The anchor is the open interest the production reading used at that very snapshot —
published once per session, so it is the start-of-day figure and never a next-day
value. **The starting inventory is Model A, not observed MM inventory**; B2 is a
labelled hybrid that exists so the aggressor assumption can be compared as a full
profile (gamma at spot, flip, walls). With no classified volume at all, B2 reduces
to A exactly — a test pins bit-level parity of flip, gamma at spot, net GEX and both
walls.

### C — Market-Maker Attributed GEX (`mm_attributed_*`)

Unchanged from the existing framework: the Cboe Market Maker long/short recursion
with left-censoring, confidence, reconciliation and the clean-only default. One
addition, for the dynamic family: **C flow since open** (`mm_attributed_flow_*`),
`net(t) − net(09:30 ET)` per series, computed over every live series regardless of
censoring because the unknown pre-window constant cancels in a difference.

### Data sources per arm

| Arm | Historical source | Read as |
|---|---|---|
| A | `gex_summary` (persisted) + `option_chains` / `option_chains_archive` (recompute) | read-only |
| B1 / B2 | `option_chains` ask/mid/bid counters (default) or `flow_contract_facts` (labelled extrapolated) | read-only, one cash session per statement, LAG per contract |
| C | Cboe Open-Close files (operator-supplied) + `option_chains` for listing dates and reconciliation | files + read-only |
| outcomes | `underlying_quotes` minute bars; `vix_bars` and `signal_scores` as controls | read-only |

---

## 4. Phase 2 — the direct attribution test (`attribution.py`)

*When the tape says a print was buyer-initiated, how often was the exchange-classified
Market Maker population actually selling that contract?*

**Grid.** Both feeds are folded onto the exchange feed's own interval (1-minute,
10-minute, or session), computed on the ET wall clock so the grid is DST-safe. Nothing
is ever compared across mismatched intervals; mixed intervals are refused. A
session-summary feed is compared end-of-day only and the report says intraday
identification was not testable.

**Cells.** For each `(bucket end, expiration, strike, call/put)`:

```
B signed change = seller_initiated − buyer_initiated      (assumed MM buys − sells)
C signed change = MM buys − MM sells                      (all position effects)
```

plus gross attributed activity, the unclassified B share, per-contract dollar gamma
(`γ · 100 · S² · 0.01`, from the chain row's gamma and the spot at the bucket) and
coverage flags.

**Coverage discipline.** A cell is compared only when both feeds covered the series on
that session. A series the exchange file never mentions is excluded and counted
(`b_activity_excluded_no_c_coverage`), as is a series ZeroGEX never classified
(`c_activity_excluded_no_b_coverage`). Within a covered series-session an absent cell
on either side is a real zero.

**Metrics** — each reported over *all matched cells* and over *active cells* whose
gross attributed MM activity clears a predeclared floor (10 contracts; swept over
1 / 5 / 10 / 25 / 50):

* sign agreement where both sides are non-zero; sign agreement excluding
  attributed-zero cells; agreement with zero as its own sign
* weighted sign agreement, weights `|C| × |$γ per contract|`
* Pearson, Spearman, gamma-weighted Pearson of the signed contract changes
* MAE, RMSE, MAE normalised by mean gross attributed activity, bias `mean(B − C)`
* the share of cells with zero attributed activity, so quiet buckets cannot pose as accuracy

**Confidence intervals** are session-block bootstraps (sessions resampled, never cells).

**Stratification** is fixed in advance: call/put; DTE 0 / 1–5 / 6+; ATM (|K/S−1| ≤ 0.5%)
/ OTM / ITM; first 30 minutes / midday / final hour; activity size <25 / 25–249 / ≥250
gross attributed contracts. Simple-vs-complex is reported only if the delivered records
carry such a tag; otherwise the report says it is unavailable. Strata under 30 cells
are flagged insufficient.

**Null check.** The synthetic plumbing check draws the two feeds independently and
lands at 50% sign agreement (49.7% on the current seed) — the expected null. A run of
that check reporting a strong agreement means the harness leaks.

---

## 5. Phase 3 — the A / B / C market-outcome battery

The existing two-arm families (`gamma_regime_test`, `gamma_flip_test`, `wall_test`,
`incremental_value_test`, `confluence_test`) are untouched. The `arms_*` functions
generalise the same families to every arm present, **always against the production
baseline, on identical rows** (rows are aligned across every present arm first, so a
coverage difference can never read as a methodology difference):

| Family | Function | What is compared |
|---|---|---|
| Gamma regime | `arms_regime_test` | sign of gamma at spot vs subsequent realized vol, absolute return, range, mean reversion, trend persistence, VWAP reversion, large moves; effect sizes per measure and horizon; "better" only when the sample supports it |
| Gamma flip | `arms_flip_test` | above / below / crossing / at each arm's flip; same-side rate and flip gap vs production |
| Walls | `arms_wall_test` | identical touch and break definitions; rejection, hold, stall, break acceleration; definition A and the MM-natural definition B where the arm has one |
| Incremental value | `arms_incremental_test` | baseline (production variables) vs baseline + each arm's variables: Δ adjusted R², F-test, HAC t-statistics, AUC / Brier / log-loss, walk-forward out-of-sample Δ R² |
| **Dynamic hedge pressure** | `hedge_pressure_test` | the flow arms B1 and C-flow-since-open: sign of gamma-weighted flow since open vs forward realized vol and signed return; the flow's *change* over the last three snapshots (same session) regressed on forward outcomes with HAC errors |
| Development / validation | `validation_split_test` | the regime and incremental families re-run on a chronological 60 / 40 split of **sessions**, carved only when ≥ 40 sessions exist |

Static GEX is never treated as a directional price forecast; the families test
volatility amplification / dampening, mean reversion / persistence, pinning and hedging
response, as the design has always done.

---

## 6. Look-ahead protections

All of the existing protections stand (session-summary Cboe buckets stamped 16:00 ET;
the replay consumes only records at or before the snapshot; outcomes strictly forward;
listing-date floor treated as unknown). Added for Model B:

* A minute row enters the B book only at its **known-at** instant (bucket end).
* The B book **resets at every cash open**; a snapshot whose session has produced no
  bucket yet sees an empty book, so day one's flow cannot leak into day two.
* B2 anchors on the open interest in the chain **at the snapshot** — the figure the
  production reading used — never on a later OI publication.
* The C flow-since-open arm differences against the book **frozen at 09:30 ET** of
  the same session, an instant the replay is asked to freeze explicitly.
* The attribution grid ceils each minute row to the bucket that ends at or after it;
  a 09:41 row can never inform a 09:40 exchange bucket (tested).
* No forward-filling across intervals; no future bars in any predictor.
* The B replay is an **ex-post reconstruction from persisted minute rows**. A row's
  counters can be raised by a snapshot that arrived within the drain interval after
  the bucket closed, so "known at bucket end" is exact for the persisted record and
  approximate, by up to one drain interval, for a live claim. The report labels B as
  historical attribution, not as a series that was available live.

---

## 7. Statistics and the three-arm verdict

The toolkit is the existing `stats.py`: Welch tests with effect sizes, block bootstraps
sized to the forward-window overlap, Newey-West HAC regressions, IRLS logit,
walk-forward with an embargo, Benjamini-Hochberg over the whole family (now including
the arms and hedge-pressure families). Sessions are the resampling unit wherever a
bootstrap crosses sessions.

`report.decide_arms` is mechanical, and its thresholds (`ArmThresholds`) are fixed in
code before any validation segment is read:

| Threshold | Default |
|---|---|
| scored observations | ≥ 200 |
| materially better: mean walk-forward Δ OOS R² (realized vol) | ≥ 0.005 at a majority of horizons |
| materially better: share of decided regime comparisons won vs production | ≥ 60% (read only with ≥ 6 decided comparisons) |
| materially worse | the mirror image |
| aggressor data gate | B present on ≥ 50% of scored rows, mean classified share ≥ 0.5, no extrapolated rows |
| attributed data gate | gamma coverage ≥ 20%, mean inventory confidence ≥ 0.35 |
| validation | a full-sample winner must keep its direction on the validation segment |

Decision order, first gate wins: `INCONCLUSIVE` (sample) → `INCONCLUSIVE_DATA` (no
alternative arm, or every arm fails its data gate) → `ATTRIBUTED_BETTER` /
`AGGRESSOR_BETTER` (an arm clears both floors and the validation segment agrees; ties
broken by Δ OOS R²) → `INCONCLUSIVE` (clears the floors in-sample but reverses on
validation) → `PRODUCTION_BETTER` (every evaluable arm is materially worse) →
`PRACTICALLY_EQUIVALENT`. A winner is never forced.

The existing two-arm verdict (`decide`) is unchanged and still rendered.

---

## 8. Data-quality gates for Model B

Per session, before any B reading enters the headline columns
(`AggressorGateConfig`, predeclared):

| Gate | Default |
|---|---|
| classified share `(buyer + seller) / total` | ≥ 0.50 |
| distinct contract-minute buckets | ≥ 50 |
| distinct series with classified volume | ≥ 10 |
| extrapolated source | refused unless `--allow-extrapolated` |

A failing session contributes diagnostics only (`aggressor_session_gate_passed = False`
with the reasons on the row and in provenance); its arm columns stay `None`, so a
poorly classified tape cannot enter a headline result. Every row also carries: buckets
and series observed, buyer / seller / unclassified shares, series matched to the chain
and unmatched, unpriceable series, locked and crossed quote buckets, and the source.

---

## 9. Outputs

| File | Content |
|---|---|
| `research_output/aggressor_buckets.jsonl` (+ `_coverage.json`) | Model B contract-minute buckets and per-session coverage / gates |
| `research_output/attribution_report.md` / `.json` / `_cells.csv` | the direct B-vs-C answer: headline, sensitivity, strata, session-bootstrap CIs, every matched cell |
| `research_output/mm_dataset.jsonl` / `.csv` / `_provenance.json` | one row per snapshot with A, B1, B2, C, C-flow, diagnostics, controls, per-universe detail |
| `research_output/mm_report.md` / `.json` | the two-arm report as before, plus §8 "Three-arm comparison" (static families, hedge pressure, validation split, data gates) and both verdicts |

The markdown never collapses the six categories of §1, and it never uses the
forbidden terms; tests check the rendered prose.

---

## 10. Tests

```bash
make mmgex-test          # pytest tests/ -k mm_attributed
```

The existing 177 tests pass unchanged. New, all on hand-checkable synthetic inputs:

* `tests/test_mm_attributed_aggressor.py` — side mapping for calls **and** puts
  (buyer → negative, seller → positive, unclassified → nothing, option type never
  flips it); session reset and refusal of out-of-order sessions; causal replay
  (exact-stamp included, later excluded, empty before the first bucket of a new
  session); anchoring (no flow → exactly ±OI; flow → anchor + change; unmatched
  series counted); **B2 without flow reproduces production's flip, gamma at spot,
  net GEX and walls to 1e-12**; DST-safe interval ends on EST and EDT dates;
  aggregation never leaks backward; gates; JSONL round trip; the database readers
  against a fake cursor (known-at shift, field mapping, 09:30 / 16:15 ET bounds,
  extrapolated labelling).
* `tests/test_mm_attributed_attribution.py` — perfect agreement, complete
  disagreement, attributed zero-flow cells, one large gamma-weighted disagreement
  dominating the weighted rate, missing coverage on each side excluded not zeroed, a
  quiet tape bucket inside a covered series-session as a real zero, the active floor,
  MM-only records, session-summary feeds disabling intraday claims, mixed intervals
  refused, no backward leak, the fixed strata families, terminology and the three
  output files.
* `tests/test_mm_attributed_arms.py` — the builder with a tape and no exchange file,
  B2 equal to A on an unclassified tape, classified flow moving B2, a gated-out
  session carrying diagnostics only, the cash-open reset inside the dataset, all
  three arms in one row, C flow since open differencing against 09:30 (not the window
  start) and recording a full reversal; the battery reporting every arm on identical
  rows, running without C, carving a validation segment by sessions; every verdict
  path of `decide_arms`; the rendered section keeping the categories apart.

---

## 11. Running it

Everything runs from the repository root, read-only against the database. The
`make` targets wrap the commands below.

```bash
# 0. Plumbing only — synthetic inputs, never a result.
make mmgex-pipeline-check

# 1. Extract ZeroGEX's own aggressor-classified tape for the window (Model B).
#    One cash session per statement; expect minutes for a multi-month window.
make mmgex-aggressor START=2026-06-01T13:30:00Z END=2026-08-29T20:00:00Z
#    -> research_output/aggressor_buckets.jsonl + _coverage.json (per-session gates)

# 2. A-vs-B today, no Cboe file needed.
make mmgex-dataset-ab START=2026-06-01T13:30:00Z END=2026-08-29T20:00:00Z \
    MMGEX_AGGRESSOR=research_output/aggressor_buckets.jsonl
make mmgex-backtest
#    The three-arm section reports Production vs Aggressor-Inferred; the attributed
#    arm is absent and the report says so.

# 3. With real Cboe files: confirm the mapping exactly as before.
make mmgex-inspect MMGEX_FILE=<cboe-file>.csv
make mmgex-confirm REVIEWED=yes
make mmgex-check-load  MMGEX_FILES=<cboe-dir>/
make mmgex-reconstruct MMGEX_FILES=<cboe-dir>/

# 4. Phase 2 — the direct attribution test (B vs C).
make mmgex-attribution MMGEX_FILES=<cboe-dir>/ MMGEX_AGGRESSOR=research_output/aggressor_buckets.jsonl
#    -> research_output/attribution_report.md / .json / _cells.csv

# 5. Phase 3 — the A / B / C dataset and battery.
make mmgex-dataset-abc MMGEX_FILES=<cboe-dir>/ START=... END=... \
    MMGEX_AGGRESSOR=research_output/aggressor_buckets.jsonl
make mmgex-backtest
#    -> research_output/mm_report.md with both verdicts
```

Equivalent `python -m research.mm_attributed_gex.cli` commands: `build-aggressor`,
`compare-attribution`, `build-dataset [files] --aggressor <jsonl>`, `backtest`.

Use identical session coverage across arms for the headline comparison: build one
dataset over one window with both inputs. The battery aligns rows across arms itself;
it does not compare A on one hundred days with C on forty and call the difference
methodology.

---

## 12. What is still needed to produce a result

1. **Cboe C1 SPX Open-Close files** meeting §4 of the existing design document —
   Market Maker buy and sell volume at minimum, all participant categories preferably,
   intraday (10-minute or finer) cadence preferably — with **history reaching back to
   each target contract's listing date** (≥ 3 weeks before the window for 0DTE, 2–3
   months for weeklies, 4 months for ≤ 45 DTE). Without that lead-in the attributed arm
   reports `INCONCLUSIVE_DATA`, correctly.
2. **The study window inside `option_chains` retention.** Model B needs the
   session-cumulative counters, which the archive does not carry. Extract the tape
   (`build-aggressor`) as soon as a window is chosen, and keep the JSONL: it is the
   durable record of the classified tape once the chain rows age out.
3. **Sixty sessions minimum, one hundred or more preferred**, SPX only, 0DTE and
   near-term universes first, identical sessions across arms for the headline.

Without a Cboe file the pipeline still answers the A-vs-B question on ZeroGEX's own
data and produces the B data-quality picture. It cannot say anything about attribution.

---

## 13. Known weaknesses

* **Minute granularity.** ZeroGEX stores no prints, so B is a contract-minute
  aggregate; a minute with both buyer- and seller-initiated volume nets inside the
  cell. The exchange feed is also an interval aggregate, so the comparison is fair,
  but a per-trade test of the assumption is out of reach with this data.
* **The classifier itself.** Lee-Ready with a mid band against a one-minute
  snapshot NBBO is a good, not perfect, classifier of initiation. Its own error is
  inside B and is not separable from the identity assumption in the B-vs-C comparison;
  the unclassified share and the locked / crossed quote counts are reported so that
  contribution can be bounded, not removed.
* **C1-tagged population.** C identifies the market makers the exchange tagged on C1;
  a market maker booking through another firm, or trading elsewhere, is not in C.
  Disagreement between B and C includes that coverage limit.
* **B2 is a hybrid.** Its starting inventory is the production convention. If the
  convention is wrong at a strike, B2 inherits that error and adds the day's assumed
  flow on top. That is stated on every surface; it is still the only way to give the
  aggressor assumption a full profile without inventing an inventory.
* **Retention.** B history is bounded by the chain's retention window. Extending
  retention or persisting the classified deltas durably is a production decision, not
  part of this research package.
* **Live availability.** B is reconstructed from persisted rows; a live series would
  lag by up to one drain interval (§6).
* **No result yet.** All of the above describes an instrument that has not been
  pointed at real attributed data. Nothing in it should be read as evidence for or
  against any arm.
