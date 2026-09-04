# Does the MSI regime gauge predict realized price excursion?

**Status:** structural arm complete with a result. Empirical arm complete, tested, and
**unrun** — no database was reachable from the environment it was built in, so no
market data has been through it.

**Code:** [`research/msi_regime_excursion/`](../../research/msi_regime_excursion/)
**Run it:** [`docs/runbooks/msi_regime_excursion_how_to_run.md`](../runbooks/msi_regime_excursion_how_to_run.md)

---

## 1. Where this came from

A trader trialled ZeroGEX, cancelled at day 7, and then answered a founder follow-up
with a specific, falsifiable claim. He is an intraday futures trader — scalps bonds on
microstructure and order flow, wanted to add ES:

> With price micro structure analysis, I track price targets constantly and it can be
> consistent for a while (4 point, 8 points, 10 points most typically). But it had no
> connection with regime at all. Not just the gamma flip. That is my main concern.

Stated precisely: **realized price excursion magnitude is independent of the regime
read.** The product asserts the opposite, explicitly, in customer-facing copy.

`content/methodology.md` §5 already commits the product to settling this kind of
disagreement with data:

> Compare against appropriate baselines. A signal has to beat a reasonable null: the
> unconditional base rate, a simpler construction, or the existing production method.
> "Better than nothing" is not a result.

This is the first time that standard has been pointed at the central signal.

## 2. What is being tested

`frontend/core/regime.ts` bands the 0–100 Composite Score / MSI, and each band's copy
is a claim about how far price travels:

| Band | Score | Copy | Reading as an excursion claim |
|---|---|---|---|
| `trend_expansion` | ≥ 70 | "Strong directional regime — favor trades in the prevailing bias." | excursion **above** base rate |
| `controlled_trend` | 40–70 | "Moderate directional edge — trade with reduced size." | **above** |
| `chop_range` | 20–40 | "Range-bound — fade extremes, avoid trend trades." | **below** |
| `high_risk_reversal` | < 20 | "Mean-reversion only — extreme move risk elevated." | **below** |

`bands.py` carries that table and is asserted, boundary by boundary, to agree with
`ScoringEngine._regime_label` — so the study can never end up measuring bands the
product does not show anyone.

## 3. The structural finding — no data required

Before asking what the market did, it is worth asking what the number *is*.

The composite is a **signed** sum, `sum_offset = Σ(points_i × score_i)`, mapped through
`50 + 50·tanh(sum_offset / 50)` (`src/signals/scoring_engine.py`). There is no absolute
value anywhere in that path. Every component enters on a signed [−1, +1] axis, and
`ComponentBase` documents that axis as "+1.0 = strongly bullish, −1.0 = strongly
bearish".

But the components are not all on the same axis. Each one's own docstring says which:

| Component | Points | What its own docstring says +1 means | Axis |
|---|---:|---|---|
| `gamma_anchor` | 30 | "price is 'free' … → expect movement"; −1 "anchored → expect chop/pinning" | magnitude |
| `net_gex_sign` | 16 | short gamma, "amplifies volatility" | magnitude |
| `volatility_regime` | 6 | `(vix − 20) / 10` | magnitude |
| `order_flow_imbalance` | 19 | "call premium dominates (**bullish** model output)" | **direction** |
| `dealer_delta_pressure` | 17 | dealers short delta → "**bullish** for price" | **direction** |
| `put_call_ratio` | 12 | comment claims "larger potential move"; formula `(pcr−1)/sat` is the standard bearish-sentiment gauge | ambiguous |

`order_flow_imbalance`'s docstring is explicit that this is what it is: "a
**directional** house heuristic lifted into the MSI composite".

So **at least 36 of 100 points are a direction read on a scale whose bands are labelled
as a regime**. Meanwhile `frontend/core/impliedDirection.ts` says of the same number:

> The MSI itself is a *regime* gauge (0–100) … **It is deliberately directionless.**

### The experiment

`research/msi_regime_excursion/structural.py` drives the **real production
`ScoringEngine`** with the real components. Every gamma-structure input — net GEX, the
gamma flip, local gamma, the max-gamma strike, VIX, the put/call ratio, the price path
— is held **identical across the sweep**. The only things that vary are `smart_call` /
`smart_put` and `dealer_net_delta`: the direction of flow.

If the MSI were directionless, the band could not move.

**Neutral gamma structure** (no magnitude component saturated — the ordinary state of
the market):

| flow skew | MSI | Band |
|---:|---:|---|
| −1.00 (all put premium) | 24.78 | Chop / Range — *"Range-bound — fade extremes, avoid trend trades."* |
| −0.75 | 28.08 | Chop / Range |
| −0.50 | 31.64 | Chop / Range |
| −0.25 | 44.52 | Controlled Trend |
| 0.00 | 62.60 | Controlled Trend |
| +0.25 | 70.68 | Trend / Expansion |
| +0.50 | 80.69 | Trend / Expansion |
| +0.75 | 83.20 | Trend / Expansion |
| +1.00 (all call premium) | 85.44 | Trend / Expansion — *"Strong directional regime — favor trades in the prevailing bias."* |

**60.7 MSI points and three regime bands, on direction alone.** Repeated with the
structure pinned at both extremes:

| Structure held fixed at | MSI range | Span | Bands crossed |
|---|---|---:|---|
| pinned (every magnitude signal says "damped") | 3.20 → 37.08 | 33.9 | High-Risk Reversal → Chop / Range |
| neutral | 24.78 → 85.44 | **60.7** | Chop / Range → Controlled Trend → Trend / Expansion |
| free (every magnitude signal says "moves can run") | 66.75 → 97.28 | 30.5 | Controlled Trend → Trend / Expansion |

### What this means

Two consequences, and the second is the one that matches the customer's report.

**The bands are directionally asymmetric.** A hard *down* trend — put premium bought
aggressively, dealers accumulating long delta — subtracts up to 36 points and lands in
`chop_range` or `high_risk_reversal`: "Range-bound — fade extremes, avoid trend
trades." Those are frequently the largest-excursion sessions in any sample. The gauge
can only report "trend" for *up*-trends.

**"Regime" and "direction" are being read off one axis.** A single scalar cannot
independently encode "how far" and "which way". Whatever regime information the
magnitude components carry is being summed with, and partially cancelled by, a
direction read — which is a coherent reason for a trader to find that his price targets
"had no connection with regime at all". He was not comparing his targets against a
travel forecast. He was comparing them against a number that is roughly a third
sentiment.

This is a property of the shipped code and it is now pinned by tests
(`test_flow_direction_alone_moves_the_regime_band`,
`test_the_neutral_structure_crosses_three_bands_on_direction_alone`).

## 4. The empirical arm

### Design

For each persisted reading in `signal_scores` (≈ 1/minute per underlying, carrying
`composite_score`, `direction`, and the per-component payload), measure what price did
next.

**Horizons.** 5 / 15 / 30 / 60 minutes and rest-of-session, always reported separately.
The MSI is a session-level statistic and the customer trades scalps; if it holds at
session scale and fails at scalp scale, that is a **positioning** finding, not a
validation failure, and the report has to be able to say so.

**Measures.** Three families, because the copy makes three different kinds of claim:

- *Directionless magnitude* — `max_up`, `max_down`, `range`, `abs_ret`. What "trends
  can run" versus "range-bound" is a claim about.
- *Bias-conditioned* — MFE / MAE against the prevailing bias, read from the prior 30
  minutes exactly as `impliedDirection.ts` defines it. "Favor trades in the prevailing
  bias" is a claim about a trade taken in the direction of the existing move, so it has
  to be scored against a direction, and that direction must come from information the
  reading already had.
- *Point targets* — P(a 4 / 8 / 10 point run in ES), by band. The customer's own units.

**Baseline.** The **unconditional base rate** for the same instrument and horizon, with
the bucket included in its own baseline. Bucket-versus-complement is an easier and
different question; §5 of methodology.md names the unconditional rate.

**Buckets.** The four bands, and score deciles — deciles catch a monotone relationship
the four coarse bands would miss, and would show a gauge that carries real information
banded at the wrong thresholds.

**Alternative constructions.** methodology.md also names "a simpler construction", so
the same scoring runs against:

- `msi_folded` — `|msi − 50| × 2`. The cheapest possible repair: report distance from
  neutral in either direction.
- `msi_magnitude` — rebuilt from the magnitude components only, using the engine's own
  renormalization rule for a partial component set.
- `msi_magnitude_pcr` — the same, plus the ambiguous `put_call_ratio`.
- `msi_direction` — rebuilt from the directional components only. **The negative
  control.** If it tracks excursion magnitude as well as the shipped MSI does, the
  shipped score's apparent regime content is direction wearing a regime label.

### The statistical trap

This is the part most likely to produce a wrong answer, so it is worth stating plainly.

Readings land about once a minute and forward windows overlap: a 30-minute measure at
10:00 shares 29 of its 30 minutes with the one at 10:01. Regime bands also persist for
long stretches within a session. A row-level test believes it has tens of thousands of
independent observations when it has closer to one per session.

Measured, under a **true null** with exactly that correlation structure:

| Test | Rejection rate at α = 0.05 |
|---|---:|
| Welch t over rows | **65%** |
| Session-level block bootstrap | **5.8%** |

A row-level significance test on this data is about **13× over-confident**. Every
headline p-value here is a session-level block bootstrap; the naive p-value is reported
beside it so the gap stays visible. Effect size is Cliff's delta, which is rank-based
and needs no distributional assumption — excursion is non-negative and heavy-tailed.
Instruments × horizons × measures × bands is several hundred tests, so every p-value in
a run goes through Benjamini-Hochberg together.

### Verdict logic

Written down in `report.py` rather than applied by eye. `supported` requires all three:

1. **Does the score order excursion at all?** Spearman ρ with a session-level interval,
   and |ρ| ≥ 0.05 — with tens of thousands of rows, detectable and useful are very
   different things.
2. **Do bands beat the unconditional base rate by a material amount?** |Cliff's delta|
   ≥ 0.147, the conventional floor for "not negligible".
3. **Do the bands run in the right order?** Trend / Expansion must show more travel than
   Chop / Range. A gauge whose bands are ordered backwards is worse than one that does
   nothing, because it is confidently wrong — and the report says `INVERTED` rather
   than reporting the absolute value of a relationship.

### Validation

The pipeline is checked end to end against three synthetic worlds whose answers are
known by construction (`selftest.py`), each 28 sessions of autocorrelated minute data:

| World | Built-in relationship | ρ found | Verdict returned |
|---|---|---:|---|
| `signal` | excursion scale rises with MSI | +0.708 | `supported` ✓ |
| `null` | excursion independent of MSI | +0.015 | `not supported` ✓ |
| `inverted` | excursion falls as MSI rises | −0.714 | `INVERTED — bands run backwards` ✓ |

Synthetic data verifies the machinery and is **never used as evidence about the
market**.

### Instruments, and what ES/NQ actually are

| Instrument | Score from | Bars from |
|---|---|---|
| SPY, SPX, QQQ, NDX | own `signal_scores` | `underlying_quotes` |
| ES | **SPX** `signal_scores` | `futures_quotes` (`index_symbol = 'SPX'`) |
| NQ | **NDX** `signal_scores` | `futures_quotes` (`index_symbol = 'NDX'`) |

ES and NQ have no MSI of their own. `src/jobs/futures_projection.py` states that
projection is read-side only — "no projected value ever reaches GEX, greeks, signals,
settlement or any DB write" — and `es_nq_futures_rollout.md` lists scores among the
fields that are **not** projected. An ES trader reading the regime gauge is reading the
SPX score with ES prices on the chart.

That is worth being explicit about, because it means the ES arm is not a
convenience approximation: measuring the SPX-derived score against the ES tape is
measuring exactly what an ES customer is shown. It is also why the customer could not
have run this himself — `/backtesting` is SPY/SPX/QQQ/NDX only, and measures option
round-trip P&L rather than excursion.

### Why the existing backtester could not answer this

`/backtesting` splits results *by* regime but measures **option round-trip P&L** —
premium entries and exits, slippage, commission. Related, but confounded by option
pricing and implied volatility, and not a measurement of how far the underlying
travelled. `src/backtesting/msi_regime_sweep.py` is closer: it scores continuation
versus reversal by band. It is a good instrument for the question it asks, and it does
not answer this one, because it has no excursion measures (only the signed return at
the window's end), no comparison against the unconditional base rate, no effect size,
no significance test, no deciles, no rest-of-session horizon, and no ES/NQ.

## 5. Result

**The structural arm has a result and it is reported in §3.**

**The empirical arm has produced no numbers.** The pipeline was built and validated in
an environment with no reachable database (`pg_isready`: no response; no `.env`; no
`DB_*` in the environment), and no archived extract exists in either repository. There
is therefore **no finding yet** about whether the bands beat the base rate.

That is a statement about where the work stopped, not a result. It must not be read as
"the signal held" or as "the signal failed". Running §3 of the runbook against the
archive produces the answer; the answer belongs in this section when it exists.

### What is already known about the size of the window

`DATA_RETENTION_DAYS` defaults to 90 and prunes `signal_scores`, `underlying_quotes`
and `futures_quotes` alike, so the deepest available history is about a quarter unless
retention was raised in advance. `src/tools/futures_backfill.py` can extend ES/NQ bars,
but its own docstring warns that anything loaded outside the retention window is
deleted on the next prune unless `FUTURES_BARS_RETENTION_DAYS` is raised **first**.
Check with `cli describe` before assuming a window exists.

## 6. Recommendation

Two of these do not depend on the empirical result. The third does.

**1. Fix the description, or fix the number — but the current pair cannot both stand.**
§3 establishes without any market data that the MSI is not "deliberately directionless"
and that its bands move on direction alone. Either:

- *Describe it as what it is* — a blended positioning-and-direction score — and stop
  labelling its bands with travel claims; or
- *Make it what the copy says* — score the bands off a magnitude-only composite
  (`msi_magnitude`, already implemented), keep the directional components as the
  separate directional read they are already documented to be, and let
  `impliedDirection.ts` do the job it was written for.

The second is the smaller change and the one the code is already shaped for: the
directional components are individually documented as directional, and the frontend
already has a separate direction overlay. The two reads are being summed into one
scalar and then split apart again downstream.

**2. Whatever the empirical arm returns, the copy needs the honest verb.** "Trends can
run" is a prediction. If the bands do beat the base rate, the copy can say so with the
effect size attached. If they do not, the fix is the description, not the number: a
positioning gauge that describes the dealer book is a legitimate product; a positioning
gauge whose bands promise travel it does not deliver is not.

**3. Do not change customer-facing copy on the strength of §3 alone.** §3 shows the
scale is contaminated. It does not show the bands fail to predict excursion — a
contaminated scale can still be correlated with travel. The correction the bands need
depends on which of `msi`, `msi_folded` and `msi_magnitude` actually orders excursion,
and that is exactly what the unrun arm decides.

## 7. Second question: the CVD / limit-order framing

The customer's model: aggressive orders → CVD → "50% of the picture"; the other half is
resting limit orders. Assessed separately in
[`docs/design/futures-aggressor-delta.md`](futures-aggressor-delta.md).
