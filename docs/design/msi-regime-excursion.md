# Does the MSI regime gauge predict realized price excursion?

**Status:** complete. Both arms have run. Headline: the regime bands **do** order
realized excursion, in the claimed direction, on every instrument and horizon — but
weakly, and a magnitude-only rebuild of the composite beats the shipped one on all 30
instrument-horizon cells. See §5.

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

**Run:** 2026-06-29 to 2026-09-03, 141,210 readings across six instruments, 30-47
sessions each. Bars were the binding constraint, not scores: cash bars begin
2026-06-29 and futures bars 2026-07-06, so April-June scores had nothing to
measure against.

### The customer's claim does not survive

GP asserted that realized excursion "had no connection with regime at all." It has a
connection, in the right direction, at every horizon, on every instrument.

**Band ordering is correct everywhere.** Mean forward range as a ratio of the
unconditional base rate, 15-minute horizon:

| | reversal | chop | controlled | trend |
|---|---:|---:|---:|---:|
| SPY | 0.81 | 0.97 | 1.04 | 1.09 |
| SPX | 0.86 | 0.89 | 1.07 | 1.19 |
| QQQ | 0.86 | 0.94 | 1.06 | 1.09 |
| NDX | 0.89 | 0.90 | 1.16 | 1.77 |
| ES | 0.86 | 0.89 | 1.07 | 1.19 |
| NQ | 0.91 | 0.90 | 1.14 | 1.77 |

Monotone in every row (NQ's bottom two are inverted by 0.01, which is noise). No
instrument at any horizon produced an `INVERTED` verdict.

**In the customer's own units** — P(ES travels at least 10 points), by band:

| horizon | reversal | base | trend |
|---|---:|---:|---:|
| 15m | 8.5% | 13.7% | 19.2% |
| 30m | 17.0% | 24.9% | 31.5% |
| 60m | 27.7% | 36.6% | 44.4% |

A 10-point ES run in 15 minutes is **2.3x more likely** in the top band than the
bottom. That is not nothing, and it is exactly the question he was asking.

**1,386 of 4,140 tests survive Benjamini-Hochberg at 5%** — 33% against a 5% chance
expectation.

### But the effect is small, and that matters

Cliff's delta is mostly 0.10-0.23: "small" by the conventional labels. Spearman rho
between the MSI and forward range is +0.20 to +0.24 on SPX and ES, and only +0.06 to
+0.14 on SPY and QQQ, where it does **not** survive BH correction.

So the gauge orders excursion, but weakly enough that a discretionary trader
eyeballing charts could reasonably fail to see it. GP's conclusion was wrong; his
experience was not unreasonable.

The exception is NDX/NQ `trend_expansion`, where the ratio is 1.77 and Cliff's delta
reaches 0.59 — a large effect. That band holds only 622 readings over 30 sessions, so
it is the least reliable cell in the study and the first thing to re-check on a longer
window.

### The structural finding is confirmed, and the repair is measured

Section 3 predicted, from code alone, that the directional components contaminate the
scale. The data agrees, and quantifies the cost.

**`msi_direction`, the negative control, carries no excursion information at all** --
rho between +0.03 and -0.18, centred near zero. The two components worth 36 of the
100 points contribute nothing to what the bands claim to measure.

**`msi_magnitude_pcr` beats the shipped MSI on all 30 instrument-horizon cells.**
Spearman rho vs forward range:

| | 5m | 15m | 30m | 60m | session |
|---|---:|---:|---:|---:|---:|
| SPY msi / mag+pcr | .097 / **.125** | .104 / **.131** | .090 / **.117** | .075 / **.098** | .085 / **.093** |
| SPX msi / mag+pcr | .212 / **.254** | .230 / **.268** | .230 / **.264** | .228 / **.251** | .197 / **.241** |
| QQQ msi / mag+pcr | .097 / **.137** | .098 / **.141** | .082 / **.123** | .058 / **.097** | .141 / **.217** |
| NDX msi / mag+pcr | .191 / **.336** | .211 / **.362** | .195 / **.369** | .158 / **.358** | .149 / **.271** |
| ES msi / mag+pcr | .222 / **.259** | .235 / **.266** | .233 / **.260** | .238 / **.256** | .199 / **.237** |
| NQ msi / mag+pcr | .194 / **.345** | .206 / **.361** | .186 / **.362** | .150 / **.350** | .152 / **.275** |

Improvement runs from +13% (ES) to +75% (NQ). Thirty cells, thirty wins.

Two refinements the run settled that the code analysis could not:

* **`put_call_ratio` is load-bearing, not ambiguous.** `msi_magnitude` *without* PCR
  loses to the shipped MSI on SPY and QQQ at most horizons (QQQ 60m: .002 vs .058).
  With PCR it wins everywhere. Whatever PCR is measuring, it belongs in the regime
  read.
* **Folding is the wrong repair.** `msi_folded` is *negative* at every cell
  (-0.02 to -0.14). Distance-from-neutral is not the missing structure; dropping the
  directional components is.

### A defect this run surfaced that nobody was looking for

The `reconstructible` column splits the instruments cleanly in two:

| SPX | ES | SPY | QQQ | NDX | NQ |
|---:|---:|---:|---:|---:|---:|
| 97.9% | 97.8% | 54.9% | 55.4% | 57.9% | 57.9% |

A row fails to reconstruct when components **abstained** -- the composite was built
from a partial set and renormalized onto the full 100-point scale. On SPY, QQQ, NDX
and NQ that is happening on roughly **45% of readings**.

Those are exactly the four instruments whose rho fails to survive BH correction, while
the two clean instruments (SPX, ES) both survive at every horizon. The likely cause is
that SPY and QQQ persist ~800 readings/session against SPX's ~350 -- i.e. they score
through extended hours, where the options tape is too thin to feed the components. That
is a hypothesis this run did not test; see §6.

## 6. Recommendation

The empirical arm has now run, so these are ordered by what the data supports.

**1. Ship `msi_magnitude_pcr` as the band source. This is the strongest result in the
study.** Thirty of thirty cells improve, the negative control confirms the mechanism,
and the alternative was rebuilt with the engine's own renormalization rule rather than
a new formula. Do it as an **additive** change: persist the variant alongside the
shipped composite, backtest the re-gating offline, then switch the bands. The blast
radius is 17 playbook patterns that gate on `valid_regimes` / `preferred_regime`, plus
the portfolio engine and eight frontend surfaces -- this is not a copy change.

**2. Correct the "deliberately directionless" claim in
`frontend/core/impliedDirection.ts` now.** It is an internal spec comment, not customer
copy, it is measurably false, and it costs nothing to fix.

**3. Keep the band copy, but attach the effect size.** The bands do order excursion in
the direction they claim, so the copy is not a false promise -- it is an
unquantified one. "Strong directional regime" is fair for a band that precedes 1.19x
the base range on SPX and a 2.3x higher chance of a 10-point ES run. Consider stating
the number rather than the adjective.

**4. Investigate the 45% abstention rate on SPY/QQQ/NDX/NQ before anything else.**
This may be the larger prize. If the cause is scoring through extended hours on a thin
options tape, the fix is a session gate, not a model change -- and it would plausibly
lift those four instruments toward the SPX/ES relationship, which is roughly twice as
strong. Check whether reconstruction failures cluster outside 09:30-16:00 ET.

**5. Re-run on a longer window once bars accumulate.** The study is bar-limited to ~47
sessions. NDX/NQ `trend_expansion` (622 readings, 30 sessions) carries the largest
effect in the study on the thinnest sample and should be confirmed before being quoted.

## 7. Second question: the CVD / limit-order framing

The customer's model: aggressive orders → CVD → "50% of the picture"; the other half is
resting limit orders. Assessed separately in
[`docs/design/futures-aggressor-delta.md`](futures-aggressor-delta.md).
