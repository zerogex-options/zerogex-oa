# Opening-range extension × gamma confluence — findings

**Status: complete through Phase 4. Recommendation: do not build the bots.**

Two of the three hypotheses are falsified on a fair test. The third is
underpowered rather than refuted. And the study's own headline number turned out
to be an artefact of how the events were defined — which is the most useful
thing it produced.

Methodology and repository assessment:
[`or-extension-gamma-confluence.md`](./or-extension-gamma-confluence.md).
Harness: [`research/or_gamma_confluence/`](../../research/or_gamma_confluence).

---

## 1. What was tested

A customer (Jim Edwards, via `zerogex.io Mail — Re: Friday`, 2026-09-07) proposed
that fixed levels projected away from the opening range act as reversion
targets — *"the further from the opening price, the stronger the rubber band
effect when it reverts"* — and, secondarily, that levels which are **not**
respected signal continuation.

| | hypothesis | verdict |
|---|---|---|
| **H1** | deeper extension → higher reversion | **falsified** |
| **H2** | broken ladders → continuation | **underpowered**, direction consistent |
| **H3** | pre-existing gamma confluence raises reversion | **falsified as specified** |

**Data.** 2026-06-29 .. 2026-09-05. SPY, QQQ, SPX, NDX, ES, NQ. 49 calendar
sessions, 249 symbol-sessions, 1,499 first-touch events (1,465 resolved).
Availability clock `visible`, minimum gamma lead 120 s.

**Sample ceiling.** 59 sessions for SPY/QQQ, 49 for SPX/ES, **31 for NDX/NQ** —
gamma frames bind everywhere, bars never do. NQ, the symbol the idea came from,
is the thinnest.

---

## 2. The finding that matters most: the baseline was never 50%

The parameter sweep exposed it. On **one tape**, across the same 49 sessions,
the reversal-first rate falls monotonically as the ladder step widens:

| step | 0.25R | 0.5R | 1.0R |
|---|---|---|---|
| reversal-first | 55.1% | 52.6% | 49.4% |
| n | 3,079 | 1,465 | 599 |

A market property cannot depend on how finely we choose to draw the ladder.
That column is the harness measuring itself.

**Cause.** A touch fires when the bar's **extreme** reaches the rung, but the
forward scan starts from that bar's **close**, which has usually retreated back
inside. Price therefore begins the prev-versus-next race already displaced
toward prev — and that head start is a larger *fraction* of the gap when the
step is small. For a driftless walk between barriers at `−step` and `+step`
starting at `−d`, gambler's ruin gives

```
P(reversal) = 0.5 + d / (2 · step)
```

**Three confirmations.**

1. Backing `d` out of each cell gives the *same* value from the two
   best-powered ones: **0.0255R** (n=3,079) and **0.0260R** (n=1,465).
2. Feeding a single `d = 0.026R` forward predicts them to within
   **0.1 percentage points** (55.2 vs 55.1, 52.6 vs 52.6).
3. A driftless random walk pushed through the identical event rule produces
   54.3 / 52.0 / 51.1 — with no mean reversion in the generator at all.

**So the 52.6% headline was never evidence of mean reversion.** It is what this
event definition returns on a random walk.

### And the gross expectancy is exactly zero

Entering at the close, targeting `prev` and stopping at `next`:

```
E = P·(s − d) − (1 − P)·(s + d)     with P = ½ + d/(2s)
  = −d + d
  = 0
```

Algebraically zero, verified numerically across a grid of `s` and `d`. The
elevated win rate buys nothing, because the target is nearer than the stop by
exactly the amount that explains it. A fair game before costs, and a losing one
after.

Every event now carries `touch_offset_r`, and the report leads with this
mechanical null so no future run can read the baseline as 50%.

---

## 3. H1 — extension distance does not predict reversion

| depth | n | sessions | reversal | 95% CI |
|---|---:|---:|---:|---|
| 0.5–1R | 340 | 49 | 56.5% | [51.2 – 61.6] |
| 1–2R | 464 | 48 | 49.8% | [45.3 – 54.3] |
| 2–3R | 291 | 38 | 52.2% | [46.5 – 57.9] |
| 3–5R | 276 | 30 | 52.5% | [46.7 – 58.4] |
| ≥5R | 94 | 14 | 54.3% | [44.2 – 64.0] |

Non-monotone, every interval overlapping. The *shallowest* band reverts most,
which is backwards from the claim. Identical across two independent builds.

Across all nine ladder geometries, Spearman(depth, reversal) reads
−0.029, −0.021, +0.087, +0.043, +0.113, −0.019, +0.042, +0.125, +0.125 —
sign-flipping between neighbouring cells, every magnitude ≤ 0.125.

**Falsified, and now also explained**: the apparent tilt is the same
close-offset, since a shallower rung has a smaller `step` and therefore a larger
`d/(2·step)`.

---

## 4. H3 — gamma confluence adds nothing as specified

| threshold | confluence | no confluence | gap |
|---|---:|---:|---:|
| ≤2 pts | 53.5% (n=645) | 52.0% (n=820) | +1.5 |
| ≤5 pts | 53.1% | 52.0% | +1.1 |
| ≤10 pts | 52.6% | 52.7% | −0.1 |
| ≤15 pts | 52.7% | 52.3% | +0.4 |
| ≤20 pts | 52.3% | 54.2% | −2.0 |

Sign-flipping across adjacent buckets. Across nine ladder geometries the effect
runs +1.5, −0.6, +2.0, +0.3, +0.2, +2.4, +0.8, −0.0, +2.4 points — also
sign-flipping, all small.

**Nothing survives multiplicity.** Benjamini-Hochberg across the cohort family:
eleven testable cohorts, smallest p = 0.051, rank-1 threshold 0.0045, **zero
survivors**. The two rows that looked like findings — confluence + negative GEX
(+6.0 pts, p=0.051) and confluence + below flip (+6.4 pts, p=0.057) — are what
this many comparisons produce by chance. They are also not two findings but one:
the two cohorts overlap almost entirely (329 vs 303 events).

**It reverses out of sample.** Discovery +4.6 pts, validation −3.8, test −1.6.

### Every cohort that looked good in-sample flips out of sample

Excess over the baseline **of its own column** (the baseline drifts: 49.7% /
56.0% / 57.9%):

| cohort | discovery | validation | test | holds sign? |
|---|---:|---:|---:|---|
| 5. confluence + negative GEX | +10.4 | +7.0 | **−4.7** | no — reverses |
| 7. confluence + below flip | +11.6 | +9.4 | **−5.6** | no — reverses |
| 4. confluence + positive GEX | −7.0 | −6.0 | **+3.6** | no — reverses |
| 17. ≥4 metrics agree | +7.9 | **−14.6** | −3.2 | no — reverses |
| 11. prior extensions broken | −2.6 | −13.1 | −10.5 | **yes** |

The two rows that reached p≈0.05 in-sample are the two that reverse hardest.
That is what the split is for.

### The product's own claim, tested

"When four metrics agree on one strike, that's the level." Base rate of
**distinct kinds** agreeing (not level count — four adjacent GEX strikes are one
kind of evidence):

| threshold | ≥2 metrics | ≥3 metrics | ≥4 metrics |
|---|---:|---:|---:|
| ≤2 pts | 33.2% | 22.3% | **13.2%** |
| ≤5 pts | 45.9% | 38.6% | 28.2% |
| ≤10 pts | 57.1% | 46.7% | 36.8% |
| ≤20 pts | 71.0% | 59.6% | 49.2% |

So four-metric agreement within 2 points happens at about **1 in 8** touched
prices — selective, but not rare. Within 10 points it is better than 1 in 3.

Does it predict anything? Cohorts 15/16/17 read 52.1% / 52.5% / **54.1%** — a
mild rise with the number of agreeing metrics, but +1.5 points at p=0.68, no BH
survival, and it swings 57.6% → 41.4% → 54.7% across the out-of-sample split.

**Scope matters here.** This tests one narrow question: does multi-metric
agreement *at an opening-range extension* predict reversion-versus-continuation.
It does not. That is not the same as "the folded label is uninformative" — it
says nothing about whether those prices matter in other ways (reaction size,
volume, dwell time), which this study never measured.

**And it is insensitive to lead time.** Across 0 / 30 / 60 / 120 / 180 s the
effect reads +0.3, +0.2, +0.5, −0.1, −0.1 — while H1 and H2 stay bit-identical
in all five cells, which is the consistency check passing (neither depends on
gamma).

### One caveat that is genuinely load-bearing

The first run used `gex_ladder_depth = 10`, which production silently clamps to
5 — so the run's fingerprint described a depth it had not used, and **79% of
touches registered "confluence"**. A cohort holding four fifths of the sample is
not a filter. That was corrected to production's default of 3 and the study
re-run; the cohort now holds 44% at the 2-point threshold, and the depth
confound was checked and is **not** present (median depth 1.50R in both groups).

The null above is from the corrected run. It is a fair test.

---

## 5. H2 — broken ladders: the only live thread

| | |
|---|---|
| effect vs baseline | **−6.0 points** (less reversion, i.e. continuation) |
| p (session-clustered) | 0.256 |
| n | 163 events, 30 sessions |

Across the nine ladder geometries every non-null cell is negative: −1.7, −6.0,
−1.7, −1.3, −11.8, −4.8, −4.7, −0.4.

**And it is the only cohort that keeps its sign out of sample** — −2.6, −13.1,
−10.5 against each column's own baseline, getting *stronger* in the holdout
while every confluence cohort flips. That is weak evidence, on 104 / 21 / 38
events, but it is the only thing in the study that behaves the way a real
effect behaves.

**That 8-for-8 sign consistency is not evidence, and must not be reported as
such.** Those cells are the same 49 sessions re-cut with different ladders — one
sample viewed eight ways, not eight samples. `research/wall_break_odds` uses
sign consistency across *symbols*, which are independent books; this is not
that.

What is true is narrower and worth keeping: H2 is consistently *directionally*
right, never contradicted, at a sample too small to resolve a 6-point effect.
This is the one hypothesis that more data could actually settle.

---

## 6. Is any of it tradeable? No — and here is the hurdle

On NQ, with a typical 60-point 5-minute range, a 0.5R step, $20/point,
$4.50 round-turn commission and one tick of slippage per side:

| | |
|---|---|
| round-turn cost | **$14.50** = 0.725 points |
| mechanical null win rate | 52.60% |
| break-even win rate | 53.81% |
| **required excess over the null** | **+1.21 points of win rate** |

Measured confluence effects ran −0.6 to +2.4 points with intervals of roughly
±5 points. **The hurdle sits inside the confidence interval.**

This is an important distinction and the report should not overstate it: we did
not prove the effect is zero. We bounded it loosely around a threshold that
matters. The honest statement is *this sample cannot tell a tradeable edge from
a null one* — not *there is definitely nothing there*.

---

## 7. Would more data change the answer?

| hypothesis | more data helps? | why |
|---|---|---|
| **H1** | **No** | ρ ≈ 0 at n=3,079; the interval already excludes anything tradeable, and the residual tilt is explained by the artefact |
| **H3** | **Only at ~4 years** | resolving the 1.2-point cost hurdle at 80% power needs ~1,024 sessions |
| **H2** | **Yes — ~1 year** | a 2.4-point effect needs ~256 sessions; the measured 6-point effect needs fewer |

Scaling from the measured power curve (59 sessions resolve a 5-point effect at
~78% power, and power scales with n·effect²):

```
  5.0 pt effect  ->   ~59 sessions  (~0.2 years)
  2.4 pt effect  ->  ~256 sessions  (~1.0 years)
  1.2 pt effect  -> ~1024 sessions  (~4.1 years)
```

Since `gex_summary` became retention-exempt on 2026-08-25, history now accrues
rather than rolling off. **Re-running in ~6 months roughly doubles the sample**
and is the single highest-value action available.

---

## 8. What IS actionable

The trading thesis failed. Four things came out of it that did not.

### 8.1 A measured answer to the customer's actual question

Jim's opening question was why levels "move into position after price has come
and gone". That now has a number rather than an explanation:

| book | p50 lag | p95 | max |
|---|---:|---:|---:|
| SPY | 40.7 s | 71.0 s | 1,618 s |
| QQQ | 33.5 s | 66.8 s | 651 s |
| SPX | 41.1 s | 62.1 s | 82.6 s |
| NDX | 29.4 s | 61.3 s | 96.1 s |

Publish lag is the gap between the option-chain instant a level is computed
*from* and the instant the row is written. Adding the indicator's 30-second poll
puts a level **~60–70 seconds behind its own timestamp at the median** — which
independently confirms what the customer was told ("about a minute behind"), and
quantifies the tail he was actually noticing. Zero NULL `created_at`, zero
backfills, one negative-lag row across ~146k rows.

### 8.2 "Four metrics agree" has never had its base rate measured

The product's copy says *"when four metrics agree on one strike, that's the
level."* That is only selective if it is rare. This study found that with a
depth-3 ladder there are ~10 levels per snapshot and the median distance from an
arbitrary extension to its nearest level is **2.76 points** — levels are
everywhere.

The harness now prints the agreement base rate (how often 2 / 3 / 4 *distinct
kinds* land together at each threshold) and tests cohorts built on it. **This
re-cohorts the existing dataset with no rebuild** — it is one `analyze` run.
It tests the product's own claim, which every cohort so far has not: previous
confluence cohorts asked "is *any* level near", which four adjacent ranked GEX
strikes satisfy trivially.

### 8.3 A reusable, look-ahead-proof level-study engine

What was built is not specific to opening ranges. It is a general engine for
*"does pre-existing gamma structure at price X predict what happens next"*, with
the parts that are easy to get wrong already solved: the three availability
clocks, fail-closed on untrustworthy frames, ranks recomputed against the
snapshot's own spot (not the touch spot), calendar-session clustering, the
pooling check, multiplicity correction, and the mechanical null. Any future
level hypothesis — VWAP bands, prior-day levels, session opens — is a new
`ranges.py` against the same machinery.

### 8.4 A methodological check worth running on published statistics

The close-offset artefact generalises. **Any statistic of the form "price
reached level L, then what happened" inherits a displacement if the event fires
on a bar extreme and the outcome is measured from the close.** That is worth
checking against `wall_break_odds`, whose published curve backs a shipped
product feature. Its event shape differs — a confirmed break rather than a race
between two barriers — so the gambler's-ruin form does not transfer directly,
but the direction of the bias (toward "held") is the same and the check is
cheap.

---

## 9. Recommendation on the four candidate bots

| bot | thesis | recommendation |
|---|---|---|
| **1. OR Gamma Reversion** | H1 + H3 | **Do not build.** Both falsified; gross expectancy exactly zero. |
| **2. OR Gamma Continuation** | H2 | **Defer.** Direction consistent, underpowered. Re-check in ~6 months. |
| **3. OR Gamma Confluence** | H3, stricter | **Do not build.** Stricter filters on a null effect destroy sample without adding edge. |
| **4. OR Gamma Regime Switcher** | routes 1 ↔ 2 | **Do not build.** A router over two strategies, neither of which has cleared. |

No candidate specs were added to `src/tradeworkz/registry.py`. Putting
scaffolding in the registry for a thesis the data does not support is the exact
mistake `PutWallMagnetReversal` is preserved to document.

---

## 10. Caveats

* **Dealer sign is modelled, not observed.** Walls come from the call-positive /
  put-negative open-interest convention. See `research/mm_attributed_gex`.
* **The ±3% ingest strike band censors deep extensions**, where no gamma level
  can exist by construction. The depth confound was checked and is not present,
  but the coverage limit is real.
* **The trader's own indicator was not replicated.** HMA / EMA / VWAP-slope /
  trade-bias are candidate filters; his implementation is proprietary. Cohorts
  8 and 9 were empty in the default run because `trend_filter` defaults to
  `none`.
* **Rebuilds are not bit-identical.** Event counts moved between runs (1,514 →
  1,499) partly under a changed fingerprint and partly because `futures_quotes`
  is live. `.meta.json` records row counts so this is detectable.
* **Nothing here has been validated live.**

---

## 11. Reproducing

```bash
make orgc-selftest                                    # no database; invented numbers
make orgc-coverage                                    # history depth + clock health
make orgc-dataset START=2026-06-29 END=2026-09-05     # ~9 min
make orgc-analyze                                     # cohorts, BH, OOS, pooling
make orgc-analyze CONFLUENCE_DISTANCE=2 TREND_FILTER=ema_slope
make orgc-sweep      START=2026-06-29 END=2026-09-05 FAST=yes   # 9 ladder cells
make orgc-sweep-lead START=2026-06-29 END=2026-09-05            # 5 lead cells
make orgc-test                                        # 80 tests
```

Config fingerprint of the headline run: `717ef98adf6b2f88`.
