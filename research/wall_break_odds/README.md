# P(break | tested) — gamma wall break odds

Research-only. **Changes no production behaviour**: it imports from `src`, `src`
imports nothing from here, every database statement is a `SELECT`, and outputs go
to files.

Methodology and the reasoning behind each threshold:
[`docs/design/wall-break-odds.md`](../../docs/design/wall-break-odds.md).

## The question

> A trader is watching price arrive at the call wall. What is the probability it
> breaks through and *stays* through — and which observable inputs move that
> probability?

This is deliberately **not** `P(the wall gets tested)`. Production already models
that, in `src/jobs/forecast_range_model.py`, with the reflection principle
`P = 2·(1 − Φ(d/σ))` tilted by the dealer regime. Once the test has happened,
distance to the wall is ~0 by construction and carries nothing. Conflating the
two is the most common way this question gets answered wrongly, which is why
`distance` is absent from the feature vector and a test pins its absence.

## Status

The pipeline is complete, tested, and verified end to end on synthetic data and
against production.

### First measured result — SPX, 2026-06-29 .. 09-03, 48 sessions, 178 tests

**P(break within t | tested):**

| within | P(break) | 95% CI |
|---|---|---|
| 15 min | 7.6% | [4.4 – 13.0] |
| 30 min | 17.5% | [12.4 – 24.5] |
| 45 min | 26.8% | [20.4 – 34.8] |
| 60 min | **30.7%** | [23.9 – 39.0] |

Call and put walls are indistinguishable at every horizon (60-min: call 32.4%
[22.4–45.4], put 29.4% [20.7–40.7]).

The censoring assumption was checked, not assumed: log-rank on the session
halves gives p=0.13 over all tests and **p=0.46 restricted to first tests**.
The gap between those two is the survivorship the event rule creates — a
broken wall is spent, so the afternoon sample is enriched for survivors — and
controlling for it removes most of the apparent difference. The pooled curve
stands.

### SPX and QQQ are NOT one process

Adding QQQ over the same window (58 sessions, 209 tests) and running the
pooling check:

| symbol | n | breaks | P(30m) | P(60m) |
|---|---|---|---|---|
| SPX | 178 | 45 | 17.5% | 30.7% |
| QQQ | 209 | 91 | 30.0% | **50.1%** |

Log-rank chi2=11.60, **p=0.0007**. QQQ walls break at roughly a coin flip
within the hour where SPX walls hold two times in three. Pooling them is
therefore not available as a route to the model's event floor, and `analyze`
withholds every pooled quantity when the check fails rather than printing an
average that describes neither.

That suppression matters most for the SCREEN, not the curve: pooled, dollar-
scale features stop measuring walls and start measuring *which symbol a row
came from*. `wall_strength_log` reads −2 points on SPX alone and −15 pooled,
which is the confound, not a finding.

Break rates are per-symbol. A number from one product does not carry to
another.

### The features do not replicate

QQQ is a second, independent sample, so every candidate can be asked the
harder question: does it show up again in data it has never seen?

**Spearman r = −0.01 between the two screens' effect sizes. Sign agreement
9/18 = 50%, exactly chance.**

The largest SPX effects are precisely the ones that fail:

| feature | SPX | QQQ | agrees |
|---|---|---|---|
| `net_gex_log_signed` | −30 | −9 | yes, much weaker |
| `flip_distance` | −22 | **+5** | no |
| `spot_above_flip` | −20 | **+3** | no |
| `wall_strength_trend` | +19 | **−9** | no |
| `flow_toward_break` | −16 | **+1** | no |
| `convexity_risk_log` | −18 | −19 | yes |

So the "regime around the wall" story — net GEX, distance to the flip — is
**not supported**. It looked like the strongest thing in the SPX screen and it
did not survive contact with a second symbol.

`convexity_risk_log` is the one substantive feature agreeing closely across
both. With 19 features and 50% overall agreement, one close match is what
chance produces; it is a candidate to watch, not a finding.

The features that do agree are mostly mechanical — time of day, minutes to
close, test ordinal. Their link to the resolution window is structural and
would agree in any two samples.

`analyze` computes this automatically whenever two symbols are supplied.

### What has NOT been established

* **No feature is FDR-significant.** Nineteen screened, session-clustered
  bootstrap plus Benjamini-Hochberg, none survive. The largest effect
  (`net_gex_log_signed`, −30 points) does not survive either.
* **Wall size shows nothing.** `wall_strength_log` splits 35% / 33% — the
  variable everyone anchors on carried no information in this sample. That is
  a null at n=92, not proof of no effect.
* **No model.** 131 resolved events against a floor of 200; the walk-forward
  block declines to fit and will keep declining until the sample grows.

Nothing here is calibrated for trading and no result has been validated live.

## Quick start

```bash
# 0. Plumbing only — no database, no market data, invented numbers.
python -m research.wall_break_odds.cli selftest

# 1. Label wall tests over a window (read-only against production).
python -m research.wall_break_odds.cli build-dataset SPX \
    --start 2026-01-02 --end 2026-06-30 \
    --out research_output/wall_events.jsonl

# 2. Base rates, the curve, the screen, and the walk-forward evaluation.
python -m research.wall_break_odds.cli analyze research_output/wall_events.jsonl --by-side

# 3. A second symbol, and a check on whether they can be pooled.
python -m research.wall_break_odds.cli build-dataset QQQ \
    --start 2026-01-02 --end 2026-06-30 --out research_output/qqq_events.jsonl
python -m research.wall_break_odds.cli analyze \
    research_output/wall_events.jsonl research_output/qqq_events.jsonl
```

`--strike-step` defaults per symbol ($5 for SPX/NDX/RUT, $1 otherwise), so QQQ
does not need it set by hand. Aiming a $5 ladder at $1 strikes finds no
contracts and reads as "no flow at the wall" rather than as an error.

**Pooling is not free.** Passing several datasets to `analyze` combines them
and prints a POOLING CHECK: per-symbol curves plus a log-rank on whether they
are one process. Pooling to clear the model's 200-event floor is only honest
if that test says they agree — otherwise the extra events buy an average
describing neither symbol. Note SPY is close to a duplicate of SPX rather than
an independent sample; QQQ is a genuinely different book.

`build-dataset` also writes `<out>.meta.json` — symbol, window, sessions seen vs
used, skip reasons, censored count, and the exact label thresholds. The report
reads it, so a printed report always carries its own provenance.

## The headline is a curve

Sweeping only the resolution horizon on the first real run moved the point
estimate from 15.3% (30 min) to 29.7% (45) to 34.4% (60), on non-overlapping
intervals — a longer watch simply gives price more chances to go. So the
report leads with a Kaplan-Meier curve of `P(break within t)`, which also
recovers the ~25% of events that were previously censored and discarded.

Quote the curve, or quote a rate **with** its horizon. A bare "walls break X%
of the time" is not a claim this study supports.

## The event definition

| | |
|---|---|
| **tested** | price came within 5 bp of the wall in force at that minute |
| **broke** | it then closed 5 bp beyond the wall for **10 consecutive minutes** |
| **held** | 60 minutes elapsed without that confirmation |
| **censored** | the 60-minute horizon ran past 16:00 ET — outcome never observable |

Three consequences worth being explicit about:

* **A pierce is not a break.** ZeroGEX's own published research on failed
  breakouts notes they "often hold for the first ten or fifteen minutes before
  unwinding". A label that fires on the first tick through the level counts all
  of those as breaks, and the resulting probability means nothing.
* **Censored events are excluded, never folded into `held`.** That bias would
  fall entirely on late-session tests, which is exactly where the interesting
  cases live.
* **One grind at a wall is one observation.** After a test resolves, the wall
  re-arms only after a cooldown; a wall that breaks is spent for the session.
  Without this a forty-minute press at the wall would contribute forty rows and
  every standard error in the study would be a fiction.

Every threshold is a CLI flag (`--touch`, `--buffer`, `--confirm`, `--horizon`,
`--rearm`). A break under a 10-minute confirmation is a pierce under a 20-minute
one, so re-run before quoting any figure as settled.

## Checking the screen's calibration

Rows are clustered by session — a session-level feature like wall strength is
identical for every test that day, and those tests share the day's regime. A
textbook two-proportion z-test treats them as independent and is measurably
anti-conservative for it. The screen uses a session-clustered bootstrap instead.

Measured false-positive rate at α=0.05 on synthetic null data (300 sessions, 1–3
events each, 200 trials per cell, both arms on the same balanced split):

| design | naive z-test | this |
|---|---|---|
| independent rows | 0.045 | 0.035 |
| session-clustered rows | **0.110** | **0.055** |

The clustered row is the one that matters, and it is the one the naive test gets
wrong.

## What this cannot tell you

* **Dealer sign is modelled, not observed.** Walls come from the
  call-positive / put-negative open-interest convention. On a day when customers
  were net *buyers* of the wall-side options, the "wall" was never resistance and
  the event is mislabelled at source. No feature here detects that; see
  [`research/mm_attributed_gex`](../mm_attributed_gex) for the attribution work.
* **Nothing here is calibrated for trading**, and no result has been validated
  live.
