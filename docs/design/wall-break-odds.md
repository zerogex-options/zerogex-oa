# P(break | tested) — wall break odds: methodology

**Status:** research-only. No production metric, table, endpoint or dashboard is
changed by this document or the package it describes.

**Question:** given that price has come to the call wall (or put wall), what is
the probability it breaks through and stays through, and which observable inputs
move that probability?

**Origin:** a user question — *"we had a strong call wall at 7720 SPX and we broke
through it. I want to learn how to determine the statistical probability of that
breakthrough. Most of the time it holds, but today something gave us lots of
buying pressure."*

---

## 1. Why this is a different question from the one we already answer

`src/jobs/forecast_range_model.py` already publishes level-touch odds:

```
P(touch) = 2·(1 − Φ(d / σ))          # reflection principle, driftless 1-day path
σ        = vol_basis · implied_move  # calibrated intraday, ~0.74× for SPY/SPX
tilt     = 0.90 long gamma | 1.12 short gamma
```

That is `P(the wall gets reached)`, it is graded on a Brier score daily, and
distance `d` is its dominant term.

`P(break | tested)` is the conditional continuation. **Conditioning on the test
removes the distance term entirely** — at the moment of a test, `d ≈ 0` by
construction. This is the single most important structural fact about the study,
and getting it wrong is the standard failure mode: a model that includes distance
appears to work, but what it has learned is `P(touch)` all over again.

What is left once distance is gone:

* whether the wall is being **consumed** while it is tested,
* whether the **tape** is pushing through it,
* how much **room and time** remain in the session.

`FEATURE_NAMES` is built around those three, and
`test_distance_to_wall_is_not_a_feature` pins the omission.

## 2. Labelling

Implemented in `research/wall_break_odds/events.py`.

| term | definition | source of the constant |
|---|---|---|
| tested | price within `touch_pct` of the wall in force at that minute | `level_history.TOUCH_PCT` = 5 bp |
| broke | closes `break_buffer_pct` beyond it for `confirm_minutes` consecutive minutes | `level_history.MIN_HOLD_MINUTES` = 10 |
| held | `resolution_minutes` elapsed without confirmation | 60, a study parameter |
| censored | the horizon ran past 16:00 ET | — |

Constants are inherited from production where production has an opinion, so the
study is not quietly measuring a different level than the product draws.

### Why not reuse `level_history._score_segment`

`src/jobs/level_history.py` already labels walls `broke` / `held` / `untested`
for the bulletin write-ups. It is the right tool there and the wrong one here:

1. **It scores a pierce as a break.** It fires the moment the session extreme
   pokes past the level by the touch band. ZeroGEX's own published research on
   failed breakouts observes that they "often hold for the first ten or fifteen
   minutes before unwinding" — under that label every one of those is a break.
2. **It scores a segment, not a decision.** A segment is the window during which
   the wall sat at one value, which can be the whole session.

This package keeps `level_history`'s *conventions* and replaces its *event model*.

### Independence

A forty-minute grind at the wall is one decision a trader faces, not forty. After
a test resolves, the wall re-arms only after `rearm_minutes`; a wall that breaks
is spent for the session. Without this, sample size — and therefore every
standard error — would be inflated several-fold.

Residual clustering remains (a trending day produces correlated tests), and it is
handled in §4 rather than ignored.

### Censoring

A test at 15:45 has fifteen minutes of session left and a sixty-minute horizon.
Its outcome is **not observable**, and folding it into `held` would bias the base
rate upward precisely where 0DTE gamma is largest and the question is most
interesting. Censored events are counted, reported, and excluded.

## 3. Features

All measured at or before `tested_at`. `build_features` takes the test timestamp
as a hard cutoff; `test_features_are_blind_to_the_future` poisons every
post-test value with absurd numbers and asserts the vector does not move.

| group | features | what it is asking |
|---|---|---|
| wall size | `wall_strength_log`, `wall_strength_share`, `wall_strength_pctile_trailing` | is a big wall actually a strong one? |
| consumption | `wall_strength_trend`, `wall_migration_toward_break`, `wall_age_minutes` | is the wall being dismantled while tested? |
| regime | `net_gex_log_signed`, `net_gex_trend`, `flip_distance`, `spot_above_flip`, `convexity_risk_log`, `local_gex_share` | is the hedging reflex damping or amplifying? |
| tape | `flow_toward_break`, `flow_acceleration` | are customers buying the wall-side options? |
| room | `minutes_to_close`, `travel_budget`, `realized_sigma_ratio`, `vix_change_intraday` | is there time and energy left to travel? |
| context | `minutes_since_open`, `test_ordinal` | first test of the day, or the third? |

Three notes on specific choices:

* **`wall_strength_share`** normalises the wall against the book. A $25bn wall
  inside a +$60bn book is a chokepoint; the same $25bn inside +$400bn is one
  strike among many. The raw dollar figure is kept alongside it precisely so the
  study can show whether it carries weight once normalised.
* **`wall_strength_pctile_trailing`** is computed against the *previous* 30
  sessions, time-of-day bucketed. Ranking today's wall against a distribution
  that includes today is leakage; `TrailingStrength` appends the current session
  only after its events are built, and a test pins that ordering.
* **`flow_toward_break`** is oriented so positive means "pressure in the
  direction that breaks this wall" on **both** sides. `net_premium` is
  buy-minus-sell aggressor premium, and customers buying the wall-side option is
  what shortens dealers in it — so the same sign means the same thing for calls
  and puts, and the two fits can be read against each other.

### The cumulative-flow trap

`flow_by_contract.net_premium` is **day-to-date cumulative per contract**, reset
at 09:30 ET. A 30-minute window figure is therefore a *difference* of two
cumulatives, never a sum over buckets — summing books the whole morning into
every window and makes the feature grow monotonically through the day regardless
of what the tape did.

The difference is taken **per contract and then summed**, not on pre-summed
totals: a contract whose first print lands mid-window has no earlier row, so its
implicit prior cumulative is zero, and differencing a pre-summed total would book
its entire day-to-date figure as window activity. Both properties are pinned by
tests.

## 4. Evaluation

Implemented in `research/wall_break_odds/model.py`, reusing the statistical
primitives in `research/mm_attributed_gex/stats.py` so both studies are graded by
the same code.

**Base rates** carry Wilson intervals — the sample splits into small buckets and
Wilson behaves near 0 and 1 where the normal approximation does not.

**The univariate screen** uses a **session-clustered bootstrap**. Rows within a
day are not independent: a session-level feature is identical for every test that
day, and those tests share the day's regime. Measured false-positive rate at
α=0.05 on synthetic null data:

| design | naive z-test | session-clustered bootstrap |
|---|---|---|
| independent rows | 0.040 | 0.068 |
| session-clustered rows | **0.110** | **0.032** |

Benjamini-Hochberg then controls the false-discovery rate across the feature
family, because screening twenty columns at α=0.05 produces one spurious hit per
screen by construction.

**The model** is a logit, evaluated **only** out of sample, on walk-forward folds
whose boundaries are snapped to session edges — an index-based split can cut a
session in half and put the morning's tests in train and the afternoon's in test,
leaking the day's regime. Standardisation uses train moments only.

**The baseline is the base rate.** A model that cannot beat an intercept-only
predictor out of sample on log-loss has found nothing, whatever its p-values say.
`skill = 1 − logloss_model / logloss_baseline` is reported, and a negative value
is a legitimate, reportable finding.

**Reporting floors.** Below 200 resolved events no coefficient is reported at
all; below 30 in a bucket, not even a rate. Twenty features on eighty events is a
memoriser, not a model.

## 5. What this cannot establish

* **Dealer sign is modelled, not observed.** Walls come from the
  call-positive / put-negative open-interest convention. On a day when customers
  were net *buyers* of the wall-side options, the "wall" was never resistance —
  the event is mislabelled at source and no feature here can detect it. This is
  the deepest reason walls break "unexpectedly", and it is what
  `research/mm_attributed_gex` exists to test.
* **`P(break | tested)` is not `P(break)`.** For the unconditional question the
  production reflection-principle touch odds remain the right tool. The two
  compose: `P(break) ≈ P(touch) · P(break | tested)`.
* **Labels are threshold-sensitive.** A break under a 10-minute confirmation is a
  pierce under a 20-minute one. Every threshold is a CLI flag; re-run before
  treating a figure as settled.
* **Nothing here is calibrated for use as a trading signal**, and no result has
  been validated live.

## 6. If it works

The natural product surface is the existing Range Break Imminence signal
(`src/signals/advanced/range_break_imminence.py`), which today fuses four inputs
into a 0–100 score with hand-set weights. A validated `P(break | tested)` would
give that score a measured, calibrated backing for the specific case where price
is *at* a wall, rather than a heuristic blend. That is a separate proposal and
would need its own backtest; nothing in this package assumes it.
