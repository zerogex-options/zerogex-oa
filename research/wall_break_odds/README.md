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

The pipeline is complete, tested, and verified end to end on synthetic data.

**No result has been produced**, because producing one requires running
`build-dataset` against a database with `gex_summary` and `underlying_quotes`
history. Nothing in this package reports a break rate it did not measure; on an
empty or short window the report says so and stops.

## Quick start

```bash
# 0. Plumbing only — no database, no market data, invented numbers.
python -m research.wall_break_odds.cli selftest

# 1. Label wall tests over a window (read-only against production).
python -m research.wall_break_odds.cli build-dataset SPX \
    --start 2026-01-02 --end 2026-06-30 \
    --out research_output/wall_events.jsonl

# 2. Base rates, the screen, and the walk-forward evaluation.
python -m research.wall_break_odds.cli analyze research_output/wall_events.jsonl --by-side
```

`build-dataset` also writes `<out>.meta.json` — symbol, window, sessions seen vs
used, skip reasons, censored count, and the exact label thresholds. The report
reads it, so a printed report always carries its own provenance.

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

Measured false-positive rate at α=0.05 on synthetic null data (400 sessions, 1–3
events each, 250 trials per cell):

| design | naive z-test | this |
|---|---|---|
| independent rows | 0.040 | 0.068 |
| session-clustered rows | **0.110** | **0.032** |

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
