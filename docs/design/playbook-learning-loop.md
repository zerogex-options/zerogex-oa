# Playbook learning loop: grade every Card, earn the bar per symbol

**Status:** shipped (on by default) · **Last updated:** 2026-09-24
**Repo:** `zerogex-oa`

## Problem

Three things made the Action Cards late and kept them from improving:

1. **The same idea was re-issued all through the move.** Most patterns trigger
   on a condition that stays true for the whole move ("price is above the
   opening range", "dealer-delta pressure above 30"). The engine re-checks every
   second, and the only brake was the dwell window (5 minutes for 0DTE, 15 for
   1DTE, 60 for swing). So a pattern issued the same trade every few minutes
   for as long as the move lasted, each Card at a worse price than the last.
   The later Cards arrived after most of the move and just before it turned.
   The engine had a rule against this (no new entry while the pattern's last
   trade is inside its hold window) but was never told about any open trade:
   both context builders passed an empty `open_positions`.
2. **Nothing checked whether a Card worked.** The nightly calibration job
   measures hit rates, but the live engine only reads them when
   `SIGNALS_PATTERN_CALIBRATION_ENABLED` is on (it is off by default), and even
   then it only nudges a pattern's confidence inside a band whose lowest value
   still clears the 0.25 floor. A losing pattern kept publishing.
3. **The floor was one number for every pattern and symbol.** 0.25 sits under
   nearly every Card the patterns produce, so it filtered almost nothing, and
   nobody had checked whether confidence predicts results at all.

## What it does

```
live cycle (1s) ──► PlaybookEngine
                     1. regime gate
                     2. one Card per idea   ◄── ideas.py: latest idea per pattern
                     3. hysteresis                (published + held back, with grade)
                     4. entry bar           ◄── adaptive_gate.py: bar per
                     5. resolve                   (pattern, symbol, direction)
                         │ publish                      ▲
                         ▼                              │ reload every 15 min
                 signal_action_cards                    │
                         │                      playbook_card_outcomes
                         └── grading.py (every 2 min) ──►  one graded row per idea
                 held-back ideas ──────────────────────►  (card_id NULL)
```

### 1. One Card per idea (`ideas.py`)

Both the signal cycle and the `/api/signals/action` endpoint now load each
pattern's latest idea on the symbol: its last published Card or an idea the
entry bar held back, with the grader's verdict so far. While that idea is inside
its own hold window, the pattern issues nothing new there. A stopped-out idea
frees the slot for a Card in the other direction (a reversal is a new idea); a
target hit does not (re-entering after the move played out is the chase).

### 2. Grading (`grading.py`, table `playbook_card_outcomes`)

Every published Card, and every idea the entry bar held back, becomes a row.
When the underlying reaches the target or stop, or the hold runs out, the row
is graded with the backtest harness's own `compute_outcome` (intrabar
high/low, touch/break entries must fill, a same-bar tie goes to the stop). A
0DTE Card's hold is capped at that day's close. The target and stop are the
prices the Card printed: `call_wall_fade` and `put_wall_bounce` label their
stop `premium_pct` but set it to the wall price the catalog names, and the Card
page shows it as the stop, so it is graded as that price (a value too far from
the entry to be an underlying price is ignored).

| Column | Meaning |
|---|---|
| `outcome` | `target_hit`, `stop_hit`, `time_exit` (graded); `no_fill`, `no_data`, `unresolved`, `off_session`, `mispriced` (not counted). `mispriced` is an at-market Card whose quoted price was outside what traded within two minutes of it: every Card the API built before 2026-09-24 quoted VWAP as the price, and grading those would blame the patterns for that bug. |
| `r_multiple` | Result in units of the Card's risk (entry to stop): target = reward/risk (capped at 5), stop = -1, time exit = where price ended, clipped. No price stop: the target distance is the unit. |
| `prior_move_pct` | How far price had already moved the Card's way in the 30 minutes before it. The lateness read. |
| `is_repeat` | A Card issued while the same pattern's same-direction Card was still inside its hold window. Counted once, as the idea. |

The signals service grades its own symbol every
`PLAYBOOK_GRADING_INTERVAL_SECONDS` (120). `make playbook-grade` backfills the
lookback, and the nightly `zerogex-oa-pattern-calibration` job re-runs it for
every symbol as a backstop.

### 3. The entry bar (`adaptive_gate.py`)

For each (pattern, symbol, direction) the gate computes an expected result per
idea, in R, net of a friction charge (`PLAYBOOK_ADAPTIVE_COST_R`, 0.10):

* graded ideas from the last `PLAYBOOK_ADAPTIVE_LOOKBACK_DAYS` (90), each
  weighted by age with a `PLAYBOOK_ADAPTIVE_HALF_LIFE_DAYS` (30) half-life, so
  the record follows the market rather than averaging it forever;
* blended with how the same pattern did everywhere else (other symbols, the
  other direction), which is itself blended with "no edge", each side counted
  as `PLAYBOOK_ADAPTIVE_PRIOR_WEIGHT` (10) ideas. Three lucky or unlucky trades
  can't swing the bar; forty can.

| Expected result per idea | Status | Bar (confidence needed) |
|---|---|---|
| fewer than `MIN_IDEAS` (8) graded ideas for the pattern | learning | 0.25 (the old flat floor) |
| ≥ +0.25 R | proven | 0.20 (everything the pattern produces) |
| 0 to +0.25 R | positive | 0.25 → 0.20 |
| 0 to −0.25 R | lagging | 0.25 → 0.75 |
| ≤ −0.25 R | paused | nothing published |

A Card that clears its bar carries `context.track_record` (status, bar, record).
One that doesn't is listed in the Stand Down card's near misses in plain
English, and, when it would have cleared the old 0.25 floor, is recorded as a
held-back idea and graded like any Card. That is how a paused pattern earns its
way back: its ideas keep being graded, and when they start working the
expected result climbs and the bar comes down.

## Operating it

| Command | What it does |
|---|---|
| `make schema-apply` | Creates `playbook_card_outcomes` (before restarting services). |
| `make playbook-grade` | Grades the lookback of existing Cards. Safe to re-run. `REBUILD=1` regrades published Cards from scratch after a change to the grading rules; ideas the gate held back are kept. |
| `make playbook-record` | Read-only report: per pattern, symbol and direction, graded ideas, won/lost/flat, average R, how far price had already moved, repeats, held-back ideas, and the status and bar the gate applies now. Also compares first Cards with re-issued ones. `DAYS=30 SYMBOL=SPY` to narrow. |

Switches (`.env`, then restart the signals and API services):

* `PLAYBOOK_ADAPTIVE_GATE_ENABLED=0`: back to the flat 0.25 floor. Grading continues.
* `PLAYBOOK_ONE_CARD_PER_IDEA=0`: back to re-issuing every dwell window.
* `PLAYBOOK_GRADING_ENABLED=0`: the signals service stops grading (the nightly job still grades).
* The bar's shape: `PLAYBOOK_ADAPTIVE_{NEUTRAL,MIN,MAX}_BAR`, `PLAYBOOK_ADAPTIVE_{PROVEN,PAUSE}_R`, `PLAYBOOK_ADAPTIVE_MIN_IDEAS`, `PLAYBOOK_ADAPTIVE_PRIOR_WEIGHT`.

Everything is best-effort: if the table is missing, the idea gate falls back to
published Cards, the bar stays at 0.25, grading logs one line per ten minutes,
and the signal cycle never breaks.

## Limits

* **Underlying, not option P&L.** Grades read the underlying's path against
  the Card's own levels. A long 0DTE option that goes nowhere loses premium;
  the friction charge stands in for that. `make pattern-calibration-explain`
  still measures real option P&L per pattern.
* **Non-directional Cards are not graded** (premium-selling structures with no
  price stop). They keep the flat 0.25 floor.
* **Lateness is measured, not gated.** `prior_move_pct` shows which patterns
  enter after the move; the bar scales back a pattern whose late entries lose,
  but no rule blocks an individual late entry yet.
* **Repeats in history.** Cards issued before this shipped include the
  re-issue streams. The record counts each idea once (`is_repeat`), and the
  report shows how the re-issues did separately.
