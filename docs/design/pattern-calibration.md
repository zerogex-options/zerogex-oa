# Playbook Pattern Calibration — Empirical-Base Feedback Loop

**Status:** shipped (OFF by default) · **Last updated:** 2026-06-28
**Repo:** `zerogex-oa`

> **Data source note:** calibration can feed from either of two measurement
> harnesses, both writing `playbook_pattern_stats` (tagged by a `source`
> column):
>
> * **`underlying_touch`** — `playbook/backtest.py`. The conservative proxy:
>   "did the underlying reach the target/stop?" Ignores premium decay, bid/ask,
>   and commission, so it overstates 0DTE edge. Single-leg, directional.
> * **`option_pnl`** — `src/backtesting/calibration_feed.py`. Realized leg-level
>   option P&L: "did the trade actually profit after real fills + slippage +
>   commission?" A trade is a win when `net_pnl > 0`. Runs a standardized
>   single-leg spec with disciplined premium exits — a stop-loss
>   (`SIGNALS_PATTERN_CALIBRATION_PNL_STOP_PCT`, default −50%) that cuts a
>   decaying long, and a take-profit
>   (`SIGNALS_PATTERN_CALIBRATION_PNL_TARGET_PCT`, default +75%) that books a
>   spike before it gives back — overlaid on each card's own underlying levels
>   (whichever triggers first). The stop caps loss *size* (lifts profit factor /
>   avg loss); the take-profit is what moves the win-*rate* that drives
>   `proposed_base`. Both are tunable; re-measure with
>   `pattern-calibration-explain` after changing them.
>
> The instrument matters as much as the exit: a near-dated single long bleeds to
> spread + theta. `make pattern-calibration-structures` measures each pattern as
> a single long **vs a defined-risk vertical** (pattern-mode `structure` override
> on `BacktestSpec`, short strike `width_pct` of the entry price OTM) and prints
> win% / profit factor / expectancy side by side — so you can tell a theta/vega
> problem (the vertical lifts it) from a genuine lack of directional edge (both
> stay poor).
>
> The live store picks which to trust via **`SIGNALS_PATTERN_CALIBRATION_SOURCE`**
> (`underlying_touch` (default) | `option_pnl` | `auto`). `auto` prefers
> `option_pnl` per (pattern, underlying) when it has a trustworthy window and
> falls back to `underlying_touch` otherwise. Both harnesses run in the nightly
> refresh; the consult, gates, and confidence formula are unchanged — only the
> `base` input source differs.
>
> The clamp band is per-source: `option_pnl` has its own
> `SIGNALS_PATTERN_CALIBRATION_FLOOR_OPTION_PNL` /
> `…_CEIL_OPTION_PNL` (default = the global `[0.40, 0.85]`), so an operator can
> give the honest realized-P&L measure a wider band — e.g. a lower floor so a
> genuinely losing pattern is marked down rather than pinned at 0.40 — without
> touching the touch-proxy band.
>
> Inspect both sources side by side before switching:
> `make pattern-calibration-compare` (read-only; prints prior vs touch vs
> option_pnl per pattern, with Δ, a below-sample-gate marker, and an `auto→`
> column showing the gated+clamped base the live engine would use under
> `source=auto`, tagged P/T/w/prior). Drill into one pattern's realized trades
> with `make pattern-calibration-explain PATTERN=<id>` — it dumps each trade and
> the outcome distribution with per-outcome profitability, so a touch-vs-P&L
> divergence can be confirmed as a real theta trap (target hit, premium lost)
> rather than a pricing artifact.

## Problem

Every playbook pattern carries a hand-set `pattern_base` prior (`PatternBase`,
default 0.50, band [0.40, 0.85]). Live Action Card **confidence** is
`pattern_base × confluence × regime_fit` (`playbook/base.py:compute_confidence`).
Those bases are guesses. The backtest harness already *measures* the truth —
`playbook_pattern_stats.proposed_base`, a beta-smoothed empirical win rate — but
the code explicitly writes it "for review only," and the live engine keeps using
the priors. This closes that loop.

## What it does

When enabled, the live engine **replaces a pattern's prior with its measured
win rate** for the specific underlying, so confidence reflects what the pattern
actually did. The replacement is gated and clamped so it can never destabilize
live behavior.

```
nightly:  pattern_calibration_refresh  ──► playbook_pattern_stats (proposed_base)
                                                     │
live:     SignalEngineService.run_cycle ──► calibration.maybe_refresh (TTL)
                                                     │ loads store
          PatternBase.compute_confidence ──► calibrated_base(id, underlying, prior)
                                                     │
                                            confidence = base × confluence × regime_fit
```

## Components

- **`src/signals/playbook/calibration.py`** — the store + consult.
  - `calibrated_base(pattern_id, underlying, fallback)` — pure in-memory hot-path
    lookup used by `compute_confidence`. Returns `fallback` (the prior) unchanged
    when disabled or no trustworthy measurement exists, so the feature is
    **behavior-preserving by construction**.
  - `CalibrationStore` — `by_pair[(pattern, underlying)]` + a sample-weighted
    `by_pattern[pattern]` fallback; `loaded_at` for TTL.
  - `load_store(conn)` / `maybe_refresh(ttl)` — DB load of the latest stats
    window per pair; `maybe_refresh` is a cheap no-op between reloads and never
    raises into a signal cycle.
- **`src/signals/playbook/base.py`** — one-line consult: `base = calibrated_base(
  self.id, ctx.underlying, self.pattern_base)`.
- **`src/signals/main_engine.py`** — calls `maybe_refresh()` once per cycle.
- **`src/tools/pattern_calibration_refresh.py`** — nightly job: re-runs the
  playbook backtest per underlying to refresh `playbook_pattern_stats`, then
  prints a `prior → calibrated` diff for review.

## Gates (all configurable, all conservative)

| Gate | Env | Default | Effect |
|---|---|---|---|
| Master switch | `SIGNALS_PATTERN_CALIBRATION_ENABLED` | `false` | Off ⇒ priors used verbatim. |
| Min sample | `SIGNALS_PATTERN_CALIBRATION_MIN_SAMPLES` | `20` | Fewer resolved trades ⇒ keep prior. |
| Freshness | `SIGNALS_PATTERN_CALIBRATION_MAX_AGE_DAYS` | `45` | Stale window ⇒ keep prior. |
| Clamp | `…_FLOOR` / `…_CEIL` | `0.40` / `0.85` | Calibrated base bounded to the catalog band. |
| Reload TTL | `…_REFRESH_SECONDS` | `21600` (6h) | How often the live process reloads. |
| Backtest window | `…_LOOKBACK_DAYS` | `60` | History each nightly backtest scans. |
| Auto-veto threshold | `…_AUTO_DISAGREEMENT_THRESHOLD` | `0.15` | Under `source=auto`, a (pattern, underlying) touch base is dropped when it is HIGHER than a sub-gate `option_pnl` reading by this much. Asymmetric: only fires on the unsafe side (touch over-stating). Set to `0` to disable the veto. |
| Auto-veto min sample | `…_AUTO_PNL_SOFT_MIN_SAMPLES` | `8` | Minimum `option_pnl` trades for the veto to fire; below this a single trade could veto an otherwise-good touch base. Set to `0` to disable the veto. |

### Auto-source disagreement veto

Under `SIGNALS_PATTERN_CALIBRATION_SOURCE=auto`, the live store prefers
`option_pnl` per (pattern, underlying) when its window passes `MIN_SAMPLES`, and
otherwise falls back to `underlying_touch`. The touch proxy can over-state edge
— a target/stop hit is not a profitable trade — so a pair with `touch n=40 at
0.85` and a sub-gate `option_pnl n=10 at 0.30` would currently leave the
inflated touch base in place under `auto`.

The veto closes that hole. When a sub-gate `option_pnl` reading meets the soft
sample minimum AND is at least `AUTO_DISAGREEMENT_THRESHOLD` lower than the
clamped touch base for the same pair, the touch base for that pair is dropped
before the merge. The live consult then falls through to the pattern-wide
cross-underlying mean (computed from non-vetoed samples) or the catalog prior —
preserving "conservative by construction": we never *replace* touch with thin
pnl, but we refuse to *use* a touch base the honest measure visibly contradicts.

The veto is **one-directional**: touch *under-rating* relative to pnl is the
conservative side and is left alone. The veto fires only on `touch − pnl ≥
threshold` (post-clamp on each side's own band).

`make pattern-calibration-compare` honors the same veto: the `auto→` column
shows `0.X v` (pattern-wide fall-through after veto) or `veto→prior`, and a
footer line lists every vetoed pair.

## Rollout

1. Deploy with the switch **off** (default). Install the timer; let
   `playbook_pattern_stats` accumulate and review the nightly diff in the
   journal.
2. When the measured bases look sane, set
   `SIGNALS_PATTERN_CALIBRATION_ENABLED=1`. Live confidence shifts toward
   measured edge on the next reload; no code change or restart logic required.
3. To revert instantly, set it back to `0`.

> **Go-live caveat.** Under `source=auto`, pairs whose `option_pnl` window is
> just below the `MIN_SAMPLES` gate fall back to the touch base, and touch can
> over-state edge. Before flipping `ENABLED=1`, run
> `make pattern-calibration-compare` and confirm the patterns you care about
> clear the gate on `option_pnl` (or that the auto-disagreement veto fires on
> any inflated touch pairs you'd otherwise inherit — vetoed pairs appear with
> `v` in the `auto→` column and in the footer list).

## Notes

- Both measurement harnesses (`underlying_touch` and `option_pnl`) write into
  `playbook_pattern_stats`, tagged by `source`. The live store picks one (or
  merges both under `auto`) via `SIGNALS_PATTERN_CALIBRATION_SOURCE`. The
  consult, gates, and confidence formula are unchanged.
- Calibration changes the *base*; confluence and regime_fit multipliers are
  untouched, and the final `[0.20, 0.95]` confidence clamp still applies.
