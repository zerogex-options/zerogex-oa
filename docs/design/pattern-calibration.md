# Playbook Pattern Calibration — Empirical-Base Feedback Loop

**Status:** shipped (OFF by default) · **Last updated:** 2026-06-18
**Repo:** `zerogex-oa`

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

## Rollout

1. Deploy with the switch **off** (default). Install the timer; let
   `playbook_pattern_stats` accumulate and review the nightly diff in the
   journal.
2. When the measured bases look sane, set
   `SIGNALS_PATTERN_CALIBRATION_ENABLED=1`. Live confidence shifts toward
   measured edge on the next reload; no code change or restart logic required.
3. To revert instantly, set it back to `0`.

## Notes

- v1 feeds calibration from the **underlying-touch** harness
  (`playbook/backtest.py`), the conservative, reviewed measurement. The new
  leg-level platform (`src/backtesting/`) can later write into the same stats
  table to drive calibration from realized option P&L instead.
- Calibration changes the *base*; confluence and regime_fit multipliers are
  untouched, and the final `[0.20, 0.95]` confidence clamp still applies.
