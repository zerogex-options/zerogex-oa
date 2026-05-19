# Gamma-flip relative-threshold recalibration — REVIEW ARTIFACT

> **STATUS: PROPOSAL FOR OWNER DISCUSSION — NOT APPLIED. No code/threshold
> changed by this document.** Numbers below are from a thin, non-stationary
> post-deploy window and are *illustrative*; do not adopt before re-running
> on a stable window (see "Required before adoption").

## 1. Context

`_calculate_gamma_flip_point` was redefined and has kept moving:

- `7106711` per-strike adjacent sign-change → cumulative zero-gamma level
  (deployed 2026-05-15 ~22:40 ET)
- `1731efc` gamma flip from a spot-shift dealer gamma profile
- `62c70df` DTE-weight the spot-shift profile so 0DTE can't pin the flip
- `f5c4ded` flip unresolved (NULL+WARN) on a degraded one-sided chain

Every consumer reads the flip **relatively** (`flip_distance =
(price − flip)/price`), so no *absolute* threshold is miscalibrated. But
the redefinition moves where the flip sits, shifting the empirical
distribution of that relative distance, and two firing-rate-sensitive
gates ride on it:

- The shared near-flip gate `gamma_anchor.flip_distance_subscore ≥
  _FLIP_DISTANCE_MIN` (**0.6**), used by `gamma_flip_bounce` /
  `gamma_flip_break`. That subscore *is* `FlipDistanceComponent`:
  `clamp(1 − |fd|/sat)`, `sat` vol-adaptive in
  `[_FLIP_MIN_PCT 0.5%, _FLIP_MAX_PCT 5%]`, fallback `_FLIP_FALLBACK_PCT
  2%`.
- `portfolio_engine`'s inline `abs(flip_distance) ≥ 0.006` "flip not
  near" band.

## 2. Evidence (SPY, `make gamma-flip-revalidate`, 2026-05-19, 30d window)

| metric | PRE (old per-strike, n=8327) | POST (n=1013) |
|---|---|---|
| \|flip_distance\| p50 | 0.0041 | 0.0084 (**×2.05**) |
| \|flip_distance\| p75 | 0.0079 | 0.0205 |
| \|flip_distance\| p95 | 0.0121 | 0.0235 |
| share ≥ 0.006 (portfolio band) | 38.7% | 69.8% |
| near-flip gate fires @ minσ | 32.8% | 11.3% |
| near-flip gate fires @ fallbackσ | **75.5%** | **47.3%** |
| near-flip gate fires @ maxσ | 100.0% | 72.8% |

Corroboration: the playbook backtest (`--no-write --days 4`,
post-deploy) emitted **zero** `gamma_flip_bounce` / `gamma_flip_break`
cards — the patterns are effectively dormant under the new flip + the
unchanged 0.6 gate.

## 3. Key analytical insight

The gate fires iff `subscore ≥ 0.6` ⇔ `|fd| ≤ 0.4·sat`:

- fallback σ (0.02) ⇒ admits `|fd| ≤ 0.008`
- min σ (0.005) ⇒ `|fd| ≤ 0.002`
- max σ (0.05) ⇒ `|fd| ≤ 0.020`

Post-deploy p50 `|fd|` ≈ 0.0084 already exceeds the fallback-σ admit
band (0.008), and p75 ≈ 0.0205 exceeds even the **max-σ** band — i.e.
the redefinition pushed typical distance to where the saturation band
itself, not the 0.6 gate, is the binding constraint. High-σ regimes are
barely affected (maxσ still fires 72.8%); the loss is concentrated in
low/normal-σ. **Lowering `_FLIP_DISTANCE_MIN` alone cannot restore the
prior cadence** (see Option C).

## 4. Options

### Option A — accept the new firing rate (RECOMMENDED, pending §5)
Change nothing. Rationale: the old per-strike flip hugged spot (p95 only
1.2%), so the patterns likely *over-fired*; ~47% at fallback σ may be
the more discriminating, correct behavior. Lowest risk, zero blast
radius. Validate by hit-rate on a stable window, not by restoring an
arbitrary prior cadence.

### Option B — restore prior cadence by widening the saturation band
To admit post p75 (`|fd| ≈ 0.0205`) at `G=0.6` needs
`0.4·sat ≥ 0.0205` ⇒ `sat ≳ 0.051`, i.e. `_FLIP_FALLBACK_PCT 0.02 →
~0.05` (and `_FLIP_MAX_PCT` above that, and the `k·σ` constant).
*Illustrative only.* Powerful but **broad blast radius**:
`flip_distance` feeds the `gamma_anchor` blend used well beyond these
two patterns. Only justify if §5 hit-rate shows the patterns are
net-positive and the org wants them firing at the prior rate.

### Option C — lower `_FLIP_DISTANCE_MIN` only (REJECTED as insufficient)
At fallback σ, restoring ~76% needs `0.02·(1−G) ≥ 0.0205` ⇒ `G ≲ −0.03`
— infeasible (gate becomes trivially always-true). Documented so the
"just lower 0.6" instinct is visibly ruled out at normal σ.

## 5. Required before adoption

This window is ~4 trading days (post n=1013) **and non-stationary** —
those rows may span 7106711 / 1731efc / 62c70df. Do **not** decide on
it. When the flip calc has been stable in production for ≥ ~2 weeks:

```
make gamma-flip-revalidate GAMMA_FLIP_STABLE_SINCE=<62c70df prod-deploy ET instant>
python -m src.signals.playbook.backtest --no-write --days <stable-window-days>
```

`GAMMA_FLIP_STABLE_SINCE` restricts the POST era to the single settled
definition (transitional rows excluded and reported). Decide **A vs B**
from the stable distribution **plus** the flip-pattern hit-rates. Loop
in the owner of `62c70df` / `1731efc` since the flip calc is theirs and
still moving.

## 6. Decision log (to be completed by owner)

| date | reviewer | stable-window result | decision (A / B / other) | applied? |
|---|---|---|---|---|
| | | | | no |

---
*Generated as a review artifact for the `claude/fix-gex-calculation-KkUVW`
follow-up. The re-validation tooling (`src/tools/gamma_flip_revalidation.py`,
`make gamma-flip-revalidate`) is read-only and changed no thresholds.*
