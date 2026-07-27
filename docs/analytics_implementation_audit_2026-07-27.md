# Analytics implementation audit — 2026-07-27

## Scope and conclusion

This audit traced the public analytics and signal outputs through `src/analytics`,
`src/signals`, `src/api`, playbook patterns, and their tests. The implementation
uses a **modeled**, traditional dealer-position convention; public OI is not an
observation of dealer ownership. No API field was removed or renamed.

### Correct as implemented

* Net GEX is `call_gex - gross_put_gex` (equivalently signed `call_gex +
  put_gex`): modeled calls positive, modeled puts negative, with the same
  contract multiplier and dollar-per-1%-spot scaling on both sides.
* Canonical walls aggregate OI-weighted gamma across the caller-selected
  expirations, include spot equality, rank calls above spot and unsigned puts
  below spot, reject unusable values, and deterministically prefer the nearer
  strike on a tie. Wall inputs are official snapshot OI; volume does not create
  intraday OI. A Put Wall is a structural concentration, not automatic buying
  or a guaranteed floor.
* Max Pain minimizes aggregate call-plus-put intrinsic payout over eligible
  strikes, applies the 100-share multiplier, has a lowest-strike tie break, and
  returns null when no usable OI exists. It is a hypothetical settlement
  statistic, not a target forecast.
* Black-Scholes gamma uses settlement-aware positive TTE and returns a finite
  zero for expired/degenerate inputs. ATM gamma is allowed to concentrate as
  expiry approaches; expiration and an OI refresh remain distinct events.
* Charm and vanna finite differences use calendar-day and one-vol-point units,
  respectively. Their aggregate outputs are conditional modeled hedge
  sensitivities, not observed or scheduled orders.
* Forced Flow compares full-repriced modeled dealer delta on the same option
  universe. Its hedge sign offsets the options delta change; first-order
  gamma/charm/vanna components reconcile through an explicit residual.
* Composite signal outputs are clamped. Missing inputs are gated or surfaced;
  model scores and playbook confidence are heuristic ranks, not calibrated win
  probabilities unless a separate backtest result explicitly says otherwise.

### Correct but previously poorly described

Dealer-gamma comments incorrectly described dealers as short calls/long puts
while assigning calls positive and puts negative. Descriptions now say the
actual convention: modeled dealer-long calls and dealer-short puts. Vanna,
charm, and delta-pressure descriptions now distinguish modeled sensitivity
from observed execution.

### Technically incorrect and corrected

1. **EOD Pressure:** negative gamma formerly multiplied target distance by
   `-1`, mechanically inventing repulsion from Max Pain/Max Gamma. It now uses
   the causal signed return from the oldest to newest supplied `recent_closes`
   in negative gamma. Missing/non-finite GEX, or fewer than two usable closes,
   produces a neutral contribution. Zero GEX retains the established
   non-negative/attraction boundary. The final score remains clamped to
   `[-1, 1]` after time and calendar multipliers.
2. **Positioning Trap Squeeze playbook:** `positioning_trap.score` already
   points in the squeeze-resolution direction, but the playbook inverted it a
   second time and required tape with the opposite sign. Direction and tape
   gating now use the score's published contract.

### House heuristics retained

The EOD weights, 0.3% pin and momentum saturation levels, ATM charm band,
calendar multipliers, trigger thresholds, signal weights, and playbook
confluence adjustments are deterministic house heuristics. They are not
claimed to be empirically optimal or calibrated probabilities.

### Requires empirical/product decisions

Backtesting—not option-theory edits—is required to establish Put Wall support
reliability, Call Wall resistance reliability, selected Gamma Flip playbook
performance, SPX/SPY confluence value, pin thresholds, EOD Pressure predictive
value, score calibration, Squeeze Setup/Positioning Trap/Trap Detection
performance, and ranking of aggressor/premium flow metrics. Aggressor side and
premium do not reveal opening/closing status, ultimate ownership, strategy, or
information advantage; legacy `smart_*` names therefore remain compatibility
fields describing aggressor-classified premium, not “informed money.”

## Gamma Flip resolver

The canonical profile reprices gamma on a hypothetical spot grid with
strike-level snapshot IV held sticky-strike, uses the same modeled dealer sign,
and detects interpolated zero crossings. The resolver deterministically selects
the qualifying crossing nearest spot; adaptive structural filters may reject
weak, edge, or distant candidates and return null. Multiple crossings are local
regime boundaries: selecting one does **not** imply every point globally above
it is positive or every point globally below it is negative. A wall crossing is
not a Gamma Flip crossing.

## Historical-output impact

* **Net GEX, walls, Gamma Flip, Max Pain, Forced Flow:** no formula/value change
  in this audit; wording changes only.
* **EOD Pressure:** historical negative-gamma observations can change. Old pin
  component: `-clip((target-close)/close/0.003, -1, 1)`. New negative-gamma
  component: `clip((last_recent_close-first_recent_close)/first_recent_close /
  0.003, -1, 1)`. Positive- and zero-gamma target attraction is unchanged.
  Missing directional history now neutralizes this component. Signal magnitude,
  sign, and the `abs(score) >= 0.2` trigger frequency may therefore differ.
  This change is mechanically motivated and is **not empirically calibrated**.
* **Positioning Trap Squeeze:** historical action-card direction and trigger
  events can change because the prior implementation double-inverted the
  positioning score and applied an inconsistent tape gate. API shape is
  unchanged.
* **API payloads:** no fields were removed, renamed, or made newly required.
  EOD context adds non-breaking diagnostic fields (`pin_component` and
  `directional_return`) and may return `gamma_regime="unknown"` for invalid
  GEX rather than throwing or inventing a direction.

