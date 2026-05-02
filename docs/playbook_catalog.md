# Playbook Engine: Pattern Catalog Specification

**Status:** Draft (PR-1 deliverable)
**Branch:** `claude/clarify-composite-score-api-Yw2vp`
**Replaces:** Implicit decision logic in `portfolio_engine.py` and the direct-trigger bypass that lets advanced signals enter trades around the MSI conviction gate (`portfolio_engine.py:354` — `aggregation["advanced_trigger"]` / `["confluence_trigger"]`).
**Does NOT replace:** Existing `/api/signals/*` endpoints. All keep their current response shapes. Adds one new endpoint: `/api/signals/action`.

---

## Table of Contents

1. [What this is](#1-what-this-is)
2. [Action Card schema](#2-action-card-schema)
3. [Pattern interface](#3-pattern-interface)
4. [Evaluation rules](#4-evaluation-rules)
5. [Confidence rubric](#5-confidence-rubric)
6. [STAND_DOWN logic](#6-stand_down-logic)
7. [Pattern catalog](#7-pattern-catalog)
   - 7.1 [Tier 1: 0DTE (5 patterns)](#tier-1-0dte)
   - 7.2 [Tier 2: 1DTE (2 patterns)](#tier-2-1dte)
   - 7.3 [Tier 3: Swing 2–3 day (5 patterns)](#tier-3-swing-23-day)
8. [Open questions / PR-2+ dependencies](#8-open-questions--pr-2-dependencies)
9. [Out of scope for PR-1](#9-out-of-scope-for-pr-1)

---

## 1. What this is

The **Playbook Engine** consumes:

- **Market Posture** (the existing `/api/signals/score` MSI composite, treated as a regime tag)
- **Advanced Signals** (existing `/api/signals/advanced/*` outputs)
- **Basic Signals** (existing `/api/signals/basic/*` outputs)
- **Live market structure**: gamma flip, max-gamma strike, call wall, put wall, max pain, VWAP, opening range, recent closes
- **Position state**: any open positions, for management Cards

…and emits **exactly one Action Card** per evaluation cycle, per underlying in `SIGNALS_UNDERLYINGS`. The Card is the unambiguous trade instruction: instrument, entry, target, stop, size guidance, confidence, plain-English rationale.

A Card is one of:

- A specific trade (`BUY_*` / `SELL_*` enum values)
- A management action on an existing position (`TAKE_PROFIT`, `TIGHTEN_STOP`, `CLOSE`)
- `STAND_DOWN` — emitted **only when no pattern matches**. Not a default for "we're not sure."

The Playbook does not size trades — it specifies them. Sizing remains in `portfolio_engine.py`, which consumes Action Cards going forward.

---

## 2. Action Card schema

```json
{
  "underlying": "SPY",
  "timestamp": "2026-05-01T18:42:13Z",
  "action": "BUY_PUT_DEBIT",
  "pattern": "call_wall_fade",
  "tier": "0DTE",
  "direction": "bearish",
  "confidence": 0.68,
  "size_multiplier": 0.6,
  "max_hold_minutes": 90,
  "legs": [
    {
      "expiry": "2026-05-01",
      "strike": 678.0,
      "right": "P",
      "side": "BUY",
      "qty": 1
    }
  ],
  "entry": {
    "ref_price": 678.40,
    "limit_premium": 0.65,
    "trigger": "at_market"
  },
  "target": {
    "ref_price": 675.00,
    "exit_premium": 1.95,
    "kind": "level",
    "level_name": "max_pain"
  },
  "stop": {
    "ref_price": 679.55,
    "exit_premium": 0.30,
    "kind": "level",
    "level_name": "call_wall_break"
  },
  "rationale": "Pinned at 678 max-gamma + smart money dumping calls + low VIX = fade rally into close.",
  "context": {
    "msi": 0,
    "regime": "high_risk_reversal",
    "net_gex": 7100190798.9,
    "call_wall": 678.0,
    "put_wall": 674.0,
    "gamma_flip": 676.5,
    "max_pain": 675.0,
    "vwap": 677.8,
    "advanced_signals_aligned": ["trap_detection", "gamma_vwap_confluence"],
    "basic_signals_aligned": ["positioning_trap", "tape_flow_bias"]
  },
  "alternatives_considered": [
    {"pattern": "stand_down", "reason": "rejected: pattern matched"},
    {"pattern": "buy_put_spread", "reason": "rejected: lower confidence (0.52)"}
  ]
}
```

### Action enum

| Value | Meaning |
|---|---|
| `BUY_CALL_DEBIT`, `BUY_PUT_DEBIT` | Single-leg debit |
| `BUY_CALL_SPREAD`, `BUY_PUT_SPREAD` | Vertical debit spread |
| `SELL_CALL_SPREAD`, `SELL_PUT_SPREAD` | Vertical credit spread |
| `BUY_IRON_CONDOR`, `SELL_IRON_CONDOR` | 4-leg defined risk |
| `BUY_BUTTERFLY`, `BUY_CALENDAR`, `BUY_DIAGONAL` | Multi-leg structures |
| `TAKE_PROFIT`, `TIGHTEN_STOP`, `CLOSE` | Management on existing position |
| `STAND_DOWN` | No pattern matches |

### Field semantics

- `legs[]` describes the structure exactly: 1 leg = single, 2 legs = vertical, 4 legs = condor, etc. Any structure expressible.
- `entry.trigger` ∈ `"at_market"` | `"at_touch"` | `"on_break"` | `"at_close"` | `"at_open_next"`.
- `target.kind` and `stop.kind` ∈ `"level"` | `"premium_pct"` | `"time"` | `"signal_event"`.
- `confidence` ∈ `[0.20, 0.95]`. Values outside this range are clamped — never claim certainty.
- `size_multiplier` is a hint to `portfolio_engine`. Final position size is portfolio-engine's call.
- `alternatives_considered[]` is the audit trail of patterns that almost won.

---

## 3. Pattern interface

```python
class PatternBase:
    id: str                           # snake_case, unique
    name: str                         # display name
    tier: Literal["0DTE", "1DTE", "swing"]
    direction: Literal["bullish", "bearish", "non_directional", "context_dependent"]
    valid_regimes: list[str]          # subset of {"trend_expansion", "controlled_trend", "chop_range", "high_risk_reversal"}

    def match(self, ctx: PlaybookContext) -> ActionCard | None:
        """Return None if conditions not met; ActionCard if matched."""
```

`PlaybookContext` extends the existing `MarketContext` (`src/signals/components/base.py`) with:

- The MSI snapshot (composite_score, regime, components)
- Latest advanced signal snapshots (`score`, `clamped_score`, `triggered`, `signal`, `context_values`)
- Latest basic signal snapshots (same shape)
- Live levels: `call_wall`, `put_wall`, `gamma_flip`, `max_pain`, `max_gamma_strike`, `vwap`, `opening_range_high`, `opening_range_low`
- Time-of-day: `et_time`, `is_first_30min`, `is_last_30min`, `minutes_to_close`, `day_of_week`
- Position state: `current_positions[]` (empty list when flat)

### File layout

- Built-in patterns: `src/signals/playbook/patterns/<pattern_id>.py`, each exporting `PATTERN: PatternBase`.
- Custom patterns: discovered from `SIGNALS_PLAYBOOK_CUSTOM_DIR` (default `~/.zerogex/playbook/custom/`). Same interface; auto-loaded at engine start. **No code changes needed to add new patterns** — just drop a Python file.

---

## 4. Evaluation rules

On every scoring cycle, for each underlying in `SIGNALS_UNDERLYINGS`:

1. Build `PlaybookContext`.
2. Collect candidate Cards by calling `match()` on every registered pattern (built-in + custom).
3. **Regime gate**: drop Cards whose pattern's `valid_regimes` doesn't include the current MSI regime.
4. **Position-state gate**: management Cards (`TAKE_PROFIT`, `TIGHTEN_STOP`, `CLOSE`) are only valid when there's a matching open position. Entry Cards are dropped when the same pattern's prior trade is still open within its `max_hold_minutes`.
5. **Confidence floor**: drop Cards with `confidence < 0.25`.
6. **Hysteresis**: drop a Card if the same `(pattern_id, instrument_signature)` was emitted within the pattern's tier dwell window (default 5 min for 0DTE, 15 min for 1DTE, 60 min for swing).
7. **Resolve**:
   - 0 surviving Cards → emit a structured `STAND_DOWN` Card. See §6.
   - 1 → emit it.
   - >1 → highest `confidence` wins. Ties broken by tier priority:
     - During regular hours and pre-15:30 ET: `0DTE > 1DTE > swing`.
     - 15:30 ET onward: `1DTE > swing > 0DTE` (0DTE is closing out; 1DTE is the live setup).
     - End-of-day swing scoring (after 15:55 ET): `swing > 1DTE > 0DTE`.
   - Final tie: alphabetical pattern ID for determinism.
8. Surface losing candidates in `alternatives_considered[]` with a one-line reason each.

---

## 5. Confidence rubric

```
confidence = pattern_base * confluence_multiplier * regime_fit
confidence = clamp(confidence, 0.20, 0.95)
```

### `pattern_base` (0.40 – 0.85)

The historical hit rate of the pattern, capped to prevent overstating untested patterns.

**Until PR-3 backtests land, every pattern uses its prior-belief base specified in §7.** These priors range 0.50–0.55 and reflect intuition, not data. PR-3 onwards replaces each prior with the empirical hit rate over a ≥ 90-day backtest window. A pattern whose backtested hit rate is below 0.40 is automatically dropped from the catalog.

### `confluence_multiplier` (0.7 – 1.4)

How many independent signals align with the pattern's thesis vs oppose it.

- Each pattern lists `confluence_signals_for[]` (signals that, if aligned, add support) and `confluence_signals_against[]` (signals that, if aligned, oppose).
- Each aligned-for signal adds +0.05 to the multiplier (starting from 1.0).
- Each aligned-against signal subtracts -0.10.
- Clamped to [0.7, 1.4].

### `regime_fit` (0.8 – 1.1)

How well the current MSI regime matches the pattern's preferred regime.

- Pattern's preferred regime exact match → 1.1
- Adjacent regime in the gradient (`trend_expansion ↔ controlled_trend ↔ chop_range ↔ high_risk_reversal`) → 1.0
- Declared-valid but two steps away → 0.8

---

## 6. STAND_DOWN logic

`STAND_DOWN` is emitted only when, after gating and hysteresis:

- Zero patterns produced a surviving Card, OR
- All surviving Cards have `confidence < 0.25`, AND
- There is no open position requiring management.

The Card carries a structured rationale:

```json
{
  "action": "STAND_DOWN",
  "rationale": "No tradable structure: price 0.6% from nearest gamma level, no flow imbalance above floor, no breakout setup.",
  "near_misses": [
    {
      "pattern": "call_wall_fade",
      "missing": [
        "price not within 0.2% of call_wall (currently 0.45% away)",
        "trap_detection.signal != 'bearish_fade' (currently 'none')"
      ]
    },
    {
      "pattern": "gamma_flip_break",
      "missing": [
        "no 5-min cross of gamma_flip in last 30 minutes",
        "vol_expansion.triggered == false"
      ]
    }
  ]
}
```

This is **information**, not equivocation. A trader reading this knows precisely why nothing's there.

---

## 7. Pattern catalog

> **Threshold notation.** All numeric thresholds in this section are **priors** intended to be tuned in PR-3 against backtest data. Each pattern's implementation will expose its thresholds as env-overridable constants.

> **Signal score scale.** Advanced/basic signal scores are on `[-100, +100]` (per existing API). Component scores from MSI are on `[-1, +1]`. Thresholds below match the field they reference.

### Tier 1: 0DTE

#### 1.1 `call_wall_fade` — Fade Touches of the Call Wall (long-gamma)

**Thesis.** Net positive GEX → dealers long gamma → they sell into rallies. When price tags the call wall (highest call-side gamma strike), the dealer sell program peaks. Fades reliably in chop / long-gamma regimes.

**Direction:** bearish. **Tier:** 0DTE. **Valid regimes:** `chop_range`, `high_risk_reversal`. **Pattern base:** 0.55.

**Triggers (all required):**
- `net_gex > +1.5e9`
- `|close - call_wall| / close <= 0.0020` (within 0.2%)
- `et_time >= 10:00`
- `tape_flow_bias.score <= -20` OR `order_flow_imbalance.score <= -20`
- `trap_detection.signal == "bearish_fade"` OR `gamma_vwap_confluence.signal == "bearish_confluence"`
- `range_break_imminence.label != "Breakout Mode"`

**Instrument:**
- Default: `SELL_CALL_SPREAD` — short `call_wall` strike, long `call_wall + 5pts` (or `+1 × ATR(5min)` rounded to strike grid). 0DTE.
- If `realized_sigma(30min) > 0.0025`: `BUY_PUT_DEBIT` at `call_wall` strike instead (vol high enough that debit beats credit's theta drag).

**Entry:** `at_touch` of `call_wall`. Limit at mid + 0.05.

**Target:** `max_pain` if below; else `gamma_flip` if below; else 50% of max profit (spread) / +100% of debit (long put).

**Stop:** Close above `call_wall × 1.0030`, or 200% of credit lost (spread) / -50% premium (debit).

**Max hold:** 90 minutes or close-of-day, whichever first.

**Confluence signals:**
- For: `positioning_trap` (bearish), `dealer_delta_pressure` (bearish), `vanna_charm_flow` (bearish)
- Against: `range_break_imminence.label == "Breakout Mode"`, `vol_expansion.triggered`

**Notes:** -0.10 confidence penalty when `vix_level > 22` (trend regime drowns walls).

---

#### 1.2 `put_wall_bounce` — Bounce Off the Put Wall (long-gamma)

Mirror of `call_wall_fade` with reversed signs.

**Direction:** bullish. **Tier:** 0DTE. **Valid regimes:** `chop_range`, `high_risk_reversal`. **Pattern base:** 0.55.

**Triggers:**
- `net_gex > +1.5e9`
- `|close - put_wall| / close <= 0.0020`
- `et_time >= 10:00`
- `tape_flow_bias.score >= +20` OR `order_flow_imbalance.score >= +20`
- `trap_detection.signal == "bullish_fade"` OR `gamma_vwap_confluence.signal == "bullish_confluence"`
- `range_break_imminence.label != "Breakout Mode"`

**Instrument:**
- Default: `SELL_PUT_SPREAD` — short `put_wall` strike, long `put_wall - 5pts`. 0DTE.
- If `realized_sigma(30min) > 0.0025`: `BUY_CALL_DEBIT` at `put_wall` strike.

**Entry:** `at_touch` of `put_wall`.

**Target:** `max_pain` if above; else `gamma_flip` if above; else 50% / +100%.

**Stop:** Close below `put_wall × 0.9970`, or 200% credit / -50% debit.

**Max hold:** 90 minutes / EOD.

**Confluence signals:** mirror of 1.1.

---

#### 1.3 `gamma_flip_break` — Trade Through the Gamma Flip

**Thesis.** Crossing the gamma flip transitions dealers from suppressing moves (above flip / long-gamma) to amplifying them (below flip / short-gamma). The cross direction is the trade.

**Direction:** context_dependent (matches cross). **Tier:** 0DTE. **Valid regimes:** all four. **Pattern base:** 0.50.

**Triggers:**
- `gamma_anchor.flip_distance_subscore >= +0.6` (price near flip per vol-adaptive saturation)
- A 5-min bar closes on the opposite side of `gamma_flip` from the prior 30-min mode
- `range_break_imminence.label in ["Break Watch", "Breakout Mode"]`
- `vol_expansion.triggered == true`
- `et_time >= 10:00`

**Instrument:** `BUY_CALL_DEBIT` (above-to-below flip cross is bearish — but cross direction depends on which side dealers were on; spec convention is *trade with the cross*). Single-leg ATM 0DTE.

**Entry:** `on_break` — break + 0.05% buffer past `gamma_flip` (avoid wick fakeouts).

**Target:** Next major gamma level in cross direction (`max_gamma_strike` on the new side), OR `2 × realized_sigma(30min) × close` from entry, whichever closer.

**Stop:** Back through `gamma_flip` ± 0.05% buffer.

**Max hold:** 60 minutes (gamma transitions fade fast).

**Confluence signals:**
- For: `gex_gradient` (aligned with cross), `tape_flow_bias`, `order_flow_imbalance` (aligned)
- Against: `gamma_vwap_confluence.regime_direction == "mean_reversion"`

**Notes:** -0.15 confidence penalty when `realized_sigma(30min) < 0.0010` (no vol to power the move).

---

#### 1.4 `eod_pressure_drift` — Last-Hour Hedging Drift

**Thesis.** In the last hour, dealer 0DTE hedging dominates flow. The `eod_pressure` signal aggregates this directional pressure; lean into it.

**Direction:** context_dependent. **Tier:** 0DTE. **Valid regimes:** all four. **Pattern base:** 0.55.

**Triggers:**
- `et_time >= 15:00`
- `eod_pressure.triggered == true` AND `|eod_pressure.score| >= 30`
- No opposing wall (call_wall above for bullish drift, put_wall below for bearish) within 0.30%
- No same-direction position currently open

**Instrument:** `BUY_CALL_DEBIT` or `BUY_PUT_DEBIT`, ATM 0DTE.

**Entry:** at signal trigger + 1 confirming 1-min bar in direction.

**Target:** VWAP + (1.5 × distance from VWAP at entry), OR +30% premium, whichever first.

**Stop:** VWAP cross against position, OR close past breakout level, OR -50% premium.

**Max hold:** until 15:55 ET (close before the bell).

**Confluence signals:**
- For: `0dte_position_imbalance` (aligned), `dealer_delta_pressure` (aligned), `gamma_anchor` (neutral-to-aligned)
- Against: opposing-direction `tape_flow_bias`

---

#### 1.5 `zero_dte_imbalance_drift` — Smart-Money 0DTE Bias

**Thesis.** Smart-money 0DTE flow leads price by ~30s on liquid names. Heavy one-sided flow drags price.

**Direction:** context_dependent. **Tier:** 0DTE. **Valid regimes:** `controlled_trend`, `trend_expansion`, `chop_range`. **Pattern base:** 0.50.

**Triggers:**
- `0dte_position_imbalance.triggered == true` AND `|score| >= 30`
- `et_time >= 11:00` AND `et_time <= 14:30` (avoid open noise and EOD overlap)
- `flow_source == "zero_dte"` (not the all-expiry fallback)
- `trap_detection.triggered == false` OR `trap_detection.signal` aligned with imbalance direction

**Instrument:** `BUY_CALL_SPREAD` (call-heavy) or `BUY_PUT_SPREAD` (put-heavy). 0DTE, +5 strike width debit spread.

**Entry:** at trigger.

**Target:** `+2 × ATR(5min)` from entry, OR first opposing wall, whichever closer.

**Stop:** `-0.5 × ATR(5min)` from entry, OR -50% premium.

**Max hold:** 90 minutes.

**Confluence signals:**
- For: `tape_flow_bias` (aligned), `vanna_charm_flow` (aligned)
- Against: `range_break_imminence.label == "Range Fade"` (drift won't run)

---

### Tier 2: 1DTE

#### 2.1 `pin_risk_premium_sell` — Sell Premium into Overnight Pin

**Thesis.** Price tightly bracketed between max_pain and the nearest wall in a long-gamma regime; dealer hedging pins overnight. Sell defined-risk premium.

**Direction:** non_directional. **Tier:** 1DTE. **Valid regimes:** `chop_range`, `high_risk_reversal`. **Pattern base:** 0.50.

**Triggers:**
- `et_time >= 15:30`
- `net_gex > +2.0e9`
- `|close - max_pain| / close <= 0.0030`
- `realized_sigma(last 30min) <= 0.0012`
- `range_break_imminence.label in ["Range Fade", "Weak Range"]`
- No major economic event scheduled before next regular open (see §8 dependency #1)

**Instrument:** `BUY_IRON_CONDOR` centered on `max_pain`. Wings at `max_pain ± 2 × realized_sigma(30min) × close`. 1DTE.

**Entry:** `at_close` window 15:35–15:50 ET.

**Target:** 50% of max profit by next-day 11:00 ET.

**Stop:** Either wing breached overnight (close at next open if gapped through), OR `max_loss × 1.5` capped — whichever first.

**Max hold:** next-day 14:00 ET (close before 1DTE pin acceleration).

**Confluence signals:**
- For: `gamma_anchor.local_gamma_subscore <= -0.5`, `gex_gradient.score` near zero, `volatility_regime.score <= -0.4`
- Against: `vol_expansion.triggered`, `0dte_position_imbalance.triggered`

---

#### 2.2 `overnight_trap_continuation` — Trap Reversal Held Overnight

**Thesis.** Failed end-of-day breakouts that fire `trap_detection` in the last hour often extend overnight as foreign markets and after-hours flow continue to fade the original direction.

**Direction:** context_dependent. **Tier:** 1DTE. **Valid regimes:** all four. **Pattern base:** 0.55.

**Triggers:**
- `et_time >= 14:30`
- `trap_detection.triggered == true`
- `trap_detection.context_values.wall_migrated_up == false` AND `wall_migrated_down == false`
- `gamma_anchor.flip_distance_subscore <= 0.0` (price not at flip)

**Instrument:**
- `bearish_fade` trap → `BUY_PUT_DEBIT` 1DTE OTM by `1 × realized_sigma(daily) × close`.
- `bullish_fade` trap → `BUY_CALL_DEBIT` mirror.

**Entry:** at signal trigger.

**Target:** Prior intraday range midpoint at next-day open.

**Stop:** Wall migration in trap direction triggers immediate close (real-time monitor). Else -60% premium.

**Max hold:** next-day 11:00 ET.

**Confluence signals:**
- For: `positioning_trap` (aligned), `skew_delta` (aligned with reversal direction)
- Against: `0dte_position_imbalance` opposing direction

---

### Tier 3: Swing 2–3 Day

#### 3.1 `squeeze_breakout` — Vol-Compression Resolves

**Thesis.** Multi-day low realized vol + dense gamma + asymmetric `gex_gradient` charges potential energy. Breakout direction is pre-revealed by gradient asymmetry; net_gex flip risk indicates trigger proximity.

**Direction:** context_dependent. **Tier:** swing. **Valid regimes:** `trend_expansion`, `controlled_trend`, `chop_range`. **Pattern base:** 0.55.

**Triggers:**
- `squeeze_setup.triggered == true` for ≥ 2 consecutive trading days
- `vol_expansion.score >= 30` (vol crossing into expansion)
- `|gex_gradient.score| >= 30` (indicates breakout direction)
- `|net_gex| <= 1.0e9` (flip risk — dealer regime not entrenched)

**Instrument:** `BUY_CALL_SPREAD` or `BUY_PUT_SPREAD` 5–7 DTE, debit, +10 strike width, in `gex_gradient`-favored direction.

**Entry:** First daily close past the squeeze envelope in favored direction.

**Target:** `2 × prior 5-day true range` from entry, OR next major gamma level + 0.5%, whichever closer.

**Stop:** Daily close back inside the squeeze envelope, OR -50% premium.

**Max hold:** 3 trading days. Roll or close.

**Confluence signals:**
- For: `positioning_trap` (aligned with breakout direction — crowd squeezed), `tape_flow_bias` (aligned), `dealer_delta_pressure` (aligned)
- Against: `range_break_imminence.label == "Range Fade"`

---

#### 3.2 `skew_inversion_reversal` — Fear Spike Fade

**Thesis.** Extreme `skew_delta` (puts pricing in disproportionate fear) when underlying tape is *not* breaking down → contrarian bullish reversal.

**Direction:** bullish. **Tier:** swing. **Valid regimes:** `chop_range`, `controlled_trend`. **Pattern base:** 0.50.

**Triggers:**
- `skew_delta.score <= -50`
- `tape_flow_bias.score >= 0` (tape not actively bearish)
- `volatility_regime.score >= 0.3` (vol elevated, room for compression)
- Daily close held within 0.5% of 20-day moving average

**Instrument:** `BUY_CALL_DEBIT` 5DTE OTM by `1.5 × ATR(daily)`.

**Entry:** Next regular session open after trigger session.

**Target:** Mean of `skew_delta.score` over prior 20 days × `current ATR(daily)` → translated to upside price target. Capped at +75% premium.

**Stop:** -40% premium, OR `skew_delta` makes new 20-day low (thesis broken).

**Max hold:** 3 trading days.

**Confluence signals:**
- For: `vanna_charm_flow` (bullish), `positioning_trap` (showing put crowding)
- Against: `dealer_delta_pressure` (bearish)

---

#### 3.3 `vanna_charm_glide` — End-of-Week Hedging Drift

**Thesis.** Vanna + charm pressure both push the same direction across multiple days; dealer hedging unwind into Friday close amplifies the drift.

**Direction:** context_dependent. **Tier:** swing. **Valid regimes:** `controlled_trend`, `chop_range`. **Pattern base:** 0.50.

**Triggers:**
- `|vanna_charm_flow.score| >= 40` for ≥ 2 consecutive days, same sign
- `day_of_week in ["Tue", "Wed", "Thu"]` (need glide runway into Friday)
- `positioning_trap.score` aligned (crowd not against the drift)

**Instrument:** `BUY_CALL_DEBIT` or `BUY_PUT_DEBIT`, Friday-expiry ATM, in drift direction.

**Entry:** Trigger day's close.

**Target:** Friday open + `drift_direction × 2 × ATR(daily)`.

**Stop:** `vanna_charm_flow.score` sign-flips, OR -50% premium.

**Max hold:** Friday 14:00 ET.

**Confluence signals:**
- For: `gex_gradient` (aligned), `tape_flow_bias` (aligned)
- Against: `range_break_imminence.label == "Breakout Mode"` (this is drift, not break)

---

#### 3.4 `positioning_trap_squeeze` — One-Way Crowding Squeeze

**Thesis.** `positioning_trap` flags one-sided positioning; the squeeze against the crowd produces multi-day moves once the tape starts cooperating.

**Direction:** context_dependent. **Tier:** swing. **Valid regimes:** `chop_range`, `controlled_trend`, `high_risk_reversal`. **Pattern base:** 0.55.

**Triggers:**
- `|positioning_trap.score| >= 50` (crowd heavily positioned)
- `tape_flow_bias.score` opposite sign to `positioning_trap.score` (tape turning against crowd)
- `volatility_regime.score >= -0.2` (some vol to fuel the squeeze)

**Instrument:** `BUY_CALL_SPREAD` or `BUY_PUT_SPREAD`, 5–7 DTE, debit, opposite the crowd's positioning, +10 strike width.

**Entry:** Next daily close in squeeze direction.

**Target:** `2 × prior 5-day range` against the crowd.

**Stop:** `positioning_trap.score` magnitude retreats by 30%+ (crowd unwinds gracefully) OR -50% premium.

**Max hold:** 3 trading days.

**Confluence signals:**
- For: `skew_delta` (confirms crowd extreme pricing), `dealer_delta_pressure` (aligned with squeeze)
- Against: `vanna_charm_flow` opposite squeeze direction

---

#### 3.5 `gex_gradient_trend` — Asymmetric Gamma Drift

**Thesis.** Asymmetric dealer gamma above vs below spot creates a multi-day drift toward the lower-gamma direction (less hedging resistance there).

**Direction:** context_dependent. **Tier:** swing. **Valid regimes:** `controlled_trend`, `chop_range`. **Pattern base:** 0.50.

**Triggers:**
- `|gex_gradient.score| >= 40` for ≥ 1 day
- `net_gex` sign agrees with gradient drift direction
- `range_break_imminence.label != "Breakout Mode"` (this is drift, not break)
- `volatility_regime.score >= -0.5`

**Instrument:** `BUY_CALL_DEBIT` or `BUY_PUT_DEBIT`, 5DTE OTM by `0.5 × ATR(daily)`, in drift direction.

**Entry:** Trigger day's close + 1 confirming 4-hour bar in direction.

**Target:** Drift direction × `1.5 × ATR(daily)`. Capped at +75% premium.

**Stop:** `|gex_gradient.score|` drops below 20, OR -50% premium.

**Max hold:** 3 trading days.

**Confluence signals:**
- For: `dealer_delta_pressure` (aligned), `tape_flow_bias` (aligned)
- Against: `vol_expansion.triggered` (breakout overrides drift)

---

## 8. Open questions / PR-2+ dependencies

1. **Event calendar input.** `pin_risk_premium_sell` needs awareness of overnight FOMC / CPI / earnings risk. PR-2 must wire an economic event calendar source into `PlaybookContext`. Until then, the pattern's trigger conservatively requires no scheduled high-impact event in the next 18 hours — fall back to "skip this pattern" when the calendar source isn't available.

2. **Per-pattern backtest baselines.** PR-3 onward must produce hit rates per pattern over ≥ 90 days of historical data. Each pattern that fails to clear 0.40 hit rate gets dropped. Until then, all patterns use the priors specified in §7.

3. **Position-state Cards (`TAKE_PROFIT` / `TIGHTEN_STOP` / `CLOSE`).** Intentionally absent from PR-1 catalog — management lives in `portfolio_engine` already. Re-implementing it in Playbook is deferred until telemetry shows portfolio-engine management is suboptimal. PR-2 wires the *plumbing* for management Cards (action enum, evaluation order); patterns are added later.

4. **Multi-symbol coordination.** When `SIGNALS_UNDERLYINGS` contains multiple symbols, do correlated patterns across symbols need awareness of each other (e.g., SPY and QQQ both firing the same setup)? PR-1 default: each symbol evaluated independently. Revisit if portfolio engine reports excessive correlated drawdowns.

5. **Hysteresis dwell windows.** The 5/15/60-minute defaults in §4.6 are priors. PR-3 backtests should validate or tune.

6. **Action Card persistence.** PR-2 must spec a new table (`signal_action_cards`?) to persist emitted Cards for audit, backtesting, and the `/api/signals/action` history endpoint. Schema deferred to PR-2.

7. **`vix_level` consumption inside patterns.** Currently sourced via `MarketContext.extra.vix_level`. PR-2 must confirm this remains populated for the new `PlaybookContext`.

---

## 9. Out of scope for PR-1

- No code changes — this is a spec document only.
- No changes to existing `/api/signals/*` endpoint shapes.
- No changes to `scoring_engine.py`, `portfolio_engine.py`, or any signal component.
- No changes to the database schema.
- No removal of the `advanced_trigger` / `confluence_trigger` bypass — that gets stripped in the **final** PR after every advanced signal has been ported and backtest-validated.
- No backtesting work. Pattern priors in §7 are intuition, not data; PR-3 onward replaces them.

**PR-1 deliverable:** this document, reviewed and approved.

---

## 10. Implementation status

| PR | Scope | Status |
|---|---|---|
| PR-1 | This spec document | ✅ Shipped |
| PR-2 | Engine scaffold + types + `/api/signals/action` + `call_wall_fade` | ✅ Shipped |
| PR-3 | `put_wall_bounce` + Action Card persistence (`signal_action_cards` table) + real hysteresis | ✅ Shipped |
| PR-4 | `gamma_flip_break` (context-dependent direction; first non-mirror pattern) | ✅ Shipped |
| PR-5 | `eod_pressure_drift` | ⏳ |
| PR-6 | `zero_dte_imbalance_drift` | ⏳ |
| PR-7 | Tier 2 patterns: `pin_risk_premium_sell`, `overnight_trap_continuation` | ⏳ |
| PR-8–11 | Tier 3 swing patterns | ⏳ |
| Final | Backtest validation per pattern, then strip `advanced_trigger` / `confluence_trigger` bypass from `portfolio_engine.py` | ⏳ |

After PR-3 the Playbook is feature-complete enough to drive trade
selection — the engine produces persistable Cards, hysteresis works,
and two opposite-direction patterns are wired in.  Subsequent PRs add
breadth (more patterns) and validation (backtests).
