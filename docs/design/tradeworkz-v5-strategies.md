# TradeWorkz v5 — Unconsumed-Data Strategies

**Status:** candidates (registered + backtestable; **not live**) — first
screen run 2026-08-15, results + calibration in §8; `hedge_impulse_quiet_tape`
SCREENED OUT.
**Date:** 2026-08-15
**Extends:** `tradeworkz-edge-strategies.md` (the v4 fleet and its screen verdicts)

---

## 1. Where the v5 edge thesis comes from

The v4 postmortems drew two hard boundaries:

1. **Static first-order levels have no edge** (the entire retired fleet;
   confirmed exhaustively for walls by the 32-config execution sweep — best
   PF 0.67, not one robust config).
2. **Following aggregate flow on 0DTE has no edge** (two decisive failures,
   PF 0.31 / 0.33 over 865 trades; the 33%-win / wins-smaller-than-losses
   signature says a flow burst *marks a local extreme*).

What v4 left standing: the *mechanism* bots (charm, vanna) read breakeven-to-
promising but underpowered, and the platform's richest data never reached the
bot tier at all. A full audit of the analytics engine, schema, and signals
layer (2026-08-15) produced a concrete inventory of **computed-but-unconsumed
data** — fields ZeroGEX writes every cycle that no strategy, retired or live,
has ever read:

| Untapped layer | Where it lives | What it encodes |
|---|---|---|
| Forced-flow **decomposition** | `forced_flow_profile.close_charm_flow` vs `close_charm_flow_smooth` | raw − smooth isolates the pure **0DTE settlement-unwind** hedge (TW previously collapsed the pair to one value) |
| **Dual-convention flip** | `gex_summary.gamma_flip_raw` + `gamma_flip_span_used` | where the un-DTE-weighted (0DTE-dominated, fast-rehedging) book flips vs the structural flip — the SPREAD between conventions is a ZeroGEX-only quantity |
| **Curve geometry** | `gex_profile.profile` (JSONB, 40–100 BS-repriced points) | the dealers' full forced order schedule G(s): shelf depth, one-sidedness, trough location — consumed by one frontend overlay, zero bots |
| **Hedge obligation** | `flow_contract_facts.delta` × Lee-Ready `buy/sell_volume` | the literal SHARES dealers must trade to hedge recent flow — the obligation, not the sentiment |
| **Per-type flow split** | `flow_contract_facts.option_type` buy/sell premium | put-only panic vs two-way repricing; every prior flow bot read symbol aggregates |
| **Bucketed charm ladder** | `gex_by_strike.dealer_charm_exposure` × `expiration_bucket` | the weekly (1–7 DTE) book's deterministic time-decay rebalance vs the 0DTE book — TW consumed only a front-expiration total |

The v5 thesis in one line:

> **Trade the mechanisms the data already quantifies but nobody consumes —
> and encode each v4 postmortem as a hard entry gate, not a hope.**

Every v5 bot names who is *forced* to trade and why; none references a static
published level as its signal; the one flow-direction bot is built as the
structural inverse of the two that died (delta-weighted obligation + a
flat-tape gate that *refuses* the entries the dead bots took); and per the
operator's directive, entries/exits are custom per bot and the structures are
not all 0DTE debit singles — the set spans debit verticals with
mechanism-located short strikes, the fleet's first **credit-exit** structure,
and the first **1DTE** candidate.

---

## 2. Data layer (what was added to the snapshot)

All additive and best-effort, mirroring the v4 pattern — a missing table or
thin history nulls only its own fields, every read is as-of bounded for the
backtest harness, and every dependent bot abstains on `None`:

- **`flow_context.fetch_forced_flow`** now returns raw and smooth separately
  → `MarketSnapshot.close_charm_flow_raw` / `close_charm_flow_smooth`
  (the collapsed smooth-preferred `close_charm_flow` is unchanged for
  back-compat) + helper `settlement_flow_residual()`.
- **`fetch_second_order_by_bucket`** → `dealer_charm_0dte/weekly`,
  `dealer_vanna_0dte/weekly` (the vanna split comes free for future bots).
- **`fetch_hedge_impulse`** → `hedge_impulse_shares` (Σ δ·(buys−sells)·100
  over the last ~15 min) + `hedge_tape_volume` + helper
  `hedge_impulse_ratio()`.
- **`fetch_put_panic_window`** → `put_panic_premium` / `put_panic_dominance`
  / `put_panic_baseline` (session median per-window baseline, so the burst
  multiple is self-scaling across SPY/SPX).
- **`fetch_profile_geometry`** (gex_profile JSONB) → `profile_gex_down/up`
  (curve ~0.5% off spot), `profile_trough_price/gex` (most negative point
  within ±1.5%).
- The existing 2-row `gex_summary` fetch gains two columns →
  `gamma_flip_raw`, `prior_gamma_flip_raw`, `gamma_flip_span_used`.
- **`BaseBot._credit_exit_decision`** — the credit-structure premium exit the
  base class's own comment deferred ("a wider profit-loss check … later"):
  opt-in per bot via `credit_take_frac` (book at a fraction of the credit;
  respects min-hold) and `credit_stop_mult` (damage-control at a multiple of
  the credit; fires past the entry grace regardless of min-hold).
  Generalizes `wall_reversion.wall_credit_exit` and adds the loss side.
  Bots that set neither param are byte-for-byte unchanged.
- `src/tools/tw_edge_field_probe.py` covers all the new columns/fields, so
  gate calibration stays data-driven (run it **before** trusting any screen).

Known data traps, designed around (not into):

- `forced_flow_profile.charm_flip` / `vanna_flip` / `profile` are persisted
  **NULL/empty** since the compute moved off the analytics path — no v5 bot
  reads them.
- `gex_by_strike.expiration_bucket` is a recent ALTER — early history may be
  null, which shrinks `weekly_charm_grind`'s effective window (probe reports
  it; the bot abstains on None).
- `dealer_charm_exposure`'s sign convention is flagged inconsistent with the
  forced-flow model-A convention. The same family already drives
  `vanna_vol_crush_rider`; the screen is the arbiter of sign — a systematic
  inversion is a **data bug to report**, never a tuning axis.
- `net_gex_pctile` / wall-strength percentiles are not as-of in the harness
  (today's 30d bands at every past instant) — they gate strong/weak splits
  only, never a sign.

---

## 3. The strategies

### 3.1 Settlement Residual Snap — `settlement_flow_snap`

- **Axis:** 0DTE settlement mechanics (raw − smooth forced-flow decomposition).
- **Mechanism:** into expiry every 0DTE delta collapses toward a step
  function; dealers must converge their stock hedge to the settlement delta
  by the close regardless of price. `close_charm_flow` (raw) includes that
  same-day resolution to intrinsic; `close_charm_flow_smooth` is drift only.
  **D = raw − smooth is the pure settlement-unwind leg** — non-zero precisely
  when big near-ATM 0DTE strikes are resolving, and computable only with a
  full-chain exact reprice (nobody publishes it; TW itself used to collapse
  the pair).
- **Entry (14:30–15:30 ET):** both fields present · `|D| ≥ 0.25·local_gex`
  (the flow rivals the gamma-hedge flow of a ~25 bp move — relative, so it
  means the same for SPY and SPX) · `|D| ≥ 1.5·|smooth|` (the settlement leg
  must DOMINATE the drift `charm_close_magnet` trades — the two bots' firing
  sets stay nearly disjoint) · abstain if fresh aggressor flow > $2M opposes
  D · bias-veto.
- **Structure:** 0DTE debit vertical, long ATM, short 2 increments in the
  flow direction (naked late-day 0DTE debits bleed; the short leg is financed
  by elevated late-day ATM premium).
- **Exit:** target +0.30% / stop −0.20% spot · **invalidation re-reads D
  every tick** — sign flip (`flow_flip`) or decay below 0.4× the entry value
  (`flow_faded`) closes regardless of P&L · time-stop rides to ~15:52 ET (the
  flow completes INTO the close; the 15:55 cap + settlement backstop own the
  tail) · premium stop loosened to 45% (late-day spread noise).

### 3.2 Dual-Flip Dislocation — `dual_flip_dislocation`

- **Axis:** flip-convention disagreement geometry (`gamma_flip_raw`, an
  unconsumed column).
- **Mechanism:** when the un-weighted (0DTE-dominated) flip sits ABOVE the
  structural flip and spot is between them, the hedgers who rebalance fastest
  are short gamma — forced to amplify every move — while the structural longs
  who would offset them rebalance daily. Price dynamics inside the band
  belong to the fast book; the move runs to the band's far edge. Every
  competitor publishes ONE flip under ONE convention; the spread between
  conventions exists only here.
- **Entry (10:00–15:00 ET):** both flips non-null · structural flip resolved
  on the base span rung (`gamma_flip_span_used ≤ GAMMA_PROFILE_SPAN_PCT` —
  an expansion-rung flip is marginal by the engine's own definition) · band
  ≥ 0.20% of spot · correctly ordered (raw > structural) · spot inside ·
  **fresh crossing this tick** (prev close outside on the entry side) with
  ≥ 0.05% 2-bar momentum in the crossing direction — no chasing a stale band.
  Down-cross of raw ⇒ short toward the structural flip; up-cross of
  structural ⇒ long toward the raw flip.
- **Structure:** 0DTE debit vertical; **short leg at the band's far edge** —
  max payoff congruent with the mechanical exhaustion level.
- **Exit:** target = far edge · stop = buffered re-cross of the entry edge
  (fast book flipped back — no confirmation bars) · invalidation: band
  collapse < 0.10% or ordering inversion closes immediately · 75-min
  time-stop.

### 3.3 Gamma Shelf Cascade — `profile_shelf_breaker`

- **Axis:** `gex_profile` curve geometry (the dealers' full forced order
  schedule).
- **Mechanism:** G(s) near zero at spot with a cliff just below means a 25–50
  bp slide flips the hedging community into short gamma of size |G_down| —
  forced selling that grows with each tick until the trough bottom, where
  dG/dS flattens and the flow exhausts. The trough is a computed, unpublished
  level. Mirror above spot for upside cascades.
- **Entry (10:00–14:30 ET):** shelf depth `≤ −0.75·local_gex` · one-sided
  ≥ 2:1 vs the other side (a two-sided bowl is just negative gamma — other
  bots' regime) · spot on the shoulder (`|net_gex| ≤ 0.5·local_gex`) · **a
  live trigger**: 5-bar slide ≥ 0.10% toward the shelf (the geometry alone is
  a map, not a trade) · aggressor flow not opposing · bias-veto.
- **Structure:** 0DTE debit vertical spanning the forced-flow zone — long 1
  increment OTM in the cascade direction, **short at the trough bottom**
  (width-capped).
- **Exit:** target = trough price · stop = 0.15% recovery against the slide ·
  invalidation re-reads the fresh curve — shelf refilled (`> −0.4·local_gex`)
  or no longer one-sided (< 1.3:1) closes regardless of P&L · 100-min
  time-stop · ladder stays ON (real directional target).

### 3.4 Quiet-Tape Hedge Impulse — `hedge_impulse_quiet_tape` (SCREENED OUT, PF 0.32)

- **Axis:** the pending hedge OBLIGATION (per-contract delta × Lee-Ready
  split — fields no strategy ever consumed).
- **Mechanism:** dealers must neutralize inherited delta within minutes. The
  obligation in shares is Σ δ·(buys−sells)·100 over the recent window. The
  two dead flow bots followed *premium* (sentiment) *after* price moved —
  buying local extremes. This bot is their structural inverse: (1)
  **delta-weighting** deletes the lotto noise that polluted raw premium; (2)
  the **flat-tape gate** (< 0.15% move in 15 min) means the hedge has NOT
  executed yet — we are ahead of the forced stock flow, and the bot *refuses*
  precisely the entries the dead bots took; (3) **negative gamma only** — the
  ensuing hedge is amplified, not absorbed (the positive-gamma mirror belongs
  to `climax_flow_fade`); (4) **persistence** — the impulse sign must hold
  across two consecutive evaluations (prints are not campaigns).
- **Entry (10:00–15:00 ET):** `|hedge_impulse_ratio| ≥ 8%` of same-window
  tape volume (relative, self-scaling) + the four structural gates above +
  bias-veto.
- **Structure:** 0DTE debit vertical, long ATM, short 2 increments out.
- **Exit:** target ±0.40% / stop ∓0.30% · invalidation: impulse flips sign at
  ≥ 4% (`impulse_flip`), or 45 minutes elapse with the tape still flat —
  hedge internalized or crossed off-tape, the thesis had a clock
  (`impulse_expired`) · 75-min time-stop · ladder off for a clean screen.
- **Screen note:** the ablation IS the test — a variant with the flat-tape
  gate disabled should collapse toward PF 0.33; if gated and ungated perform
  identically, delta-weighting added nothing and the flow axis is confirmed
  closed.

### 3.5 Put Capitulation Credit Fade — `put_capitulation_credit_fade`

- **Axis:** per-type flow split + positioning percentile + the **vol leg**
  (first credit-exit bot).
- **Mechanism:** in a top-quartile long-gamma book the dealers' hedging is
  contractually contrarian — as spot falls they MUST buy. A put-only panic
  burst into that book is structurally mistimed: capitulators pay
  panic-inflated 0DTE put IV at the exact moment the market's largest player
  is a forced dip-buyer. Two aligned counterparties: the forced dealer bid,
  and the put buyers whose fresh longs bleed theta + their own IV spike once
  spot stabilizes. Flow-only shops see the burst but not the absorber;
  positioning shops see the gamma but not the trigger.
- **Entry (10:15–14:45 ET):** `gex_regime() == positive_strong` (percentile-
  conditioned — the absorber at FULL strength) · a real dip (30-min return ≤
  −0.35%) · put-side net buy premium ≥ 3× the session's per-15-min median
  baseline AND put share ≥ 65% of gross (one-way fear, not two-way
  repricing) · spot ≥ 0.25% above the put wall (dealers get to buy before
  the floor is even tested) · ΔVIX ≤ +2.0 (a macro vol event is not a flow
  capitulation) · trend-veto + bias-veto.
- **Structure:** 0DTE **put credit vertical** — short ~0.35% below spot
  (where panic IV is fattest), long wing 2 increments lower. Credit, not
  debit, because the edge has two legs — direction AND vol — and the credit
  collected widens exactly when the signal fires. `MIN_CREDIT_PER_SHARE`
  filters weak-vol non-events.
- **Exit:** premium-based via the new base branch — take at 55% of the
  credit, stop at 1.75× the credit given back · structural floor: spot below
  the put wall = the absorber failed (`wall_lost`), exit regardless of
  premium · everything closed ≥ 15 min before the bell (short 0DTE gamma
  into the close is not the thesis). **The wall is only the invalidation
  floor, never the trigger** — the trigger can fire 1% above it, which is
  what separates this from the three-times-dead wall-fade family.

### 3.6 Weekly Charm Grind — `weekly_charm_grind`

- **Axis:** the bucketed charm ladder (weekly vs 0DTE — unconsumed) + the
  first 1DTE candidate.
- **Mechanism:** charm is the one forced flow driven by a variable that
  advances with certainty. The weekly (1–7 DTE) book's deltas decay intraday
  whether or not price moves, and desks rebalance to the decayed deltas on
  their normal schedule. In a compressed positive-gamma midday, gamma-hedge
  flow is ~zero, so the weekly charm rebalance is the DOMINANT forced flow —
  a slow one-way grind. Computing it needs full-chain OI-weighted FD charm
  split by expiration, which ZeroGEX persists and nothing consumes.
- **Entry (11:00–14:00 ET):** positive-gamma regime · session range so far ≤
  0.6% (no trend already consuming the flow) · **bucket dominance**
  `|charm_weekly| ≥ 2·|charm_0dte|` (keeps it strictly out of the EOD bots'
  territory) · remaining-window flow ≥ 0.10·local_gex (relative) ·
  bias-veto. Direction = sign of the weekly dealer charm.
- **Structure:** **1DTE** debit vertical (`dte_target=1`) — the edge is a
  25–35 bp grind over 2–3 hours and 0DTE ATM theta eats exactly that;
  1DTE roughly halves the theta at similar net delta. Always closed same-day.
- **Exit:** target +0.35% / stop −0.25% · hard exit ~15:00 ET (before the
  EOD charm regime opens; never overnight) · invalidation: weekly charm sign
  flip (`charm_flip`) or the 0DTE bucket catching up (< 1.2× —
  `bucket_mixed`) · ladder off.

---

## 4. Differentiation matrix

| v5 bot | Untapped axis | Nearest prior | Why it is not that |
|---|---|---|---|
| Settlement Residual Snap | raw−smooth forced-flow decomposition | charm_close_magnet | trades the ORTHOGONAL component, requires it to dominate the drift; no magnet/level involved |
| Dual-Flip Dislocation | flip-convention spread (`gamma_flip_raw`) | gamma_flip_breaker (retired), gamma_regime_shift_rider | no single level, no realized net-GEX transition — the disagreement GEOMETRY plus a fresh confirmed crossing |
| Gamma Shelf Cascade | `gex_profile` curve shape | gamma_flip_* (retired) | never references the flip; pre-transition geometry + live slide trigger + computed exhaustion target |
| Quiet-Tape Hedge Impulse | δ-weighted hedge obligation | aggressor_flow_divergence / fresh_flow_momentum (both dead) | obligation not sentiment; REFUSES moved tapes (the dead bots' entry state); negative-γ only; ablation planned |
| Put Capitulation Credit Fade | per-type split + percentile + vol leg | climax_flow_fade (screening); wall fades (dead) | put-only burst + full-strength absorber condition; wall is floor not trigger; first credit-exit structure |
| Weekly Charm Grind | bucketed charm ladder, 1DTE tier | charm_close_magnet | different field family, different clock (out before 15:00), drift not magnet-reversion, 0DTE bucket must be SMALL |

Six bots, six distinct axes; the set is non-overlapping with each other and
with everything retired, screened out, live, or screening.

## 5. Backlog (specced, deliberately not built)

Kept small so each addition screens on its own — the v4 discipline:

- **Wall Erosion Breaker** — aggressors consuming the wall strike in real
  time (strike-filtered signed flow ≥ ~12% of that strike's OI, accelerating,
  while spot presses the wall) → trade the wall FAILING, the inverse of the
  dead fade family, on evidence competitors cannot see until tomorrow's OI
  print. Deferred: the wall axis carries three documented failures, so the
  prior is low; needs a per-strike flow fetcher + OI join; revisit after the
  v5 screens.
- **EOD 0DTE Inventory Accelerant** — negative-gamma afternoons where fresh
  0DTE aggressor flow agrees in sign with `close_charm_flow`: the reactive
  (gamma) and deterministic (charm) dealer legs stack with a hard 16:00
  deadline. The explicit negative-γ complement of `charm_close_magnet`.
  Deferred: negative-gamma afternoons are a minority of sessions — likely
  "insufficient" on a 45–60d screen; hold until history accumulates (same
  posture as the VIX-gated vanna bot).
- **Pinned-Book Theta Harvest** — midday 0DTE iron condor when a
  top-quartile positive-γ book + high `local_gex` density suppresses realized
  vol below what the morning priced. Deferred: needs an intraday IV-vs-
  realized field to be a mechanism rather than folklore, and the retired
  `range_iron_condor` (location-based) is a cautionary neighbor.

---

## 6. Promotion gate — unchanged, nothing goes live on a thesis

All six are in `STRATEGY_CLASSES` and `CANDIDATE_SPECS`; **`DEFAULT_ROSTER`
stays empty**. A candidate is promoted only after clearing the same gate as
always — **PF ≥ 1.1, positive expectancy, ≥ 20 trades**:

```
make tradeworkz-backtest ARGS="--days 60 --interval-min 5 --bots settlement_flow_snap,dual_flip_dislocation,profile_shelf_breaker,put_capitulation_credit_fade,weekly_charm_grind --json"
```

(The original six-bot command included `hedge_impulse_quiet_tape`; it was
screened out on the first run — see §7.)

Screen order of operations (the v4 lessons, proceduralized):

1. **Probe before screening** — `python -m src.tools.tw_edge_field_probe
   --days 60` now reports coverage for every new column (residual pair,
   `gamma_flip_raw`, `expiration_bucket`, `flow_contract_facts.delta`,
   `gex_profile`) and the new snapshot fields. A 0-trade result must name its
   gate via `miss_reasons`, never be guessed at.
2. **Calibrate gates from the probe's own distributions** (the v4 flip-gate
   lesson: thresholds that never fire are indistinguishable from edge until
   instrumented). Every v5 magnitude gate is RELATIVE (`local_gex` fractions,
   tape-volume ratios, session baselines) so SPY/SPX mean the same thing.
3. **Sweep executions, judged out-of-sample** — `tw_execution_sweep` grids
   (width / window / thresholds / take-stop) with the train/test split;
   `robust` requires positive expectancy in BOTH halves. The
   hedge-impulse **ablation** (flat-tape gate off) is part of its screen: if
   the ablated variant performs the same, the axis is closed, not tuned.
4. **Honest verdicts** — regime-gated bots (put-capitulation needs
   positive_strong sessions; the shelf needs shoulder days) may return
   "insufficient" on 45–60d. That is a sample-size statement, not a defect;
   they hold as candidates and validate forward, exactly like the vanna bot.

Frequency expectations (to be verified by the probe, not assumed): each bot
is designed to fire ~1–4×/week across SPY/QQQ/SPX so the 20-trade bar is
reachable within the hot window; if the funnel shows a gate absorbing
everything, the sweep — not an ad-hoc loosening — decides.

---

## 7. First screen — results & calibration (2026-08-15, 60d / 5m / 1 contract)

The first `make tradeworkz-backtest` run (2026-06-16 → 2026-08-15, 3,318
steps × SPY/QQQ/SPX, full chain coverage) produced real verdicts on day one:

| Bot | Trades | PF | Expectancy | Verdict |
|---|---:|---:|---:|---|
| profile_shelf_breaker | 5 | **2.28** | **+$89.99** | insufficient (best early read in the fleet) |
| hedge_impulse_quiet_tape | 383 | **0.32** | −$11.66 | **no-edge → SCREENED OUT** |
| settlement_flow_snap | 1 | 0.00 | −$29.16 | insufficient (magnitude gate mis-calibrated) |
| dual_flip_dislocation | 2 | 0.37 | −$74.38 | insufficient (fresh-cross gate vs replay cadence) |
| put_capitulation_credit_fade | 0 | — | — | insufficient (**bug**: could never fire) |
| weekly_charm_grind | 0 | — | — | insufficient (gate intersection near-empty) |

**The flow-direction axis is now closed — four formulations, one verdict.**
`hedge_impulse_quiet_tape` was the strongest read that axis will ever get:
delta-weighted OBLIGATION instead of premium sentiment, a flat-tape gate that
entered *ahead* of the hedge instead of chasing it, negative-gamma
amplification, and sign persistence. It returned PF 0.32 at a 22% win rate
over 383 trades — statistically indistinguishable from
`aggressor_flow_divergence` (0.31) and `fresh_flow_momentum` (0.33). The
designed ablation is thereby answered in the strongest possible way: the
failure was never the *formulation* of flow-following; recent flow direction
simply does not predict 0DTE price direction on these underlyings at this
cadence. Moved to a standalone screened-out spec (out of `CANDIDATE_SPECS`,
kept backtestable). Do not build a fifth variant; the only open read of this
data is the contrarian one (`climax_flow_fade`, still screening).

**Gamma Shelf Cascade is the one to watch.** PF 2.28, +$450, 40% win rate
with average wins 3.4× average losses — exactly the profile the mechanism
predicts (capped losses at the stop, cascades that run to the trough). But 5
trades is a read, not a verdict, and the funnel shows the geometry gates
(`no_shelf` 3,495 / `not_on_shoulder` 3,329) throttle it below the 20-trade
bar on a window that cannot grow (chain history starts 2026-06-15). Depth and
shoulder gates were widened one notch (0.75→0.60 / 0.5→0.60 of local_gex);
the defining mechanism gates (2:1 one-sidedness, the live slide trigger) are
untouched. This is a recalibrated hypothesis screened from scratch — the 2.28
carries no validation weight for the new config.

**Fixed from the funnel (every change names its miss counter):**

- `put_capitulation_credit_fade` — **implementation bug**: `no_history` on
  all 1,300 regime-passing ticks. The 30-bar displacement window needed 31
  closes; `build_snapshot` carries at most 30. `displacement_bars` → 25. The
  strategy was never actually tested; this screen tells us nothing about it.
- `dual_flip_dislocation` — **cadence bug**: the fresh-cross test compared
  the last 1-MINUTE close, but the engine/replay evaluate every ~5 minutes,
  so a cross older than 60s never registered (valid bands on ~1,500 ticks,
  2 entries). The cross is now detected over the last 6 one-minute closes —
  a cadence-compatibility fix, not a loosening.
- `settlement_flow_snap` — the `0.25 × local_gex` magnitude prior blocked
  1,607 in-window ticks and admitted one trade; the residual is real but
  smaller relative to local gamma than assumed. Floor → `0.10`; the
  dominance-over-drift gate (the qualitative differentiator vs
  `charm_close_magnet`) is unchanged.
- `weekly_charm_grind` — only 3 ticks in 60d survived the gate stack, and
  all 3 then died at `conviction` because the quality scale saturated far
  above the entry floor. Gates relaxed one notch each (range ≤ 0.8%,
  dominance ≥ 1.5×, flow ≥ 0.08 × local_gex) and the quality scale matched
  to the floor so a gate-passing setup can pass conviction.

This mirrors the v4 sequence exactly (first screen → probe/funnel →
calibration → re-screen): the counters, not intuition, chose every change.

### 7.1 Second screen (2026-08-17, 60d / 5m / 1 contract)

The recalibrated five-bot fleet flipped to net-positive: **25 trades,
+$96.40, expectancy +$3.86** (vs 391 trades / −$4,194 with the flow bot in).

| Bot | Trades | PF | Expectancy | Read |
|---|---:|---:|---:|---|
| profile_shelf_breaker | 17 | **1.27** | **+$17.62** | insufficient — **3 trades short of the bar**, barbell intact (avg win $356 vs avg loss $87) |
| weekly_charm_grind | 3 | — (3W/0L) | +$12.32 | insufficient — the drift is real but small ($12/trade at time-stop); accumulating |
| settlement_flow_snap | 3 | 0.00 | −$30.46 | insufficient — all 3 churned out in ~8 min by the fade exit (design flaw, below) |
| dual_flip_dislocation | 2 | 0.37 | −$74.38 | insufficient — the amplifying ordering is structurally rare (~20% of ticks); 3 of 6 fresh crosses died at conviction |
| put_capitulation_credit_fade | 0 | — | — | insufficient — bug fixed, but a structural tension surfaced (below) |

**Shelf: hold the line.** PF 1.27 with positive expectancy across 17 trades,
and the win/loss shape the mechanism predicts. No knob is touched this round
— it crosses the 20-trade bar on data accumulation alone within ~2 weeks, and
its verdict should come from the config as-is.

**Mechanism-level fixes from this screen (not knob tweaks):**

- `settlement_flow_snap` — **the fade exit was wrong on its own mechanism.**
  `close_charm_flow` measures the flow REMAINING by the close, so it
  mechanically shrinks as dealers execute it: |D| decaying is the trade
  *working*, and the `flow_faded` exit closed every position within ~8
  minutes for it. Removed; invalidation is now sign-flip only, and the spot
  stop / target / near-bell time-stop own the exits.
- `put_capitulation_credit_fade` — **the dip gate fought the regime gate.**
  With `no_history` fixed, 958 of 962 regime-passing ticks died at `no_dip`:
  the strong positive-gamma book this bot requires *suppresses* fixed-0.35%
  dips, so trigger and absorber could ~never co-occur. The dip is now
  vol-relative (`2σ·√bars` of the tape's own realized 1-min sigma, floored
  at 0.20%) — "a real dip" measured against the pinned tape it happens in.
- `settlement` + `dual_flip` — quality scales matched to entry floors (the
  same disease weekly_charm_grind had on screen one: setups clearing every
  hard gate then dying at conviction because the quality scale saturated far
  above the floors — 13 and 3 conviction deaths respectively).

Re-screen with the same five-bot command:

```
make tradeworkz-backtest ARGS="--days 60 --interval-min 5 --bots settlement_flow_snap,dual_flip_dislocation,profile_shelf_breaker,put_capitulation_credit_fade,weekly_charm_grind --json"
```

Promotion criteria unchanged: PF ≥ 1.1, positive expectancy, ≥ 20 trades.
Frequency reality after two screens: shelf reaches the bar soon;
settlement / dual-flip / weekly / put-capitulation are low-frequency,
regime-conditional setups that validate on accumulation (the
vanna_vol_crush_rider posture) — the next lever for any of them is a
`tw_execution_sweep` grid with the train/test split, not further ad-hoc
loosening.

---

## 8. What shipped with this doc

- `src/tradeworkz/flow_context.py` — raw/smooth forced-flow split + 4 new
  as-of, savepoint-isolated fetchers (bucketed second-order, hedge impulse,
  put-panic window, profile geometry).
- `src/tradeworkz/context.py` — 18 new best-effort `MarketSnapshot` fields +
  `settlement_flow_residual()` / `hedge_impulse_ratio()` helpers; the
  `gex_summary` read now carries `gamma_flip_raw` / `gamma_flip_span_used`
  (and pads short rows defensively — this also fixed a latent
  fixture-drift test failure).
- `src/tradeworkz/bots/base.py` — opt-in credit premium exits
  (`credit_take_frac` / `credit_stop_mult`), the branch the debit premium
  stop's comment had deferred.
- Six bot implementations under `src/tradeworkz/bots/`, registered in
  `registry.py` as candidates.
- `src/tools/tw_edge_field_probe.py` — coverage for every new column and
  snapshot field.
- Tests: `tests/test_tw_v5_bots_open.py` (46 cases: fire/direction/structure,
  every edge-filter abstention, custom exit invalidations, the credit
  take/stop branch, registry invariants) plus new fetcher cases in
  `tests/test_tw_flow_context.py`. Full tradeworkz suite: 429 passing.
