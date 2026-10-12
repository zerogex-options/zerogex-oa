# TradeWorkz v6 — Trade the Magnitude

**Status:** candidates (registered + backtestable; **not live**). First screens
not yet run — see §7 for the order to run them in.
**Date:** 2026-10-11
**Extends:** `tradeworkz-edge-strategies.md` (v4), `tradeworkz-v5-strategies.md`
(v5), `strategy-catalog.md`.

---

## 0. The honest summary

- Twenty-eight directional bots died on a signal that measures **how far**,
  not **which way**. The structure paid for "less travel than the market
  priced" is a short option structure. That is the v6 thesis, and the one
  bot built on it (§3.1) is a defined-risk 0DTE ATM iron butterfly.
- The published 0DTE variance risk premium is the only part of that thesis
  with a replicated prior. The gamma **gate** on top of it has a weaker prior
  than the designers assumed: the only clock-and-tape-adjusted gamma figures
  on this platform are short gamma 1.08x and the "neither" zone 0.92x; long
  gamma was never reported and is implied to be about 1.0x. So the bot ships
  with its tape-only twin as the **primary comparison**, not a control.
- Eighty sessions cannot distinguish a profit factor of 1.1 from 1.0 on ~35
  gated trades. The first screen can return `insufficient` or `no-edge` on
  the gamma conditioning; it cannot return a defensible `edge`. Run the
  option-free measurement in the pre-screen (§7 step 1, section G) first.
- One directional exception (§3.2) with a published mechanism and a fixed
  clock, untested here, and the cheapest action of all: a clean re-screen of
  the one VALIDATED strategy (§3.3).

## 1. Why every earlier bot was the wrong shape

Twenty-eight bots have shipped since the fleet began. Every one of them is a
**directional** bet: a debit single or vertical that needs spot to go
somewhere. Two neutral bots existed and both were location-gated and failed
(`range_iron_condor` PF 0.67 at the walls, `vol_expansion_straddle` PF 0.44
buying vol in compressed sessions).

Over the same period five independent research efforts on this platform's own
data converged on one finding, and it is not a directional one:

| Study | What it asked | What it found |
|---|---|---|
| `docs/design/msi-regime-excursion.md` | Do the MSI regime bands predict realized excursion? | Yes, monotonically, on every instrument (15m forward range vs base: SPY 0.81/0.97/1.04/1.09 across the four bands); 1,386 of 4,140 tests survive BH. A **magnitude-only** rebuild of the MSI beats the shipped one on 30 of 30 cells; the directional components carry zero excursion information. **These ratios are against the unconditional base rate, not clock- or tape-adjusted.** |
| `research/trade_bias_movement` | Do the Trade Bias states say how far price moves? | Nothing detectable for the states. Production's short-gamma definition travels **1.08x [1.03, 1.13]** after adjusting for clock and tape; the "neither" zone 0.92x [0.88, 0.97]. **Long gamma was not reported; the adjusted ratios average to 1.0 across the zones by construction, so long gamma is implied near 1.0x after adjustment.** |
| `research/short_gamma_trend` | Does short gamma + one-sided flow continue? | No (leans to reversal). Those minutes DO move 1.29x as far. |
| `research/trade_bias_inputs` | Does any of the nine inputs predict 30m direction? | None, Holm-corrected. |
| `research/wall_break_odds` | What predicts a wall break? | Nothing replicates across symbols. Break odds are a per-index property (S&P ~31%, Nasdaq ~50% at 60m). |

**Dealer gamma predicts how far, not which way.** Twenty-eight bots tried to
extract direction from a magnitude signal. The structure that is paid for
"less travel than the market priced" is a short option structure, and the
most replicated anomaly in index options — the variance risk premium, and
specifically the 0DTE version (Beckmeyer, Branger & Gayda 2023; Bandi, Fusari
& Reno 2023) — says the market systematically overprices that travel. The
edge-defining question is only **when not to sell**.

What the record does **not** establish is that long gamma, after the clock
and the tape are accounted for, compresses the day below what a quiet tape
alone would. The short-gamma side is confirmed at 1.08x; the long-gamma side
is inferred. That is why the v6 thesis is stated as an experiment:

> **Sell the 0DTE move, defined-risk, when the tape is realizing less than the
> straddle charges — and run the gamma-gated version against the tape-only
> twin on the same sessions, so the gate has to earn its place or be
> retired.**

Plus one directional exception with a published mechanism and a fixed clock
that no study here has tested (§3.2).

## 2. How these were designed, and what did not run

Five independent designers (a variance trader, a dealer-hedging quant, an
empirical-finance academic, the repository's own post-mortem engineer, and an
options execution specialist) each read the full research record and
proposed strategies from their own lens: fifteen proposals, ten distinct
candidates after clustering. Four of the five independently proposed the
core trade (§3.1); two proposed the directional exception (§3.2); two
proposed an overnight 1DTE condor (§9).

The plan was for three adversarial reviewers (an archivist looking for dead
rows in disguise, a market maker pricing the friction and the tail, and a
statistician checking testability and look-ahead) to attack every candidate,
then a synthesis. **The review run hit the session limit after four of the
thirty verdicts**: all three reviewers on the core fly, and the archivist on
the overnight condor. The other eight candidates — the close-momentum
exception included — were **not** reviewed, and the synthesis step did not
run; the author did the synthesis from the four verdicts and the brief. The
four verdicts changed the fly materially before its first screen (the list
in §3.1), which is the strongest argument for finishing the other
twenty-six before anything here is believed.

## 3. The strategies

Both are registered in `STRATEGY_CLASSES`, carry a `RESEARCH` catalog entry,
and are screenable with `make tradeworkz-backtest`. `DEFAULT_ROSTER` stays
empty.

### 3.1 Long-Gamma Iron Butterfly — `long_gamma_iron_fly`

- **Who is forced, and when:** dealers long gamma must sell every rally and
  buy every dip, compressing realized range; 0DTE buyers pay for the move
  regardless. The bot sells to the second only while the first is at full
  strength.
- **Structure:** 0DTE ATM iron butterfly (`build_iron_condor` with equal
  short strikes), wings 1.25 sigma out. **Sigma is the market's own**: the
  ATM front straddle's mid divided by 0.798 (a straddle is worth ~E|Z|
  sigma), so wing width, flip clearance and exits all scale with what the
  market is pricing for the rest of the day, on SPY and SPX alike.
- **Entry (10:30–13:30 ET), every gate a named `_skip`:** `window` ·
  `no_straddle` / `no_sigma` · `regime` — `positive_strong` (top quartile of
  the symbol's own 30-day history; **fails closed** on a missing percentile,
  never the absolute fallback) · `tail_realized` — the trailing 30-bar
  realized move, projected over the rest of the day on the cone model's
  diurnal profile, at most 0.85 of the straddle's own remaining move
  (**dollars against dollars**, see below) · `forecast_expansion` /
  `range_spent` — the morning forecast's own expansion call and today's
  cash range vs its implied move, when a forecast exists · `flip_near` —
  neither flip convention within a sigma of the strike · `vol_shock` (30m
  ΔVIX > 5% of level) · `event_day` · `conviction`. Two more gates exist but
  are **off by default**: `magnitude_band` (the magnitude-only MSI in chop or
  reversal; collinear with the regime gate and built on unadjusted ratios, so
  it is a sweep axis, `gated/band`) and `iv_elevated` (the live ATM IV and
  its 60-day history come from different computations and do not rank on one
  scale; carried as a diagnostic until reconciled).
- **Exits:** `credit_take` at 50% of the credit, `credit_stop` at **0.5×** the
  credit (base-class credit branch), plus invalidations that close regardless
  of P&L — `regime_lost` (net gamma turns negative; only when the regime gate
  is on), `flip_entered` (a flip within half a sigma of the strike),
  `short_touched`, `vol_shock` — and a time stop 20 minutes before the bell.
  Never holds the short straddle into the settlement window by default; the
  `gated/hold_to_settle` arm does.
- **What the review changed before the first screen:**
  - *Convention.* The first draft compared a realized vol annualized on RTH
    minutes (sqrt(252·390)) with an ATM IV annualized on calendar seconds
    (`market_calendar.calculate_time_to_expiration`). For a fairly priced
    0DTE straddle that ratio reads about 0.43, so a 0.85 cap would have
    admitted a tape realizing twice what the straddle charged. The gate is
    now `realized_over_straddle`: the trailing variance projected over the
    rest of the day by `intraday_cone_model.variance_fraction`, against the
    straddle's remaining 1-sigma move. The annualized form is kept on the
    snapshot and in the components as a diagnostic only.
  - *Stop.* At a 1.25-sigma wing the fly's maximum loss is roughly 0.8–0.9×
    the credit, so the 1.0× stop could never fire. It is 0.5× now, with a
    `gated/no_stop` arm where the wing is the only stop.
  - *Gates off.* Band and IV percentile, as above.
  - *Controls can actually drop their gates.* Params switch a gate off with a
    falsy value (`0`, `""`, `[]`); `None` means "engine default". The first
    draft read `None` as the default, so the twins silently kept the gates
    they were meant to drop.
- **Why not a dead row:** `range_iron_condor` sold at the walls (where a
  tested wall breaks 31–50% of the time), on absolute VIX, any positive
  gamma, a flat 180-minute hold and no credit exit. This bot never reads a
  wall, centers on the money where credit per unit of wing is largest, gates
  on the symbol-relative percentile and a measured realized-under-straddle
  condition, and books or cuts on the credit. The archivist agreed it is not
  a dead row in disguise — and added that the long-gamma evidence is weaker
  than the brief implied (§1).
- **Controls (params, not bots), all in `tw_execution_sweep`:**
  `twin/tail_only` (`required_regimes: "any"`: the quiet tape plus the
  variance premium, no gamma — **the primary comparison**), `twin/clock_only`
  (tail gate off as well: pure 0DTE variance premium at this clock),
  `twin/negative_gamma` (the complement the mechanism says should NOT work),
  `gated/band` (the magnitude band switched on), `gated/uniform_gates` (the
  VIX, forecast and cash-range stand-downs off, so both halves of the
  train/test split run one gate set whatever the input coverage), and the
  geometry arms (wing 1.0 / 1.5 sigma, take at 35%, flat at 15:05, hold to
  settlement, 1.0-sigma condor, no credit stop).
- **How to read it:** the twins trade a superset of the gated bot's sessions.
  The statistic is the **paired** comparison — the gated bot's result on its
  sessions against the tape-only twin's result on those same sessions —
  not two profit factors side by side. If the twin matches the gated bot
  within 0.1 PF on the paired sessions, the gamma conditioning adds nothing
  and the catalog says so. `twin/negative_gamma` beating `gated/default`
  retires the mechanism outright.
- **Power.** Roughly 35 gated trades in 80 sessions with a ~70% win rate and
  losers near 2× winners give a PF sampling error of order 0.5. The screen
  cannot distinguish PF 1.1 from 1.0 on the gated arm; it can only say
  `insufficient`, or `no-edge` if the arm is plainly negative. The pre-screen's
  section G (straddle charged vs tape paid, by gamma sign, ~60 sessions per
  slot per symbol) is the better-powered test and needs no option fill model.
- **Prior:** the unconditional 0DTE straddle sale at bid, after the wings and
  the harness's 2% slippage on four legs, is near breakeven on the
  published premium; whatever edge exists must come from the gates. Honest
  prior for the gated arm: PF 1.0–1.2, with the failure mode being three to
  five afternoon trend days in 80 sessions landing in one half of the split.

### 3.2 Hedging-Demand Close Momentum — `hedging_demand_close_momentum`

**Not reviewed by the adversarial panel** (§2). The design stands on the two
papers and the brief.

- **Published mechanism:** Gao, Han, Li & Zhou (2018, JFE): the day's return
  predicts the last half hour in the same direction. Baltussen, Da, Lammers &
  Martens (2021, JFE): the effect lives where two forced traders rebalance
  WITH the move into the bell — options market makers who are net **short
  gamma**, and leveraged ETFs. ZeroGEX measures the hedgers' sign directly.
- **Why not a dead row:** `research/short_gamma_trend` tested ANY 30-minute
  one-sided-flow continuation at any hour; the follow-the-flow bots read
  aggressor option flow; the Trade Bias inputs were per-minute reads. None is
  the fixed clock-to-clock structure the papers document, which the research
  brief records as untested here.
- **Entry (15:00–15:20 ET):** `regime` — net gamma negative (`required_gamma_sign`
  is a param: `"any"` is the unconditional Gao trade, `"positive"` the
  complement the mechanism says should NOT work) · `day_flat` /
  `day_exhausted` — the cash-session return at least 0.5 and at most 3.0
  implied daily sigmas (the morning forecast's implied move when present, else
  VIX-implied) · optional first-half-hour agreement · `conviction`. The bias
  veto is **off** for this bot: the fused bias measured as noise on nine
  inputs and would muddy the screen.
- **Structure:** 0DTE ATM debit vertical in the day's direction, ~0.5 sigma
  wide (`structure: "credit_vertical"` sells the ATM option the move is
  leaving behind instead). Held to 15:55 — the published object IS the
  last-half-hour return — with `momentum_lost` (a cross back through the cash
  open) and a 60% premium stop as the only early exits.
- **Prior:** honest read is `insufficient`. The effect is real but small
  (single-digit bps on average); the gamma conditioning multiplies it. The
  cash-tape pre-test in §7 step 1 (section E) answers most of this before an
  option is priced.

### 3.3 The cheapest action: re-screen `gex_gradient_trend` properly

The one VALIDATED strategy (PF 2.80 on 33 QQQ trades, pattern side) has never
had a clean bot screen. The 2026-09-14 attempt found Gate 1 unreachable under
long gamma by arithmetic: the component damps its reading by 0.40 and the
threshold is 40 on a ×100 scale. The bot now (a) splits that miss into
`gradient_weak_long` / `gradient_weak_short` so the funnel shows which side
starved, and (b) carries `undamp_long_gamma` (default **false**) which
compares a long-gamma reading at the same effective asymmetry. The fidelity
run keeps the pattern's exact gate; the undamped run is a separate hypothesis
about the positive-gamma side.

**A caveat the skeptic designer raised that must be checked before the 2.80
is trusted:** the recorded expectancy of $2,411 per trade cannot be one QQQ
5-DTE OTM contract. Confirm the playbook-calibration harness's sizing and
units before comparing its PF to a 1-contract `tradeworkz-backtest` figure.

## 4. The data layer (what was added)

All additive, best-effort, as-of bounded and savepoint-isolated, following the
v4/v5 fetcher contract. Every existing bot ignores them.

| Snapshot field | Source | Fetcher | Note |
|---|---|---|---|
| `atm_strike_front`, `atm_straddle_mid_front`, `atm_straddle_bid_front`, `atm_iv_front` | `option_chains` → `option_chains_archive` fallback, front (same-day) expiration, nearest two-sided strike, 10-minute lookback | `fetch_atm_front_quote` | The implied side of every v6 gate. Mid is the market's price for the remaining move; bid is what a seller keeps before slippage. |
| `forecast_implied_move`, `forecast_expected_vol_ratio`, `forecast_expected_vol_state`, `forecast_vix_z`, `forecast_is_event_day` | `daily_forecast` row for the session date with `open_ts <= as_of` | `fetch_daily_forecast` | Immutable once committed at 08:30 ET; a replayed 08:00 tick does not see it. The writer covers SPY, QQQ, SPX and NDX in production (pre-screen section B: ~60 graded days each); a symbol without a forecast reads None and its forecast gates stand aside. |
| `atm_iv_pctile_60d` | `daily_atm_iv`, trading dates strictly before today | `fetch_atm_iv_history` + `_percentile_rank` | Today's row is rewritten all day, so only prior (frozen) days are ranked against. Needs 20 observations. **Diagnostic only until the live chain IV and the daily table's IV are shown to be on one convention.** |
| `cash_open`, `cash_high`, `cash_low`, `first_half_hour_close` | `underlying_quotes` from the 09:30 ET cash open (09:31 for a cash index) | `fetch_cash_session_bars` | The existing `session_*` fields span the trailing 24 hours and include the pre-market. |

Helpers on `MarketSnapshot`:

- `magnitude_score()` / `magnitude_band()` — the magnitude-only MSI. Prefers
  the engine's published `__aggregation__.magnitude_score`, which only began
  persisting in September 2026, and otherwise rebuilds it from the four
  persisted component scores with the engine's own renormalize-then-`tanh`
  arithmetic, so a replay over the whole archive reads the same number.
- `straddle_implied_sigma()` — the straddle mid over sqrt(2/π): the 1-sigma
  dollar move the market prices for the rest of the front expiration.
- `realized_remaining_move()` — the trailing one-minute variance scaled by
  the cone model's diurnal variance profile to the rest of the day, in
  dollars. `realized_over_straddle()` divides the two: **the gate's ratio,
  convention-free.**
- `realized_sigma_1min()`, `realized_over_implied()` — the annualized form.
  `realized_over_implied` mixes an RTH-minute and a calendar-second
  convention and reads about 0.43 for a fairly priced straddle; it is on the
  snapshot and in the trade components as a diagnostic, not a gate.
- `expected_remaining_move()` — the forecast's implied move on the same
  diurnal profile; `cash_range_over_implied()`, `day_return()`,
  `first_half_hour_return()`.

`src/tools/tw_edge_field_probe.py` reports coverage for the two new tables and
all fourteen new fields. Run it before trusting any screen.

## 5. The shared engine

`src/tradeworkz/bots/short_premium.py` — `ShortPremiumBot`. Every v6 short
structure is a thin subclass: it sets catalog params and may override
`extra_gates`, `center_strike` and `quality`. The engine owns the window, the
implied-side presence check, the regime / band / tail / forecast / flip /
VIX / IV / event gates, the sigma source fallback (straddle → forecast;
realized vol is opt-in), the structure geometry (fly or condor, wings floored
at one increment, snapped to the symbol grid), the conviction scale
(calibrated so a top-quartile regime with realized at a third of the straddle
clears the 0.50 floor, and a boundary regime with realized near the cap does
not — the v5 lesson about zero-trade screens dying at conviction), and the
invalidation exits. One module to screen, one set of miss labels across the
family, one sweep grid. The param convention is the one the sweep depends
on: a falsy value switches a gate off, `None` means the engine default.

## 6. What the harness measures, and what it does not

- Four 0DTE legs each pay bid/ask plus 2% slippage twice. On an ATM SPY fly
  that is roughly 8–12% of the credit; on a 1-sigma condor roughly 40%. The
  `credit_take_frac` of 0.5 is set so a "win" survives it.
- `MIN_CREDIT_PER_SHARE` (0.10) applies at the fill. A SPY condor one sigma
  out at lunch may not clear it; the fly always does. `entry_rejects:
  below_min_credit` in the report is the tell.
- The harness models slippage but **not commissions** (~$2.60 round trip on
  a 2-leg vertical, roughly double on four legs). A PF just over 1.1 on
  thin per-trade expectancy is breakeven in practice.
- **The regime percentile is a look-ahead for this bot.** `net_gex_pctile` is
  today's 30-day time-of-day band applied at every past instant (disclosed in
  `backtest.py`). For a directional bot that only flips a sign it was
  harmless; for a gate that selects the *top quartile* it means the replay
  knows the window's gamma distribution in advance. Read the gated-vs-twin
  comparison as "conditioned on the end-of-window ranking" until the
  percentile is made as-of (§9). The gamma **sign**, which section G of the
  pre-screen and `twin/negative_gamma` use, has no such problem.
- Several stand-downs (`vol_shock`, the forecast gates, `range_spent`) depend
  on inputs whose coverage differs across the window; a train/test split can
  then compare two different gate sets. `gated/uniform_gates` runs the same
  set on both halves.
- The dead rows this family must not become: a short premium structure
  that works only because the window happened to be quiet. That is why the
  twins run in the same screen on the same sessions, and why the pre-screen
  reports the straddle charged against the move paid by gamma sign before any
  option is priced.

## 7. What to run first, in order

Cheapest and most informative first. Steps 1–3 need no new code paths beyond
this branch and run in seconds to a minute.

1. **`make tradeworkz-v6-prescreen`** (read-only SQL,
   `setup/database/diagnostics/tradeworkz_v6_prescreen.sql`). It answers:
   - whether the morning forecast's `expected_vol_ratio` carries skill
     (Spearman vs the realized receipt, per symbol) — if rho is ~0 the
     forecast gates are noise and only the raw-VRP control is worth running;
   - how often positive gamma holds midday and negative gamma at 15:00 (the
     two regime base rates the bots' frequency estimates rest on);
   - **the raw last-hour momentum test** (section E): on sessions with a
     ≥0.5-sigma day return, how often 15:00→15:55 continues, split by gamma
     sign at 15:00. If the short-gamma row is not clearly above 50%, §3.2
     cannot be rescued by an option structure;
   - whether positive gamma compresses the realized day on this tape
     (section F: realized range / normal range, split by gamma sign at noon);
   - **the fly's P&L before any bot runs** (section G): at 10:30, 12:00 and
     13:30 ET, the 0DTE ATM straddle mid and bid against |px_1555 − strike|,
     split by gamma sign before entry. The mean paid/charged ratio is 1.0
     under fair pricing and near 0.85–0.90 on the published premium. **The
     gamma gate earns its place only if the long-gamma row sits ≥0.10 below
     the all-sessions row at the same slot.** `seller_bps_of_spot` negative
     on a row means no exit logic rescues it; `beyond_1p25_sigma_wing_pct` is
     how often the fly would have reached its wing.
2. **`python -m src.tools.tw_edge_field_probe --days 90`** — coverage for
   `daily_forecast`, `daily_atm_iv` and the fourteen new snapshot fields. A
   field at 0% non-null is the reason a bot will abstain, found before the
   hour-long screen.
3. **The gradient re-screen** (no new code): both variants over the
   pattern's own window, QQQ included.
   ```
   make tradeworkz-backtest ARGS="--days 90 --interval-min 5 --bots gex_gradient_trend --json"
   ```
   Then once more with `undamp_long_gamma: true` via `tw_execution_sweep`
   (or a temporary param override). Read `miss_reasons` for
   `gradient_weak_long` vs `gradient_weak_short`.
4. **The v6 screen**, gated bots and their twins in one pass:
   ```
   make tradeworkz-backtest ARGS="--days 90 --interval-min 5 --bots long_gamma_iron_fly,hedging_demand_close_momentum --json"
   ```
   and the controls through the execution sweep, which picks its grid from
   the bot's class and scores every config on a train/test split:
   ```
   python -m src.tools.tw_execution_sweep --bot long_gamma_iron_fly --days 90
   python -m src.tools.tw_execution_sweep --bot hedging_demand_close_momentum --days 90
   ```
   The fly's grid is `gated/default` against `twin/tail_only` (the primary
   comparison), `twin/clock_only`, `twin/negative_gamma`, `gated/band`,
   `gated/uniform_gates` and the geometry arms; the momentum bot's grid is
   `required_gamma_sign ∈ {negative, any, positive}` × `structure ∈ {debit,
   credit}` plus the first-half-hour agreement and a higher move floor. Read
   the `twin/*` rows against `gated/default` **on the gated bot's sessions**:
   that paired comparison is the whole experiment.
5. **Read the verdicts the way the catalog does.** `edge` needs PF ≥ 1.1,
   positive expectancy, ≥ 20 trades AND `split.robust`. On the gated fly,
   treat `edge` with ~35 trades as `insufficient` whatever the harness prints
   (§3.1, Power). Record every run in the catalog's `research` tuple whatever
   it says.

## 8. Kill criteria

- **Long-gamma fly, before the screen (section G):** the long-gamma row's
  mean paid/charged within 0.10 of the all-sessions row at every slot, with
  ≥40 sessions per row, retires the gamma gate; screen `twin/tail_only` and
  `twin/clock_only` as the hypotheses. `seller_bps_of_spot` negative on the
  all-sessions row at every slot retires the 0DTE short-premium thesis on
  this tape, gate or no gate.
- **Long-gamma fly, on the screen:** `no-edge` (n ≥ 20, PF < 1.0) retires the
  gamma-gated hypothesis for this tuning generation. The tape-only twin
  within 0.1 PF of the gated bot on the paired sessions means the gate adds
  nothing: keep whichever is simpler if it has edge, and record that the
  gamma conditioning did not. `twin/negative_gamma` ≥ `gated/default`
  retires the mechanism.
- **Close momentum:** the raw cash-tape test (section E) showing the
  short-gamma continuation share ≤ 52% with ≥ 30 qualifying sessions is a
  kill before the screen. On the screen, the negative-gamma arm must beat the
  `"any"` arm by ≥ 0.1 PF or the Baltussen conditioning is not present on
  this data.
- **Gradient re-screen:** `insufficient` on 90 days means the regime the
  pattern's 33 trades came from is episodic; that is a sample statement, not
  a verdict. `no-edge` on n ≥ 20 for the fidelity run is a verdict on the bot
  reproducing the pattern and goes in the catalog as such.

## 9. Open items this branch does not close

- **Twenty-six of thirty adversarial verdicts and the synthesis did not run**
  (§2). The close-momentum bot and the other unreviewed candidates (ATM pin
  fly, charm-decay, compression-forecast, forecast-disagreement, quiet-open,
  overnight condor) should go back through the panel before any of them is
  funded or screened beyond `RESEARCH`.
- **An as-of `net_gex_pctile`.** The replay's percentile uses the window's
  own bands (§6). A percentile computed from `gex_historical_stats` as of
  each snapshot removes the look-ahead; until then the gated arm's result is
  conditioned on information it would not have had.
- **One IV convention.** `atm_iv_front` (chain) and `daily_atm_iv` (daily
  job) must be shown to be the same quantity on the same annualization before
  `max_atm_iv_pctile` is switched on.
- The overnight 1DTE condor (Muravyev & Ni 2020: delta-hedged index option
  returns are reliably negative overnight) was proposed by two designers and
  is the one holding period no bot has ever traded. It needs a 1DTE straddle
  fetch (the front-quote fetcher takes an expiration argument, so the field
  is a one-line addition) and a next-morning time stop. The archivist agreed
  with deferring it until the 0DTE fly has a first screen, so the two are not
  confounded.
- `vanna_vol_crush_rider` is still blocked by `VIX_BARS_RETENTION_DAYS=30`.
  Raising it is a config change, not a code change. The pre-screen no longer
  depends on it: sections E and F scale by the prior session's `daily_atm_iv`
  (115+ sessions per symbol) after the first run came back with 29 sessions.

## 10. First pre-screen reading (production DB, 2026-10-11)

The first `make tradeworkz-v6-prescreen` run, before any bot screen. Sections
E and F were clamped to 29 sessions by the VIX join (fixed since; re-run
them). Section G is the table that matters, and it is read here as a
hypothesis for the bot screen, not a result: the screen runs on the same
sessions.

**B — forecast skill.** Spearman rho between the committed
`expected_vol_ratio` and the realized receipt: SPY 0.11, QQQ 0.23, SPX 0.27,
NDX 0.49 on 54–62 graded days (standard error about 0.13). Committed
compression days realized a little less than other days (SPY 0.87 vs 0.91,
QQQ 0.91 vs 1.02). On SPY the forecast is within noise, so the
`forecast_expansion` gate barely binds there and `gated/uniform_gates` is
the control that says whether it matters anywhere. The writer covers all
four symbols, which the earlier draft of this doc had wrong.

**D — base rates.** Positive gamma held 44% of SPY midday minutes, 50% QQQ,
60% SPX; negative gamma held 59% of SPY minutes at 15:00, 45% QQQ, 42% SPX.
The fly's `positive_strong` gate is the top quartile of each symbol's own
history, so roughly a quarter of midday minutes qualify before the tail
gate; expect the gated arm to trade on the order of 20–25 sessions per
symbol in a 60-session window before `tail_realized` cuts it further.

**E — last-hour momentum.** 20 qualifying sessions pooled across the three
symbols, 40% continuation, a mean of −3 bps in the day's direction; the
short-gamma cells hold one to three sessions each. Nothing visible, and not
a verdict either way at that count. Re-run with the ATM-IV scale before
judging the close-momentum bot.

**F — realized range by gamma sign.** The level (median 0.39–0.57 of the
"normal" range) is a normalization artifact: the cash range excludes the
overnight gap and implied vol carries a premium. The only reading is long
against short gamma, and on 29 sessions it ran against the thesis (SPY
median 0.51 long vs 0.38 short; SPX 0.42 vs 0.37; QQQ flat). The re-run
adds the mean and the 12:00–16:00 range, which is what the fly is exposed
to.

**G — what the straddle charged against what the tape paid.** 52 sessions
per symbol per slot (the chain was captured before every slot on 52 of the
window's sessions). Pooled over SPY, QQQ and SPX:

| entry | mean paid/charged | median | seller win % at bid | seller bps of spot | beyond 1.25σ wing |
|---|---|---|---|---|---|
| 10:30 | 0.863 | 0.765 | 68.6 | +5.4 | 12.8% |
| 12:00 | 0.877 | 0.715 | 65.4 | +3.3 | 16.0% |
| 13:30 | 0.724 | 0.606 | 70.5 | +5.7 | 9.6% |

1. **The unconditional 0DTE premium is on this tape.** A naked straddle sold
   at the bid paid out 72–88% of what it charged on average, before wings,
   slippage and commissions. That is the published 0DTE variance premium,
   and it is largest at 13:30, where the median session paid 61% and the
   wing was reached on fewer than one session in ten.
2. **The gamma gate shows up at 13:30 and nowhere earlier.** Mean paid over
   charged, long gamma vs short gamma, at 13:30: SPY 0.590 vs 0.777, SPX
   0.612 vs 0.887, QQQ 0.692 vs 0.852; the wing was reached on 4.5% / 3.0% /
   10.3% of long-gamma sessions against 13.3% / 10.5% / 17.4% of short-gamma
   ones. At 12:00 the split is mixed (SPX short gamma lost money, −2.4 bps,
   28% beyond the wing; SPY flat). At 10:30 it **reverses** on all three
   symbols: long gamma was worse for the seller (SPY 0.980 vs 0.809, SPX
   0.935 vs 0.874, QQQ 0.859 vs 0.759).
3. **How much to believe it.** Each 13:30 difference is about 1 to 1.7
   standard errors on 19–33 sessions per cell, and SPY and SPX are the same
   index, so this is one suggestive observation, not three replications.
   The 13:30 slot clears the §8 bar on SPY and SPX (long gamma at least 0.10
   below the all-sessions row) and misses it on QQQ (0.07). It is consistent
   with the mechanism — 0DTE dealer gamma and the hedging pin are largest in
   the last hours, and the day's gamma sign is not established at 10:30 —
   which is why it earns a sweep arm and not a new default.
4. **Fly economics at 13:30.** With wings at 1.25σ costing roughly 13% of
   the straddle and four-leg slippage another 8%, the fly keeps about 0.8×
   the straddle mid as credit and risks about 0.6–0.7× that credit to the
   wing. Netted against the naked seller's +5.7 bps pooled, the fly is worth
   on the order of +2 to +3 bps of spot on SPY (about $15–25 per contract)
   and +1 to +2 bps on SPX (about $100–120 per contract) before commissions.
   Thin and positive, and most of it is the unconditional premium.
5. **What changed in the branch because of it.** The sweep gained
   `gated/afternoon` and `twin/tail_only_afternoon` (entries 12:30–14:00 ET,
   held to 15:40), the pre-screen gained a 14:30 slot, and the default
   10:30–13:30 window stays as the designers set it so the screen can show
   the morning entries losing on their own.

What G does not say: nothing here is out of sample for the bot screen, the
"both legs two-sided" rule drops the sessions with no clean ATM quote in the
ten minutes before the slot (52 kept of about 63 with cash bars, per symbol),
and the gamma sign is the sign at entry, not the top quartile the bot gates
on. The screen's `twin/tail_only` row against `gated/default`, and
the afternoon pair against each other, are the next reading.
