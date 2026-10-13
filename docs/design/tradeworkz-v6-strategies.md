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
- **Entry (13:00–14:30 ET, flat by 15:40), every gate a named `_skip`:**
  the designers chose 10:30–13:30; the pre-screen's straddle table moved it
  (§10, second run), and the morning window is the `gated/morning` sweep arm.
  `window` ·
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
  - *Stop.* At a 1.25-sigma wing (1.57× the straddle mid out) with a credit
    near 0.8× the mid, the fly's maximum loss is roughly 0.9–1.0× the credit,
    so the 1.0× stop could never fire. It is 0.5× now, with a `gated/no_stop`
    arm where the wing is the only stop. On SPY's $1 grid the wing often
    rounds in to 1.0 sigma, where max loss is ~0.7× the credit.
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
- **The fade (`trade_direction: "fade"`):** sells the day's move instead.
  The pre-screen's cash-tape table (§10, second run) showed the last hour
  REVERSING the day on this tape, most under long gamma, which is what
  dealers long gamma hedging into the close would do. Sweep arms
  `positive/fade` (long gamma) and `any/fade` (unconditional); a fade's
  cross back through the open books as `fade_complete`. The continuation
  default stays so the screen records the papers' object on its own.
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
   `setup/database/diagnostics/tradeworkz_v6_prescreen.sql`; section G
   builds its straddle set once into a session-local temp table, so the
   whole file runs in a few minutes — run it under `tmux` or `nohup ... |
   tee` so an SSH drop does not lose the output). It answers:
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
   - **the fly's P&L before any bot runs** (section G): at 10:30, 12:00,
     13:30 and 14:30 ET, the 0DTE ATM straddle mid and bid against
     |px_1555 − strike|, split by gamma sign before entry. The mean
     paid/charged ratio is 1.0 under fair pricing and near 0.85–0.90 on the
     published premium. **The gamma gate earns its place only if the
     long-gamma row sits ≥0.10 below the all-sessions row at the same slot
     with ≥40 sessions in the row.** `seller_bps_of_spot` negative on a row
     means no exit logic rescues it; `beyond_1p25_sigma_wing_n` counts the
     sessions on which the fly would have reached its wing. This is a
     hold-to-15:55 measurement: it describes `gated/hold_to_settle`, not the
     default exits. G2 re-cuts the same straddles by gamma sign × direction
     of the day, G3 by straddle richness (§10, item 4): the gate has to beat
     both proxies before it is read as dealer hedging.
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
   `gated/uniform_gates`, the geometry arms and the morning pair
   (`gated/morning` vs `twin/tail_only_morning`, the designers' original
   window); the momentum bot's grid is `required_gamma_sign ∈ {negative, any,
   positive}` × `structure ∈ {debit, credit}` plus the first-half-hour
   agreement, a higher move floor, and the two fade arms (`positive/fade`,
   `any/fade`). Read
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
  this tape, gate or no gate. A long-gamma advantage that appears only in
  the up-day rows of G2, or that the cheap-straddle half of G3 matches,
  retires the mechanism reading: the gate is then a proxy for day direction
  or a quiet tape. **Status after the first run (§10): insufficient** — the
  margin clears 0.10 at one slot of three on SPY and SPX only, and no split
  row has 40 sessions.
- **Long-gamma fly, on the screen:** `no-edge` (n ≥ 20, PF < 1.0) retires the
  gamma-gated hypothesis for this tuning generation. The tape-only twin
  within 0.1 PF of the gated bot on the paired sessions means the gate adds
  nothing: keep whichever is simpler if it has edge, and record that the
  gamma conditioning did not. `twin/negative_gamma` ≥ `gated/default`
  retires the mechanism.
- **Close momentum:** section E can only rule out a LARGE effect. Pooled
  across SPY+SPX (one index, counted once) and QQQ, about 25 qualifying
  sessions and 10–12 short-gamma ones are all a 63-session window yields,
  and a 52%-on-30 test cannot separate 50% from the published mid-50s. The
  kill before the screen is therefore: pooled short-gamma continuation at or
  below 50% with the long-gamma rows no worse, on the re-run. On the
  screen, the negative-gamma arm must beat the `"any"` arm by ≥ 0.1 PF or
  the Baltussen conditioning is not present on this data.
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
  after the first run came back with 29 sessions; the binding window is now
  the 95-day cash-bar read, about 63 sessions per symbol.
- **A 0DTE-book gamma sign for the pre-screen.** `gex_summary.net_gex_at_spot`
  is the DTE-weighted structural read (§10, item 4). `gex_by_strike` carries
  per-strike net GEX with an `expiration_bucket`, so a same-day-bucket net
  gamma near spot can be built for G2 to test the hedging mechanism on the
  book that actually hedges intraday. Not built here.

## 10. First pre-screen reading (production DB, 2026-10-11)

The first `make tradeworkz-v6-prescreen` run, before any bot screen, read by
the author and then checked by six adversarial reviewers and a completeness
critic (all seven ran). Their corrections are folded in below; where the
first draft of this section overstated something, this version says what
survived. Sections E and F were clamped to 29 sessions by the VIX join
(fixed; re-run them). Section G is the table that matters, and it is a
hypothesis for the bot screen, not a result: the screen runs on the same
sessions.

**B — forecast skill.** Spearman rho between the committed
`expected_vol_ratio` and the realized receipt: SPY 0.11, QQQ 0.23, SPX 0.27,
NDX 0.49 on 54–62 graded days (standard error about 0.13, so SPY is 0.9 SE
from zero, SPX 2.1, NDX 3.5). Committed compression days realized a little
less than other days (SPY 0.87 vs 0.91, QQQ 0.91 vs 1.02). On SPY the
`forecast_expansion` stand-down is a random trade cut on an arm that already
expects ~35 trades, so `gated/uniform_gates` is the primary gated arm there.
The writer covers all four symbols, which the earlier draft of this doc had
wrong.

**D — base rates.** Positive gamma held 44% of SPY midday minutes, 50% QQQ,
60% SPX; negative gamma held 59% of SPY minutes at 15:00, 45% QQQ, 42% SPX.
SPY and SPX are the same index, so the 44% vs 60% is the gamma classifier
disagreeing with itself on the same days (it does so on 11–13 of 52 sessions
per slot in G). Expect the gated arm to trade on the order of 20–25 sessions
per symbol in a 60-session window before `tail_realized` cuts it further.

**E — last-hour momentum.** Uninformative, not discouraging: 20 qualifying
symbol-sessions pooled, which is at most 14 distinct symbol-days (SPY and
SPX are the same six days) and roughly 8–10 distinct days; 8 of 20
continued (two-sided p 0.50, 95% interval 22–61%, which contains the null
and the published effect alike); the mean of −3 bps is within one standard
error of zero. Pooled short gamma is 3 of 6. The SPY and SPX labels disagree
on those six days (3 long / 3 short against 5 long / 1 short), so the
short-gamma row is not one condition across symbols. The low admission rate
is arithmetic, not a bug: the 09:30→15:00 return omits the overnight gap and
the last hour, and realized ran about 0.4–0.5 of implied, so a 0.5-sigma
floor admits ~17% of sessions on SPY/SPX. After the fix the window is ~63
sessions, which projects ~25 qualifying pooled and 10–12 short gamma; §8's
E rule is restated accordingly.

**F — realized range by gamma sign.** The level (median 0.39–0.57 of the
"normal" range) is a normalization artifact: the cash range excludes the
overnight gap and implied vol carries a premium, so the 0.85 "compression"
share was hit by construction (93%) and those columns are gone. QQQ was also
scaled by VIX rather than its own vol (its level ran 1.46× SPX's against an
IV ratio of 1.57). The long-vs-short medians on 29 sessions leaned the wrong
way on SPY (0.51 vs 0.38, about 1.5–2 SE on 11 vs 18 sessions), under 1 SE
on SPX, flat on QQQ, with the classifier disagreeing on at least 5 of the 29
sessions: no support for the thesis, within noise, and 2.5 of the 6.5 hours
of range were realized before the 12:00 gamma reading. The re-run's
`pm_ratio` (12:00–16:00 range) is the clean read; the 29-session result
should not be scored.

**G — what the straddle charged against what the tape paid.** 52 sessions
per symbol per slot (52 kept of about 63 with cash bars; a session is dropped
when no ATM strike has two-sided quotes on both legs in the ten minutes
before the slot). Hold to 15:55, no stop. Pooled over SPY, QQQ and SPX,
which are three readings of two correlated instruments on the same days, so
the effective sample is ~52 sessions, not 156:

| entry | mean paid/charged | median | seller win % at bid | seller bps of spot | beyond 1.25σ wing |
|---|---|---|---|---|---|
| 10:30 | 0.863 | 0.765 | 68.6 | +5.4 | 12.8% |
| 12:00 | 0.877 | 0.715 | 65.4 | +3.3 | 16.0% |
| 13:30 | 0.724 | 0.606 | 70.5 | +5.7 | 9.6% |

1. **The 0DTE premium is on this tape, resolved at 13:30 only.** All nine
   symbol × slot rows have a positive point estimate for the naked seller at
   bid (+1.1 to +9.9 bps of spot). At 10:30 and 12:00 the pooled ratios
   (0.86, 0.88) sit on the published 0.85–0.90 but are only ~1.5 SE below
   fair pricing, and the SPX rows there (+2.3, +1.1 bps) are under 1 SE from
   zero. At 13:30 every symbol is 3–4 SE below fair pricing (0.70, 0.71,
   0.76; +4.4 to +7.2 bps), which is richer than the published range, not
   merely consistent with it. 13:30 leads by ratio and win rate on every
   symbol; by bps of spot it ties 10:30 (5.7 vs 5.4), QQQ is best at 10:30,
   and no slot-vs-slot difference exceeds 1 SE. Picking the best of three
   slots adds roughly 0.85 SE of optimism to whatever it shows. The 15:55
   proxy for settlement drops the closing auction and understates what was
   paid by 2–4%, most at 13:30.
2. **The gamma split at 13:30 is a lean, not a finding.** Long gamma below
   short gamma on mean paid/charged on all three symbols (SPY 0.590 vs
   0.777, SPX 0.612 vs 0.887, QQQ 0.692 vs 0.852), each 0.8–1.6 SE on 19–33
   sessions per cell; in bps at bid the gap is +1.5 / +4.9 / +5.4. The
   wing-share contrast is counts of 1 vs 4, 1 vs 2 and 3 vs 4 sessions
   (Fisher p 0.38–0.69). Against §8's own rule (long row ≥0.10 below the
   all-sessions row, ≥40 sessions per row) the margins are 0.108 / 0.100 /
   0.071 and the largest split row has 34 sessions: **insufficient**.
   Pooled over the three slots the gamma effect is −0.06 (SPY 0.00, SPX
   −0.13, QQQ −0.04); one slot of three agreeing at ~1.3 SE happens about a
   quarter of the time under the null before the symbol dependence is
   counted.
3. **The 10:30 "reversal" is a pricing effect, not a realized one.** It
   holds on means only: on QQQ the median and win rate favor long gamma, SPX
   is flat, and only SPY reverses on every metric (0.85 SE). In dollars,
   from the 10:30 strike the tape settled the same distance away under both
   gamma signs (SPY $2.17 vs $2.04, SPX 20.3 vs 22.0, QQQ 2.76 vs 2.69); the
   whole gap is the short-gamma straddle being 10–16% richer, which is IV bid
   on a down open. At 13:30 the realized move itself differs (SPY $0.81 vs
   $1.34, SPX 8.0 vs 14.0, QQQ 1.31 vs 1.87) on top of a 16–26% richer
   short-gamma straddle. The short-gamma straddle was richer in all nine
   cells: gamma sign travels with the IV level.
4. **What the gate measures.** `gex_summary.net_gex_at_spot` is read off the
   DTE-weighted structural profile (`GAMMA_PROFILE_DTE_WEIGHTING`, linear
   ramp `min(1, DTE / 5 days)`), built from open interest that updates once
   a day. A same-day contract carries ~2% weight at 13:30, a 1DTE ~20%. The
   gate therefore reads which side of the multi-day flip spot sits on — on
   most days, whether the day is up or down — not intraday 0DTE dealer
   gamma. The 13:30 split is consistent with the hedging-pin thesis and
   equally with "down-day afternoons realize more". The re-run's G2 (gamma
   sign × direction of the day at entry) and G3 (cheap vs rich straddle
   halves) separate the readings: a long-gamma advantage inside BOTH
   direction rows, and beyond what the cheap-straddle half shows, is the
   mechanism; anything less makes the gate a proxy and `twin/tail_only` the
   trade.
5. **Fly economics at 13:30, hold to 15:55.** Harness slippage is 2% of each
   leg's price, about 2.2% of the straddle at entry (the 8% in §6 is bid/ask
   plus slippage, round trip). Wings at 1.25σ are worth ~12.7% of the
   straddle at normal-model mid, a lower bound before skew and the ask. Net
   credit ≈ 0.76–0.82× the straddle mid; max loss ≈ 0.9–1.0× the credit.
   The strike grid matters: at the mean 13:30 SPY straddle ($1.58, σ $1.98)
   the $1 grid rounds the wing to $2 = 1.0σ, so the SPY fly the harness
   trades pays ~21% of the straddle for wings; SPX's $5 grid gives $20 =
   1.14σ. Net of wings and the wing payoff on beyond-wing sessions: SPY
   about +1.0 to +3.1 bps of spot ($8–24 per contract) before ~$5 of
   commissions, which is 20–60% of the edge; SPX about +1.1 to +2.2 bps
   ($86–170 per contract) before ~$10–13. **SPX is the instrument.** The
   gate's increment over the unconditional fly is ~+0.9 bps on SPY and ~+1.8
   bps on SPX, neither outside noise. None of this is the default bot's
   expectancy: it takes at 50% of credit, stops at 0.5×, and is flat at
   15:40, when a still-ATM 13:30 straddle retains ~37% of its time value.
6. **What changed in the branch because of it.** The sweep gained
   `gated/afternoon` and `twin/tail_only_afternoon` (entries 13:00–14:00 ET,
   held to 15:40; bounds to be re-set from the 13:30 and 14:30 rows of the
   re-run), the pre-screen gained the 14:30 slot and the G2/G3 re-cuts, F
   lost its artifact columns and gained the afternoon range, and E/F scale by
   each symbol's own ATM IV. The default window stays 10:30–13:30 so the
   screen can show the morning entries on their own.
7. **Pre-committed reading for the re-run.** A 14:30 pooled ratio at or
   below ~0.75 with long gamma under short gamma on SPY and SPX continues
   the 0.86 → 0.88 → 0.72 trend and keeps the afternoon arms; a return to
   ~0.85 with long ≈ short marks 13:30 as a one-slot fluctuation. The 14:30
   straddle is ~0.75× the 13:30 one, so fixed commissions take a larger
   share there.

What G does not say: nothing here is out of sample for the bot screen, one
contiguous window is one vol regime, the two-sided-quote rule may drop busy
sessions, and the screen's `twin/tail_only` row against `gated/default` on
the same sessions, then the afternoon pair against each other, are the next
reading.
