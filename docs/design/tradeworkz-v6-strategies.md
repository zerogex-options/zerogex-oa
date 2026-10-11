# TradeWorkz v6 — Trade the Magnitude

**Status:** candidates (registered + backtestable; **not live**). First screens
not yet run — see §7 for the order to run them in.
**Date:** 2026-10-11
**Extends:** `tradeworkz-edge-strategies.md` (v4), `tradeworkz-v5-strategies.md`
(v5), `strategy-catalog.md`.

---

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
| `docs/design/msi-regime-excursion.md` | Do the MSI regime bands predict realized excursion? | Yes, monotonically, on every instrument (15m forward range vs base: SPY 0.81/0.97/1.04/1.09 across the four bands); 1,386 of 4,140 tests survive BH. A **magnitude-only** rebuild of the MSI beats the shipped one on 30 of 30 cells; the directional components carry zero excursion information. |
| `research/trade_bias_movement` | Do the Trade Bias states say how far price moves? | Nothing detectable for the states. But production's short-gamma definition travels **1.08x [1.03, 1.13]** after adjusting for clock and tape; the "neither" zone 0.92x [0.88, 0.97]. |
| `research/short_gamma_trend` | Does short gamma + one-sided flow continue? | No (leans to reversal). Those minutes DO move 1.29x as far. |
| `research/trade_bias_inputs` | Does any of the nine inputs predict 30m direction? | None, Holm-corrected. |
| `research/wall_break_odds` | What predicts a wall break? | Nothing replicates across symbols. Break odds are a per-index property (S&P ~31%, Nasdaq ~50% at 60m). |

**Dealer gamma predicts how far, not which way.** Twenty-eight bots tried to
extract direction from a magnitude signal. The structure that is paid for
"less travel than the market priced" is a short option structure, and the
most replicated anomaly in index options — the variance risk premium, and
specifically the 0DTE version (Beckmeyer, Branger & Gayda 2023; Bandi, Fusari
& Reno 2023) — says the market systematically overprices that travel. The
edge-defining question is only **when not to sell**, and that is exactly the
question the magnitude findings answer.

The v6 thesis in one line:

> **Sell the 0DTE move, defined-risk, only when this platform's confirmed
> magnitude signals say the tape will travel less than the straddle is
> charging — and run the gamma-blind twin alongside so the gate has to earn
> its place.**

Plus one directional exception with a published mechanism and a fixed clock
that no study here has tested (§3.2).

## 2. How these were designed

Five independent designers (a variance trader, a dealer-hedging quant, an
empirical-finance academic, the repository's own post-mortem engineer, and an
options execution specialist) each read the full research record and
proposed strategies from their own lens. Their proposals were clustered, then
each candidate was attacked by three adversarial reviewers (an archivist
looking for dead rows in disguise, a market maker pricing the friction and the
tail, and a statistician checking testability and look-ahead). Only candidates
at most one reviewer could refute went forward. Four of the five designers
independently proposed the same core trade (§3.1); two proposed the same
directional exception (§3.2).

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
  never the absolute fallback) · `magnitude_band` — the magnitude-only MSI
  (`snap.magnitude_score()`, §4) in chop or reversal · `tail_realized` —
  trailing 30-bar realized vol ≤ 0.85× the ATM implied · `forecast_expansion`
  / `range_spent` — the morning forecast's own expansion call and today's
  cash range vs its implied move, when a forecast exists · `flip_near` —
  neither flip convention within a sigma of the strike · `vol_shock` (30m
  ΔVIX > 5% of level) · `iv_elevated` (ATM IV above its 80th percentile of
  the prior 60 sessions) · `event_day` · `conviction`.
- **Exits:** `credit_take` at 50% of the credit, `credit_stop` at 1.0× the
  credit (base-class credit branch), plus invalidations that close regardless
  of P&L — `regime_lost` (net gamma turns negative), `flip_entered` (a flip
  within half a sigma of the strike), `short_touched`, `vol_shock` — and a
  time stop 20 minutes before the bell. Never holds the short straddle into
  the settlement window.
- **Why not a dead row:** `range_iron_condor` sold at the walls (where a
  tested wall breaks 31–50% of the time), on absolute VIX, any positive
  gamma, a flat 180-minute hold and no credit exit. This bot never reads a
  wall, centers on the money where credit per unit of wing is largest, gates
  on the symbol-relative percentile and a measured realized-under-implied
  condition, and books or cuts on the credit.
- **Controls (params, not bots):** `required_regimes: "any"` is the
  gamma-blind twin (pure clock + VRP); `allowed_magnitude_bands: null` drops
  the band gate; `short_offset_sigma: 1.0` turns it into a condor. If the
  gamma-blind twin's PF is within 0.1 of the gated bot's, the gamma gate adds
  nothing and the catalog will say so.
- **Prior:** PF 1.1–1.3, win rate 65–75%, losers 2× winners, ~3–5
  trades/week across SPY/QQQ/SPX. The failure mode is three to five afternoon
  trend days in 80 sessions landing in one half of the split.

### 3.2 Hedging-Demand Close Momentum — `hedging_demand_close_momentum`

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
  cash-tape pre-test in §7 step 1 answers most of this before an option is
  priced.

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
| `forecast_implied_move`, `forecast_expected_vol_ratio`, `forecast_expected_vol_state`, `forecast_vix_z`, `forecast_is_event_day` | `daily_forecast` row for the session date with `open_ts <= as_of` | `fetch_daily_forecast` | Immutable once committed at 08:30 ET; a replayed 08:00 tick does not see it. **The writer defaults to SPY only** — QQQ/SPX read None unless the forecast runs for them, and forecast-gated bots abstain there honestly. |
| `atm_iv_pctile_60d` | `daily_atm_iv`, trading dates strictly before today | `fetch_atm_iv_history` + `_percentile_rank` | Today's row is rewritten all day, so only prior (frozen) days are ranked against. Needs 20 observations. |
| `cash_open`, `cash_high`, `cash_low`, `first_half_hour_close` | `underlying_quotes` from the 09:30 ET cash open (09:31 for a cash index) | `fetch_cash_session_bars` | The existing `session_*` fields span the trailing 24 hours and include the pre-market. |

Helpers on `MarketSnapshot`: `magnitude_score()` / `magnitude_band()` (the
magnitude-only MSI — prefers the engine's published
`__aggregation__.magnitude_score`, which only began persisting in September
2026, and otherwise rebuilds it from the four persisted component scores with
the engine's own renormalize-then-`tanh` arithmetic, so a replay over the
whole archive reads the same number); `realized_sigma_1min()`,
`realized_over_implied()`, `straddle_implied_sigma()`,
`expected_remaining_move()` (the forecast's implied move scaled by the
intraday cone model's **diurnal** variance profile, not sqrt-of-time),
`cash_range_over_implied()`, `day_return()`, `first_half_hour_return()`.

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
(calibrated so a top-quartile regime with realized at half of implied clears
the 0.50 floor — the v5 lesson about zero-trade screens dying at conviction),
and the invalidation exits. One module to screen, one set of miss labels
across the family, one sweep grid.

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
- The regime percentile bands are today's 30-day aggregate at every past
  instant (disclosed in `backtest.py`); they never flip a sign.
- The dead rows this family must not become: a short premium structure
  that works only because the window happened to be quiet. That is why the
  gamma-blind twin runs in the same screen, and why §7 step 1 reports the
  realized-vs-normal range split by gamma sign before any option is priced.

## 7. What to run first, in order

Cheapest and most informative first. Steps 1–3 need no new code paths beyond
this branch and run in seconds.

1. **`make tradeworkz-v6-prescreen`** (read-only SQL,
   `setup/database/diagnostics/tradeworkz_v6_prescreen.sql`). It answers:
   - whether the morning forecast's `expected_vol_ratio` carries skill
     (Spearman vs the realized receipt, per symbol) — if rho is ~0 the
     forecast gates are noise and only the raw-VRP control is worth running;
   - how often positive gamma holds midday and negative gamma at 15:00 (the
     two regime base rates the bots' frequency estimates rest on);
   - **the raw last-hour momentum test**: on sessions with a ≥0.5-sigma day
     return, how often 15:00→15:55 continues, split by gamma sign at 15:00.
     If the short-gamma row is not clearly above 50%, §3.2 cannot be rescued
     by an option structure;
   - whether positive gamma compresses the realized day on this tape
     (realized range / normal range, split by gamma sign at noon).
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
   and the controls through the execution sweep, which now picks its grid
   from the bot's class and scores every config on a train/test split:
   ```
   python -m src.tools.tw_execution_sweep --bot long_gamma_iron_fly --days 90
   python -m src.tools.tw_execution_sweep --bot hedging_demand_close_momentum --days 90
   ```
   The fly's grid is the gated default against the twins that drop one gate
   at a time (`twin/gamma_blind`, `twin/no_band`, `twin/tail_only`,
   `twin/clock_only`) plus wing / take / condor / stop geometry; the
   momentum bot's grid is `required_gamma_sign ∈ {negative, any, positive}`
   × `structure ∈ {debit, credit}` plus the first-half-hour agreement and a
   higher move floor. Read the `twin/*` rows against `gated/default`: that
   comparison is the whole experiment.
5. **Read the verdicts the way the catalog does.** `edge` needs PF ≥ 1.1,
   positive expectancy, ≥ 20 trades AND `split.robust`. Record every run in
   the catalog's `research` tuple whatever it says.

## 8. Kill criteria

- **Long-gamma fly:** `no-edge` (n ≥ 20, PF < 1.0) retires the gamma-gated
  hypothesis for this tuning generation. The gamma-blind twin within 0.1 PF
  of the gated bot means the gate adds nothing: keep whichever is simpler if
  it has edge, and record that the magnitude conditioning did not.
- **Close momentum:** the raw cash-tape test (step 1) showing the short-gamma
  continuation share ≤ 52% with ≥ 30 qualifying sessions is a kill before the
  screen. On the screen, the negative-gamma arm must beat the `"any"` arm by
  ≥ 0.1 PF or the Baltussen conditioning is not present on this data.
- **Gradient re-screen:** `insufficient` on 90 days means the regime the
  pattern's 33 trades came from is episodic; that is a sample statement, not
  a verdict. `no-edge` on n ≥ 20 for the fidelity run is a verdict on the bot
  reproducing the pattern and goes in the catalog as such.

## 9. Open items this branch does not close

- The overnight 1DTE condor (Muravyev & Ni 2020: delta-hedged index option
  returns are reliably negative overnight) was proposed by two designers and
  is the one holding period no bot has ever traded. It needs a 1DTE straddle
  fetch (the front-quote fetcher takes an expiration argument, so the field
  is a one-line addition) and a next-morning time stop. Deferred until the
  0DTE fly has a first screen, so the two are not confounded.
- `vanna_vol_crush_rider` is still blocked by `VIX_BARS_RETENTION_DAYS=30`.
  Raising it is a config change, not a code change.
- Forecast coverage for QQQ and SPX: the morning writer runs for SPY by
  default. Enabling it for the other two symbols would make the forecast
  gates and `expected_remaining_move()` available fleet-wide.
