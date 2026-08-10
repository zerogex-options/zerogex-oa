# TradeWorkz v4 — Edge-Metric Strategies

**Status:** candidates (registered + backtestable; **not live**)
**Date:** 2026-08-09
**Supersedes the intent of:** the fleet shelved in `df9d593`

---

## 1. Why the old fleet had no edge

On 2026-08-09 the entire TradeWorkz fleet was shelved. The backtest harness
screened every bot over 45 days / 2,065 trades and found none with entry edge —
best profit factor 0.78, live PF ~0.42, −$174K. A frictionless run on the worst
loser proved *the signal was dead, not just expensive*.

Looking at the fourteen retired bots, they share one root cause:

> **Every retired bot traded a *static, first-order* positioning level.**

Walls, gamma flip, VWAP, max-pain, net-GEX sign. `DealerDeltaPressureRider`'s
"dealer delta" was literally `net_gex_at_spot` re-used. These are the exact
levels every SpotGamma-style dashboard publishes — the retail GEX narrative
("positive gamma pins, negative gamma trends, price gravitates to max pain").
A well-known, widely-published level is arbitraged away; trading *the level*
statically has no edge left in it.

What the retired fleet **never touched** is the richer data ZeroGEX also
generates — the layers where information, not folklore, lives:

| Layer | Table / field | What it is |
|---|---|---|
| **Aggressor order flow** | `flow_series_5min.net_premium_cum`, `net_volume_cum` | Lee-Ready buy/sell-classified signed option premium. *Who is aggressively paying up, right now.* |
| **Second-order forced dealer flow** | `gex_by_strike.dealer_vanna_exposure`, `dealer_charm_exposure` | The mechanical hedge dealers **must** do as time passes (charm) and as IV moves (vanna). Direction-known, scheduled. |
| **Modeled close-charm flow** | `forced_flow_profile.close_charm_flow(_smooth)` | "$ of stock dealers must trade by the cash close if spot holds." A *quantified* version of the afternoon-drift folklore. |
| **Pin Strike** | `gex_summary.pin_strike`, `pin_confidence` | The gamma-restoring, reachability-filtered magnet — distinct from OI-based max-pain. |
| **Positioning *velocity*** | `net_gex` vs `prior_net_gex`, `flip_distance`, `convexity_risk` | The *change* in dealer positioning, not the standing level. |

The v4 thesis in one line:

> **Trade flow, change, and second-order mechanics — not static first-order
> levels.** Where the retired bots read a level, these read a *forced flow* or a
> *transition*, and each fires only when the mechanism actually points the way
> the folklore assumes.

---

## 2. The data layer (what made these possible)

The bots consume a `MarketSnapshot`, and that snapshot historically carried
**only** the first-order levels. Two additions open the new layers to the bot
tier, both strictly additive and best-effort (every existing bot ignores them):

- **`src/tradeworkz/flow_context.py`** — four as-of-bounded, transaction-isolated
  fetchers (`fetch_forced_flow`, `fetch_second_order_totals`,
  `fetch_recent_option_flow`, `fetch_vix_lookback`). Each mirrors the failure
  semantics of `_fetch_trade_bias`: a missing table / thin history nulls only
  its own fields and never aborts the snapshot, and every read is bounded to
  `timestamp <= COALESCE(as_of, NOW())` so the backtest harness stays faithful.
- **`MarketSnapshot`** gains `pin_strike/score/confidence`, `flip_distance`,
  `convexity_risk`, `local_gex`, `prior_net_gex/gamma_flip`,
  `dealer_vanna_total`, `dealer_charm_total`, `close_charm_flow`, `charm_flip`,
  `vanna_flip`, `flow_net_premium(_prev)`, `flow_net_volume`, `prior_vix`, plus
  derived helpers `net_gex_change()`, `flow_premium_delta()`, `vix_change()`,
  `distance_to_pin_pct()`.

---

## 3. The strategies

Each is a defined-risk debit **vertical** (naked 0DTE debits bleed too fast);
each has an explicit *edge filter* that the folklore version lacks.

### 3.1 Charm Close Magnet — `charm_close_magnet`

- **Supersedes:** `EodPinDrifter`, `MaxPainGravitator` (drifted toward OI
  max-pain on displacement + time-of-day alone — folklore).
- **Mechanism:** In the final ~2 hours, charm (dΔ/dt) forces an *accelerating*
  dealer hedge into expiry. ZeroGEX **quantifies** it:
  `forced_flow_profile.close_charm_flow` is the dollars dealers must trade by
  the close, and the magnet is the gamma-restoring **Pin Strike**, not OI
  max-pain.
- **Direction** = the sign of `close_charm_flow` itself (the forced flow),
  confirmed by a **magnet** on the same side: `pin_strike` when a real pin
  exists, else `max_pain` (see §7 — the pin is persisted only ~0% of the time,
  so max_pain is the working magnet). The differentiator survives: unlike the
  retired pin-drifters it demands the QUANTIFIED charm flow point the same way
  before it trades.
- **Entry:** last 10–120 min to close · positive-γ · magnet on the flow's side ·
  magnet 0.1–1.0% away · bias-veto.
- **Structure/exit:** narrow vertical toward the magnet; target = magnet; stop =
  a move to the wrong side of `charm_flip`; time-stop into the close.

### 3.2 Vanna Vol-Crush Rider — `vanna_vol_crush_rider`

- **Supersedes:** `VixRegimeBreakout` (used only the VIX *level* > 16; never
  vanna, never the vol *change*).
- **Mechanism:** forced dealer flow ≈ `dealer_vanna_total × ΔIV`.
  `dealer_vanna_exposure` is "$ dealers must trade per +1 vol point"; multiply
  by the session ΔVIX for the sign **and** size of the hedge. The canonical case
  is the vol-crush melt-up: a short-vanna book (`dealer_vanna_total < 0`) into a
  falling VIX yields a positive product → dealers **buy**.
- **Edge filter:** direction is a *two-sign product* (`vanna × ΔVIX`), not a
  VIX threshold — non-obvious and not the retail "high VIX ⇒ trend" trade.
  Requires a real vol move (`|ΔVIX| ≥ 0.30`) and meaningful vanna.
- **Structure/exit:** vertical in the forced direction; modest grind target
  (vanna flow is steady, not explosive) capped at `vanna_flip`; small stop.

### 3.3 Aggressor Flow Divergence — `aggressor_flow_divergence`

- **Supersedes:** *nothing* — **no retired bot used option order flow at all.**
- **Mechanism:** aggressor-classified net premium (`net_premium_cum`) leads
  price on liquid names. The edge is a **divergence**: real money is
  aggressively paying up on one side while price has **not yet** moved to match.
  That gap is a coiled directional move — lead it.
- **Edge filter:** strong, *still-accelerating* net premium **confirmed on
  volume**, while the recent price move is still small (< 0.25%). Stands down in
  a strong positive-γ pin, where dealers absorb the flow and the lead-lag breaks.
- **Structure/exit:** vertical in the flow direction; target = first wall that
  way; stop = a small move *against* the flow (thesis invalidated).

### 3.4 Gamma Regime Shift Rider — `gamma_regime_shift_rider`

- **Supersedes:** `GammaFlipBreaker`, `DealerDeltaPressureRider` (traded a
  static flip *break* / static net-GEX *sign*).
- **Mechanism:** the edge is the **derivative**, not the level. When dealer net
  gamma collapses tick-over-tick (`net_gex_change` strongly negative) *through*
  positive territory, with spot **at** the flip and `convexity_risk` elevated,
  the regime is transitioning long→short — absorption turning into
  amplification. That transition is the clean leg the static break missed.
- **Edge filter:** requires a genuine **crossing** — `prior_net_gex > 0` and a
  one-tick shed of ≥ 25% of `|prior_net_gex|` (RELATIVE, so it means the same
  for SPY and SPX), resolving into short γ (net-GEX crossed ≤ 0 **or** spot
  within ~1.2% of the flip — see §7), plus aggressor **volume that confirms**
  the break.
- **Structure/exit:** vertical in the break direction; target = far wall (short-γ
  lets price run); stop = a **reclaim of the flip** (transition aborted).

---

## 4. Differentiation matrix

| v4 bot | Untapped axis | Retired analog | Why the analog failed |
|---|---|---|---|
| Charm Close Magnet | time-Greek + quantified forced flow + pin | EodPinDrifter / MaxPainGravitator | drift on displacement + folklore, wrong magnet |
| Vanna Vol-Crush Rider | vol-Greek × Δvol | VixRegimeBreakout | VIX *level* only, no vanna, no Δvol |
| Aggressor Flow Divergence | order flow (lead-lag) | *(none)* | fleet never used flow |
| Gamma Regime Shift Rider | positioning *velocity* | GammaFlipBreaker / DealerDeltaPressureRider | static level / sign |

Each hits a **distinct** axis, so as a set they are non-overlapping.

---

## 5. Promotion gate — nothing goes live on a thesis

These are registered in `STRATEGY_CLASSES` and carried in `CANDIDATE_SPECS`, but
**`DEFAULT_ROSTER` stays empty** — they never provision, size, or open. That is
deliberate: the whole point of the shelving was that nothing goes live on hope.

A candidate is never provisioned into `tw_bots`, so the backtest harness
resolves an un-provisioned `--bots <id>` from the registry catalog
(`registry.known_specs`, wired into `_load_backtest_bots`) — that is what makes
a candidate screenable *before* it is ever seeded. A candidate is promoted
**only** after it clears the same gate revival requires:

```
make tradeworkz-backtest ARGS="--days 45 --interval-min 5 --bots charm_close_magnet,vanna_vol_crush_rider,aggressor_flow_divergence,gamma_regime_shift_rider --json"
```

Promotion criterion (unchanged from the shelving note): **profit factor ≥ 1.1,
positive expectancy, ≥ 20 trades.** To promote a bot that clears it, move its
spec from `CANDIDATE_SPECS` into `DEFAULT_ROSTER` (leave it out of
`RETIRED_BOT_IDS`). Nothing else is required — the engine provisions it on the
next boot.

> The backtest requires historical `gex_by_strike` / `forced_flow_profile` /
> `flow_series_5min` / `option_chains(_archive)` coverage. On symbols/windows
> where those tables are thin, the relevant snapshot fields read `None` and the
> dependent bot simply abstains (it never trades on absent data) — so a thin
> backtest reads as thin coverage, never as false edge.

---

## 6. Backlog & successors

### 6.1 Fresh Flow Momentum — `fresh_flow_momentum` (SCREENED OUT, PF 0.33)

The successor to the screened-out `aggressor_flow_divergence`. Its post-mortem
(§8) was that the day-to-date CUMULATIVE net premium is a *lagging* aggregate.
This is a NEW hypothesis on the same data axis, not a revival:

- **Signal:** the *fresh* windowed flow — net aggressor premium over the last
  ~15 min (`flow_recent_premium`, a k-bucket cumulative-difference on
  `flow_series_5min`) — and its **acceleration** vs the window before it
  (`flow_prior_window_premium`). A burst that is *bigger* than the prior window,
  confirmed on volume, that price has only *begun* to follow.
- **Trade:** momentum-continuation (ride the documented ~30s lead-lag), not the
  old strict price-flat fade. Stands down in a strong positive-γ pin.
- **Data:** `flow_series_5min` is ~100% populated over the window, so unlike the
  charm/vanna bots this one is immediately screenable. New snapshot fields
  `flow_recent_premium` / `flow_recent_volume` / `flow_prior_window_premium` via
  `flow_context.fetch_recent_flow_window`.

If it clears PF ≥ 1.1 / positive expectancy / ≥ 20 trades it earns
`DEFAULT_ROSTER`; if not, it is shelved like its predecessor — a fresh signal is
a hypothesis, not a promise.

### 6.2 Climax Flow Fade — `climax_flow_fade` (implemented; screening)

The contrarian read the two flow-follow failures pointed at directly. Their
signature (33% win rate, wins < losses) says an aggressive 0DTE flow burst marks
a *local extreme that reverts*. So this bot flips both knobs the follow-bots had
wrong:

- **Direction is OPPOSITE the flow** — fade a call-led buying burst short, a
  put-led selling burst long.
- **Regime is POSITIVE gamma** — the mean-reverting regime (dealers sell
  rallies / buy dips), where the follow-bots stood *down*.

The trigger — and the differentiator from the retired VWAP-reversion / wall-fade
bots (which faded extension in positive γ and also failed) — is that the fade is
armed by a large, volume-confirmed **flow burst** that has *overshot* price (the
climax/exhaustion signal those bots lacked), not by extension alone. Target is
the mean (VWAP); stop is a *continuation* of the burst; a `_trend_veto` blocks
fading a sustained trend (only a fresh spike qualifies). Uses the same
`flow_recent_*` window fields. If it clears the gate it is the first promotion;
if not, it is shelved and the flow axis is closed.

### 6.3 Split flagship wall strategy — `call_wall_rejector` + `put_wall_bouncer` (implemented; screening)

The retired `PutCallWallBouncer` faded both walls from one context bot and was
**the worst loser in the fleet** — PF 0.53 *frictionless*, which the retirement
called out as proof the signal was dead, not just expensive. Its flaw: it faded
on mere PROXIMITY with a naked 0DTE ATM debit — it entered on the touch, treated
every wall the same, and could not tell a rejection from a break.

Split into two directional bots (so each side tunes/enables independently),
sharing one engine (`bots/wall_reversion.py`), with the three filters the
original lacked — the difference between "price touches a wall a lot" and "a
tradeable fade":

1. **Rejection confirmation** — price must TAG the wall and ROLL BACK (a recent
   extreme reached it and the close has retreated), not just sit near it.
2. **Wall-strength gate** — only fade a historically LARGE wall
   (`{call,put}_wall_strength_pctile`); dealers defend big walls, small ones
   break. Best-effort (skipped when the percentile history is absent).
3. **Flow no-pierce** — using the fresh windowed flow: if order flow is still
   piercing the wall (call-led buying up into the call wall / put-led selling
   down into the put wall), it is breaking — stand down.

Plus a defined-risk vertical toward max_pain / flip (far less 0DTE theta than
the original's single long option) and the original's good vol-scaled,
wick-confirmed wall-break stop (`wall_ref_side`). **Honest caveat:** splitting
alone changes nothing — a call-wall fade and a put-wall fade are the two halves
of the same signal that scored 0.53. The three filters are the bet; the backtest
decides. If they don't clear, the flagship stays retired for a documented,
data-backed reason, not a hunch.

### 6.4 Skew Snap Reversal (not yet built)

- `skew_delta` (OTM put−call IV differential) at a fear extreme while the tape
  is *not* breaking down → contrarian long. Distinct axis again (vol *skew*, not
  level/flow/time). Deferred so the candidate set stays small and each addition
  is screened on its own.

---

## 7. Backtest reality & calibration (2026-08-09)

The first screen (45d / 2,370 steps) opened **0 trades** on all four — with 0
vetoes and 0 fill failures, i.e. `open_criteria` returned `None` every tick.
The `tw_edge_field_probe` tool (`python -m src.tools.tw_edge_field_probe`) traced
it to two causes, and the bots were calibrated accordingly:

**Data availability over the window** (probe coverage):

| field | non-null | consequence |
|---|---|---|
| `close_charm_flow`, `dealer_vanna/charm_total`, `flow_net_premium/volume`, `flip_distance`, `convexity_risk`, `prior_net_gex` | ~100% | usable |
| **`pin_strike` / `pin_confidence`** | **2 / 59,682 (~0%)** | Charm bot re-pointed to a `max_pain` magnet (pin optional) |
| **`prior_vix`** (VIX bars: 781 over 45d, mostly recent) | **~1/8** | Vanna bot is VIX-history-gated; validate forward once VIX is dense |

**Over-strict gates** (fixed, justified by the probe's own values):

- **Gamma Regime Shift** — spot was 0.4–2.6% from the flip in every sample, so
  the old `|flip_distance| ≤ 0.3%` gate was unreachable, and the absolute
  `5e8` net-GEX-shed floor was noise for SPX. Now the trigger is the **crossing**
  itself (`prior_net_gex > 0` → `net_gex ≤ 0`), or a not-yet-completed collapse
  near the flip (≤ 1.2%) that shed ≥ 10% of `|prior_net_gex|` this tick — all
  RELATIVE, so it means the same for SPY and SPX. (The 25%-in-one-tick floor
  from the first calibration was still too rare — `shed_small` dominated the
  miss tally — so it was lowered and the crossing was made the primary path.)
- **Charm Close Magnet** — magnet falls back from the (empty) pin to `max_pain`.

**The entry-classification bug (the dominant blocker).** The second screen
surfaced it via the new `signals` / `entry_rejects` counters: Aggressor Flow
produced **~1,700 signals but 0 trades**, with no quote failures. The cause was
in `reconciler.open_position` (and mirrored in the backtest): it classified any
structure with a short leg as a *credit* structure and required
`-entry_price ≥ MIN_CREDIT_PER_SHARE`. But a defined-risk **debit spread** (the
structure all four v4 bots — and the retired `bull_momentum_climber` — use) has
`entry_price > 0`, so `-entry_price` is negative and **every debit spread was
rejected at entry, live and in backtest**. This almost certainly explains why
`bull_momentum_climber` "never fired at all" in the retirement screen. Fixed by
classifying on the **sign of the net** (debit if `entry_price > 0`, credit if
`< 0`, unfillable at `0`) in both `reconciler.open_position` and the harness.

**Instrumentation.** Every gate now records a reason via `BaseBot._skip(...)`,
surfaced per bot as `miss_reasons` in the backtest JSON. A 0-trade result now
names the gate that abstained every tick, so further tuning is data-driven, not
guesswork (this is the tradeworkz analog of the signals playbook's
`explain_miss`).

## 8. Screen results (2026-08-09, 45d / interval 5m / 1 contract)

The third screen (entry bug fixed) produced real trades and real verdicts:

| Bot | Trades | PF | Expectancy | Verdict |
|---|---:|---:|---:|---|
| aggressor_flow_divergence | 404 | **0.31** | −$36.63 | **no-edge → SCREENED OUT** |
| fresh_flow_momentum | 461 | **0.33** | −$34.68 | **no-edge → SCREENED OUT** |
| gamma_regime_shift_rider | 9 | 0.23 | −$30.97 | insufficient (weak early read) |
| charm_close_magnet | 8 | 1.01 | +$0.97 | insufficient (breakeven) |
| vanna_vol_crush_rider | 5 | 15.3 | +$109.89 | insufficient (VIX-gated, promising) |

**The "aggressor flow LEADS price" thesis is dead on 0DTE — two decisive
failures.** `fresh_flow_momentum` was built specifically to fix the lagging-
signal diagnosis (fresh windowed flow + acceleration instead of the cumulative),
and it produced the *same* result over 461 trades: 33% win rate, wins smaller
than losses, a monotonic bleed. That asymmetry is the tell — following an
aggressive flow burst on 0DTE systematically buys a local extreme that reverts.
The signal is real but its sign is backwards for a momentum trade. Any further
work on this axis should test the CONTRARIAN read (fade the flow extreme), which
is a NEW hypothesis screened from scratch — not a third "follow the flow"
variant. Both flow-following bots are shelved as standalone screened-out specs
(registered + backtestable for the record).

**Nothing is promoted — `DEFAULT_ROSTER` stays empty.** The honest reading:

- **aggressor_flow_divergence — decisive no-edge, shelved.** 404 trades is a
  real sample; PF 0.31 with wins *smaller* than losses at a 31% win rate is a
  directional-prediction failure, not a stop/target tuning issue — the
  cumulative day-to-date net premium is a lagging signal. Moved to a standalone
  screened-out spec (out of `CANDIDATE_SPECS`, kept backtestable). A "fresh-flow"
  redesign — keying on the per-bucket flow *delta* rather than the cumulative —
  would be a NEW hypothesis, screened from scratch, not a revival of this one.
- **charm / gamma — underpowered.** The option-chain history only reaches back
  to 2026-06-10, so ~45d is near the maximum window; these low-frequency bots
  (8 and 9 trades) simply cannot reach the 20-trade bar on available data. Their
  early reads (charm ≈ breakeven, gamma weak-negative) are not conclusions.
- **vanna — promising but unvalidated.** The 5 trades it could take (where VIX
  history existed) went 4-1 for +$549 (PF 15), but `no_vix_change` still gated
  ~70% of ticks. It validates forward once VIX bars are dense — not now.

The value delivered stands independently of these verdicts: the flow /
second-order / pin **data layer** on the snapshot, the **backtest instrumentation**
(`signals` / `entry_rejects` / `miss_reasons`), and the **debit-spread entry-gate
fix** (which unblocked every debit-vertical bot, including the retired
`bull_momentum_climber`). The screen did its job — it killed a losing thesis
with data instead of hope.

**Honest status of the gate.** With the entry-classification bug fixed,
`aggressor_flow_divergence` (which was already firing ~1,700 signals) and
`gamma_regime_shift_rider` (crossing trigger) should now produce real trades to
screen. `charm_close_magnet` is inherently low-frequency (positive-γ, final 2h,
flow/magnet sign-agreement) — it may need a longer window than 45 days to reach
≥ 20 trades; that is a sample-size limit, not a defect. `vanna_vol_crush_rider`
cannot be validated on a window without dense VIX history — a data gap, not a
thesis failure; it screens forward once VIX bars accumulate. No bot is promoted
until it clears PF ≥ 1.1 / positive expectancy / ≥ 20 trades on its own — none
goes live on this calibration alone.
