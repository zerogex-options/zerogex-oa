"""THE consolidated strategy catalog — single source of truth.

Every strategy ZeroGEX trades, backtests, or measures is defined exactly once,
here. Three surfaces read this module and nothing else:

* **TradeWorkz bots** (``src/tradeworkz/registry.py``) — resolves each entry's
  ``bot_class`` and layers ``bot_params`` over the catalog ``params``. A bot
  pulls the general strategy and tunes it to its own spec; it never redefines
  the strategy.
* **Backtesting** (``src/backtesting/meta.py``) — lists the catalog as the
  testable universe. Pattern-bound entries replay persisted Action Cards;
  bot-bound entries replay the bot's own entry rule over reconstructed
  snapshots (``src/backtesting/bot_replay.py``).
* **Pattern Insights** (``src/backtesting/queries.py``) — folds measured stats
  onto catalog ids, so one strategy reads as one row however it was measured.

Identity rules, which exist because renaming breaks foreign keys and history:

* ``id`` is canonical and permanent. Where a strategy has both engines, ``id``
  follows the **pattern** id, because that is the id already visible to
  customers in Backtesting and Pattern Insights.
* ``bot_id_legacy`` / ``pattern_id_legacy`` carry the ids history already
  wrote (``tw_trades.bot_id``, ``signal_action_cards.pattern``) when they
  differ from the canonical one. Nothing is ever renamed.

Stage rules: ``stage`` records *evidence*, not deployment. Only a VALIDATED
entry with a bot binding may take live capital. Everything else that is still
being worked on sits in RESEARCH — which is where most of the catalog lives,
because the 2026-08-09 fleet screen rested on 45 days of history and the
retirement bar is five years (see ``policy``).
"""

from __future__ import annotations

from datetime import date
from typing import Dict, Iterable, Optional, Tuple

from src.strategies.models import (
    Family,
    ResearchRun,
    Stage,
    StrategyEntry,
    Verdict,
)

# ── Shared evidence ────────────────────────────────────────────────────
# The fleet-wide screen that shelved every shipped bot. One run object per
# strategy (trade counts differ per bot), but they share window and verdict:
# 45 days, 2,065 trades across the fleet, best profit factor 0.78, four bots
# that never fired at all. Decisive for a 45-day window — and 45 days is 2.5%
# of what the retirement policy asks for, which is exactly why every one of
# these strategies is RESEARCH rather than RETIRED.
_FLEET_SCREEN_DATE = date(2026, 8, 9)
_FLEET_SCREEN_WINDOW_DAYS = 45


def _fleet_screen(
    trades: int,
    profit_factor: Optional[float] = None,
    *,
    verdict: Verdict = Verdict.NO_EDGE,
    expectancy: Optional[float] = None,
    win_rate: Optional[float] = None,
    notes: str = "",
) -> ResearchRun:
    """One bot's slice of the 2026-08-09 fleet-wide shelving screen."""
    return ResearchRun(
        ran_on=_FLEET_SCREEN_DATE,
        window_days=_FLEET_SCREEN_WINDOW_DAYS,
        trades=trades,
        verdict=verdict,
        profit_factor=profit_factor,
        expectancy=expectancy,
        win_rate=win_rate,
        harness="tradeworkz-backtest",
        tuning_generation=0,
        symbols=("SPY", "QQQ", "SPX"),
        notes=notes or "2026-08-09 fleet-wide screen; best PF in the fleet was 0.78.",
    )


# =======================================================================
# Wall structure
# =======================================================================

_WALL_FILTER_PARAMS = {
    "wall_proximity_pct": 0.002,
    "wall_reject_margin_pct": 0.0007,
    "min_wall_strength_pctile": 50.0,
    "pierce_premium": 6.0e5,
    "max_hold_minutes": 90,
    "dte_target": 0,
}

_WALL: Tuple[StrategyEntry, ...] = (
    StrategyEntry(
        id="call_wall_fade",
        name="Fade Touches of the Call Wall",
        family=Family.WALL,
        tier="0DTE",
        direction_mode="bearish",
        tagline="Confirmed rejection at a big call wall. Fade the rally.",
        thesis=(
            "In a long-gamma backdrop the call wall is where dealer hedging "
            "opposes further upside: price tagging it with flow turning "
            "negative should reject rather than break. Traded as a fade of a "
            "confirmed rejection, filtered on wall strength and on flow not "
            "piercing the level."
        ),
        stage=Stage.RESEARCH,
        bot_class="CallWallRejector",
        bot_id_legacy="call_wall_rejector",
        has_pattern=True,
        params=dict(_WALL_FILTER_PARAMS),
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 10),
                window_days=45,
                trades=33,
                verdict=Verdict.NO_EDGE,
                profit_factor=0.193,
                win_rate=0.15,
                tuning_generation=1,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "Rejection-confirmation + wall-strength + flow-no-pierce filters cut "
                    "frequency hard without creating edge. Structural diagnosis: a DEBIT "
                    "vertical needs price to move to a target, but 'the wall holds' is a "
                    "boundary, not a target — the structure to test next is a CREDIT "
                    "spread beyond the wall. The bearish leg was also fighting the "
                    "window's up-drift."
                ),
            ),
            ResearchRun(
                ran_on=date(2026, 9, 1),
                window_days=60,
                trades=0,
                verdict=Verdict.UNDERPOWERED,
                harness="playbook-calibration",
                tuning_generation=1,
                symbols=("SPY",),
                notes=(
                    "Pattern-side realized-P&L calibration: positive on SPY but on a "
                    "sample too small to promote. Watched, not promoted."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="put_wall_bounce",
        name="Bounce Off the Put Wall",
        family=Family.WALL,
        tier="0DTE",
        direction_mode="bullish",
        tagline="Confirmed bounce off a big put wall. Fade the dip.",
        thesis=(
            "The symmetric mirror of the call-wall fade: a long-gamma backdrop "
            "plus price tagging the put wall plus bullish flow should produce a "
            "defended bounce, because dealer hedging there buys into weakness."
        ),
        stage=Stage.RESEARCH,
        bot_class="PutWallBouncer",
        bot_id_legacy="put_wall_bouncer",
        has_pattern=True,
        params=dict(_WALL_FILTER_PARAMS),
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 10),
                window_days=45,
                trades=29,
                verdict=Verdict.NO_EDGE,
                profit_factor=0.577,
                tuning_generation=1,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "Materially better than the bearish call-wall leg (PF 0.58 vs 0.19) "
                    "but still losing. Same structural diagnosis: a boundary thesis wants "
                    "a credit structure, not a debit that needs a move."
                ),
            ),
            ResearchRun(
                ran_on=date(2026, 9, 1),
                window_days=60,
                trades=19,
                verdict=Verdict.INSUFFICIENT,
                harness="playbook-calibration",
                tuning_generation=1,
                symbols=("SPY", "QQQ"),
                notes=(
                    "Pattern-side realized-P&L calibration looks excellent but samples "
                    "are thin (SPY n=14, QQQ n=5). Promising — needs sample, not tuning."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="put_call_wall_bouncer",
        provisioned_history=True,
        name="Put/Call Wall Bouncer (combined)",
        family=Family.WALL,
        tier="0DTE",
        direction_mode="context",
        tagline="Fade the wall. Ride the mean reversion. Cut if the wall breaks.",
        thesis=(
            "The original single-bot wall fade, taking either side from one "
            "rule set. Split into the two directional strategies above so each "
            "side could carry its own filters and be measured independently — "
            "the combined form masked that the bearish and bullish legs behave "
            "very differently."
        ),
        stage=Stage.SUPERSEDED,
        superseded_by="call_wall_fade",
        bot_class="PutCallWallBouncer",
        params={"wall_proximity_pct": 0.002, "wall_break_pct": 0.003, "max_hold_minutes": 90},
        research=(
            _fleet_screen(
                trades=218,
                profit_factor=0.53,
                notes=(
                    "First of three wall-fade failures. Directionally agnostic entry "
                    "hid that the two sides diverge; superseded by the split pair."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="put_wall_magnet_reversal",
        name="Put Wall Magnet Reversal",
        family=Family.WALL,
        tier="0DTE",
        direction_mode="bullish",
        tagline="Negative-γ knife into a massive put wall. Fade the magnet, cut fast on a break.",
        thesis=(
            "Distinct from the defended-wall fades: in a NEGATIVE-gamma regime "
            "a historically large put wall acts as a max-pain / liquidity-node "
            "magnet rather than a dealer-defended level. Fade into it with a "
            "defined-risk call debit spread, sized up with wall size, stopped "
            "tight because a break in negative gamma cascades."
        ),
        stage=Stage.RESEARCH,
        bot_class="PutWallMagnetReversal",
        params={"min_put_wall_pctile": 90.0},
        research=(
            ResearchRun(
                ran_on=date(2026, 7, 16),
                window_days=90,
                trades=0,
                verdict=Verdict.NO_EDGE,
                harness="thesis-backtest",
                tuning_generation=0,
                symbols=("SPY", "QQQ"),
                notes=(
                    "Thesis backtest (src/tools/put_wall_magnet_backtest.py) did not "
                    "support 'bigger wall bounces better': on SPY expectancy INVERTED "
                    "with wall size (90-95th pct +0.30R vs 99-100th -0.15R), ~breakeven "
                    "overall; QQQ noisy and non-monotone. And that is before 0DTE theta "
                    "on a ~65%-timeout distribution. Needs a theta-aware spread-P&L "
                    "backtest before any revival."
                ),
            ),
        ),
    ),
)


# =======================================================================
# Gamma flip
# =======================================================================

_GAMMA_FLIP: Tuple[StrategyEntry, ...] = (
    StrategyEntry(
        id="gamma_flip_bounce",
        provisioned_history=True,
        name="Bounce Off the Gamma Flip",
        family=Family.GAMMA_FLIP,
        tier="0DTE",
        direction_mode="context",
        tagline="Sell rallies into the flip. Buy dips into the flip. Positive-γ only.",
        thesis=(
            "Inside a positive-gamma regime the gamma flip acts as "
            "support/resistance that dealers defend: price tagging it from one "
            "side and rejecting back is the tradable event. The mirror of the "
            "flip BREAK — this one trades the defense, not the transition."
        ),
        stage=Stage.RESEARCH,
        bot_class="GammaFlipDefender",
        bot_id_legacy="gamma_flip_defender",
        has_pattern=True,
        params={"flip_proximity_pct": 0.0015, "flip_break_pct": 0.003, "max_hold_minutes": 60},
        research=(_fleet_screen(trades=142, profit_factor=0.61),),
    ),
    StrategyEntry(
        id="gamma_flip_break",
        provisioned_history=True,
        name="Trade Through the Gamma Flip",
        family=Family.GAMMA_FLIP,
        tier="0DTE",
        direction_mode="context",
        tagline="Ride the breakout. Dealer hedging turns momentum into a wave.",
        thesis=(
            "Crossing the gamma flip moves dealers from suppressing moves "
            "(above flip, long gamma) to amplifying them (below flip, short "
            "gamma). The cross direction IS the trade direction."
        ),
        stage=Stage.RESEARCH,
        bot_class="GammaFlipBreaker",
        bot_id_legacy="gamma_flip_breaker",
        has_pattern=True,
        params={"cross_min_pct": 0.0015, "flip_reentry_pct": 0.0008, "max_hold_minutes": 60},
        research=(
            _fleet_screen(
                trades=187,
                profit_factor=0.78,
                notes=(
                    "Best profit factor in the whole 2026-08-09 fleet screen at 0.78 — "
                    "still losing, but the least-bad first-order level strategy, and the "
                    "one the regime-transition successor was built from."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="gamma_regime_shift_rider",
        name="Gamma Regime Shift Rider",
        family=Family.GAMMA_FLIP,
        tier="0DTE",
        direction_mode="context",
        tagline="Dealer gamma is flipping short at the flip. Ride the regime break.",
        thesis=(
            "Trades the long-to-short gamma TRANSITION rather than a static "
            "level break: net GEX collapsing tick over tick with spot at the "
            "flip and convexity elevated. Flow-confirmed; stop is a reclaim of "
            "the flip. The velocity read the static-level strategies lacked."
        ),
        stage=Stage.CANDIDATE,
        bot_class="GammaRegimeShiftRider",
        supersedes=("gamma_flip_break", "dealer_delta_pressure"),
        params={
            "min_shed_frac": 0.10,
            "max_flip_distance_pct": 0.012,
            "min_break_trend_pct": 0.0010,
            "target_pct": 0.006,
            "max_hold_minutes": 60,
            "dte_target": 0,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 15),
                window_days=60,
                trades=7,
                verdict=Verdict.UNDERPOWERED,
                tuning_generation=0,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "Underpowered on available history: the tick-over-tick shed gate "
                    "needs a denser snapshot cadence than the replay interval provides. "
                    "Not a verdict on the thesis."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="dual_flip_dislocation",
        name="Dual-Flip Dislocation",
        family=Family.GAMMA_FLIP,
        tier="0DTE",
        direction_mode="context",
        tagline="Two flip conventions disagree. Ride the fast book across the band.",
        thesis=(
            "Fires on a fresh, momentum-confirmed entry into the band between "
            "gamma_flip_raw (where the un-DTE-weighted 0DTE fast book flips) "
            "and the structural DTE-weighted flip — the state where intraday "
            "hedgers are short gamma while single-flip dashboards still read "
            "positive. Targets the band's far edge; stops on a re-cross."
        ),
        stage=Stage.CANDIDATE,
        bot_class="DualFlipDislocation",
        params={
            "min_band_pct": 0.002,
            "min_momentum_pct": 0.0005,
            "cross_lookback_bars": 6,
            "stop_buffer_pct": 0.001,
            "band_collapse_pct": 0.001,
            "max_hold_minutes": 75,
            "dte_target": 0,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 15),
                window_days=60,
                trades=2,
                verdict=Verdict.UNDERPOWERED,
                tuning_generation=0,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "Harness bug, since fixed: the fresh-cross test compared against the "
                    "last 1-minute close while the replay evaluates every ~5 minutes, so "
                    "a cross older than a minute never registered (outside_band 1455 / "
                    "no_fresh_cross 115). Cross detection now spans N one-minute closes; "
                    "needs a re-screen."
                ),
            ),
        ),
    ),
)


# =======================================================================
# Pin & max pain
# =======================================================================

_PIN: Tuple[StrategyEntry, ...] = (
    StrategyEntry(
        id="eod_pressure_drift",
        provisioned_history=True,
        name="Last-Hour Hedging Drift",
        family=Family.PIN,
        tier="0DTE",
        direction_mode="context",
        tagline="Ride the last-hour pin. Dealers push spot toward max-pain.",
        thesis=(
            "In the last hour of regular trading dealer 0DTE hedging dominates "
            "the tape, and the aggregate directional push is measurable. Lean "
            "into it with an ATM 0DTE debit, anchored to VWAP for target and "
            "invalidation."
        ),
        stage=Stage.RESEARCH,
        bot_class="EodPinDrifter",
        bot_id_legacy="eod_pin_drifter",
        has_pattern=True,
        params={"min_drift_pct": 0.001, "max_hold_minutes": 60},
        research=(
            _fleet_screen(
                trades=163,
                profit_factor=0.55,
                notes=(
                    "Entered on displacement from max_pain alone — no measurement of "
                    "whether dealers actually had to trade toward the pin. The forced-flow "
                    "successor (charm_close_magnet) adds exactly that test."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="max_pain_gravitation",
        provisioned_history=True,
        name="Drift Back to Max Pain",
        family=Family.PIN,
        tier="1DTE",
        direction_mode="context",
        tagline="Distant max_pain in a pin regime. Ride the drift back.",
        thesis=(
            "When the underlying is materially displaced from max pain and "
            "gamma is positive, open interest concentration exerts a pull back "
            "toward the pin over a 1-2 day horizon."
        ),
        stage=Stage.RESEARCH,
        bot_class="MaxPainGravitator",
        bot_id_legacy="max_pain_gravitator",
        has_pattern=True,
        params={"min_drift_pct": 0.004, "max_hold_minutes": 1440},
        research=(
            _fleet_screen(
                trades=71,
                profit_factor=0.49,
                notes=(
                    "OI-based max_pain is a weaker magnet than the gamma-based, "
                    "reachability-filtered Pin Strike the forced-flow tier uses."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="pin_risk_premium_sell",
        name="Sell Premium into Overnight Pin",
        family=Family.PIN,
        tier="1DTE",
        direction_mode="neutral",
        tagline="Strong pin into the close. Sell the overnight premium.",
        thesis=(
            "A confident pin with a compressed expected range means overnight "
            "option premium is rich relative to the move the pin implies. Sell "
            "it as a defined-risk credit structure."
        ),
        stage=Stage.RESEARCH,
        has_pattern=True,
        params={},
        research=(),
    ),
)


# =======================================================================
# Forced dealer flow (charm / vanna / settlement)
# =======================================================================

_FORCED_FLOW: Tuple[StrategyEntry, ...] = (
    StrategyEntry(
        id="charm_close_magnet",
        name="Charm Close Magnet",
        family=Family.FORCED_FLOW,
        tier="0DTE",
        direction_mode="context",
        tagline="Ride the forced charm flow into the pin. Quantified, not folklore.",
        thesis=(
            "Final-window drift toward the gamma-restoring Pin Strike, but "
            "only when the modeled close_charm_flow — the dollars dealers must "
            "trade by the close — actually points at the pin. Positive-γ, "
            "confident pin, defined-risk vertical. The quantified version of "
            "the drift the EOD strategies took on faith."
        ),
        stage=Stage.CANDIDATE,
        bot_class="CharmCloseMagnet",
        supersedes=("eod_pressure_drift", "max_pain_gravitation"),
        params={
            "max_minutes_to_close": 120,
            "min_pin_confidence": 0.55,
            "min_drift_pct": 0.001,
            "max_drift_pct": 0.010,
            "max_hold_minutes": 90,
            "dte_target": 0,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 15),
                window_days=60,
                trades=11,
                verdict=Verdict.INSUFFICIENT,
                profit_factor=1.31,
                tuning_generation=0,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "Encouraging but underpowered — the charm-flow gates are narrow and "
                    "available chain history caps the sample. Needs deeper history rather "
                    "than looser gates."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="settlement_flow_snap",
        name="Settlement Residual Snap",
        family=Family.FORCED_FLOW,
        tier="0DTE",
        direction_mode="context",
        tagline="Ride the pure settlement-unwind hedge into the close.",
        thesis=(
            "Trades the raw-minus-smooth close_charm_flow residual — the "
            "dollars of the by-close dealer hedge that come from 0DTE strikes "
            "resolving to intrinsic, not from smooth charm drift. Fires only "
            "when that settlement leg dominates the drift (keeping it disjoint "
            "from the charm magnet) and rivals the local gamma pool in size."
        ),
        stage=Stage.CANDIDATE,
        bot_class="SettlementFlowSnap",
        params={
            "min_minutes_to_close": 30,
            "max_minutes_to_close": 90,
            "min_residual_local_gex_frac": 0.10,
            "min_residual_dominance": 1.5,
            "target_pct": 0.003,
            "stop_pct": 0.002,
            "max_premium_loss_pct": 0.45,
            "dte_target": 0,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 15),
                window_days=60,
                trades=1,
                verdict=Verdict.UNDERPOWERED,
                tuning_generation=0,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "The 0.25 residual/local_gex prior blocked 1,607 in-window ticks "
                    "(miss_reason=residual_small) and let one trade through in 60d. The "
                    "residual is real but runs smaller relative to local_gex than assumed; "
                    "prior relaxed to 0.10 with the dominance gate kept as the qualitative "
                    "filter. Awaiting re-screen."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="vanna_vol_crush_rider",
        name="Vanna Vol-Crush Rider",
        family=Family.FORCED_FLOW,
        tier="0DTE",
        direction_mode="context",
        tagline="Vol is moving. Ride the vanna hedging flow it forces on dealers.",
        thesis=(
            "Trades the sign and size of dealer_vanna_total x ΔVIX: a "
            "short-vanna book into a vol crush must buy (the melt-up), and the "
            "mirror holds for a vol spike. Uses the vol CHANGE and the book's "
            "vanna exposure, not the VIX level."
        ),
        stage=Stage.CANDIDATE,
        bot_class="VannaVolCrushRider",
        supersedes=("vix_regime_breakout",),
        params={
            "min_vix_change": 0.30,
            "min_dealer_vanna": 4.0e7,
            "target_pct": 0.004,
            "stop_pct": 0.003,
            "max_hold_minutes": 60,
            "dte_target": 0,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 15),
                window_days=60,
                trades=4,
                verdict=Verdict.UNDERPOWERED,
                tuning_generation=0,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "Gated by VIX history depth: vix_bars retention (30d) is shorter than "
                    "the screen window, so most ticks had no computable ΔVIX. Raising "
                    "VIX_BARS_RETENTION_DAYS is a precondition for screening this at all."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="vanna_charm_glide",
        name="End-of-Week Hedging Drift",
        family=Family.FORCED_FLOW,
        tier="swing",
        direction_mode="context",
        tagline="Weekly charm and vanna decay glide price through the week's end.",
        thesis=(
            "Second-order dealer flow over a multi-day horizon: charm and "
            "vanna decay on the weekly book produce a persistent, "
            "direction-stable hedging drift into Friday's expiry."
        ),
        stage=Stage.RESEARCH,
        has_pattern=True,
        params={},
        research=(),
    ),
    StrategyEntry(
        id="weekly_charm_grind",
        name="Weekly Charm Grind",
        family=Family.FORCED_FLOW,
        tier="1DTE",
        direction_mode="context",
        tagline="Quiet positive-gamma midday: weekly-book time decay is the only flow.",
        thesis=(
            "Rides the deterministic delta-decay rebalance of the 1-7 DTE "
            "weekly dealer book through the 11:00-14:00 lull, gated on the "
            "weekly charm bucket dominating the 0DTE bucket and a compressed "
            "session. 1DTE debit vertical — half the theta of 0DTE over the "
            "hold — hard exit 15:00 ET, never overnight."
        ),
        stage=Stage.CANDIDATE,
        bot_class="WeeklyCharmGrind",
        params={
            "min_bucket_dominance": 1.5,
            "min_flow_local_gex_frac": 0.08,
            "max_session_range_pct": 0.008,
            "quality_flow_saturation": 0.15,
            "target_pct": 0.0035,
            "stop_pct": 0.0025,
            "scale_out_enabled": False,
            "dte_target": 1,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 15),
                window_days=60,
                trades=0,
                verdict=Verdict.UNDERPOWERED,
                tuning_generation=0,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "Zero trades: only 3 ticks in 60d survived the gate stack and all 3 "
                    "died at conviction (quality saturated above the entry floor). The "
                    "intersection of range<=0.6% AND dominance>=2x AND flow>=0.10*local_gex "
                    "was near-empty (range_wide 2100 / odte_dominates 347 / flow_small 41). "
                    "Each gate relaxed one notch and the quality scale matched to the entry "
                    "floor; awaiting re-screen."
                ),
            ),
        ),
    ),
)


# =======================================================================
# Aggressor order flow
# =======================================================================
# FAMILY FINDING (2026-08-15): four independent formulations of "trade the
# DIRECTION of recent option flow on 0DTE" have failed with the same
# signature — PF 0.31 / 0.33 / 0.32, win rates 22-33%, and wins SMALLER than
# losses, which is a directional-prediction failure rather than a stop/target
# tuning problem. Cumulative premium, windowed premium + acceleration, and
# delta-weighted hedge obligation both behind and ahead of the tape all lost.
# The follow-the-flow read is closed pending deeper history; the CONTRARIAN
# read (fade the climax) is the open hypothesis. Note all four screens ran on
# 45-60 day windows — decisive for that window, and ~3% of what the retirement
# policy requires, which is why none of these is RETIRED.

_ORDER_FLOW: Tuple[StrategyEntry, ...] = (
    StrategyEntry(
        id="aggressor_flow_divergence",
        name="Aggressor Flow Divergence",
        family=Family.ORDER_FLOW,
        tier="0DTE",
        direction_mode="context",
        tagline="Aggressive option flow leans hard; price hasn't caught up. Lead it.",
        thesis=(
            "Led price with aggressor-classified net option premium "
            "accumulated day-to-date, on the theory that informed flow front-"
            "runs the move. Diagnosis after the screen: a cumulative signal is "
            "lagging by construction."
        ),
        stage=Stage.SUPERSEDED,
        superseded_by="fresh_flow_momentum",
        bot_class="AggressorFlowDivergence",
        params={
            "min_net_premium": 5.0e5,
            "max_price_move_pct": 0.0025,
            "target_pct": 0.004,
            "stop_pct": 0.003,
            "max_hold_minutes": 60,
            "dte_target": 0,
        },
        research=(
            _fleet_screen(
                trades=404,
                profit_factor=0.31,
                expectancy=-36.63,
                win_rate=0.31,
                notes=(
                    "Decisive no-edge on a 45-day window: monotonic bleed to -$14.8K with "
                    "wins SMALLER than losses at a 31% win rate — a directional-prediction "
                    "failure, not a tuning issue. The day-to-date cumulative premium it "
                    "keyed on is lagging. Do not revive this exact design."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="fresh_flow_momentum",
        name="Fresh Flow Momentum",
        family=Family.ORDER_FLOW,
        tier="0DTE",
        direction_mode="context",
        tagline="Fresh aggressor-flow burst leads price. Ride the pulse.",
        thesis=(
            "The fresh-flow successor to the cumulative read: keys on the "
            "recent WINDOWED flow plus acceleration rather than day-to-date "
            "totals, precisely to fix the lagging-signal diagnosis."
        ),
        stage=Stage.SUPERSEDED,
        superseded_by="hedge_impulse_quiet_tape",
        bot_class="FreshFlowMomentum",
        supersedes=("aggressor_flow_divergence",),
        params={
            "min_recent_premium": 2.0e5,
            "accel_mult": 1.15,
            "max_price_move_pct": 0.004,
            "target_pct": 0.004,
            "stop_pct": 0.003,
            "max_hold_minutes": 45,
            "dte_target": 0,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 10),
                window_days=45,
                trades=461,
                verdict=Verdict.NO_EDGE,
                profit_factor=0.331,
                expectancy=-34.68,
                win_rate=0.33,
                tuning_generation=0,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "Fixing the lagging signal did not help — same bleed, same signature "
                    "as its predecessor. Two decisive failures settled 'aggressor flow "
                    "LEADS price' on 0DTE: following the burst systematically buys a local "
                    "extreme that reverts."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="hedge_impulse_quiet_tape",
        name="Quiet-Tape Hedge Impulse",
        family=Family.ORDER_FLOW,
        tier="0DTE",
        direction_mode="context",
        tagline="A hedge obligation is staged and the tape hasn't moved. Front-run it.",
        thesis=(
            "The strongest formulation the flow-direction axis got: weights "
            "flow by per-contract DELTA (the hedge OBLIGATION, not premium "
            "sentiment), requires a FLAT tape so the entry is ahead of the "
            "hedge rather than chasing it, conditions on NEGATIVE gamma for "
            "amplification, and requires sign persistence."
        ),
        stage=Stage.RESEARCH,
        bot_class="HedgeImpulseQuietTape",
        supersedes=("fresh_flow_momentum",),
        params={
            "min_impulse_ratio": 0.08,
            "max_flat_move_pct": 0.0015,
            "flip_exit_ratio": 0.04,
            "no_move_expiry_minutes": 45,
            "target_pct": 0.004,
            "stop_pct": 0.003,
            "max_hold_minutes": 75,
            "scale_out_enabled": False,
            "dte_target": 0,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 15),
                window_days=60,
                trades=383,
                verdict=Verdict.NO_EDGE,
                profit_factor=0.32,
                expectancy=-11.66,
                win_rate=0.22,
                tuning_generation=0,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "Fourth and strongest flow-direction formulation; failed with the same "
                    "signature as the first three. Closes the follow-the-flow read for this "
                    "cadence and these underlyings on the history available. Do not build a "
                    "fifth variant — re-screen the axis when deep history lands."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="climax_flow_fade",
        name="Climax Flow Fade",
        family=Family.ORDER_FLOW,
        tier="0DTE",
        direction_mode="context",
        tagline="Aggressive flow burst spiked price into a pin. Fade the climax.",
        thesis=(
            "The CONTRARIAN read of the four screened-out follow-the-flow "
            "strategies: their ~33% win rate with wins < losses showed that "
            "chasing a 0DTE flow burst buys a local extreme that reverts. This "
            "FADES a large, volume-confirmed fresh flow burst that has "
            "overshot price, in a positive-γ (mean-reverting) regime. The flow "
            "burst is the exhaustion trigger the VWAP-reversion strategies "
            "lacked."
        ),
        stage=Stage.CANDIDATE,
        bot_class="ClimaxFlowFade",
        params={
            "min_recent_premium": 3.0e5,
            "min_extension_pct": 0.0015,
            "target_pct": 0.003,
            "stop_pct": 0.004,
            "max_hold_minutes": 45,
            "dte_target": 0,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 17),
                window_days=60,
                trades=14,
                verdict=Verdict.INSUFFICIENT,
                profit_factor=1.18,
                tuning_generation=0,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "First read of the contrarian hypothesis is mildly positive on a sample "
                    "below the promotion bar. The one open question left on this data axis."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="put_capitulation_credit_fade",
        name="Put Capitulation Credit Fade",
        family=Family.ORDER_FLOW,
        tier="0DTE",
        direction_mode="context",
        tagline="Panic put buying into a full-strength long-gamma book. Sell it the IV.",
        thesis=(
            "When a put-only aggressor burst (3x session baseline, "
            "put-dominant) hits a top-quartile positive-gamma book with spot "
            "still above the put wall, sell a 0DTE put credit vertical below "
            "the dip low — short the IV the capitulators spiked, long the "
            "mechanical dealer dip-buy. The first credit-exit strategy in the "
            "catalog; the put wall is only the invalidation floor, never the "
            "trigger."
        ),
        stage=Stage.CANDIDATE,
        bot_class="PutCapitulationCreditFade",
        params={
            "min_burst_multiple": 3.0,
            "min_put_dominance": 0.65,
            "min_displacement_pct": 0.0020,
            "displacement_sigma_mult": 2.0,
            "displacement_fallback_pct": 0.0035,
            "displacement_bars": 25,
            "min_wall_room_pct": 0.0025,
            "short_strike_offset_pct": 0.0035,
            "credit_take_frac": 0.55,
            "credit_stop_mult": 1.75,
            "max_hold_minutes": 120,
            "dte_target": 0,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 15),
                window_days=60,
                trades=0,
                verdict=Verdict.INVALID,
                tuning_generation=0,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "Harness bug: the 30-bar displacement window needed 31 closes but "
                    "build_snapshot fetches only 30, so all 1,300 regime-passing ticks died "
                    "at no_history and the strategy could never fire. Window cut to 25 bars."
                ),
            ),
            ResearchRun(
                ran_on=date(2026, 8, 17),
                window_days=60,
                trades=4,
                verdict=Verdict.UNDERPOWERED,
                tuning_generation=1,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "With no_history fixed, 958 of 962 regime-passing ticks died at no_dip — "
                    "the strong positive-gamma regime this strategy REQUIRES suppresses fixed "
                    "0.35% dips, so trigger and absorber could ~never co-occur. Dip is now "
                    "vol-relative (sigma_mult x realized 1-min sigma x sqrt(bars)) with a "
                    "noise floor; awaiting re-screen."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="zero_dte_imbalance_drift",
        name="Smart-Money 0DTE Bias",
        family=Family.ORDER_FLOW,
        tier="0DTE",
        direction_mode="context",
        tagline="0DTE positioning imbalance points one way. Drift with it.",
        thesis=(
            "Aggregate 0DTE call/put positioning imbalance, read as a "
            "smart-money directional bias, produces an intraday drift in the "
            "imbalance's direction."
        ),
        stage=Stage.RESEARCH,
        has_pattern=True,
        params={},
        research=(),
    ),
)


# =======================================================================
# Volatility regime
# =======================================================================

_VOL_REGIME: Tuple[StrategyEntry, ...] = (
    StrategyEntry(
        id="vix_regime_breakout",
        provisioned_history=True,
        name="Volatility-Expansion Breakout",
        family=Family.VOL_REGIME,
        tier="0DTE",
        direction_mode="context",
        tagline="Vol is expanding. Ride the trend before the wall catches it.",
        thesis=(
            "When VIX is elevated and net gamma is negative (short-gamma, "
            "amplifying), range breaks tend to run because dealers hedge "
            "pro-trend. Stands down when volatility is calm or gamma is "
            "positive, where breaks fail."
        ),
        stage=Stage.RESEARCH,
        bot_class="VixRegimeBreakout",
        has_pattern=True,
        # Catalog default takes the PATTERN's VIX floor (18.0), the
        # better-evidenced of the two.
        params={"min_vix": 18.0, "min_move_pct": 0.003, "max_hold_minutes": 60},
        # The bot and the pattern disagreed on the VIX floor for no recorded
        # reason (bot 16.0, pattern PLAYBOOK_VIX_BREAKOUT_MIN 18.0). The
        # catalog default is the pattern's, which is the better-evidenced of
        # the two; the bot's own historical setting is preserved as an explicit
        # override so its measured record stays interpretable.
        bot_params={"min_vix": 16.0},
        research=(
            _fleet_screen(
                trades=96,
                profit_factor=0.58,
                notes=(
                    "Keyed on the VIX LEVEL only — never the vol CHANGE, and never the "
                    "book's vanna exposure that determines which way a vol move forces "
                    "dealers. The vanna successor adds both."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="squeeze_breakout",
        name="Vol-Compression Resolves",
        family=Family.VOL_REGIME,
        tier="swing",
        direction_mode="context",
        tagline="Compression resolves. Take the side it breaks.",
        thesis=(
            "Sustained volatility compression resolves directionally, and the "
            "dealer book's gamma distribution biases which way — traded over a "
            "multi-day horizon once the break confirms."
        ),
        stage=Stage.RESEARCH,
        has_pattern=True,
        params={},
        research=(),
    ),
    StrategyEntry(
        id="vol_expansion_straddle",
        provisioned_history=True,
        name="Vol Expansion Straddle",
        family=Family.VOL_REGIME,
        tier="0DTE",
        direction_mode="neutral",
        tagline="Compressed vol + tight range + short γ. Buy the straddle.",
        thesis=(
            "Buys an ATM straddle when VIX is compressed, the session has "
            "ranged tightly, and gamma is negative — the setup where a "
            "breakout in either direction reinforces via dealer hedging. "
            "Neutral-vega, so it rebalances a fleet otherwise short delta."
        ),
        stage=Stage.RESEARCH,
        bot_class="VolExpansionStraddle",
        params={
            "max_vix": 16.0,
            "max_range_pct": 0.006,
            "max_hold_minutes": 120,
            "dte_target": 0,
        },
        research=(
            _fleet_screen(
                trades=38,
                profit_factor=0.44,
                notes=(
                    "Long two legs of 0DTE theta: the straddle needed a large move just to "
                    "cover decay, and the compressed-vol entry condition selects sessions "
                    "least likely to deliver one."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="range_iron_condor",
        provisioned_history=True,
        name="Range Iron Condor",
        family=Family.VOL_REGIME,
        tier="0DTE",
        direction_mode="neutral",
        tagline="Positive-γ pin, low VIX. Sell the condor between the walls.",
        thesis=(
            "Sells a symmetric iron condor when spot sits between the put and "
            "call walls in positive gamma with subdued VIX. Non-directional "
            "theta capture with wing-capped downside — the one credit "
            "structure in the shipped fleet."
        ),
        stage=Stage.RESEARCH,
        bot_class="RangeIronCondor",
        params={
            "max_vix": 18.0,
            "min_wall_span_pct": 0.008,
            "wing_width": 2,
            "short_buffer": 2,
            "max_hold_minutes": 180,
            "dte_target": 0,
        },
        research=(
            _fleet_screen(
                trades=52,
                profit_factor=0.67,
                notes=(
                    "Sold premium at the walls and still lost — a caution for the credit-"
                    "structure hypothesis the wall-fade diagnosis points at. Worth "
                    "re-testing with the wall-strength and rejection filters the later wall "
                    "strategies added, which this one never had."
                ),
            ),
        ),
    ),
)


# =======================================================================
# Trend & momentum
# =======================================================================

_TREND: Tuple[StrategyEntry, ...] = (
    StrategyEntry(
        id="gex_gradient_trend",
        name="Asymmetric Gamma Drift",
        family=Family.TREND,
        tier="swing",
        direction_mode="context",
        tagline="Gamma is lopsided. Drift toward the thin side.",
        thesis=(
            "Asymmetric dealer gamma above versus below spot creates a "
            "multi-day drift toward the lower-gamma direction, where there is "
            "less hedging resistance to absorb the move."
        ),
        stage=Stage.VALIDATED,
        bot_class="GexGradientDrift",
        has_pattern=True,
        # Catalog defaults are the pattern's own calibrated thresholds (its
        # PLAYBOOK_GGT_* env defaults), so bot and pattern gate identically.
        params={
            "min_gradient_score": 40.0,
            "min_volatility_regime": -0.5,
            "otm_atr_mult": 0.5,
            "target_atr_mult": 1.5,
            "dte_target": 5,
            "max_hold_minutes": 3 * 24 * 60,
            "gradient_exit_score": 20.0,
            "vol_expansion_penalty": 0.10,
        },
        # Live-engine specifics the Action Card had no need for. See the bot's
        # module docstring for why each one exists.
        bot_params={
            # The pattern enters at_close; without a window a 5-second tick
            # loop would open a multi-day drift trade at 09:35.
            "max_minutes_to_close": 30,
            # The pattern's stop is a signal event, so the premium half of
            # "gradient_decay_below_20_or_-50pct_premium" lives here.
            "max_premium_loss_pct": 0.50,
            # Mirrors the pattern's pattern_base = 0.50.
            "confidence_base_default": 0.50,
            # Mirrors the pattern's size_multiplier = 0.5 on every card.
            "static_size_multiplier": 0.5,
            # Conviction scale. The engine refuses to open below
            # confidence_threshold and the pattern has no such gate, so these
            # saturations are set at gradient / vol readings that actually
            # print — otherwise every gate-passing setup dies at conviction
            # and the screen returns zero trades (see weekly_charm_grind).
            "quality_gradient_saturation": 70.0,
            "quality_vol_saturation": 0.5,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 9, 1),
                window_days=60,
                trades=33,
                verdict=Verdict.EDGE,
                profit_factor=2.80,
                expectancy=2411.0,
                win_rate=0.64,
                harness="playbook-calibration",
                tuning_generation=0,
                symbols=("QQQ",),
                notes=(
                    "The one conclusive realized-P&L edge in the catalog, on the "
                    "standardized single-long calibration backtest net of fills, slippage "
                    "and commission. This is PATTERN-side evidence: it says the thesis "
                    "pays, not that the bot reproduces it. The bot (GexGradientDrift) is "
                    "written and screenable but has no run of its own yet, so "
                    "is_provisionable stays False until `make tradeworkz-backtest --bots "
                    "gex_gradient_trend` clears the gate. Nothing goes live on another "
                    "engine's receipt. CAVEAT on this sample: the pattern picks its "
                    "expiry as entry_date + 5 CALENDAR days, which is a Saturday for a "
                    "Monday entry and a Sunday for a Tuesday entry. The backtester "
                    "matches expiry exactly, so those cards found no quote and were "
                    "dropped — these 33 trades are therefore filtered by day of week, "
                    "not a clean 60-day sample."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="vwap_reversion",
        provisioned_history=True,
        name="VWAP Mean-Reversion",
        family=Family.TREND,
        tier="0DTE",
        direction_mode="context",
        tagline="Fade the stretch to VWAP. Positive-γ magnet.",
        thesis=(
            "In a positive-gamma regime session VWAP acts as a magnet, so "
            "over-extension away from it reverts. Small-size 0DTE scalps "
            "against the stretch."
        ),
        stage=Stage.RESEARCH,
        bot_class="VwapReversionScalper",
        bot_id_legacy="vwap_reversion_scalper",
        has_pattern=True,
        params={"min_stretch_pct": 0.0025, "max_hold_minutes": 30},
        research=(
            _fleet_screen(
                trades=311,
                profit_factor=0.51,
                notes=(
                    "Faded extension with no exhaustion trigger — distance from VWAP alone "
                    "does not mark a local extreme. The climax-fade strategy supplies the "
                    "missing trigger (a volume-confirmed flow burst)."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="opening_range_break",
        provisioned_history=True,
        name="Opening Range Break",
        family=Family.TREND,
        tier="0DTE",
        direction_mode="context",
        tagline="Break the opening range. Trend + thin γ = clean legs.",
        thesis=(
            "Breaks of the opening range continue in regimes where dealer "
            "gamma does not choke the move, so the gamma backdrop is the "
            "filter that separates a clean leg from a failed break."
        ),
        stage=Stage.RESEARCH,
        bot_class="OpeningRangeHunter",
        bot_id_legacy="opening_range_hunter",
        has_pattern=True,
        params={"break_buffer_pct": 0.0005, "max_hold_minutes": 90},
        research=(_fleet_screen(trades=134, profit_factor=0.63),),
    ),
    StrategyEntry(
        id="bull_momentum_climber",
        provisioned_history=True,
        name="Bull Momentum Climber",
        family=Family.TREND,
        tier="0DTE",
        direction_mode="bullish",
        tagline="Positive-γ climbing above VWAP and flip. Buy the call debit spread.",
        thesis=(
            "Fires only when spot has cleared both VWAP AND the gamma flip in "
            "a positive-γ regime with confirming 5-bar momentum. Narrow bull "
            "call debit spread targeting the call wall; stop is a break back "
            "below the flip. The catalog's first dedicated bullish strategy."
        ),
        stage=Stage.RESEARCH,
        bot_class="BullMomentumClimber",
        params={
            "min_trend_pct": 0.0015,
            "min_wall_room_pct": 0.003,
            "max_hold_minutes": 60,
            "dte_target": 0,
        },
        research=(
            _fleet_screen(
                trades=64,
                profit_factor=0.72,
                notes=(
                    "Among the better first-order results, and the only long-biased one — "
                    "worth re-screening over a window that is not dominated by a single "
                    "drift direction."
                ),
            ),
        ),
    ),
)


# =======================================================================
# Positioning & skew
# =======================================================================

_POSITIONING: Tuple[StrategyEntry, ...] = (
    StrategyEntry(
        id="dealer_delta_pressure",
        provisioned_history=True,
        name="Dealer Hedging Pressure",
        family=Family.POSITIONING,
        tier="0DTE",
        direction_mode="context",
        tagline="Ride dealer delta pressure. Momentum + short-γ regime = follow-through.",
        thesis=(
            "A heavily one-sided dealer net delta in a negative-gamma regime "
            "implies hedging that pushes price the same way, so recent price "
            "agreement plus a lopsided book should produce follow-through."
        ),
        stage=Stage.RESEARCH,
        bot_class="DealerDeltaPressureRider",
        bot_id_legacy="dealer_delta_pressure_rider",
        has_pattern=True,
        params={"delta_threshold": 5.0e8, "max_hold_minutes": 60},
        research=(
            _fleet_screen(
                trades=176,
                profit_factor=0.59,
                notes=(
                    "Read a static delta SIGN rather than its rate of change; the "
                    "regime-transition successor trades the shift instead."
                ),
            ),
        ),
    ),
    StrategyEntry(
        id="positioning_trap_squeeze",
        name="One-Way Crowding Squeeze",
        family=Family.POSITIONING,
        tier="swing",
        direction_mode="context",
        tagline="Everyone is on one side. Squeeze the crowd.",
        thesis=(
            "Extreme one-way positioning in the option book leaves no marginal "
            "buyer, so a move against the crowd forces unwinds that amplify it."
        ),
        stage=Stage.RESEARCH,
        has_pattern=True,
        params={},
        research=(),
    ),
    StrategyEntry(
        id="overnight_trap_continuation",
        name="Trap Reversal Held Overnight",
        family=Family.POSITIONING,
        tier="1DTE",
        direction_mode="context",
        tagline="The trap reversed into the close. Hold it overnight.",
        thesis=(
            "A positioning-trap reversal that holds into the close tends to "
            "continue the next session, because the unwind it triggered is not "
            "finished by the bell."
        ),
        stage=Stage.RESEARCH,
        has_pattern=True,
        params={},
        research=(),
    ),
    StrategyEntry(
        id="skew_inversion_reversal",
        name="Fear Spike Fade",
        family=Family.POSITIONING,
        tier="swing",
        direction_mode="context",
        tagline="Skew inverted on a fear spike. Fade it.",
        thesis=(
            "An inverted put/call skew marks panic hedging demand rather than "
            "information, so the spike in downside premium reverts once the "
            "hedging flow clears."
        ),
        stage=Stage.RESEARCH,
        has_pattern=True,
        params={},
        research=(),
    ),
)


# =======================================================================
# Gamma profile geometry
# =======================================================================

_PROFILE: Tuple[StrategyEntry, ...] = (
    StrategyEntry(
        id="profile_shelf_breaker",
        name="Gamma Shelf Cascade",
        family=Family.PROFILE,
        tier="0DTE",
        direction_mode="context",
        tagline="A gamma cliff sits just off spot and price is sliding into it.",
        thesis=(
            "Reads the persisted gex_profile spot-shift curve for a steep "
            "one-sided negative-gamma shelf within ~0.5% of spot, and buys a "
            "vertical spanning it when the tape slides that way. The dealers' "
            "own hedge schedule accelerates the move; the trough bottom is the "
            "target."
        ),
        stage=Stage.CANDIDATE,
        bot_class="ProfileShelfBreaker",
        params={
            "min_shelf_depth_local_frac": 0.60,
            "min_asymmetry_ratio": 2.0,
            "max_spot_gex_local_frac": 0.60,
            "min_trigger_pct": 0.001,
            "stop_pct": 0.0015,
            "max_hold_minutes": 100,
            "dte_target": 0,
        },
        research=(
            ResearchRun(
                ran_on=date(2026, 8, 15),
                window_days=60,
                trades=5,
                verdict=Verdict.INSUFFICIENT,
                profit_factor=2.28,
                expectancy=90.0,
                tuning_generation=0,
                symbols=("SPY", "QQQ", "SPX"),
                notes=(
                    "Best early read in the catalog (PF 2.28, +$450) but only 5 trades — "
                    "the geometry gates throttle it far below the promotion bar "
                    "(no_shelf 3495 / not_on_shoulder 3329) and the window cannot grow "
                    "because chain history starts 2026-06-15. Depth and shoulder widened "
                    "modestly to buy sample; the 2:1 asymmetry and live-slide gates that "
                    "define the mechanism are unchanged. This is a recalibrated hypothesis "
                    "to be screened from scratch, NOT a validated PF 2.28."
                ),
            ),
        ),
    ),
)


# =======================================================================
# The catalog
# =======================================================================

#: Every strategy, in family order. This tuple IS the catalog.
STRATEGIES: Tuple[StrategyEntry, ...] = (
    _WALL
    + _GAMMA_FLIP
    + _PIN
    + _FORCED_FLOW
    + _ORDER_FLOW
    + _VOL_REGIME
    + _TREND
    + _POSITIONING
    + _PROFILE
)


def _build_index() -> Dict[str, StrategyEntry]:
    """Map every alias (canonical, bot, pattern id) onto its entry.

    Built once at import. Collisions are a programming error and raise here
    rather than silently shadowing — a duplicate id would mean two strategies
    competing for the same history.
    """
    index: Dict[str, StrategyEntry] = {}
    for entry in STRATEGIES:
        for alias in entry.aliases:
            existing = index.get(alias)
            if existing is not None and existing.id != entry.id:
                raise ValueError(
                    f"strategy catalog: id {alias!r} claimed by both "
                    f"{existing.id!r} and {entry.id!r}"
                )
            index[alias] = entry
    return index


_INDEX: Dict[str, StrategyEntry] = _build_index()


def all_strategies() -> Tuple[StrategyEntry, ...]:
    """The whole catalog, in family order."""
    return STRATEGIES


def get(strategy_id: str) -> StrategyEntry:
    """Resolve any known id (canonical, bot, or pattern) to its entry."""
    try:
        return _INDEX[strategy_id]
    except KeyError:
        raise KeyError(f"unknown strategy id {strategy_id!r}; known: {sorted(_INDEX)}") from None


def find(strategy_id: str) -> Optional[StrategyEntry]:
    """Like :func:`get` but returns None instead of raising."""
    return _INDEX.get(strategy_id)


def canonical_id(strategy_id: str) -> Optional[str]:
    """Fold a legacy bot/pattern id onto its canonical catalog id."""
    entry = _INDEX.get(strategy_id)
    return entry.id if entry else None


def by_stage(*stages: Stage) -> Tuple[StrategyEntry, ...]:
    return tuple(e for e in STRATEGIES if e.stage in stages)


def by_family(family: Family) -> Tuple[StrategyEntry, ...]:
    return tuple(e for e in STRATEGIES if e.family is family)


def bot_bound() -> Tuple[StrategyEntry, ...]:
    """Entries a TradeWorkz bot class implements."""
    return tuple(e for e in STRATEGIES if e.bot_class is not None)


def pattern_bound() -> Tuple[StrategyEntry, ...]:
    """Entries a playbook pattern implements."""
    return tuple(e for e in STRATEGIES if e.has_pattern)


def provisionable() -> Tuple[StrategyEntry, ...]:
    """Entries eligible for a live capital sleeve: VALIDATED with a bot."""
    return tuple(e for e in STRATEGIES if e.is_provisionable)


def iter_aliases() -> Iterable[Tuple[str, StrategyEntry]]:
    """(alias, entry) for every known id."""
    return _INDEX.items()
