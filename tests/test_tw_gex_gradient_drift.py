"""GexGradientDrift — the bot for the one validated strategy in the catalog.

The strategy's measured edge (PF 2.80 on 33 QQQ trades) belongs to the
*pattern's* exact gates and geometry, so the tests that matter most here are
**parity tests**: the bot and the pattern are handed the same market state and
must agree on whether to fire, in which direction, and where the strike and
target land. A bot that merely gestures at the same thesis would not inherit
the measurement.

Everything is driven off the pattern test's own ``_ctx`` builder, so the two
sides cannot drift apart through separately-maintained fixtures.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.signals.playbook.patterns.gex_gradient_trend import PATTERN as GGT
from src.signals.playbook.patterns.gex_gradient_trend import _realized_sigma_1min
from src.strategies import get
from src.tradeworkz.bots.gex_gradient_drift import GexGradientDrift, realized_sigma_1min
from src.tradeworkz.context import MarketSnapshot
from src.tradeworkz.models import OpenPosition
from src.tradeworkz.registry import known_specs
from tests.test_playbook_gex_gradient_trend import _ctx

# 15:40 ET on 2026-05-01 = 19:40 UTC (EDT), inside the bot's at_close window.
_AT_CLOSE = datetime(2026, 5, 1, 19, 40, tzinfo=timezone.utc)


def _bot(**param_overrides) -> GexGradientDrift:
    spec = known_specs()["gex_gradient_trend"]
    if param_overrides:
        spec = type(spec)(**{**spec.__dict__, "params": {**spec.params, **param_overrides}})
    return GexGradientDrift(spec, ml_state=None)


def _snap(ctx, *, timestamp: datetime = _AT_CLOSE, underlying: str = "SPY") -> MarketSnapshot:
    """A MarketSnapshot carrying the same state as a PlaybookContext.

    Maps the playbook's signal snapshots onto the persisted
    ``signal_component_scores`` shape the bot reads, so both sides are looking
    at one set of numbers.
    """
    components = {}
    for name, snapshot in {**ctx.basic_signals, **ctx.advanced_signals}.items():
        components[name] = {
            "clamped_score": snapshot.clamped_score,
            "score": snapshot.score,
            "context_values": {
                **(snapshot.context_values or {}),
                "triggered": snapshot.triggered,
            },
        }
    return MarketSnapshot(
        underlying=underlying,
        timestamp=timestamp,
        spot=ctx.close,
        net_gex=ctx.net_gex,
        recent_closes=list(ctx.market.recent_closes),
        msi_score=ctx.msi_score,
        msi_regime=ctx.msi_regime,
        msi_components=dict(ctx.msi_components),
        signal_components=components,
        strike_increment=1.0,
    )


def _position(signal, *, direction: str = "bullish") -> OpenPosition:
    return OpenPosition(
        id=1,
        bot_id="gex_gradient_trend",
        underlying="SPY",
        opened_at=_AT_CLOSE,
        direction=direction,
        strategy_type=signal.strategy_type,
        legs=[],
        entry_price=1.0,
        current_price=1.0,
        quantity_open=1,
        unrealized_pnl=0.0,
        stop_price=None,
        target_price=signal.target_price,
        time_stop_at=signal.time_stop_at,
        min_hold_until=None,
        wall_ref_price=None,
        wall_ref_side=None,
        entry_conviction=signal.conviction,
        components_at_entry=dict(signal.components_at_entry),
    )


# ======================================================================
# Fidelity: the bot must inherit the pattern's measurement
# ======================================================================


@pytest.mark.parametrize(
    "closes",
    [
        [500.0 + (i % 7) * 0.11 - (i % 3) * 0.07 for i in range(40)],
        [678.0 + 0.5 * (i % 4 - 1.5) for i in range(35)],
        [100.0] * 8,  # zero variance
        [1.0, 2.0],  # too few closes
        [],
    ],
)
def test_atr_estimator_is_identical_to_the_patterns(closes):
    """Strike offset and target are both multiples of this one number.

    The base class ships ``_short_horizon_sigma_pct``, which looks at the last
    15 returns; the pattern uses 30 closes. Using the base helper would have
    silently moved the trade off the geometry that was measured.
    """
    assert realized_sigma_1min(closes) == _realized_sigma_1min(closes)


@pytest.mark.parametrize(
    "gradient_score,net_gex,delta,expect_fire",
    [
        (50.0, 0.5e9, 0.05, True),  # clean bullish drift
        (-50.0, -0.5e9, -0.05, True),  # clean bearish drift
        (30.0, 0.5e9, 0.05, False),  # gradient below the 40 floor
        (50.0, -0.5e9, 0.05, False),  # net_gex disagrees
        (50.0, 0.5e9, -0.05, False),  # no confirming bar
    ],
)
def test_bot_and_pattern_agree_on_whether_to_fire(gradient_score, net_gex, delta, expect_fire):
    ctx = _ctx(gradient_score=gradient_score, net_gex=net_gex, last_close_delta=delta)
    card = GGT.match(ctx)
    signal = _bot().open_criteria(_snap(ctx))
    assert (card is not None) is expect_fire
    assert (signal is not None) is expect_fire


def test_bot_and_pattern_agree_on_direction_strike_and_target():
    """The geometry, not just the decision."""
    for gradient_score, net_gex, delta in ((50.0, 0.5e9, 0.05), (-50.0, -0.5e9, -0.05)):
        ctx = _ctx(gradient_score=gradient_score, net_gex=net_gex, last_close_delta=delta)
        card = GGT.match(ctx)
        signal = _bot().open_criteria(_snap(ctx))
        assert card is not None and signal is not None
        assert signal.direction == card.direction
        assert signal.legs[0].strike == pytest.approx(card.legs[0].strike)
        assert signal.target_price == pytest.approx(card.target.ref_price, abs=1e-3)
        expect_type = "call" if card.direction == "bullish" else "put"
        assert signal.legs[0].option_type == expect_type
        assert signal.legs[0].side == "long"


def test_both_reject_breakout_mode():
    ctx = _ctx(rbi_label="Breakout Mode")
    assert GGT.match(ctx) is None
    assert _bot().open_criteria(_snap(ctx)) is None


def test_both_reject_a_dead_volatility_regime():
    ctx = _ctx(vol_regime_score=-0.9)
    assert GGT.match(ctx) is None
    assert _bot().open_criteria(_snap(ctx)) is None


def test_vol_expansion_docks_conviction_rather_than_vetoing():
    """The pattern reduces confidence; it does not stand down. Nor does the bot.

    Measured well above the conviction floor so the penalty is visible as a
    smaller number rather than as a refusal — the near-floor case is the next
    test.
    """
    calm = _ctx(gradient_score=90.0, vol_x_triggered=False)
    hot = _ctx(gradient_score=90.0, vol_x_triggered=True)
    calm_sig = _bot().open_criteria(_snap(calm))
    hot_sig = _bot().open_criteria(_snap(hot))
    assert calm_sig is not None and hot_sig is not None
    assert hot_sig.conviction < calm_sig.conviction


def test_near_the_floor_the_vol_expansion_penalty_can_decline_the_trade():
    """A documented divergence, not a bug.

    The pattern has no minimum confidence — it cards every setup that clears
    its triggers. The engine refuses to open below ``confidence_threshold``,
    so a penalty applied near that floor changes the outcome rather than just
    the number. The bot is therefore strictly MORE selective than the pattern
    in this region, which the bot screen will measure as it is configured.
    """
    bot = _bot()
    ctx = _ctx(gradient_score=50.0, vol_x_triggered=True)
    assert bot.open_criteria(_snap(ctx)) is None
    assert bot.miss_reasons == {"conviction": 1}
    # Same setup without the breakout warning does open.
    assert _bot().open_criteria(_snap(_ctx(gradient_score=50.0))) is not None


# ======================================================================
# Live-engine specifics the Action Card did not need
# ======================================================================


def test_a_typical_gate_passing_setup_also_clears_conviction():
    """Regression for the failure mode the catalog records for weekly_charm_grind.

    There, three ticks in 60 days cleared every hard gate and all three died
    at conviction — a zero-trade screen that said nothing about the thesis.
    A conviction scale has to be calibrated against the entry floor, so a
    setup at ordinary readings must actually open.
    """
    bot = _bot()
    ctx = _ctx(gradient_score=50.0, vol_regime_score=0.0)
    assert bot.open_criteria(_snap(ctx)) is not None, bot.miss_reasons


def test_conviction_still_filters_the_weakest_corner():
    """It is a filter, not a formality: gradient at the floor with dead vol
    clears the hard gates and is still declined."""
    bot = _bot()
    ctx = _ctx(gradient_score=40.0, vol_regime_score=-0.5)
    assert bot.open_criteria(_snap(ctx)) is None
    assert bot.miss_reasons == {"conviction": 1}


def test_opens_anywhere_in_the_session_like_the_pattern():
    """Regression for the first screen's 1-trade result.

    An earlier revision restricted entry to the last 30 minutes, reading the
    pattern's ``Entry(trigger="at_close")`` as "enter at the closing print".
    It is not — ``at_close`` is in ``playbook.backtest._IMMEDIATE_TRIGGERS``,
    meaning "fill at this bar, at market". The pattern emits all session, so
    restricting the bot cut its opportunity set by ~92%.
    """
    ctx = _ctx()
    morning = datetime(2026, 5, 1, 13, 40, tzinfo=timezone.utc)  # 09:40 ET
    midday = datetime(2026, 5, 1, 16, 30, tzinfo=timezone.utc)  # 12:30 ET
    for ts in (morning, midday, _AT_CLOSE):
        assert _bot().open_criteria(_snap(ctx, timestamp=ts)) is not None, ts


def test_at_close_really_is_an_immediate_fill_trigger():
    """Pins the fact the correction rests on, so it cannot be re-misread."""
    from src.signals.playbook.backtest import _IMMEDIATE_TRIGGERS
    from src.signals.playbook.patterns.gex_gradient_trend import PATTERN

    card = PATTERN.match(_ctx())
    assert card is not None
    assert card.entry.trigger == "at_close"
    assert card.entry.trigger in _IMMEDIATE_TRIGGERS


def test_premium_stop_grace_outlasts_a_replay_step():
    """The other half of the first screen's failure.

    Its single trade closed after one 5-minute replay step on
    ``premium_stop``: the fleet grace is 45s, so the stop was measured
    bid-vs-ask on a fresh position — the exact thing the grace exists to
    prevent. A grace shorter than one step protects nothing.
    """
    from src.tradeworkz.backtest import _DEFAULT_INTERVAL_MIN

    grace = _bot()._premium_stop_grace_seconds()
    assert grace > _DEFAULT_INTERVAL_MIN * 60, grace
    # And it is a per-bot override, not a fleet-wide change.
    from src.tradeworkz import config as tw_config

    assert grace != tw_config.PREMIUM_STOP_GRACE_SECONDS


def test_strike_snaps_to_the_underlyings_real_grid():
    """A $1-rounded SPX strike does not exist and would never fill."""
    ctx = _ctx()
    snap = _snap(ctx, underlying="SPX")
    snap.strike_increment = 5.0
    signal = _bot().open_criteria(snap)
    assert signal is not None
    assert signal.legs[0].strike % 5 == 0


def test_strike_stays_otm_when_a_tight_atr_rounds_it_to_the_money():
    """A near-zero ATR rounds the offset away; the structure must stay OTM."""
    flat = [678.0] * 34 + [678.01]
    ctx = _ctx(closes=flat, last_close_delta=0.01)
    signal = _bot().open_criteria(_snap(ctx))
    assert signal is not None
    assert signal.legs[0].strike > ctx.close


def test_expiration_is_five_weekdays_out():
    ctx = _ctx()
    signal = _bot().open_criteria(_snap(ctx))
    assert signal is not None
    # 2026-05-01 is a Friday; five weekdays on is the following Friday.
    assert signal.legs[0].expiration == "2026-05-08"


@pytest.mark.parametrize("entry_day", [4, 5])  # 2026-05-04 Mon, 2026-05-05 Tue
def test_expiry_never_lands_on_a_weekend(entry_day):
    """Deliberate divergence — do not "fix" this back to calendar days.

    The pattern uses ``et_date + timedelta(days=5)``, which is a Saturday for
    a Monday entry and a Sunday for a Tuesday entry. The backtester matches
    expiry EXACTLY (``AND expiration = %s``; only strike is fuzzy-matched),
    so those cards find no quote and are silently dropped — which is why the
    pattern's validated sample is filtered by day of week. A weekday walk is
    the correct behaviour for something that has to actually fill.
    """
    from datetime import date, timedelta

    ts = datetime(2026, 5, entry_day, 19, 40, tzinfo=timezone.utc)
    signal = _bot().open_criteria(_snap(_ctx(), timestamp=ts))
    assert signal is not None
    expiry = date.fromisoformat(signal.legs[0].expiration)
    assert expiry.weekday() < 5, f"{expiry} is a {expiry.strftime('%A')}"
    # And confirm the pattern's own arithmetic would have missed.
    naive = date(2026, 5, entry_day) + timedelta(days=5)
    assert naive.weekday() >= 5, "fixture no longer exercises the weekend case"


def test_no_spot_stop_is_set_because_the_patterns_stop_is_a_signal_event():
    signal = _bot().open_criteria(_snap(_ctx()))
    assert signal is not None
    assert signal.stop_price is None
    # The premium half of the pattern's stop lives in params.
    assert _bot().params["max_premium_loss_pct"] == 0.50


def test_time_stop_is_three_days_out():
    signal = _bot().open_criteria(_snap(_ctx()))
    assert signal is not None
    assert signal.time_stop_at is not None
    assert signal.time_stop_at - _AT_CLOSE >= timedelta(days=3) - timedelta(minutes=1)


def test_sizes_at_half_matching_the_patterns_size_multiplier():
    assert _bot().size_multiplier() == pytest.approx(0.5)


# ======================================================================
# Fail closed
# ======================================================================


@pytest.mark.parametrize("drop", ["gex_gradient", "range_break_imminence", "vol_expansion"])
def test_a_missing_component_never_fires_on_a_default(drop):
    """A signals-engine gap must not be read as a passing gate.

    Dropping range_break_imminence or vol_expansion is survivable (absent is
    not "Breakout Mode", and the vol penalty is optional); dropping the
    gradient itself must stand the bot down.
    """
    ctx = _ctx()
    snap = _snap(ctx)
    snap.signal_components.pop(drop, None)
    signal = _bot().open_criteria(snap)
    if drop == "gex_gradient":
        assert signal is None
    else:
        assert signal is not None


def test_missing_msi_components_stands_down():
    ctx = _ctx()
    snap = _snap(ctx)
    snap.msi_components = {}
    assert _bot().open_criteria(snap) is None


def test_missing_net_gex_stands_down():
    ctx = _ctx()
    snap = _snap(ctx)
    snap.net_gex = None
    assert _bot().open_criteria(snap) is None


def test_zero_sigma_stands_down_rather_than_sizing_off_a_zero_atr():
    ctx = _ctx(closes=[678.0] * 35, last_close_delta=0.0)
    assert _bot().open_criteria(_snap(ctx)) is None


# ======================================================================
# Exits — the gradient half of the pattern's signal_event stop
# ======================================================================


def _open_signal_and_position():
    ctx = _ctx()
    signal = _bot().open_criteria(_snap(ctx))
    assert signal is not None
    return ctx, signal, _position(signal)


def test_closes_when_the_gradient_decays_below_twenty():
    ctx, signal, pos = _open_signal_and_position()
    decayed = _snap(_ctx(gradient_score=15.0))
    decision = _bot().exit_criteria(decayed, pos)
    assert decision.should_close
    assert decision.reason == "gradient_decayed"


def test_closes_when_the_gradient_flips_sign():
    ctx, signal, pos = _open_signal_and_position()
    flipped = _snap(_ctx(gradient_score=-55.0, net_gex=-0.5e9))
    decision = _bot().exit_criteria(flipped, pos)
    assert decision.should_close
    assert decision.reason == "gradient_flipped"


def test_a_still_strong_gradient_does_not_force_an_exit():
    ctx, signal, pos = _open_signal_and_position()
    decision = _bot().exit_criteria(_snap(_ctx(gradient_score=55.0)), pos)
    assert decision.reason != "gradient_decayed"
    assert decision.reason != "gradient_flipped"


def test_a_signals_gap_does_not_force_an_exit():
    """A missing component is an engine gap, not a thesis failure.

    Closing on it would exit every open position the moment the signals
    writer hiccups; the time stop still bounds the trade.
    """
    ctx, signal, pos = _open_signal_and_position()
    snap = _snap(_ctx())
    snap.signal_components.pop("gex_gradient")
    decision = _bot().exit_criteria(snap, pos)
    assert decision.reason not in ("gradient_decayed", "gradient_flipped")


# ======================================================================
# Catalog wiring
# ======================================================================


def test_catalog_binds_the_bot_to_the_validated_strategy():
    entry = get("gex_gradient_trend")
    assert entry.bot_class == "GexGradientDrift"
    assert entry.has_pattern is True
    assert [e.value for e in entry.engines] == ["bot", "pattern"]


def test_bot_params_carry_the_live_engine_specifics_over_catalog_defaults():
    entry = get("gex_gradient_trend")
    effective = entry.effective_bot_params()
    # Pattern-calibrated thresholds shared with the pattern.
    assert effective["min_gradient_score"] == 40.0
    assert effective["target_atr_mult"] == 1.5
    assert effective["dte_target"] == 5
    # Live-only additions.
    assert effective["static_size_multiplier"] == 0.5
    assert effective["max_premium_loss_pct"] == 0.50
    assert effective["premium_stop_grace_seconds"] == 1200
    # The entry window is gone — see the first-screen correction.
    assert "max_minutes_to_close" not in effective


def test_writing_the_bot_does_not_fund_it():
    """Capital rides on the bot, so the bot is what has to be screened.

    The strategy is VALIDATED on PATTERN-side evidence. Until a bot harness
    measures an edge for GexGradientDrift itself, this stays unfunded and the
    fleet roster stays empty.
    """
    from src.tradeworkz.registry import DEFAULT_ROSTER

    entry = get("gex_gradient_trend")
    assert entry.stage.value == "validated"
    assert entry.bot_validated is False
    assert entry.is_provisionable is False
    assert DEFAULT_ROSTER == ()


def test_the_audit_queues_it_for_a_bot_screen():
    from src.strategies.audit import gaps

    g = gaps()
    assert g["awaiting_bot_screen"] == ["gex_gradient_trend"]
    assert g["validated_without_bot"] == []
