"""Pure-maths tests for the Hedging Flow engine.

The sign convention is the thing worth pinning hardest: a flipped sign here
would render a panel that is confidently backwards on every bar, and nothing
downstream would notice. The four-case table in
:mod:`src.analytics.hedging_flow` is therefore asserted directly, and against
the Forced Flow engine's convention so the modeled and estimated sources
cannot drift into disagreeing about which way "positive" points.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.analytics.hedging_flow import (
    DEFAULT_SMOOTHING_BARS,
    HedgingFlowBar,
    hedge_usd,
    session_scale,
    sign_flip_events,
    smooth,
    zero_cross_events,
)

UTC = timezone.utc
T0 = datetime(2026, 4, 24, 13, 30, tzinfo=UTC)

SPOT = 600.0
CALL_DELTA = 0.5
PUT_DELTA = -0.5


def _bar(i: int, net: float, *, call: float = 0.0, put: float = 0.0, cum: float = 0.0):
    return HedgingFlowBar(
        bar_start=T0 + timedelta(minutes=5 * i),
        call_flow_usd=call,
        put_flow_usd=put,
        net_flow_usd=net,
        cum_call_usd=0.0,
        cum_put_usd=0.0,
        cum_net_usd=cum,
        underlying_price=SPOT,
        contract_count=1,
        classified_ratio=1.0,
        is_synthetic=False,
    )


def _series(nets, cums=None):
    cums = cums if cums is not None else [0.0] * len(nets)
    return [_bar(i, n, cum=c) for i, (n, c) in enumerate(zip(nets, cums))]


# --------------------------------------------------------------------------- #
# The primitive: all four customer actions hedge the right way
# --------------------------------------------------------------------------- #
def test_customer_buying_calls_makes_dealer_buy_stock():
    assert hedge_usd(10, CALL_DELTA, SPOT) == 300_000.0


def test_customer_selling_calls_makes_dealer_sell_stock():
    assert hedge_usd(-10, CALL_DELTA, SPOT) == -300_000.0


def test_customer_buying_puts_makes_dealer_sell_stock():
    assert hedge_usd(10, PUT_DELTA, SPOT) == -300_000.0


def test_customer_selling_puts_makes_dealer_buy_stock():
    """The case a naive call-bullish / put-bearish split gets backwards."""
    assert hedge_usd(-10, PUT_DELTA, SPOT) == 300_000.0


def test_put_activity_can_push_pressure_positive():
    """Put-driven does not mean downward — the whole reason for splitting by
    contributed pressure rather than colouring puts bearish."""
    put_side = hedge_usd(-40, PUT_DELTA, SPOT)  # customers sold 40 puts
    call_side = hedge_usd(-10, CALL_DELTA, SPOT)  # customers sold 10 calls
    assert put_side > 0 and call_side < 0
    assert put_side + call_side > 0


def test_sign_convention_matches_forced_flow_engine():
    """Positive == the hedge buys stock, in both engines.

    Forced Flow documents positive total_usd as "an offsetting delta-flat
    hedge buys stock". combine_flow_sources adds the two, so a mismatch would
    silently subtract. Pin it by construction: a short-gamma dealer book under
    a spot rise must buy, and customer call buying must buy.
    """
    from src.analytics.forced_flow import ContractLeg, combine_flow_sources, dealer_hedge_flow

    # Dealers short gamma below the flip: a spot rise forces buying.
    book = [ContractLeg(strike=100.0, option_type="P", open_interest=5000, iv=0.2, tte_years=0.08)]
    predicted = dealer_hedge_flow(book, 100.0, 0.02, 0.0, 0.0, 0.05)

    observed = hedge_usd(10, CALL_DELTA, SPOT)
    assert observed > 0

    combined = combine_flow_sources(predicted, observed)
    assert combined == predicted.total_usd + observed


def test_zero_delta_contributes_nothing():
    assert hedge_usd(1000, 0.0, SPOT) == 0.0


# --------------------------------------------------------------------------- #
# Smoothing
# --------------------------------------------------------------------------- #
def test_smooth_is_trailing_and_leaves_leading_nulls():
    assert smooth([1.0, 2.0, 3.0, 4.0, 5.0], 3) == [None, None, 2.0, 3.0, 4.0]


def test_smooth_window_one_is_identity():
    assert smooth([1.0, -2.0, 3.0], 1) == [1.0, -2.0, 3.0]


def test_smooth_refuses_to_average_across_a_gap():
    """A hole must not be silently averaged into a number that looks measured."""
    out = smooth([1.0, None, 3.0, 4.0, 5.0], 3)
    assert out[2] is None and out[3] is None
    assert out[4] == 4.0


def test_smooth_shorter_than_window_is_all_none():
    assert smooth([1.0, 2.0], 3) == [None, None]


def test_smooth_kills_a_single_bar_spike():
    """The reason flips are read off the smoothed series, in one assertion."""
    raw = [100.0, 100.0, -50.0, 100.0, 100.0]
    assert min(v for v in smooth(raw, 3) if v is not None) > 0


# --------------------------------------------------------------------------- #
# Session scale
# --------------------------------------------------------------------------- #
def test_session_scale_is_robust_to_one_huge_bar():
    """A single 0DTE surge into the close must not redefine 'normal'."""
    quiet = [10_000.0] * 20
    assert session_scale(quiet + [50_000_000.0]) == 10_000.0


def test_session_scale_floors_on_a_dead_tape():
    assert session_scale([1.0, -2.0, 3.0]) == 1_000.0


def test_session_scale_of_nothing_is_the_floor():
    assert session_scale([]) == 1_000.0
    assert session_scale([None, None]) == 1_000.0


# --------------------------------------------------------------------------- #
# Rate flips
# --------------------------------------------------------------------------- #
def test_rate_flip_detected_on_smoothed_series():
    bars = _series([80_000.0] * 4 + [-80_000.0] * 4)
    events = sign_flip_events(bars)

    assert [e.kind for e in events] == ["rate"]
    assert events[0].direction == "to_selling"


def test_rate_flip_reports_both_directions_in_order():
    bars = _series([50_000.0] * 3 + [-50_000.0] * 3 + [50_000.0] * 3)
    events = sign_flip_events(bars)

    assert [e.direction for e in events] == ["to_selling", "to_buying"]
    assert events[0].bar_start < events[1].bar_start


def test_single_bar_spike_does_not_manufacture_a_flip():
    """Raw sign changes twice; the smoothed series never crosses."""
    bars = _series([90_000.0, 90_000.0, -30_000.0, 90_000.0, 90_000.0])
    assert sign_flip_events(bars) == []


def test_flat_bar_between_signs_flips_once_not_twice():
    bars = _series([60_000.0] * 3 + [0.0] * 3 + [-60_000.0] * 3)
    events = sign_flip_events(bars, window=1)

    assert len(events) == 1
    assert events[0].direction == "to_selling"


def test_sub_floor_tape_produces_no_flip_at_all():
    """Below the session-scale floor there is no direction change to name.

    This used to report the flip and mark it insignificant. The deadband
    supersedes that: a tape whose whole range sits inside the noise floor is
    flat, and emitting a "flip" for it is what put a dozen dots on a live
    session even with the significance filter on.
    """
    bars = _series([400.0] * 3 + [-400.0] * 3)

    assert sign_flip_events(bars) == []


def test_modest_but_real_flip_on_an_active_tape_is_still_reported():
    """The guard against the deadband swallowing genuine, moderate turns."""
    bars = _series([250_000.0] * 4 + [-250_000.0] * 4)
    events = sign_flip_events(bars)

    assert len(events) == 1
    assert events[0].direction == "to_selling"


def test_large_flip_is_significant():
    bars = _series([5_000.0] * 3 + [-900_000.0] * 3)
    events = sign_flip_events(bars)

    assert events and events[-1].is_significant is True
    assert events[-1].session_ratio > 1.0


def test_significance_threshold_is_honoured():
    bars = _series([40_000.0] * 3 + [-40_000.0] * 3)
    assert sign_flip_events(bars, significance_ratio=0.0)[0].is_significant is True
    assert sign_flip_events(bars, significance_ratio=9.0)[0].is_significant is False


def test_no_flips_in_a_one_sided_session():
    assert sign_flip_events(_series([25_000.0] * 10)) == []


def test_empty_series_has_no_flips():
    assert sign_flip_events([]) == []
    assert zero_cross_events([]) == []


def test_flip_carries_price_for_chart_annotation():
    bars = _series([70_000.0] * 3 + [-70_000.0] * 3)
    assert sign_flip_events(bars)[0].underlying_price == SPOT


def test_default_window_is_used_when_unspecified():
    """Guards against the default drifting away from the documented 15 min."""
    nets = [30_000.0] * 5 + [-30_000.0] * 5
    assert sign_flip_events(nets and _series(nets)) == sign_flip_events(
        _series(nets), window=DEFAULT_SMOOTHING_BARS
    )


# --------------------------------------------------------------------------- #
# Cumulative crossings
# --------------------------------------------------------------------------- #
def test_cumulative_zero_cross_detected():
    # Magnitudes in millions, as real cumulative delta notional is. The old
    # fixture used dollars, which sits entirely inside the noise floor.
    bars = _series([0.0] * 5, cums=[100e6, 60e6, 20e6, -30e6, -90e6])
    events = zero_cross_events(bars)

    assert [e.kind for e in events] == ["cumulative"]
    assert events[0].direction == "to_selling"


def test_cumulative_never_crossing_yields_nothing():
    bars = _series([0.0] * 4, cums=[10e6, 50e6, 90e6, 200e6])
    assert zero_cross_events(bars) == []


def test_rate_and_cumulative_flips_are_distinguishable():
    """They answer different questions and must never be conflated."""
    bars = _series(
        [40_000.0, 40_000.0, 40_000.0, -90_000.0, -90_000.0, -90_000.0],
        cums=[40_000.0, 80_000.0, 120_000.0, 30_000.0, -60_000.0, -150_000.0],
    )
    kinds = {e.kind for e in sign_flip_events(bars)} | {e.kind for e in zero_cross_events(bars)}
    assert kinds == {"rate", "cumulative"}


# --------------------------------------------------------------------------- #
# Properties the two fixes above depend on
# --------------------------------------------------------------------------- #
def test_flip_magnitude_is_the_swing_not_the_level():
    """A series is near zero AT a crossing, so the level there measures
    nothing. Magnitude must describe how hard it crossed."""
    bars = _series([10_000.0] * 3 + [-800_000.0] * 3)
    event = sign_flip_events(bars)[0]

    smoothed_at_flip = smooth([b.net_flow_usd for b in bars], DEFAULT_SMOOTHING_BARS)
    level = abs(next(v for v in smoothed_at_flip if v is not None and v < 0))

    assert event.magnitude_usd > level


def test_significance_is_causal_and_survives_truncation():
    """Scored from bars before the flip only, so a live session scores a flip
    the same way the completed session will — an alert that needed later bars
    could not fire when it mattered."""
    nets = [20_000.0] * 3 + [-500_000.0] * 6
    full = sign_flip_events(_series(nets))
    truncated = sign_flip_events(_series(nets[:5]))

    assert full and truncated
    assert full[0].bar_start == truncated[0].bar_start
    assert full[0].session_ratio == truncated[0].session_ratio
    assert full[0].is_significant == truncated[0].is_significant


def test_pass_through_exact_zero_still_reports_the_crossing():
    """Zero is on neither side of zero; holding the last established sign
    through a flat bar is what keeps the real crossing visible."""
    bars = _series([50_000.0, 50_000.0, 0.0, 0.0, -50_000.0, -50_000.0])
    events = sign_flip_events(bars, window=1)

    assert [e.direction for e in events] == ["to_selling"]


# --------------------------------------------------------------------------- #
# The deadband — the fix for flip counts on a live tape
# --------------------------------------------------------------------------- #
def test_chatter_around_zero_produces_no_flips():
    """The actual complaint: on a live session the rate hugged zero and nicked
    across it repeatedly, and every nick became a dot even with the
    significance filter on. A line that small is flat, not reversing."""
    active = [900_000.0, -900_000.0] * 3  # establishes a real scale first
    chatter = [12_000.0, -9_000.0, 8_000.0, -11_000.0, 7_000.0, -6_000.0]
    bars = _series(active + chatter)

    flips = sign_flip_events(bars, window=1)
    chatter_start = _bar(len(active), 0.0).bar_start

    assert [e for e in flips if e.bar_start >= chatter_start] == []


def test_a_real_reversal_after_chatter_still_registers():
    """Guard against the band swallowing the turn that matters."""
    bars = _series([900_000.0, -900_000.0] * 3 + [10_000.0, -8_000.0, 9_000.0] + [1_400_000.0] * 4)
    flips = sign_flip_events(bars, window=1)

    assert flips
    assert flips[-1].direction == "to_buying"


def test_band_ratio_zero_restores_pre_band_behaviour():
    """The knob is real: at 0 only exact zeros are flat, which is what the
    scan did before the band existed."""
    values = [600_000.0] * 3 + [-40_000.0, 50_000.0, -45_000.0] + [700_000.0] * 3
    bars = _series(values)

    banded = sign_flip_events(bars, window=1)
    unbanded = sign_flip_events(bars, window=1, flat_band_ratio=0.0)

    assert len(unbanded) > len(banded)


def test_wider_band_never_reports_more_flips():
    """Monotonic in the knob, so tuning it has a predictable direction."""
    values = [800_000.0, -700_000.0, 60_000.0, -50_000.0, 900_000.0, -850_000.0]
    bars = _series(values)

    counts = [
        len(sign_flip_events(bars, window=1, flat_band_ratio=r)) for r in (0.0, 0.25, 0.5, 1.0, 2.0)
    ]
    assert counts == sorted(counts, reverse=True)


def test_flip_is_timestamped_where_the_move_establishes():
    """The band delays the flag to the first bar the move is knowable as a
    move, which is the earliest honest moment — not hindsight."""
    bars = _series([500_000.0] * 3 + [-20_000.0, -60_000.0, -900_000.0])
    flips = sign_flip_events(bars, window=1)

    assert len(flips) == 1
    assert flips[0].bar_start == bars[5].bar_start
