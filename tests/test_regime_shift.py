"""Tests for the Gamma Regime Shift analytics (:mod:`src.analytics.regime_shift`).

The suite is organised around the three questions the module exists to
answer, plus the trap that makes a naive implementation of the first one
wrong:

* **the diff** — per-strike change, and specifically that an expired
  expiration is EXCLUDED rather than booked as a giant shed;
* **the roll-off** — gamma grouped by expiration, nearest first;
* **the read** — the lean/stability projection and the quadrant it lands in,
  including the sign conventions that make "green" mean the same thing on
  both sides of spot.

The headline property — a build BELOW spot and an identical build ABOVE spot
produce OPPOSITE leans — is :func:`test_lean_flips_sign_across_spot`.
"""

from __future__ import annotations

import math
from datetime import date

import pytest

from src.analytics import regime_shift as rs


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _row(strike, net, exp="2026-08-21", call_oi=0, put_oi=0, call_gex=None, put_gex=None):
    """One ``gex_by_strike``-shaped row as the module consumes it."""
    return {
        "strike": strike,
        "expiration": exp,
        "net_gex": net,
        "call_gex": net if call_gex is None else call_gex,
        "put_gex": 0.0 if put_gex is None else put_gex,
        "call_oi": call_oi,
        "put_oi": put_oi,
    }


def _diff_rows(pairs_a, pairs_b, exp="2026-08-21"):
    """Build a diff from two {strike: net} maps sharing one expiration."""
    a = [_row(k, v, exp=exp) for k, v in pairs_a.items()]
    b = [_row(k, v, exp=exp) for k, v in pairs_b.items()]
    return rs.shift_rows(a, b).rows


# --------------------------------------------------------------------------- #
# 1. The diff
# --------------------------------------------------------------------------- #
class TestShiftRows:
    def test_simple_delta_per_strike(self):
        rows = _diff_rows({100: 10.0, 101: -5.0}, {100: 25.0, 101: -5.0})
        by_strike = {r.strike: r for r in rows}
        assert by_strike[100.0].d_net == pytest.approx(15.0)
        assert by_strike[101.0].d_net == pytest.approx(0.0)

    def test_strike_only_in_b_counts_as_a_full_build(self):
        rows = _diff_rows({100: 10.0}, {100: 10.0, 105: 40.0})
        by_strike = {r.strike: r for r in rows}
        assert by_strike[105.0].net_a == 0.0
        assert by_strike[105.0].d_net == pytest.approx(40.0)

    def test_expired_expiration_is_excluded_not_booked_as_a_shed(self):
        """The trap: yesterday's 0DTE is gone today.

        Differencing "everything" would report the whole expired tranche as
        dealers shedding gamma, which on SPY dwarfs any real repositioning
        and prints DETERIORATING every morning.  The tranche must land in
        ``expired_*`` and leave the per-strike rows untouched.
        """
        a = [
            _row(100, 500.0, exp="2026-08-20"),  # expires overnight
            _row(100, 100.0, exp="2026-08-21"),
        ]
        b = [_row(100, 120.0, exp="2026-08-21")]

        diff = rs.shift_rows(a, b)

        assert diff.expired_expirations == (date(2026, 8, 20),)
        assert diff.expired_net_gex == pytest.approx(500.0)
        assert diff.expired_abs_gex == pytest.approx(500.0)
        assert diff.common_expirations == (date(2026, 8, 21),)
        # The surviving expiration moved +20, and that is the ONLY change the
        # diff reports — not -380.
        assert [r.d_net for r in diff.rows] == pytest.approx([20.0])

    def test_newly_listed_expiration_is_excluded_too(self):
        """A new weekly opening for trading is not dealer repositioning."""
        a = [_row(100, 100.0, exp="2026-08-21")]
        b = [
            _row(100, 100.0, exp="2026-08-21"),
            _row(100, 900.0, exp="2026-08-28"),  # newly listed
        ]

        diff = rs.shift_rows(a, b)

        assert diff.added_expirations == (date(2026, 8, 28),)
        assert diff.added_net_gex == pytest.approx(900.0)
        assert [r.d_net for r in diff.rows] == pytest.approx([0.0])

    def test_restrict_expirations_narrows_the_intersection(self):
        a = [_row(100, 10.0, exp="2026-08-21"), _row(100, 10.0, exp="2026-08-28")]
        b = [_row(100, 30.0, exp="2026-08-21"), _row(100, 50.0, exp="2026-08-28")]

        diff = rs.shift_rows(a, b, restrict_expirations=[date(2026, 8, 21)])

        assert diff.common_expirations == (date(2026, 8, 21),)
        assert [r.d_net for r in diff.rows] == pytest.approx([20.0])

    def test_restrict_to_a_foreign_expiration_yields_an_empty_diff(self):
        """A stale filter must degrade to "nothing to compare", never a wrong
        number silently computed on the wrong subset."""
        a = [_row(100, 10.0, exp="2026-08-21")]
        b = [_row(100, 30.0, exp="2026-08-21")]

        diff = rs.shift_rows(a, b, restrict_expirations=[date(2026, 12, 31)])

        assert diff.rows == ()

    def test_rows_without_expirations_still_difference(self):
        """Older frame payloads carry no expiration column; the diff should
        still work, with no roll-off caveat to report."""
        a = [{"strike": 100, "net_gex": 10.0}]
        b = [{"strike": 100, "net_gex": 40.0}]

        diff = rs.shift_rows(a, b)

        assert [r.d_net for r in diff.rows] == pytest.approx([30.0])
        assert diff.expired_expirations == ()

    def test_accepts_the_net_gamma_column_vocabulary(self):
        """strike-profile-timeseries says ``net_gamma`` for the same quantity."""
        a = [{"strike": 100, "expiration": "2026-08-21", "net_gamma": 10.0}]
        b = [{"strike": 100, "expiration": "2026-08-21", "net_gamma": 25.0}]

        diff = rs.shift_rows(a, b)

        assert [r.d_net for r in diff.rows] == pytest.approx([15.0])


class TestPositioningSplit:
    def test_pure_repricing_shows_no_positioning(self):
        """Same contracts, richer gamma: positioning ~0, repricing carries it."""
        a = [_row(100, 100.0, call_oi=1000, call_gex=100.0, put_gex=0.0)]
        b = [_row(100, 150.0, call_oi=1000, call_gex=150.0, put_gex=0.0)]

        row = rs.shift_rows(a, b).rows[0]

        assert row.positioning == pytest.approx(0.0)
        assert row.repricing == pytest.approx(50.0)

    def test_pure_oi_growth_is_all_positioning(self):
        """Contracts doubled at the same per-contract gamma."""
        a = [_row(100, 100.0, call_oi=1000, call_gex=100.0, put_gex=0.0)]
        b = [_row(100, 200.0, call_oi=2000, call_gex=200.0, put_gex=0.0)]

        row = rs.shift_rows(a, b).rows[0]

        assert row.positioning == pytest.approx(100.0)
        assert row.repricing == pytest.approx(0.0)

    def test_zero_open_interest_on_both_sides_is_not_a_divide_by_zero(self):
        a = [_row(100, 0.0, call_oi=0, put_oi=0, call_gex=0.0, put_gex=0.0)]
        b = [_row(100, 0.0, call_oi=0, put_oi=0, call_gex=0.0, put_gex=0.0)]

        row = rs.shift_rows(a, b).rows[0]

        assert row.positioning == 0.0


# --------------------------------------------------------------------------- #
# 2. The roll-off
# --------------------------------------------------------------------------- #
class TestRolloff:
    def test_nearest_expiration_is_the_next_tranche(self):
        rows = [
            _row(100, 300.0, exp="2026-08-28"),
            _row(100, 700.0, exp="2026-08-21"),
        ]

        out = rs.rolloff_tranches(rows, as_of=date(2026, 8, 21))

        assert out.next_tranche is not None
        assert out.next_tranche.expiration == date(2026, 8, 21)
        assert out.next_tranche.dte == 0
        assert out.next_tranche.net_gex == pytest.approx(700.0)
        assert out.next_tranche.share == pytest.approx(0.7)

    def test_share_is_measured_on_absolute_gamma(self):
        """An internally offsetting tranche is still a large tranche — all of
        it leaves at the close, so it must not net itself down to ~zero."""
        rows = [
            _row(100, 500.0, exp="2026-08-21"),
            _row(90, -500.0, exp="2026-08-21"),
            _row(100, 200.0, exp="2026-09-18"),
        ]

        out = rs.rolloff_tranches(rows, as_of=date(2026, 8, 21))

        assert out.next_tranche.net_gex == pytest.approx(0.0)
        assert out.next_tranche.abs_gex == pytest.approx(1000.0)
        assert out.next_tranche.share == pytest.approx(1000.0 / 1200.0)

    def test_shares_sum_to_one_with_a_folded_tail(self):
        rows = [
            _row(100, 100.0, exp=f"2026-09-{day:02d}")
            for day in range(1, 13)  # 12 expirations, ladder caps at 8
        ]

        out = rs.rolloff_tranches(rows, as_of=date(2026, 8, 21), max_tranches=8)

        assert len(out.tranches) == 9  # 8 + the folded "later" row
        assert sum(t.share for t in out.tranches) == pytest.approx(1.0)
        assert out.tranches[-1].abs_gex == pytest.approx(400.0)

    def test_next_by_strike_is_strike_ordered_and_limited(self):
        rows = [_row(strike, float(strike), exp="2026-08-21") for strike in range(90, 130)]

        out = rs.rolloff_tranches(
            rows, as_of=date(2026, 8, 21), next_strike_limit=5
        )

        strikes = [k for k, _ in out.next_by_strike]
        assert len(strikes) == 5
        assert strikes == sorted(strikes)
        # The limit keeps the LARGEST exposures, then re-sorts them by strike.
        assert strikes == [125.0, 126.0, 127.0, 128.0, 129.0]

    def test_empty_chain_is_not_an_error(self):
        out = rs.rolloff_tranches([], as_of=date(2026, 8, 21))
        assert out.tranches == ()
        assert out.next_tranche is None

    def test_rows_without_an_expiration_are_ignored(self):
        out = rs.rolloff_tranches(
            [{"strike": 100, "net_gex": 999.0}], as_of=date(2026, 8, 21)
        )
        assert out.next_tranche is None


# --------------------------------------------------------------------------- #
# 3. The read
# --------------------------------------------------------------------------- #
class TestWeightedScores:
    def test_lean_flips_sign_across_spot(self):
        """THE headline property.

        The same dollar build is supportive below spot and capping above it.
        A view that colours both the same — which is what a raw dNet ladder
        does — is the single biggest misread in the current page.
        """
        spot = 100.0
        below = rs.weighted_scores(_diff_rows({99: 0.0}, {99: 100.0}), spot)
        above = rs.weighted_scores(_diff_rows({101: 0.0}, {101: 100.0}), spot)

        assert below.lean > 0
        assert above.lean < 0
        assert below.lean == pytest.approx(-above.lean)
        # ...while BOTH read as stabilizing: gamma near spot went up either way.
        assert below.stability > 0
        assert above.stability > 0

    def test_stability_is_negative_when_gamma_is_shed_near_spot(self):
        scores = rs.weighted_scores(_diff_rows({100: 500.0}, {100: 100.0}), 100.0)
        assert scores.stability < 0

    def test_far_strikes_barely_move_the_score(self):
        """Unreachable gamma must not drive the read: a build 8% out of the
        money changes nothing about how the tape trades today."""
        spot = 100.0
        near = rs.weighted_scores(_diff_rows({100: 0.0}, {100: 100.0}), spot)
        far = rs.weighted_scores(_diff_rows({108: 0.0}, {108: 100.0}), spot)

        assert abs(far.stability) < abs(near.stability) * 0.01

    def test_at_the_money_build_is_stability_only(self):
        scores = rs.weighted_scores(_diff_rows({100: 0.0}, {100: 100.0}), 100.0)
        assert scores.lean == pytest.approx(0.0)
        assert scores.stability > 0

    def test_net_and_gross_shift_are_unweighted_totals(self):
        scores = rs.weighted_scores(
            _diff_rows({100: 0.0, 108: 0.0}, {100: 100.0, 108: -40.0}), 100.0
        )
        assert scores.net_shift == pytest.approx(60.0)
        assert scores.gross_shift == pytest.approx(140.0)

    def test_positioning_lens_scores_the_oi_component(self):
        a = [_row(99, 100.0, call_oi=1000, call_gex=100.0, put_gex=0.0)]
        b = [_row(99, 400.0, call_oi=1000, call_gex=400.0, put_gex=0.0)]
        rows = rs.shift_rows(a, b).rows

        total = rs.weighted_scores(rows, 100.0, use_positioning=False)
        positioning = rs.weighted_scores(rows, 100.0, use_positioning=True)

        # All re-pricing, no contracts traded: the strict lens reads ~zero.
        assert total.lean > 0
        assert positioning.lean == pytest.approx(0.0)

    def test_sigma_defaults_to_one_percent_of_spot(self):
        scores = rs.weighted_scores(_diff_rows({100: 0.0}, {100: 1.0}), 700.0)
        assert scores.sigma_price == pytest.approx(7.0)

    def test_explicit_sigma_overrides_the_default(self):
        scores = rs.weighted_scores(
            _diff_rows({100: 0.0}, {100: 1.0}), 700.0, sigma_price=14.0
        )
        assert scores.sigma_price == pytest.approx(14.0)

    def test_zero_spot_does_not_divide_by_zero(self):
        scores = rs.weighted_scores(_diff_rows({100: 0.0}, {100: 1.0}), 0.0)
        assert math.isfinite(scores.stability)


class TestClassify:
    @pytest.mark.parametrize(
        "lean_z,stability_z,expected",
        [
            (2.0, 2.0, "FIRMING"),
            (-2.0, 2.0, "CAPPING"),
            (2.0, -2.0, "FRAGILE_BID"),
            (-2.0, -2.0, "DETERIORATING"),
            (0.1, 0.1, "QUIET"),
        ],
    )
    def test_quadrants(self, lean_z, stability_z, expected):
        assert rs.classify(lean_z, stability_z).state == expected

    def test_quiet_wins_inside_the_noise_band_regardless_of_quadrant(self):
        """Naming a direction for a sub-noise move is how a read loses trust."""
        read = rs.classify(-0.4, -0.4)
        assert read.magnitude < rs.QUIET_Z
        assert read.state == "QUIET"

    def test_magnitude_is_the_vector_length(self):
        assert rs.classify(3.0, 4.0).magnitude == pytest.approx(5.0)

    @pytest.mark.parametrize(
        "magnitude,expected",
        [(0.2, "barely"), (1.0, "modestly"), (2.0, "clearly"), (3.5, "dramatically")],
    )
    def test_adverb_ladder(self, magnitude, expected):
        assert rs.adverb_for(magnitude) == expected

    def test_every_state_has_a_meaning(self):
        for lean, stab in [(2, 2), (-2, 2), (2, -2), (-2, -2), (0, 0)]:
            assert rs.classify(lean, stab).meaning


class TestConcentrationBand:
    def test_finds_the_tight_band_carrying_the_change(self):
        """The band is the NARROWEST run clearing the coverage threshold.

        Here two of the three big strikes already carry 64% of the change, so
        765-766 is the honest answer and 765-767 would be needlessly loose.
        """
        deltas = {k: 1.0 for k in range(740, 790)}
        deltas.update({765: 400.0, 766: 400.0, 767: 400.0})
        rows = _diff_rows({k: 0.0 for k in deltas}, deltas)

        band = rs.concentration_band(rows, spot=765.0)

        assert band.resolved
        assert band.low == 765.0
        assert band.high == 766.0
        assert band.share >= rs.BAND_COVERAGE

    def test_diffuse_change_is_reported_unresolved(self):
        """Spread evenly across the chain there IS no band, and quoting one
        would invent a level that is not there."""
        deltas = {k: 100.0 for k in range(700, 800)}
        rows = _diff_rows({k: 0.0 for k in deltas}, deltas)

        band = rs.concentration_band(rows, spot=750.0)

        assert not band.resolved

    def test_no_change_at_all_is_unresolved(self):
        rows = _diff_rows({100: 5.0, 101: 5.0}, {100: 5.0, 101: 5.0})
        band = rs.concentration_band(rows, spot=100.0)
        assert not band.resolved
        assert band.share == 0.0

    def test_a_single_dominant_strike_collapses_to_a_point_band(self):
        deltas = {764: 1.0, 765: 1000.0, 766: 1.0}
        rows = _diff_rows({k: 0.0 for k in deltas}, deltas)

        band = rs.concentration_band(rows, spot=765.0)

        assert band.resolved
        assert band.low == band.high == 765.0


# --------------------------------------------------------------------------- #
# Normalization
# --------------------------------------------------------------------------- #
class TestNormalization:
    def test_zscore_divides_by_sigma(self):
        assert rs.zscore(300.0, 100.0) == pytest.approx(3.0)

    def test_zscore_clamps_for_display(self):
        """A degraded snapshot must not take the plane's axis scale with it."""
        assert rs.zscore(1e9, 1.0) == 6.0
        assert rs.zscore(-1e9, 1.0) == -6.0

    @pytest.mark.parametrize("sigma", [None, 0.0, -1.0, float("nan")])
    def test_zscore_without_a_usable_sigma_is_zero(self, sigma):
        assert rs.zscore(500.0, sigma) == 0.0

    def test_stdev_needs_at_least_two_points(self):
        assert rs.stdev([]) is None
        assert rs.stdev([1.0]) is None
        assert rs.stdev([2.0, 4.0]) == pytest.approx(1.0)

    def test_stdev_of_a_constant_series_is_none_not_zero(self):
        """Zero would make every z-score infinite; None routes the caller to
        its proxy instead."""
        assert rs.stdev([5.0, 5.0, 5.0]) is None

    def test_stdev_ignores_non_finite_values(self):
        assert rs.stdev([2.0, 4.0, float("nan"), float("inf")]) == pytest.approx(1.0)

    def test_percentile_of(self):
        pop = [1.0, 2.0, 3.0, 4.0]
        assert rs.percentile_of(4.0, pop) == pytest.approx(1.0)
        assert rs.percentile_of(2.0, pop) == pytest.approx(0.5)
        assert rs.percentile_of(0.0, pop) == pytest.approx(0.0)

    def test_percentile_of_an_empty_population_is_none(self):
        assert rs.percentile_of(1.0, []) is None


# --------------------------------------------------------------------------- #
# End-to-end: the published SPY session, reproduced
# --------------------------------------------------------------------------- #

#: The Aug-21 SPY column as published, strike -> net dealer GEX in $M.  Used
#: as one realistic surface rather than a hand-tuned fixture, so the
#: assertions below are about behaviour on real data.
_AUG21_NET_GEX_MM: dict[int, float] = {
    748: -16, 749: -18, 750: -255, 751: -42, 752: -122, 753: -25, 754: -33,
    755: -527, 756: 104, 757: -73, 758: -193, 759: -41, 760: 38, 761: -193,
    762: -218, 763: 349, 764: 202, 765: 1500, 766: 972, 767: 814, 768: 415,
    769: 410, 770: 1200, 771: 283, 772: 202, 773: 169, 774: 85, 775: 299,
    776: 52, 777: 41, 778: 20, 779: 22, 780: 111, 781: 8, 782: 12, 783: -12,
}


def _aug21_rows():
    """The published surface as a shift from a flat prior session."""
    flat = {k: 0.0 for k in _AUG21_NET_GEX_MM}
    return _diff_rows(flat, _AUG21_NET_GEX_MM)


def test_published_session_reads_strongly_stabilizing():
    """Whatever spot does, a long-gamma stack parked on it is stabilizing.

    Stability is the unambiguous half of the read: dealers hedging against
    moves near spot suppress vol regardless of which side the strikes sit on.
    """
    rows = _aug21_rows()
    for spot in (761.0, 765.4, 768.0, 772.0):
        assert rs.weighted_scores(rows, spot).stability > 0


def test_published_session_lean_follows_which_side_of_spot_the_build_lands_on():
    """The directional half is positional, and that is the whole point.

    The same surface is CAPPING when spot sits under the 764-770 stack (it is
    a ceiling) and FIRMING once spot trades up through it (it is now a
    floor).  A view that colours the build green either way — which is what a
    raw dNet ladder does — cannot express this at all.

    Note this is stricter than the loose vernacular that calls any positive-
    gamma regime "bullish": positive gamma near spot is *stabilizing*, and
    whether it is *supportive* depends on where it sits.
    """
    rows = _aug21_rows()
    sigma = 400.0  # $M, a plausible session-scale denominator

    def read_at(spot):
        s = rs.weighted_scores(rows, spot)
        return rs.classify(rs.zscore(s.lean, sigma), rs.zscore(s.stability, sigma))

    assert read_at(765.4).state == "CAPPING"
    assert read_at(772.0).state == "FIRMING"
    # Both are large moves; only the direction changed.
    assert read_at(765.4).adverb == "dramatically"
    assert read_at(772.0).adverb == "dramatically"


def test_published_session_names_the_band_the_post_named():
    """The concentration band reproduces the "765-770" a human typed by hand,
    and does so identically at every plausible spot — the band is a property
    of the change, not of where price happens to be."""
    rows = _aug21_rows()
    bands = [rs.concentration_band(rows, spot=s) for s in (761.0, 765.4, 768.0, 772.0)]

    for band in bands:
        assert band.resolved
        assert band.low == 764.0
        assert band.high == 770.0
        assert band.share >= rs.BAND_COVERAGE
