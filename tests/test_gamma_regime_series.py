"""The intraday Gamma Shift series.

The maths is regime_shift's and is tested there; what is tested here is the
series construction — that the anchored and rolling lenses mean what the
module says they mean, that the incremental writer agrees with the batch
build, and that the two documented traps are actually avoided:

* rolling readings must NOT be treated as summing to the anchored one (the
  proximity kernel re-centres on each bar's own spot);
* an expiry rolling off mid-session must be reported, never booked as
  dealers shedding gamma.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from src.analytics.gamma_regime_series import (
    DEFAULT_ROLLING_BARS,
    ChainSnapshot,
    build_latest_bar,
    build_series,
)

UTC = timezone.utc
T0 = datetime(2026, 4, 24, 13, 30, tzinfo=UTC)
EXP = date(2026, 4, 24)
FAR = date(2026, 5, 15)


def row(strike: float, net: float, expiration: date = EXP) -> dict:
    return {
        "strike": strike,
        "expiration": expiration,
        "net_gex": net,
        "call_gamma": max(net, 0.0),
        "put_gamma": min(net, 0.0),
        "call_oi": 100,
        "put_oi": 100,
    }


def snap(i: int, rows, spot: float = 700.0) -> ChainSnapshot:
    return ChainSnapshot(bar_start=T0 + timedelta(minutes=5 * i), spot=spot, rows=rows)


FLAT = [row(690, 1e6), row(700, 2e6), row(710, 1e6)]


# --------------------------------------------------------------------------- #
# Shape
# --------------------------------------------------------------------------- #
def test_empty_input_yields_empty_series():
    assert build_series([]) == []


def test_one_bar_per_snapshot_including_the_anchor():
    snaps = [snap(i, FLAT) for i in range(10)]
    series = build_series(snaps)

    assert len(series) == 10
    assert [b.bar_start for b in series] == [s.bar_start for s in snaps]


def test_anchor_bar_has_zero_anchored_shift():
    """Comparing the anchor to itself is no change — but it is still a bar."""
    series = build_series([snap(i, FLAT) for i in range(3)])

    assert series[0].anchored_stability == 0.0
    assert series[0].anchored_lean == 0.0
    assert series[0].spot == 700.0


def test_rolling_is_none_until_the_lookback_exists():
    series = build_series([snap(i, FLAT) for i in range(DEFAULT_ROLLING_BARS + 3)])

    for bar in series[:DEFAULT_ROLLING_BARS]:
        assert bar.rolling_stability is None
        assert bar.rolling_lean is None
    for bar in series[DEFAULT_ROLLING_BARS:]:
        assert bar.rolling_stability is not None


def test_rolling_window_is_configurable():
    series = build_series([snap(i, FLAT) for i in range(6)], rolling_bars=2)

    assert series[1].rolling_stability is None
    assert series[2].rolling_stability is not None


# --------------------------------------------------------------------------- #
# The scores mean what the docstring says
# --------------------------------------------------------------------------- #
def test_gamma_building_near_spot_reads_as_stabilizing():
    """More long gamma at spot = dealers hedge against moves = pinning."""
    before = [row(700, 1e6)]
    after = [row(700, 5e6)]
    series = build_series([snap(0, before), snap(1, after)])

    assert series[1].anchored_stability > 0


def test_gamma_shedding_near_spot_reads_as_accelerant():
    before = [row(700, 5e6)]
    after = [row(700, 1e6)]
    series = build_series([snap(0, before), snap(1, after)])

    assert series[1].anchored_stability < 0


def test_building_below_spot_is_supportive_lean():
    before = [row(690, 1e6)]
    after = [row(690, 5e6)]
    series = build_series([snap(0, before), snap(1, after)])

    assert series[1].anchored_lean > 0


def test_building_above_spot_is_capping_lean():
    """The same build, on the other side of spot, means the opposite thing —
    the distinction a raw signed ladder throws away."""
    before = [row(710, 1e6)]
    after = [row(710, 5e6)]
    series = build_series([snap(0, before), snap(1, after)])

    assert series[1].anchored_lean < 0


def test_a_quiet_book_scores_near_zero():
    series = build_series([snap(i, FLAT) for i in range(8)])

    assert series[-1].anchored_stability == pytest.approx(0.0)
    assert series[-1].rolling_stability == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# The two documented traps
# --------------------------------------------------------------------------- #
def test_rolling_readings_do_not_sum_to_the_anchored_one():
    """The kernel re-centres on each bar's own spot, so bar-to-bar diffs are
    not additive. Pinning this stops someone 'optimising' the anchored lens
    into a running sum of the rolling one."""
    # Structure builds steadily while spot walks away from it.
    snaps = [
        snap(i, [row(700, 1e6 * (i + 1)), row(690, 5e5)], spot=700.0 + 2.0 * i) for i in range(5)
    ]
    series = build_series(snaps, rolling_bars=1)

    summed = sum(b.rolling_stability or 0.0 for b in series)
    anchored = series[-1].anchored_stability

    assert anchored != pytest.approx(summed, rel=1e-6)


def test_expiry_rolloff_is_reported_not_booked_as_a_shed():
    """An expiry leaving the board is not dealers selling gamma. The diff must
    exclude it and say so."""
    before = [row(700, 3e6, EXP), row(700, 2e6, FAR)]
    after = [row(700, 2e6, FAR)]  # EXP has rolled off
    series = build_series([snap(0, before), snap(1, after)])

    assert EXP in series[1].expired_expirations
    # The far expiry is unchanged, so the measured shift is nothing.
    assert series[1].anchored_stability == pytest.approx(0.0)


def test_a_genuine_shed_in_a_surviving_expiry_still_registers():
    """Guard against the previous test's fix swallowing real changes."""
    before = [row(700, 3e6, EXP), row(700, 4e6, FAR)]
    after = [row(700, 1e6, FAR)]
    series = build_series([snap(0, before), snap(1, after)])

    assert EXP in series[1].expired_expirations
    assert series[1].anchored_stability < 0


# --------------------------------------------------------------------------- #
# The incremental writer agrees with the batch build
# --------------------------------------------------------------------------- #
def test_incremental_bar_matches_the_batch_build():
    """The engine writes one bar per cycle; if that disagreed with a rebuild,
    a backfill would silently rewrite history."""
    snaps = [
        snap(i, [row(700, 1e6 + 2e5 * i), row(710, 8e5)], spot=700.0 + i)
        for i in range(DEFAULT_ROLLING_BARS + 4)
    ]
    series = build_series(snaps)

    idx = len(snaps) - 1
    incremental = build_latest_bar(
        anchor=snaps[0],
        lookback=snaps[idx - DEFAULT_ROLLING_BARS],
        current=snaps[idx],
    )

    assert incremental == series[idx]


def test_incremental_bar_without_a_lookback_matches_early_session():
    snaps = [snap(i, [row(700, 1e6 + 1e5 * i)]) for i in range(3)]
    series = build_series(snaps)

    incremental = build_latest_bar(anchor=snaps[0], lookback=None, current=snaps[2])

    assert incremental == series[2]
    assert incremental.rolling_stability is None


# --------------------------------------------------------------------------- #
# Expiration filter
# --------------------------------------------------------------------------- #
def test_restricting_expirations_scopes_the_diff():
    before = [row(700, 1e6, EXP), row(700, 1e6, FAR)]
    after = [row(700, 5e6, EXP), row(700, 1e6, FAR)]

    all_exps = build_series([snap(0, before), snap(1, after)])
    far_only = build_series([snap(0, before), snap(1, after)], restrict_expirations=[FAR])

    assert all_exps[1].anchored_stability > 0
    assert far_only[1].anchored_stability == pytest.approx(0.0)
