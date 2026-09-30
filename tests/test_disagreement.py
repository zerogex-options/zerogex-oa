"""Unit tests for the pressure-versus-structure disagreement study.

Every number this produces rests on two judgment calls, and both are the kind
that look fine forever if they are wrong: that Lean is read relative to
pressure's direction rather than absolutely, and that the window starts one bar
AFTER the signal rather than on it. Neither would raise anything. Both are
pinned here.
"""

from src.analytics import base_rates as br
from src.analytics import disagreement as d
from src.analytics import flip_cushion as fc
from src.analytics import gamma_weather as gw


def bar(
    pressure=gw.PRESSURE_BUYING,
    structure=d.CONTAIN,
    spot=100.0,
    typical_move=1.0,
    cushion_state=fc.STATE_SECURE,
    cushion_rate=None,
):
    return d.DisagreementBar(
        pressure=pressure,
        structure=structure,
        spot=spot,
        typical_move=typical_move,
        cushion_state=cushion_state,
        cushion_rate=cushion_rate,
    )


# ── the vote ──────────────────────────────────────────────────────────────────


def test_lean_is_read_against_pressure_not_absolutely():
    """The interpretive call the whole study rests on.

    A supportive book helps a buyer and resists a seller, so the SAME lean
    votes opposite ways depending on which side pressure is pushing. Reading it
    absolutely would put half the sample in the wrong arm and the number would
    look perfectly reasonable.
    """
    supportive_buying = d.structure_stance(
        gw.LEAN_SUPPORTIVE, gw.STRUCTURE_FLAT, gw.STRUCTURE_ACCELERATIVE, gw.PRESSURE_BUYING
    )
    supportive_selling = d.structure_stance(
        gw.LEAN_SUPPORTIVE, gw.STRUCTURE_FLAT, gw.STRUCTURE_PINNING, gw.PRESSURE_SELLING
    )
    assert supportive_buying == d.EXTEND
    assert supportive_selling == d.CONTAIN


def test_pinning_and_building_contain_regardless_of_side():
    # Neither has a side, so neither changes with pressure's direction.
    for side in (gw.PRESSURE_BUYING, gw.PRESSURE_SELLING):
        assert (
            d.structure_stance(None, gw.STRUCTURE_PINNING, gw.STRUCTURE_PINNING, side) == d.CONTAIN
        )
        assert (
            d.structure_stance(None, gw.STRUCTURE_ACCELERATIVE, gw.STRUCTURE_ACCELERATIVE, side)
            == d.EXTEND
        )


def test_flat_abstains_and_one_vote_is_not_a_read():
    # One component agreeing with nobody is a component, not a structure read.
    assert (
        d.structure_stance(None, gw.STRUCTURE_FLAT, gw.STRUCTURE_PINNING, gw.PRESSURE_BUYING)
        is None
    )
    # And a one-all tie has no majority to report.
    assert (
        d.structure_stance(
            None, gw.STRUCTURE_PINNING, gw.STRUCTURE_ACCELERATIVE, gw.PRESSURE_BUYING
        )
        is None
    )


def test_two_against_one_carries():
    assert (
        d.structure_stance(
            gw.LEAN_SUPPORTIVE, gw.STRUCTURE_PINNING, gw.STRUCTURE_PINNING, gw.PRESSURE_BUYING
        )
        == d.CONTAIN
    )


def test_no_confirmed_pressure_means_no_structure_read():
    # Lean cannot be placed without a side to place it against.
    assert (
        d.structure_stance(gw.LEAN_SUPPORTIVE, gw.STRUCTURE_PINNING, gw.STRUCTURE_PINNING, None)
        is None
    )


# ── what counts as pressure ───────────────────────────────────────────────────


def test_a_pulse_is_not_a_disagreement():
    """Explicit in the spec, and it is the line between a condition and noise."""
    assert d.confirmed_pressure(gw.PRESSURE_BUYING, gw.PERSISTENCE_PULSE) is None
    for persistence in (
        gw.PERSISTENCE_BUILDING,
        gw.PERSISTENCE_PERSISTENT,
        gw.PERSISTENCE_REVERSED,
    ):
        assert d.confirmed_pressure(gw.PRESSURE_BUYING, persistence) == gw.PRESSURE_BUYING


def test_mixed_pressure_has_no_side_to_disagree_with():
    assert d.confirmed_pressure(gw.PRESSURE_MIXED, gw.PERSISTENCE_PERSISTENT) is None


# ── anchoring ─────────────────────────────────────────────────────────────────


def test_a_run_counts_once_however_long_it_holds():
    """Twelve overlapping windows are one event, not twelve trials."""
    bars = [bar(structure=d.CONTAIN) for _ in range(12)]
    assert d.onsets(bars, d.DISAGREE) == [0]


def test_a_second_episode_counts_again():
    bars = (
        [bar(structure=d.CONTAIN)] * 3
        + [bar(structure=d.EXTEND)] * 2
        + [bar(structure=d.CONTAIN)] * 4
    )
    assert d.onsets(bars, d.DISAGREE) == [0, 5]
    assert d.onsets(bars, d.AGREE) == [3]


def test_a_gap_with_no_stance_still_separates_episodes():
    bars = [bar(structure=d.CONTAIN)] * 2 + [bar(pressure=None)] * 2 + [bar(structure=d.CONTAIN)]
    assert d.onsets(bars, d.DISAGREE) == [0, 4]


# ── the outcome ───────────────────────────────────────────────────────────────


def test_the_window_starts_on_the_bar_after_the_signal():
    """No look-ahead, and the reason the study is worth running at all.

    The signal for bar 0 exists once bar 0 has printed, so the first price
    anyone could act on is bar 1's. A move that happened during bar 0 must not
    count toward it. Here all of the move is inside the signal bar and none
    after, so this is a loss even though spot ended far higher than it started.
    """
    spots = [100.0, 110.0, 110.0, 110.0]
    bars = [bar(spot=s, typical_move=1.0) for s in spots]
    assert d.extension(bars, anchor=0, horizon_bars=2, threshold=1.0) is False


def test_a_move_after_the_signal_counts():
    spots = [100.0, 100.0, 101.0, 103.0]
    bars = [bar(spot=s, typical_move=1.0) for s in spots]
    # From bar 1 (100.0) to bar 3 (103.0) is three typical moves.
    assert d.extension(bars, anchor=0, horizon_bars=2, threshold=1.0) is True
    assert d.extension(bars, anchor=0, horizon_bars=2, threshold=4.0) is False


def test_selling_wins_when_price_falls():
    spots = [100.0, 100.0, 98.0]
    bars = [bar(pressure=gw.PRESSURE_SELLING, spot=s, typical_move=1.0) for s in spots]
    assert d.extension(bars, anchor=0, horizon_bars=1, threshold=1.0) is True

    rising = [
        bar(pressure=gw.PRESSURE_SELLING, spot=s, typical_move=1.0) for s in [100.0, 100.0, 102.0]
    ]
    assert d.extension(rising, anchor=0, horizon_bars=1, threshold=1.0) is False


def test_a_window_past_the_session_is_unresolved_not_a_loss():
    """Counting a censored window as a failure would understate every rate."""
    bars = [bar() for _ in range(3)]
    assert d.extension(bars, anchor=0, horizon_bars=5) is None
    assert d.extension(bars, anchor=2, horizon_bars=1) is None


def test_a_bar_with_no_yardstick_is_unresolved():
    # Without a typical move there is no size to compare against, and a
    # fallback number would be a different measurement wearing the same name.
    bars = [bar(typical_move=None) for _ in range(5)]
    assert d.extension(bars, anchor=0, horizon_bars=2) is None

    zeroed = [bar(typical_move=0.0) for _ in range(5)]
    assert d.extension(zeroed, anchor=0, horizon_bars=2) is None


def test_a_missing_spot_is_unresolved():
    bars = [bar(), bar(spot=None), bar(), bar()]
    assert d.extension(bars, anchor=0, horizon_bars=2) is None


# ── the cushion cut ───────────────────────────────────────────────────────────


def test_only_secure_and_thin_narrowing_are_banded():
    assert d.cushion_band(bar(cushion_state=fc.STATE_SECURE)) == "secure"
    assert d.cushion_band(bar(cushion_state=fc.STATE_THIN, cushion_rate=-2.0)) == "thin, narrowing"
    # A thin cushion that is WIDENING is not the case the question is about.
    assert d.cushion_band(bar(cushion_state=fc.STATE_THIN, cushion_rate=1.5)) is None
    assert d.cushion_band(bar(cushion_state=fc.STATE_THIN, cushion_rate=None)) is None
    assert d.cushion_band(bar(cushion_state=fc.STATE_NORMAL)) is None
    assert d.cushion_band(bar(cushion_state=fc.STATE_NO_FLIP)) is None


# ── assembly ──────────────────────────────────────────────────────────────────


def test_trials_carry_both_arms_because_lift_needs_a_control():
    session = (
        [bar(structure=d.CONTAIN, spot=100.0)] * 2
        + [bar(structure=d.EXTEND, spot=100.0)] * 2
        + [bar(structure=d.EXTEND, spot=100.0)] * 2
    )
    out = d.trials([session], horizon_bars=1)
    assert set(out) == {d.DISAGREE, d.AGREE}
    assert len(out[d.DISAGREE]) == 1
    assert len(out[d.AGREE]) == 1


def test_the_cushion_split_drops_bars_it_cannot_band():
    session = [
        bar(structure=d.CONTAIN, cushion_state=fc.STATE_NORMAL),
        bar(structure=d.EXTEND, cushion_state=fc.STATE_SECURE),
        bar(structure=d.EXTEND, cushion_state=fc.STATE_SECURE),
    ]
    out = d.trials_by_cushion([session], horizon_bars=1)
    assert "DISAGREE · secure" not in out
    assert "AGREE · secure" in out


# ── the report section ────────────────────────────────────────────────────────
#
# The assembly and the printer, without a database. A formatting error in a
# report only surfaces when someone runs it on the box, which is the worst
# place to find one.


class _FakeLoaded:
    """Stands in for a LoadedSession, which needs a database to build."""

    def __init__(self, bars):
        self._bars = bars

    def disagreement_bars(self, confirm_bars):  # noqa: ARG002 - signature match
        return self._bars


def _run(spots, structure, pressure=gw.PRESSURE_BUYING, **kw):
    return [bar(pressure=pressure, structure=structure, spot=s, **kw) for s in spots]


def test_the_section_reports_both_arms_and_never_a_p_value():
    from src.tools import gamma_weather_base_rates as tool

    # One disagreement that extends, one agreement that does not.
    session = _run([100.0, 100.0, 105.0, 105.0], d.CONTAIN) + _run(
        [105.0, 105.0, 105.0, 105.0], d.EXTEND
    )
    study = tool.disagreement_tables([_FakeLoaded(session)], confirm_bars=2)

    groups = {row.group for row in study["main"]}
    assert groups == {d.DISAGREE, d.AGREE}
    # Overlap makes significance meaningless here, so it is never offered.
    assert all(row.p_value is None for row in study["main"])
    assert all(row.verdict == br.VERDICT_DESCRIPTIVE for row in study["main"])


def test_the_sweep_moves_the_bar_on_the_same_anchors():
    from src.tools import gamma_weather_base_rates as tool

    # A one-unit move: counts as extended at 0.5, not at 1.5.
    session = _run([100.0, 100.0, 101.0, 101.0, 101.0, 101.0, 101.0], d.CONTAIN)
    study = tool.disagreement_tables([_FakeLoaded(session)], confirm_bars=2)

    by_level = {row["extension"]: row for row in study["sweep"]}
    assert set(by_level) == set(d.EXTENSION_SWEEP)
    # Same anchor count at every threshold; only the outcome moves.
    counts = {row["disagree"].n for row in study["sweep"]}
    assert len(counts) == 1, "moving the bar must not change how many trials there are"
    assert by_level[0.5]["disagree"].hits >= by_level[1.5]["disagree"].hits


def _stub_session():
    """The smallest br.Session format_report will print rather than skip."""
    from datetime import datetime, timedelta, timezone

    start = datetime(2026, 9, 28, 13, 30, tzinfo=timezone.utc)
    states = ["A"] * 40
    return br.Session(
        label="2026-09-28",
        bar_starts=[start + timedelta(minutes=5 * i) for i in range(len(states))],
        states=states,
        warnings=[False] * len(states),
        ages=[],
        warmup=0,
    )


def test_the_printer_renders_the_section():
    from src.tools import gamma_weather_base_rates as tool

    report = tool.build_report([_stub_session()], horizon_bars=6)
    report["disagreement"] = tool.disagreement_tables(
        [_FakeLoaded(_run([100.0] * 8, d.CONTAIN))], confirm_bars=2
    )
    text = tool.format_report("SPY", report, skipped=[])

    assert "9. DOES PRESSURE WIN WHEN STRUCTURE SAYS CONTAIN?" in text
    assert "x typical move" in text
    assert "Split by cushion" in text
    # The honesty line has to survive any future edit to the header block.
    assert "read the gap, not its significance" in text


def test_a_report_without_the_study_still_prints():
    # The section is attached by main(), so anything calling build_report on its
    # own must not trip over its absence.
    from src.tools import gamma_weather_base_rates as tool

    text = tool.format_report(
        "SPY", tool.build_report([_stub_session()], horizon_bars=6), skipped=[]
    )
    assert "9. DOES PRESSURE WIN" not in text
