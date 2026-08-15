"""Tests for src.jobs.level_history — the bulletin write-ups' intraday
level-path tracking.

The behaviour these pin down is the fix for a real bad post: a Post-Market
Read that quoted "765 → Put Wall (never tested)" when the put wall had
actually walked 777 → 776 → 775 during the session, breaking the first two
and bouncing off the third, and only reset to 765 after the bell once the
day's 0DTE rolled off the chain.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from src.jobs import level_history as lh

ET = ZoneInfo("America/New_York")
SESSION = date(2026, 8, 13)


def _ts(hour: int, minute: int) -> datetime:
    """An ET wall-clock moment on the session date, as an aware datetime.

    Minutes past 59 roll into the hour so callers can build windows
    arithmetically (``9, 30 + n * 20``)."""
    base = datetime(SESSION.year, SESSION.month, SESSION.day, hour, 0, tzinfo=ET)
    return base + timedelta(minutes=minute)


def _levels(
    start: tuple[int, int],
    end: tuple[int, int],
    *,
    put_wall: float | None = None,
    call_wall: float | None = None,
    gamma_flip: float | None = None,
    max_pain: float | None = None,
    net_gex_at_spot: float | None = None,
    step_minutes: int = 5,
) -> list[dict]:
    """Per-minute-ish gex_summary rows holding one structure over a window."""
    rows = []
    cursor = _ts(*start)
    last = _ts(*end)
    while cursor <= last:
        rows.append(
            {
                "timestamp": cursor,
                "put_wall": put_wall,
                "call_wall": call_wall,
                "gamma_flip": gamma_flip,
                "max_pain": max_pain,
                "net_gex_at_spot": net_gex_at_spot,
            }
        )
        cursor += timedelta(minutes=step_minutes)
    return rows


def _bar(hour: int, minute: int, low: float, high: float, close: float) -> dict:
    return {"timestamp": _ts(hour, minute), "low": low, "high": high, "close": close}


# The session the bad post described: the put wall rolls down a strike at a
# time, price breaks the first two and turns on the third, the call wall never
# moves, and the whole thing re-prices after the bell.
def _spy_session_levels() -> list[dict]:
    return (
        _levels((9, 30), (10, 30), put_wall=777, call_wall=780, gamma_flip=769.75)
        + _levels((10, 35), (12, 0), put_wall=776, call_wall=780, gamma_flip=769.75)
        + _levels((12, 5), (16, 0), put_wall=775, call_wall=780, gamma_flip=769.75)
    )


def _spy_post_close_levels() -> list[dict]:
    # 0DTE rolls off → the put wall resets three strikes lower for tomorrow.
    # The flip barely moves, so it must NOT be reported as a reset.
    return _levels((16, 5), (16, 30), put_wall=765, call_wall=780, gamma_flip=769.80)


def _spy_bars() -> list[dict]:
    return [
        _bar(9, 30, 778.20, 779.90, 778.40),  # morning push stalls at the call wall
        _bar(10, 0, 776.40, 778.60, 776.60),  # 777 gives way
        _bar(11, 0, 775.30, 776.90, 775.60),  # 776 gives way
        _bar(13, 0, 774.95, 776.10, 775.40),  # 775 tested, turns
        _bar(15, 55, 775.80, 776.90, 776.80),
    ]


def _history(mode: str = "close", *, bars: list[dict] | None = None) -> lh.LevelHistory:
    history = lh.build_level_history(
        "SPY",
        SESSION,
        mode,
        _spy_session_levels() + _spy_post_close_levels(),
        _spy_bars() if bars is None else bars,
    )
    assert history is not None
    return history


# ---------------------------------------------------------------------------
# Wall migration
# ---------------------------------------------------------------------------


def test_wall_migration_is_tracked_with_per_print_outcomes():
    history = _history()
    assert history.put_wall is not None
    assert history.put_wall.values == [777, 776, 775]
    assert [s.outcome for s in history.put_wall.segments] == ["broke", "broke", "held"]
    assert history.put_wall.changed is True
    assert history.any_change is True


def test_put_wall_note_states_what_happened_at_each_print():
    """The note that replaces the old, wrong "(never tested)"."""
    note = lh.note_for(_history(), "put_wall")
    assert note == "moved 777 → 776 → 775; both broke, 775 tested and held"


def test_unmoved_wall_notes_only_the_outcome():
    # The call wall sat at 780 all day and the 9:30 push stalled 10c under it,
    # inside the touch band — tested, not broken, not untested.
    assert lh.note_for(_history(), "call_wall") == "tested and held"


def test_untested_wall_is_reported_as_untested():
    """A level price never came near must never read as defended."""
    history = lh.build_level_history(
        "SPY",
        SESSION,
        "close",
        _levels((9, 30), (16, 0), put_wall=750, call_wall=780, gamma_flip=769.75),
        _spy_bars(),
    )
    assert history is not None
    assert history.put_wall is not None
    assert history.put_wall.segments[0].outcome == "untested"
    assert lh.note_for(history, "put_wall") == "never tested"


def test_single_earlier_print_reads_naturally():
    history = lh.build_level_history(
        "SPY",
        SESSION,
        "close",
        _levels((9, 30), (11, 0), put_wall=777, gamma_flip=769.75)
        + _levels((11, 5), (16, 0), put_wall=775, gamma_flip=769.75),
        _spy_bars(),
    )
    assert history is not None
    assert (
        lh.note_for(history, "put_wall") == "moved 777 → 775; the first broke, 775 tested and held"
    )


def test_flicker_between_near_tied_strikes_is_pruned():
    """A one-minute wall blip is noise, not a migration worth reporting."""
    rows = (
        _levels((9, 30), (11, 0), put_wall=777, gamma_flip=769.75)
        + _levels((11, 1), (11, 1), put_wall=776, gamma_flip=769.75)
        + _levels((11, 2), (16, 0), put_wall=777, gamma_flip=769.75)
    )
    history = lh.build_level_history("SPY", SESSION, "close", rows, _spy_bars())
    assert history is not None
    assert history.put_wall is not None
    assert history.put_wall.values == [777]
    assert history.put_wall.changed is False


def test_long_chain_elides_the_middle():
    rows: list[dict] = []
    for idx, wall in enumerate((780, 779, 778, 777, 776, 775)):
        rows += _levels((9, 30 + idx * 20), (9, 45 + idx * 20), put_wall=wall, gamma_flip=769.75)
    history = lh.build_level_history("SPY", SESSION, "close", rows, _spy_bars())
    assert history is not None
    assert history.put_wall is not None
    assert history.put_wall.values == [780, 779, 778, 777, 776, 775]
    note = lh.note_for(history, "put_wall")
    assert note.startswith("moved 780 → … → 777 → 776 → 775;")


def test_outcomes_are_unknown_without_bars():
    """No tape to judge by → describe the migration, claim nothing about it."""
    history = _history(bars=[])
    assert history.put_wall is not None
    assert {s.outcome for s in history.put_wall.segments} == {"unknown"}
    assert lh.note_for(history, "put_wall") == "moved 777 → 776 → 775"


def test_outcome_is_scoped_to_the_window_the_level_was_in_force():
    """Price trading through 775 in the morning — before 775 was the wall —
    must not be counted against the 775 print that only formed at noon."""
    rows = _levels((9, 30), (11, 0), put_wall=790, gamma_flip=769.75) + _levels(
        (12, 5), (16, 0), put_wall=775, gamma_flip=769.75
    )
    bars = [
        _bar(9, 45, 770.00, 779.00, 771.00),  # deep below 775, but 790 is the wall
        _bar(13, 0, 776.50, 778.00, 777.00),  # never comes back to 775
    ]
    history = lh.build_level_history("SPY", SESSION, "close", rows, bars)
    assert history is not None
    assert history.put_wall is not None
    assert history.put_wall.segments[-1].value == 775
    assert history.put_wall.segments[-1].outcome == "untested"


# ---------------------------------------------------------------------------
# Gamma flip
# ---------------------------------------------------------------------------


def test_flip_reports_side_and_crossings():
    note = lh.note_for(_history(), "gamma_flip")
    assert note == "spot held above all session"
    assert _history().gamma_flip is not None
    assert _history().gamma_flip.crossings == 0


def test_flip_counts_crossings_and_reports_drift():
    rows = _levels((9, 30), (12, 0), gamma_flip=770.0, put_wall=775) + _levels(
        (12, 5), (16, 0), gamma_flip=772.0, put_wall=775
    )
    bars = [
        _bar(9, 45, 771.0, 773.0, 772.5),  # above
        _bar(10, 45, 767.0, 770.5, 768.0),  # below → 1 crossing
        _bar(13, 0, 772.5, 775.0, 774.0),  # above 772 → 2 crossings
        _bar(15, 55, 770.0, 773.0, 770.5),  # below 772 → 3 crossings
    ]
    history = lh.build_level_history("SPY", SESSION, "close", rows, bars)
    assert history is not None
    assert history.gamma_flip is not None
    assert history.gamma_flip.crossings == 3
    assert history.gamma_flip.side_start == "above"
    assert history.gamma_flip.side_end == "below"
    assert lh.note_for(history, "gamma_flip") == "drifted 770–772; spot crossed 3x, closed below"


def test_flip_grinding_on_the_line_does_not_register_crossings():
    """Closes inside the touch band leave the recorded side alone."""
    rows = _levels((9, 30), (16, 0), gamma_flip=770.0, put_wall=765)
    bars = [
        _bar(9, 45, 770.5, 771.5, 771.0),  # decisively above
        _bar(10, 45, 769.95, 770.10, 770.02),  # inside the band — not a cross
        _bar(15, 55, 770.4, 771.2, 771.1),
    ]
    history = lh.build_level_history("SPY", SESSION, "close", rows, bars)
    assert history is not None
    assert history.gamma_flip is not None
    assert history.gamma_flip.crossings == 0
    assert history.gamma_flip.side_end == "above"


# ---------------------------------------------------------------------------
# The post-close roll-off — the "765 was never in play" bug
# ---------------------------------------------------------------------------


def test_session_close_levels_exclude_the_post_bell_reset():
    history = _history()
    assert history.session_close_levels["put_wall"] == 775
    assert history.post_close_levels["put_wall"] == 765
    assert history.put_wall is not None
    assert history.put_wall.session_value == 775
    assert history.put_wall.post_close_value == 765
    assert history.put_wall.post_close_shift is True
    assert history.post_close_shifted is True


def test_post_close_line_names_only_the_levels_that_actually_reset():
    line = lh.post_close_line(_history())
    assert line == (
        "After the bell: today's 0DTE rolled off and Put Wall resets to 765. "
        "That's tomorrow's map, not today's tape."
    )
    # The call wall didn't move and the flip moved less than the touch band —
    # neither belongs in the line.
    assert "Call Wall" not in line
    assert "Gamma Flip" not in line


def test_no_post_close_line_when_the_chain_did_not_reprice():
    history = lh.build_level_history("SPY", SESSION, "close", _spy_session_levels(), _spy_bars())
    assert history is not None
    assert history.post_close_shifted is False
    assert lh.post_close_line(history) == ""


def test_prompt_dict_flags_the_roll_off_and_carries_the_path():
    payload = _history().to_prompt_dict()
    assert payload["levels_moved_during_session"] is True
    assert payload["structure_as_of"] == "16:00"
    assert [seg["value"] for seg in payload["put_wall"]["path"]] == [777, 776, 775]
    assert [seg["outcome"] for seg in payload["put_wall"]["path"]] == ["broke", "broke", "held"]
    assert payload["put_wall"]["at_session_close"] == 775
    assert payload["put_wall"]["after_the_bell"] == 765
    assert "0DTE" in payload["post_close_roll_off"]
    # The call wall never reset, so it carries no after-the-bell key at all.
    assert "after_the_bell" not in payload["call_wall"]


def test_quoted_values_cover_superseded_prints():
    """The invented-price guard has to accept a wall the tape has moved past."""
    values = _history().quoted_values()
    for expected in (777, 776, 775, 765, 780):
        assert expected in values


# ---------------------------------------------------------------------------
# Degradation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("rows", [None, [], _levels((9, 30), (9, 40), put_wall=775)])
def test_thin_or_missing_path_yields_no_history(rows):
    assert lh.build_level_history("SPY", SESSION, "close", rows, _spy_bars()) is None


def test_helpers_tolerate_a_none_history():
    assert lh.note_for(None, "put_wall") == ""
    assert lh.post_close_line(None) == ""


def test_pre_market_and_next_day_rows_are_ignored():
    stray = _levels((7, 0), (9, 25), put_wall=800, gamma_flip=769.75)
    for row in _levels((9, 30), (9, 45), put_wall=790, gamma_flip=769.75):
        row["timestamp"] = row["timestamp"] + timedelta(days=1)
        stray.append(row)
    history = lh.build_level_history(
        "SPY", SESSION, "close", stray + _spy_session_levels(), _spy_bars()
    )
    assert history is not None
    assert history.put_wall is not None
    assert history.put_wall.values == [777, 776, 775]
