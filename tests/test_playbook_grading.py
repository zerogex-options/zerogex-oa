"""The Playbook grader: every idea checked against what price did after it."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
import pytz

from src.signals.playbook import grading
from src.signals.playbook.grading import Idea

_ET = pytz.timezone("America/New_York")


def _et(h, m, day=22):
    return _ET.localize(datetime(2026, 9, day, h, m)).astimezone(timezone.utc)


def _bars(path, start=None):
    """1-minute bars from a list of closes, each with a 0.05 wick either side."""
    start = start or _et(9, 30)
    return [(start + timedelta(minutes=i), p, p + 0.05, p - 0.05, p) for i, p in enumerate(path)]


def _idea(**kw):
    base = dict(
        id=1,
        underlying="SPY",
        pattern="opening_range_break",
        action="BUY_CALL_DEBIT",
        tier="0DTE",
        direction="bullish",
        issued_at=_et(10, 0),
        entry_price=100.0,
        entry_trigger="at_market",
        target_price=101.0,
        stop_price=99.5,
        max_hold_minutes=60,
    )
    base.update(kw)
    return Idea(**base)


def _grade(idea, bars, now):
    return grading.grade_idea(idea, bars, [b[0] for b in bars], now)


# ----------------------------------------------------------------------
# R multiple
# ----------------------------------------------------------------------


def test_r_multiple_scores_in_units_of_risk():
    assert grading.r_multiple("bullish", 100, 101, 99.5, "target_hit", 101) == 2.0
    assert grading.r_multiple("bullish", 100, 101, 99.5, "stop_hit", 99.5) == -1.0
    assert grading.r_multiple("bearish", 100, 99, 100.5, "time_exit", 99.75) == 0.5
    # A time exit is clipped to the Card's own levels.
    assert grading.r_multiple("bullish", 100, 101, 99.5, "time_exit", 98.0) == -1.0


def test_r_multiple_without_a_price_stop_uses_the_target_distance():
    assert grading.r_multiple("bullish", 100, 102, None, "time_exit", 101) == 0.5
    assert grading.r_multiple("bullish", 100, 102, None, "target_hit", 102) == 1.0


def test_r_multiple_caps_a_hairline_stop():
    assert grading.r_multiple("bullish", 100, 105, 99.99, "target_hit", 105) == 5.0


def test_bad_geometry_is_unresolved():
    idea = _idea(target_price=99.0)  # bullish target below entry
    grade = _grade(idea, _bars([100.0] * 90), _et(12, 0))
    assert grade.outcome == "unresolved"


# ----------------------------------------------------------------------
# grade_idea
# ----------------------------------------------------------------------


def test_target_hit_is_graded_as_soon_as_it_happens():
    # Flat to 10:00, then up 0.1 a minute: 101 is touched (with the wick) at 10:09.
    path = [100.0] * 30 + [100.0 + 0.1 * i for i in range(1, 40)]
    grade = _grade(_idea(), _bars(path), now=_et(10, 20))
    assert grade.outcome == "target_hit"
    assert grade.r_multiple == 2.0
    assert grade.resolved_at == _et(10, 9)


def test_stop_hit():
    path = [100.0] * 30 + [100.0 - 0.1 * i for i in range(1, 40)]
    grade = _grade(_idea(), _bars(path), now=_et(10, 20))
    assert grade.outcome == "stop_hit"
    assert grade.r_multiple == -1.0


def test_open_idea_stays_pending_until_its_hold_ends():
    path = [100.0] * 90
    assert _grade(_idea(), _bars(path), now=_et(10, 30)) is None
    grade = _grade(_idea(), _bars(path), now=_et(11, 10))
    assert grade.outcome == "time_exit"
    assert grade.r_multiple == 0.0
    assert grade.resolved_at == _et(11, 0)


def test_time_exit_scores_where_price_ended():
    path = [100.0] * 30 + [100.25] * 70  # +0.25 against 0.5 of risk
    grade = _grade(_idea(), _bars(path), now=_et(11, 10))
    assert grade.outcome == "time_exit"
    assert grade.r_multiple == pytest.approx(0.5)


def test_0dte_hold_is_capped_at_the_close():
    idea = _idea(issued_at=_et(15, 30), max_hold_minutes=90)
    assert grading.effective_hold_minutes("0DTE", idea.issued_at, 90) == 30
    assert grading.effective_hold_minutes("swing", idea.issued_at, 90) == 90
    bars = _bars([100.0] * 60, start=_et(15, 0))
    grade = _grade(idea, bars, now=_et(16, 10))
    assert grade.outcome == "time_exit"
    assert grade.resolved_at == _et(16, 0)


def test_0dte_card_after_the_close_has_no_hold():
    assert grading.effective_hold_minutes("0DTE", _et(16, 30), 90) == 0


def test_touch_entry_that_never_fills():
    idea = _idea(entry_price=105.0, target_price=106.0, stop_price=104.0, entry_trigger="at_touch")
    grade = _grade(idea, _bars([100.0] * 120), now=_et(11, 30))
    assert grade.outcome == "no_fill"
    assert grade.r_multiple is None


def test_non_directional_card_is_not_price_gradable():
    grade = _grade(_idea(direction="non_directional"), _bars([100.0] * 90), _et(12, 0))
    assert grade.outcome == "unresolved"


def test_prior_move_reads_how_late_the_card_was():
    # 09:30-10:00 climbs 100 -> 101: a bullish Card at 10:00 is 1% late,
    # a bearish one is fading that move (-1%).
    path = [100.0 + i / 30 for i in range(31)] + [101.0] * 30
    bars = _bars(path)
    times = [b[0] for b in bars]
    moved = grading.prior_move_pct(bars, times, _et(10, 0), "bullish")
    assert moved == pytest.approx(0.01)
    assert grading.prior_move_pct(bars, times, _et(10, 0), "bearish") == pytest.approx(-0.01)


def test_prior_move_needs_bars_at_both_ends():
    bars = _bars([100.0] * 5, start=_et(9, 58))
    times = [b[0] for b in bars]
    assert grading.prior_move_pct(bars, times, _et(10, 0), "bullish") is None


# ----------------------------------------------------------------------
# sync_published_cards: repeats and off-session Cards
# ----------------------------------------------------------------------


class _Cursor:
    def __init__(self, conn):
        self.conn = conn
        self._rows = []

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        if "FROM signal_action_cards c" in sql:
            self._rows = self.conn.cards
        elif "SELECT DISTINCT ON (pattern)" in sql:
            self._rows = self.conn.prior_ideas
        else:
            self._rows = []

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, cards, prior_ideas=()):
        self.cards = cards
        self.prior_ideas = list(prior_ideas)
        self.executed = []
        self.commits = 0

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def inserts(self):
        return [p for s, p in self.executed if "INSERT INTO playbook_card_outcomes" in s]


def _card(card_id, ts, pattern="opening_range_break", direction="bullish", hold=120):
    payload = {
        "max_hold_minutes": hold,
        "entry": {"ref_price": 100.0, "trigger": "at_market"},
        "target": {"ref_price": 101.0 if direction == "bullish" else 99.0, "kind": "level"},
        "stop": {"ref_price": 99.5 if direction == "bullish" else 100.5, "kind": "level"},
    }
    return (card_id, ts, pattern, "BUY_CALL_DEBIT", "0DTE", direction, 0.5, json.dumps(payload))


def test_reissues_inside_the_hold_window_are_marked_repeats():
    conn = _Conn(
        [
            _card(1, _et(10, 5)),
            _card(2, _et(10, 10)),  # same idea, 5 minutes later
            _card(3, _et(10, 40)),  # still inside the first Card's 120m
            _card(4, _et(10, 45), direction="bearish"),  # the other way: a new idea
            _card(5, _et(12, 10)),  # after the first Card's window: a new idea
        ]
    )
    added = grading.sync_published_cards(conn, "SPY", _et(9, 0))
    assert added == 5
    repeats = {params[0]: params[8] for params in conn.inserts()}
    assert repeats == {1: False, 2: True, 3: True, 4: False, 5: False}
    assert conn.commits == 1


def test_repeat_check_carries_over_from_earlier_syncs():
    conn = _Conn(
        [_card(9, _et(10, 30))],
        prior_ideas=[("opening_range_break", "bullish", _et(10, 5), 120)],
    )
    grading.sync_published_cards(conn, "SPY", _et(9, 0))
    assert conn.inserts()[0][8] is True


def test_cards_outside_the_session_are_set_aside():
    conn = _Conn([_card(1, _et(8, 0)), _card(2, _et(8, 5))])
    grading.sync_published_cards(conn, "SPY", _et(7, 0))
    outcomes = [params[-1] for params in conn.inserts()]
    repeats = [params[8] for params in conn.inserts()]
    assert outcomes == ["off_session", "off_session"]
    assert repeats == [False, False]


def test_levels_come_from_the_card_payload():
    conn = _Conn([_card(1, _et(10, 5))])
    grading.sync_published_cards(conn, "SPY", _et(9, 0))
    params = conn.inserts()[0]
    # entry, trigger, target, stop, hold
    assert params[9:14] == (100.0, "at_market", 101.0, 99.5, 120)


# ----------------------------------------------------------------------
# Mispriced at-market Cards
# ----------------------------------------------------------------------


def test_at_market_card_quoting_a_price_nobody_traded_is_set_aside():
    """Before 2026-09-24 the API priced every Card at VWAP (Card #11418 quoted
    $768.99 while SPY traded 767.75-767.99). Grading its levels would blame
    the pattern for the pricing bug."""
    idea = _idea(entry_price=101.0, target_price=102.0, stop_price=100.5)
    grade = _grade(idea, _bars([100.0] * 120), now=_et(12, 0))
    assert grade.outcome == "mispriced"
    assert grade.r_multiple is None


def test_touch_entry_away_from_price_is_not_mispriced():
    idea = _idea(entry_price=101.0, target_price=102.0, stop_price=100.5, entry_trigger="at_touch")
    grade = _grade(idea, _bars([100.0] * 120), now=_et(12, 0))
    assert grade.outcome == "no_fill"


def test_at_market_card_at_the_traded_price_is_graded():
    # 100.04 sits inside the 10:00 bar's 99.95-100.05 range.
    idea = _idea(entry_price=100.04, target_price=101.0, stop_price=99.5)
    grade = _grade(idea, _bars([100.0] * 120), now=_et(12, 0))
    assert grade.outcome == "time_exit"


def test_mispriced_is_named_before_bad_geometry():
    """A Card priced at VWAP often has its target at that same price too; the
    price is the cause, so that is the label."""
    idea = _idea(entry_price=101.0, target_price=101.0, stop_price=100.5)
    grade = _grade(idea, _bars([100.0] * 120), now=_et(12, 0))
    assert grade.outcome == "mispriced"


def test_wall_fade_card_is_stopped_at_its_printed_stop():
    """call_wall_fade prints its stop as a price labeled premium_pct. Read as
    no stop, it could never lose; read as printed, a break of the wall stops it."""
    from src.signals.playbook.ideas import idea_levels

    levels = idea_levels(
        {
            "entry": {"ref_price": 100.0, "trigger": "at_touch"},
            "target": {"ref_price": 99.0, "kind": "level"},
            "stop": {"ref_price": 100.3, "kind": "premium_pct"},
            "max_hold_minutes": 90,
        }
    )
    idea = _idea(
        direction="bearish",
        entry_trigger=levels["entry_trigger"],
        entry_price=levels["entry_price"],
        target_price=levels["target_price"],
        stop_price=levels["stop_price"],
    )
    path = [100.0] * 30 + [100.0 + 0.05 * i for i in range(1, 30)]
    grade = _grade(idea, _bars(path), now=_et(10, 40))
    assert grade.outcome == "stop_hit"
    assert grade.r_multiple == -1.0


class _RebuildCursor:
    def __init__(self, conn):
        self.conn = conn
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        if sql.lstrip().startswith("DELETE"):
            self.rowcount = 7

    def fetchall(self):
        return []


class _RebuildConn:
    def __init__(self):
        self.executed = []
        self.commits = 0

    def cursor(self):
        return _RebuildCursor(self)

    def commit(self):
        self.commits += 1


def test_rebuild_clears_published_grades_first_and_keeps_held_back_ideas():
    conn = _RebuildConn()
    out = grading.run(conn, "SPY", days=90, now=_et(16, 30), rebuild=True)
    assert out["cleared"] == 7
    deletes = [s for s, _ in conn.executed if s.lstrip().startswith("DELETE")]
    assert len(deletes) == 1
    assert "card_id IS NOT NULL" in deletes[0]  # held-back ideas can't be rebuilt
    first_sql = conn.executed[0][0]
    assert first_sql.lstrip().startswith("DELETE")  # before the sync reads


def test_plain_run_clears_nothing():
    conn = _RebuildConn()
    out = grading.run(conn, "SPY", days=90, now=_et(16, 30))
    assert "cleared" not in out
    assert not [s for s, _ in conn.executed if s.lstrip().startswith("DELETE")]
