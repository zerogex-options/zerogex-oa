"""A missing gex_summary row must not read as "the profile had no flip".

``gamma_regime_5min.gamma_flip`` is resolved from whichever ``gex_summary`` row
landed inside the bar's own five minutes. The original lookup ended in
``ORDER BY timestamp DESC LIMIT 1`` and took ``None`` for an answer, which made
two different facts identical:

* no row landed in the window -- the analytics cycle was late, restarting, or
  stalled upstream, and NOTHING was measured;
* a row landed and its ``gamma_flip_point`` was NULL -- the dealer-gamma
  profile is one-signed and genuinely has no crossing.

Every consumer reads the second meaning. :mod:`src.analytics.flip_cushion`
turns a NULL flip into ``STATE_NO_FLIP`` with ``BASIS_NONE``, the Gamma Weather
panel prints "no gamma flip in the profile", and the base-rate tool scores the
session as having had no cushion at all. So a slow cycle published a confident
statement about the market that nothing in the stack could contradict, and
nothing logged.

Without these tests: the carry could quietly regress to the old lookup (case
one), or over-correct and paper a stale level over a genuine one-signed profile
(case two), or drag yesterday's flip across the overnight gap (case three), or
resolve a gap-filled bar from a LATER bar's reading so that a cold-started
session disagreed with the one the live writer would have produced (case four).
Each is silent in production; each is asserted here.
"""

import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.analytics import gamma_flip_carry as carry
from src.analytics import main_engine
from src.analytics.flip_cushion import BASIS_NONE, STATE_NO_FLIP, classify, measure
from src.analytics.main_engine import AnalyticsEngine

UTC = timezone.utc

#: A short stand-in session grid. The engine's is 82 bars; nothing here
#: depends on the length, only on the ordering.
BAR0 = datetime(2026, 4, 24, 13, 30, tzinfo=UTC)  # 09:30 ET


def grid(n, start=BAR0):
    return [start + timedelta(minutes=5 * i) for i in range(n)]


# --------------------------------------------------------------------------- #
# The four cases, on the resolver itself.
# --------------------------------------------------------------------------- #


def test_a_bar_with_no_source_row_carries_the_last_measured_level():
    """Case one: the regression. A window with no ``gex_summary`` row used to
    store NULL, which every consumer reads as "no boundary exists"."""
    bars = grid(4)
    resolved = carry.resolve_session_flips(bars, [(bars[0], 690.0)])

    assert resolved[bars[0]].flip == 690.0
    assert resolved[bars[0]].measured
    for missing in bars[1:]:
        assert resolved[missing].flip == 690.0, "a late cycle blanked the flip"
        assert not resolved[missing].measured
        assert resolved[missing].carried_from == bars[0]

    assert [resolved[b].stale_bars for b in bars] == [0, 1, 2, 3]


def test_a_measured_null_is_written_through_and_never_papered_over():
    """Case two, and the reason this is not just ``COALESCE``. A row that
    exists and reports no crossing is a MEASUREMENT: the profile is one-signed.
    Carrying a stale level over it would assert a boundary that is not there.
    """
    bars = grid(3)
    resolved = carry.resolve_session_flips(bars, [(bars[0], 690.0), (bars[1], None)])

    assert resolved[bars[1]].flip is None
    assert resolved[bars[1]].measured
    assert not resolved[bars[1]].carried

    # ...and the measured absence propagates like any other reading, so a later
    # bar with no row of its own carries "no crossing" rather than reviving 690.
    assert resolved[bars[2]].flip is None
    assert resolved[bars[2]].carried_from == bars[1]


def test_a_measured_null_still_reaches_the_cushion_as_no_flip():
    """The consumer end of case two: the state the panel prints must be
    unchanged for a genuinely one-signed profile."""
    bars = grid(2)
    resolved = carry.resolve_session_flips(bars, [(bars[0], None)])

    _, distance_frac, cushion_pts, _ = measure(700.0, resolved[bars[0]].flip)
    state, _, basis = classify(distance_frac, cushion_pts, 20.0)

    assert (state, basis) == (STATE_NO_FLIP, BASIS_NONE)


def test_nothing_is_carried_into_a_session_from_before_its_open():
    """Case three. Bars ahead of the session's first reading have nothing
    legitimate behind them -- yesterday's flip is not a statement about today
    -- so they stay NULL and are reported as unresolved rather than carried."""
    bars = grid(4)
    resolved = carry.resolve_session_flips(bars, [(bars[2], 690.0)])

    for early in bars[:2]:
        assert resolved[early].flip is None
        assert resolved[early].unresolved
        assert resolved[early].carried_from is None

    assert resolved[bars[2]].flip == 690.0
    assert resolved[bars[3]].flip == 690.0


def test_an_observation_outside_the_session_grid_is_ignored_not_snapped():
    """The same boundary from the other side: a reading that does not land on
    a bar of THIS session must not be pulled onto one."""
    bars = grid(3)
    yesterday = bars[0] - timedelta(days=1)

    resolved = carry.resolve_session_flips(bars, [(yesterday, 690.0)])

    assert all(resolved[b].flip is None for b in bars)
    assert all(resolved[b].unresolved for b in bars)


def test_a_gap_filled_bar_resolves_exactly_as_the_live_write_would_have():
    """Case four: causality. ``_refresh_gamma_regime_snapshot`` writes one bar
    per cycle live but walks whole runs of bars on cold start and gap-fill. A
    backfilled bar must get what a live write would have produced AT THAT
    MOMENT, so resolving a prefix of the session has to agree with resolving
    all of it -- bar for bar, including where a LATER reading exists that a
    backward-looking resolution must not reach for."""
    bars = grid(8)
    observations = [(bars[0], 690.0), (bars[3], None), (bars[5], 701.5)]

    full = carry.resolve_session_flips(bars, observations)

    for cutoff in range(1, len(bars) + 1):
        live = carry.resolve_session_flips(bars[:cutoff], observations)
        for bar in bars[:cutoff]:
            assert live[bar] == full[bar], f"bar {bar} moved when the session grew"

    # Specifically: bar 4 must not see bar 5's 701.5, and bar 6 must not
    # resurrect 690.0 across the measured "no crossing" at bar 3.
    assert full[bars[4]].flip is None
    assert full[bars[6]].flip == 701.5


# --------------------------------------------------------------------------- #
# The summary that makes a degraded session visible.
# --------------------------------------------------------------------------- #


def test_the_summary_counts_where_each_bar_got_its_level():
    bars = grid(5)
    resolved = carry.resolve_session_flips(bars, [(bars[1], 690.0), (bars[2], None)])
    summary = carry.summarize(resolved[b] for b in bars)

    assert (summary.bars, summary.measured, summary.carried, summary.unresolved) == (5, 2, 2, 1)
    assert summary.max_stale_bars == 2
    assert summary.degraded


def test_one_bar_of_carry_is_not_worth_a_log_line_but_a_stall_is():
    """The newest bar has not had time for a row of its own on the first cycle
    after it opens, so a one-bar carry is the ordinary state and reporting it
    would bury the signal under ~78 lines a session."""
    bars = grid(6)
    just_opened = carry.summarize(
        carry.resolve_session_flips(bars[:2], [(bars[0], 690.0)])[b] for b in bars[:2]
    )
    stalled = carry.summarize(
        carry.resolve_session_flips(bars, [(bars[0], 690.0)])[b] for b in bars
    )

    assert not just_opened.notable
    assert stalled.notable and stalled.sustained


def test_a_fully_measured_session_is_not_reported_as_degraded():
    bars = grid(4)
    summary = carry.summarize(
        carry.resolve_session_flips(bars, [(b, 690.0) for b in bars])[b] for b in bars
    )

    assert not summary.degraded
    assert not summary.notable


# --------------------------------------------------------------------------- #
# The writer.
# --------------------------------------------------------------------------- #


class _Cursor:
    """Cursor that answers the writer's probes and records its statements.

    ``observed`` maps a bar_start to the ``gamma_flip_point`` of the newest
    ``gex_summary`` row inside it. A bar ABSENT from the mapping had no row at
    all, which is precisely the distinction under test -- so it is modelled the
    way the real query models it, by the row simply not being emitted.
    """

    def __init__(self, observed, already_written=()):
        self._observed = dict(observed)
        self._already_written = set(already_written)
        self.statements = []
        self._last = ""

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        self._last = sql

    def fetchone(self):
        if "percentile_cont" in self._last:
            return (20.0,)
        return None

    def fetchall(self):
        if "information_schema.columns" in self._last:
            return [("gamma_flip",), ("typical_move_30m",)]
        if "SELECT bar_start FROM gamma_regime_5min" in self._last:
            return [(b,) for b in sorted(self._already_written)]
        if "gamma_flip_point" in self._last:
            return sorted(self._observed.items())
        return [(700.0, 700.0, datetime(2026, 4, 24).date(), 1.0e6, 1.0e6, 0.0, 10, 10)]

    @property
    def description(self):
        return [
            ("spot_price",),
            ("strike",),
            ("expiration",),
            ("net_gex",),
            ("call_gex",),
            ("put_gex",),
            ("call_oi",),
            ("put_oi",),
        ]


def _run(observed, already_written=()):
    """Drive one snapshot refresh over a session with these observations."""
    AnalyticsEngine._gamma_regime_optional_cols = None  # re-probe per test
    engine = AnalyticsEngine.__new__(AnalyticsEngine)
    engine._analytics_flow_cache_refresh_enabled = True
    engine.db_symbol = "SPY"

    cursor = _Cursor(observed, already_written)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._refresh_gamma_regime_snapshot(datetime(2026, 4, 24, 17, 0, tzinfo=UTC))
    return cursor


def _written_flips(cursor):
    """``{bar_start: gamma_flip}`` for every bar the writer upserted."""
    return {
        params["bar_start"]: params["gamma_flip"]
        for sql, params in cursor.statements
        if "INSERT INTO gamma_regime_5min" in sql
    }


#: The session the fixed timestamp above resolves to: 09:30 ET through 16:15 ET.
SESSION_BARS = grid(82)


def test_the_writer_carries_a_level_over_a_window_with_no_source_row():
    written = _written_flips(_run({SESSION_BARS[0]: 690.0}))

    assert written[SESSION_BARS[0]] == 690.0
    assert written[SESSION_BARS[1]] == 690.0, "the writer still blanks a missing window"
    assert written[SESSION_BARS[40]] == 690.0


def test_the_writer_stores_null_for_a_row_that_measured_no_crossing():
    written = _written_flips(
        _run({SESSION_BARS[0]: 690.0, SESSION_BARS[1]: None, SESSION_BARS[2]: 701.5})
    )

    assert written[SESSION_BARS[1]] is None, "a one-signed profile was papered over"
    assert written[SESSION_BARS[2]] == 701.5


def test_a_bar_written_alone_agrees_with_the_same_bar_written_in_a_run():
    """The engine writes one bar per cycle live and walks runs of them on cold
    start and gap-fill. Both go through the same resolution, so a bar's value
    must not depend on which path wrote it.

    (That a backfilled bar never sees a LATER reading is the strictly
    backward-looking property pinned above, over every prefix of the grid.)
    """
    observed = {SESSION_BARS[0]: 690.0, SESSION_BARS[4]: None, SESSION_BARS[9]: 701.5}
    in_a_run = _written_flips(_run(observed))

    for index in (1, 3, 6, 11, 40):
        bar = SESSION_BARS[index]
        # Everything but this bar is already on disk, so the writer rewrites it
        # alone, alongside the still-filling newest bar -- the live shape.
        alone = _written_flips(
            _run(observed, already_written=[b for b in SESSION_BARS if b != bar])
        )

        assert set(alone) == {bar, SESSION_BARS[-1]}
        assert alone[bar] == in_a_run[bar], f"{bar} differs between live and gap-fill"


def test_the_flip_is_resolved_in_one_query_per_cycle_not_one_per_bar():
    """A cold start walks ~82 bars; the old lookup issued a round trip for each
    one to learn what a single grouped scan of the session already says."""
    cursor = _run({SESSION_BARS[0]: 690.0})
    lookups = [sql for sql, _ in cursor.statements if "gamma_flip_point" in sql]

    assert len(lookups) == 1


def test_the_flip_query_is_bounded_by_the_session():
    """The carry can only reach as far back as the query reaches. An unbounded
    scan would let a bar at the open inherit yesterday's close."""
    cursor = _run({SESSION_BARS[0]: 690.0})
    sql, params = next(
        (s, p) for s, p in cursor.statements if "gamma_flip_point" in s and "gex_summary" in s
    )

    assert "session_start" in sql and "session_end" in sql
    assert params["session_start"] == SESSION_BARS[0]
    assert params["session_end"] == SESSION_BARS[-1]


def test_sustained_carry_is_logged_rather_than_written_silently(caplog):
    """The point of the change: a degraded read stops being indistinguishable
    from a quiet market. A whole session resolved from one reading at the open
    is the shape a stalled upstream produces."""
    with caplog.at_level(logging.INFO):
        _run({SESSION_BARS[0]: 690.0})

    carried = [r for r in caplog.records if "carried forward" in r.getMessage()]
    assert carried, "a session built almost entirely from one reading logged nothing"
    assert any(r.levelno >= logging.WARNING for r in carried)


def test_a_fully_measured_session_logs_no_carry(caplog):
    """The counterpart: the line has to be rare enough to mean something."""
    with caplog.at_level(logging.INFO):
        _run({bar: 690.0 for bar in SESSION_BARS})

    assert not [r for r in caplog.records if "carried forward" in r.getMessage()]


def test_no_flip_query_at_all_when_the_column_is_absent(monkeypatch):
    """Nowhere to put the answer, so do not pay for the query -- the behaviour
    ``tests/test_gamma_regime_schema_skew.py`` pins, held through the rewrite.
    """
    AnalyticsEngine._gamma_regime_optional_cols = None
    monkeypatch.setattr(
        AnalyticsEngine, "_gamma_regime_optional_columns", lambda self, cursor: {"typical_move_30m"}
    )
    cursor = _run({SESSION_BARS[0]: 690.0})

    assert [sql for sql, _ in cursor.statements if "gamma_flip_point" in sql] == []
