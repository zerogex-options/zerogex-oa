"""--drop-uncovered deletes history, so its guard is the part worth pinning.

A daily row whose session has no surface buckets cannot be rebuilt as a
session mean, and ``zero_bid_pct`` is NOT NULL so there is no way to mark it
unmeasurable short of a schema change. Deleting is therefore the only way to
get the old statistic out of the window the page ranks against -- and the
window is the last 60 STORED SESSIONS rather than 60 calendar days, so rows
that look safely historical by date can still be inside it.

The failure that matters is not deleting too little. It is a symbol whose
surface history never reached back far enough having its whole rollup
deleted by a tool that then reports success, leaving the page with nothing
to rank against at all. These tests pin the floor that prevents it, that it
applies per symbol rather than aborting the run, and that a dry run touches
nothing.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple
from unittest.mock import patch

from src.config import SPREAD_SURFACE_MIN_SESSIONS
from src.tools import daily_zero_bid_repair as tool

SEP = date(2026, 9, 16)
JUL = date(2026, 7, 13)


class _Cursor:
    """Answers each of the tool's statements by matching on its text."""

    def __init__(
        self,
        changes: Sequence[Tuple[Any, ...]],
        uncovered: Tuple[Any, ...],
        doomed: Sequence[Tuple[Any, ...]],
        surviving: Sequence[Tuple[str, int]],
    ):
        self._changes = list(changes)
        self._uncovered = uncovered
        self._doomed = list(doomed)
        self._surviving = list(surviving)
        self._pending: List[Tuple[Any, ...]] = []
        self._one: Optional[Tuple[Any, ...]] = None
        self.deleted: List[Tuple[Any, ...]] = []
        self.updated: List[Tuple[Any, ...]] = []

    def execute(self, sql: str, params: Any = ()) -> None:
        if sql.startswith("\nDELETE"):
            self.deleted.append(tuple(params))
        elif sql.startswith("\nUPDATE"):
            self.updated.append(tuple(params))
        elif "session_mean" in sql:
            self._pending = list(self._changes)
        elif "COUNT(*), MIN(trading_date)" in sql:
            self._one = self._uncovered
        elif "COUNT(DISTINCT d.trading_date)" in sql:
            self._pending = list(self._surviving)
        elif "d.option_type\n  FROM daily_spread_stats" in sql:
            self._pending = list(self._doomed)
        else:  # pragma: no cover - a statement the fake does not know
            raise AssertionError(f"unexpected statement: {sql[:80]}")

    def fetchall(self) -> List[Tuple[Any, ...]]:
        return self._pending

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        return self._one


class _Conn:
    def __init__(self, cursor: _Cursor):
        self._cursor = cursor
        self.committed = False

    def cursor(self) -> _Cursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def __enter__(self) -> "_Conn":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


def _run(cur: _Cursor, **kwargs: Any) -> Dict[str, Any]:
    with patch.object(tool, "db_connection", lambda: _Conn(cur)):
        _, summary = tool.repair(["SPX", "NDX"], None, None, **kwargs)
    return summary


def _cursor(doomed: Sequence[Tuple[Any, ...]], surviving: Sequence[Tuple[str, int]]) -> _Cursor:
    return _Cursor(
        changes=[], uncovered=(len(doomed), JUL, JUL), doomed=doomed, surviving=surviving
    )


# ---------------------------------------------------------------------------
# The floor
# ---------------------------------------------------------------------------


def test_a_symbol_that_would_be_left_without_history_is_refused():
    """The failure this guard exists for: a clean-looking wipe."""
    cur = _cursor(
        doomed=[("NDX", JUL, "C"), ("NDX", JUL, "P")],
        surviving=[("NDX", SPREAD_SURFACE_MIN_SESSIONS - 1)],
    )
    summary = _run(cur, dry_run=False, drop_uncovered=True)

    assert summary["refused_symbols"] == {"NDX": SPREAD_SURFACE_MIN_SESSIONS - 1}
    assert summary["dropped_rows"] == 0
    assert cur.deleted == []


def test_the_floor_is_inclusive():
    """Exactly at the floor is enough; the guard is 'below', not 'at or below'."""
    cur = _cursor(
        doomed=[("NDX", JUL, "C")],
        surviving=[("NDX", SPREAD_SURFACE_MIN_SESSIONS)],
    )
    summary = _run(cur, dry_run=False, drop_uncovered=True)

    assert summary["refused_symbols"] == {}
    assert cur.deleted == [("NDX", JUL, "C")]


def test_a_symbol_with_no_surviving_rows_at_all_is_refused_not_missed():
    """It is absent from the surviving counts, not present with a zero.

    Reading a missing key as "no constraint" is exactly how a wipe would get
    through, so the lookup has to default to zero rather than skip.
    """
    cur = _cursor(doomed=[("QQQ", JUL, "A")], surviving=[("SPX", 40)])
    summary = _run(cur, dry_run=False, drop_uncovered=True)

    assert summary["refused_symbols"] == {"QQQ": 0}
    assert cur.deleted == []


def test_one_refused_symbol_does_not_block_the_others():
    """A run over four symbols must not be all-or-nothing.

    Aborting would mean an under-backfilled symbol keeps every other symbol's
    contaminated rows in the window too.
    """
    cur = _cursor(
        doomed=[("SPX", JUL, "C"), ("SPX", JUL, "P"), ("NDX", JUL, "C")],
        surviving=[("SPX", 42), ("NDX", 2)],
    )
    summary = _run(cur, dry_run=False, drop_uncovered=True)

    assert summary["refused_symbols"] == {"NDX": 2}
    assert cur.deleted == [("SPX", JUL, "C"), ("SPX", JUL, "P")]
    assert summary["dropped_rows"] == 2
    assert summary["dropped_sessions"] == 1


# ---------------------------------------------------------------------------
# Dry run, and not opting in
# ---------------------------------------------------------------------------


def test_a_dry_run_reports_the_drop_without_making_it():
    cur = _cursor(doomed=[("SPX", JUL, "C")], surviving=[("SPX", 42)])
    summary = _run(cur, dry_run=True, drop_uncovered=True)

    assert summary["dropped_rows"] == 1
    assert summary["dropped_applied"] == 0
    assert cur.deleted == []


def test_the_flag_is_opt_in_and_uncovered_rows_are_otherwise_left_alone():
    """The default run must never delete, whatever the coverage looks like."""
    cur = _cursor(doomed=[("SPX", JUL, "C")], surviving=[("SPX", 42)])
    summary = _run(cur, dry_run=False, drop_uncovered=False)

    assert summary["dropped_rows"] == 0
    assert cur.deleted == []
    assert summary["uncovered_rows"] == 1
