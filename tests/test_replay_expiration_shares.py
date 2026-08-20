"""Unit tests for ``get_gex_expiration_shares_for_session`` — the per-strike
expiration mix that drives the replay scrubber's expiry colour gradient
(nearest expiration boldest, fanning out to the faintest on the furthest).

The endpoint tests in ``test_replay_endpoint.py`` stub this method wholesale,
so the folding / normalising logic inside it is only exercised here. The DB
connection is stubbed with canned rows: no Postgres, but the real method runs.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone

import pytest

from src.api.database import DatabaseManager

SESSION = date(2026, 6, 29)
BAR_A = datetime(2026, 6, 29, 13, 30, tzinfo=timezone.utc)
BAR_B = datetime(2026, 6, 29, 13, 31, tzinfo=timezone.utc)


class _ScriptedConn:
    """Returns a queued result per ``fetch`` call, recording the SQL."""

    def __init__(self, results):
        self._results = list(results)
        self.queries = []

    async def fetch(self, query, *args):
        self.queries.append(query)
        return self._results.pop(0) if self._results else []


def _run(results, **kwargs):
    db = DatabaseManager()
    conn = _ScriptedConn(results)

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    out = asyncio.run(
        db.get_gex_expiration_shares_for_session("SPY", SESSION, **kwargs)
    )
    return out, conn


def _exp_rows(*dates):
    return [{"expiration": d} for d in dates]


def _slot(ts, strike, slot, call_mag, put_mag):
    return {"timestamp": ts, "strike": strike, "slot": slot,
            "call_mag": call_mag, "put_mag": put_mag}


def test_shares_normalise_per_side_nearest_first():
    """Each side's slots sum to 1, positionally aligned to the legend."""
    exps = _exp_rows(date(2026, 6, 29), date(2026, 7, 17))
    slots = [
        _slot(BAR_A, 600.0, 1, 30.0, 5.0),
        _slot(BAR_A, 600.0, 2, 10.0, 15.0),
    ]
    out, _ = _run([exps, slots])
    assert out["expirations"] == ["2026-06-29", "2026-07-17"]
    assert out["far_bucket"] is False
    cell = out["rows"][(BAR_A, 600.0)]
    # 30/40 and 10/40 — the nearest expiration carries most of the call bar.
    assert cell["call"] == [0.75, 0.25]
    assert cell["put"] == [0.25, 0.75]
    assert sum(cell["call"]) == pytest.approx(1.0)
    assert sum(cell["put"]) == pytest.approx(1.0)


def test_expirations_past_the_cap_fold_into_one_far_bucket():
    """Only the nearest ``max_expirations`` get their own slot.

    This is the payload bound: a session carrying ~15-20 expirations would
    otherwise put that many floats on every strike of every minute.
    """
    exps = _exp_rows(
        date(2026, 6, 29), date(2026, 6, 30), date(2026, 7, 1), date(2026, 7, 17)
    )
    # PG has already folded everything past the cap into slot cap+1.
    slots = [
        _slot(BAR_A, 600.0, 1, 50.0, 0.0),
        _slot(BAR_A, 600.0, 2, 25.0, 0.0),
        _slot(BAR_A, 600.0, 3, 25.0, 0.0),  # the fold-in slot for a cap of 2
    ]
    out, _ = _run([exps, slots], max_expirations=2)
    assert out["expirations"] == ["2026-06-29", "2026-06-30"]
    assert out["far_bucket"] is True
    # Two real slots plus the trailing catch-all.
    assert out["rows"][(BAR_A, 600.0)]["call"] == [0.5, 0.25, 0.25]


def test_a_side_with_no_gamma_yields_no_segments():
    """An all-zero side returns [] so the client draws one solid bar.

    Normalising 0/0 would be a divide-by-zero; emitting an empty list is the
    signal the renderer already understands.
    """
    exps = _exp_rows(date(2026, 6, 29), date(2026, 7, 17))
    slots = [
        _slot(BAR_A, 600.0, 1, 40.0, 0.0),
        _slot(BAR_A, 600.0, 2, 60.0, 0.0),
    ]
    out, _ = _run([exps, slots])
    cell = out["rows"][(BAR_A, 600.0)]
    assert cell["call"] == [0.4, 0.6]
    assert cell["put"] == []


def test_rows_are_keyed_per_minute_and_strike():
    """The mix is per (minute, strike) — it changes as the session moves."""
    exps = _exp_rows(date(2026, 6, 29), date(2026, 7, 17))
    slots = [
        _slot(BAR_A, 600.0, 1, 90.0, 0.0),
        _slot(BAR_A, 600.0, 2, 10.0, 0.0),
        _slot(BAR_A, 605.0, 1, 50.0, 0.0),
        _slot(BAR_A, 605.0, 2, 50.0, 0.0),
        _slot(BAR_B, 600.0, 1, 10.0, 0.0),
        _slot(BAR_B, 600.0, 2, 90.0, 0.0),
    ]
    out, _ = _run([exps, slots])
    assert out["rows"][(BAR_A, 600.0)]["call"] == [0.9, 0.1]
    assert out["rows"][(BAR_A, 605.0)]["call"] == [0.5, 0.5]
    assert out["rows"][(BAR_B, 600.0)]["call"] == [0.1, 0.9]


def test_no_expirations_returns_empty_without_the_second_query():
    """A session with no chain data short-circuits before the heavy scan."""
    out, conn = _run([[]])
    assert out == {"expirations": [], "far_bucket": False, "rows": {}}
    assert len(conn.queries) == 1


def test_db_failure_degrades_instead_of_raising():
    """The replay payload must still render if this lookup blows up."""
    db = DatabaseManager()

    @asynccontextmanager
    async def _boom():
        raise RuntimeError("connection reset")
        yield  # pragma: no cover

    db._acquire_connection = _boom  # type: ignore[method-assign]
    out = asyncio.run(db.get_gex_expiration_shares_for_session("SPY", SESSION))
    assert out == {"expirations": [], "far_bucket": False, "rows": {}}


def test_query_folds_slots_and_filters_to_session_expirations():
    """Pin the two bounds that keep this query cheap.

    Without the LEAST() fold Postgres would return one row per expiration per
    strike-minute; without the expiration >= session-date filter a rolled-off
    contract could occupy a slot on the ramp.
    """
    exps = _exp_rows(date(2026, 6, 29))
    _, conn = _run([exps, []])
    heavy = conn.queries[1]
    assert "LEAST(se.rn" in heavy
    assert "expiration >= $5" in heavy
    # Shares are scale-free, so the raw gamma columns are enough — no spot
    # join needed to compute them.
    assert "call_gamma" in heavy and "put_gamma" in heavy
