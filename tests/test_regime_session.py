"""Tests for the stored per-session Gamma Regime read.

:mod:`src.analytics.regime_shift` is covered by ``test_regime_shift.py``; this
suite covers the layer above it — the orchestration in
:mod:`src.analytics.regime_session` and the refresh job in
:mod:`src.tools.regime_session_refresh` — driven through an in-memory stub
rather than Postgres.

The properties that matter here are the ones a live database would hide until
they had already corrupted a month of history:

* a session whose anchor falls through to the PRIOR session must be skipped,
  not stored as that session's read;
* a session must never normalize against itself;
* a backfill must run oldest-first, so each row is normalized against the
  sessions that preceded it rather than the whole window.
"""

from __future__ import annotations

import asyncio
import math
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional

import pytest
from zoneinfo import ZoneInfo

from src.analytics import regime_session as rsess
from src.analytics import regime_shift as rs
from src.tools import regime_session_refresh as tool

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


# --------------------------------------------------------------------------- #
# In-memory stand-in for the slice of DatabaseManager the module uses.
# --------------------------------------------------------------------------- #
def _et(day: date, hh: int, mm: int) -> datetime:
    return datetime.combine(day, time(hh, mm), tzinfo=ET).astimezone(UTC)


def _chain(strikes: Dict[float, float], exp: str = "2026-08-21") -> List[Dict[str, Any]]:
    return [
        {
            "strike": k,
            "expiration": exp,
            "net_gex": v,
            "call_gex": v,
            "put_gex": 0.0,
            "call_oi": 100,
            "put_oi": 0,
        }
        for k, v in strikes.items()
    ]


class FakeDb:
    """Serves snapshots from a {timestamp -> (spot, rows)} map, at-or-before."""

    def __init__(
        self,
        snapshots: Dict[datetime, tuple],
        sessions: Optional[List[Dict[str, Any]]] = None,
        historical_context: Optional[Dict[str, Any]] = None,
    ):
        self.snapshots = dict(sorted(snapshots.items()))
        self.sessions = sessions or []
        self.historical_context = historical_context
        self.written: List[Dict[str, Any]] = []

    async def get_gex_chain_at_ts(self, symbol, at_ts, max_rows=6000):
        candidates = [ts for ts in self.snapshots if ts <= at_ts]
        if not candidates:
            return {"timestamp": None, "spot": None, "rows": []}
        ts = max(candidates)
        spot, rows = self.snapshots[ts]
        return {"timestamp": ts, "spot": spot, "rows": rows}

    async def get_gex_summary_at_ts(self, symbol, at_ts):
        return {
            "timestamp": at_ts,
            "gamma_flip": 100.0,
            "call_wall": 105.0,
            "put_wall": 95.0,
            "net_gex_at_spot": 1.0,
        }

    async def get_regime_sessions(self, symbol, limit=60, before_date=None):
        rows = self.sessions
        if before_date is not None:
            rows = [r for r in rows if r["session_date"] < before_date]
        return sorted(rows, key=lambda r: r["session_date"], reverse=True)[:limit]

    async def get_gex_historical_context(self, symbol):
        return self.historical_context

    async def upsert_regime_session(self, row):
        self.written.append(dict(row))

    async def connect(self):
        return None

    async def disconnect(self):
        return None


DAY = date(2026, 8, 21)


def _two_snapshot_db(**kwargs) -> FakeDb:
    """Open flat, close with gamma built at 101 (above a spot of 100)."""
    return FakeDb(
        {
            _et(DAY, 9, 30): (100.0, _chain({99: 0.0, 100: 0.0, 101: 0.0})),
            _et(DAY, 16, 0): (100.0, _chain({99: 0.0, 100: 0.0, 101: 500.0})),
        },
        **kwargs,
    )


# --------------------------------------------------------------------------- #
# compute_session_read
# --------------------------------------------------------------------------- #
class TestComputeSessionRead:
    def test_builds_a_row_from_the_open_and_the_close(self):
        db = _two_snapshot_db()
        row = asyncio.run(rsess.compute_session_read(db, "SPY", DAY))

        assert row is not None
        assert row["underlying"] == "SPY"
        assert row["session_date"] == DAY
        assert row["open_ts"] == _et(DAY, 9, 30)
        assert row["close_ts"] == _et(DAY, 16, 0)
        # A build above spot is capping, and it is near spot so it stabilizes.
        assert row["lean_raw"] < 0
        assert row["stability_raw"] > 0

    def test_raw_scores_are_stored_unclamped(self):
        """The z columns are a convenience; the raw columns are the record.

        A future backfill recomputes every z from these, so clamping here
        would permanently cap what any later normalization could see.
        """
        db = _two_snapshot_db()
        row = asyncio.run(rsess.compute_session_read(db, "SPY", DAY))
        assert abs(row["stability_raw"]) > 6  # far outside the z clamp

    def test_skips_a_session_whose_anchor_fell_into_the_prior_session(self):
        """Asking for 16:05 on a day the engine never ran would otherwise
        return YESTERDAY's final frame and store it as today's read."""
        prior = DAY - timedelta(days=1)
        db = FakeDb(
            {
                _et(prior, 9, 30): (100.0, _chain({100: 0.0})),
                _et(prior, 16, 0): (100.0, _chain({100: 500.0})),
            }
        )

        assert asyncio.run(rsess.compute_session_read(db, "SPY", DAY)) is None

    def test_skips_when_only_one_snapshot_exists(self):
        db = FakeDb({_et(DAY, 9, 30): (100.0, _chain({100: 0.0}))})
        assert asyncio.run(rsess.compute_session_read(db, "SPY", DAY)) is None

    def test_skips_a_session_with_no_snapshots_at_all(self):
        assert asyncio.run(rsess.compute_session_read(FakeDb({}), "SPY", DAY)) is None

    def test_end_ts_override_reads_the_session_mid_flight(self):
        db = FakeDb(
            {
                _et(DAY, 9, 30): (100.0, _chain({101: 0.0})),
                _et(DAY, 12, 0): (100.0, _chain({101: 200.0})),
                _et(DAY, 16, 0): (100.0, _chain({101: 900.0})),
            }
        )
        midday = asyncio.run(
            rsess.compute_session_read(db, "SPY", DAY, end_ts=_et(DAY, 12, 30))
        )
        full = asyncio.run(rsess.compute_session_read(db, "SPY", DAY))

        assert midday["close_ts"] == _et(DAY, 12, 0)
        assert full["close_ts"] == _et(DAY, 16, 0)
        assert abs(midday["stability_raw"]) < abs(full["stability_raw"])

    def test_band_low_high_are_null_when_unresolved(self):
        """An unresolved band must not leak a range consumers would quote."""
        spread = {float(k): 100.0 for k in range(50, 150)}
        db = FakeDb(
            {
                _et(DAY, 9, 30): (100.0, _chain({k: 0.0 for k in spread})),
                _et(DAY, 16, 0): (100.0, _chain(spread)),
            }
        )
        row = asyncio.run(rsess.compute_session_read(db, "SPY", DAY))

        assert row["band_resolved"] is False
        assert row["band_low"] is None and row["band_high"] is None

    def test_rolloff_columns_describe_the_nearest_expiration(self):
        rows = _chain({100: 300.0}, exp="2026-08-21") + _chain(
            {100: 100.0}, exp="2026-09-18"
        )
        db = FakeDb(
            {
                _et(DAY, 9, 30): (100.0, _chain({100: 0.0}, exp="2026-08-21")),
                _et(DAY, 16, 0): (100.0, rows),
            }
        )
        row = asyncio.run(rsess.compute_session_read(db, "SPY", DAY))

        assert row["rolloff_expiration"] == date(2026, 8, 21)
        assert row["rolloff_share"] == pytest.approx(0.75)
        assert row["chain_abs_gex"] == pytest.approx(400.0)


# --------------------------------------------------------------------------- #
# trailing_sigmas
# --------------------------------------------------------------------------- #
def _stored(day: date, lean: float, stability: float) -> Dict[str, Any]:
    return {"session_date": day, "lean_raw": lean, "stability_raw": stability}


class TestTrailingSigmas:
    def test_uses_stored_sessions_once_there_are_enough(self):
        sessions = [
            _stored(DAY - timedelta(days=i + 1), float(i), float(i) * 2)
            for i in range(rs.MIN_SESSIONS_FOR_SIGMA + 5)
        ]
        db = FakeDb({}, sessions=sessions)

        lean_sigma, stab_sigma, norm, n = asyncio.run(
            rsess.trailing_sigmas(db, "SPY", before=DAY)
        )

        assert norm == "trailing"
        assert n == len(sessions)
        assert lean_sigma and stab_sigma
        assert stab_sigma == pytest.approx(lean_sigma * 2)

    def test_falls_back_to_the_proxy_while_bootstrapping(self):
        db = FakeDb({}, sessions=[_stored(DAY - timedelta(days=1), 1.0, 2.0)])
        scores = rs.ShiftScores(
            lean=0.0,
            stability=0.0,
            net_shift=0.0,
            gross_shift=0.0,
            sigma_price=1.0,
            near_spot_stock=1000.0,
        )

        lean_sigma, stab_sigma, norm, _n = asyncio.run(
            rsess.trailing_sigmas(db, "SPY", before=DAY, scores=scores)
        )

        assert norm == "proxy"
        assert lean_sigma == stab_sigma
        assert lean_sigma == pytest.approx(rs.PROXY_SHIFT_FRACTION * 1000.0)

    def test_the_proxy_is_a_change_scale_not_a_level_scale(self):
        """The bootstrap denominator must be the same KIND of quantity as the
        score it divides.

        It used to borrow the dispersion of ``net_gex_at_spot`` — the spread
        of a LEVEL read at one point on the spot-shift curve — to normalize a
        proximity-weighted sum of CHANGES across ~30 strikes.  Nothing ties
        those magnitudes together, and in practice the borrowed number was
        large enough to push every z toward zero, so a freshly-deployed
        symbol printed QUIET on every session whatever the book did.
        """
        db = FakeDb(
            {},
            sessions=[],
            historical_context={
                "metrics": {"net_gex_at_spot": {"windows": {"30d": {"std": 1e12}}}}
            },
        )
        scores = rs.ShiftScores(
            lean=60.0,
            stability=60.0,
            net_shift=0.0,
            gross_shift=0.0,
            sigma_price=1.0,
            near_spot_stock=500.0,
        )

        lean_sigma, _stab, norm, _n = asyncio.run(
            rsess.trailing_sigmas(db, "SPY", before=DAY, scores=scores)
        )

        assert norm == "proxy"
        # Scaled off the chain, not off the (deliberately absurd) stored level.
        assert lean_sigma == pytest.approx(rs.PROXY_SHIFT_FRACTION * 500.0)
        assert rs.classify(
            rs.zscore(scores.lean, lean_sigma), rs.zscore(scores.stability, lean_sigma)
        ).state != "QUIET"

    def test_reports_none_when_neither_source_is_available(self):
        db = FakeDb({}, sessions=[], historical_context=None)
        lean_sigma, stab_sigma, norm, n = asyncio.run(
            rsess.trailing_sigmas(db, "SPY", before=DAY)
        )
        assert (lean_sigma, stab_sigma, norm, n) == (None, None, "none", 0)

    def test_a_short_window_is_measured_against_a_short_yardstick(self):
        """A 1h lookback against a full-session sigma reads QUIET every time.

        Every stored read is a since-open session, so the trailing sigma
        answers "how big is a full session's shift".  Applying it unscaled to
        a one-hour window measures the clock rather than the book.
        """
        sessions = [
            _stored(DAY - timedelta(days=i + 1), float(i), float(i))
            for i in range(rs.MIN_SESSIONS_FOR_SIGMA + 2)
        ]
        db = FakeDb({}, sessions=sessions)

        full, _s, _n, _c = asyncio.run(
            rsess.trailing_sigmas(db, "SPY", before=DAY, hours=rs.SESSION_HOURS)
        )
        hour, _s2, _n2, _c2 = asyncio.run(
            rsess.trailing_sigmas(db, "SPY", before=DAY, hours=1.0)
        )
        week, _s3, _n3, _c3 = asyncio.run(
            rsess.trailing_sigmas(db, "SPY", before=DAY, hours=rs.SESSION_HOURS * 5)
        )

        assert hour < full < week
        assert full == pytest.approx(
            hour / rs.horizon_factor(1.0), rel=1e-9
        )


    def test_a_session_never_normalizes_against_itself(self):
        """Including the in-progress row would drag its own z toward zero
        exactly when the move is large — the worst possible time."""
        sessions = [
            _stored(DAY - timedelta(days=i + 1), float(i), float(i))
            for i in range(rs.MIN_SESSIONS_FOR_SIGMA + 2)
        ]
        sessions.append(_stored(DAY, 1e9, 1e9))  # today, a huge outlier
        db = FakeDb({}, sessions=sessions)

        lean_sigma, _stab, norm, _n = asyncio.run(
            rsess.trailing_sigmas(db, "SPY", before=DAY)
        )

        assert norm == "trailing"
        assert lean_sigma < 100  # the outlier is excluded

    def test_a_motionless_axis_borrows_the_other_axis_sigma(self):
        """Zero would make that axis's z infinite on the next real move."""
        sessions = [
            _stored(DAY - timedelta(days=i + 1), 0.0, float(i + 1))
            for i in range(rs.MIN_SESSIONS_FOR_SIGMA + 2)
        ]
        db = FakeDb({}, sessions=sessions)

        lean_sigma, stab_sigma, norm, _n = asyncio.run(
            rsess.trailing_sigmas(db, "SPY", before=DAY)
        )

        assert norm == "trailing"
        assert lean_sigma == stab_sigma and lean_sigma > 0

    def test_a_constant_axis_is_no_longer_degenerate(self):
        """The sigma is measured about ZERO, so a chain that shifts by the
        same amount every session has a perfectly good scale — and a typical
        session on it reads as typical rather than borrowing another axis's.

        Under a mean-centred estimator this axis had no dispersion at all,
        which is how a steady, repeated, entirely real shift got reported as
        nothing happening.
        """
        sessions = [
            _stored(DAY - timedelta(days=i + 1), 5.0, float(i + 1))
            for i in range(rs.MIN_SESSIONS_FOR_SIGMA + 2)
        ]
        db = FakeDb({}, sessions=sessions)

        lean_sigma, stab_sigma, norm, _n = asyncio.run(
            rsess.trailing_sigmas(db, "SPY", before=DAY)
        )

        assert norm == "trailing"
        assert lean_sigma != stab_sigma
        assert lean_sigma == pytest.approx(5.0 * 1.4826)


# --------------------------------------------------------------------------- #
# rth_hours_between
# --------------------------------------------------------------------------- #
class TestRthHoursBetween:
    def test_counts_only_the_cash_session(self):
        # 15:00 Thursday -> 11:00 Friday: 1h Thursday + 1.5h Friday.
        start = _et(date(2026, 8, 20), 15, 0)
        end = _et(date(2026, 8, 21), 11, 0)
        assert rsess.rth_hours_between(start, end) == pytest.approx(2.5)

    def test_drops_the_weekend(self):
        # Friday close -> Monday open is zero trading time, not 65 hours.
        start = _et(date(2026, 8, 21), 16, 0)
        end = _et(date(2026, 8, 24), 9, 30)
        assert rsess.rth_hours_between(start, end) == pytest.approx(0.0)

    def test_an_intraday_window_is_its_own_length(self):
        start = _et(DAY, 10, 0)
        end = _et(DAY, 11, 30)
        assert rsess.rth_hours_between(start, end) == pytest.approx(1.5)

    def test_a_partial_since_open_window_is_partial(self):
        """"Since open" at 09:45 is a quarter hour, not a session."""
        assert rsess.rth_hours_between(_et(DAY, 9, 30), _et(DAY, 9, 45)) == pytest.approx(
            0.25
        )

    def test_tolerates_missing_or_reversed_bounds(self):
        assert rsess.rth_hours_between(None, _et(DAY, 10, 0)) == 0.0
        assert rsess.rth_hours_between(
            _et(DAY, 12, 0), _et(DAY, 10, 0)
        ) == pytest.approx(2.0)


# --------------------------------------------------------------------------- #
# recent_sessions
# --------------------------------------------------------------------------- #
class TestRecentSessions:
    def test_skips_weekends(self):
        # 2026-08-21 is a Friday.
        out = rsess.recent_sessions(date(2026, 8, 21), 3)
        assert out == [date(2026, 8, 21), date(2026, 8, 20), date(2026, 8, 19)]

    def test_walks_back_over_a_weekend(self):
        out = rsess.recent_sessions(date(2026, 8, 24), 2)  # Monday
        assert out == [date(2026, 8, 24), date(2026, 8, 21)]

    def test_a_weekend_as_of_date_yields_only_weekdays(self):
        out = rsess.recent_sessions(date(2026, 8, 22), 2)  # Saturday
        assert all(d.weekday() < 5 for d in out)


# --------------------------------------------------------------------------- #
# The refresh job
# --------------------------------------------------------------------------- #
class TestRefreshSymbol:
    def _multi_session_db(self, days: List[date]) -> FakeDb:
        snapshots = {}
        for i, day in enumerate(days):
            snapshots[_et(day, 9, 30)] = (100.0, _chain({101: 0.0}))
            snapshots[_et(day, 16, 0)] = (100.0, _chain({101: 100.0 * (i + 1)}))
        return FakeDb(snapshots)

    def test_writes_one_row_per_session(self):
        days = [date(2026, 8, 19), date(2026, 8, 20), date(2026, 8, 21)]
        db = self._multi_session_db(days)

        written, skipped = asyncio.run(tool.refresh_symbol(db, "SPY", days))

        assert (written, skipped) == (3, 0)
        assert [r["session_date"] for r in db.written] == days

    def test_processes_oldest_first_so_each_row_sees_its_own_history(self):
        """A newest-first backfill would give the OLDEST rows the whole window
        and the newest rows nothing — confident about its least-supported bars."""
        days = [date(2026, 8, 19), date(2026, 8, 20), date(2026, 8, 21)]
        db = self._multi_session_db(days)

        asyncio.run(tool.refresh_symbol(db, "SPY", days))

        dates = [r["session_date"] for r in db.written]
        assert dates == sorted(dates)

    def test_skips_sessions_without_snapshots(self):
        days = [date(2026, 8, 19), date(2026, 8, 20)]
        db = self._multi_session_db([date(2026, 8, 20)])

        written, skipped = asyncio.run(tool.refresh_symbol(db, "SPY", days))

        assert (written, skipped) == (1, 1)
        assert [r["session_date"] for r in db.written] == [date(2026, 8, 20)]

    def test_dry_run_writes_nothing(self):
        days = [date(2026, 8, 21)]
        db = self._multi_session_db(days)

        written, skipped = asyncio.run(
            tool.refresh_symbol(db, "SPY", days, dry_run=True)
        )

        assert (written, skipped) == (1, 0)
        assert db.written == []

    def test_a_failed_write_is_counted_and_does_not_abort_the_run(self):
        days = [date(2026, 8, 20), date(2026, 8, 21)]
        db = self._multi_session_db(days)
        calls = {"n": 0}

        async def flaky(row):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient")
            db.written.append(dict(row))

        db.upsert_regime_session = flaky

        written, skipped = asyncio.run(tool.refresh_symbol(db, "SPY", days))

        assert (written, skipped) == (1, 1)
        assert [r["session_date"] for r in db.written] == [date(2026, 8, 21)]

    def test_rerunning_is_idempotent_in_the_values_it_computes(self):
        days = [date(2026, 8, 21)]
        db = self._multi_session_db(days)

        asyncio.run(tool.refresh_symbol(db, "SPY", days))
        first = dict(db.written[-1])
        asyncio.run(tool.refresh_symbol(db, "SPY", days))
        second = dict(db.written[-1])

        assert first["lean_raw"] == second["lean_raw"]
        assert first["state"] == second["state"]


class TestDefaultSymbols:
    def test_reads_the_bulletin_symbol_list(self, monkeypatch):
        monkeypatch.setenv("BULLETIN_TWEET_SYMBOLS", "spy, qqq ,SPX")
        assert tool.default_symbols() == ["SPY", "QQQ", "SPX"]

    def test_falls_back_to_spy(self, monkeypatch):
        monkeypatch.delenv("BULLETIN_TWEET_SYMBOLS", raising=False)
        assert tool.default_symbols() == ["SPY"]

    def test_an_empty_env_var_falls_back_rather_than_refreshing_nothing(
        self, monkeypatch
    ):
        monkeypatch.setenv("BULLETIN_TWEET_SYMBOLS", "  ,  ")
        assert tool.default_symbols() == ["SPY"]


# --------------------------------------------------------------------------- #
# The refresh tool's calendar gate and failure signal
# --------------------------------------------------------------------------- #
class TestRefreshWindow:
    def test_open_inside_the_cash_session(self):
        assert tool.within_refresh_window(
            datetime.combine(DAY, time(12, 0), tzinfo=ET)
        )

    def test_closed_before_there_is_anything_to_compare(self):
        """The read is SINCE THE OPEN, so at 09:31 there is no second
        snapshot and every symbol logs a skip."""
        assert not tool.within_refresh_window(
            datetime.combine(DAY, time(9, 31), tzinfo=ET)
        )

    def test_open_past_the_bell_so_the_last_write_lands(self):
        assert tool.within_refresh_window(
            datetime.combine(DAY, time(16, 15), tzinfo=ET)
        )
        assert not tool.within_refresh_window(
            datetime.combine(DAY, time(17, 0), tzinfo=ET)
        )

    def test_closed_at_the_weekend(self):
        saturday = date(2026, 8, 22)
        assert saturday.weekday() == 5
        assert not tool.within_refresh_window(
            datetime.combine(saturday, time(12, 0), tzinfo=ET)
        )

    def test_a_run_that_writes_nothing_inside_the_window_fails(self):
        """The bug this guards is not a crash — it is a job that exits 0
        having written nothing, every day, so the history strip stays empty
        and nobody is told.  The calendar gate above has already excluded the
        days where writing nothing is correct."""
        db = FakeDb({})  # no snapshots: every session is skipped

        code = asyncio.run(
            tool._run(["SPY"], backfill=5, dry_run=False, as_of=DAY, db=db)
        )

        assert code == 1
        assert db.written == []

    def test_a_run_that_writes_something_succeeds(self):
        db = FakeDb(
            {
                _et(DAY, 9, 30): (100.0, _chain({99: 0.0, 100: 0.0})),
                _et(DAY, 16, 0): (100.0, _chain({99: 500.0, 100: 0.0})),
            }
        )

        code = asyncio.run(
            tool._run(["SPY"], backfill=1, dry_run=False, as_of=DAY, db=db)
        )

        assert code == 0
        assert [r["session_date"] for r in db.written] == [DAY]
