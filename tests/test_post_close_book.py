"""The book after the close: expired expirations leave every current-book read.

After the 16:00 ET close the analytics engine keeps a cash-index session's last
snapshot (SPX, NDX, and ES / NQ projected from them) as the newest row until
the next open.  That snapshot still carries the day's expiring contracts,
priced in their final minutes when their gamma is largest, so overnight and
all weekend the strike panel and the walls showed positioning that no longer
existed.  On Sunday 2026-11-01 the NQ Gamma Terminal's longest put bars were
96% Friday-expired contracts, and the public SPX Put Wall sat on a strike that
was 72% of them.

These tests pin the read-side rule:

  * ``first_live_expiration`` moves past the day's own expiration at the
    close (13:00 on an early-close day), never before it;
  * the NEWEST strike-profile bucket, read after the close, leaves out the
    expired expirations, re-picks its walls from what survives, and names the
    dates it removed in ``expired_excluded``; every earlier bucket is history
    and keeps them;
  * the summary re-picks its walls and re-sums its Net GEX totals the same
    way, and the profile reports the summary's walls;
  * the latest by-strike rows drop expired expirations in SQL;
  * a read made while the session is open is untouched and costs nothing
    extra.

The unit suite pins the expiration clock before every fixture date
(``tests/conftest.py``); each test here sets the moment it reads at.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

import src.market_calendar as market_calendar
from src.api.database import DatabaseManager
from src.api.models import GEXProfile, GEXSummary, StrikeProfileBucket
from src.market_calendar import first_live_expiration

_ET = ZoneInfo("America/New_York")

FRIDAY = date(2026, 10, 30)  # the session's own expiration, gone at 16:00 ET
MONDAY = date(2026, 11, 2)


def _et(day: int, hhmm: str, month: int = 10) -> datetime:
    hour, minute = (int(x) for x in hhmm.split(":"))
    return datetime(2026, month, day, hour, minute, tzinfo=_ET).astimezone(timezone.utc)


def _read_at(monkeypatch, moment: datetime) -> None:
    monkeypatch.setattr(market_calendar, "_expiration_clock", lambda: moment)


# Friday's last two 5-minute buckets and the snapshots that represent them.
BUCKET_A, REP_A = _et(30, "15:50"), _et(30, "15:54")
BUCKET_B, REP_B = _et(30, "15:55"), _et(30, "15:59")
SUNDAY_NIGHT = _et(1, "23:40", month=11)
FRIDAY_IN_SESSION = _et(30, "15:59")


# ---------------------------------------------------------------------------
# first_live_expiration
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "moment,expected",
    [
        (_et(30, "09:29"), FRIDAY),  # pre-open: the day's contracts still trade
        (_et(30, "15:59"), FRIDAY),
        (_et(30, "16:00"), date(2026, 10, 31)),  # gone at the bell
        (_et(30, "23:59"), date(2026, 10, 31)),
        (_et(1, "12:00", month=11), date(2026, 11, 1)),  # weekend: Friday's are gone
        (SUNDAY_NIGHT, MONDAY),  # nothing expires on a Sunday either way
        (_et(2, "08:00", month=11), MONDAY),  # Monday's still trade pre-open
    ],
)
def test_the_floor_moves_past_the_days_expiration_at_the_close(moment, expected):
    assert first_live_expiration(moment) == expected


def test_an_early_close_moves_the_floor_at_one_oclock(monkeypatch):
    half_day = date(2026, 11, 27)  # the day after Thanksgiving
    monkeypatch.setattr(market_calendar, "NYSE_HALF_DAYS", {half_day})
    assert first_live_expiration(_et(27, "12:59", month=11)) == half_day
    assert first_live_expiration(_et(27, "13:00", month=11)) == date(2026, 11, 28)


def test_the_floor_reads_naive_values_as_utc_and_defaults_to_now(monkeypatch):
    # 20:30 UTC on Friday is 16:30 ET: after the close.
    assert first_live_expiration(datetime(2026, 10, 30, 20, 30)) == date(2026, 10, 31)
    _read_at(monkeypatch, SUNDAY_NIGHT)
    assert first_live_expiration() == MONDAY


# ---------------------------------------------------------------------------
# Fixtures shared by the reads
# ---------------------------------------------------------------------------

# Per-strike raw gamma (OI-weighted, unsigned) in the closing snapshot, summed
# over every expiration as the timeseries query returns it, and the part of it
# that is NOT Friday's.  The 495 put is the published Put Wall at 15:59 and is
# almost all Friday 0DTE; the 490 put is longer-dated and survives.
CLOSING = {  # strike: (call_raw, put_raw)
    485.0: (0.0, 100.0),
    490.0: (0.0, 150.0),
    495.0: (0.0, 400.0),
    505.0: (200.0, 0.0),
    510.0: (50.0, 0.0),
}
SURVIVING = {
    485.0: (0.0, 100.0),
    490.0: (0.0, 150.0),
    495.0: (0.0, 10.0),
    505.0: (200.0, 0.0),
    510.0: (50.0, 0.0),
}
CLOSE = 500.0
SCALE = 100 * CLOSE * CLOSE * 0.01  # dollar GEX per unit of raw gamma


def _flat_row(bucket_ts, rep_ts, strike, call_raw, put_raw):
    """One (bucket, strike) row shaped like the timeseries query's SELECT."""
    return {
        "timestamp": bucket_ts,
        "open": Decimal(str(CLOSE)),
        "high": Decimal(str(CLOSE + 1)),
        "low": Decimal(str(CLOSE - 1)),
        "close": Decimal(str(CLOSE)),
        "gamma_flip": Decimal("497.5"),
        "pin_strike": None,
        "pin_confidence": None,
        "max_gamma_strike": None,
        "wall_anchor": Decimal("500"),
        "rep_ts": rep_ts,
        "wall_refresh_ts": rep_ts,
        "stored_call_wall": Decimal("505"),
        "stored_put_wall": Decimal("495"),
        "strike": Decimal(str(strike)),
        "call_gamma_raw": Decimal(str(call_raw)),
        "put_gamma_raw": Decimal(str(put_raw)),
        "call_gex": Decimal(str(call_raw * SCALE)),
        "put_gex": Decimal(str(-put_raw * SCALE)),
        "net_gex": Decimal(str((call_raw - put_raw) * SCALE)),
        "call_oi": 100,
        "put_oi": 200,
    }


def _timeseries_rows():
    rows = []
    for bucket_ts, rep_ts in ((BUCKET_A, REP_A), (BUCKET_B, REP_B)):
        for strike, (call_raw, put_raw) in CLOSING.items():
            rows.append(_flat_row(bucket_ts, rep_ts, strike, call_raw, put_raw))
    return rows


def _surviving_rows():
    return [
        {
            "strike": Decimal(str(strike)),
            "call_gamma": Decimal(str(call_raw)),
            "put_gamma": Decimal(str(put_raw)),
            "call_oi": 40,
            "put_oi": 60,
        }
        for strike, (call_raw, put_raw) in SURVIVING.items()
    ]


class _Conn:
    """Answers each read by the marker it carries, and records the after-close ones."""

    def __init__(self, summary_row=None, profile_row=None):
        self.summary_row = summary_row
        self.profile_row = profile_row
        self.expired_calls = []
        self.surviving_calls = []
        self.body_calls = 0

    async def fetch(self, query, *args, **_kwargs):
        if "-- expired expirations in a snapshot" in query:
            self.expired_calls.append(args)
            _symbol, snapshot_ts, floor, exp_filter = args
            exps = [FRIDAY, MONDAY] if exp_filter is None else exp_filter
            return [{"expiration": e} for e in exps if e < floor]
        if "-- surviving strikes in a snapshot" in query:
            self.surviving_calls.append(args)
            return _surviving_rows()
        if "bucket_reps AS" in query:
            self.body_calls += 1
            return _timeseries_rows()
        raise AssertionError(f"unexpected query: {query[:120]}")

    async def fetchrow(self, query, *args, **_kwargs):
        if "newest-row probe" in query:
            return {
                "timestamp": self.summary_row["timestamp"],
                "computed_at": self.summary_row["computed_at"],
            }
        if "FROM gex_profile gp" in query:
            return self.profile_row
        self.body_calls += 1
        return dict(self.summary_row)


def _db(conn) -> DatabaseManager:
    db = DatabaseManager()

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    db._strike_profile_timeseries_cache_ttl_seconds = 0.0
    return db


# ---------------------------------------------------------------------------
# Strike-profile timeseries: the newest bucket is the book carried forward
# ---------------------------------------------------------------------------


def _strikes_by_price(bucket):
    return {float(s["strike"]): s for s in bucket["strikes"]}


def test_after_the_close_the_newest_bucket_leaves_out_the_expired_expiration(monkeypatch):
    _read_at(monkeypatch, SUNDAY_NIGHT)
    conn = _Conn()
    older, newest = asyncio.run(_db(conn).get_strike_profile_timeseries("SPX", "5min", 40))

    # The newest bucket is the closing book without Friday's contracts...
    assert newest["expired_excluded"] == [FRIDAY]
    strikes = _strikes_by_price(newest)
    assert float(strikes[495.0]["put_gamma"]) == pytest.approx(-10.0 * SCALE)
    assert float(strikes[490.0]["put_gamma"]) == pytest.approx(-150.0 * SCALE)
    assert float(strikes[505.0]["call_gamma"]) == pytest.approx(200.0 * SCALE)
    assert float(strikes[495.0]["net_gamma"]) == pytest.approx(-10.0 * SCALE)
    assert strikes[490.0]["put_oi"] == 60
    # ...so the Put Wall, which was the expiring 495 put, hands over to the
    # biggest surviving put; the Call Wall, built from contracts that survive,
    # keeps its place.
    assert newest["put_wall"] == 490.0
    assert [w["strike"] for w in newest["put_walls"]] == [490.0, 485.0, 495.0]
    assert newest["call_wall"] == 505.0
    assert newest["put_walls"][0]["strength"] == pytest.approx(150.0 * SCALE)
    # The All view keeps the engine's flip.
    assert newest["gamma_flip"] == Decimal("497.5")

    # The earlier bucket is history: Friday's contracts were trading then.
    assert "expired_excluded" not in older
    assert float(_strikes_by_price(older)[495.0]["put_gamma"]) == pytest.approx(-400.0 * SCALE)
    assert older["put_wall"] == 495.0

    # The after-close read asked about the newest bucket's snapshot only.
    assert [args[:3] for args in conn.expired_calls] == [("SPX", REP_B, MONDAY)]
    assert len(conn.surviving_calls) == 1


def test_during_the_session_the_newest_bucket_is_served_as_read(monkeypatch):
    _read_at(monkeypatch, FRIDAY_IN_SESSION)
    conn = _Conn()
    older, newest = asyncio.run(_db(conn).get_strike_profile_timeseries("SPX", "5min", 40))

    assert "expired_excluded" not in newest
    assert float(_strikes_by_price(newest)[495.0]["put_gamma"]) == pytest.approx(-400.0 * SCALE)
    assert newest["put_wall"] == 495.0
    # Nothing has expired, so nothing extra was read.
    assert conn.expired_calls == [] and conn.surviving_calls == []


def test_a_delayed_read_judges_expiry_at_its_own_moment(monkeypatch):
    """A 15-minute-delayed view at 16:10 shows the book as of 15:55, when
    Friday's contracts still traded; the same view at 16:20 shows 16:05."""
    _read_at(monkeypatch, _et(30, "16:10"))
    conn = _Conn()
    db = _db(conn)
    _, newest = asyncio.run(
        db.get_strike_profile_timeseries("SPX", "5min", 3, as_of=_et(30, "15:55"))
    )
    assert "expired_excluded" not in newest
    assert conn.expired_calls == []

    _, newest = asyncio.run(
        db.get_strike_profile_timeseries("SPX", "5min", 3, as_of=_et(30, "16:05"))
    )
    assert newest["expired_excluded"] == [FRIDAY]
    assert newest["put_wall"] == 490.0


def test_a_filtered_view_drops_only_its_own_expired_dates(monkeypatch):
    _read_at(monkeypatch, SUNDAY_NIGHT)
    conn = _Conn()
    db = _db(conn)

    async def _no_chain(*_args, **_kwargs):
        return {}

    db._scope_wall_chain = _no_chain  # type: ignore[method-assign]
    _, newest = asyncio.run(db.get_strike_profile_timeseries("SPX", "5min", 40, [FRIDAY, MONDAY]))
    assert newest["expired_excluded"] == [FRIDAY]
    # The filter rides into both after-close reads.
    assert conn.expired_calls[0][3] == [FRIDAY, MONDAY]
    assert conn.surviving_calls[0][3] == [FRIDAY, MONDAY]
    # A filtered view's flip is recomputed from the strikes it now draws.
    assert newest["gamma_flip"] != Decimal("497.5")


def test_the_assembled_window_rebuilds_the_live_bucket_and_caches_history(monkeypatch):
    """With per-bucket caching on, the window is read in chunks; only the
    chunk holding the live bucket can tell it is the newest, and the closed
    bucket that goes into the cache is the history, not the after-close book."""
    _read_at(monkeypatch, SUNDAY_NIGHT)
    conn = _Conn()
    db = _db(conn)
    db._strike_profile_bucket_cache_ttl_seconds = 900.0

    async def _bucket_list(_symbol, _timeframe, _window_units):
        return [BUCKET_A, BUCKET_B]

    db._strike_profile_bucket_list = _bucket_list  # type: ignore[method-assign]
    older, newest = asyncio.run(db.get_strike_profile_timeseries("SPX", "5min", 40))
    assert newest["expired_excluded"] == [FRIDAY] and newest["put_wall"] == 490.0
    assert "expired_excluded" not in older and older["put_wall"] == 495.0


# ---------------------------------------------------------------------------
# Summary and profile: walls and totals follow the surviving book
# ---------------------------------------------------------------------------


def _summary_row():
    return {
        "timestamp": REP_B,
        "symbol": "SPX",
        "computed_at": REP_B,
        "data_as_of": REP_B,
        "spot_price": Decimal(str(CLOSE)),
        "total_call_gex": Decimal(str(250.0 * SCALE)),
        "total_put_gex": Decimal(str(-650.0 * SCALE)),
        "net_gex": Decimal(str(-400.0 * SCALE)),
        "net_gex_at_spot": Decimal("-1000"),
        "gamma_flip": Decimal("497.5"),
        "max_pain": Decimal("500"),
        "call_wall": Decimal("505"),
        "put_wall": Decimal("495"),
        "wall_anchor": Decimal("500"),
        "call_wall_ladder_strikes": [Decimal("505"), Decimal("510")],
        "call_wall_ladder_gammas": [Decimal("200"), Decimal("50")],
        "put_wall_ladder_strikes": [Decimal("495"), Decimal("490"), Decimal("485")],
        "put_wall_ladder_gammas": [Decimal("400"), Decimal("150"), Decimal("100")],
        "max_gamma_strike": Decimal("495"),
        "pin_strike": None,
    }


def test_after_the_close_the_summary_repicks_its_walls_and_resums_its_totals(monkeypatch):
    _read_at(monkeypatch, SUNDAY_NIGHT)
    conn = _Conn(summary_row=_summary_row())
    summary = asyncio.run(_db(conn).get_latest_gex_summary("SPX"))

    assert summary["expired_excluded"] == [FRIDAY]
    assert summary["put_wall"] == 490.0
    assert summary["call_wall"] == 505.0
    assert [w["label"] for w in summary["put_walls"]] == ["P1", "P2", "P3"]
    assert [w["strike"] for w in summary["put_walls"]] == [490.0, 485.0, 495.0]
    assert summary["total_call_gex"] == pytest.approx(250.0 * SCALE)
    assert summary["total_put_gex"] == pytest.approx(-260.0 * SCALE)
    assert summary["net_gex"] == pytest.approx(-10.0 * SCALE)
    # What the engine computed whole at publish time stays as published.
    assert summary["gamma_flip"] == Decimal("497.5")
    assert summary["max_pain"] == Decimal("500")
    assert summary["net_gex_at_spot"] == Decimal("-1000")
    # The anchor rode along for the re-pick only.
    assert "wall_anchor" not in summary
    # And the response model carries the dates as ISO strings.
    body = GEXSummary(**summary).model_dump(mode="json")
    assert body["expired_excluded"] == ["2026-10-30"]
    assert body["put_wall"] == 490.0


def test_during_the_session_the_summary_is_served_as_read(monkeypatch):
    _read_at(monkeypatch, FRIDAY_IN_SESSION)
    conn = _Conn(summary_row=_summary_row())
    summary = asyncio.run(_db(conn).get_latest_gex_summary("SPX"))

    assert "expired_excluded" not in summary
    assert summary["put_wall"] == Decimal("495")
    assert summary["net_gex"] == Decimal(str(-400.0 * SCALE))
    assert "wall_anchor" not in summary
    assert conn.expired_calls == []
    assert GEXSummary(**summary).model_dump(mode="json")["expired_excluded"] == []


def test_a_delayed_summary_judges_expiry_at_its_own_moment(monkeypatch):
    _read_at(monkeypatch, SUNDAY_NIGHT)
    conn = _Conn(summary_row=_summary_row())
    db = _db(conn)
    assert "expired_excluded" not in asyncio.run(
        db.get_latest_gex_summary("SPX", as_of=_et(30, "15:59"))
    )
    delayed = asyncio.run(db.get_latest_gex_summary("SPX", as_of=_et(30, "16:01")))
    assert delayed["expired_excluded"] == [FRIDAY] and delayed["put_wall"] == 490.0


def test_after_the_close_the_profile_reports_the_summarys_walls(monkeypatch):
    _read_at(monkeypatch, SUNDAY_NIGHT)
    profile_row = {
        "symbol": "SPX",
        "timestamp": REP_B,
        "spot_price": Decimal(str(CLOSE)),
        "span_pct": 0.2,
        "profile": [{"price": 495.0, "gex": -1.0}, {"price": 505.0, "gex": 1.0}],
        "gamma_flip": Decimal("497.5"),
        "net_gex_at_spot": Decimal("-1000"),
        "call_wall": Decimal("505"),
        "put_wall": Decimal("495"),
    }
    conn = _Conn(summary_row=_summary_row(), profile_row=profile_row)
    profile = asyncio.run(_db(conn).get_latest_gex_profile("SPX"))
    assert (profile["call_wall"], profile["put_wall"]) == (505.0, 490.0)
    assert profile["expired_excluded"] == [FRIDAY]
    assert GEXProfile(**profile).model_dump(mode="json")["expired_excluded"] == ["2026-10-30"]

    _read_at(monkeypatch, FRIDAY_IN_SESSION)
    in_session = asyncio.run(
        _db(_Conn(summary_row=_summary_row(), profile_row=profile_row)).get_latest_gex_profile(
            "SPX"
        )
    )
    assert in_session["put_wall"] == Decimal("495")
    assert "expired_excluded" not in in_session


def test_a_failed_after_close_read_serves_the_closing_book_as_published(monkeypatch):
    """The after-close book is a refinement: if its read fails the endpoint
    still answers, with the snapshot as the engine published it."""
    _read_at(monkeypatch, SUNDAY_NIGHT)

    class _Failing(_Conn):
        async def fetch(self, query, *args, **kwargs):
            if "-- expired expirations in a snapshot" in query:
                raise RuntimeError("statement timeout")
            return await super().fetch(query, *args, **kwargs)

    _, newest = asyncio.run(_db(_Failing()).get_strike_profile_timeseries("SPX", "5min", 40))
    assert "expired_excluded" not in newest and newest["put_wall"] == 495.0


def test_the_bucket_model_serializes_the_removed_dates():
    bucket = {
        "timestamp": BUCKET_B,
        "symbol": "SPX",
        "close": Decimal("500"),
        "strikes": [],
        "expired_excluded": [FRIDAY],
    }
    assert StrikeProfileBucket(**bucket).model_dump(mode="json")["expired_excluded"] == [
        "2026-10-30"
    ]
    del bucket["expired_excluded"]
    assert StrikeProfileBucket(**bucket).model_dump(mode="json")["expired_excluded"] == []


# ---------------------------------------------------------------------------
# Latest by-strike rows
# ---------------------------------------------------------------------------


class _RecordingConn:
    def __init__(self):
        self.calls = []

    async def fetch(self, query, *args, **_kwargs):
        self.calls.append((query, args))
        return []


def test_the_latest_by_strike_rows_leave_out_expired_expirations(monkeypatch):
    conn = _RecordingConn()
    db = _db(conn)

    _read_at(monkeypatch, FRIDAY_IN_SESSION)
    asyncio.run(db.get_gex_by_strike("SPX", 200, "impact"))
    _read_at(monkeypatch, _et(30, "16:00"))
    asyncio.run(db.get_gex_by_strike("SPX", 200, "impact"))

    (sql, before), (_, after) = conn.calls
    assert "AND g.expiration >= $3::date" in sql
    assert before == ("SPX", 200, FRIDAY)
    # The cutoff is part of the cache key: the bell is not served from a
    # body cached a second before it.
    assert after == ("SPX", 200, date(2026, 10, 31))
