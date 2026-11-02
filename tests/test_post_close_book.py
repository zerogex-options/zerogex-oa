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
import json
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
        # Friday's same-day pin, as the engine published it.
        "pin_strike": Decimal("500"),
        "pin_confidence": 0.6,
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


# Monday's contracts in the closing snapshot's chain: the call open interest
# piles up at 505, a strike above the 500 close that price can reach by
# Monday's settlement, so the rolled Pin has somewhere to land.
MONDAY_CHAIN = [
    {
        "option_symbol": f"SPXW 261102{kind}{int(strike)}",
        "strike": Decimal(str(strike)),
        "expiration": MONDAY,
        "option_type": kind,
        "open_interest": oi,
        "implied_volatility": Decimal("0.18"),
    }
    for strike, kind, oi in (
        (495.0, "C", 1500),
        (500.0, "C", 4000),
        (505.0, "C", 9000),
        (510.0, "C", 2500),
        (495.0, "P", 800),
        (505.0, "P", 300),
    )
]


# Every contract on or after Monday in the closing snapshot: Monday's above,
# and a Nov 20 book with puts stacked under the close and calls over it, so
# the re-priced dealer-gamma curve crosses zero near spot.
LATER_CHAIN = [
    {
        "option_symbol": f"SPXW 261120{kind}{int(strike)}",
        "strike": Decimal(str(strike)),
        "expiration": date(2026, 11, 20),
        "option_type": kind,
        "open_interest": oi,
        "implied_volatility": Decimal("0.2"),
    }
    for strike, kind, oi in (
        (470.0, "P", 20000),
        (480.0, "P", 30000),
        (490.0, "P", 25000),
        (495.0, "P", 8000),
        (505.0, "C", 8000),
        (510.0, "C", 25000),
        (520.0, "C", 30000),
        (530.0, "C", 20000),
    )
]
SURVIVING_CHAIN = MONDAY_CHAIN + LATER_CHAIN


def _engine_options(rows):
    return [
        {
            **row,
            "strike": float(row["strike"]),
            "implied_volatility": float(row["implied_volatility"]),
            "gamma": 0.001,
            "volume": 0,
        }
        for row in rows
    ]


def _expected_rolled_flip():
    """The flip family the API should roll to: the engine's own computation on
    the surviving chain, at the snapshot."""
    from src.analytics.main_engine import AnalyticsEngine

    return AnalyticsEngine(underlying="SPX").spot_shift_readings(
        _engine_options(SURVIVING_CHAIN), CLOSE, REP_B
    )


def _expected_rolled_pin(spot=CLOSE):
    """The Pin the API should roll to: the shared model, run directly."""
    from src.analytics.main_engine import pin_strike_for_expiration
    from src.config import RISK_FREE_RATE, resolve_dividend_yield

    options = [
        {
            **row,
            "strike": float(row["strike"]),
            "implied_volatility": float(row["implied_volatility"]),
        }
        for row in MONDAY_CHAIN
    ]
    return pin_strike_for_expiration(
        options,
        symbol="SPX",
        spot=spot,
        timestamp=REP_B,
        expiration=MONDAY,
        risk_free_rate=RISK_FREE_RATE,
        dividend_yield=resolve_dividend_yield("SPX"),
    )


class _Conn:
    """Answers each read by the marker it carries, and records the after-close ones."""

    def __init__(self, summary_row=None, profile_row=None):
        self.summary_row = summary_row
        self.profile_row = profile_row
        self.expiration_calls = []
        self.surviving_calls = []
        self.chain_calls = []
        self.surviving_chain_calls = []
        self.body_calls = 0

    async def fetch(self, query, *args, **_kwargs):
        if "-- expirations in a snapshot" in query:
            self.expiration_calls.append(args)
            return [{"expiration": e} for e in (FRIDAY, MONDAY, date(2026, 11, 20))]
        if "-- surviving strikes in a snapshot" in query:
            self.surviving_calls.append(args)
            return _surviving_rows()
        if "-- one expiration's chain at a snapshot" in query:
            self.chain_calls.append(args)
            _symbol, _ts, _since, expiration = args
            return [row for row in MONDAY_CHAIN if row["expiration"] == expiration]
        if "-- surviving chain at a snapshot" in query:
            self.surviving_chain_calls.append(args)
            _symbol, _ts, _since, floor = args
            return [
                {**row, "gamma": Decimal("0.001"), "volume": 0}
                for row in SURVIVING_CHAIN
                if row["expiration"] >= floor
            ]
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
        if "-- spot at a snapshot" in query:
            return {"close": Decimal(str(CLOSE))}
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
    # The All view's flip is the engine's spot-shift flip re-priced on the
    # surviving chain; the earlier bucket keeps the one it had.
    assert newest["gamma_flip"] == _expected_rolled_flip()["gamma_flip_point"]
    assert older["gamma_flip"] == Decimal("497.5")

    # The earlier bucket is history: Friday's contracts were trading then.
    assert "expired_excluded" not in older
    assert float(_strikes_by_price(older)[495.0]["put_gamma"]) == pytest.approx(-400.0 * SCALE)
    assert older["put_wall"] == 495.0

    # The after-close read asked about the newest bucket's snapshot only.
    assert conn.expiration_calls == [("SPX", REP_B)]
    assert [args[:3] for args in conn.surviving_calls] == [("SPX", REP_B, MONDAY)]

    # The Pin rolls with the book: priced on Monday's contracts in the same
    # snapshot.  The earlier bucket keeps the pin it had then.
    expected = _expected_rolled_pin()
    assert expected.is_active
    assert newest["pin_strike"] == expected.pin_strike
    assert newest["pin_confidence"] == pytest.approx(expected.pin_confidence)
    assert older["pin_strike"] == Decimal("500")
    assert [args[3] for args in conn.chain_calls] == [MONDAY]


def test_during_the_session_the_newest_bucket_is_served_as_read(monkeypatch):
    _read_at(monkeypatch, FRIDAY_IN_SESSION)
    conn = _Conn()
    older, newest = asyncio.run(_db(conn).get_strike_profile_timeseries("SPX", "5min", 40))

    assert "expired_excluded" not in newest
    assert float(_strikes_by_price(newest)[495.0]["put_gamma"]) == pytest.approx(-400.0 * SCALE)
    assert newest["put_wall"] == 495.0
    assert newest["pin_strike"] == Decimal("500")
    # Nothing has expired, so nothing extra was read.
    assert conn.expiration_calls == [] and conn.surviving_calls == [] and conn.chain_calls == []


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
    assert conn.expiration_calls == []

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
    # The filter decides what counts as expired here and rides into the
    # survivors' read; the Pin is whole-chain and rolls the same way anyway.
    assert conn.surviving_calls[0][3] == [FRIDAY, MONDAY]
    assert newest["pin_strike"] == _expected_rolled_pin().pin_strike
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
        # Friday's same-day pin and max pain, as the engine published them,
        # with the per-expiration max pain the row stores (JSONB comes back
        # from asyncpg as text).
        "pin_strike": Decimal("500"),
        "pin_score": 1.0e9,
        "pin_confidence": 0.6,
        "pin_strike_reason": None,
        "max_pain_by_expiration": json.dumps(
            {"2026-10-30": 500.0, "2026-11-02": 505.0, "2026-11-20": 520.0}
        ),
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
    # Max Pain and the Pin describe ONE expiration, so they roll to the one
    # that settles next: Monday's max pain from the row's own breakdown, and
    # the pin priced on Monday's contracts in the same snapshot.
    assert summary["rolled_to_expiration"] == MONDAY
    assert summary["max_pain"] == 505.0
    expected = _expected_rolled_pin()
    assert summary["pin_strike"] == expected.pin_strike
    assert summary["pin_score"] == pytest.approx(expected.pin_score)
    assert summary["pin_confidence"] == pytest.approx(expected.pin_confidence)
    assert summary["pin_strike_reason"] is None
    # The GEX King and the flip family come off the surviving book too: the
    # King from the surviving strikes (505, the largest |net| once the
    # expiring 495 puts are gone), the flip family from the engine's own
    # computation on the surviving chain.
    assert summary["max_gamma_strike"] == 505.0
    flip = _expected_rolled_flip()
    assert flip["gamma_flip_point"] is not None
    assert summary["gamma_flip"] == flip["gamma_flip_point"]
    assert summary["gamma_flip_raw"] == flip["gamma_flip_raw"]
    assert summary["gamma_flip_span_used"] == flip["gamma_flip_span_used"]
    assert summary["gamma_flip_reason"] is None
    assert summary["net_gex_at_spot"] == pytest.approx(flip["net_gex_at_spot"])
    assert summary["flip_distance"] == pytest.approx((CLOSE - flip["gamma_flip_point"]) / CLOSE)
    assert summary["convexity_risk"] == pytest.approx(
        abs(summary["net_gex"]) / abs(summary["flip_distance"])
    )
    # The anchor and the breakdown rode along for the after-close step only.
    assert "wall_anchor" not in summary
    assert "max_pain_by_expiration" not in summary
    # And the response model carries the dates as ISO strings.
    body = GEXSummary(**summary).model_dump(mode="json")
    assert body["expired_excluded"] == ["2026-10-30"]
    assert body["rolled_to_expiration"] == "2026-11-02"
    assert body["put_wall"] == 490.0
    assert body["max_pain"] == 505.0


def test_during_the_session_the_summary_is_served_as_read(monkeypatch):
    _read_at(monkeypatch, FRIDAY_IN_SESSION)
    conn = _Conn(summary_row=_summary_row())
    summary = asyncio.run(_db(conn).get_latest_gex_summary("SPX"))

    assert "expired_excluded" not in summary
    assert summary["put_wall"] == Decimal("495")
    assert summary["net_gex"] == Decimal(str(-400.0 * SCALE))
    # Friday's own max pain and pin, as published, while Friday still trades.
    assert summary["max_pain"] == Decimal("500")
    assert summary["pin_strike"] == Decimal("500")
    assert "wall_anchor" not in summary
    assert "max_pain_by_expiration" not in summary
    assert summary["gamma_flip"] == Decimal("497.5")
    assert summary["max_gamma_strike"] == Decimal("495")
    assert conn.expiration_calls == [] and conn.chain_calls == []
    assert conn.surviving_chain_calls == []
    body = GEXSummary(**summary).model_dump(mode="json")
    assert body["expired_excluded"] == [] and body["rolled_to_expiration"] is None


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
    # The curve is the surviving book's, with its flip and its value at spot.
    flip = _expected_rolled_flip()
    assert profile["gamma_flip"] == flip["gamma_flip_point"]
    assert profile["net_gex_at_spot"] == pytest.approx(flip["net_gex_at_spot"])
    assert profile["profile"] == [
        {"price": float(p), "gex": float(g)} for p, g in flip["gamma_profile"]
    ]
    assert profile["span_pct"] == flip["gamma_flip_span_used"]
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
            if "-- expirations in a snapshot" in query:
                raise RuntimeError("statement timeout")
            return await super().fetch(query, *args, **kwargs)

    _, newest = asyncio.run(_db(_Failing()).get_strike_profile_timeseries("SPX", "5min", 40))
    assert "expired_excluded" not in newest and newest["put_wall"] == 495.0
    # With no read of what is next, the expired same-day pin is not served.
    assert newest["pin_strike"] is None


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


# ---------------------------------------------------------------------------
# The Pin model the API rolls with is the engine's own
# ---------------------------------------------------------------------------


def test_the_engine_and_the_api_price_the_pin_with_one_model():
    """The engine's same-day pin and the API's rolled pin both come from
    pin_strike_for_expiration; for the same contracts and expiration they are
    the same answer, so a rolled pin is the pin the engine would publish."""
    from src.analytics.main_engine import AnalyticsEngine, pin_strike_for_expiration

    engine = AnalyticsEngine(underlying="SPX")
    in_session = _et(2, "10:30", month=11)
    options = [
        {
            **row,
            "strike": float(row["strike"]),
            "implied_volatility": float(row["implied_volatility"]),
        }
        for row in MONDAY_CHAIN
    ]
    published = engine._calculate_pin_strike(options, CLOSE, in_session)
    shared = pin_strike_for_expiration(
        options,
        symbol=engine.db_symbol,
        spot=CLOSE,
        timestamp=in_session,
        expiration=MONDAY,
        risk_free_rate=engine.risk_free_rate,
        dividend_yield=engine.dividend_yield,
    )
    assert published.is_active
    assert published.summary_fields() == shared.summary_fields()


# ---------------------------------------------------------------------------
# /api/max-pain/current
# ---------------------------------------------------------------------------


class _MaxPainConn:
    """The daily snapshot (as of Friday's pre-market), its per-expiration rows,
    the live overlay off the closing gex_summary row, and the after-close read."""

    async def fetchrow(self, query, *args, **_kwargs):
        if "FROM max_pain_oi_snapshot\n" in query:
            return {
                "symbol": "SPX",
                "as_of_date": FRIDAY,
                "source_timestamp": _et(30, "08:00"),
                "underlying_price": Decimal("498"),
                "max_pain": Decimal("500"),
                "difference": Decimal("2"),
            }
        if "live_max_pain" in query:
            return {
                "live_timestamp": REP_B,
                "live_max_pain": Decimal("500"),
                "live_max_pain_by_expiration": json.dumps(
                    {"2026-10-30": 500.0, "2026-11-02": 505.0, "2026-11-20": 520.0}
                ),
                "live_underlying_price": Decimal(str(CLOSE)),
            }
        raise AssertionError(f"unexpected fetchrow: {query[:120]}")

    async def fetch(self, query, *args, **_kwargs):
        if "FROM max_pain_oi_snapshot_expiration" in query:
            return [
                {
                    "expiration": exp,
                    "max_pain": Decimal(str(mp)),
                    "difference_from_underlying": Decimal(str(mp - 498)),
                    "strikes": "[]",
                }
                for exp, mp in ((FRIDAY, 500.0), (MONDAY, 505.0), (date(2026, 11, 20), 520.0))
            ]
        if "-- expirations in a snapshot" in query:
            return [{"expiration": e} for e in (FRIDAY, MONDAY, date(2026, 11, 20))]
        if "-- surviving strikes in a snapshot" in query:
            return _surviving_rows()
        raise AssertionError(f"unexpected fetch: {query[:120]}")


def test_after_the_close_current_max_pain_is_the_next_expirations(monkeypatch):
    _read_at(monkeypatch, SUNDAY_NIGHT)
    current = asyncio.run(_db(_MaxPainConn()).get_max_pain_current("SPX"))
    assert current["max_pain"] == 505.0
    assert current["difference"] == pytest.approx(505.0 - CLOSE)
    # The settled expiration is no longer "current" either.
    assert [e["expiration"] for e in current["expirations"]] == [MONDAY, date(2026, 11, 20)]

    _read_at(monkeypatch, FRIDAY_IN_SESSION)
    during = asyncio.run(_db(_MaxPainConn()).get_max_pain_current("SPX"))
    assert during["max_pain"] == Decimal("500")
    assert [e["expiration"] for e in during["expirations"]][0] == FRIDAY


# ---------------------------------------------------------------------------
# Pin stability: the closed session's path, hidden once the pin has rolled
# ---------------------------------------------------------------------------


def _pin_stability_client(monkeypatch):
    import sys
    from unittest.mock import AsyncMock

    from fastapi.testclient import TestClient

    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod

    frames = [
        {"timestamp": _et(30, f"{hh}:{mm:02d}"), "pin_strike": 500.0}
        for hh in (14, 15)
        for mm in range(0, 60, 5)
    ]
    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)
    dbmod.DatabaseManager.get_pin_path_for_session = AsyncMock(return_value=frames)

    from src.api.main import app

    return TestClient(app)


def test_pin_stability_is_hidden_once_the_session_has_closed(monkeypatch):
    client = _pin_stability_client(monkeypatch)
    _read_at(monkeypatch, FRIDAY_IN_SESSION)
    with client:
        during = client.get("/api/gex/pin-stability?symbol=SPX")
    assert during.status_code == 200
    assert during.json()["current_pin"] == 500.0

    _read_at(monkeypatch, _et(30, "16:00"))
    with client:
        after = client.get("/api/gex/pin-stability?symbol=SPX")
    assert after.status_code == 404


def test_without_the_live_row_current_max_pain_rolls_on_the_daily_snapshot(monkeypatch):
    """No live overlay: the daily snapshot's headline is its front expiration,
    and once that has settled the next listed one is the front."""

    class _NoLive(_MaxPainConn):
        async def fetchrow(self, query, *args, **kwargs):
            if "live_max_pain" in query:
                return None
            return await super().fetchrow(query, *args, **kwargs)

    _read_at(monkeypatch, SUNDAY_NIGHT)
    current = asyncio.run(_db(_NoLive()).get_max_pain_current("SPX"))
    assert current["max_pain"] == Decimal("505.0")
    assert [e["expiration"] for e in current["expirations"]] == [MONDAY, date(2026, 11, 20)]


def test_a_failed_chain_read_keeps_the_published_flip_and_still_rolls_the_king(monkeypatch):
    """The flip needs the chain; the King needs only the surviving strikes.
    Without the chain the flip family stays as published rather than failing
    the summary."""

    class _NoChain(_Conn):
        async def fetch(self, query, *args, **kwargs):
            if "-- surviving chain at a snapshot" in query:
                raise RuntimeError("statement timeout")
            return await super().fetch(query, *args, **kwargs)

    _read_at(monkeypatch, SUNDAY_NIGHT)
    summary = asyncio.run(_db(_NoChain(summary_row=_summary_row())).get_latest_gex_summary("SPX"))
    assert summary["gamma_flip"] == Decimal("497.5")
    assert summary["net_gex_at_spot"] == Decimal("-1000")
    assert summary["max_gamma_strike"] == 505.0
    assert summary["expired_excluded"] == [FRIDAY]
