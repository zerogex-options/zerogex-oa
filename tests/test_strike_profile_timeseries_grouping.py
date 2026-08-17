"""Unit coverage for get_strike_profile_timeseries' row-grouping + wall pass.

The flat ``(bucket_ts, strike)`` result set the SQL returns is folded into
per-bucket dicts in Python, and each bucket's ``call_wall`` / ``put_wall`` —
and, for a filtered expiration set, its ``gamma_flip`` — is computed from that
bucket's OWN strikes against that bucket's OWN close.  The only test that
exercised this fold was ``test_strike_profile_timeseries_expiration_sum``,
which is ``integration``-marked and skips unless a scratch-Postgres DSN is
exported, so the fold itself ran unguarded in CI.

These tests mock the connection and pin the fold's observable contract:

  * flat rows group into buckets, response ascending by bucket time
  * walls are strictly PER BUCKET (a later bucket's strikes must not leak
    into an earlier bucket's walls, and vice versa)
  * a bucket with no strike rows still appears, with ``strikes == []`` and
    NULL walls
  * all-zero strike rows are dropped from the payload
  * a bucket with no underlying close leaves both walls ``None``
  * ``expirations=None`` keeps the persisted ``gamma_flip``; a filtered set
    replaces it with the recomputed cumulative-net-GEX crossing
  * the result is served from the process-local cache on the second call
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from decimal import Decimal

from src.analytics.walls import compute_call_put_walls
from src.api.database import DatabaseManager

_TS1 = datetime(2026, 8, 17, 14, 30, tzinfo=timezone.utc)
_TS2 = datetime(2026, 8, 17, 14, 35, tzinfo=timezone.utc)
_TS3 = datetime(2026, 8, 17, 14, 40, tzinfo=timezone.utc)


class _FakeConn:
    """Returns a fixed flat row set and records how often it was queried."""

    def __init__(self, rows):
        self.rows = rows
        self.calls = 0

    async def fetch(self, _query, *_args, **_kwargs):
        self.calls += 1
        return self.rows


def _row(
    ts,
    close,
    strike=None,
    call_raw=0.0,
    put_raw=0.0,
    gamma_flip=Decimal("505.5"),
    call_gex=None,
    put_gex=None,
    net_gex=None,
    call_oi=100,
    put_oi=200,
):
    """One flat (bucket_ts, strike) row shaped like the SQL's SELECT list."""
    scale = 0.0 if close is None else 100 * float(close) * float(close) * 0.01
    return {
        "timestamp": ts,
        "open": None if close is None else Decimal(str(close)),
        "high": None if close is None else Decimal(str(close)) + 1,
        "low": None if close is None else Decimal(str(close)) - 1,
        "close": None if close is None else Decimal(str(close)),
        "gamma_flip": gamma_flip,
        "strike": None if strike is None else Decimal(str(strike)),
        "call_gamma_raw": Decimal(str(call_raw)),
        "put_gamma_raw": Decimal(str(put_raw)),
        "call_gex": Decimal(str(call_raw * scale)) if call_gex is None else Decimal(str(call_gex)),
        "put_gex": Decimal(str(-put_raw * scale)) if put_gex is None else Decimal(str(put_gex)),
        "net_gex": (
            Decimal(str((call_raw - put_raw) * scale)) if net_gex is None else Decimal(str(net_gex))
        ),
        "call_oi": call_oi,
        "put_oi": put_oi,
    }


# Bucket 1 (close 505): call wall 510 (200 > 50), put wall 495 (300 > 100).
# Bucket 2 (close 506): call wall 515 (900 > 20), put wall 500 (400 > 10).
# Deliberately DIFFERENT walls per bucket, so a fold that pooled every
# bucket's strikes into one wall computation would fail here.
_BUCKET1 = [
    _row(_TS1, 505.0, strike=495.0, call_raw=1.0, put_raw=300.0),
    _row(_TS1, 505.0, strike=500.0, call_raw=10.0, put_raw=100.0),
    _row(_TS1, 505.0, strike=510.0, call_raw=200.0, put_raw=5.0),
    _row(_TS1, 505.0, strike=515.0, call_raw=50.0, put_raw=1.0),
]
_BUCKET2 = [
    _row(_TS2, 506.0, strike=495.0, call_raw=2.0, put_raw=10.0),
    _row(_TS2, 506.0, strike=500.0, call_raw=3.0, put_raw=400.0),
    _row(_TS2, 506.0, strike=510.0, call_raw=20.0, put_raw=7.0),
    _row(_TS2, 506.0, strike=515.0, call_raw=900.0, put_raw=4.0),
]

_ROWS = _BUCKET1 + _BUCKET2


def _run(rows, symbol="SPY", timeframe="5min", window_units=40, expirations=None):
    db = DatabaseManager()
    conn = _FakeConn(rows)

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]
    result = asyncio.run(
        db.get_strike_profile_timeseries(symbol, timeframe, window_units, expirations)
    )
    return result, conn, db


def _walls_of(rows, close):
    """Expected walls straight from the canonical helper, same inputs the
    endpoint feeds it — pins the fold's wiring without re-deriving ranking."""
    return compute_call_put_walls(
        [
            {
                "strike": float(r["strike"]),
                "call_gamma": float(r["call_gamma_raw"]),
                "put_gamma": float(r["put_gamma_raw"]),
            }
            for r in rows
        ],
        close,
    )


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------


def test_flat_rows_group_into_buckets_in_query_order():
    result, _conn, _db = _run(_ROWS)

    assert [b["timestamp"] for b in result] == [_TS1, _TS2]
    assert [len(b["strikes"]) for b in result] == [4, 4]
    assert result[0]["symbol"] == "SPY"
    # Strike order within a bucket follows the query's strike ASC ordering.
    assert [float(s["strike"]) for s in result[0]["strikes"]] == [495.0, 500.0, 510.0, 515.0]


def test_strike_payload_uses_dollar_gex_under_request_names():
    result, _conn, _db = _run(_ROWS)

    row = result[0]["strikes"][0]
    assert set(row) == {"strike", "call_gamma", "put_gamma", "net_gamma", "call_oi", "put_oi"}
    # Values are the dollar-GEX columns, not the raw gamma used for walls.
    assert row["call_gamma"] == _BUCKET1[0]["call_gex"]
    assert row["put_gamma"] == _BUCKET1[0]["put_gex"]
    assert row["net_gamma"] == _BUCKET1[0]["net_gex"]
    assert isinstance(row["call_oi"], int)


def test_all_zero_strike_rows_are_dropped():
    """A strike with no gamma and no OI on either side renders nothing, so it
    is omitted rather than bloating a few-hundred-bucket payload."""
    rows = _BUCKET1 + [
        _row(_TS1, 505.0, strike=520.0, call_raw=0.0, put_raw=0.0, call_oi=0, put_oi=0)
    ]
    result, _conn, _db = _run(rows)

    assert [float(s["strike"]) for s in result[0]["strikes"]] == [495.0, 500.0, 510.0, 515.0]


def test_bucket_without_strikes_still_appears_with_empty_array():
    """LEFT JOIN miss: the bucket must keep its slot so the chart's rewind
    index stays 1:1 with the bucket grid."""
    rows = _BUCKET1 + [_row(_TS3, 507.0, strike=None)] + _BUCKET2
    result, _conn, _db = _run(rows)

    by_ts = {b["timestamp"]: b for b in result}
    assert set(by_ts) == {_TS1, _TS2, _TS3}
    assert by_ts[_TS3]["strikes"] == []
    assert by_ts[_TS3]["call_wall"] is None
    assert by_ts[_TS3]["put_wall"] is None


# ---------------------------------------------------------------------------
# Per-bucket walls
# ---------------------------------------------------------------------------


def test_walls_are_computed_per_bucket_not_pooled():
    """Each bucket's walls come from ITS OWN strikes against ITS OWN close."""
    result, _conn, _db = _run(_ROWS)

    cw1, pw1 = _walls_of(_BUCKET1, 505.0)
    cw2, pw2 = _walls_of(_BUCKET2, 506.0)

    # Guard the fixture itself: the two buckets must disagree, otherwise this
    # test could not detect strikes leaking between buckets.
    assert (cw1, pw1) != (cw2, pw2)
    assert (cw1, pw1) == (510.0, 495.0)
    assert (cw2, pw2) == (515.0, 500.0)

    assert (result[0]["call_wall"], result[0]["put_wall"]) == (cw1, pw1)
    assert (result[1]["call_wall"], result[1]["put_wall"]) == (cw2, pw2)


def test_bucket_without_close_leaves_walls_none():
    """No underlying tape for the bucket -> no spot to split strikes on."""
    rows = [
        _row(_TS1, None, strike=495.0, call_raw=1.0, put_raw=300.0),
        _row(_TS1, None, strike=510.0, call_raw=200.0, put_raw=5.0),
    ]
    result, _conn, _db = _run(rows)

    assert len(result) == 1
    assert result[0]["close"] is None
    assert result[0]["call_wall"] is None
    assert result[0]["put_wall"] is None
    # The strikes themselves still ship.
    assert len(result[0]["strikes"]) == 2


# ---------------------------------------------------------------------------
# Gamma flip scope
# ---------------------------------------------------------------------------


def test_gamma_flip_is_persisted_value_when_unfiltered():
    """expirations=None keeps the canonical spot-shift flip from gex_summary,
    so the chart's "All" view stays in parity with /api/gex/summary."""
    result, _conn, _db = _run(_ROWS, expirations=None)

    assert result[0]["gamma_flip"] == Decimal("505.5")
    assert result[1]["gamma_flip"] == Decimal("505.5")


def test_gamma_flip_is_recomputed_for_a_filtered_expiration_set():
    """A subset can't reuse the aggregate spot-shift flip, so it is rebuilt
    per bucket from the same summed-by-strike gamma the bars render."""
    from src.analytics.walls import compute_gamma_flip_from_strikes

    result, _conn, _db = _run(_ROWS, expirations=[date(2026, 8, 17)])

    expected = [
        compute_gamma_flip_from_strikes(
            [
                {
                    "strike": float(r["strike"]),
                    "call_gamma": float(r["call_gamma_raw"]),
                    "put_gamma": float(r["put_gamma_raw"]),
                }
                for r in bucket_rows
            ],
            close,
        )
        for bucket_rows, close in ((_BUCKET1, 505.0), (_BUCKET2, 506.0))
    ]

    assert [b["gamma_flip"] for b in result] == expected
    # And it genuinely differs from the persisted value it replaced.
    assert result[0]["gamma_flip"] != Decimal("505.5")


def test_filtered_bucket_without_strikes_gets_null_flip():
    rows = [_row(_TS3, 507.0, strike=None)]
    result, _conn, _db = _run(rows, expirations=[date(2026, 8, 17)])

    assert result[0]["gamma_flip"] is None


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


def test_second_identical_call_is_served_from_cache():
    db = DatabaseManager()
    db._strike_profile_timeseries_cache_ttl_seconds = 60.0
    conn = _FakeConn(_ROWS)

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    first = asyncio.run(db.get_strike_profile_timeseries("SPY", "5min", 40, None))
    second = asyncio.run(db.get_strike_profile_timeseries("spy", "5min", 40, None))

    assert conn.calls == 1
    assert first == second


def test_distinct_expiration_sets_do_not_share_a_cache_entry():
    db = DatabaseManager()
    db._strike_profile_timeseries_cache_ttl_seconds = 60.0
    conn = _FakeConn(_ROWS)

    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]

    asyncio.run(db.get_strike_profile_timeseries("SPY", "5min", 40, [date(2026, 8, 17)]))
    asyncio.run(db.get_strike_profile_timeseries("SPY", "5min", 40, [date(2026, 8, 18)]))

    assert conn.calls == 2
