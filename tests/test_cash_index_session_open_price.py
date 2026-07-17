"""Cash-index session-open price correction.

A cash index (SPX, NDX, RUT, DJX) has no transactional tape of its own, so
TradeStation carries the prior session's close forward and stamps the 09:30 ET
open bar's ``Open`` with that prior close until the constituents actually open.
Rendered as a candle, that paints a phantom full-range bar from the prior close
down to the real level instead of the true opening gap.

The fix uses the first genuine print of the session as the open for that one
bar. On the live stream that is the first streamed ``close`` of the 09:30 ET
bucket; on a completed backfilled bar the true first print is unrecoverable, so
the bar's own ``close`` (the level at the end of the opening minute) is the
closest honest proxy. Only the 09:30 ET open bar of a cash index is touched —
every other bar, and every non-index symbol (ETFs trade pre-market and have a
real open), is left untouched.

Pure-logic tests with fakes — no live TradeStation or Postgres. The ET
boundaries below are exercised on both an EDT day (2026-05-15, UTC-4) and an EST
day (2026-01-15, UTC-5) so the DST-correct 09:30 anchor is pinned.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.ingestion.main_engine import IngestionEngine
from src.tools.underlying_backfill import _bar_to_row, upsert_bars
from src.validation import is_cash_session_open_bucket


def _utc(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


# ===========================================================================
# is_cash_session_open_bucket — the 09:30 ET anchor
# ===========================================================================
def test_session_open_bucket_true_only_at_0930_et():
    # 2026-05-15 is a Friday (EDT, UTC-4): 09:30 ET == 13:30 UTC.
    assert is_cash_session_open_bucket(_utc(2026, 5, 15, 13, 30)) is True
    # One minute later is no longer the open bar.
    assert is_cash_session_open_bucket(_utc(2026, 5, 15, 13, 31)) is False
    # Mid-session.
    assert is_cash_session_open_bucket(_utc(2026, 5, 15, 18, 0)) is False
    # The prior 09:30 UTC (05:30 ET) is not the cash open.
    assert is_cash_session_open_bucket(_utc(2026, 5, 15, 9, 30)) is False


def test_session_open_bucket_is_dst_correct():
    # 2026-01-15 is a Thursday (EST, UTC-5): 09:30 ET == 14:30 UTC.
    assert is_cash_session_open_bucket(_utc(2026, 1, 15, 14, 30)) is True
    # The EDT offset (13:30 UTC) is NOT the open on an EST day.
    assert is_cash_session_open_bucket(_utc(2026, 1, 15, 13, 30)) is False


def test_session_open_bucket_naive_is_treated_as_utc():
    assert is_cash_session_open_bucket(datetime(2026, 5, 15, 13, 30)) is True
    assert is_cash_session_open_bucket(datetime(2026, 5, 15, 13, 31)) is False


def test_session_open_bucket_false_on_non_trading_day():
    # 2026-05-16 is a Saturday: cash_session_date rolls back to Friday, so a
    # Saturday 09:30 ET instant is not any session's open.
    assert is_cash_session_open_bucket(_utc(2026, 5, 16, 13, 30)) is False


# ===========================================================================
# Live ingestion path — _store_underlying
# ===========================================================================
def _underlying_engine(db_symbol: str) -> IngestionEngine:
    e = IngestionEngine.__new__(IngestionEngine)
    e.db_symbol = db_symbol
    e._db_backoff_until = 0.0
    e._db_consecutive_failures = 0
    e.errors_count = 0
    e.underlying_bars_stored = 0
    e.last_flush_time = None
    e._last_underlying_signature = None
    e.latest_underlying_price = None
    e.latest_underlying_timestamp = None
    return e


def _capture_store(monkeypatch, e, data):
    monkeypatch.setattr(e, "_log_parity_signature", lambda *a, **k: None)
    captured: dict = {}
    monkeypatch.setattr(e, "_upsert_underlying_quote", lambda payload: captured.update(payload))
    e._store_underlying(data)
    return captured


# The 09:30 ET open bar is close-stamped 09:31 ET == 13:31 UTC on this EDT day;
# _store_underlying floors (ts - 1s) to the bar's own 09:30 minute (13:30 UTC).
_OPEN_BAR = {
    "timestamp": _utc(2026, 5, 15, 13, 31),
    "open": 5500.0,  # phantom: TradeStation's carried-forward prior close
    "high": 5455.0,
    "low": 5440.0,
    "close": 5450.0,  # first genuine print of the session
    "up_volume": 10,
    "down_volume": 5,
}


def test_store_underlying_cash_index_open_uses_first_print(monkeypatch):
    e = _underlying_engine("SPX")
    captured = _capture_store(monkeypatch, e, dict(_OPEN_BAR))

    # Stored on the bar's own 09:30 minute...
    assert captured["timestamp"] == _utc(2026, 5, 15, 13, 30)
    # ...with the phantom prior-close open replaced by the first print (close).
    assert captured["open"] == 5450.0
    # High / low / close are untouched — only the open was corrected.
    assert captured["high"] == 5455.0
    assert captured["low"] == 5440.0
    assert captured["close"] == 5450.0


def test_store_underlying_cash_index_non_open_bar_keeps_open(monkeypatch):
    e = _underlying_engine("SPX")
    # 14:38 UTC close-stamp -> 14:37 UTC bucket (10:37 ET), a normal mid-session
    # bar whose TradeStation open is real and must be preserved.
    data = dict(_OPEN_BAR, timestamp=_utc(2026, 5, 15, 14, 38))
    captured = _capture_store(monkeypatch, e, data)

    assert captured["timestamp"] == _utc(2026, 5, 15, 14, 37)
    assert captured["open"] == 5500.0  # unchanged


def test_store_underlying_non_index_open_bar_keeps_open(monkeypatch):
    # SPY (an ETF) trades pre-market and has a genuine 09:30 open — the
    # correction must NOT apply to it.
    e = _underlying_engine("SPY")
    captured = _capture_store(monkeypatch, e, dict(_OPEN_BAR))

    assert captured["timestamp"] == _utc(2026, 5, 15, 13, 30)
    assert captured["open"] == 5500.0  # unchanged


# ===========================================================================
# Backfill path — upsert_bars
# ===========================================================================
class _FakeCursor:
    def __init__(self):
        self.rows = None

    def executemany(self, sql, seq):
        self.sql = sql
        self.rows = list(seq)


class _FakeConn:
    def __init__(self):
        self._cur = _FakeCursor()

    def cursor(self):
        return self._cur


def _bar(ts_z: str, open_="5500.0", close="5450.0"):
    return {
        "TimeStamp": ts_z,
        "Open": open_,
        "High": "5455.0",
        "Low": "5440.0",
        "Close": close,
        "UpVolume": "10",
        "DownVolume": "5",
    }


def _written_open(conn):
    ((_, _ts, open_, *_rest),) = conn._cur.rows
    return open_


def test_upsert_bars_cash_index_open_bar_uses_close_proxy():
    conn = _FakeConn()
    # 09:30 ET on the EDT day == 13:30 UTC; the backfill stores the raw stamp.
    row = _bar_to_row(_bar("2026-05-15T13:30:00Z"))
    assert row is not None
    upsert_bars(conn, "SPX", [row])
    # Phantom prior-close open (5500) replaced by the completed bar's close.
    assert _written_open(conn) == 5450.0


def test_upsert_bars_cash_index_non_open_bar_keeps_open():
    conn = _FakeConn()
    row = _bar_to_row(_bar("2026-05-15T13:31:00Z"))  # 09:31 ET, not the open
    upsert_bars(conn, "SPX", [row])
    assert _written_open(conn) == 5500.0


def test_upsert_bars_non_index_open_bar_keeps_open():
    conn = _FakeConn()
    row = _bar_to_row(_bar("2026-05-15T13:30:00Z"))
    upsert_bars(conn, "SPY", [row])  # ETF: real open, no correction
    assert _written_open(conn) == 5500.0
