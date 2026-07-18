"""The live ingester de-phantoms a cash-index 09:30 ET open bar once it's done.

``_store_underlying`` calls ``_repair_session_open_if_needed`` after each write;
for a cash index, the moment we're on a bar past the session open it rewrites the
stale opening-rotation open/extreme in place (via
``cash_index_open_repair.repair_one_session_open``). These are unit tests with a
fake DB — no live Postgres.
"""

from __future__ import annotations

import contextlib
from datetime import date, datetime, timezone

import src.ingestion.main_engine as me
from src.ingestion.main_engine import IngestionEngine

# 2026-07-17 is a Friday (EDT): 09:30 ET == 13:30 UTC, 09:31 ET == 13:31 UTC.
_OPEN_TS = datetime(2026, 7, 17, 13, 30, tzinfo=timezone.utc)
_NEXT_BAR = datetime(2026, 7, 17, 13, 31, tzinfo=timezone.utc)
_SESSION_DATE = date(2026, 7, 17)


class _Cur:
    def __init__(self, rowcount=1):
        self.rowcount = rowcount
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))


class _Conn:
    def __init__(self, cur):
        self._cur = cur
        self.commits = 0

    def cursor(self):
        return self._cur

    def commit(self):
        self.commits += 1


def _patch_conn(monkeypatch, cur, *, raise_exc=None):
    @contextlib.contextmanager
    def _cm():
        if raise_exc is not None:
            raise raise_exc
        yield _Conn(cur)

    monkeypatch.setattr(me, "db_connection", _cm)


def _engine(symbol="SPX"):
    e = IngestionEngine.__new__(IngestionEngine)
    e.db_symbol = symbol
    e._db_backoff_until = 0.0
    e._session_open_repaired = None
    return e


def test_repairs_open_bar_on_first_post_open_bar(monkeypatch):
    cur = _Cur(rowcount=1)
    _patch_conn(monkeypatch, cur)
    e = _engine("SPX")

    e._repair_session_open_if_needed(_NEXT_BAR)

    # One UPDATE, scoped to the 09:30 open bar, and the session marked done.
    assert len(cur.calls) == 1
    sql, params = cur.calls[0]
    assert "UPDATE underlying_quotes" in sql
    assert params == {"symbol": "SPX", "ts": _OPEN_TS}
    assert e._session_open_repaired == _SESSION_DATE


def test_noop_on_the_open_bar_itself(monkeypatch):
    cur = _Cur()
    _patch_conn(monkeypatch, cur)
    e = _engine("SPX")

    # The open bar isn't finished until we're on a later bucket.
    e._repair_session_open_if_needed(_OPEN_TS)

    assert cur.calls == []
    assert e._session_open_repaired is None


def test_runs_once_per_session(monkeypatch):
    cur = _Cur()
    _patch_conn(monkeypatch, cur)
    e = _engine("SPX")
    e._session_open_repaired = _SESSION_DATE  # already handled today

    e._repair_session_open_if_needed(_NEXT_BAR)

    assert cur.calls == []


def test_skips_during_db_backoff(monkeypatch):
    cur = _Cur()
    _patch_conn(monkeypatch, cur)
    e = _engine("SPX")
    e._db_backoff_until = me._time.monotonic() + 999.0

    e._repair_session_open_if_needed(_NEXT_BAR)

    assert cur.calls == []
    assert e._session_open_repaired is None


def test_swallows_db_error_and_leaves_flag_unset(monkeypatch):
    _patch_conn(monkeypatch, _Cur(), raise_exc=RuntimeError("db down"))
    e = _engine("SPX")

    # Must not raise; must not mark the session done, so the next bar retries.
    e._repair_session_open_if_needed(_NEXT_BAR)

    assert e._session_open_repaired is None


def test_store_underlying_only_repairs_cash_indexes(monkeypatch):
    # The is_cash_index gate lives in _store_underlying — SPY must not trigger it.
    calls = []
    for symbol, expected in (("SPX", 1), ("SPY", 0)):
        e = _engine(symbol)
        e._last_underlying_signature = None
        e.latest_underlying_price = None
        e.latest_underlying_timestamp = None
        e.underlying_bars_stored = 0
        monkeypatch.setattr(e, "_log_parity_signature", lambda *a, **k: None)
        monkeypatch.setattr(e, "_upsert_underlying_quote", lambda payload: None)
        seen = []
        monkeypatch.setattr(e, "_repair_session_open_if_needed", lambda bucket: seen.append(bucket))
        e._store_underlying(
            {
                "timestamp": _NEXT_BAR,
                "open": 7447.0,
                "high": 7448.0,
                "low": 7445.0,
                "close": 7447.0,
                "up_volume": 1,
                "down_volume": 1,
            }
        )
        calls.append(len(seen))
        assert len(seen) == expected, symbol
    assert calls == [1, 0]
