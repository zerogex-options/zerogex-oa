"""Durable usage metering: in-memory aggregation, flush UPSERT, retry-on-
failure merge-back, and the disabled fast path.

``API_USAGE_METERING_ENABLED`` is read at import time in ``src.api.usage``,
so tests that need metering on flush ``src.api.usage`` from ``sys.modules``
and re-import (mirrors the other ``test_api_*`` reload suites). Async
methods are driven with ``asyncio.run`` so the suite needs no
pytest-asyncio.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pytest

# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


@dataclass
class _Identity:
    caller_user_id: Optional[str] = None
    caller_key_id: Optional[int] = None
    end_user_id: Optional[str] = None


class _FakeConn:
    def __init__(self, fail: bool = False) -> None:
        self.calls: list = []
        self._fail = fail

    async def executemany(self, sql: str, rows) -> None:
        if self._fail:
            raise RuntimeError("simulated DB failure")
        self.calls.append((sql, list(rows)))


class _Acquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc) -> bool:
        return False


class _FakePool:
    def __init__(self, fail: bool = False) -> None:
        self.conn = _FakeConn(fail=fail)

    def acquire(self) -> _Acquire:
        return _Acquire(self.conn)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _reload_usage(monkeypatch: pytest.MonkeyPatch, *, enabled: bool):
    monkeypatch.delenv("API_USAGE_METERING_ENABLED", raising=False)
    if enabled:
        monkeypatch.setenv("API_USAGE_METERING_ENABLED", "1")
    sys.modules.pop("src.api.usage", None)
    return importlib.import_module("src.api.usage")


# --------------------------------------------------------------------------
# Disabled fast path
# --------------------------------------------------------------------------


def test_record_is_noop_when_disabled(monkeypatch: pytest.MonkeyPatch):
    usage = _reload_usage(monkeypatch, enabled=False)
    meter = usage.UsageMeter()
    meter.record(_Identity(caller_user_id="acct-1", caller_key_id=7), 200)
    assert meter._counts == {}


def test_start_launches_no_task_when_disabled(monkeypatch: pytest.MonkeyPatch):
    usage = _reload_usage(monkeypatch, enabled=False)
    meter = usage.UsageMeter()

    async def _drive():
        meter.start()
        return meter._task

    assert asyncio.run(_drive()) is None


# --------------------------------------------------------------------------
# In-memory aggregation
# --------------------------------------------------------------------------


def test_record_aggregates_by_bucket(monkeypatch: pytest.MonkeyPatch):
    usage = _reload_usage(monkeypatch, enabled=True)
    meter = usage.UsageMeter()
    today = datetime.now(timezone.utc).date()

    cust = _Identity(caller_user_id="acct-1", caller_key_id=7, end_user_id="seat-9")
    meter.record(cust, 200)
    meter.record(cust, 200)
    meter.record(cust, 503)  # 5xx → also counts as an error

    other = _Identity(caller_user_id="acct-2", caller_key_id=3)
    meter.record(other, 200)

    assert meter._counts[(today, "acct-1", 7, "seat-9")] == [3, 1]
    # No end-user token → sentinel '-'; no error.
    assert meter._counts[(today, "acct-2", 3, "-")] == [1, 0]


def test_anonymous_identity_uses_sentinels(monkeypatch: pytest.MonkeyPatch):
    usage = _reload_usage(monkeypatch, enabled=True)
    meter = usage.UsageMeter()
    today = datetime.now(timezone.utc).date()

    meter.record(None, 200)  # no identity at all
    assert meter._counts[(today, "-", 0, "-")] == [1, 0]


# --------------------------------------------------------------------------
# Flush
# --------------------------------------------------------------------------


def test_flush_upserts_and_clears(monkeypatch: pytest.MonkeyPatch):
    usage = _reload_usage(monkeypatch, enabled=True)
    meter = usage.UsageMeter()
    today = datetime.now(timezone.utc).date()

    pool = _FakePool()
    meter.configure(lambda: pool)

    meter.record(_Identity(caller_user_id="acct-1", caller_key_id=7), 200)
    meter.record(_Identity(caller_user_id="acct-1", caller_key_id=7), 500)

    written = asyncio.run(meter.flush())
    assert written == 1
    assert meter._counts == {}  # drained

    assert len(pool.conn.calls) == 1
    _sql, rows = pool.conn.calls[0]
    # (day, caller_user_id, caller_key_id, end_user_id, request_count, error_count)
    assert rows == [(today, "acct-1", 7, "-", 2, 1)]


def test_flush_without_pool_is_noop(monkeypatch: pytest.MonkeyPatch):
    usage = _reload_usage(monkeypatch, enabled=True)
    meter = usage.UsageMeter()
    meter.record(_Identity(caller_user_id="acct-1", caller_key_id=1), 200)
    # No pool configured → nothing flushed, counts retained for later.
    assert asyncio.run(meter.flush()) == 0
    assert len(meter._counts) == 1


def test_flush_failure_merges_back(monkeypatch: pytest.MonkeyPatch):
    usage = _reload_usage(monkeypatch, enabled=True)
    meter = usage.UsageMeter()
    today = datetime.now(timezone.utc).date()

    pool = _FakePool(fail=True)
    meter.configure(lambda: pool)
    meter.record(_Identity(caller_user_id="acct-1", caller_key_id=2), 200)

    written = asyncio.run(meter.flush())
    assert written == 0
    # Counts retried, not lost.
    assert meter._counts[(today, "acct-1", 2, "-")] == [1, 0]


def test_merge_back_sums_with_concurrent_records(monkeypatch: pytest.MonkeyPatch):
    """A record() landing after the swap but before merge-back must not be
    clobbered — re-add is additive."""
    usage = _reload_usage(monkeypatch, enabled=True)
    meter = usage.UsageMeter()
    today = datetime.now(timezone.utc).date()
    bucket = (today, "acct-1", 2, "-")

    snapshot = {bucket: [1, 0]}
    meter._counts[bucket] = [5, 0]  # "concurrent" requests since the swap
    meter._merge_back(snapshot)
    assert meter._counts[bucket] == [6, 0]


def test_stop_flushes_final_window(monkeypatch: pytest.MonkeyPatch):
    usage = _reload_usage(monkeypatch, enabled=True)
    meter = usage.UsageMeter()
    pool = _FakePool()

    async def _drive():
        meter.configure(lambda: pool)
        meter.start()  # launches the loop
        meter.record(_Identity(caller_user_id="acct-1", caller_key_id=1), 200)
        await meter.stop()  # cancels loop + final flush

    asyncio.run(_drive())
    assert meter._counts == {}
    assert len(pool.conn.calls) == 1
    _sql, rows = pool.conn.calls[0]
    assert rows[0][:4] == (rows[0][0], "acct-1", 1, "-")
    assert rows[0][4] == 1  # request_count
