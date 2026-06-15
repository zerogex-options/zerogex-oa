"""TradeStation rate-limit governor + strikes cache regression tests.

The governor caps combined API call volume across ingestion processes in
each 5-min UTC window via the existing ``tradestation_api_calls`` table.
The strikes cache short-circuits repeated /strikes calls for the same
(underlying, expiration) within a TTL.  Both were added in response to
sustained 429s when call volume saturated TradeStation's per-window cap
during pre-market.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List
from unittest.mock import patch

from src.ingestion.tradestation_client import TradeStationClient


class _Resp:
    def __init__(self, status_code=200, content=b"{}", payload=None):
        self.status_code = status_code
        self.content = content
        self.text = content.decode() if isinstance(content, bytes) else str(content)
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"HTTP {self.status_code}")


def _bare_client() -> TradeStationClient:
    """Bypass __init__ (which needs creds + auth network)."""
    from threading import Lock

    c = TradeStationClient.__new__(TradeStationClient)
    c.base_url = "https://api.tradestation.com/v3"
    c._stream_lock = Lock()
    c._stream_state = {}
    c._api_session_counter_lock = Lock()
    c._api_session_window_start = TradeStationClient._floor_to_five_minute_window(
        datetime.now(timezone.utc)
    )
    c._api_session_window_count = 0
    c._api_session_window_persisted = 0
    c._api_call_window_writer = None
    c._api_call_window_reader = None
    c._rate_limit_persisted_count = 0
    c._rate_limit_persisted_last_sync_mono = 0.0
    c._strikes_cache = {}
    c._strikes_cache_lock = Lock()
    return c


# --- Rate-limit governor ----------------------------------------------------


def test_gate_no_op_when_cap_zero():
    c = _bare_client()
    c._api_call_window_reader = lambda _w: 10_000  # well past any real cap
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_PER_5MIN", 0), patch(
        "src.ingestion.tradestation_client.time.sleep"
    ) as sleep_mock:
        c._gate_for_rate_limit()
    sleep_mock.assert_not_called()


def test_gate_no_op_when_no_reader_wired():
    c = _bare_client()
    # Reader stays None -> governor disabled even if cap > 0.
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_PER_5MIN", 100), patch(
        "src.ingestion.tradestation_client.time.sleep"
    ) as sleep_mock:
        # Local count under cap; reader missing means we can't sync persisted.
        c._api_session_window_count = 50
        c._gate_for_rate_limit()
    sleep_mock.assert_not_called()


def test_gate_no_op_under_cap():
    c = _bare_client()
    c._api_call_window_reader = lambda _w: 100  # persisted across other procs
    c._api_session_window_count = 50  # local in-flight
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_PER_5MIN", 900), patch(
        "src.ingestion.tradestation_client.time.sleep"
    ) as sleep_mock:
        c._gate_for_rate_limit()
    sleep_mock.assert_not_called()
    # Reader was consulted -> persisted-count cache updated.
    assert c._rate_limit_persisted_count == 100


def test_gate_sleeps_to_next_window_when_at_cap():
    c = _bare_client()
    # Cap = 100. Persisted across other processes = 90, local = 15 -> 105 >= 100.
    c._api_call_window_reader = lambda _w: 90
    c._api_session_window_count = 15
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_PER_5MIN", 100), patch(
        "src.ingestion.tradestation_client.TS_RATE_LIMIT_SYNC_INTERVAL", 5
    ), patch("src.ingestion.tradestation_client.time.sleep") as sleep_mock:
        c._gate_for_rate_limit()
    assert sleep_mock.call_count == 1
    sleep_seconds = sleep_mock.call_args[0][0]
    # Window length is 5min, so sleep is in (0, 300] seconds.
    assert 0 < sleep_seconds <= 300 + 1


def test_gate_flushes_partial_count_on_sync_interval():
    c = _bare_client()
    persisted_writes = []
    c._api_call_window_writer = lambda w, n: persisted_writes.append((w, n))
    c._api_call_window_reader = lambda _w: 0
    c._api_session_window_count = 42  # in-flight, never flushed
    # sync_last_mono = 0 means stale -> partial flush fires.
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_PER_5MIN", 900), patch(
        "src.ingestion.tradestation_client.TS_RATE_LIMIT_SYNC_INTERVAL", 5
    ):
        c._gate_for_rate_limit()
    # The 42 in-flight calls should have been persisted (delta = 42 - 0).
    assert len(persisted_writes) == 1
    assert persisted_writes[0][1] == 42
    # Local persisted-marker advances so we don't double-flush next time.
    assert c._api_session_window_persisted == 42


def test_gate_avoids_double_counting_own_contribution():
    """Persisted count already includes this process's flushed partial.

    When the reader returns persisted that INCLUDES this process's earlier
    flush, we must subtract _api_session_window_persisted to avoid
    double-counting our own calls into the cap check.
    """
    c = _bare_client()
    c._api_session_window_count = 50  # 50 local in-flight
    c._api_session_window_persisted = 50  # already flushed all 50
    c._api_call_window_reader = lambda _w: 50  # reader returns OUR flushed 50
    # Cap = 100. Naive sum would be persisted(50) + local(50) = 100 (block).
    # With correct subtraction: others(50-50=0) + local(50) = 50 (allow).
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_PER_5MIN", 100), patch(
        "src.ingestion.tradestation_client.time.sleep"
    ) as sleep_mock:
        c._gate_for_rate_limit()
    sleep_mock.assert_not_called()


def test_gate_reader_exception_falls_back_to_local():
    c = _bare_client()

    def _boom(_w):
        raise RuntimeError("DB down")

    c._api_call_window_reader = _boom
    c._api_session_window_count = 50
    # Cap = 1000, local = 50 -> should not block even though reader threw.
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_PER_5MIN", 1000), patch(
        "src.ingestion.tradestation_client.time.sleep"
    ) as sleep_mock:
        c._gate_for_rate_limit()
    sleep_mock.assert_not_called()


# --- Strikes cache ----------------------------------------------------------


def _stub_request_returning_strikes(c: TradeStationClient, strikes: List[float]) -> List[int]:
    """Make `_request` count calls and return a strikes payload."""
    calls = [0]

    def _r(method, endpoint, params=None, data=None):
        calls[0] += 1
        return {"Strikes": [[s] for s in strikes]}

    c._request = _r  # type: ignore[assignment]
    return calls


def test_strikes_cache_hit_skips_api_call():
    c = _bare_client()
    calls = _stub_request_returning_strikes(c, [100.0, 105.0, 110.0])
    with patch("src.ingestion.tradestation_client.TS_STRIKES_CACHE_TTL", 3600):
        first = c.get_option_strikes("SPY", expiration="06-29-2026")
        second = c.get_option_strikes("SPY", expiration="06-29-2026")
    assert first == [100.0, 105.0, 110.0]
    assert second == [100.0, 105.0, 110.0]
    assert calls[0] == 1  # second call served from cache


def test_strikes_cache_miss_on_different_expiration():
    c = _bare_client()
    calls = _stub_request_returning_strikes(c, [100.0, 105.0])
    with patch("src.ingestion.tradestation_client.TS_STRIKES_CACHE_TTL", 3600):
        c.get_option_strikes("SPY", expiration="06-29-2026")
        c.get_option_strikes("SPY", expiration="07-05-2026")
    assert calls[0] == 2  # different expirations -> two upstream calls


def test_strikes_cache_disabled_with_ttl_zero():
    c = _bare_client()
    calls = _stub_request_returning_strikes(c, [100.0])
    with patch("src.ingestion.tradestation_client.TS_STRIKES_CACHE_TTL", 0):
        c.get_option_strikes("SPY", expiration="06-29-2026")
        c.get_option_strikes("SPY", expiration="06-29-2026")
    assert calls[0] == 2  # disabled cache -> two upstream calls


def test_strikes_cache_does_not_store_empty_result():
    """A transient 429 / empty result must not poison the cache."""
    c = _bare_client()

    payloads = [{"Strikes": []}, {"Strikes": [[100.0]]}]
    idx = [0]

    def _r(method, endpoint, params=None, data=None):
        p = payloads[idx[0]]
        idx[0] += 1
        return p

    c._request = _r  # type: ignore[assignment]

    with patch("src.ingestion.tradestation_client.TS_STRIKES_CACHE_TTL", 3600):
        first = c.get_option_strikes("SPY", expiration="06-29-2026")
        second = c.get_option_strikes("SPY", expiration="06-29-2026")

    assert first == []
    assert second == [100.0]  # second call hit upstream because empty wasn't cached


def test_strikes_cache_expires_after_ttl():
    c = _bare_client()
    calls = _stub_request_returning_strikes(c, [100.0])

    fake_now = [1000.0]

    def fake_monotonic():
        return fake_now[0]

    with patch("src.ingestion.tradestation_client.TS_STRIKES_CACHE_TTL", 60), patch(
        "src.ingestion.tradestation_client.time.monotonic", side_effect=fake_monotonic
    ):
        c.get_option_strikes("SPY", expiration="06-29-2026")
        fake_now[0] += 61  # past TTL
        c.get_option_strikes("SPY", expiration="06-29-2026")
    assert calls[0] == 2


def test_invalidate_strikes_cache_targeted():
    c = _bare_client()
    calls = _stub_request_returning_strikes(c, [100.0])

    with patch("src.ingestion.tradestation_client.TS_STRIKES_CACHE_TTL", 3600):
        c.get_option_strikes("SPY", expiration="06-29-2026")
        c.get_option_strikes("QQQ", expiration="06-29-2026")
        c.invalidate_strikes_cache("SPY")
        c.get_option_strikes("SPY", expiration="06-29-2026")  # forced refetch
        c.get_option_strikes("QQQ", expiration="06-29-2026")  # still cached

    assert calls[0] == 3
