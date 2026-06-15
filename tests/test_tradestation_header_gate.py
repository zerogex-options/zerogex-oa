"""Header-driven rate-limit gate regression tests.

TradeStation publishes per-resource quota state on every response via
``X-RateLimit-*`` headers (and concurrency state via ``X-Concurrency-*``).
This module verifies that:

* The client parses those headers correctly into per-resource state.
* The pre-request gate consults the parsed state and sleeps to reset when
  remaining is at/below ``TS_RATE_LIMIT_HEADER_MIN_REMAINING``.
* 429 retry delay prefers ``X-RateLimit-Reset`` over blind exponential backoff.
* Missing/malformed headers don't poison the cache or crash the client.
* Stale observations are ignored (we don't gate on data older than
  ``TS_RATE_LIMIT_HEADER_STALE_SECONDS``).
* The header gate is purely ADDITIVE: it never trumps an enabled static
  cap, it just front-runs it when its observation is fresher.
"""

from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Optional
from unittest.mock import patch

from requests.structures import CaseInsensitiveDict

from src.ingestion.tradestation_client import TradeStationClient


class _Resp:
    def __init__(self, status_code: int = 200, headers: Optional[dict] = None,
                 content: bytes = b"{}", payload=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.content = content
        self.text = content.decode() if isinstance(content, bytes) else str(content)
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"HTTP {self.status_code}")


def _bare_client() -> TradeStationClient:
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
    c._resource_rate_limit_state = {}
    c._endpoint_to_resource = {}
    c._resource_rate_limit_lock = Lock()
    c._concurrency_state = {}
    return c


# --- Endpoint normalization -------------------------------------------------


def test_endpoint_pattern_strips_dynamic_segments():
    pat = TradeStationClient._endpoint_pattern
    assert pat("marketdata/options/strikes/SPY") == "marketdata/options/strikes"
    assert pat("marketdata/options/strikes/SPY?expiration=06-29-2026") == (
        "marketdata/options/strikes"
    )
    assert pat("marketdata/quotes/SPY,QQQ") == "marketdata/quotes"
    assert pat("marketdata/options/expirations/SPY") == "marketdata/options/expirations"


def test_endpoint_pattern_distinguishes_stream_from_snapshot():
    pat = TradeStationClient._endpoint_pattern
    # Stream endpoints have a distinct rate-limit resource on TradeStation
    # (e.g. "streaming-quotes" vs "quotes"), so the pattern must keep the
    # ``stream`` segment so the two resources never collide in our cache.
    assert pat("marketdata/stream/quotes/SPY,QQQ") == "marketdata/stream/quotes"
    assert pat("marketdata/stream/barcharts/SPY") == "marketdata/stream/barcharts"
    assert pat("marketdata/quotes/SPY") == "marketdata/quotes"
    assert pat("marketdata/stream/quotes/SPY,QQQ") != pat("marketdata/quotes/SPY,QQQ")


def test_endpoint_pattern_handles_short_paths():
    pat = TradeStationClient._endpoint_pattern
    # Edge cases: 1- or 2-segment paths have no symbol to strip.
    assert pat("accounts") == "accounts"
    assert pat("brokerage/accounts") == "brokerage/accounts"


# --- Header parsing ---------------------------------------------------------


def test_record_rate_limit_headers_caches_state():
    c = _bare_client()
    resp = _Resp(
        headers={
            "X-RateLimit-Limit": "500",
            "X-RateLimit-Period": "300",
            "X-RateLimit-Remaining": "423",
            "X-RateLimit-Reset": "287",
            "X-RateLimit-Resource": "quotes",
        }
    )
    c._record_rate_limit_headers(resp, "marketdata/quotes/SPY")
    state = c._resource_rate_limit_state["quotes"]
    assert state["limit"] == 500
    assert state["period_seconds"] == 300
    assert state["remaining"] == 423
    assert state["resource"] == "quotes"
    # Endpoint -> resource mapping learned.
    assert c._endpoint_to_resource["marketdata/quotes"] == "quotes"


def test_record_rate_limit_headers_tolerates_missing_fields():
    """A response missing one of the X-RateLimit-* headers must not poison cache."""
    c = _bare_client()
    resp = _Resp(
        headers={
            "X-RateLimit-Limit": "500",
            # No Period / Remaining / Reset
            "X-RateLimit-Resource": "quotes",
        }
    )
    c._record_rate_limit_headers(resp, "marketdata/quotes/SPY")
    # No state should be written; the gate stays a no-op for this resource.
    assert "quotes" not in c._resource_rate_limit_state


def test_record_rate_limit_headers_tolerates_malformed_values():
    c = _bare_client()
    resp = _Resp(
        headers={
            "X-RateLimit-Limit": "five-hundred",  # ValueError on int()
            "X-RateLimit-Period": "300",
            "X-RateLimit-Remaining": "423",
            "X-RateLimit-Reset": "287",
            "X-RateLimit-Resource": "quotes",
        }
    )
    c._record_rate_limit_headers(resp, "marketdata/quotes/SPY")
    assert "quotes" not in c._resource_rate_limit_state


def test_record_rate_limit_headers_clamps_negative_remaining():
    c = _bare_client()
    resp = _Resp(
        headers={
            "X-RateLimit-Limit": "500",
            "X-RateLimit-Period": "300",
            "X-RateLimit-Remaining": "-5",  # Server bug -- treat as 0
            "X-RateLimit-Reset": "287",
            "X-RateLimit-Resource": "quotes",
        }
    )
    c._record_rate_limit_headers(resp, "marketdata/quotes/SPY")
    assert c._resource_rate_limit_state["quotes"]["remaining"] == 0


def test_record_rate_limit_headers_handles_lowercase():
    """TradeStation example responses use lowercase header names.

    ``requests.Response.headers`` is a ``CaseInsensitiveDict`` in production
    so our parser doesn't need a case-insensitive lookup itself -- it
    inherits the behaviour from the response object.  This test pins that
    contract: we use the real header container so a future regression
    that breaks the inheritance is caught.
    """
    c = _bare_client()
    resp = _Resp(
        headers=CaseInsensitiveDict({
            "x-ratelimit-limit": "500",
            "x-ratelimit-period": "300",
            "x-ratelimit-remaining": "0",
            "x-ratelimit-reset": "120",
            "x-ratelimit-resource": "quotes",
        })
    )
    c._record_rate_limit_headers(resp, "marketdata/quotes/SPY")
    assert "quotes" in c._resource_rate_limit_state
    assert c._resource_rate_limit_state["quotes"]["remaining"] == 0


# --- Gate behaviour ---------------------------------------------------------


def test_gate_no_op_when_no_observation():
    """Cold start: no header seen yet -> gate is a no-op."""
    c = _bare_client()
    with patch("src.ingestion.tradestation_client.time.sleep") as sleep_mock:
        c._gate_for_resource("marketdata/quotes/SPY")
    sleep_mock.assert_not_called()


def test_gate_no_op_when_remaining_above_threshold():
    c = _bare_client()
    _seed_state(c, "marketdata/quotes/SPY", "quotes", remaining=100, reset=300, period=300)
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_HEADER_GATE_ENABLED", True), patch(
        "src.ingestion.tradestation_client.TS_RATE_LIMIT_HEADER_MIN_REMAINING", 1
    ), patch("src.ingestion.tradestation_client.time.sleep") as sleep_mock:
        c._gate_for_resource("marketdata/quotes/SPY")
    sleep_mock.assert_not_called()


def test_gate_sleeps_when_remaining_at_zero():
    c = _bare_client()
    _seed_state(c, "marketdata/quotes/SPY", "quotes", remaining=0, reset=120, period=300)
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_HEADER_GATE_ENABLED", True), patch(
        "src.ingestion.tradestation_client.TS_RATE_LIMIT_HEADER_MIN_REMAINING", 1
    ), patch("src.ingestion.tradestation_client.time.sleep") as sleep_mock:
        c._gate_for_resource("marketdata/quotes/SPY")
    assert sleep_mock.call_count == 1
    sleep_arg = sleep_mock.call_args[0][0]
    # ~120s reset + small cushion, capped at period
    assert 100 <= sleep_arg <= 305


def test_gate_caps_sleep_at_period():
    """A bogus reset value >> period must not strand the client."""
    c = _bare_client()
    _seed_state(c, "marketdata/quotes/SPY", "quotes", remaining=0, reset=99999, period=300)
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_HEADER_GATE_ENABLED", True), patch(
        "src.ingestion.tradestation_client.TS_RATE_LIMIT_HEADER_MIN_REMAINING", 1
    ), patch("src.ingestion.tradestation_client.time.sleep") as sleep_mock:
        c._gate_for_resource("marketdata/quotes/SPY")
    sleep_arg = sleep_mock.call_args[0][0]
    # Sleep capped at the period (+ ~0.25s cushion).
    assert sleep_arg <= 301


def test_gate_disabled_by_env():
    c = _bare_client()
    _seed_state(c, "marketdata/quotes/SPY", "quotes", remaining=0, reset=120, period=300)
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_HEADER_GATE_ENABLED", False), patch(
        "src.ingestion.tradestation_client.time.sleep"
    ) as sleep_mock:
        c._gate_for_resource("marketdata/quotes/SPY")
    sleep_mock.assert_not_called()


def test_gate_ignores_stale_observation():
    """An observation older than STALE_SECONDS must not trigger sleep."""
    c = _bare_client()
    _seed_state(c, "marketdata/quotes/SPY", "quotes", remaining=0, reset=120, period=300)
    # Force the observation to be ancient.
    c._resource_rate_limit_state["quotes"]["observed_at_mono"] -= 10_000
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_HEADER_GATE_ENABLED", True), patch(
        "src.ingestion.tradestation_client.TS_RATE_LIMIT_HEADER_STALE_SECONDS", 600
    ), patch("src.ingestion.tradestation_client.time.sleep") as sleep_mock:
        c._gate_for_resource("marketdata/quotes/SPY")
    sleep_mock.assert_not_called()


def test_gate_per_resource_isolation():
    """quotes resource exhausted -> options endpoint must NOT be gated."""
    c = _bare_client()
    _seed_state(c, "marketdata/quotes/SPY", "quotes", remaining=0, reset=120, period=300)
    # Options endpoint has its own (healthy) resource state.
    _seed_state(
        c,
        "marketdata/options/strikes/SPY",
        "option-strikes",
        remaining=80,
        reset=30,
        period=60,
    )
    with patch("src.ingestion.tradestation_client.TS_RATE_LIMIT_HEADER_GATE_ENABLED", True), patch(
        "src.ingestion.tradestation_client.TS_RATE_LIMIT_HEADER_MIN_REMAINING", 1
    ), patch("src.ingestion.tradestation_client.time.sleep") as sleep_mock:
        c._gate_for_resource("marketdata/options/strikes/SPY")
    sleep_mock.assert_not_called()


# --- 429 retry delay --------------------------------------------------------


def test_retry_delay_uses_reset_header():
    c = _bare_client()
    resp = _Resp(status_code=429, headers={"X-RateLimit-Reset": "47"})
    delay = c._retry_delay_for_429(resp, retry_count=0)
    # 47s + 0.5s cushion
    assert 47.0 <= delay <= 48.0


def test_retry_delay_falls_back_to_exponential_when_no_header():
    c = _bare_client()
    resp = _Resp(status_code=429, headers={})
    delay = c._retry_delay_for_429(resp, retry_count=2)
    # API_RETRY_DELAY * (API_RETRY_BACKOFF ** 2) with defaults (1.0, 2.0) -> 4.0
    assert delay == 4.0


def test_retry_delay_caps_runaway_reset_value():
    c = _bare_client()
    resp = _Resp(status_code=429, headers={"X-RateLimit-Reset": "100000"})
    delay = c._retry_delay_for_429(resp, retry_count=0)
    # Hard cap at 600s.
    assert delay <= 600.5


# --- Concurrency telemetry --------------------------------------------------


def test_concurrency_headers_recorded():
    c = _bare_client()
    resp = _Resp(
        headers={
            "X-Concurrency-Limit": "10",
            "X-Concurrency-Remaining": "3",
            "X-Concurrency-Resource": "streaming-quotes",
        }
    )
    c._record_rate_limit_headers(resp, "marketdata/stream/quotes/SPY")
    assert c._concurrency_state["streaming-quotes"]["limit"] == 10
    assert c._concurrency_state["streaming-quotes"]["remaining"] == 3


# --- Helpers ----------------------------------------------------------------


def _seed_state(client, endpoint, resource, *, remaining, reset, period):
    """Plant a per-resource state snapshot as if a real response had set it."""
    import time as _time

    now_mono = _time.monotonic()
    with client._resource_rate_limit_lock:
        client._resource_rate_limit_state[resource] = {
            "resource": resource,
            "limit": 500,
            "period_seconds": period,
            "remaining": remaining,
            "reset_at_mono": now_mono + reset,
            "observed_at_mono": now_mono,
            "observed_at_utc": datetime.now(timezone.utc),
        }
        client._endpoint_to_resource[TradeStationClient._endpoint_pattern(endpoint)] = resource
