"""X-Concurrency-* streaming-budget header observability in stream_manager.

IF TradeStation emits ``X-Concurrency-*`` headers on a stream response (max
streams, remaining slots, resource group), the stream readers parse and log
that per-API-key concurrent-connection budget on connect — the authoritative,
deterministic counterpart to the lifetime-based cap heuristic, which can only
*infer* exhaustion from a short connection lifetime.

Reality check (2026-07-19 live probe): a standard API key does NOT receive
these headers — both stream resources (``quote-change-stream`` and
``stream-barcharts``) return only ``X-Ratelimit-*``. The parser is therefore
forward-looking and dormant on a standard key; these tests exercise it with
synthetic headers so it is correct and safe for the day the headers appear.

These tests pin two guarantees:

  * the parser is correct and case-insensitive, and
  * the whole feature is STRICTLY observational — it never raises and never
    perturbs the reader's control flow, no matter how malformed the headers.
"""

import json
import threading
from unittest.mock import MagicMock

from src.ingestion import stream_manager as sm
from src.ingestion.stream_manager import (
    OptionStreamAccumulator,
    _log_concurrency_budget,
    _parse_concurrency_headers,
)

# ---------------------------------------------------------------------------
# _parse_concurrency_headers — pure parsing
# ---------------------------------------------------------------------------


def test_parse_full_headers_lowercase():
    parsed = _parse_concurrency_headers(
        {
            "x-concurrency-limit": "10",
            "x-concurrency-remaining": "3",
            "x-concurrency-resource": "quotes",
        }
    )
    assert parsed == {
        "limit": 10,
        "remaining": 3,
        "resource": "quotes",
        "raw": {
            "x-concurrency-limit": "10",
            "x-concurrency-remaining": "3",
            "x-concurrency-resource": "quotes",
        },
    }


def test_parse_is_case_insensitive():
    """Real responses use ``X-Concurrency-Limit`` capitalization."""
    parsed = _parse_concurrency_headers(
        {"X-Concurrency-Limit": "10", "X-Concurrency-Remaining": "0"}
    )
    assert parsed is not None
    assert parsed["limit"] == 10
    assert parsed["remaining"] == 0


def test_parse_ignores_unrelated_headers():
    parsed = _parse_concurrency_headers(
        {"Content-Type": "application/json", "X-RateLimit-Limit": "120"}
    )
    assert parsed is None


def test_parse_partial_headers():
    """Only the limit present — remaining/resource come back as None."""
    parsed = _parse_concurrency_headers({"X-Concurrency-Limit": "10"})
    assert parsed["limit"] == 10
    assert parsed["remaining"] is None
    assert parsed["resource"] is None


def test_parse_malformed_int_does_not_raise():
    parsed = _parse_concurrency_headers(
        {"X-Concurrency-Limit": "N/A", "X-Concurrency-Remaining": "3"}
    )
    assert parsed["limit"] is None
    assert parsed["remaining"] == 3


def test_parse_none_and_non_mapping_return_none():
    assert _parse_concurrency_headers(None) is None
    assert _parse_concurrency_headers(42) is None
    assert _parse_concurrency_headers("not-a-mapping") is None


def test_parse_misbehaving_mapping_returns_none():
    class _BadHeaders:
        def items(self):
            raise RuntimeError("boom")

    # Must be swallowed inside the parser, not propagate.
    assert _parse_concurrency_headers(_BadHeaders()) is None


# ---------------------------------------------------------------------------
# _log_concurrency_budget — observational logging, never raises
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, headers):
        self.headers = headers


def test_log_healthy_budget_logs_info_not_warning(monkeypatch):
    fake_logger = MagicMock()
    monkeypatch.setattr(sm, "logger", fake_logger)

    parsed = _log_concurrency_budget(
        "Option stream",
        _Resp({"X-Concurrency-Limit": "10", "X-Concurrency-Remaining": "4"}),
    )

    assert parsed["remaining"] == 4
    assert fake_logger.info.call_count == 1
    assert fake_logger.warning.call_count == 0
    assert "concurrent-stream budget" in fake_logger.info.call_args.args[0]


def test_log_exhausted_budget_logs_warning(monkeypatch):
    fake_logger = MagicMock()
    monkeypatch.setattr(sm, "logger", fake_logger)

    parsed = _log_concurrency_budget(
        "Option stream",
        _Resp({"X-Concurrency-Limit": "10", "X-Concurrency-Remaining": "0"}),
    )

    assert parsed["remaining"] == 0
    assert fake_logger.warning.call_count == 1
    assert fake_logger.info.call_count == 0
    assert "EXHAUSTED" in fake_logger.warning.call_args.args[0]


def test_log_no_headers_is_silent(monkeypatch):
    fake_logger = MagicMock()
    monkeypatch.setattr(sm, "logger", fake_logger)

    # Response with no X-Concurrency-* headers, and a response with no
    # ``headers`` attribute at all, both no-op silently.
    assert _log_concurrency_budget("x", _Resp({"Content-Type": "text/plain"})) is None
    assert _log_concurrency_budget("x", object()) is None
    assert fake_logger.info.call_count == 0
    assert fake_logger.warning.call_count == 0


def test_log_never_raises_on_bad_headers(monkeypatch):
    fake_logger = MagicMock()
    monkeypatch.setattr(sm, "logger", fake_logger)

    class _BadHeaders:
        def items(self):
            raise RuntimeError("boom")

    # Even a headers object that raises on iteration must not escape.
    assert _log_concurrency_budget("x", _Resp(_BadHeaders())) is None


# ---------------------------------------------------------------------------
# End-to-end wiring: _read_stream populates last_concurrency without changing
# any existing control flow.
# ---------------------------------------------------------------------------


class _FakeResponse:
    """``requests.Response`` stand-in with an optional ``headers`` map."""

    def __init__(self, lines, status_code=200, headers=None):
        self._lines = list(lines)
        self.status_code = status_code
        self.headers = headers if headers is not None else {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_lines(self, decode_unicode=False):
        for line in self._lines:
            yield line

    def close(self):
        pass


def _bare_option_acc():
    acc = object.__new__(OptionStreamAccumulator)
    acc._client = MagicMock()
    acc._client.base_url = "https://api.tradestation.com/v3"
    acc._client.auth.get_headers.return_value = {}
    acc._running = True
    acc._chunks = [["SPY"]]
    acc._current_responses = [None]
    acc._response_lock = threading.Lock()
    acc._connected = threading.Event()
    acc._state = {}
    acc._lock = threading.Lock()
    acc._dirty = set()
    acc._updates_received = 0
    acc._wakeup = None
    return acc


def test_read_stream_populates_last_concurrency(monkeypatch):
    """A live connect that carries the headers surfaces them on the
    accumulator's ``last_concurrency`` property — with no change to the
    existing cap-classifier behavior (one quote payload => cap branch,
    no injected sleep)."""
    acc = _bare_option_acc()
    payload_line = json.dumps({"Quotes": [{"Symbol": "SPY", "Last": "500.00"}]})
    resp = _FakeResponse(
        [payload_line],
        headers={"X-Concurrency-Limit": "10", "X-Concurrency-Remaining": "2"},
    )
    monkeypatch.setattr(sm, "_requests", MagicMock(get=lambda *a, **k: resp))

    sleep_calls = []
    monkeypatch.setattr(sm.time, "sleep", lambda s: sleep_calls.append(s))
    fake_logger = MagicMock()
    monkeypatch.setattr(sm, "logger", fake_logger)

    acc._read_stream(0, ["SPY"], "Option stream")

    # Budget surfaced on the property.
    assert acc.last_concurrency == {
        "limit": 10,
        "remaining": 2,
        "resource": None,
        "raw": {"x-concurrency-limit": "10", "x-concurrency-remaining": "2"},
    }
    # Existing behavior unchanged: cap-exhaustion branch fired (data flowed
    # then short close) and it did NOT inject a degraded-upstream sleep.
    assert sleep_calls == []
    info_msgs = [c.args[0] for c in fake_logger.info.call_args_list]
    assert any("concurrent-stream budget" in m for m in info_msgs), info_msgs


def test_last_concurrency_defaults_none_before_connect():
    acc = _bare_option_acc()
    assert acc.last_concurrency is None
