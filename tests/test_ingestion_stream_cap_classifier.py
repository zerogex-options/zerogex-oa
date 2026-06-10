"""Cap-exhaustion vs degraded-upstream classifier in the option stream
``_read_stream`` finally block.

The original heuristic logged the same "TradeStation per-account
concurrent-stream cap (~10) exhausted" WARN for every short-lifetime
disconnect, regardless of whether any data flowed. The 2026-06-09
journal recorded a 9-warning burst at 06:33:05 with elapsed=0.0s, capped
by an explicit 502 — that's an upstream gateway hiccup, not the cap.
These tests pin the distinction:

  * connection that yielded quote payloads + short lifetime → cap
    exhaustion message (the per-account cap is the dominant explanation
    for a stream that was working and got cut);
  * connection that returned 200 with no quote payloads + short lifetime
    → upstream-degraded message + 2s back-off so the reader loop
    doesn't hot-loop the upstream.
"""

import json
import threading
import types
from unittest.mock import MagicMock

from src.ingestion import stream_manager as sm
from src.ingestion.stream_manager import OptionStreamAccumulator


class _FakeResponse:
    """Minimal stand-in for ``requests.Response`` covering the surface
    ``_read_stream`` uses: ``status_code``, ``raise_for_status``,
    ``iter_lines``, ``close``."""

    def __init__(self, lines, status_code=200):
        self._lines = list(lines)
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_lines(self, decode_unicode=False):
        for line in self._lines:
            yield line

    def close(self):
        pass


def _bare_option_acc(monkeypatch):
    """Engine bypassing __init__, wired with the attributes ``_read_stream``
    touches. ``_running`` is set False after the first connection so the
    reader loop wrapper (when used) only iterates once — for these tests
    we call ``_read_stream`` directly though."""
    acc = object.__new__(OptionStreamAccumulator)
    acc._client = MagicMock()
    acc._client.base_url = "https://api.tradestation.com/v3"
    acc._client.auth.get_headers.return_value = {}
    acc._running = True
    acc._chunks = [["SPY"], ["QQQ"]]  # two chunks so the WARN reports `len=2`
    acc._current_responses = [None, None]
    acc._response_lock = threading.Lock()
    acc._connected = threading.Event()
    acc._state = {}
    acc._lock = threading.Lock()
    acc._dirty = set()
    acc._updates_received = 0
    acc._wakeup = None
    return acc


def test_empty_iterlines_logs_degraded_upstream_and_backs_off(monkeypatch):
    """The 2026-06-09 06:33 burst fingerprint: 200 OK, iter_lines yields
    nothing, elapsed ≈ 0s. The WARN must name upstream degradation, NOT
    the per-account cap, and the finally block must sleep so the reader
    loop doesn't hammer TradeStation."""
    acc = _bare_option_acc(monkeypatch)
    monkeypatch.setattr(sm, "_requests", types.SimpleNamespace(get=lambda *a, **k: _FakeResponse([])))

    sleep_calls = []
    monkeypatch.setattr(sm.time, "sleep", lambda s: sleep_calls.append(s))

    fake_logger = MagicMock()
    monkeypatch.setattr(sm, "logger", fake_logger)

    acc._read_stream(0, ["SPY"], "Option stream chunk 1/2")

    warn_msgs = [c.args[0] for c in fake_logger.warning.call_args_list]
    assert any("without yielding any quotes" in m for m in warn_msgs), warn_msgs
    # The degraded message contains "NOT the per-account...cap" — assert
    # against the affirmative cap-exhaustion wording instead so the two
    # branches can't false-match each other.
    assert not any("(~10) exhausted." in m for m in warn_msgs), warn_msgs
    # Back-off must have fired exactly once with the configured interval.
    assert sleep_calls == [sm._STREAM_DEGRADED_UPSTREAM_BACKOFF_SECONDS]


def test_data_then_short_close_logs_cap_exhaustion(monkeypatch):
    """A connection that streamed quotes then was cut after a few seconds
    is the actual cap-exhaustion fingerprint — keep the existing wording
    and DO NOT back off (the reader loop's standard reconnect handles it)."""
    acc = _bare_option_acc(monkeypatch)
    payload_line = json.dumps({"Quotes": [{"Symbol": "SPY", "Last": "500.00"}]})
    monkeypatch.setattr(
        sm,
        "_requests",
        types.SimpleNamespace(get=lambda *a, **k: _FakeResponse([payload_line])),
    )

    sleep_calls = []
    monkeypatch.setattr(sm.time, "sleep", lambda s: sleep_calls.append(s))

    fake_logger = MagicMock()
    monkeypatch.setattr(sm, "logger", fake_logger)

    acc._read_stream(0, ["SPY"], "Option stream chunk 1/2")

    warn_msgs = [c.args[0] for c in fake_logger.warning.call_args_list]
    assert any("(~10) exhausted." in m for m in warn_msgs), warn_msgs
    assert not any("without yielding any quotes" in m for m in warn_msgs), warn_msgs
    # Cap-exhaustion path must not insert its own sleep — the reader loop's
    # exception-driven 2s sleep is the existing recovery path.
    assert sleep_calls == []


def test_stopped_mid_read_does_not_warn(monkeypatch):
    """``stop()`` flips ``_running`` False; the resulting clean exit must
    not look like cap exhaustion or degradation."""
    acc = _bare_option_acc(monkeypatch)
    acc._running = False  # simulate stop() already called
    monkeypatch.setattr(sm, "_requests", types.SimpleNamespace(get=lambda *a, **k: _FakeResponse([])))

    sleep_calls = []
    monkeypatch.setattr(sm.time, "sleep", lambda s: sleep_calls.append(s))

    fake_logger = MagicMock()
    monkeypatch.setattr(sm, "logger", fake_logger)

    acc._read_stream(0, ["SPY"], "Option stream chunk 1/2")

    assert fake_logger.warning.call_count == 0
    assert sleep_calls == []
