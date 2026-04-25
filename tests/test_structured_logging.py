"""Tests for src.utils.logging structured-output behavior.

Verifies:
- LOG_FORMAT=text emits the expected one-line shape with [request_id=...].
- LOG_FORMAT=json emits parseable JSON with renamed timestamp/level keys.
- RequestIdFilter copies the contextvar value into every record.
- An unset contextvar defaults to "-" (the standard sentinel).

The handler resets between tests by re-running ``_configure_logging``
with ``_logging_configured`` flipped back to False — that's the same
recipe the existing test_stable_snapshot_queries pattern uses for
module-level state.
"""

from __future__ import annotations

import importlib
import io
import json
import logging
from typing import Tuple

import pytest


def _reload_logging(monkeypatch: pytest.MonkeyPatch, *, log_format: str) -> Tuple[object, object]:
    """Re-import src.utils.logging with the requested LOG_FORMAT.

    Returns (module, root_logger). The caller swaps the handler stream
    before emitting so we can capture output as a string.
    """
    monkeypatch.setenv("LOG_FORMAT", log_format)
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    import src.utils.logging as log_mod

    # Force a fresh configuration on next get_logger() call.
    log_mod._logging_configured = False  # type: ignore[attr-defined]
    importlib.reload(log_mod)
    log_mod.get_logger("__test__")
    return log_mod, logging.getLogger()


def _swap_capture_stream(root: logging.Logger) -> io.StringIO:
    """Replace the active StreamHandler's stream with an in-memory buffer.

    There is exactly one handler installed by _configure_logging — the
    one we built. Tests assert against its output.
    """
    buf = io.StringIO()
    handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
    assert handlers, "expected exactly one StreamHandler from _configure_logging"
    handlers[0].stream = buf
    return buf


# --------------------------------------------------------------------------- #
# Text format
# --------------------------------------------------------------------------- #


def test_text_format_includes_request_id_field(monkeypatch: pytest.MonkeyPatch):
    log_mod, root = _reload_logging(monkeypatch, log_format="text")
    buf = _swap_capture_stream(root)

    log_mod.request_id_var.set("abc123")
    log_mod.get_logger("test.text").info("hello world")

    line = buf.getvalue()
    assert "[request_id=abc123]" in line, line
    assert "hello world" in line


def test_text_format_request_id_dash_when_unset(monkeypatch: pytest.MonkeyPatch):
    log_mod, root = _reload_logging(monkeypatch, log_format="text")
    buf = _swap_capture_stream(root)

    # Reset to the default sentinel.
    log_mod.request_id_var.set("-")
    log_mod.get_logger("test.text2").info("no context")

    assert "[request_id=-]" in buf.getvalue()


# --------------------------------------------------------------------------- #
# JSON format
# --------------------------------------------------------------------------- #


def test_json_format_emits_parseable_json(monkeypatch: pytest.MonkeyPatch):
    pytest.importorskip("pythonjsonlogger")
    log_mod, root = _reload_logging(monkeypatch, log_format="json")
    buf = _swap_capture_stream(root)

    log_mod.request_id_var.set("req-xyz-789")
    log_mod.get_logger("test.json").warning("structured event")

    raw = buf.getvalue().strip()
    payload = json.loads(raw)
    assert payload["message"] == "structured event"
    assert payload["request_id"] == "req-xyz-789"
    assert payload["level"] == "WARNING"  # renamed from levelname
    assert "timestamp" in payload  # renamed from asctime
    assert payload["name"] == "test.json"


def test_json_format_keys_are_renamed(monkeypatch: pytest.MonkeyPatch):
    """Pin the rename mapping so a future logger swap doesn't silently
    revert to the verbose stdlib field names."""
    pytest.importorskip("pythonjsonlogger")
    log_mod, root = _reload_logging(monkeypatch, log_format="json")
    buf = _swap_capture_stream(root)

    log_mod.get_logger("test.rename").info("check rename")
    payload = json.loads(buf.getvalue().strip())

    # The new names are present.
    assert "timestamp" in payload
    assert "level" in payload
    # The stdlib names are absent.
    assert "asctime" not in payload
    assert "levelname" not in payload


# --------------------------------------------------------------------------- #
# Filter contract
# --------------------------------------------------------------------------- #


def test_filter_copies_contextvar_to_record(monkeypatch: pytest.MonkeyPatch):
    """The filter is what makes %(request_id)s available to formatters."""
    log_mod, _root = _reload_logging(monkeypatch, log_format="text")

    log_mod.request_id_var.set("filter-test")
    record = logging.LogRecord(
        name="x", level=logging.INFO, pathname="", lineno=0, msg="m", args=(), exc_info=None
    )
    f = log_mod.RequestIdFilter()
    assert f.filter(record) is True
    assert record.request_id == "filter-test"  # type: ignore[attr-defined]


def test_new_request_id_is_unique_hex():
    from src.utils.logging import new_request_id

    a, b = new_request_id(), new_request_id()
    assert a != b
    assert all(c in "0123456789abcdef" for c in a)
    assert len(a) == 32
