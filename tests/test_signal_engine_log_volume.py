"""The signal engine cycles once a SECOND, so anything it logs unconditionally
is ~3,600 lines an hour per symbol.

Measured on the box 2026-09-01: three per-cycle INFO lines accounted for the
top repeated shapes in the journal — 3,339 copies each in one hour — against a
300M cap shared by four services that consequently held ~2 hours of history.
That is the same retention that left the 2026-08-17 QQQ post-mortem with no
evening logs to read, so the noise is not merely untidy: it destroys the
evidence the next outage will need.

These tests pin the two latches that keep the steady state out of the journal
while leaving every state CHANGE at INFO.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from src.signals.unified_signal_engine import UnifiedSignalEngine


class _Card:
    def __init__(self, action="HOLD", pattern="none", confidence=0.5):
        self.action = SimpleNamespace(value=action)
        self.pattern = pattern
        self.confidence = confidence


@pytest.fixture
def engine():
    """A bare engine: __init__ builds real components, so bypass it and set
    only the attributes the logging paths touch."""
    eng = object.__new__(UnifiedSignalEngine)
    eng.underlying = "SPX"
    eng.db_symbol = "SPX"
    eng._legacy_disabled_logged = False
    eng._last_playbook_state = None
    return eng


def _emit_playbook(engine, card, monkeypatch):
    """Drive _evaluate_playbook's logging with the DB work stubbed out."""
    monkeypatch.setattr(
        "src.signals.playbook.cycle.evaluate_and_persist",
        lambda **kw: card,
    )
    monkeypatch.setattr("src.signals.unified_signal_engine.db_connection", _NullConn)
    monkeypatch.setattr("src.signals.playbook.PlaybookEngine", lambda *a, **k: object())
    engine._playbook_engine = object()
    return engine._evaluate_playbook(None, None, None, None)


class _NullConn:
    def __enter__(self):
        return None

    def __exit__(self, *exc):
        return False


def test_an_unchanged_playbook_card_stops_logging_at_info(engine, monkeypatch, caplog):
    """The steady state: same action and pattern, second after second."""
    caplog.set_level(logging.DEBUG, logger="src.signals.unified_signal_engine")
    for _ in range(10):
        _emit_playbook(engine, _Card("HOLD", "chop"), monkeypatch)
    at_info = [r for r in caplog.records if r.levelno == logging.INFO and "Playbook" in r.message]
    assert len(at_info) == 1, "only the first card is news"


def test_a_changed_playbook_card_is_logged_at_info(engine, monkeypatch, caplog):
    """The transition is the part an operator actually reads."""
    caplog.set_level(logging.DEBUG, logger="src.signals.unified_signal_engine")
    _emit_playbook(engine, _Card("HOLD", "chop"), monkeypatch)
    _emit_playbook(engine, _Card("LONG", "squeeze"), monkeypatch)
    at_info = [r for r in caplog.records if r.levelno == logging.INFO and "Playbook" in r.message]
    assert len(at_info) == 2


def test_drifting_confidence_alone_is_not_a_change(engine, monkeypatch, caplog):
    """Confidence moves on nearly every cycle; keying on it would restore the
    per-second logging while looking like it had been fixed."""
    caplog.set_level(logging.DEBUG, logger="src.signals.unified_signal_engine")
    for c in (0.50, 0.51, 0.52, 0.53):
        _emit_playbook(engine, _Card("HOLD", "chop", c), monkeypatch)
    at_info = [r for r in caplog.records if r.levelno == logging.INFO and "Playbook" in r.message]
    assert len(at_info) == 1


def test_a_card_returning_to_a_prior_state_logs_again(engine, monkeypatch, caplog):
    """HOLD -> LONG -> HOLD is two transitions back to a seen state, not a
    repeat: only the immediately preceding state is compared."""
    caplog.set_level(logging.DEBUG, logger="src.signals.unified_signal_engine")
    for action, pattern in (("HOLD", "chop"), ("LONG", "squeeze"), ("HOLD", "chop")):
        _emit_playbook(engine, _Card(action, pattern), monkeypatch)
    at_info = [r for r in caplog.records if r.levelno == logging.INFO and "Playbook" in r.message]
    assert len(at_info) == 3
