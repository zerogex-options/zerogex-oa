"""Tests for the signal-persistence heartbeat that keeps history dense."""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone

import pytest


def test_heartbeat_disabled_when_window_zero(monkeypatch: pytest.MonkeyPatch):
    """SIGNAL_HEARTBEAT_SECONDS=0 must restore the original score-dedupe
    behaviour (skip persist when score hasn't changed)."""
    monkeypatch.setenv("SIGNAL_HEARTBEAT_SECONDS", "0")
    import src.signals.unified_signal_engine as mod

    importlib.reload(mod)
    now = datetime(2026, 5, 12, 18, 0, tzinfo=timezone.utc)
    cls = mod.UnifiedSignalEngine
    assert cls._HEARTBEAT_SECONDS == 0
    # Even with a never-persisted state, a zero-window means the heartbeat
    # is a no-op — the dedupe logic owns the decision.
    assert cls._heartbeat_due(None, now) is False
    assert cls._heartbeat_due(now - timedelta(hours=1), now) is False


def test_heartbeat_fires_when_window_elapsed(monkeypatch: pytest.MonkeyPatch):
    """Once the heartbeat interval is exceeded the persist must run even if
    the score didn't move, so trap_detection/eod_pressure don't strand the
    history at a single row when they sit near zero."""
    monkeypatch.setenv("SIGNAL_HEARTBEAT_SECONDS", "300")
    import src.signals.unified_signal_engine as mod

    importlib.reload(mod)
    now = datetime(2026, 5, 12, 18, 0, tzinfo=timezone.utc)
    cls = mod.UnifiedSignalEngine
    assert cls._HEARTBEAT_SECONDS == 300

    # First persist ever — no prior timestamp means we always force a row.
    assert cls._heartbeat_due(None, now) is True

    # Same bucket — no heartbeat yet.
    assert cls._heartbeat_due(now - timedelta(seconds=60), now) is False
    assert cls._heartbeat_due(now - timedelta(seconds=299), now) is False

    # Window crossed — heartbeat is due.
    assert cls._heartbeat_due(now - timedelta(seconds=300), now) is True
    assert cls._heartbeat_due(now - timedelta(seconds=305), now) is True
    assert cls._heartbeat_due(now - timedelta(hours=1), now) is True


def test_heartbeat_handles_missing_timestamps(monkeypatch: pytest.MonkeyPatch):
    """A NoneType.current_ts shouldn't crash the persist path — opt for
    'force persist' so we get at least one row rather than silently dropping
    the cycle."""
    monkeypatch.setenv("SIGNAL_HEARTBEAT_SECONDS", "60")
    import src.signals.unified_signal_engine as mod

    importlib.reload(mod)
    cls = mod.UnifiedSignalEngine
    assert cls._heartbeat_due(None, None) is True
    assert cls._heartbeat_due(datetime.now(timezone.utc), None) is True
