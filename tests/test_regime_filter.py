"""Tests for the time-of-day / scheduled-event regime filter."""

import importlib
from datetime import datetime, time

import pytest
import pytz

ET = pytz.timezone("US/Eastern")


def _reload_filter(monkeypatch, **env):
    """Reload config + regime_filter so env-var overrides take effect.

    Defaults are restored automatically because monkeypatch reverses each
    setenv at teardown.
    """
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    import src.config as config
    import src.signals.regime_filter as rf

    importlib.reload(config)
    importlib.reload(rf)
    return rf


@pytest.fixture
def filter_module(monkeypatch):
    """Default filter with all gates enabled and no events on the calendar.

    ``SIGNALS_LUNCH_BYPASS_SOURCES`` is cleared so the lunch-conviction
    override remains the gate under test; carve-out behavior is covered
    in dedicated tests below.
    """
    return _reload_filter(
        monkeypatch,
        SIGNALS_TIME_FILTER_ENABLED="true",
        SIGNALS_LUNCH_START_ET="11:30",
        SIGNALS_LUNCH_END_ET="13:30",
        SIGNALS_LUNCH_MSI_OVERRIDE="0.75",
        SIGNALS_LUNCH_BYPASS_SOURCES="",
        SIGNALS_LATE_CLOSE_LOCKDOWN_MINUTES="10",
        SIGNALS_EVENT_BUFFER_MINUTES="15",
        SIGNALS_EVENT_CALENDAR="",
    )


def _at(et_hour: int, et_minute: int = 0) -> datetime:
    return ET.localize(datetime(2026, 4, 28, et_hour, et_minute))


class TestLunchChop:
    def test_low_conviction_blocked_during_lunch(self, filter_module):
        d = filter_module.evaluate(
            timestamp=_at(12, 15),
            msi_conviction=0.50,
            signal_source="advanced:squeeze_setup",
        )
        assert d.skip is True
        assert "Lunch chop" in d.reason

    def test_high_conviction_overrides_lunch(self, filter_module):
        d = filter_module.evaluate(
            timestamp=_at(12, 15),
            msi_conviction=0.80,
            signal_source="advanced:squeeze_setup",
        )
        assert d.skip is False

    def test_outside_lunch_window_unaffected(self, filter_module):
        d = filter_module.evaluate(
            timestamp=_at(10, 0),
            msi_conviction=0.40,
            signal_source="advanced:squeeze_setup",
        )
        assert d.skip is False


class TestLunchBypassSources:
    """When ``SIGNALS_LUNCH_BYPASS_SOURCES`` matches the entry source the
    lunch chop conviction override is skipped — the advanced signal /
    Action Card already vetted the setup-specific filters."""

    def test_advanced_source_bypasses_lunch_override(self, monkeypatch):
        rf = _reload_filter(
            monkeypatch,
            SIGNALS_TIME_FILTER_ENABLED="true",
            SIGNALS_LUNCH_START_ET="11:30",
            SIGNALS_LUNCH_END_ET="13:30",
            SIGNALS_LUNCH_MSI_OVERRIDE="0.75",
            SIGNALS_LUNCH_BYPASS_SOURCES="advanced:,card:",
        )
        # Conviction well below the 0.75 override would normally block.
        d = rf.evaluate(
            timestamp=_at(12, 15),
            msi_conviction=0.40,
            signal_source="advanced:trap_detection",
        )
        assert d.skip is False

    def test_card_source_bypasses_lunch_override(self, monkeypatch):
        rf = _reload_filter(
            monkeypatch,
            SIGNALS_TIME_FILTER_ENABLED="true",
            SIGNALS_LUNCH_BYPASS_SOURCES="advanced:,card:",
        )
        d = rf.evaluate(
            timestamp=_at(12, 15),
            msi_conviction=0.20,
            signal_source="card:gamma_flip_break",
        )
        assert d.skip is False

    def test_composite_source_still_subject_to_override(self, monkeypatch):
        rf = _reload_filter(
            monkeypatch,
            SIGNALS_TIME_FILTER_ENABLED="true",
            SIGNALS_LUNCH_MSI_OVERRIDE="0.75",
            SIGNALS_LUNCH_BYPASS_SOURCES="advanced:,card:",
        )
        # Plain MSI-driven entries do not match the carve-out and still
        # have to clear the conviction override during lunch.
        d = rf.evaluate(
            timestamp=_at(12, 15),
            msi_conviction=0.50,
            signal_source="composite",
        )
        assert d.skip is True
        assert "Lunch chop" in d.reason

    def test_empty_bypass_list_disables_carve_out(self, monkeypatch):
        rf = _reload_filter(
            monkeypatch,
            SIGNALS_TIME_FILTER_ENABLED="true",
            SIGNALS_LUNCH_MSI_OVERRIDE="0.75",
            SIGNALS_LUNCH_BYPASS_SOURCES="",
        )
        d = rf.evaluate(
            timestamp=_at(12, 15),
            msi_conviction=0.40,
            signal_source="advanced:trap_detection",
        )
        assert d.skip is True


class TestLateCloseLockdown:
    def test_non_eod_signals_blocked_in_lockdown(self, filter_module):
        d = filter_module.evaluate(
            timestamp=_at(15, 55),  # 5 minutes before close
            msi_conviction=0.85,
            signal_source="advanced:squeeze_setup",
        )
        assert d.skip is True
        assert "Late-close lockdown" in d.reason

    def test_eod_pressure_allowed_in_lockdown(self, filter_module):
        d = filter_module.evaluate(
            timestamp=_at(15, 55),
            msi_conviction=0.85,
            signal_source="advanced:eod_pressure",
        )
        assert d.skip is False
        assert d.allow_eod_signal is True

    def test_outside_lockdown_unaffected(self, filter_module):
        d = filter_module.evaluate(
            timestamp=_at(15, 30),  # 30 minutes before close
            msi_conviction=0.65,
            signal_source="advanced:squeeze_setup",
        )
        assert d.skip is False


class TestEventBuffer:
    def test_event_window_blocks_all_signals(self, monkeypatch):
        rf = _reload_filter(
            monkeypatch,
            SIGNALS_TIME_FILTER_ENABLED="true",
            SIGNALS_EVENT_BUFFER_MINUTES="15",
            SIGNALS_EVENT_CALENDAR="2026-04-28T08:30",
        )
        # 8:30 ET is FOMC/CPI release time; ±15 min buffer.
        d = rf.evaluate(
            timestamp=_at(8, 35),
            msi_conviction=0.95,
            signal_source="advanced:eod_pressure",
        )
        assert d.skip is True
        assert "Event buffer" in d.reason

    def test_outside_event_buffer_unaffected(self, monkeypatch):
        rf = _reload_filter(
            monkeypatch,
            SIGNALS_TIME_FILTER_ENABLED="true",
            SIGNALS_EVENT_BUFFER_MINUTES="15",
            SIGNALS_EVENT_CALENDAR="2026-04-28T08:30",
        )
        d = rf.evaluate(
            timestamp=_at(10, 0),
            msi_conviction=0.65,
            signal_source="advanced:squeeze_setup",
        )
        assert d.skip is False


class TestDisabled:
    def test_disabled_filter_lets_everything_through(self, monkeypatch):
        rf = _reload_filter(monkeypatch, SIGNALS_TIME_FILTER_ENABLED="false")
        d = rf.evaluate(
            timestamp=_at(12, 15),
            msi_conviction=0.10,
            signal_source="advanced:squeeze_setup",
        )
        assert d.skip is False
