"""The API burned 24.2 MB/h of journal — four times any other service — against
a 300M cap shared by all four, leaving ~2 hours of history.

Two shapes dominated its repeated lines, measured on the box 2026-09-01:

    1941 x  INFO: connection closed          (uvicorn.error, per websocket)
    1936 x  INFO: connection open
    1786 x  src.analytics.main_engine - INFO - Risk-free rate: N.N

The first two are uvicorn handing its OWN logger to the websockets server, so
they cannot be quieted by level without silencing uvicorn's real errors. The
third is an AnalyticsEngine startup banner — the API constructs one per
request, so a daemon's four-line announcement ran ~7,000 times an hour.
"""

from __future__ import annotations

import logging

import pytest

from src.utils.logging import _DropExactMessages, _quiet_third_party_chatter


@pytest.fixture
def uvicorn_error_logger():
    log = logging.getLogger("uvicorn.error")
    original = list(log.filters)
    for f in list(log.filters):
        log.removeFilter(f)
    yield log
    for f in list(log.filters):
        log.removeFilter(f)
    for f in original:
        log.addFilter(f)


def _record(logger, msg):
    return logger.makeRecord(logger.name, logging.INFO, __file__, 1, msg, None, None)


def test_the_two_websocket_lines_are_dropped(uvicorn_error_logger, monkeypatch):
    monkeypatch.delenv("QUIET_WEBSOCKET_LOGS", raising=False)
    _quiet_third_party_chatter()
    log = uvicorn_error_logger
    assert not any(f.filter(_record(log, "connection open")) for f in log.filters)
    assert not any(f.filter(_record(log, "connection closed")) for f in log.filters)


def test_every_other_uvicorn_message_survives(uvicorn_error_logger, monkeypatch):
    """The whole reason for a message filter rather than a level: uvicorn's
    errors share this logger, and losing them would be far worse than the
    noise."""
    monkeypatch.delenv("QUIET_WEBSOCKET_LOGS", raising=False)
    _quiet_third_party_chatter()
    log = uvicorn_error_logger
    for msg in (
        "Application startup complete.",
        "Uvicorn running on http://0.0.0.0:8000",
        "ERROR:    Exception in ASGI application",
        "connection open but not really",
        "closed",
    ):
        assert all(f.filter(_record(log, msg)) for f in log.filters), msg


def test_the_filter_is_not_stacked_on_repeat_configuration(uvicorn_error_logger, monkeypatch):
    monkeypatch.delenv("QUIET_WEBSOCKET_LOGS", raising=False)
    for _ in range(5):
        _quiet_third_party_chatter()
    installed = [f for f in uvicorn_error_logger.filters if isinstance(f, _DropExactMessages)]
    assert len(installed) == 1


def test_it_can_be_turned_off(uvicorn_error_logger, monkeypatch):
    monkeypatch.setenv("QUIET_WEBSOCKET_LOGS", "false")
    _quiet_third_party_chatter()
    assert not [f for f in uvicorn_error_logger.filters if isinstance(f, _DropExactMessages)]


# --- the AnalyticsEngine startup banner -------------------------------------


def test_the_engine_banner_is_logged_once_per_symbol(caplog, monkeypatch):
    """Constructs the REAL AnalyticsEngine rather than replaying its banner
    block: a test that re-implements the logic it is checking stays green when
    the call site stops using it, which is the failure this repo has already
    been bitten by once.

    The daemon builds one engine per symbol and must still announce each; the
    API rebuilds the same symbols every request and must not.
    """
    import src.analytics.main_engine as me

    monkeypatch.setattr(me, "_BANNER_LOGGED_FOR", set())
    caplog.set_level(logging.DEBUG, logger="src.analytics.main_engine")

    for _ in range(5):
        me.AnalyticsEngine(underlying="SPX")
    for _ in range(5):
        me.AnalyticsEngine(underlying="NDX")

    def banner_at(level):
        return [
            r
            for r in caplog.records
            if r.levelno == level and "Initialized AnalyticsEngine" in r.msg
        ]

    assert len(banner_at(logging.INFO)) == 2, "once for SPX, once for NDX"
    assert {r.args[0] for r in banner_at(logging.INFO)} == {"SPX", "NDX"}
    assert len(banner_at(logging.DEBUG)) == 8, "the other eight are demoted"


def test_the_risk_free_line_stops_repeating(caplog, monkeypatch):
    """The specific line measured at 1,786/hour in the API's journal."""
    import src.analytics.main_engine as me

    monkeypatch.setattr(me, "_BANNER_LOGGED_FOR", set())
    caplog.set_level(logging.DEBUG, logger="src.analytics.main_engine")

    for _ in range(20):
        me.AnalyticsEngine(underlying="SPX")

    at_info = [r for r in caplog.records if r.levelno == logging.INFO and "Risk-free rate" in r.msg]
    assert len(at_info) == 1
