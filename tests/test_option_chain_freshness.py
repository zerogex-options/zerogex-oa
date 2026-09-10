"""Freshness of the option-chain endpoints: they keep the options session.

``/api/option/quote``, ``/api/option/contract`` and
``/api/market/open-interest`` all read ``option_chains``. A chain row is
written when an option quote ticks, and options trade **09:30-16:15 ET** —
while the underlying bar feed under ``USEQ24Hour`` runs **04:00-20:00**.

Graded on the tape's window (``realtime_quote``) these three reported
``stale`` from 04:00-09:30 and from 16:15-20:00 on a perfectly healthy
system: 9h15m of false alarm per weekday against the 6h45m in which they
could be right — wrong for more of the day than not. It never surfaced
because every smoke run landed outside market hours, where the old
04:00-20:00 window happened to say ``session_closed`` anyway.

The ops-side ingestion check reached the same premise from the other
direction and measured it in production: 21 of 69 gaps over 15 minutes in
``option_chains`` fell outside the options session, up to 45 minutes wide,
and every one of them paged. Both paths now read the window from a single
helper, ``market_calendar.option_chain_feed_expected``, so the API and the
alerting cannot drift apart — the guard at the bottom of this file is what
holds that.

The subtle part is the tail. The chain window runs 15 minutes past the
16:00 close, which ``market_context`` labels ``after-hours``, so the profile
cannot be modelled as "cash session only" the way flow is: that would go
blind over 16:00-16:15, when SPY/QQQ chains are still ticking. It keeps
``extended_seconds`` set and gates on the calendar instead, which also knows
the two things a session label cannot — cash-index chains stop at 16:00 with
the index they price, and every chain stops at the early close on a half day.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import src.market_calendar as mc
from src.api import freshness as fr
from src.api import v2 as v2mod

ET = fr._ET
PROFILE = fr.resolve_profile("/api/option/quote")


def _et(h, m, day=21):
    """2026-08-21 is a Friday; 2026-08-22 a Saturday."""
    return ET.localize(datetime(2026, 8, day, h, m)).astimezone(timezone.utc)


def _grade(now, source, symbol="SPY"):
    return fr.build_freshness({"quote_timestamp": source}, profile=PROFILE, now=now, symbol=symbol)


def _healthy(now, symbol="SPY"):
    """A chain feed doing its job: the newest bucket is under a minute old."""
    return _grade(now, now - timedelta(seconds=45), symbol)


# ---------------------------------------------------------------------------
# The bug: 9h15m/day of false stale
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hour,minute",
    [(4, 1), (6, 0), (8, 0), (9, 29), (16, 20), (17, 0), (18, 30), (19, 55)],
)
def test_a_closed_options_market_is_not_an_outage(hour, minute):
    """Every one of these read `stale` before the split, on a healthy feed.

    Outside the options session the newest row that can exist is the last one
    written before the close, so an ageing payload is the correct answer and
    not a fault.
    """
    now = _et(hour, minute)
    last_tick = _et(16, 15, 20 if hour < 12 else 21)
    assert _grade(now, last_tick).freshness_status is fr.FreshnessStatus.SESSION_CLOSED


@pytest.mark.parametrize("hour,minute", [(9, 31), (10, 0), (12, 0), (15, 59), (16, 14)])
def test_a_healthy_chain_is_fresh_all_through_the_options_session(hour, minute):
    now = _et(hour, minute)
    assert _healthy(now).freshness_status is fr.FreshnessStatus.FRESH
    assert _healthy(now).expected_update_cadence_seconds == PROFILE.regular_seconds


def test_a_dead_chain_inside_the_session_is_still_caught():
    """The fix must narrow the window, not suppress the signal."""
    f = _grade(_et(13, 0), _et(11, 0))
    assert f.freshness_status is fr.FreshnessStatus.STALE
    assert f.age_seconds == pytest.approx(7200)


# ---------------------------------------------------------------------------
# The 16:00-16:15 tail — why extended_seconds stays set
# ---------------------------------------------------------------------------


def test_the_late_session_is_graded_not_written_off():
    """SPY/QQQ chains tick until 16:15. Modelling this profile as cash-session
    only (extended_seconds=None, the way flow is modelled) would report
    `session_closed` from 16:00 and go blind over the last fifteen minutes."""
    assert PROFILE.extended_seconds is not None
    assert _healthy(_et(16, 5)).freshness_status is fr.FreshnessStatus.FRESH
    # And a chain that died at the close is caught during the tail.
    assert _grade(_et(16, 12), _et(15, 58)).freshness_status is fr.FreshnessStatus.STALE


def test_cash_index_chains_stop_with_the_index_at_1600():
    """SPX/NDX chains stop when the index stops printing — Greeks are refused
    without an underlying price — so the tail is an ETF fact, not a market one."""
    late = _et(16, 5)
    assert _healthy(late, "SPY").freshness_status is fr.FreshnessStatus.FRESH
    assert _grade(late, _et(15, 59), "SPX").freshness_status is fr.FreshnessStatus.SESSION_CLOSED


def test_the_early_close_on_a_half_day_stops_the_chains(monkeypatch: pytest.MonkeyPatch):
    """A half day closes at 13:00. Without the calendar the afternoon would
    grade against a tape that stopped hours earlier."""
    monkeypatch.setattr(mc, "NYSE_HALF_DAYS", {date(2026, 8, 21)})
    assert _grade(_et(14, 0), _et(13, 0)).freshness_status is fr.FreshnessStatus.SESSION_CLOSED
    # The morning of a half day still grades normally.
    assert _healthy(_et(11, 0)).freshness_status is fr.FreshnessStatus.FRESH


def test_a_weekend_is_closed_for_chains_too():
    assert _grade(_et(13, 0, 22), _et(16, 15)).freshness_status is fr.FreshnessStatus.SESSION_CLOSED


# ---------------------------------------------------------------------------
# The window must not open straight into stale
# ---------------------------------------------------------------------------


def test_the_chain_window_does_not_open_into_stale():
    """``feed_opens_et`` is load-bearing. ``feed_window_open`` infers 04:00 for
    any profile with extended_seconds set, and this profile has to keep that
    set for the 16:15 tail — so without the explicit override the anchor would
    be 04:00, five and a half hours before a chain row can exist, and every
    endpoint here would be stale the instant the session opened."""
    assert PROFILE.feed_opens_et == fr.time(9, 30)
    opened = fr.feed_window_open(PROFILE, _et(9, 31))
    assert opened == _et(9, 30)
    # Nothing written yet today: the prior close is the newest row there is.
    for minutes in (0, 1, 2):
        f = _grade(_et(9, 30) + timedelta(minutes=minutes), _et(16, 15, 20))
        assert f.freshness_status is not fr.FreshnessStatus.STALE, f"stale at +{minutes}min"
    # A window that stays genuinely empty still goes stale.
    assert _grade(_et(9, 40), _et(16, 15, 20)).freshness_status is fr.FreshnessStatus.STALE


def test_the_tape_keeps_its_own_wider_window():
    """The split must not drag /api/market/quote along: underlying bars really
    do run 04:00-20:00, and narrowing them would hide a dead pre-market tape."""
    tape = fr.resolve_profile("/api/market/quote")
    assert tape is fr.REALTIME_QUOTE
    assert fr.feed_window_open(tape, _et(6, 0)) == _et(4, 0)
    assert (
        fr.build_freshness({"timestamp": _et(4, 0)}, profile=tape, now=_et(6, 0)).freshness_status
        is fr.FreshnessStatus.STALE
    )


# ---------------------------------------------------------------------------
# The reported session stays truthful
# ---------------------------------------------------------------------------


def test_the_session_label_is_not_bent_to_fit_the_verdict():
    """`market_session_status` answers "what is the equity market doing", and a
    consumer reads it beside /api/market/quote's own `session` field. Only the
    cadence narrows: at 17:00 the market is in after-hours AND no chain update
    is due, and the envelope says both."""
    f = _grade(_et(17, 0), _et(16, 15))
    assert f.market_session_status == "after-hours"
    assert f.freshness_status is fr.FreshnessStatus.SESSION_CLOSED
    assert f.expected_update_cadence is None
    assert _grade(_et(8, 0), _et(16, 15, 20)).market_session_status == "pre-market"


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    ["/api/option/quote", "/api/option/contract", "/api/market/open-interest"],
)
def test_every_chain_backed_endpoint_gets_the_chain_window(path):
    assert fr.resolve_profile(path) is fr.OPTION_CHAIN


def test_open_interest_is_matched_before_the_market_family():
    """It is an option_chains read wearing a /api/market/ path, so its registry
    entry has to precede the `/api/market/*` glob or it is unreachable."""
    patterns = [p for p, _ in fr.ENDPOINT_CADENCE]
    assert patterns.index("/api/market/open-interest*") < patterns.index("/api/market/*")


def test_the_option_calculator_is_not_swept_up():
    """It computes from caller-supplied inputs and has no feed at all."""
    assert fr.resolve_profile("/api/tools/option-calculator") is fr.ON_DEMAND


# ---------------------------------------------------------------------------
# One definition of "when should a chain row exist"
# ---------------------------------------------------------------------------


def test_the_window_comes_from_the_calendar_not_a_second_copy(
    monkeypatch: pytest.MonkeyPatch,
):
    """The whole point of the split is that the API and the ingestion alerting
    agree. A local re-implementation of 09:30-16:15 would pass every test above
    and still drift the day someone retunes the shared helper, so drive the
    grading through a calendar that has been moved and prove it followed.
    """
    monkeypatch.setattr(mc, "option_chain_feed_expected", lambda dt, symbol=None: False)
    assert _healthy(_et(12, 0)).freshness_status is fr.FreshnessStatus.SESSION_CLOSED

    # And the other direction, at an hour the real calendar calls closed but
    # market_context still grades (pre-market is an extended-hours session).
    monkeypatch.setattr(mc, "option_chain_feed_expected", lambda dt, symbol=None: True)
    assert _grade(_et(8, 0), _et(6, 0)).freshness_status is fr.FreshnessStatus.STALE


def test_a_broken_calendar_fails_open(monkeypatch: pytest.MonkeyPatch):
    """A freshness envelope must never hide an outage behind its own bug. If
    the calendar cannot be consulted, grade the payload rather than declaring
    no update due."""

    def boom(dt, symbol=None):
        raise RuntimeError("calendar unavailable")

    monkeypatch.setattr(mc, "option_chain_feed_expected", boom)
    assert _grade(_et(13, 0), _et(11, 0)).freshness_status is fr.FreshnessStatus.STALE


def test_the_symbol_reaches_the_chain_gate_through_a_real_request(
    monkeypatch: pytest.MonkeyPatch,
):
    """SPX and SPY differ by fifteen minutes, which is worth nothing if the
    symbol never arrives. Asserting on the helper alone is vacuous — the
    wrapper could stop passing it and the test above would stay green — so
    spy on the real call through a mounted request.
    """
    from fastapi import FastAPI

    seen: list = []
    real = fr.option_chain_market_day

    def spy(now, symbol=None):
        seen.append(symbol)
        return real(now, symbol)

    monkeypatch.setattr(fr, "option_chain_market_day", spy)

    app = FastAPI()

    @app.get("/api/option/quote")
    async def quote(underlying: str = "SPY"):
        return {"underlying": underlying, "quote_timestamp": "2026-08-21T17:00:00+00:00"}

    v2mod.mount_v2(app)
    with TestClient(app) as c:
        assert c.get("/api/v2/option/quote?underlying=SPX").status_code == 200

    assert seen == ["SPX"], f"symbol did not reach the chain gate: {seen}"
