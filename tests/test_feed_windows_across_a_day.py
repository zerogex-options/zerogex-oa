"""Sweep every feed-backed endpoint across a whole day, at every clock.

Four false-``stale`` bugs shipped here in a row, and not one was caught
before production:

  1. ``realtime_quote`` advertised the 5 s poll rate instead of the 60 s
     storage bucket — a healthy tape read stale for 65% of every minute.
  2. Every feed window opened straight into ``stale``: ``stale_after`` was
     anchored to an observation that, one second into a new window, still
     sits in the PREVIOUS one.
  3. ES/NQ were graded on the NYSE calendar — wrong for every hour CME
     trades and NYSE does not.
  4. Option chains were graded on the underlying's 04:00-20:00 window
     instead of the 09:30-16:15 options session — 9h15m wrong per weekday.

They share only one thing: *when* they were wrong. Every smoke run and every
``make api-test`` landed outside market hours, where ``session_closed`` masks
all four, so a green board meant nothing. This file removes the need to wait
for the right hour — it sweeps a synthetic day at 15-minute resolution, on a
normal weekday, an early-close day and a Saturday.

**Everything it grades against is stated here as an independent fact.** That
is the whole design. The obvious version of this test — "an observation one
advertised cadence old must not be stale" — is self-referential: it takes
the number under test as the definition of healthy, so bug 1 sails straight
through it (5 s is not stale under a 5 s cadence; the point was that a
healthy tape is 60 s old). ``GROUND_TRUTH`` below therefore states, per feed
and from outside the code: the window it writes in, and the oldest a healthy
observation can be. If a sweep here fails, the disagreement IS the finding —
do not resolve it by reading the expectation back out of the profile.

Each of the four bugs above is reintroduced as a mutation in review and must
turn this file red.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

import pytest

from src.api import freshness as fr

ET = fr._ET

WEEKDAY = date(2026, 8, 21)  # Friday
HALF_DAY = date(2026, 11, 27)  # day after Thanksgiving, 13:00 close
WEEKEND = date(2026, 8, 22)  # Saturday


@dataclass(frozen=True)
class Feed:
    """What a feed really does, stated independently of the code under test.

    ``window`` — ET (open, close) it writes rows in on a normal weekday.
    ``granularity`` — the oldest a HEALTHY observation can be, in seconds:
    the storage bucket or bar interval, not the poll rate. This is the number
    bug 1 got wrong, and stating it here is what makes the sweep able to
    notice.
    """

    window: tuple[time, time]
    granularity: float


# Sources for each figure, so a future reader can re-derive rather than trust:
#   underlying bars  04:00-20:00  TradeStation USEQ24Hour delivery window;
#                    60 s         _store_underlying floors to
#                                 AGGREGATION_BUCKET_SECONDS
#   option chains    09:30-16:15  options trade the cash session plus the
#                                 late 15 minutes; no tick, no row
#                    60 s         same bucket as the tape
#   VIX/VXN          04:00-20:00  CBOE index feed
#                    300 s        VOLATILITY_BAR_INTERVAL, 5-minute bars
#   analytics        04:00-20:00  engine recomputes off the live tape
#                    60 s         ANALYTICS_INTERVAL
#   flow             09:30-16:00  accrued over the cash session only
#                    300 s        flow_by_contract rows are keyed to 5-minute
#                                 bucket starts (database.get_flow and
#                                 get_flow_series both floor to 300), NOT to
#                                 AGGREGATION_BUCKET_SECONDS
#   signals          04:00-20:00  engine runs while its analytics inputs move
#                    60 s         a scored row carries uq.timestamp — the
#                                 newest underlying_quotes row, floored to the
#                                 60 s bucket — so it can never be fresher
GROUND_TRUTH = {
    "realtime_quote": Feed((time(4, 0), time(20, 0)), 60.0),
    "option_chain": Feed((time(9, 30), time(16, 15)), 60.0),
    "volatility_bar": Feed((time(4, 0), time(20, 0)), 300.0),
    "analytics_cycle": Feed((time(4, 0), time(20, 0)), 60.0),
    "flow_aggregate": Feed((time(9, 30), time(16, 0)), 300.0),
    "signals_cycle": Feed((time(4, 0), time(20, 0)), 60.0),
}

# One representative REAL path per profile, resolved through the registry.
# Without these the sweeps would prove each profile behaves per its declared
# window and say nothing about whether an endpoint is on the right profile —
# and that routing question is exactly where bug 4 lived.
REPRESENTATIVE_PATHS = {
    "/api/market/quote": "realtime_quote",
    "/api/option/quote": "option_chain",
    "/api/option/contract": "option_chain",
    "/api/market/open-interest": "option_chain",
    "/api/market/volatility": "volatility_bar",
    "/api/gex/summary": "analytics_cycle",
    "/api/flow/series": "flow_aggregate",
    "/api/signals/score": "signals_cycle",
}


def _sweep(day: date):
    """Every quarter hour of ``day``, as UTC instants."""
    for minutes in range(0, 24 * 60, 15):
        naive = datetime(day.year, day.month, day.day) + timedelta(minutes=minutes)
        yield ET.localize(naive).astimezone(timezone.utc)


def _et_at(day: date, t: time):
    return ET.localize(datetime.combine(day, t)).astimezone(timezone.utc)


def _feed_backed_profiles():
    for profile in fr.CADENCE_PROFILES.values():
        # daily_cycle ages in trading sessions rather than wall-clock seconds;
        # historical/on_demand are not feed-backed at all.
        if not profile.feed_backed or profile.session_scoped:
            continue
        yield profile


def _window_on(profile_name: str, day: date) -> tuple[time, time] | None:
    """The feed's window on ``day``, or None when it writes nothing."""
    if day.weekday() > 4:
        return None
    open_t, close_t = GROUND_TRUTH[profile_name].window
    if day == HALF_DAY:
        # Everything stops at the 13:00 early close, extended hours included.
        return (open_t, min(close_t, time(13, 0))) if open_t < time(13, 0) else None
    return open_t, close_t


@pytest.fixture(autouse=True)
def _half_day_calendar(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("src.market_calendar.NYSE_HALF_DAYS", {HALF_DAY})


def test_ground_truth_covers_every_feed_backed_profile():
    """A new profile must be given a feed here, or it sweeps unchecked —
    which is precisely how option_chain shipped on the tape's window."""
    assert {p.name for p in _feed_backed_profiles()} == set(GROUND_TRUTH)


def test_every_profile_has_a_representative_route():
    """And it must be reachable, or the routing sweeps below skip it."""
    assert set(REPRESENTATIVE_PATHS.values()) == set(GROUND_TRUTH)


@pytest.mark.parametrize("path,expected", sorted(REPRESENTATIVE_PATHS.items()))
def test_each_endpoint_resolves_to_the_profile_matching_its_feed(path, expected):
    assert fr.resolve_profile(path).name == expected


# ---------------------------------------------------------------------------
# Invariant 1 — a healthy feed is never stale, at any clock
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("day", [WEEKDAY, HALF_DAY, WEEKEND], ids=["weekday", "half", "weekend"])
@pytest.mark.parametrize("path,name", sorted(REPRESENTATIVE_PATHS.items()))
def test_a_healthy_feed_is_never_stale(path, name, day):
    """Graded against the feed's real STORAGE GRANULARITY, not the cadence
    the profile advertises. That distinction is bug 1: a tape polled every
    5 s but written in 60 s buckets is 60 s old at its freshest, and a
    profile advertising the poll rate called it stale for most of every
    minute — while a cadence-relative version of this test stayed green.
    """
    profile = fr.resolve_profile(path)
    window = _window_on(name, day)
    granularity = GROUND_TRUTH[name].granularity
    for now in _sweep(day):
        et_t = now.astimezone(ET).time()
        if window is not None and window[0] <= et_t < window[1]:
            source = now - timedelta(seconds=granularity)
        else:
            # Outside the window the newest row that can exist is the last one
            # written before the feed stopped.
            last_day = day if window is not None and et_t >= window[1] else day - timedelta(days=1)
            close_t = (_window_on(name, last_day) or GROUND_TRUTH[name].window)[1]
            source = _et_at(last_day, close_t) - timedelta(seconds=granularity)
        status = fr.build_freshness(
            {"timestamp": source}, profile=profile, now=now, symbol="SPY"
        ).freshness_status
        assert status is not fr.FreshnessStatus.STALE, (
            f"{path} at {now.astimezone(ET):%a %H:%M ET}: graded stale on a healthy "
            f"{name} feed whose freshest possible row is {granularity:.0f}s old "
            f"(window {window}, observation {source.astimezone(ET):%a %H:%M ET})"
        )


def test_a_window_does_not_open_straight_into_stale():
    """Bug 2, and the reason ``feed_opens_et`` exists.

    One minute into a new window the newest row is still the last of the
    PREVIOUS window — hours old, because nothing has had time to arrive yet.
    Anchoring ``stale_after`` to that observation alone flips every feed-backed
    endpoint session_closed -> stale in one second, with the whole ``aging``
    grace band unreachable at exactly the boundary it exists for.
    """
    for path, name in sorted(REPRESENTATIVE_PATHS.items()):
        profile = fr.resolve_profile(path)
        open_t, close_t = GROUND_TRUTH[name].window
        yesterdays_last = _et_at(WEEKDAY - timedelta(days=1), close_t) - timedelta(minutes=1)
        for offset in (0, 1, 2):
            now = _et_at(WEEKDAY, open_t) + timedelta(minutes=offset)
            status = fr.build_freshness(
                {"timestamp": yesterdays_last}, profile=profile, now=now, symbol="SPY"
            ).freshness_status
            assert status is not fr.FreshnessStatus.STALE, (
                f"{path} is stale {offset} min into its window: nothing can be "
                f"late yet, because nothing has had time to arrive"
            )


# ---------------------------------------------------------------------------
# Invariant 2 — a cadence is advertised exactly while the feed writes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("day", [WEEKDAY, HALF_DAY, WEEKEND], ids=["weekday", "half", "weekend"])
@pytest.mark.parametrize("path,name", sorted(REPRESENTATIVE_PATHS.items()))
def test_a_cadence_is_advertised_exactly_while_the_feed_writes(path, name, day):
    """Advertising a cadence outside the feed's real window is the false-stale
    bug — we demand updates nothing can supply. Withholding one inside it is
    the blind spot: a dead feed reads `session_closed`."""
    profile = fr.resolve_profile(path)
    window = _window_on(name, day)
    for now in _sweep(day):
        et_t = now.astimezone(ET).time()
        # The closing INSTANT is not asserted: the system holds two
        # conventions there — market_context treats a close as exclusive
        # (20:00:00 is already `closed`) while option_chain_feed_expected is
        # inclusive (16:15:00 still counts). The difference is one second
        # wide, unobservable to a consumer, and not the shape of any bug here.
        if window is not None and et_t == window[1]:
            continue
        due = window is not None and window[0] <= et_t <= window[1]
        advertised = (
            fr.build_freshness(
                {"timestamp": now}, profile=profile, now=now, symbol="SPY"
            ).expected_update_cadence_seconds
            is not None
        )
        if advertised != due:
            fault = (
                "advertises a cadence but the feed writes nothing then"
                if advertised
                else "expects nothing while the feed is writing"
            )
            raise AssertionError(
                f"{path} at {now.astimezone(ET):%a %H:%M ET}: {fault} " f"(feed window {window})"
            )


def test_a_dead_feed_is_still_caught_at_every_in_window_clock():
    """Narrowing a window is not the same as suppressing a signal: inside its
    own window, a feed silent for an hour must read stale at every tick."""
    for path, name in sorted(REPRESENTATIVE_PATHS.items()):
        profile = fr.resolve_profile(path)
        window = _window_on(name, WEEKDAY)
        assert window is not None
        first = (datetime.combine(WEEKDAY, window[0]) + timedelta(hours=1)).time()
        for now in _sweep(WEEKDAY):
            et_t = now.astimezone(ET).time()
            if not (first <= et_t < window[1]):
                continue
            status = fr.build_freshness(
                {"timestamp": now - timedelta(hours=1)},
                profile=profile,
                now=now,
                symbol="SPY",
            ).freshness_status
            assert status is fr.FreshnessStatus.STALE, (
                f"{path} at {now.astimezone(ET):%a %H:%M ET}: a feed silent for "
                f"an hour inside its own window reads {status.value}"
            )


# ---------------------------------------------------------------------------
# The same sweep on the CME calendar
# ---------------------------------------------------------------------------

# ES/NQ trade Sun 18:00 -> Fri 17:00 ET with a 17:00-18:00 maintenance break,
# and are served from futures_quotes on the same 60 s bucket. Stated here
# independently of is_futures_session_open, which is the code under test.
FUTURES_GRANULARITY = 60.0


def _cme_open(now: datetime) -> bool:
    et = now.astimezone(ET)
    wd, t = et.weekday(), et.time()
    if time(17, 0) <= t < time(18, 0):
        return False  # daily maintenance break
    if wd == 5:
        return False  # Saturday
    if wd == 6:
        return t >= time(18, 0)  # Sunday reopen
    if wd == 4:
        return t < time(17, 0)  # Friday close
    return True


@pytest.mark.parametrize("symbol", ["ES", "NQ"])
@pytest.mark.parametrize(
    "day",
    [WEEKDAY, WEEKEND, date(2026, 8, 23), date(2026, 8, 24)],
    ids=["friday", "saturday", "sunday", "monday"],
)
def test_futures_are_graded_on_the_cme_session_not_the_nyse_one(symbol, day):
    """Bug 3. For every hour CME trades and NYSE does not, the NYSE calendar
    reported `session_closed` — so a dead overnight futures feed was invisible
    while the v1 body beside it said `stale: true, data_age_seconds: 2701`.
    """
    profile = fr.resolve_profile("/api/market/quote")
    for now in _sweep(day):
        trading = _cme_open(now)
        f = fr.build_freshness(
            {"timestamp": now - timedelta(seconds=FUTURES_GRANULARITY)},
            profile=profile,
            now=now,
            symbol=symbol,
        )
        assert (f.expected_update_cadence_seconds is not None) == trading, (
            f"{symbol} at {now.astimezone(ET):%a %H:%M ET}: CME is "
            f"{'open' if trading else 'shut'} but the envelope "
            f"{'expects nothing' if trading else 'expects an update'}"
        )
        if trading:
            # And a healthy overnight feed must not read stale...
            assert f.freshness_status is not fr.FreshnessStatus.STALE
            # ...while a dead one must, once a grace period has elapsed since
            # the 18:00 reopen. At the reopen instant itself `aging` is the
            # right answer for the same reason it is at 04:00 on the cash
            # side: nothing can be late yet, because the session just began.
            if time(18, 0) <= now.astimezone(ET).time() < time(19, 0):
                continue
            dead = fr.build_freshness(
                {"timestamp": now - timedelta(hours=1)},
                profile=profile,
                now=now,
                symbol=symbol,
            )
            assert dead.freshness_status is fr.FreshnessStatus.STALE, (
                f"{symbol} at {now.astimezone(ET):%a %H:%M ET}: a futures feed "
                f"silent for an hour reads {dead.freshness_status.value}"
            )


# ---------------------------------------------------------------------------
# Invariant 3 — never promise faster than the feed can store
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path,name", sorted(REPRESENTATIVE_PATHS.items()))
def test_a_cadence_is_never_faster_than_the_feed_can_store(path, name):
    """The root cause of this whole family, stated directly.

    ``expected_update_cadence`` is a published contract: "a new observation is
    due this often". Advertising a number smaller than the storage granularity
    promises something the feed structurally cannot deliver, so a healthy feed
    spends most of every bucket past its own advertised cadence — reading
    ``aging`` at best and ``stale`` at worst, forever, with nothing wrong.

    Invariants 1 and 2 both miss this. Invariant 1 only asks "not stale", so a
    profile permanently parked in ``aging`` passes; and when the granularity
    happens to equal the stale window it passes by a single second of margin.
    This asks the question the other two dance around.
    """
    profile = fr.resolve_profile(path)
    granularity = GROUND_TRUTH[name].granularity
    for session, advertised in (
        (fr.SESSION_REGULAR, profile.regular_seconds),
        (fr.SESSION_PRE_MARKET, profile.extended_seconds),
    ):
        if advertised is None:
            continue  # the feed writes nothing in that session — invariant 2
        assert advertised >= granularity, (
            f"{path} advertises a {advertised:.0f}s cadence in the {session} "
            f"session, but its feed stores at best every {granularity:.0f}s. A "
            f"healthy feed is past that cadence for "
            f"{100 * (1 - advertised / granularity):.0f}% of every bucket."
        )


def test_the_flow_bar_constant_matches_what_the_queries_actually_floor_to():
    """``FLOW_BAR_SECONDS`` is published as a cadence contract, but the value
    that decides where a flow row really lands is the divisor inside the
    queries. They were written as a bare ``300`` long before the envelope
    existed, so naming the constant did not join them up — retune one and the
    API would advertise a cadence the stored rows do not keep, which is the
    bug this file exists to prevent.
    """
    from pathlib import Path

    from src.config import FLOW_BAR_SECONDS

    src = Path("src/api/database.py").read_text()
    # get_flow aligns its window to the bucket grid; get_flow_series sizes and
    # floors its bars on the same number.
    assert f"bucket_seconds = {FLOW_BAR_SECONDS}" in src, (
        "get_flow no longer floors to FLOW_BAR_SECONDS — the advertised flow "
        "cadence and the stored bucket grid have come apart"
    )
    assert src.count(f"// {FLOW_BAR_SECONDS}) * {FLOW_BAR_SECONDS}") >= 2, (
        "get_flow_series no longer floors its bar boundaries to " "FLOW_BAR_SECONDS"
    )


def test_the_signal_score_cannot_outrun_the_bucket_it_reads():
    """Why signals_cycle is pinned to the tape bucket rather than the engine's
    loop interval: a score's timestamp IS the underlying-quote timestamp, and
    those are floored to AGGREGATION_BUCKET_SECONDS. The engine may loop every
    second; the observation it stamps cannot move faster than its input."""
    from pathlib import Path

    from src.config import AGGREGATION_BUCKET_SECONDS

    engine = Path("src/signals/unified_signal_engine.py").read_text()
    assert '"timestamp": ts,' in engine
    assert "uq.timestamp," in engine, (
        "the score context no longer reads its timestamp from underlying_quotes "
        "— re-derive the signals granularity before trusting this cadence"
    )
    assert fr.SIGNALS_CYCLE.regular_seconds == float(AGGREGATION_BUCKET_SECONDS)
