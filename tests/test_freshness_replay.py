"""The auditor needs auditing: a replay that cannot fail is worthless.

Its whole job is to answer "would this window have paged, on real data" —
so the tests feed it days whose answer is known by construction, including
the one shape a naive replay gets wrong: a window narrowed until it never
grades anything, which produces zero pages and looks like a pass.
"""

import datetime as dt

import pytz

from src.tools.freshness_replay import FeedSpec, _last_write_at, replay_day
from src.tools.ingestion_freshness_healthcheck import FEED_CHAINS, FEED_UNDERLYING

ET = pytz.timezone("US/Eastern")
MAX_STALE = dt.timedelta(minutes=15)
MONDAY = dt.date(2026, 8, 31)


def _et(hh, mm, day=MONDAY):
    return ET.localize(dt.datetime.combine(day, dt.time(hh, mm)))


def _every_minute(start_hh, start_mm, end_hh, end_mm, day=MONDAY):
    """A healthy 1-minute feed between two ET times."""
    out, t, end = [], _et(start_hh, start_mm, day), _et(end_hh, end_mm, day)
    while t <= end:
        out.append(t)
        t += dt.timedelta(minutes=1)
    return out


def _replay(specs, **kw):
    return replay_day(specs, MONDAY, "USEQ24Hour", MAX_STALE, **kw)


# --- the bug this shipped to fix -------------------------------------------


def test_chains_writing_only_in_the_options_session_never_page():
    """The production shape: dense 09:30-16:15, silent either side."""
    spec = FeedSpec(FEED_CHAINS, "QQQ", _every_minute(9, 30, 16, 15), "chains")
    pages, graded = _replay([spec])
    assert pages == []
    assert graded[(FEED_CHAINS, "QQQ")] > 0, "graded nothing — blind, not clean"


def test_the_same_day_pages_under_the_legacy_window():
    """Same data, old rule: the pre-market and evening tails both alert."""
    spec = FeedSpec(FEED_CHAINS, "QQQ", _every_minute(9, 30, 16, 15), "chains")
    pages, _ = _replay([spec], legacy_chain_window=True)
    assert pages, "legacy rule must reproduce the pages that prompted the fix"
    hours = {p.at.hour for p in pages}
    assert any(h < 9 for h in hours), "missed the pre-market tail"
    assert any(h >= 16 for h in hours), "missed the evening tail"


# --- the failure the check must keep catching ------------------------------


def test_a_chain_stream_dead_mid_session_still_pages():
    spec = FeedSpec(FEED_CHAINS, "QQQ", _every_minute(9, 30, 11, 0), "chains")
    pages, _ = _replay([spec])
    assert pages
    assert min(p.at for p in pages).strftime("%H:%M") == "11:20"
    assert all(p.feed == FEED_CHAINS for p in pages)


def test_a_bar_stream_dead_mid_session_still_pages():
    """Narrowing the chain window must not touch the bar feed."""
    spec = FeedSpec(FEED_UNDERLYING, "SPY", _every_minute(4, 0, 10, 0), "bars")
    pages, _ = _replay([spec])
    assert pages
    assert min(p.at for p in pages).strftime("%H:%M") == "10:20"


def test_a_stream_that_never_wrote_at_all_pages():
    """No rows anywhere: anchored at the session open, it must not read fresh."""
    pages, graded = _replay([FeedSpec(FEED_CHAINS, "QQQ", [], "chains")])
    assert pages
    assert pages[0].last_write is None
    assert graded[(FEED_CHAINS, "QQQ")] > 0


# --- the guard that separates "clean" from "blind" -------------------------


def test_graded_tick_count_exposes_a_window_that_checks_nothing():
    """A weekend has no graded ticks and no pages. Zero pages alone is not a
    pass, which is exactly why replay_day reports both."""
    saturday = dt.date(2026, 8, 29)
    spec = FeedSpec(FEED_CHAINS, "QQQ", [], "chains")
    pages, graded = replay_day([spec], saturday, "USEQ24Hour", MAX_STALE)
    assert pages == []
    assert graded[(FEED_CHAINS, "QQQ")] == 0


# --- the primitive everything rests on -------------------------------------


def test_last_write_at_reads_what_the_check_would_have_read():
    writes = [_et(9, 30), _et(9, 40), _et(9, 50)]
    assert _last_write_at(writes, _et(9, 45)) == _et(9, 40)
    assert _last_write_at(writes, _et(9, 40)) == _et(9, 40), "inclusive at the tick"
    assert _last_write_at(writes, _et(9, 0)) is None
    assert _last_write_at([], _et(9, 45)) is None
