"""Per-symbol freshness detection: the check the QQQ outage did not have.

One ingestion worker (one child process per symbol) died at the Monday
session close and was never restarted, so QQQ wrote nothing until an operator
noticed by eye ~6 h into Tuesday's session. Every existing check asks "is the
process up" -- the parent was, so systemd, ``Restart=always``, ``OnFailure``
and the ``systemctl is-active`` liveness watchdog all reported healthy. None
asked "is the data arriving", which is the only question that distinguishes a
single dead worker from healthy siblings.

These tests pin two things:

* **classification** -- stale inside the cash session alerts; silence outside
  it, and in extended hours where the vendor's own delivery is discontinuous,
  never does; and the staleness clock anchors at the regular OPEN so the first
  check after it doesn't report the overnight gap as a fault;
* **paging** -- a stale symbol pages on the edge and then at most once per
  re-notify interval, not once per 10-minute run for the whole outage.
"""

import json
from datetime import datetime, timedelta

import pytz

from src.tools import ingestion_freshness_healthcheck as healthcheck
from src.tools.ingestion_freshness_healthcheck import evaluate, reconcile_alerts

ET = pytz.timezone("US/Eastern")
TEMPLATE = "USEQ24Hour"  # 04:00-20:00 ET, as production runs
MAX_STALE = timedelta(minutes=15)
EXTENDED_MAX_STALE = timedelta(minutes=90)  # illustrative opt-in value


def _et(y, m, d, hh, mm):
    return ET.localize(datetime(y, m, d, hh, mm))


# --- classification --------------------------------------------------------


def test_stale_symbol_inside_session_is_flagged():
    """QQQ's actual outage shape: mid-session, hours since the last bar."""
    now = _et(2026, 8, 18, 12, 0)  # Tuesday, well inside the cash session
    last_bar = _et(2026, 8, 17, 19, 59)  # Monday's final bar

    result = evaluate("QQQ", last_bar, now, TEMPLATE, MAX_STALE)

    assert result.status == "stale"
    # Anchored at Tuesday's 09:30 open, not Monday's last bar -- the gap that
    # matters is "2.5h into a session with nothing", not "16h since a bar".
    assert result.stale_minutes == 150.0


def test_fresh_symbol_passes():
    now = _et(2026, 8, 18, 12, 0)
    result = evaluate("QQQ", _et(2026, 8, 18, 11, 58), now, TEMPLATE, MAX_STALE)

    assert result.status == "fresh"
    assert result.stale_minutes == 2.0


def test_silence_outside_the_delivery_window_never_alerts():
    """02:00 ET: the feed is legitimately dark, so staleness is meaningless."""
    now = _et(2026, 8, 18, 2, 0)
    result = evaluate("QQQ", _et(2026, 8, 17, 19, 59), now, TEMPLATE, MAX_STALE)

    assert result.status == "not_expected"
    assert result.stale_minutes is None


def test_weekend_never_alerts():
    now = _et(2026, 8, 15, 12, 0)  # Saturday
    result = evaluate("QQQ", _et(2026, 8, 14, 19, 59), now, TEMPLATE, MAX_STALE)

    assert result.status == "not_expected"


def test_cash_index_premarket_is_not_expected():
    """SPX prints no underlying level before 09:30 even under a 24h template.

    Without the cash-index clamp this fires a false alarm every weekday from
    04:00 to 09:30 on SPX and NDX -- four hours of daily noise, which is how
    an alert gets muted and stops being a detector at all.
    """
    now = _et(2026, 8, 18, 7, 0)
    result = evaluate("SPX", _et(2026, 8, 17, 15, 59), now, TEMPLATE, MAX_STALE)

    assert result.status == "not_expected"

    # ...but the same symbol IS checked once the cash session is open.
    open_now = _et(2026, 8, 18, 11, 0)
    stale = evaluate("SPX", _et(2026, 8, 17, 15, 59), open_now, TEMPLATE, MAX_STALE)
    assert stale.status == "stale"


def test_after_hours_quiet_tape_does_not_alert():
    """The 2026-08-18 16:40 ET false alarm, verbatim.

    SPY and QQQ both stopped at 16:14 ET -- the same minute, 14 minutes past
    the cash close -- with their workers alive and supervised. Two independent
    child processes do not die on the same minute; the after-hours tape simply
    stopped printing. Under a flat 15-minute threshold across 04:00-20:00 this
    paged every 10 minutes until 20:00.
    """
    now = _et(2026, 8, 18, 16, 40)
    last_bar = _et(2026, 8, 18, 16, 14)

    result = evaluate("SPY", last_bar, now, TEMPLATE, MAX_STALE)

    assert result.status == "not_checked"
    assert result.stale_minutes is None


def test_premarket_vendor_gap_does_not_alert():
    """QQQ served no bars at all from 04:00 to 07:35 ET on 2026-08-18.

    probe_bar_availability got the same 07:35 first bar under every session
    template and both query modes, so that hole is in TradeStation's store,
    not in our request or our worker. A 15-minute threshold anchored at the
    04:00 open would have paged ~20 times before the first bar existed.
    """
    now = _et(2026, 8, 18, 7, 0)
    result = evaluate("QQQ", _et(2026, 8, 17, 19, 59), now, TEMPLATE, MAX_STALE)

    assert result.status == "not_checked"


def test_extended_hours_are_checked_when_a_threshold_is_configured():
    """The opt-in knob for a deployment whose extended-hours feed is denser."""
    now = _et(2026, 8, 18, 16, 40)
    last_bar = _et(2026, 8, 18, 16, 14)

    within = evaluate("SPY", last_bar, now, TEMPLATE, MAX_STALE, EXTENDED_MAX_STALE)
    assert within.status == "fresh"
    assert within.stale_minutes == 26.0

    # Past the wider threshold it still alerts -- opting in is not opting out.
    much_later = _et(2026, 8, 18, 19, 0)
    beyond = evaluate("SPY", last_bar, much_later, TEMPLATE, MAX_STALE, EXTENDED_MAX_STALE)
    assert beyond.status == "stale"


def test_session_open_grace_avoids_a_spurious_alert_at_the_open():
    """At 09:35 with only a sparse pre-market print, the gap is 5 min of open.

    Anchoring at the feed's 04:00 open instead would measure from whatever
    thin pre-market bar came last and page in the first minutes of every
    session -- the densest, least ambiguous part of the day.
    """
    now = _et(2026, 8, 18, 9, 35)
    result = evaluate("QQQ", _et(2026, 8, 18, 8, 15), now, TEMPLATE, MAX_STALE)

    assert result.status == "fresh"
    assert result.stale_minutes == 5.0


def test_worker_dead_overnight_is_caught_shortly_after_the_open():
    """What the extended-hours carve-out costs, and what it still catches.

    The original incident -- worker dead from Monday's close, nothing written
    all night -- is provable the moment the cash session has been open longer
    than the threshold. 09:45 ET, versus the 10:03 a human managed by eye.
    """
    now = _et(2026, 8, 18, 9, 46)
    result = evaluate("QQQ", _et(2026, 8, 17, 19, 59), now, TEMPLATE, MAX_STALE)

    assert result.status == "stale"
    assert result.stale_minutes == 16.0


def test_symbol_with_no_bars_at_all_is_measured_from_the_open():
    """A worker that never started writing still gets caught, not skipped."""
    now = _et(2026, 8, 18, 12, 0)
    result = evaluate("QQQ", None, now, TEMPLATE, MAX_STALE)

    assert result.status == "stale"
    assert result.stale_minutes == 150.0
    assert result.last_bar is None


# --- paging ----------------------------------------------------------------

RENOTIFY = timedelta(minutes=60)


def test_first_stale_run_pages():
    state, to_alert, recovered = reconcile_alerts({}, ["QQQ"], _et(2026, 8, 18, 10, 0), RENOTIFY)

    assert to_alert == ["QQQ"]
    assert recovered == []
    assert state["QQQ"]["since"] == _et(2026, 8, 18, 10, 0).isoformat()


def test_a_sustained_outage_does_not_page_every_run():
    """The complaint this exists to fix: 6 emails an hour until someone acts."""
    now = _et(2026, 8, 18, 10, 0)
    state, _, _ = reconcile_alerts({}, ["QQQ"], now, RENOTIFY)

    for minutes in (10, 20, 30, 40, 50):
        state, to_alert, _ = reconcile_alerts(
            state, ["QQQ"], now + timedelta(minutes=minutes), RENOTIFY
        )
        assert to_alert == [], f"re-paged at +{minutes} min"

    # ...and the episode's start survives every suppressed run, so the page
    # that does come says how long it has really been going.
    assert state["QQQ"]["since"] == now.isoformat()


def test_a_sustained_outage_pages_again_after_the_renotify_interval():
    now = _et(2026, 8, 18, 10, 0)
    state, _, _ = reconcile_alerts({}, ["QQQ"], now, RENOTIFY)

    state, to_alert, _ = reconcile_alerts(state, ["QQQ"], now + RENOTIFY, RENOTIFY)

    assert to_alert == ["QQQ"]
    assert state["QQQ"]["since"] == now.isoformat()  # episode start, not the re-page
    assert state["QQQ"]["last_alert"] == (now + RENOTIFY).isoformat()


def test_recovery_clears_the_episode_so_the_next_one_pages_immediately():
    now = _et(2026, 8, 18, 10, 0)
    state, _, _ = reconcile_alerts({}, ["QQQ"], now, RENOTIFY)

    state, to_alert, recovered = reconcile_alerts(state, [], now + timedelta(minutes=10), RENOTIFY)
    assert recovered == ["QQQ"]
    assert to_alert == []
    assert state == {}

    # A fresh stall inside what would have been the quiet period still pages.
    _, to_alert, _ = reconcile_alerts(state, ["QQQ"], now + timedelta(minutes=20), RENOTIFY)
    assert to_alert == ["QQQ"]


def test_each_symbol_is_deduped_independently():
    """A second symbol going dark during the first one's quiet period must
    still page -- that is a new fact, not a repeat of the one already sent."""
    now = _et(2026, 8, 18, 10, 0)
    state, _, _ = reconcile_alerts({}, ["QQQ"], now, RENOTIFY)

    state, to_alert, _ = reconcile_alerts(
        state, ["QQQ", "SPY"], now + timedelta(minutes=10), RENOTIFY
    )

    assert to_alert == ["SPY"]
    assert set(state) == {"QQQ", "SPY"}


def test_unreadable_state_entry_pages_rather_than_going_silent():
    """A truncated or hand-edited timestamp must fail toward alerting."""
    now = _et(2026, 8, 18, 10, 0)
    state = {"QQQ": {"since": "not-a-timestamp", "last_alert": "not-a-timestamp"}}

    _, to_alert, _ = reconcile_alerts(state, ["QQQ"], now, RENOTIFY)

    assert to_alert == ["QQQ"]


def test_database_errors_are_deduped_too(tmp_path, monkeypatch):
    """A DB outage lasts as long as it lasts; six emails an hour helps nobody.

    Exercises the real ``main`` I/O path -- state file written, reloaded and
    honoured -- which ``reconcile_alerts`` alone does not cover.
    """
    monkeypatch.setenv("INGEST_UNDERLYINGS", "SPY")
    monkeypatch.setattr(
        healthcheck,
        "_fetch_last_bars",
        lambda symbols: (_ for _ in ()).throw(RuntimeError("connection refused")),
    )
    state = str(tmp_path / "nested" / "freshness.json")  # the dir is created on demand

    assert healthcheck.main(["--state-file", state]) == 2  # the edge pages
    assert healthcheck.main(["--state-file", state]) == 0  # ...the next run does not
    assert set(json.load(open(state))) == {"database"}

    # And with no state file a hand run reports the failure every time.
    assert healthcheck.main([]) == 2
    assert healthcheck.main([]) == 2
