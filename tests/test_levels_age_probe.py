"""Tests for the levels age probe.

The probe exists to answer a tester's "is it the polling?" with numbers, so
the parts that turn responses into numbers are pure functions, tested for
what they compute rather than for merely running. The loop is tested with
an injected fetch and clock so a full "hour" takes no time.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from src.tools import levels_age_probe as probe

T0 = datetime(2026, 9, 4, 14, 30, 0, tzinfo=timezone.utc)


def _z(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _v1_body(as_of: datetime, now: datetime) -> dict:
    return {"symbol": "NQ", "as_of": _z(as_of), "age_seconds": int((now - as_of).total_seconds())}


def _v2_body(as_of: datetime, now: datetime, closed: bool = False) -> dict:
    return {
        "data": _v1_body(as_of, now),
        "freshness": {
            "evaluated_at": _z(now),
            "generated_at": _z(as_of),
            "source_timestamp": _z(as_of),
            "age_seconds": (now - as_of).total_seconds(),
            "cadence_profile": "analytics_cycle",
            "market_session_status": "closed" if closed else "regular",
            "freshness_status": "session_closed" if closed else "fresh",
        },
    }


class FakeClock:
    """A clock that only moves when the probe sleeps."""

    def __init__(self, start: datetime):
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


class FakeServer:
    """Publishes a new snapshot every ``period`` seconds, each born
    ``publish_lag`` seconds old -- the shape the engine actually produces."""

    def __init__(
        self,
        clock: FakeClock,
        period: float,
        publish_lag: float,
        v2: bool = True,
        closed: bool = False,
    ):
        self.clock = clock
        self.period = period
        self.publish_lag = publish_lag
        self.v2 = v2
        self.closed = closed
        self.urls: List[str] = []

    def visible_as_of(self) -> datetime:
        if self.closed:
            return T0 - timedelta(days=2, hours=21)  # Friday's close, seen on a holiday
        # Snapshot stamped S is visible from S + publish_lag onward.
        elapsed = (self.clock.now - T0).total_seconds() - self.publish_lag
        cycles = int(elapsed // self.period)
        return T0 + timedelta(seconds=cycles * self.period)

    def __call__(self, url: str) -> Tuple[int, Optional[dict], Optional[str]]:
        self.urls.append(url)
        now = self.clock.now
        if "/api/v2/" in url:
            if not self.v2:
                return 404, None, None
            return 200, _v2_body(self.visible_as_of(), now, closed=self.closed), "MISS"
        return 200, _v1_body(self.visible_as_of(), now), "MISS"


# --- parsing --------------------------------------------------------------


def test_parse_iso_accepts_trailing_z_and_offsets():
    assert probe.parse_iso("2026-09-04T14:30:00Z") == T0
    assert probe.parse_iso("2026-09-04T10:30:00-04:00") == T0
    assert probe.parse_iso("2026-09-04T14:30:00").tzinfo is timezone.utc


def test_v2_sample_uses_the_server_clock_and_the_envelope_age():
    server_now = T0 + timedelta(seconds=70)
    laptop_now = server_now + timedelta(seconds=9)  # a laptop clock nine seconds fast
    sample = probe.take_sample(_v2_body(T0, server_now), laptop_now, 2, previous=None)
    assert sample.evaluated_at == server_now
    assert sample.as_of == T0
    assert sample.age_seconds == 70.0
    assert sample.advanced is False
    assert (sample.session, sample.freshness) == ("regular", "fresh")


def test_v1_sample_falls_back_to_the_body_and_the_client_clock():
    now = T0 + timedelta(seconds=45)
    sample = probe.take_sample(_v1_body(T0, now), now, 1, previous=None)
    assert sample.evaluated_at == now
    assert sample.age_seconds == 45.0


def test_a_sample_is_flagged_when_as_of_moves():
    first = probe.take_sample(_v1_body(T0, T0), T0, 1, previous=None)
    same = probe.take_sample(_v1_body(T0, T0), T0, 1, previous=first)
    moved = probe.take_sample(_v1_body(T0 + timedelta(seconds=60), T0), T0, 1, previous=same)
    assert not same.advanced
    assert moved.advanced


# --- the numbers ----------------------------------------------------------


def test_summarize_measures_the_cycle_period_and_the_age_at_publish():
    clock = FakeClock(T0)
    server = FakeServer(clock, period=60.0, publish_lag=35.0)
    samples, code = probe.run_probe(
        fetch=server,
        base_url="https://x",
        symbol="NQ",
        interval=5.0,
        duration_seconds=240.0,
        clock=clock,
        sleep=clock.sleep,
        log=lambda _m: None,
    )
    assert code == 0
    summary = probe.summarize(samples)

    # Snapshots become visible at T0+35, +95, +155, +215: the one visible at
    # T0 itself was joined mid-cycle and must not count as a publish.
    assert summary.period_seconds == [60.0, 60.0, 60.0, 60.0]
    assert summary.publish_age_seconds == [35.0, 35.0, 35.0, 35.0]
    assert summary.snapshots == 5
    # A random poll sees anywhere from the publish age up to one period more.
    assert min(summary.ages) == 35.0
    assert max(summary.ages) == 90.0


def test_format_report_warns_when_too_few_snapshots_were_seen():
    one = probe.take_sample(_v1_body(T0, T0), T0, 1, previous=None)
    text = probe.format_report(probe.summarize([one]), interval=5.0)
    assert "fewer than three snapshots" in text
    assert "n/a" in text


def test_format_report_names_a_frozen_snapshot_for_what_it_is():
    frozen = [
        probe.take_sample(_v1_body(T0, T0 + timedelta(seconds=5 * i)), T0, 1, previous=None)
        for i in range(12)
    ]
    text = probe.format_report(probe.summarize(frozen), interval=5.0)
    assert "never advanced" in text
    assert "cash session" in text


def test_a_regression_is_counted_and_kept_out_of_the_cycle_statistics():
    """The live run served 13:41 for one sample after 13:42 had appeared.
    Folded into the periods that read as a -60s cycle and a 105s publish
    age; neither is a fact about the cycle."""
    m0, m1, m2 = T0, T0 + timedelta(seconds=60), T0 + timedelta(seconds=120)
    served = [(m0, 40), (m1, 40), (m0, 100), (m1, 50), (m2, 40)]
    samples: List[probe.Sample] = []
    prev = None
    for i, (as_of, age) in enumerate(served):
        now = as_of + timedelta(seconds=age)
        sample = probe.take_sample(_v1_body(as_of, now), T0 + timedelta(seconds=5 * i), 1, prev)
        samples.append(sample)
        prev = sample
    assert [s.regressed for s in samples] == [False, False, True, False, False]
    summary = probe.summarize(samples)
    assert summary.regressions == 1
    assert summary.snapshots == 3
    assert summary.period_seconds == [60.0, 60.0]
    assert summary.publish_age_seconds == [40.0, 40.0]
    assert "BACKWARDS" in probe.format_report(summary, interval=5.0)
    assert "WENT BACKWARDS" in probe.format_sample(samples[2])


def test_a_rewrite_of_the_same_minute_is_marked_recomputed_not_new():
    body_a = {**_v1_body(T0, T0), "computed_at": _z(T0 + timedelta(seconds=40))}
    body_b = {**_v1_body(T0, T0), "computed_at": _z(T0 + timedelta(seconds=70))}
    first = probe.take_sample(body_a, T0, 1, previous=None)
    second = probe.take_sample(body_b, T0, 1, previous=first)
    assert not second.advanced and second.recomputed
    assert "RECOMPUTED" in probe.format_sample(second)
    same = probe.take_sample(body_b, T0, 1, previous=second)
    assert not same.recomputed


def test_fmt_age_reads_like_a_person_would():
    assert probe.fmt_age(63.24) == "63.2s"
    assert probe.fmt_age(750) == "12m 30s"
    assert probe.fmt_age(3 * 3600 + 5 * 60) == "3h 5m"
    assert probe.fmt_age(250617) == "2d 21h 36m"
    assert probe.fmt_age(None) == "-"
    assert probe.fmt_age(-60.0) == "-60.0s"


def test_should_print_keeps_the_first_the_advances_and_a_heartbeat():
    quiet = probe.take_sample(_v1_body(T0, T0), T0, 1, previous=None)
    moved = probe.take_sample(_v1_body(T0 + timedelta(seconds=60), T0), T0, 1, previous=quiet)
    assert probe.should_print(quiet, 0, 12, verbose=False)
    assert not probe.should_print(quiet, 1, 12, verbose=False)
    assert probe.should_print(quiet, 12, 12, verbose=False)
    assert probe.should_print(moved, 7, 12, verbose=False)
    assert probe.should_print(quiet, 7, 12, verbose=True)


def test_percentile_interpolates():
    assert probe.percentile([10.0, 20.0, 30.0, 40.0], 50) == 25.0
    assert probe.percentile([7.0], 90) == 7.0


def test_summary_json_is_null_where_there_is_nothing_to_summarize():
    out = probe.summary_json(probe.summarize([]), interval=5.0)
    assert out["cycle_period_seconds"] is None
    assert out["samples"] == 0


# --- the loop -------------------------------------------------------------


def test_the_probe_drops_to_v1_when_v2_is_not_deployed():
    clock = FakeClock(T0)
    server = FakeServer(clock, period=60.0, publish_lag=30.0, v2=False)
    samples, code = probe.run_probe(
        fetch=server,
        base_url="https://x",
        symbol="nq",
        interval=5.0,
        duration_seconds=10.0,
        clock=clock,
        sleep=clock.sleep,
        log=lambda _m: None,
    )
    assert code == 0
    assert all(s.api_version == 1 for s in samples)
    # One v2 attempt, then v1 for the rest; the symbol is upper-cased on the wire.
    assert server.urls[0].startswith("https://x/api/v2/levels/NQ?")
    assert all("/api/v1/levels/NQ?" in u for u in server.urls[1:])


def test_a_refused_key_ends_the_run_with_exit_2():
    samples, code = probe.run_probe(
        fetch=lambda _url: (401, None, None),
        base_url="https://x",
        symbol="NQ",
        interval=5.0,
        duration_seconds=600.0,
        clock=FakeClock(T0),
        sleep=lambda _s: None,
        log=lambda _m: None,
    )
    assert code == 2
    assert samples == []


def test_an_unknown_symbol_ends_the_run_with_exit_2():
    samples, code = probe.run_probe(
        fetch=lambda _url: (404, None, None),
        base_url="https://x",
        symbol="XYZ",
        interval=5.0,
        duration_seconds=600.0,
        clock=FakeClock(T0),
        sleep=lambda _s: None,
        log=lambda _m: None,
    )
    assert code == 2
    assert samples == []


def test_a_dead_server_is_given_up_on_after_repeated_failures():
    clock = FakeClock(T0)
    calls = []
    samples, code = probe.run_probe(
        fetch=lambda url: (calls.append(url), (0, None, None))[1],
        base_url="https://x",
        symbol="NQ",
        interval=5.0,
        duration_seconds=3600.0,
        clock=clock,
        sleep=clock.sleep,
        log=lambda _m: None,
    )
    assert code == 2
    assert samples == []
    assert len(calls) == probe.MAX_CONSECUTIVE_FAILURES


def test_a_closed_session_ends_the_run_before_the_second_sample():
    clock = FakeClock(T0)
    server = FakeServer(clock, period=60.0, publish_lag=35.0, closed=True)
    messages: List[str] = []
    samples, code = probe.run_probe(
        fetch=server,
        base_url="https://x",
        symbol="NQ",
        interval=5.0,
        duration_seconds=3600.0,
        clock=clock,
        sleep=clock.sleep,
        log=messages.append,
    )
    assert code == probe.EXIT_NOTHING_TO_MEASURE
    assert len(samples) == 1
    assert any("session closed" in m for m in messages)


def test_force_polls_a_closed_session_anyway():
    clock = FakeClock(T0)
    server = FakeServer(clock, period=60.0, publish_lag=35.0, closed=True)
    samples, code = probe.run_probe(
        fetch=server,
        base_url="https://x",
        symbol="NQ",
        interval=5.0,
        duration_seconds=30.0,
        clock=clock,
        sleep=clock.sleep,
        log=lambda _m: None,
        force=True,
    )
    assert code == probe.EXIT_OK
    # Samples land at 0, 5, ... 30s inclusive: seven of them.
    assert len(samples) == 7


def test_v1_with_a_stale_first_sample_is_treated_as_closed():
    stale_as_of = T0 - timedelta(hours=1)
    samples, code = probe.run_probe(
        fetch=lambda _url: (
            (404, None, None) if "/v2/" in _url else (200, _v1_body(stale_as_of, T0), None)
        ),
        base_url="https://x",
        symbol="NQ",
        interval=5.0,
        duration_seconds=3600.0,
        clock=FakeClock(T0),
        sleep=lambda _s: None,
        log=lambda _m: None,
    )
    assert code == probe.EXIT_NOTHING_TO_MEASURE
    assert len(samples) == 1


def test_bust_cache_makes_every_url_unique_and_records_the_cache_header():
    clock = FakeClock(T0)
    server = FakeServer(clock, period=60.0, publish_lag=35.0)
    samples, code = probe.run_probe(
        fetch=server,
        base_url="https://x",
        symbol="NQ",
        interval=5.0,
        duration_seconds=20.0,
        clock=clock,
        sleep=clock.sleep,
        log=lambda _m: None,
        bust_cache=True,
    )
    assert code == probe.EXIT_OK
    assert len(set(server.urls)) == len(server.urls) == 5
    assert all("&probe=" in u for u in server.urls)
    assert all(s.cache_status == "MISS" for s in samples)
    assert "MISS" in probe.format_sample(samples[0])
    assert "eval " in probe.format_sample(samples[0])


def test_once_takes_exactly_one_sample():
    clock = FakeClock(T0 + timedelta(seconds=40))
    server = FakeServer(clock, period=60.0, publish_lag=35.0)
    samples, code = probe.run_probe(
        fetch=server,
        base_url="https://x",
        symbol="NQ",
        interval=5.0,
        duration_seconds=0.0,
        clock=clock,
        sleep=clock.sleep,
        log=lambda _m: None,
    )
    assert code == 0
    assert len(samples) == 1
