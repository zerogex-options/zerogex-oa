"""Unit tests for the lightweight monitoring collector.

The script shells out to ``df`` and ``journalctl``, and reads ``/proc``,
so these tests focus on the pure-Python pieces (parsing, aggregation,
bucket rollover, state I/O) and stub the I/O boundary where needed.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.tools import system_monitor as sm
from src.tools.system_monitor import (
    CpuStat,
    Sample,
    _aggregate_bucket,
    _disk_metric_key,
    _max_avg,
    _max_avg_median,
    _sum_by_service,
    count_error_warning,
    cpu_percent_between,
    extract_stage_timing_totals,
    floor_to_day,
    floor_to_hour,
    fold_sample,
    load_state,
    run_once,
    save_state,
)

# ---------------------------------------------------------------------------
# Log-line parsers
# ---------------------------------------------------------------------------


def test_count_error_warning_matches_make_services_check_tokens():
    """The collector MUST count exactly the lines `make services-check` counts.

    Mismatched tokens would produce a monitor that disagrees with the
    primary triage shortcut — so we lock both to ' - ERROR - ' / ' - WARNING - '.
    """
    lines = [
        "2026-05-26 10:57:01,668 - __main__ - INFO - [request_id=-] hello",
        "2026-05-26 10:57:01,668 - __main__ - ERROR - [request_id=-] kaboom",
        "2026-05-26 10:57:02,668 - __main__ - WARNING - [request_id=-] slow",
        "2026-05-26 10:57:03,668 - __main__ - WARNING - [request_id=-] slow again",
        "2026-05-26 10:57:04,668 - __main__ - DEBUG - [request_id=-] noise",
        # A WARNING-shaped substring in a non-warning line must NOT count:
        "2026-05-26 10:57:05,668 - __main__ - INFO - [request_id=-] saw WARNING in text",
    ]
    errors, warnings = count_error_warning(lines)
    assert errors == 1
    assert warnings == 2


def test_extract_stage_timing_totals_parses_real_log_line():
    """Pulled verbatim from the user's example."""
    line = (
        "May 26 10:57:01 ip-172-31-26-218 zerogex-oa-analytics[444253]: "
        "2026-05-26 10:57:01,668 - __main__ - INFO - [request_id=-] "
        "Stage timings (total 3.62s): snapshot=2.66s, gex_by_strike=0.15s, "
        "gex_summary=0.11s, store_results=0.34s, refresh_flow_caches=0.31s, "
        "flow_series_snapshot=0.04s"
    )
    assert extract_stage_timing_totals([line]) == [3.62]


def test_extract_stage_timing_totals_handles_multiple_and_integers():
    lines = [
        "Stage timings (total 1s): a=1s",
        "Stage timings (total 12.345s): b=12s",
        "Some other log line about timings without total",
        "Stage timings (total 0.05s): tiny=0.05s",
    ]
    assert extract_stage_timing_totals(lines) == [1.0, 12.345, 0.05]


def test_extract_stage_timing_totals_ignores_non_matching_lines():
    assert (
        extract_stage_timing_totals(
            [
                "Stage timings: snapshot=1s",  # legacy 'no total' format
                "Stage timings (total: 3s)",  # malformed
                "",
            ]
        )
        == []
    )


# ---------------------------------------------------------------------------
# CPU sampling math
# ---------------------------------------------------------------------------


def test_cpu_percent_between_basic_arithmetic():
    """Idle goes from 80 -> 90 (delta 10), total from 100 -> 120 (delta 20).
    Non-idle fraction = (20-10)/20 = 50%.
    """
    prev = CpuStat(
        captured_at=0.0, user=10, nice=0, system=10, idle=80, iowait=0, irq=0, softirq=0, steal=0
    )
    curr = CpuStat(
        captured_at=60.0, user=15, nice=0, system=15, idle=90, iowait=0, irq=0, softirq=0, steal=0
    )
    pct = cpu_percent_between(prev, curr)
    assert pct == pytest.approx(50.0)


def test_cpu_percent_between_iowait_counted_as_idle():
    """The kernel convention: iowait is "not really busy" — fold it into idle.
    Without this, a box waiting on disk would look 100% busy.
    """
    prev = CpuStat(
        captured_at=0.0, user=0, nice=0, system=0, idle=50, iowait=50, irq=0, softirq=0, steal=0
    )
    curr = CpuStat(
        captured_at=60.0, user=0, nice=0, system=0, idle=60, iowait=60, irq=0, softirq=0, steal=0
    )
    # idle+iowait went 100 -> 120 (delta 20), total 100 -> 120 (delta 20) → 0% busy.
    assert cpu_percent_between(prev, curr) == pytest.approx(0.0)


def test_cpu_percent_between_returns_none_on_zero_delta():
    """Two reads inside the same jiffy — refusing to divide by zero is the
    only correct behavior; returning 0 would lie about the load."""
    stat = CpuStat(
        captured_at=0.0, user=1, nice=0, system=1, idle=1, iowait=0, irq=0, softirq=0, steal=0
    )
    assert cpu_percent_between(stat, stat) is None


# ---------------------------------------------------------------------------
# Aggregation helpers
# ---------------------------------------------------------------------------


def test_max_avg_drops_none_and_nan():
    assert _max_avg([10.0, None, float("nan"), 30.0]) == {"max": 30.0, "avg": 20.0}


def test_max_avg_empty_returns_nones():
    assert _max_avg([]) == {"max": None, "avg": None}


def test_max_avg_median_reports_count():
    out = _max_avg_median([1.0, 2.0, 3.0, 4.0])
    assert out["max"] == 4.0
    assert out["avg"] == pytest.approx(2.5)
    assert out["median"] == pytest.approx(2.5)
    assert out["count"] == 4


def test_sum_by_service_pads_missing_services_with_zero():
    """If one service is silent for the whole bucket, it must still appear in
    the output as 0 — otherwise downstream dashboards lose the dimension."""
    rows = [{"a": 1, "b": 2}, {"a": 3}]
    out = _sum_by_service(rows, ["a", "b", "c"])
    assert out == {"a": 4, "b": 2, "c": 0}


def test_disk_metric_key_slugifies_path():
    assert _disk_metric_key("/") == "disk_root_pct"
    assert _disk_metric_key("/var/log") == "disk_var_log_pct"
    assert _disk_metric_key("/srv/data") == "disk_srv_data_pct"


# ---------------------------------------------------------------------------
# Bucket aggregation + rollover
# ---------------------------------------------------------------------------


def _make_sample(
    ts: datetime,
    *,
    cpu: float | None = 50.0,
    mem: float | None = 60.0,
    disk_root: float | None = 90.0,
    disk_log: float | None = 13.0,
    cycles: list[float] | None = None,
    errs: dict[str, int] | None = None,
    warns: dict[str, int] | None = None,
) -> Sample:
    return Sample(
        captured_at=ts,
        cpu_pct=cpu,
        mem_pct=mem,
        disk_pcts={"/": disk_root, "/var/log": disk_log},
        cycle_times_s=cycles or [],
        errors_by_service=errs or {"zerogex-oa-analytics": 0},
        warnings_by_service=warns or {"zerogex-oa-analytics": 0},
    )


def test_aggregate_bucket_basic_shape():
    services = ["zerogex-oa-analytics"]
    mounts = ["/", "/var/log"]
    samples = [
        _make_sample(
            datetime(2026, 5, 26, 10, 0),
            cpu=10,
            mem=50,
            cycles=[1.0, 2.0],
            disk_root=89.0,
            disk_log=12.0,
        ),
        _make_sample(
            datetime(2026, 5, 26, 10, 1),
            cpu=20,
            mem=55,
            cycles=[3.0],
            disk_root=89.5,
            disk_log=12.5,
        ),
        _make_sample(
            datetime(2026, 5, 26, 10, 2), cpu=30, mem=60, cycles=[], disk_root=90.0, disk_log=13.0
        ),
    ]
    out = _aggregate_bucket(samples, mounts, services)
    assert out["sample_count"] == 3
    assert out["cpu_pct"] == {"max": 30.0, "avg": pytest.approx(20.0)}
    assert out["mem_pct"] == {"max": 60.0, "avg": pytest.approx(55.0)}
    assert out["cycle_time_s"]["max"] == 3.0
    assert out["cycle_time_s"]["median"] == 2.0
    assert out["cycle_time_s"]["count"] == 3
    # Disk → newest-non-null only, no max/avg.  The user explicitly asked
    # for "just the latest, whatever it is" because util barely moves
    # minute-to-minute and a peak there is rarely actionable.
    assert out["disk_root_pct"] == {"latest": 90.0}
    assert out["disk_var_log_pct"] == {"latest": 13.0}


def test_aggregate_bucket_disk_latest_skips_transient_df_failures():
    """If `df` failed on the most recent tick (None reading), the bucket
    should still surface the last good value rather than reporting None."""
    services = ["svc"]
    mounts = ["/"]
    samples = [
        _make_sample(datetime(2026, 5, 26, 10, 0), disk_root=89.0),
        _make_sample(datetime(2026, 5, 26, 10, 1), disk_root=90.0),
        # transient df failure on the latest tick
        _make_sample(datetime(2026, 5, 26, 10, 2), disk_root=None),
    ]
    out = _aggregate_bucket(samples, mounts, services)
    assert out["disk_root_pct"] == {"latest": 90.0}


def test_aggregate_bucket_disk_latest_all_none_returns_none():
    """If every reading failed we report None, not the absence of the key —
    the consumer needs a stable shape to plot against."""
    services = ["svc"]
    mounts = ["/"]
    samples = [
        _make_sample(datetime(2026, 5, 26, 10, 0), disk_root=None),
        _make_sample(datetime(2026, 5, 26, 10, 1), disk_root=None),
    ]
    out = _aggregate_bucket(samples, mounts, services)
    assert out["disk_root_pct"] == {"latest": None}


def test_fold_sample_overwrites_open_bucket_until_rollover():
    """Core requirement: the LAST hourly + daily entry should be rewritten
    every minute as the running aggregate until the hour/day rolls over."""
    state: dict = {}
    services = ["svc"]
    mounts = ["/"]

    sample_a = _make_sample(datetime(2026, 5, 26, 10, 0), cpu=10)
    fold_sample(state, sample_a, mounts, services, 720, 90)
    assert len(state["hourly"]) == 1
    assert state["hourly"][-1]["metrics"]["cpu_pct"]["max"] == 10.0
    # Open bucket carries raw samples so the next minute can reaggregate.
    assert state["hourly"][-1]["samples"]

    sample_b = _make_sample(datetime(2026, 5, 26, 10, 30), cpu=70)
    fold_sample(state, sample_b, mounts, services, 720, 90)
    # Still only one hourly entry — rewrite, not append.
    assert len(state["hourly"]) == 1
    assert state["hourly"][-1]["metrics"]["cpu_pct"]["max"] == 70.0
    assert state["hourly"][-1]["metrics"]["cpu_pct"]["avg"] == pytest.approx(40.0)


def test_fold_sample_closes_old_bucket_on_hour_rollover():
    """When the hour ticks over the previous bucket should be finalised:
    raw samples dropped, aggregates retained, then a fresh bucket appended."""
    state: dict = {}
    services = ["svc"]
    mounts = ["/"]
    s1 = _make_sample(datetime(2026, 5, 26, 10, 30), cpu=10)
    s2 = _make_sample(datetime(2026, 5, 26, 11, 5), cpu=90)
    fold_sample(state, s1, mounts, services, 720, 90)
    fold_sample(state, s2, mounts, services, 720, 90)

    assert len(state["hourly"]) == 2
    # Old bucket: aggregates kept, samples buffer dropped to keep disk small.
    closed = state["hourly"][0]
    assert closed["metrics"]["cpu_pct"]["max"] == 10.0
    assert "samples" not in closed
    # New bucket: still open, samples retained for re-aggregation next tick.
    open_bucket = state["hourly"][1]
    assert open_bucket["metrics"]["cpu_pct"]["max"] == 90.0
    assert open_bucket["samples"]


def test_fold_sample_day_rollover_independent_of_hour():
    """A sample at 00:00 the next day opens a new daily bucket AND a new
    hourly bucket; one rollover must not skip the other."""
    state: dict = {}
    services = ["svc"]
    mounts = ["/"]
    s1 = _make_sample(datetime(2026, 5, 26, 23, 59), cpu=10)
    s2 = _make_sample(datetime(2026, 5, 27, 0, 1), cpu=90)
    fold_sample(state, s1, mounts, services, 720, 90)
    fold_sample(state, s2, mounts, services, 720, 90)
    assert len(state["hourly"]) == 2
    assert len(state["daily"]) == 2
    assert state["daily"][0]["metrics"]["cpu_pct"]["max"] == 10.0
    assert state["daily"][1]["metrics"]["cpu_pct"]["max"] == 90.0


def test_fold_sample_retention_trims_oldest():
    state: dict = {}
    services = ["svc"]
    mounts = ["/"]
    # 5 samples each in a distinct hour — retention=3 should keep only the
    # most recent 3.
    base = datetime(2026, 5, 26, 0, 0)
    for h in range(5):
        fold_sample(
            state,
            _make_sample(base + timedelta(hours=h), cpu=10.0 * h),
            mounts,
            services,
            hourly_retention=3,
            daily_retention=90,
        )
    assert len(state["hourly"]) == 3
    starts = [b["bucket_start"] for b in state["hourly"]]
    # Newest three: hours 2, 3, 4.
    assert starts[0].startswith("2026-05-26T02:")
    assert starts[-1].startswith("2026-05-26T04:")


def test_fold_sample_aggregates_error_counts_across_minutes():
    state: dict = {}
    services = ["zerogex-oa-analytics", "zerogex-oa-api"]
    mounts = ["/"]
    base = datetime(2026, 5, 26, 10, 0)
    fold_sample(
        state,
        _make_sample(base, errs={"zerogex-oa-analytics": 2, "zerogex-oa-api": 0}),
        mounts,
        services,
        720,
        90,
    )
    fold_sample(
        state,
        _make_sample(
            base + timedelta(minutes=5), errs={"zerogex-oa-analytics": 3, "zerogex-oa-api": 1}
        ),
        mounts,
        services,
        720,
        90,
    )
    errs = state["hourly"][-1]["metrics"]["errors_by_service"]
    assert errs == {"zerogex-oa-analytics": 5, "zerogex-oa-api": 1}


def test_floor_helpers():
    dt = datetime(2026, 5, 26, 23, 45, 17)
    assert floor_to_hour(dt) == datetime(2026, 5, 26, 23, 0, 0)
    assert floor_to_day(dt) == datetime(2026, 5, 26, 0, 0, 0)


# ---------------------------------------------------------------------------
# State I/O
# ---------------------------------------------------------------------------


def test_load_state_returns_empty_skeleton_when_missing(tmp_path: Path):
    state = load_state(tmp_path / "does_not_exist.json")
    assert state == {"version": sm.STATE_VERSION, "hourly": [], "daily": []}


def test_save_then_load_roundtrip(tmp_path: Path):
    path = tmp_path / "state.json"
    original = {
        "version": 1,
        "hourly": [{"bucket_start": "2026-05-26T10:00:00", "metrics": {}}],
        "daily": [],
        "last_sample_iso": "2026-05-26T10:00:00",
    }
    save_state(path, original)
    loaded = load_state(path)
    assert loaded["hourly"] == original["hourly"]
    assert loaded["last_sample_iso"] == original["last_sample_iso"]


def test_save_state_writes_atomically_no_partial_files(tmp_path: Path):
    """If save_state succeeded, only the final file should exist — the
    temp file used for the os.replace() must be cleaned up."""
    path = tmp_path / "state.json"
    save_state(path, {"version": 1, "hourly": [], "daily": []})
    leftovers = list(tmp_path.glob(".state.*.tmp"))
    assert leftovers == [], f"temp files left behind: {leftovers}"


def test_load_state_migrates_v1_disk_shape_in_place(tmp_path: Path):
    """A pre-v2 state file (disk metrics stored as {max, avg}) should be
    transparently upgraded to {latest} on load, using the bucket's max as
    the migration value.  Disk %used barely changes minute-to-minute so
    `max` is the closest available proxy for the closing reading we'd
    have stored under the new shape.  Migration must be idempotent.
    """
    path = tmp_path / "state.json"
    pre_v2 = {
        "version": 1,
        "hourly": [
            {
                "bucket_start": "2026-05-26T10:00:00",
                "metrics": {
                    "cpu_pct": {"max": 50.0, "avg": 30.0},
                    "disk_root_pct": {"max": 90.0, "avg": 89.5},
                    "disk_var_log_pct": {"max": 14.0, "avg": 13.5},
                },
            },
        ],
        "daily": [],
    }
    path.write_text(json.dumps(pre_v2))

    loaded = load_state(path)
    assert loaded["version"] == sm.STATE_VERSION
    bucket = loaded["hourly"][0]["metrics"]
    assert bucket["disk_root_pct"] == {"latest": 90.0}
    assert bucket["disk_var_log_pct"] == {"latest": 14.0}
    # Non-disk metrics untouched — only disk shape changed.
    assert bucket["cpu_pct"] == {"max": 50.0, "avg": 30.0}

    # Idempotency: a second load on the (already-migrated) in-memory state
    # is a no-op.
    sm._migrate_state_in_place(loaded)
    assert loaded["hourly"][0]["metrics"]["disk_root_pct"] == {"latest": 90.0}


def test_load_state_rotates_corrupt_file(tmp_path: Path):
    path = tmp_path / "state.json"
    path.write_text("{not valid json")
    state = load_state(path)
    assert state["hourly"] == []
    # The original corrupt file should have been preserved as a .corrupt.* backup.
    backups = list(tmp_path.glob("state.json.corrupt.*"))
    assert len(backups) == 1


# ---------------------------------------------------------------------------
# End-to-end run_once with all I/O stubbed
# ---------------------------------------------------------------------------


def test_run_once_persists_sample_and_rolls_cpu_state(tmp_path: Path, monkeypatch):
    """One full tick: stub out every collector to deterministic values and
    verify the state file ends up with one hourly bucket containing the
    expected metrics block. Catches integration-level regressions in the
    glue between collect_sample, fold_sample, and save_state."""

    fake_now = datetime(2026, 5, 26, 10, 30, 0)
    state_path = tmp_path / "state.json"

    def fake_cpu(prev):
        return 42.0, CpuStat(
            captured_at=fake_now.timestamp(),
            user=1,
            nice=0,
            system=1,
            idle=10,
            iowait=0,
            irq=0,
            softirq=0,
            steal=0,
        )

    monkeypatch.setattr(sm, "sample_cpu_percent", fake_cpu)
    monkeypatch.setattr(sm, "sample_memory_percent", lambda: 58.5)
    monkeypatch.setattr(sm, "sample_disk_percents", lambda mounts: {"/": 90.0, "/var/log": 13.0})

    captured = {}

    def fake_journal(svc, since_ts, until_ts):
        captured.setdefault("calls", []).append((svc, since_ts, until_ts))
        if svc == "zerogex-oa-analytics":
            return [
                "2026-05-26 10:30:00 - __main__ - INFO - [request_id=-] Stage timings (total 3.62s): a=1s",
                "2026-05-26 10:30:00 - __main__ - WARNING - [request_id=-] slow",
                "2026-05-26 10:30:00 - __main__ - ERROR - [request_id=-] oops",
            ]
        return ["2026-05-26 10:30:00 - __main__ - WARNING - [request_id=-] note"]

    monkeypatch.setattr(sm, "journalctl_lines", fake_journal)

    state = run_once(
        state_path,
        services=["zerogex-oa-analytics", "zerogex-oa-api"],
        analytics_service="zerogex-oa-analytics",
        disk_mounts=["/", "/var/log"],
        hourly_retention=720,
        daily_retention=90,
        now=fake_now,
    )
    assert state_path.exists()
    # Hourly bucket created at 10:00 with one sample.
    assert len(state["hourly"]) == 1
    bucket = state["hourly"][-1]
    assert bucket["bucket_start"].startswith("2026-05-26T10:00")
    metrics = bucket["metrics"]
    assert metrics["cpu_pct"]["max"] == 42.0
    assert metrics["mem_pct"]["max"] == 58.5
    assert metrics["disk_root_pct"] == {"latest": 90.0}
    assert metrics["disk_var_log_pct"] == {"latest": 13.0}
    assert metrics["cycle_time_s"]["max"] == 3.62
    assert metrics["errors_by_service"] == {
        "zerogex-oa-analytics": 1,
        "zerogex-oa-api": 0,
    }
    assert metrics["warnings_by_service"] == {
        "zerogex-oa-analytics": 1,
        "zerogex-oa-api": 1,
    }
    # CPU snapshot was carried over for the next run.
    assert "last_cpu_stat" in state
    assert state["last_sample_iso"].startswith("2026-05-26T10:30")
