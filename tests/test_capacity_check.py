"""The report behind ``make capacity-check`` (src/tools/capacity_check.py).

The parsers are tested against lines made by the code that writes them, where
that code is a function (the analytics cycle and loop lines), and against the
logger's format string where it is not (the ThetaData failures), so a reworded
log line fails here instead of quietly reading as zero.
"""

from datetime import datetime, timezone
from pathlib import Path

from src.analytics.main_engine import format_cycle_timing, format_loop_timing
from src.tools import capacity_check as cc

MB = 1024 * 1024
ET = cc.ET


# ── cgroup and /proc ────────────────────────────────────────────────────────


def _cgroup(tmp_path: Path, unit: str, files: dict) -> Path:
    root = tmp_path / "cgroup"
    base = root / f"{unit}.service"
    base.mkdir(parents=True)
    for name, text in files.items():
        (base / name).write_text(text)
    return root


def _proc(tmp_path: Path, pid: int, ppid: int, private_kb: int | None, rss_kb: int = 0) -> None:
    d = tmp_path / "proc" / str(pid)
    d.mkdir(parents=True)
    # A command with a space and a parenthesis, as python's can have.
    (d / "stat").write_text(f"{pid} (python -m (x)) S {ppid} 1 1 0 -1\n")
    if private_kb is not None:
        half = private_kb // 2
        (d / "smaps_rollup").write_text(
            "Rss:              999999 kB\n"
            "Shared_Clean:      50000 kB\n"
            f"Private_Clean:     {half} kB\n"
            f"Private_Dirty:     {private_kb - half} kB\n"
            "Swap:                  0 kB\n"
        )
    (d / "status").write_text(f"Name:\tpython\nVmRSS:\t{rss_kb} kB\n")


def test_a_unit_reads_its_cap_peak_kills_and_each_workers_private_memory(tmp_path):
    root = _cgroup(
        tmp_path,
        "zerogex-oa-analytics",
        {
            "memory.current": f"{700 * MB}\n",
            "memory.peak": f"{760 * MB}\n",
            "memory.max": f"{1024 * MB}\n",
            "memory.stat": f"anon {650 * MB}\nfile {50 * MB}\n",
            "memory.events": "low 0\nhigh 0\nmax 3\noom 1\noom_kill 1\n",
            "cpu.max": "100000 100000\n",
            "cgroup.procs": "100\n101\n102\n",
        },
    )
    _proc(tmp_path, 100, 1, 40_000)  # the unit's main process
    _proc(tmp_path, 101, 100, 120 * 1024)
    _proc(tmp_path, 102, 100, 140 * 1024)

    r = cc.read_unit_memory("zerogex-oa-analytics", root=root, proc=tmp_path / "proc")

    assert r.present
    assert (r.memory_current, r.memory_peak, r.memory_max) == (700 * MB, 760 * MB, 1024 * MB)
    assert r.memory_anon == 650 * MB
    assert r.oom_kills == 1
    assert r.cpu_quota_cores == 1.0
    assert r.processes == 3
    # The parent is not a worker; the workers' private memory, largest first.
    assert r.worker_private_mb == [140.0, 120.0]
    assert r.private_source == "smaps_rollup"


def test_a_worker_without_smaps_rollup_falls_back_to_rss_and_says_so(tmp_path):
    root = _cgroup(
        tmp_path, "zerogex-oa-ingestion", {"memory.max": "max\n", "cgroup.procs": "200\n201\n"}
    )
    _proc(tmp_path, 200, 1, None)
    _proc(tmp_path, 201, 200, None, rss_kb=300 * 1024)

    r = cc.read_unit_memory("zerogex-oa-ingestion", root=root, proc=tmp_path / "proc")

    assert r.memory_max is None  # "max" is no cap
    assert r.worker_private_mb == [300.0]
    assert "overstates" in r.private_source


def test_a_unit_that_is_not_running_is_reported_absent(tmp_path):
    r = cc.read_unit_memory("zerogex-oa-ingestion", root=tmp_path / "nothing", proc=tmp_path)
    assert not r.present and r.processes == 0


def test_cpu_between_two_reads_gives_cores_used_and_the_throttled_share():
    before = {"usage_usec": 1_000_000, "nr_periods": 100, "nr_throttled": 10}
    after = {"usage_usec": 31_000_000, "nr_periods": 200, "nr_throttled": 60}
    used, share = cc.cpu_between(before, after, seconds=10)
    assert used == 3.0
    assert share == 0.5
    assert cc.cpu_between({}, after, 10) == (None, None)


def test_cpu_max_reads_as_cores():
    assert cc.parse_cpu_max("300000 100000\n") == 3.0
    assert cc.parse_cpu_max("max 100000\n") is None


def test_one_more_symbol_costs_the_median_of_the_largest_workers():
    # Ingestion: five symbol workers and four light feeds (VIX, VXN, ES, NQ).
    workers = [180, 175, 170, 160, 150, 60, 55, 50, 45]
    assert cc.per_symbol_worker_mb(workers, symbol_count=5) == 170
    assert cc.per_symbol_worker_mb([], 5) is None
    assert cc.per_symbol_worker_mb([90.0], 5) == 90.0


def test_box_memory_from_meminfo(tmp_path):
    proc = tmp_path / "proc"
    proc.mkdir()
    (proc / "meminfo").write_text(
        "MemTotal:       16000000 kB\nMemAvailable:    3072000 kB\n"
        "SwapTotal:       4194304 kB\nSwapFree:        3145728 kB\n"
    )
    (proc / "loadavg").write_text("3.10 2.90 2.80 4/900 12345\n")
    box = cc.read_box(proc)
    assert box["mem_available_mb"] == 3000
    assert box["swap_used_mb"] == 1024
    assert box["load_1m"] == 3.1


# ── journal ─────────────────────────────────────────────────────────────────


def _journal(message: str) -> str:
    return (
        "2026-11-05T11:00:00-0500 ip-10-0-0-1 zerogex-oa-analytics[1234]: 2026-11-05 "
        f"11:00:00,000 - src.analytics.main_engine - INFO - [request_id=-] {message}"
    )


def test_analytics_cycle_and_loop_lines_parse_as_written():
    stamp = datetime(2026, 11, 5, 16, 0, tzinfo=timezone.utc)
    t0 = stamp.timestamp() + 12
    lines = [
        _journal(format_cycle_timing("SPY", stamp, t0, t0 + 4.5, {"snapshot": 3.0, "gex": 1.5})),
        _journal(format_cycle_timing("SPY", stamp, t0, t0 + 6.5, {"snapshot": 5.0})),
        _journal(format_loop_timing("SPY", 4.5, 1.0, 54.5, 60)),
        _journal(format_loop_timing("SPY", 50.0, 15.0, 0.0, 60)),  # overran
        _journal(format_cycle_timing("$SPX.X", stamp, t0, t0 + 9.0, {})),
        _journal(format_loop_timing("$SPX.X", 9.0, 2.0, 49.0, 60)),
        "an unrelated line",
    ]
    parsed = cc.parse_analytics_lines(lines, canonical=lambda s: {"$SPX.X": "SPX"}.get(s, s))
    summary = cc.summarize_analytics(parsed)

    assert summary["SPY"]["cycles"] == 2
    assert summary["SPY"]["duration_median_s"] == 5.5
    assert summary["SPY"]["loops"] == 2
    assert summary["SPY"]["overruns"] == 1
    assert summary["SPY"]["overrun_share"] == 0.5
    assert summary["SPY"]["busy_p95_s"] == 65.0
    assert summary["SPY"]["interval_s"] == 60
    # Workers log the vendor symbol; the report keys on the canonical one.
    assert summary["SPX"]["cycles"] == 1 and summary["SPX"]["overruns"] == 0


def test_thetadata_failures_and_worker_deaths_are_counted():
    lines = [
        "... - WARNING - [request_id=-] thetadata bar poll for NVDA failed: deadline exceeded",
        "... - WARNING - [request_id=-] thetadata bar poll for NVDA failed: deadline exceeded",
        "... - WARNING - [request_id=-] thetadata bar poll for $SPXW.X failed: unavailable",
        "... - WARNING - [request_id=-] thetadata poll failed (2 consecutive): boom",
        "... - WARNING - [request_id=-] thetadata quote snapshot failed for NVDA 2026-11-06: boom",
        "... - ERROR - [request_id=-] Ingestion worker ingest-AAPL exited unexpectedly"
        " (exitcode -9) -- respawning",
    ]
    out = cc.parse_ingestion_lines(lines, canonical=lambda s: {"$SPXW.X": "SPX"}.get(s, s))
    assert out["lines"] == 6
    assert out["bar_poll_failures"] == {"NVDA": 2, "SPX": 1}
    assert out["chain_poll_failures"] == 1
    assert out["snapshot_failures"] == {"NVDA": 1}
    assert out["worker_deaths"] == ["ingest-AAPL"]


def test_journalctls_own_notes_are_not_counted_as_journal_lines():
    # What a user who cannot read the system journal gets, with exit 0.
    assert cc.journal_entries("-- No entries --\n") == []
    text = "-- Boot 1a2b --\n2026-11-05T11:00:00-0500 host unit[1]: a line\n\n"
    assert cc.journal_entries(text) == ["2026-11-05T11:00:00-0500 host unit[1]: a line"]


def test_the_failure_lines_the_parser_reads_are_still_the_ones_the_code_writes():
    provider = Path("src/ingestion/providers/thetadata.py").read_text()
    engine = Path("src/ingestion/main_engine.py").read_text()
    assert '"thetadata bar poll for %s failed: %s"' in provider
    assert '"thetadata poll failed (%d consecutive): %s"' in provider
    assert '"thetadata %s snapshot failed for %s %s: %s"' in provider
    assert '"Ingestion worker %s exited unexpectedly (exitcode %s) -- "' in engine


# ── gates ───────────────────────────────────────────────────────────────────


def _unit(name, peak_mb, cap_mb, per_symbol_mb=None, oom=0, used=None, quota=None, throttled=None):
    return {
        "unit": name,
        "present": True,
        "memory_current": peak_mb * MB,
        "memory_peak": peak_mb * MB,
        "memory_max": cap_mb * MB if cap_mb else None,
        "memory_anon": None,
        "oom_kills": oom,
        "per_symbol_mb": per_symbol_mb,
        "cpu_used_cores": used,
        "cpu_quota_cores": quota,
        "throttled_share": throttled,
        "processes": 6,
        "worker_private_mb": [],
        "private_source": "smaps_rollup",
    }


def _healthy(**overrides):
    report = {
        "taken_at_et": "2026-11-05 11:30",
        "session": "regular",
        "window_minutes": 30,
        "window_in_session": True,
        "ingest_symbols": ["SPY", "SPX", "QQQ", "NDX", "NVDA"],
        "analytics_symbols": ["SPY", "SPX", "QQQ", "NDX", "NVDA"],
        "listed_symbols": ["SPY", "QQQ", "NVDA"],
        "units": [
            _unit(
                "zerogex-oa-ingestion",
                1200,
                2048,
                per_symbol_mb=150,
                used=1.5,
                quota=3.0,
                throttled=0.05,
            ),
            _unit(
                "zerogex-oa-analytics",
                600,
                1024,
                per_symbol_mb=110,
                used=0.6,
                quota=1.0,
                throttled=0.1,
            ),
            _unit("zerogex-oa-api", 5000, 8192),
        ],
        "box": {"mem_available_mb": 3500, "mem_total_mb": 15700, "swap_used_mb": 200},
        "analytics": {
            s: {
                "loops": 30,
                "overruns": 0,
                "overrun_share": 0.0,
                "busy_p95_s": 8.0,
                "interval_s": 60,
                "cycles": 30,
                "duration_median_s": 5.0,
                "duration_p95_s": 7.0,
            }
            for s in ["SPY", "SPX", "QQQ", "NDX", "NVDA"]
        },
        "database": {
            "per_symbol": {
                s: {
                    "gex_rows": 30,
                    "gex_age_s": 60,
                    "bar_age_s": 30,
                    "contracts": 200,
                    "chain_fresh_share": 0.8,
                }
                for s in ["SPY", "SPX", "QQQ", "NDX", "NVDA"]
            },
            "sessions": {},
            "sizes": {},
        },
        "ingestion": {
            "lines": 900,
            "bar_poll_failures": {},
            "chain_poll_failures": 0,
            "snapshot_failures": {},
            "worker_deaths": [],
        },
        "cadence": [
            {"symbol": s, "updates": 30, "median_gap": 1.0, "max_gap": 1.1, "slow_gaps": 0}
            for s in ["SPY", "QQQ", "NVDA"]
        ],
        "api": {
            "path": "/api/gex/summary?symbol=SPY",
            "ok": 5,
            "tries": 5,
            "median_ms": 20.0,
            "statuses": [200],
        },
    }
    report.update(overrides)
    return report


def _levels(gates):
    return [(g.level, g.area) for g in gates if g.level != "PASS"]


def test_a_healthy_session_passes_every_area():
    gates = cc.evaluate(_healthy())
    assert _levels(gates) == []
    assert {g.area for g in gates} >= {
        "memory",
        "box",
        "cpu",
        "analytics",
        "freshness",
        "thetadata",
        "live price",
        "api",
    }
    assert "GO:" in cc.render(_healthy(), gates)


def test_the_next_symbol_is_projected_onto_each_units_cap():
    # analytics: 760 + 110 = 870 MB of 1024 is 85%: look first.
    warn = _healthy(units=[_unit("zerogex-oa-analytics", 760, 1024, per_symbol_mb=110)])
    assert _levels(cc.evaluate(warn)) == [("WARN", "memory")]
    # 850 + 110 = 960 MB is 94%: hold.
    fail = _healthy(units=[_unit("zerogex-oa-analytics", 850, 1024, per_symbol_mb=110)])
    [gate] = [g for g in cc.evaluate(fail) if g.level != "PASS"]
    assert gate.level == "FAIL" and "960 MB" in gate.message and "94%" in gate.message


def test_a_full_unit_a_stock_does_not_add_to_only_warns():
    report = _healthy(units=[_unit("zerogex-oa-api", 7600, 8192)])
    assert _levels(cc.evaluate(report)) == [("WARN", "memory")]


def test_any_oom_kill_holds_the_rollout():
    report = _healthy(units=[_unit("zerogex-oa-ingestion", 1000, 2048, per_symbol_mb=100, oom=1)])
    assert ("FAIL", "memory") in _levels(cc.evaluate(report))


def test_outside_the_session_only_memory_box_and_cpu_are_judged():
    report = _healthy(session="after-hours", analytics={}, database={"per_symbol": {}}, cadence=[])
    gates = cc.evaluate(report)
    assert {g.area for g in gates} == {"memory", "box", "cpu", "session"}
    assert _levels(gates) == [("WARN", "session")]
    # Clean memory outside the session is not a go-ahead for the next symbol.
    assert cc.verdict(gates).startswith("NOT JUDGED YET")


def test_the_verdict_line_is_the_worst_gate():
    fail, warn, ok = (
        cc.Gate("FAIL", "memory", "x"),
        cc.Gate("WARN", "cpu", "x"),
        cc.Gate("PASS", "api", "x"),
    )
    assert cc.verdict([fail, warn, ok]).startswith("HOLD")
    assert cc.verdict([fail, cc.Gate("WARN", "session", "x")]).startswith("HOLD")
    assert cc.verdict([warn, ok]).startswith("LOOK FIRST")
    assert cc.verdict([ok]).startswith("GO")


def test_analytics_overruns_hold_and_a_rise_over_the_baseline_holds_too():
    def with_overruns(n):
        r = _healthy()
        r["analytics"]["SPY"].update(overruns=n, overrun_share=n / 30)
        return r

    assert ("FAIL", "analytics") in _levels(cc.evaluate(with_overruns(4)))  # 13%
    assert _levels(cc.evaluate(with_overruns(1))) == [("WARN", "analytics")]
    # One overrun a session was normal before; two now is not a rise worth holding for.
    assert _levels(cc.evaluate(with_overruns(2), baseline=with_overruns(1))) == [
        ("WARN", "analytics")
    ]
    assert ("FAIL", "analytics") in _levels(
        cc.evaluate(with_overruns(2), baseline=with_overruns(0))
    )


def test_fewer_gex_snapshots_than_the_baseline_are_caught():
    def with_rows(n):
        r = _healthy()
        r["database"]["per_symbol"]["SPX"]["gex_rows"] = n
        return r

    assert _levels(cc.evaluate(with_rows(26), baseline=with_rows(30))) == [("WARN", "freshness")]
    assert _levels(cc.evaluate(with_rows(21), baseline=with_rows(30))) == [("FAIL", "freshness")]
    # Without a baseline the count is judged against one a minute.
    assert _levels(cc.evaluate(with_rows(20))) == [("FAIL", "freshness")]


def test_snapshot_counts_are_not_judged_while_the_window_reaches_back_before_the_open():
    r = _healthy(window_in_session=False)
    r["database"]["per_symbol"]["SPY"]["gex_rows"] = 5
    assert _levels(cc.evaluate(r)) == [("WARN", "session")]


def test_a_stale_symbol_fails_freshness():
    r = _healthy()
    r["database"]["per_symbol"]["NVDA"].update(gex_age_s=600, bar_age_s=None)
    messages = [g.message for g in cc.evaluate(r) if g.level == "FAIL"]
    assert any("NVDA: newest GEX snapshot is 10 min old" in m for m in messages)
    assert any("NVDA: newest price bar is missing old" in m for m in messages)


def test_failed_price_polls_and_a_dead_worker():
    r = _healthy()
    r["ingestion"].update(bar_poll_failures={"SPY": 3, "NVDA": 4}, worker_deaths=["ingest-AAPL"])
    levels = _levels(cc.evaluate(r))
    assert ("WARN", "thetadata") in levels
    assert ("FAIL", "thetadata") in levels
    r["ingestion"].update(bar_poll_failures={"SPY": 40}, worker_deaths=[])
    assert _levels(cc.evaluate(r)) == [("FAIL", "thetadata")]


def test_an_unreadable_journal_does_not_pass_quietly():
    r = _healthy(analytics={"SPY": {"loops": 0}})
    r["ingestion"]["lines"] = 0
    assert sorted(_levels(cc.evaluate(r))) == [("WARN", "analytics"), ("WARN", "thetadata")]


def test_a_slower_live_price_is_caught_and_indexes_are_not_judged():
    r = _healthy()
    r["cadence"][0]["median_gap"] = 1.6  # SPY
    r["cadence"].append(
        {"symbol": "SPX", "updates": 3, "median_gap": 9.0, "max_gap": 20.0, "slow_gaps": 2}
    )
    gates = [g for g in cc.evaluate(r) if g.level != "PASS"]
    assert [(g.level, g.area) for g in gates] == [("FAIL", "live price")]
    assert gates[0].message.startswith("SPY:")
    slower = _healthy()
    slower["cadence"][2]["median_gap"] = 1.1  # NVDA, up 0.2s from its baseline of 0.9
    base = _healthy()
    base["cadence"][2]["median_gap"] = 0.9
    assert _levels(cc.evaluate(slower, baseline=base)) == [("WARN", "live price")]


def test_the_api_check_is_judged_only_when_it_ran():
    assert (
        _levels(cc.evaluate(_healthy(api={"path": "/x", "skipped": "no OPS_API_KEY in .env"})))
        == []
    )
    slow = _healthy(api={"path": "/x", "ok": 5, "tries": 5, "median_ms": 2500.0, "statuses": [200]})
    assert _levels(cc.evaluate(slow)) == [("FAIL", "api")]


# ── report and files ────────────────────────────────────────────────────────


def test_render_survives_missing_sections_and_shows_the_baseline_change():
    sparse = _healthy(
        database={"error": "OperationalError: down"},
        ingestion=None,
        cadence=[],
        api={"path": "/x", "skipped": "no OPS_API_KEY in .env"},
    )
    sparse["box"] = {}
    text = cc.render(sparse, cc.evaluate(sparse))
    assert "Database: unreadable (OperationalError: down)" in text
    assert "the ingestion journal is unreadable" in text
    assert "box: - MB available" in text

    base = _healthy()
    base["units"][1]["memory_peak"] = 500 * MB
    text = cc.render(_healthy(), cc.evaluate(_healthy(), base), base)
    assert "600 (+100)" in text  # analytics peak, against the baseline


def test_window_in_session_only_once_the_window_clears_the_open():
    assert cc.window_in_session(datetime(2026, 11, 5, 10, 0, tzinfo=ET), 30)
    assert not cc.window_in_session(datetime(2026, 11, 5, 9, 45, tzinfo=ET), 30)


def test_save_names_the_file_by_time_and_label(tmp_path):
    report = {
        "taken_at_epoch": datetime(2026, 11, 5, 11, 30, tzinfo=ET).timestamp(),
        "label": "after msft",
    }
    path = cc.save(report, tmp_path / "capacity")
    assert path.name == "2026-11-05-1130-after-msft.json"
    assert '"label": "after msft"' in path.read_text()


# ── database CPU (CloudWatch) ───────────────────────────────────────────────


def test_the_rds_instance_is_the_first_label_of_its_endpoint():
    host = "zerogex-prod.c1a2b3c4d5e6.us-east-1.rds.amazonaws.com"
    assert cc.rds_instance_from_host(host) == "zerogex-prod"
    assert cc.rds_instance_from_host("localhost") is None
    assert cc.rds_instance_from_host("/var/run/postgresql") is None


def test_database_cpu_is_averaged_over_the_window(monkeypatch):
    calls = {}

    class FakeCloudWatch:
        def get_metric_statistics(self, **kwargs):
            calls.update(kwargs)
            return {
                "Datapoints": [
                    {"Average": 40.0, "Maximum": 70.0},
                    {"Average": 50.0, "Maximum": 88.0},
                ]
            }

    import boto3

    monkeypatch.setattr(boto3, "client", lambda *a, **k: FakeCloudWatch())
    out = cc.read_rds_cpu("zerogex-prod", "us-east-1", 30)
    assert out == {"instance": "zerogex-prod", "avg_pct": 45.0, "max_pct": 88.0, "minutes": 2}
    assert calls["Dimensions"] == [{"Name": "DBInstanceIdentifier", "Value": "zerogex-prod"}]
    assert calls["Namespace"] == "AWS/RDS" and calls["MetricName"] == "CPUUtilization"


def test_database_cpu_gates():
    def at(avg, peak=50.0):
        return _healthy(db_cpu={"instance": "db", "avg_pct": avg, "max_pct": peak, "minutes": 30})

    assert _levels(cc.evaluate(at(85))) == [("FAIL", "database")]
    assert _levels(cc.evaluate(at(65))) == [("WARN", "database")]
    assert _levels(cc.evaluate(at(40, peak=95))) == [("WARN", "database")]
    assert _levels(cc.evaluate(at(40), baseline=at(20))) == [("WARN", "database")]
    assert _levels(cc.evaluate(at(40), baseline=at(35))) == []
    # No permission to read CloudWatch is not a reason to hold.
    unread = _healthy(db_cpu={"instance": "db", "error": "AccessDenied"})
    assert _levels(cc.evaluate(unread)) == []
    assert "database CPU: unavailable (AccessDenied)" in cc.render(unread, cc.evaluate(unread))
