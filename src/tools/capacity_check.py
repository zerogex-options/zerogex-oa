"""Capacity check: will one more symbol fit, and did the last one hurt anything?

Run it on the production box during the regular session: once before a stock
is added (the baseline), and again on the first full session after it
(``--baseline <that file>``). Every run is saved, so the second run prints the
change next to each number.

It only reads. Per service: the cgroup's memory against its cap, its OOM
kills, its CPU against its quota, and each worker process's private memory,
which is what one more symbol's worker will cost. Per symbol: how many GEX
snapshots analytics published in the window and how long its cycles took,
how fresh its price and chain are, and ThetaData's failed calls. Then 30 s
of the live price channel (``make quote-cadence``) and a few timed API reads.

Why these. Ingestion and analytics each fork one process per symbol inside one
systemd unit, so every symbol shares that unit's memory cap and CPU quota
(setup/systemd/zerogex-oa-{ingestion,analytics}.service). ThetaData allows 8
concurrent requests per account, and each symbol's process keeps its own pool
of THETADATA_MAX_CONCURRENCY, so nothing coordinates them. A stock that does
not fit shows up first as slower analytics cycles, skipped GEX minutes, a
stuttering live price or failed ThetaData calls, and last as an OOM kill.

Usage::

    python -m src.tools.capacity_check                      # print, and save
    python -m src.tools.capacity_check --label baseline
    python -m src.tools.capacity_check --baseline ~/monitoring/capacity/<file>.json

Exit codes: 0 no gate failed, 1 a gate failed, 2 could not read the database.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
MB = 1024 * 1024

CGROUP_ROOT = Path("/sys/fs/cgroup/system.slice")

#: Units with one worker process per symbol. Their projection is the point.
PER_SYMBOL_UNITS = ("zerogex-oa-ingestion", "zerogex-oa-analytics")
#: Reported for context; adding a stock does not add a process to these.
OTHER_UNITS = ("zerogex-oa-signals", "zerogex-oa-api", "zerogex-oa-thetaterminal")
JOURNAL_UNITS = (
    "zerogex-oa-ingestion",
    "zerogex-oa-analytics",
    "zerogex-oa-signals",
    "zerogex-oa-api",
)

SCHEMA_VERSION = 1

# Gate thresholds. A FAIL means do not add the next stock until it is
# understood; a WARN means look before adding it.
MEMORY_WARN_PCT = 80.0
MEMORY_FAIL_PCT = 90.0
OTHER_UNIT_WARN_PCT = 90.0
BOX_AVAILABLE_WARN_MB = 2048
BOX_AVAILABLE_FAIL_MB = 1024
CPU_PINNED_SHARE = 0.90
THROTTLED_WARN_SHARE = 0.50
BUSY_WARN_SHARE = 0.75  # analytics calc+flow, as a share of the interval
OVERRUN_FAIL_SHARE = 0.10
OVERRUN_RISE_FAIL = 0.05
GEX_ROWS_WARN_SHARE = 0.85  # of the window's minutes, without a baseline
GEX_ROWS_FAIL_SHARE = 0.70
GEX_ROWS_DROP_WARN = 0.10  # against the baseline
GEX_ROWS_DROP_FAIL = 0.25
GEX_AGE_WARN_S = 180
GEX_AGE_FAIL_S = 300
BAR_AGE_WARN_S = 120
BAR_AGE_FAIL_S = 180
CHAIN_FRESH_DROP_WARN = 0.15
BAR_FAILS_WARN_PER_30M = 5
BAR_FAILS_FAIL_PER_30M = 30
CHAIN_FAILS_WARN_PER_30M = 10
PRICE_GAP_WARN_S = 1.15
PRICE_GAP_FAIL_S = 1.5
PRICE_GAP_RISE_WARN_S = 0.2
API_WARN_MS = 500.0
API_FAIL_MS = 2000.0
DB_CPU_WARN_AVG = 60.0
DB_CPU_FAIL_AVG = 80.0
DB_CPU_WARN_MAX = 90.0
DB_CPU_RISE_WARN = 15.0  # percentage points over the baseline


# ── cgroup and /proc parsing (pure) ─────────────────────────────────────────


def parse_bytes_or_max(text: Optional[str]) -> Optional[int]:
    """A cgroup byte value. ``max`` (no limit) and anything unreadable are None."""
    if text is None:
        return None
    value = text.strip()
    if not value or value == "max":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_flat_keyed(text: Optional[str]) -> Dict[str, int]:
    """``key value`` lines (memory.events, memory.stat, cpu.stat) as a dict of ints."""
    out: Dict[str, int] = {}
    for line in (text or "").splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                out[parts[0]] = int(parts[1])
            except ValueError:
                continue
    return out


def parse_cpu_max(text: Optional[str]) -> Optional[float]:
    """cpu.max (``300000 100000``) as cores; None when unlimited or unreadable."""
    parts = (text or "").split()
    if len(parts) != 2 or parts[0] == "max":
        return None
    try:
        quota, period = int(parts[0]), int(parts[1])
    except ValueError:
        return None
    return quota / period if period > 0 else None


def cpu_between(
    before: Dict[str, int], after: Dict[str, int], seconds: float
) -> Tuple[Optional[float], Optional[float]]:
    """Cores used, and the share of CPU periods throttled, between two cpu.stat reads."""
    if seconds <= 0 or "usage_usec" not in before or "usage_usec" not in after:
        return None, None
    used = (after["usage_usec"] - before["usage_usec"]) / (seconds * 1_000_000)
    periods = after.get("nr_periods", 0) - before.get("nr_periods", 0)
    throttled = after.get("nr_throttled", 0) - before.get("nr_throttled", 0)
    share = throttled / periods if periods > 0 else None
    return max(0.0, used), share


def private_kb_from_smaps_rollup(text: Optional[str]) -> Optional[int]:
    """Private (unshared) memory of one process, in kB, from /proc/<pid>/smaps_rollup.

    This is what a forked worker adds to its cgroup: pages it shares with the
    parent are charged once, pages it has written to are its own.
    """
    total = 0
    found = False
    for line in (text or "").splitlines():
        key, _, rest = line.partition(":")
        if key in ("Private_Clean", "Private_Dirty", "Private_Hugetlb"):
            try:
                total += int(rest.split()[0])
                found = True
            except (IndexError, ValueError):
                continue
    return total if found else None


def rss_kb_from_status(text: Optional[str]) -> Optional[int]:
    for line in (text or "").splitlines():
        if line.startswith("VmRSS:"):
            try:
                return int(line.split()[1])
            except (IndexError, ValueError):
                return None
    return None


def ppid_from_stat(text: Optional[str]) -> Optional[int]:
    """The parent PID from /proc/<pid>/stat. The command sits in parentheses
    and can contain spaces, so split after the last ``)``."""
    if not text or ")" not in text:
        return None
    fields = text.rsplit(")", 1)[1].split()
    try:
        return int(fields[1])
    except (IndexError, ValueError):
        return None


def per_symbol_worker_mb(worker_mbs: Sequence[float], symbol_count: int) -> Optional[float]:
    """What one more symbol's worker should cost, from the workers running now.

    Ingestion's children are its symbol workers plus the VIX, VXN and futures
    feeds, and nothing in /proc says which is which (they share a command
    line). The symbol workers carry the option chains, so they are the larger
    ones: take the ``symbol_count`` largest and use their median.
    """
    if not worker_mbs:
        return None
    ordered = sorted(worker_mbs, reverse=True)
    take = ordered[: max(1, min(symbol_count, len(ordered)))]
    return float(statistics.median(take))


# ── journal parsing (pure) ──────────────────────────────────────────────────

CYCLE_RE = re.compile(
    r"Cycle timing \[(?P<sym>[^\]]+)\] snapshot=\S+ phase=(?P<phase>[+-]?[\d.]+)s "
    r"duration=(?P<duration>[\d.]+)s publish_lag=(?P<lag>[+-]?[\d.]+)s"
)
LOOP_RE = re.compile(
    r"Loop timing \[(?P<sym>[^\]]+)\] calc=(?P<calc>[\d.]+)s flow=(?P<flow>[\d.]+)s "
    r"sleep=(?P<sleep>[\d.]+)s interval=(?P<interval>\d+)s(?P<overrun> OVERRUN)?"
)
BAR_FAIL_RE = re.compile(r"thetadata bar poll for (?P<sym>\S+) failed")
CHAIN_FAIL_RE = re.compile(r"thetadata poll failed \(")
SNAPSHOT_FAIL_RE = re.compile(r"thetadata (?P<kind>\w+) snapshot failed for (?P<root>\S+) ")
WORKER_DIED_RE = re.compile(r"Ingestion worker (?P<name>\S+) (?:exited unexpectedly|has died)")
ERROR_TOKEN = " - ERROR - "
WARNING_TOKEN = " - WARNING - "


def _quantile(values: Sequence[float], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    return float(ordered[min(len(ordered) - 1, int(q * len(ordered)))])


def parse_analytics_lines(
    lines: Iterable[str], canonical: Callable[[str], str] = lambda s: s
) -> Dict[str, Dict[str, Any]]:
    """Per symbol: its cycles' durations, its loops' busy time (calc + flow), overruns."""
    out: Dict[str, Dict[str, Any]] = {}

    def slot(sym: str) -> Dict[str, Any]:
        return out.setdefault(
            canonical(sym),
            {"durations": [], "busy": [], "loops": 0, "overruns": 0, "interval": None},
        )

    for line in lines:
        m = CYCLE_RE.search(line)
        if m:
            slot(m["sym"])["durations"].append(float(m["duration"]))
            continue
        m = LOOP_RE.search(line)
        if m:
            s = slot(m["sym"])
            s["busy"].append(float(m["calc"]) + float(m["flow"]))
            s["loops"] += 1
            s["interval"] = int(m["interval"])
            if m["overrun"]:
                s["overruns"] += 1
    return out


def summarize_analytics(parsed: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    summary: Dict[str, Dict[str, Any]] = {}
    for sym, s in parsed.items():
        durations, busy = s["durations"], s["busy"]
        summary[sym] = {
            "cycles": len(durations),
            "duration_median_s": float(statistics.median(durations)) if durations else None,
            "duration_p95_s": _quantile(durations, 0.95),
            "duration_max_s": max(durations) if durations else None,
            "loops": s["loops"],
            "busy_p95_s": _quantile(busy, 0.95),
            "overruns": s["overruns"],
            "overrun_share": (s["overruns"] / s["loops"]) if s["loops"] else None,
            "interval_s": s["interval"],
        }
    return summary


def parse_ingestion_lines(
    lines: Iterable[str], canonical: Callable[[str], str] = lambda s: s
) -> Dict[str, Any]:
    """ThetaData failed calls (price polls by symbol, chain polls, snapshot calls
    by option root) and ingestion worker deaths."""
    bar_fails: Dict[str, int] = {}
    snapshot_fails: Dict[str, int] = {}
    chain_fails = 0
    worker_deaths: List[str] = []
    lines_seen = 0
    for line in lines:
        lines_seen += 1
        m = BAR_FAIL_RE.search(line)
        if m:
            sym = canonical(m["sym"])
            bar_fails[sym] = bar_fails.get(sym, 0) + 1
            continue
        if CHAIN_FAIL_RE.search(line):
            chain_fails += 1
            continue
        m = SNAPSHOT_FAIL_RE.search(line)
        if m:
            snapshot_fails[m["root"]] = snapshot_fails.get(m["root"], 0) + 1
            continue
        m = WORKER_DIED_RE.search(line)
        if m:
            worker_deaths.append(m["name"])
    return {
        "lines": lines_seen,
        "bar_poll_failures": bar_fails,
        "chain_poll_failures": chain_fails,
        "snapshot_failures": snapshot_fails,
        "worker_deaths": worker_deaths,
    }


def count_levels(lines: Iterable[str]) -> Tuple[int, int]:
    errors = warnings = 0
    for line in lines:
        if ERROR_TOKEN in line:
            errors += 1
        elif WARNING_TOKEN in line:
            warnings += 1
    return errors, warnings


# ── readers (I/O; each returns None or empty when it cannot read) ───────────


def _read(path: Path) -> Optional[str]:
    try:
        return path.read_text()
    except OSError:
        return None


@dataclass
class UnitReading:
    unit: str
    present: bool = False
    memory_current: Optional[int] = None
    memory_peak: Optional[int] = None
    memory_max: Optional[int] = None
    memory_anon: Optional[int] = None
    swap_current: Optional[int] = None
    oom_kills: Optional[int] = None
    cpu_quota_cores: Optional[float] = None
    cpu_used_cores: Optional[float] = None
    throttled_share: Optional[float] = None
    processes: int = 0
    worker_private_mb: List[float] = field(default_factory=list)
    private_source: str = "smaps_rollup"
    per_symbol_mb: Optional[float] = None
    symbols: int = 0


def read_unit_memory(
    unit: str, root: Path = CGROUP_ROOT, proc: Path = Path("/proc")
) -> UnitReading:
    """Everything about ``unit`` except CPU, which needs two reads a sample apart."""
    reading = UnitReading(unit=unit)
    base = root / f"{unit}.service"
    if not base.is_dir():
        return reading
    reading.present = True
    reading.memory_current = parse_bytes_or_max(_read(base / "memory.current"))
    reading.memory_peak = parse_bytes_or_max(_read(base / "memory.peak"))
    reading.memory_max = parse_bytes_or_max(_read(base / "memory.max"))
    reading.memory_anon = parse_flat_keyed(_read(base / "memory.stat")).get("anon")
    reading.swap_current = parse_bytes_or_max(_read(base / "memory.swap.current"))
    reading.oom_kills = parse_flat_keyed(_read(base / "memory.events")).get("oom_kill")
    reading.cpu_quota_cores = parse_cpu_max(_read(base / "cpu.max"))

    pids: List[int] = []
    for token in (_read(base / "cgroup.procs") or "").split():
        try:
            pids.append(int(token))
        except ValueError:
            continue
    reading.processes = len(pids)
    pid_set = set(pids)
    workers: List[float] = []
    for pid in pids:
        ppid = ppid_from_stat(_read(proc / str(pid) / "stat"))
        if ppid is None or ppid not in pid_set:
            continue  # the unit's main process, not a per-symbol worker
        kb = private_kb_from_smaps_rollup(_read(proc / str(pid) / "smaps_rollup"))
        if kb is None:
            kb = rss_kb_from_status(_read(proc / str(pid) / "status"))
            if kb is not None:
                reading.private_source = "VmRSS, overstates: counts shared pages"
        if kb is not None:
            workers.append(round(kb / 1024, 1))
    reading.worker_private_mb = sorted(workers, reverse=True)
    return reading


def read_cpu_stat(unit: str, root: Path = CGROUP_ROOT) -> Dict[str, int]:
    return parse_flat_keyed(_read(root / f"{unit}.service" / "cpu.stat"))


def read_box(proc: Path = Path("/proc")) -> Dict[str, Any]:
    info: Dict[str, int] = {}
    for line in (_read(proc / "meminfo") or "").splitlines():
        key, _, rest = line.partition(":")
        try:
            info[key] = int(rest.split()[0])  # kB
        except (IndexError, ValueError):
            continue
    load = (_read(proc / "loadavg") or "").split()
    swap_used = None
    if "SwapTotal" in info and "SwapFree" in info:
        swap_used = round((info["SwapTotal"] - info["SwapFree"]) / 1024)
    return {
        "mem_total_mb": round(info["MemTotal"] / 1024) if "MemTotal" in info else None,
        "mem_available_mb": round(info["MemAvailable"] / 1024) if "MemAvailable" in info else None,
        "swap_used_mb": swap_used,
        "load_1m": float(load[0]) if load else None,
        "cpus": os.cpu_count(),
    }


def read_journal(unit: str, since_ts: float, until_ts: float) -> Optional[List[str]]:
    """The unit's journal for the window, or None if journalctl would not answer."""
    cmd = [
        "journalctl",
        "-u",
        unit,
        "--no-pager",
        "-o",
        "short-iso",
        "--since",
        f"@{int(since_ts)}",
        "--until",
        f"@{int(until_ts)}",
    ]
    try:
        proc = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if proc.returncode != 0:
        return None
    return journal_entries(proc.stdout)


def journal_entries(text: str) -> List[str]:
    """journalctl's output without its own notes ("-- No entries --", boot
    markers). A user who cannot read the system journal gets only the note and
    exit 0, and counting it as a line would hide that."""
    return [line for line in text.splitlines() if line.strip() and not line.startswith("-- ")]


def read_database(
    symbols: Sequence[str], analytics_symbols: Sequence[str], window_min: int
) -> Dict[str, Any]:
    """Per-symbol freshness and cadence, plus database-wide load. Indexed reads only."""
    from src.database.connection import db_connection

    out: Dict[str, Any] = {"per_symbol": {}, "sessions": {}, "sizes": {}}
    every = sorted(set(symbols) | set(analytics_symbols))
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.sym,
                   (SELECT max(g.timestamp) FROM gex_summary g WHERE g.underlying = u.sym),
                   (SELECT count(*) FROM gex_summary g
                     WHERE g.underlying = u.sym
                       AND g.timestamp >= now() - make_interval(mins => %s)),
                   (SELECT max(q.timestamp) FROM underlying_quotes q WHERE q.symbol = u.sym),
                   now()
            FROM unnest(%s::text[]) AS u(sym)
            """,
            (window_min, every),
        )
        for sym, gex_latest, gex_rows, bar_latest, now in cur.fetchall():
            out["per_symbol"][sym] = {
                "gex_rows": int(gex_rows),
                "gex_age_s": (now - gex_latest).total_seconds() if gex_latest else None,
                "bar_age_s": (now - bar_latest).total_seconds() if bar_latest else None,
            }
        cur.execute(
            """
            SELECT underlying,
                   count(*),
                   count(*) FILTER (WHERE timestamp >= now() - interval '2 minutes')
            FROM option_chains_latest
            WHERE underlying = ANY(%s::text[]) AND expiration >= current_date
            GROUP BY underlying
            """,
            (every,),
        )
        for sym, contracts, fresh in cur.fetchall():
            row = out["per_symbol"].setdefault(sym, {})
            row["contracts"] = int(contracts)
            row["chain_fresh_share"] = (fresh / contracts) if contracts else None
        cur.execute("""
            SELECT count(*),
                   count(*) FILTER (WHERE state = 'active'),
                   count(*) FILTER (WHERE wait_event_type IN ('Lock', 'LWLock', 'IO', 'BufferPin')),
                   current_setting('max_connections')::int
            FROM pg_stat_activity WHERE datname = current_database()
            """)
        total, active, waiting, max_conn = cur.fetchone()
        out["sessions"] = {
            "connections": int(total),
            "active": int(active),
            "waiting": int(waiting),
            "max_connections": int(max_conn),
        }
        cur.execute(
            "SELECT pg_database_size(current_database()), pg_total_relation_size('option_chains')"
        )
        db_bytes, chains_bytes = cur.fetchone()
        out["sizes"] = {
            "database_mb": round(db_bytes / MB),
            "option_chains_mb": round(chains_bytes / MB),
        }
    return out


def time_api(
    path: str, key: Optional[str], tries: int = 5, base: str = "http://127.0.0.1:8000"
) -> Dict[str, Any]:
    """Wall time of a few sequential reads of one hot endpoint, the way the
    website's server reads it for a customer."""
    if not key:
        return {"path": path, "skipped": "no OPS_API_KEY in .env"}
    timings: List[float] = []
    statuses: List[int] = []
    for _ in range(tries):
        request = urllib.request.Request(base + path, headers={"X-API-Key": key})
        started = time.monotonic()
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                response.read()
                statuses.append(response.status)
        except urllib.error.HTTPError as e:
            statuses.append(e.code)
        except (urllib.error.URLError, OSError):
            statuses.append(0)
        timings.append((time.monotonic() - started) * 1000)
    if statuses and all(s in (401, 403) for s in statuses):
        return {"path": path, "skipped": f"key refused ({statuses[0]})"}
    ok = [t for t, s in zip(timings, statuses) if s == 200]
    return {
        "path": path,
        "ok": len(ok),
        "tries": tries,
        "median_ms": float(statistics.median(ok)) if ok else None,
        "max_ms": max(ok) if ok else None,
        "statuses": sorted(set(statuses)),
    }


def rds_instance_from_host(host: Optional[str]) -> Optional[str]:
    """The RDS instance identifier is the first label of its endpoint name."""
    name = (host or "").strip().lower()
    if name.endswith(".rds.amazonaws.com") and name.count(".") >= 4:
        return name.split(".", 1)[0]
    return None


def read_rds_cpu(instance: str, region: str, minutes: int) -> Dict[str, Any]:
    """The database instance's CPU over the window, from CloudWatch.

    The database is a 2-vCPU instance shared by every service: each stock adds
    its option rows, written every few seconds, and an analytics read a
    minute. Nothing inside Postgres reports its CPU, so this asks CloudWatch,
    with the box's own AWS credentials. Without permission it says so.
    """
    from datetime import timedelta, timezone

    import boto3
    from botocore.config import Config

    client = boto3.client(
        "cloudwatch",
        region_name=region,
        config=Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 1}),
    )
    end = datetime.now(timezone.utc)
    response = client.get_metric_statistics(
        Namespace="AWS/RDS",
        MetricName="CPUUtilization",
        Dimensions=[{"Name": "DBInstanceIdentifier", "Value": instance}],
        StartTime=end - timedelta(minutes=minutes),
        EndTime=end,
        Period=60,
        Statistics=["Average", "Maximum"],
    )
    points = response.get("Datapoints") or []
    if not points:
        return {"instance": instance, "error": "CloudWatch returned no CPU datapoints"}
    return {
        "instance": instance,
        "avg_pct": round(statistics.mean(p["Average"] for p in points), 1),
        "max_pct": round(max(p["Maximum"] for p in points), 1),
        "minutes": len(points),
    }


# ── gates (pure) ────────────────────────────────────────────────────────────


@dataclass
class Gate:
    level: str  # PASS | WARN | FAIL
    area: str
    message: str


def _per_30(count: float, window_min: int) -> float:
    return count * 30.0 / window_min if window_min > 0 else float(count)


def evaluate(report: Dict[str, Any], baseline: Optional[Dict[str, Any]] = None) -> List[Gate]:
    """Every WARN and FAIL, worst first, then one PASS per area that came out clean."""
    gates: List[Gate] = []
    clean: Dict[str, bool] = {}

    def hit(level: str, area: str, message: str) -> None:
        gates.append(Gate(level, area, message))
        clean[area] = False

    def checked(area: str) -> None:
        clean.setdefault(area, True)

    in_session = report.get("session") == "regular"
    whole_window = bool(report.get("window_in_session"))
    window = int(report.get("window_minutes") or 30)
    base_units = {u["unit"]: u for u in (baseline or {}).get("units", [])}

    # Memory: the projection for the next symbol, and any OOM kill.
    for unit in report.get("units", []):
        if not unit.get("present"):
            continue
        name = unit["unit"]
        if unit.get("oom_kills"):
            prior = base_units.get(name, {}).get("oom_kills")
            since = f", baseline {prior}" if prior is not None else ""
            hit(
                "FAIL", "memory", f"{name}: {unit['oom_kills']} OOM kill(s) since it started{since}"
            )
        cap = unit.get("memory_max")
        if not cap:
            continue
        checked("memory")
        high = unit.get("memory_peak") or unit.get("memory_current") or 0
        if name in PER_SYMBOL_UNITS and unit.get("per_symbol_mb"):
            projected = high / MB + unit["per_symbol_mb"]
            pct = projected * MB / cap * 100
            what = (
                f"{name}: peak {high / MB:,.0f} MB + one more symbol"
                f" ~{unit['per_symbol_mb']:,.0f} MB"
                f" = {projected:,.0f} MB, {pct:.0f}% of its {cap / MB:,.0f} MB cap"
            )
            if pct >= MEMORY_FAIL_PCT:
                hit("FAIL", "memory", what)
            elif pct >= MEMORY_WARN_PCT:
                hit("WARN", "memory", what)
        else:
            # A stock adds no process here, so a full unit is worth knowing about
            # but is not a reason to hold the stock.
            pct = high / cap * 100
            if pct >= OTHER_UNIT_WARN_PCT:
                anon = unit.get("memory_anon")
                held = f" ({anon / MB:,.0f} MB of it is process memory)" if anon else ""
                hit(
                    "WARN",
                    "memory",
                    f"{name}: peak {high / MB:,.0f} MB,"
                    f" {pct:.0f}% of its {cap / MB:,.0f} MB cap{held}",
                )

    box = report.get("box") or {}
    available = box.get("mem_available_mb")
    if available is not None:
        checked("box")
        if available < BOX_AVAILABLE_FAIL_MB:
            hit("FAIL", "box", f"only {available:,} MB of the box's memory is available")
        elif available < BOX_AVAILABLE_WARN_MB:
            hit("WARN", "box", f"only {available:,} MB of the box's memory is available")

    # CPU: informational unless pinned; the cycle gates below are what customers feel.
    for unit in report.get("units", []):
        quota, used = unit.get("cpu_quota_cores"), unit.get("cpu_used_cores")
        if unit["unit"] not in PER_SYMBOL_UNITS or not quota or used is None:
            continue
        checked("cpu")
        if used >= quota * CPU_PINNED_SHARE:
            hit(
                "WARN",
                "cpu",
                f"{unit['unit']}: {used:.2f} of its {quota:.2f} cores in use (pinned at its quota)",
            )
        share = unit.get("throttled_share")
        if share is not None and share >= THROTTLED_WARN_SHARE:
            hit("WARN", "cpu", f"{unit['unit']}: throttled in {share:.0%} of CPU periods")

    if not in_session:
        return _finish(gates, clean, report)

    # Analytics: each symbol's minute of work has to fit in its minute.
    analytics = report.get("analytics")
    base_analytics = (baseline or {}).get("analytics") or {}
    if analytics is not None and not any(a.get("loops") for a in analytics.values()):
        hit(
            "WARN",
            "analytics",
            "no analytics cycle lines in the journal window; check that this user can read it "
            "(journalctl -u zerogex-oa-analytics -n 5)",
        )
    for sym, a in sorted((analytics or {}).items()):
        if not a.get("loops"):
            continue
        checked("analytics")
        interval = a.get("interval_s") or 60
        share = a.get("overrun_share") or 0.0
        prior_share = (base_analytics.get(sym) or {}).get("overrun_share") or 0.0
        overran = f"{sym}: {a['overruns']} of {a['loops']} cycles ran past the {interval}s interval"
        if share >= OVERRUN_FAIL_SHARE or (baseline and share - prior_share >= OVERRUN_RISE_FAIL):
            hit(
                "FAIL",
                "analytics",
                overran + (f" (baseline {prior_share:.0%})" if baseline else ""),
            )
        elif share > 0 and share > prior_share:
            hit("WARN", "analytics", overran)
        busy = a.get("busy_p95_s")
        if busy is not None and busy >= interval * BUSY_WARN_SHARE:
            hit(
                "WARN",
                "analytics",
                f"{sym}: 95th-percentile cycle work {busy:.1f}s of a {interval}s interval",
            )
        prior_median = (base_analytics.get(sym) or {}).get("duration_median_s")
        median = a.get("duration_median_s")
        if (
            prior_median
            and median
            and median >= prior_median * 1.5
            and median - prior_median >= 2.0
        ):
            hit(
                "WARN",
                "analytics",
                f"{sym}: median cycle {median:.1f}s, up from {prior_median:.1f}s",
            )

    # Freshness: what a customer reads.
    base_db = ((baseline or {}).get("database") or {}).get("per_symbol") or {}
    analytics_symbols = set(report.get("analytics_symbols") or [])
    ingest_symbols = set(report.get("ingest_symbols") or [])
    for sym, row in sorted(((report.get("database") or {}).get("per_symbol") or {}).items()):
        checked("freshness")
        if sym in analytics_symbols:
            rows = row.get("gex_rows") or 0
            prior_rows = (base_db.get(sym) or {}).get("gex_rows")
            counted = f"{sym}: {rows} GEX snapshots in {window} min"
            if not whole_window:
                pass  # the window starts before the open; the count means nothing
            elif prior_rows:
                drop = 1 - rows / prior_rows
                if drop >= GEX_ROWS_DROP_FAIL:
                    hit("FAIL", "freshness", f"{counted}, down from {prior_rows}")
                elif drop >= GEX_ROWS_DROP_WARN:
                    hit("WARN", "freshness", f"{counted}, down from {prior_rows}")
            elif rows < window * GEX_ROWS_FAIL_SHARE:
                hit("FAIL", "freshness", f"{counted} (one a minute is normal)")
            elif rows < window * GEX_ROWS_WARN_SHARE:
                hit("WARN", "freshness", f"{counted} (one a minute is normal)")
            age = row.get("gex_age_s")
            if age is None or age >= GEX_AGE_FAIL_S:
                hit("FAIL", "freshness", f"{sym}: newest GEX snapshot is {_age(age)} old")
            elif age >= GEX_AGE_WARN_S:
                hit("WARN", "freshness", f"{sym}: newest GEX snapshot is {_age(age)} old")
        if sym in ingest_symbols:
            bar_age = row.get("bar_age_s")
            if bar_age is None or bar_age >= BAR_AGE_FAIL_S:
                hit("FAIL", "freshness", f"{sym}: newest price bar is {_age(bar_age)} old")
            elif bar_age >= BAR_AGE_WARN_S:
                hit("WARN", "freshness", f"{sym}: newest price bar is {_age(bar_age)} old")
        fresh = row.get("chain_fresh_share")
        prior_fresh = (base_db.get(sym) or {}).get("chain_fresh_share")
        if (
            fresh is not None
            and prior_fresh is not None
            and prior_fresh - fresh >= CHAIN_FRESH_DROP_WARN
        ):
            hit(
                "WARN",
                "freshness",
                f"{sym}: {fresh:.0%} of its contracts updated in the last 2 min,"
                f" down from {prior_fresh:.0%}",
            )

    # ThetaData: the shared request budget.
    ing = report.get("ingestion")
    if ing is not None:
        checked("thetadata")
        if not ing.get("lines"):
            hit(
                "WARN",
                "thetadata",
                "the ingestion journal window is empty; check that this user can read it "
                "(journalctl -u zerogex-oa-ingestion -n 5)",
            )
        base_ing = (baseline or {}).get("ingestion") or {}
        bars = sum(ing["bar_poll_failures"].values())
        chains = ing["chain_poll_failures"] + sum(ing["snapshot_failures"].values())
        prior_bars = sum((base_ing.get("bar_poll_failures") or {}).values())
        prior_chains = (base_ing.get("chain_poll_failures") or 0) + sum(
            (base_ing.get("snapshot_failures") or {}).values()
        )
        named = ", ".join(f"{s} {n}" for s, n in sorted(ing["bar_poll_failures"].items()))
        was = f", baseline {prior_bars}" if baseline else ""
        if _per_30(bars, window) >= BAR_FAILS_FAIL_PER_30M:
            hit("FAIL", "thetadata", f"{bars} failed price polls in {window} min{was} ({named})")
        elif _per_30(bars, window) >= BAR_FAILS_WARN_PER_30M or (
            baseline and bars > 2 * prior_bars + 5
        ):
            hit("WARN", "thetadata", f"{bars} failed price polls in {window} min{was} ({named})")
        if _per_30(chains, window) >= CHAIN_FAILS_WARN_PER_30M or (
            baseline and chains > 2 * prior_chains + 5
        ):
            was = f", baseline {prior_chains}" if baseline else ""
            hit("WARN", "thetadata", f"{chains} failed chain calls in {window} min{was}")
        if ing["worker_deaths"]:
            hit("FAIL", "thetadata", "ingestion worker(s) died: " + ", ".join(ing["worker_deaths"]))

    # Live price: listed securities should move about once a second.
    base_cadence = {c["symbol"]: c for c in (baseline or {}).get("cadence") or []}
    listed = set(report.get("listed_symbols") or [])
    for c in report.get("cadence") or []:
        if c["symbol"] not in listed or c.get("median_gap") is None:
            continue
        checked("live price")
        gap = c["median_gap"]
        prior = (base_cadence.get(c["symbol"]) or {}).get("median_gap")
        moves = f"{c['symbol']}: price moves every {gap:.2f}s (median)"
        if gap >= PRICE_GAP_FAIL_S:
            hit("FAIL", "live price", f"{moves}, normal is about 1s")
        elif gap >= PRICE_GAP_WARN_S or (
            prior is not None and gap - prior >= PRICE_GAP_RISE_WARN_S
        ):
            hit("WARN", "live price", moves + (f", was {prior:.2f}s" if prior is not None else ""))

    api = report.get("api")
    if api and api.get("tries"):
        checked("api")
        prior = ((baseline or {}).get("api") or {}).get("median_ms")
        median = api.get("median_ms")
        if median is None:
            hit(
                "FAIL",
                "api",
                f"{api['path']}: no successful read in {api['tries']} (statuses {api['statuses']})",
            )
        elif median >= API_FAIL_MS:
            hit("FAIL", "api", f"{api['path']}: median {median:.0f} ms")
        elif median >= API_WARN_MS or (prior and median >= 2 * prior and median - prior >= 100):
            hit(
                "WARN",
                "api",
                f"{api['path']}: median {median:.0f} ms"
                + (f", was {prior:.0f} ms" if prior else ""),
            )

    # The database's CPU, when CloudWatch answered.
    cpu = report.get("db_cpu") or {}
    if cpu.get("avg_pct") is not None:
        checked("database")
        avg, peak = cpu["avg_pct"], cpu["max_pct"]
        prior = ((baseline or {}).get("db_cpu") or {}).get("avg_pct")
        said = f"database CPU averaged {avg:.0f}% (peak {peak:.0f}%) over {cpu['minutes']} min"
        was = f", baseline {prior:.0f}%" if prior is not None else ""
        if avg >= DB_CPU_FAIL_AVG:
            hit("FAIL", "database", said + was)
        elif avg >= DB_CPU_WARN_AVG or peak >= DB_CPU_WARN_MAX:
            hit("WARN", "database", said + was)
        elif prior is not None and avg - prior >= DB_CPU_RISE_WARN:
            hit("WARN", "database", said + was)

    return _finish(gates, clean, report)


def _finish(gates: List[Gate], clean: Dict[str, bool], report: Dict[str, Any]) -> List[Gate]:
    order = {"FAIL": 0, "WARN": 1, "PASS": 2}
    for area, ok in clean.items():
        if ok:
            gates.append(Gate("PASS", area, "no problem found"))
    if report.get("session") != "regular":
        gates.append(
            Gate(
                "WARN",
                "session",
                "outside the regular session, so only memory, box and CPU were judged. "
                "Run it again between 10:00 and 15:30 ET.",
            )
        )
    elif not report.get("window_in_session"):
        gates.append(
            Gate(
                "WARN",
                "session",
                "the window starts before the 09:30 open, so GEX snapshot counts were not judged. "
                "Run it again after 10:00 ET.",
            )
        )
    return sorted(gates, key=lambda g: (order[g.level], g.area))


def _age(seconds: Optional[float]) -> str:
    if seconds is None:
        return "missing"
    if seconds < 120:
        return f"{seconds:.0f}s"
    if seconds < 7200:
        return f"{seconds / 60:.0f} min"
    return f"{seconds / 3600:.1f} h"


# ── report text ─────────────────────────────────────────────────────────────


def _mb(value: Optional[int]) -> str:
    return f"{value / MB:,.0f}" if value else "-"


def _n(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, (int, float)):
        return f"{value:,}"
    return str(value)


def _delta(now: Optional[float], prior: Optional[float], fmt: str = "{:+,.0f}") -> str:
    if now is None or prior is None:
        return ""
    return " (" + fmt.format(now - prior) + ")"


def render(
    report: Dict[str, Any], gates: List[Gate], baseline: Optional[Dict[str, Any]] = None
) -> str:
    lines: List[str] = []
    add = lines.append
    base_units = {u["unit"]: u for u in (baseline or {}).get("units", [])}
    window = report["window_minutes"]
    add(
        f"ZeroGEX capacity check  {report['taken_at_et']} ET  "
        f"({report['session']} session, last {window} min)"
    )
    add(f"Ingesting: {', '.join(report['ingest_symbols'])}")
    add(f"Analytics: {', '.join(report['analytics_symbols'])}")
    if baseline:
        add(
            f"Compared with: {baseline.get('taken_at_et')} ET"
            f" ({baseline.get('label') or 'no label'});"
            " changes in brackets"
        )
    add("")

    add("Services: memory is the whole unit, shared by every process in it")
    add(
        f"  {'unit':<26}{'now MB':>8}{'peak MB':>14}{'cap MB':>8}{'procs':>6}"
        f"{'+1 symbol':>11}{'cores used/quota':>18}{'throttled':>10}"
    )
    for u in report["units"]:
        if not u.get("present"):
            add(f"  {u['unit']:<26}  not running")
            continue
        prior = base_units.get(u["unit"], {})
        plus = f"~{u['per_symbol_mb']:,.0f} MB" if u.get("per_symbol_mb") else ""
        cpu = (
            f"{u['cpu_used_cores']:.2f}/{u['cpu_quota_cores']:.2f}"
            if u.get("cpu_used_cores") is not None and u.get("cpu_quota_cores")
            else "-"
        )
        throttled = f"{u['throttled_share']:.0%}" if u.get("throttled_share") is not None else "-"
        peak_now = u["memory_peak"] / MB if u.get("memory_peak") else None
        peak_then = prior["memory_peak"] / MB if prior.get("memory_peak") else None
        peak = _mb(u.get("memory_peak")) + _delta(peak_now, peak_then)
        add(
            f"  {u['unit']:<26}{_mb(u.get('memory_current')):>8}{peak:>14}"
            f"{_mb(u.get('memory_max')):>8}"
            f"{u['processes']:>6}{plus:>11}{cpu:>18}{throttled:>10}"
        )
        if u["unit"] in PER_SYMBOL_UNITS and u.get("worker_private_mb"):
            each = ", ".join(f"{m:,.0f}" for m in u["worker_private_mb"])
            add(f"    worker processes' private MB: {each} ({u['private_source']})")
    box = report.get("box") or {}
    add(
        f"  box: {_n(box.get('mem_available_mb'))} MB available of {_n(box.get('mem_total_mb'))}, "
        f"swap used {_n(box.get('swap_used_mb'))} MB, load {_n(box.get('load_1m'))} on "
        f"{_n(box.get('cpus'))} CPUs"
    )
    add("")

    db = report.get("database") or {}
    if db.get("error"):
        add(f"Database: unreadable ({db['error']})")
    else:
        base_db = ((baseline or {}).get("database") or {}).get("per_symbol") or {}
        analytics = report.get("analytics") or {}
        base_an = (baseline or {}).get("analytics") or {}
        add(f"Per symbol (GEX rows = snapshots published in {window} min; one a minute is normal)")
        add(
            f"  {'symbol':<8}{'GEX rows':>12}{'GEX age':>9}{'bar age':>9}{'contracts':>10}"
            f"{'fresh 2m':>9}{'cycle median':>16}{'p95':>7}{'overruns':>10}"
        )
        for sym, row in sorted((db.get("per_symbol") or {}).items()):
            a = analytics.get(sym) or {}
            prior = base_db.get(sym) or {}
            rows = _n(row.get("gex_rows")) + _delta(row.get("gex_rows"), prior.get("gex_rows"))
            fresh = (
                f"{row['chain_fresh_share']:.0%}"
                if row.get("chain_fresh_share") is not None
                else "-"
            )
            med = a.get("duration_median_s")
            prior_med = (base_an.get(sym) or {}).get("duration_median_s")
            med_s = (f"{med:.1f}s" if med is not None else "-") + _delta(med, prior_med, "{:+.1f}")
            p95 = f"{a['duration_p95_s']:.1f}s" if a.get("duration_p95_s") is not None else "-"
            over = f"{a['overruns']}/{a['loops']}" if a.get("loops") else "-"
            add(
                f"  {sym:<8}{rows:>12}{_age(row.get('gex_age_s')):>9}"
                f"{_age(row.get('bar_age_s')):>9}"
                f"{_n(row.get('contracts')):>10}{fresh:>9}{med_s:>16}{p95:>7}{over:>10}"
            )
        s = db.get("sessions") or {}
        z = db.get("sizes") or {}
        add(
            f"  database: {_n(s.get('connections'))} connections of"
            f" {_n(s.get('max_connections'))}, "
            f"{_n(s.get('active'))} active, {_n(s.get('waiting'))} waiting; size "
            f"{_n(z.get('database_mb'))} MB (option_chains {_n(z.get('option_chains_mb'))} MB)"
        )
    db_cpu: Dict[str, Any] = report.get("db_cpu") or {}
    if db_cpu.get("avg_pct") is not None:
        prior_cpu = ((baseline or {}).get("db_cpu") or {}).get("avg_pct")
        add(
            f"  database CPU ({db_cpu['instance']}, CloudWatch): average {db_cpu['avg_pct']:.0f}%"
            f"{_delta(db_cpu['avg_pct'], prior_cpu, '{:+.0f} pts')}, peak {db_cpu['max_pct']:.0f}%"
            f" over {db_cpu['minutes']} min"
        )
    elif db_cpu.get("error"):
        add(f"  database CPU: unavailable ({db_cpu['error']}); read it in the RDS console instead")
    add("")

    ing = report.get("ingestion")
    if ing is None:
        add("ThetaData: the ingestion journal is unreadable")
    else:
        bars = ", ".join(f"{s} {n}" for s, n in sorted(ing["bar_poll_failures"].items())) or "none"
        snaps = ", ".join(f"{r} {n}" for r, n in sorted(ing["snapshot_failures"].items())) or "none"
        add(
            f"ThetaData failed calls in {window} min: price polls {bars}; "
            f"chain polls {ing['chain_poll_failures']}; snapshot calls {snaps}"
        )
        if ing["worker_deaths"]:
            add(f"  ingestion worker deaths: {', '.join(ing['worker_deaths'])}")
    levels = report.get("journal_levels") or {}
    if levels:
        add(
            "Journal: "
            + "; ".join(
                f"{u.replace('zerogex-oa-', '')} {e} errors / {w} warnings"
                for u, (e, w) in levels.items()
            )
        )
    add("")

    cadence = report.get("cadence")
    if cadence:
        base_c = {c["symbol"]: c for c in (baseline or {}).get("cadence") or []}
        add(
            f"Live price, {report.get('cadence_seconds', 0):.0f}s of the quote channel "
            "(listed symbols should move about once a second)"
        )
        for c in cadence:
            prior = (base_c.get(c["symbol"]) or {}).get("median_gap")
            gap = f"{c['median_gap']:.2f}s" if c.get("median_gap") is not None else "-"
            worst = f"{c['max_gap']:.1f}s" if c.get("max_gap") is not None else "-"
            add(
                f"  {c['symbol']:<8}{c['updates']:>5} updates, median gap {gap}"
                f"{_delta(c.get('median_gap'), prior, '{:+.2f}s')}, worst {worst}, "
                f"{c['slow_gaps']} gaps over 1.5s"
            )
    elif report.get("cadence_error"):
        add(f"Live price: unreadable ({report['cadence_error']})")
    elif report.get("session") == "regular":
        add("Live price: no announcements heard")
    api = report.get("api")
    if api and api.get("skipped"):
        add(f"API {api['path']}: skipped ({api['skipped']})")
    elif api:
        med = f"{api['median_ms']:.0f} ms" if api.get("median_ms") is not None else "failed"
        prior = ((baseline or {}).get("api") or {}).get("median_ms")
        add(
            f"API {api['path']}: {api['ok']}/{api['tries']} ok, median {med}"
            f"{_delta(api.get('median_ms'), prior, '{:+.0f} ms')}"
        )
    add("")

    add("Verdict")
    for g in gates:
        add(f"  {g.level:<5} {g.area:<11} {g.message}")
    add("")
    add(verdict(gates))
    return "\n".join(lines)


def verdict(gates: Sequence[Gate]) -> str:
    """The one line that answers "can the next symbol go in?"."""
    if any(g.level == "FAIL" for g in gates):
        return "HOLD: do not add the next symbol until each FAIL above is understood."
    if any(g.level == "WARN" and g.area == "session" for g in gates):
        return (
            "NOT JUDGED YET: nothing failed, but the cycle, freshness and live-price gates need "
            "a run between 10:00 and 15:30 ET."
        )
    if any(g.level == "WARN" for g in gates):
        return "LOOK FIRST: nothing failed, but read each WARN before adding the next symbol."
    return "GO: nothing failed or warned."


# ── orchestration ───────────────────────────────────────────────────────────


def _canonical_list(raw: str) -> List[str]:
    from src.symbols import get_canonical_symbol, parse_underlyings

    return [get_canonical_symbol(s) for s in parse_underlyings(raw)] if raw else []


def window_in_session(taken: datetime, window_minutes: int) -> bool:
    """True when the whole window falls after the 09:30 ET open of ``taken``'s day."""
    start = taken.astimezone(ET).timestamp() - window_minutes * 60
    open_ = taken.astimezone(ET).replace(hour=9, minute=30, second=0, microsecond=0)
    return start >= open_.timestamp()


def collect(args: argparse.Namespace) -> Dict[str, Any]:
    from src.market_calendar import get_market_session
    from src.symbols import asset_class, get_canonical_symbol

    now = time.time()
    since = now - args.window_minutes * 60
    ingest = _canonical_list(
        os.getenv("INGEST_UNDERLYINGS") or os.getenv("INGEST_UNDERLYING") or "SPY"
    )
    analytics = _canonical_list(
        os.getenv("ANALYTICS_UNDERLYINGS") or os.getenv("ANALYTICS_UNDERLYING") or "SPY"
    )
    taken = datetime.now(ET)
    report: Dict[str, Any] = {
        "schema": SCHEMA_VERSION,
        "label": args.label,
        "taken_at_et": taken.strftime("%Y-%m-%d %H:%M"),
        "taken_at_epoch": now,
        "session": get_market_session(taken),
        "window_minutes": args.window_minutes,
        "window_in_session": window_in_session(taken, args.window_minutes),
        "ingest_symbols": ingest,
        "analytics_symbols": analytics,
        "listed_symbols": [s for s in ingest if asset_class(s) in ("ETF", "STOCK")],
    }

    units = [read_unit_memory(u) for u in PER_SYMBOL_UNITS + OTHER_UNITS]
    for reading in units:
        if reading.unit == "zerogex-oa-ingestion":
            reading.symbols = len(ingest)
        elif reading.unit == "zerogex-oa-analytics":
            reading.symbols = len(analytics)
        if reading.unit in PER_SYMBOL_UNITS:
            reading.per_symbol_mb = per_symbol_worker_mb(reading.worker_private_mb, reading.symbols)

    # One CPU sample, spanning the live-price listen so the run takes no longer.
    cpu_before = {u.unit: read_cpu_stat(u.unit) for u in units if u.present}
    started = time.monotonic()
    cadence: List[Dict[str, Any]] = []
    if args.cadence_seconds > 0 and report["session"] in ("regular", "pre-market", "after-hours"):
        try:
            from src.tools.quote_cadence import listen, summarize

            for row in summarize(listen(args.cadence_seconds), args.cadence_seconds):
                entry = {
                    k: (None if isinstance(v, float) and v != v else v)
                    for k, v in row._asdict().items()
                }
                entry["symbol"] = get_canonical_symbol(str(entry["symbol"]))
                cadence.append(entry)
        except Exception as e:  # noqa: BLE001 - one section must not sink the report
            report["cadence_error"] = f"{type(e).__name__}: {e}"
    else:
        time.sleep(max(1.0, args.cpu_seconds))
    elapsed = time.monotonic() - started
    for reading in units:
        if reading.present:
            after = read_cpu_stat(reading.unit)
            reading.cpu_used_cores, reading.throttled_share = cpu_between(
                cpu_before[reading.unit], after, elapsed
            )
    report["units"] = [asdict(u) for u in units]
    report["cadence"] = cadence
    report["cadence_seconds"] = args.cadence_seconds
    report["box"] = read_box()

    journals = {u: read_journal(u, since, now) for u in JOURNAL_UNITS}
    report["journal_levels"] = {
        u: count_levels(lines) for u, lines in journals.items() if lines is not None
    }
    analytics_journal = journals.get("zerogex-oa-analytics")
    if analytics_journal is not None:
        report["analytics"] = summarize_analytics(
            parse_analytics_lines(analytics_journal, get_canonical_symbol)
        )
    ingestion_journal = journals.get("zerogex-oa-ingestion")
    if ingestion_journal is not None:
        report["ingestion"] = parse_ingestion_lines(ingestion_journal, get_canonical_symbol)

    try:
        report["database"] = read_database(ingest, analytics, args.window_minutes)
    except Exception as e:  # noqa: BLE001
        report["database"] = {"error": f"{type(e).__name__}: {e}"}

    if args.api_tries > 0:
        report["api"] = time_api(
            f"/api/gex/summary?symbol={args.api_symbol}", os.getenv("OPS_API_KEY"), args.api_tries
        )

    instance = args.rds_instance or rds_instance_from_host(os.getenv("DB_HOST"))
    if instance:
        try:
            region = os.getenv("AWS_REGION") or "us-east-1"
            report["db_cpu"] = read_rds_cpu(instance, region, args.window_minutes)
        except Exception as e:  # noqa: BLE001 - no CloudWatch permission is not a failure
            report["db_cpu"] = {"instance": instance, "error": f"{type(e).__name__}: {e}"[:200]}
    else:
        report["db_cpu"] = {"error": "DB_HOST is not an RDS endpoint name; pass --rds-instance"}
    return report


def save(report: Dict[str, Any], directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromtimestamp(report["taken_at_epoch"], ET).strftime("%Y-%m-%d-%H%M")
    label = ""
    if report.get("label"):
        label = "-" + "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in report["label"])
    path = directory / f"{stamp}{label}.json"
    path.write_text(json.dumps(report, indent=1, default=str))
    return path


def main(argv: Optional[List[str]] = None) -> int:
    import logging

    from dotenv import load_dotenv

    load_dotenv()
    # The pool's connect banner is noise in a report meant to be pasted.
    for name in ("src.database.connection", "src.database.password_providers"):
        logging.getLogger(name).setLevel(logging.WARNING)
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--window-minutes", type=int, default=30, help="journal and database window (default 30)"
    )
    parser.add_argument(
        "--cadence-seconds",
        type=float,
        default=30.0,
        help="live price listen, 0 to skip (default 30)",
    )
    parser.add_argument(
        "--cpu-seconds", type=float, default=10.0, help="CPU sample when not listening (default 10)"
    )
    parser.add_argument(
        "--api-tries", type=int, default=5, help="timed API reads, 0 to skip (default 5)"
    )
    parser.add_argument("--api-symbol", default="SPY")
    parser.add_argument(
        "--rds-instance",
        default=os.getenv("RDS_INSTANCE_ID", ""),
        help="RDS instance for the database CPU read (default: from DB_HOST)",
    )
    parser.add_argument(
        "--label", default="", help="a word saved with the run, e.g. baseline or after-msft"
    )
    parser.add_argument("--baseline", default="", help="a saved run to compare against")
    parser.add_argument(
        "--save-dir",
        default="~/monitoring/capacity",
        help="where runs are saved (default ~/monitoring/capacity)",
    )
    parser.add_argument("--no-save", action="store_true")
    parser.add_argument("--json", action="store_true", help="print the raw report as JSON")
    args = parser.parse_args(argv)

    baseline = None
    if args.baseline:
        try:
            baseline = json.loads(Path(args.baseline).expanduser().read_text())
        except (OSError, ValueError) as e:
            print(f"Could not read the baseline {args.baseline}: {e}", file=sys.stderr)
            return 2

    report = collect(args)
    gates = evaluate(report, baseline)
    report["gates"] = [asdict(g) for g in gates]
    if args.json:
        print(json.dumps(report, indent=1, default=str))
    else:
        print(render(report, gates, baseline))
    if not args.no_save:
        try:
            print(f"\nSaved: {save(report, Path(args.save_dir).expanduser())}")
        except OSError as e:
            print(f"\nNot saved ({e})", file=sys.stderr)
    if (report.get("database") or {}).get("error"):
        return 2
    return 1 if any(g.level == "FAIL" for g in gates) else 0


if __name__ == "__main__":
    sys.exit(main())
