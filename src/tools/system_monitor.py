"""Lightweight system + application monitor.

Designed to run as a one-shot every minute (driven by a systemd timer).
Each tick:
    1. Collects a single sample of CPU%, Mem%, two disk-mount %used values,
       per-engine error / warning counts since the last tick, and any
       Analysis-Engine ``Stage timings`` cycle-time values emitted by the
       analytics service since the last tick.
    2. Folds the sample into the *currently open* hourly bucket and daily
       bucket inside a single JSON state file; the open bucket is rewritten
       every minute (so the most recent hourly / daily entry reflects the
       running aggregate) until the wall clock crosses into the next hour
       or day, at which point a fresh bucket is appended and the old one
       is finalised (raw samples dropped, only aggregates retained).
    3. Trims the hourly / daily history to the configured retention
       window and writes the state file atomically.

No third-party dependencies — only the Python stdlib + ``journalctl``
and ``df`` from coreutils. State lives outside the repo at
``~ubuntu/monitoring/state.json`` by default so it survives ``make
logs-clear`` and reinstalls of the application.

Exit codes:
    0 — sample collected and state written.
    1 — fatal error before state could be written (state untouched).
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger("zerogex.system_monitor")

# -----------------------------------------------------------------------------
# Defaults — every value here is overridable via env var or CLI flag so the
# script stays useful in dev (where /var/lib/zerogex doesn't exist and the
# user may want a shorter retention window) without code edits.
# -----------------------------------------------------------------------------

DEFAULT_STATE_FILE = "~/monitoring/state.json"
DEFAULT_HOURLY_RETENTION = 720  # 30 days of hourly buckets
DEFAULT_DAILY_RETENTION = 90    # 90 days of daily buckets
DEFAULT_SERVICES = (
    "zerogex-oa-ingestion",
    "zerogex-oa-analytics",
    "zerogex-oa-signals",
    "zerogex-oa-api",
)
DEFAULT_ANALYTICS_SERVICE = "zerogex-oa-analytics"
DEFAULT_DISK_MOUNTS = ("/", "/var/log")

# Matches the bash patterns in `make services-check`:
#   ERR="$(printf '%s\n' "$LOG" | grep ' - ERROR - ' || true)"
#   WARN="$(printf '%s\n' "$LOG" | grep ' - WARNING - ' || true)"
ERROR_TOKEN = " - ERROR - "
WARNING_TOKEN = " - WARNING - "

# Matches log lines like:
#   2026-05-26 10:57:01,668 - __main__ - INFO - [request_id=-]
#   Stage timings (total 3.62s): snapshot=2.66s, gex_by_strike=0.15s, ...
STAGE_TIMINGS_RE = re.compile(r"Stage timings \(total ([0-9]+(?:\.[0-9]+)?)s\)")

STATE_VERSION = 1


# -----------------------------------------------------------------------------
# Collection — pure-ish data gatherers, each returns plain Python values so
# the bucket-folding logic below can stay unit-testable.
# -----------------------------------------------------------------------------


@dataclass
class CpuStat:
    """Snapshot of ``/proc/stat`` aggregate-cpu line.

    Fields are cumulative jiffies since boot. CPU % between two snapshots
    is ``1 - (idle_delta / total_delta)`` where ``idle = idle + iowait``
    (the kernel convention also used by /usr/bin/top).
    """

    captured_at: float
    user: int
    nice: int
    system: int
    idle: int
    iowait: int
    irq: int
    softirq: int
    steal: int

    @property
    def idle_total(self) -> int:
        return self.idle + self.iowait

    @property
    def total(self) -> int:
        return (
            self.user
            + self.nice
            + self.system
            + self.idle
            + self.iowait
            + self.irq
            + self.softirq
            + self.steal
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "captured_at": self.captured_at,
            "user": self.user,
            "nice": self.nice,
            "system": self.system,
            "idle": self.idle,
            "iowait": self.iowait,
            "irq": self.irq,
            "softirq": self.softirq,
            "steal": self.steal,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "CpuStat":
        return cls(
            captured_at=float(d["captured_at"]),
            user=int(d["user"]),
            nice=int(d["nice"]),
            system=int(d["system"]),
            idle=int(d["idle"]),
            iowait=int(d["iowait"]),
            irq=int(d["irq"]),
            softirq=int(d["softirq"]),
            steal=int(d["steal"]),
        )


def read_cpu_stat(now: float | None = None) -> CpuStat:
    """Read the aggregate ``cpu`` line from ``/proc/stat``.

    Format (kernel 2.6+):
        cpu  user nice system idle iowait irq softirq steal guest guest_nice
    We ignore guest/guest_nice — they are already accounted for in user/nice.
    """
    with open("/proc/stat", "r", encoding="ascii") as fh:
        line = fh.readline().split()
    # line[0] == 'cpu' (aggregate, leading space-padded so split drops it)
    fields = [int(x) for x in line[1:9]]
    while len(fields) < 8:
        fields.append(0)
    return CpuStat(
        captured_at=now if now is not None else time.time(),
        user=fields[0],
        nice=fields[1],
        system=fields[2],
        idle=fields[3],
        iowait=fields[4],
        irq=fields[5],
        softirq=fields[6],
        steal=fields[7],
    )


def cpu_percent_between(prev: CpuStat, curr: CpuStat) -> float | None:
    """% non-idle CPU between two ``/proc/stat`` snapshots.

    Returns ``None`` when the counters didn't advance (would otherwise
    divide by zero — happens if both reads land in the same jiffy on a
    very lightly loaded box).
    """
    total_delta = curr.total - prev.total
    idle_delta = curr.idle_total - prev.idle_total
    if total_delta <= 0:
        return None
    busy = max(0, total_delta - idle_delta)
    return 100.0 * busy / total_delta


def sample_cpu_percent(prev: CpuStat | None) -> tuple[float | None, CpuStat]:
    """Return ``(cpu_pct, new_stat)``.

    If ``prev`` is None or stale (e.g. system clock jumped), fall back to
    a short blocking sample so the first tick after install still produces
    a usable value instead of a hole in the data.
    """
    curr = read_cpu_stat()
    if prev is not None and 0 < (curr.captured_at - prev.captured_at) < 3600:
        pct = cpu_percent_between(prev, curr)
        if pct is not None:
            return pct, curr
    # Cold start (or implausible delta): take a 500 ms blocking sample.
    time.sleep(0.5)
    second = read_cpu_stat()
    return cpu_percent_between(curr, second), second


def sample_memory_percent() -> float | None:
    """% memory used = 1 - MemAvailable/MemTotal.

    ``MemAvailable`` (kernel 3.14+) is the right number to alarm on —
    it accounts for reclaimable cache so we don't trip on a healthy box
    that just has its page cache warm.
    """
    try:
        with open("/proc/meminfo", "r", encoding="ascii") as fh:
            meminfo: dict[str, int] = {}
            for line in fh:
                key, _, rest = line.partition(":")
                if not rest:
                    continue
                parts = rest.strip().split()
                if not parts or not parts[0].isdigit():
                    continue
                meminfo[key] = int(parts[0])  # kB
    except OSError as exc:
        logger.warning("Could not read /proc/meminfo: %s", exc)
        return None
    total = meminfo.get("MemTotal", 0)
    available = meminfo.get("MemAvailable")
    if available is None:
        # Pre-3.14 fallback: free + buffers + cached
        available = (
            meminfo.get("MemFree", 0)
            + meminfo.get("Buffers", 0)
            + meminfo.get("Cached", 0)
        )
    if total <= 0:
        return None
    used = max(0, total - available)
    return 100.0 * used / total


def sample_disk_percents(mounts: Iterable[str]) -> dict[str, float | None]:
    """Return percent-used for each requested mount point.

    Uses ``df --output=target,pcent``: GNU coreutils refuses to combine
    ``--output`` with ``-P`` (they're mutually exclusive in newer
    versions), but ``--output`` already prints one filesystem per line
    without wrapping, which is the only POSIX-mode guarantee we need.
    Mounts not present on the host map to ``None`` rather than raising,
    so a dev machine without ``/var/log`` on its own device still
    produces a valid sample.
    """
    mounts = list(mounts)
    result: dict[str, float | None] = {m: None for m in mounts}
    if not mounts:
        return result
    try:
        out = subprocess.run(
            ["df", "--output=target,pcent"],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError) as exc:
        logger.warning("df failed: %s", exc)
        return result
    for line in out.splitlines()[1:]:  # skip header
        parts = line.split()
        if len(parts) < 2:
            continue
        target, pcent = parts[0], parts[-1]
        if target in result and pcent.endswith("%"):
            try:
                result[target] = float(pcent.rstrip("%"))
            except ValueError:
                continue
    return result


def journalctl_lines(
    service: str,
    since_ts: float | None,
    until_ts: float,
) -> list[str]:
    """Fetch journal lines for ``service`` in the half-open ``(since, until]``.

    Returns ``[]`` on permission / spawn failure rather than raising — a
    monitor that can't read journals should still emit zeros for the
    other metrics and not crash the timer unit.

    Why ``--since @<unix>``: the bare ``--since "1 minute ago"`` form is
    relative to *now*, which drifts versus the timer cadence; passing
    an absolute Unix-epoch (the ``@`` prefix) anchors the window to the
    last collection so we don't miss or double-count lines across runs.
    """
    cmd = ["journalctl", "-u", service, "--no-pager", "-o", "short-iso"]
    if since_ts is not None:
        cmd += ["--since", f"@{int(since_ts)}"]
    cmd += ["--until", f"@{int(until_ts)}"]
    try:
        proc = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        logger.warning("journalctl spawn failed for %s: %s", service, exc)
        return []
    if proc.returncode != 0:
        logger.warning(
            "journalctl %s exit=%s stderr=%s",
            service, proc.returncode, proc.stderr.strip()[:200],
        )
        return []
    return proc.stdout.splitlines()


def count_error_warning(lines: Iterable[str]) -> tuple[int, int]:
    """Count error / warning lines using the same tokens as services-check."""
    err = 0
    warn = 0
    for line in lines:
        if ERROR_TOKEN in line:
            err += 1
        elif WARNING_TOKEN in line:
            warn += 1
    return err, warn


def extract_stage_timing_totals(lines: Iterable[str]) -> list[float]:
    """Return every ``total Xs`` value parsed from analytics journal lines."""
    out: list[float] = []
    for line in lines:
        m = STAGE_TIMINGS_RE.search(line)
        if m:
            try:
                out.append(float(m.group(1)))
            except ValueError:
                continue
    return out


# -----------------------------------------------------------------------------
# Bucket folding — pure functions that take a state dict + a Sample and
# return the next state dict. No I/O happens here, so the rollover semantics
# are straightforward to unit-test.
# -----------------------------------------------------------------------------


@dataclass
class Sample:
    """One minute's worth of observations.

    All numeric fields are ``None`` when the collector couldn't produce
    a reading (e.g. journalctl permission denied). ``None`` values are
    omitted from aggregates so a single transient failure doesn't drag
    the hourly average to zero.
    """

    captured_at: datetime
    cpu_pct: float | None
    mem_pct: float | None
    disk_pcts: dict[str, float | None]
    cycle_times_s: list[float]
    errors_by_service: dict[str, int]
    warnings_by_service: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "captured_at": self.captured_at.isoformat(),
            "cpu_pct": self.cpu_pct,
            "mem_pct": self.mem_pct,
            "disk_pcts": self.disk_pcts,
            "cycle_times_s": list(self.cycle_times_s),
            "errors_by_service": dict(self.errors_by_service),
            "warnings_by_service": dict(self.warnings_by_service),
        }


def floor_to_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def floor_to_day(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _max_avg(values: list[float]) -> dict[str, float | None]:
    vals = [v for v in values if v is not None and not math.isnan(v)]
    if not vals:
        return {"max": None, "avg": None}
    return {"max": max(vals), "avg": statistics.fmean(vals)}


def _max_avg_median(values: list[float]) -> dict[str, float | None]:
    vals = [v for v in values if v is not None and not math.isnan(v)]
    if not vals:
        return {"max": None, "avg": None, "median": None, "count": 0}
    return {
        "max": max(vals),
        "avg": statistics.fmean(vals),
        "median": statistics.median(vals),
        "count": len(vals),
    }


def _sum_by_service(rows: list[dict[str, int]], services: Iterable[str]) -> dict[str, int]:
    out = {s: 0 for s in services}
    for row in rows:
        for svc, count in row.items():
            out[svc] = out.get(svc, 0) + int(count)
    return out


def _aggregate_bucket(
    samples: list[Sample],
    disk_mounts: Iterable[str],
    services: Iterable[str],
) -> dict[str, Any]:
    """Compute the final per-bucket metrics block from raw samples.

    The disk-percent keys are flattened (``disk_<mount>_pct``) so the JSON
    is easy to dashboard — nested ``{disk_pcts: {/: ...}}`` would force
    every consumer to know the original list of mounts.
    """
    cpu_vals = [s.cpu_pct for s in samples if s.cpu_pct is not None]
    mem_vals = [s.mem_pct for s in samples if s.mem_pct is not None]
    cycle_vals: list[float] = []
    for s in samples:
        cycle_vals.extend(s.cycle_times_s)
    metrics: dict[str, Any] = {
        "cpu_pct": _max_avg(cpu_vals),
        "mem_pct": _max_avg(mem_vals),
        "cycle_time_s": _max_avg_median(cycle_vals),
    }
    for mount in disk_mounts:
        key = _disk_metric_key(mount)
        vals = [s.disk_pcts.get(mount) for s in samples]
        vals = [v for v in vals if v is not None]
        metrics[key] = _max_avg(vals)
    metrics["errors_by_service"] = _sum_by_service(
        [s.errors_by_service for s in samples], services
    )
    metrics["warnings_by_service"] = _sum_by_service(
        [s.warnings_by_service for s in samples], services
    )
    metrics["sample_count"] = len(samples)
    return metrics


def _disk_metric_key(mount: str) -> str:
    if mount == "/":
        slug = "root"
    else:
        slug = mount.strip("/").replace("/", "_")
    return f"disk_{slug}_pct"


def _new_bucket(bucket_start: datetime) -> dict[str, Any]:
    return {
        "bucket_start": bucket_start.isoformat(),
        "samples": [],
    }


def _append_sample_to_bucket(
    bucket: dict[str, Any],
    sample: Sample,
    disk_mounts: Iterable[str],
    services: Iterable[str],
) -> None:
    """Mutate ``bucket`` in place: store the raw sample and refresh aggregates."""
    bucket.setdefault("samples", []).append(sample.to_dict())
    samples = [_sample_from_dict(d) for d in bucket["samples"]]
    bucket["metrics"] = _aggregate_bucket(samples, disk_mounts, services)


def _sample_from_dict(d: dict[str, Any]) -> Sample:
    return Sample(
        captured_at=datetime.fromisoformat(d["captured_at"]),
        cpu_pct=d.get("cpu_pct"),
        mem_pct=d.get("mem_pct"),
        disk_pcts=dict(d.get("disk_pcts") or {}),
        cycle_times_s=list(d.get("cycle_times_s") or []),
        errors_by_service=dict(d.get("errors_by_service") or {}),
        warnings_by_service=dict(d.get("warnings_by_service") or {}),
    )


def _finalize_closed_bucket(bucket: dict[str, Any]) -> None:
    """Drop the raw-sample buffer; keep only the aggregate metrics.

    Called when a bucket rolls over (the next sample lands in a newer
    bucket). The aggregates were already computed on the last append,
    so we just discard ``samples`` to keep the on-disk file small.
    """
    bucket.pop("samples", None)


def fold_sample(
    state: dict[str, Any],
    sample: Sample,
    disk_mounts: Iterable[str],
    services: Iterable[str],
    hourly_retention: int,
    daily_retention: int,
) -> dict[str, Any]:
    """Merge ``sample`` into the state's hourly + daily buckets.

    Returns the *same* dict, mutated in place. Returning it is just a
    convenience so callers can chain ``write(fold(...))``.
    """
    sample_dt = sample.captured_at
    state.setdefault("version", STATE_VERSION)
    state.setdefault("hourly", [])
    state.setdefault("daily", [])
    _fold_into_history(
        state["hourly"],
        sample,
        bucket_start=floor_to_hour(sample_dt),
        retention=hourly_retention,
        disk_mounts=disk_mounts,
        services=services,
    )
    _fold_into_history(
        state["daily"],
        sample,
        bucket_start=floor_to_day(sample_dt),
        retention=daily_retention,
        disk_mounts=disk_mounts,
        services=services,
    )
    return state


def _fold_into_history(
    history: list[dict[str, Any]],
    sample: Sample,
    bucket_start: datetime,
    retention: int,
    disk_mounts: Iterable[str],
    services: Iterable[str],
) -> None:
    bucket_iso = bucket_start.isoformat()
    last = history[-1] if history else None
    if last is None or last.get("bucket_start") != bucket_iso:
        if last is not None:
            _finalize_closed_bucket(last)
        history.append(_new_bucket(bucket_start))
    _append_sample_to_bucket(history[-1], sample, disk_mounts, services)
    # Retention trim — keep the newest ``retention`` buckets.
    if retention > 0 and len(history) > retention:
        del history[: len(history) - retention]


# -----------------------------------------------------------------------------
# State file I/O
# -----------------------------------------------------------------------------


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": STATE_VERSION, "hourly": [], "daily": []}
    try:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("state file %s unreadable (%s); rotating and starting fresh", path, exc)
        backup = path.with_suffix(path.suffix + f".corrupt.{int(time.time())}")
        try:
            shutil.copy2(path, backup)
        except OSError:
            pass
        return {"version": STATE_VERSION, "hourly": [], "daily": []}
    if not isinstance(data, dict):
        return {"version": STATE_VERSION, "hourly": [], "daily": []}
    data.setdefault("version", STATE_VERSION)
    data.setdefault("hourly", [])
    data.setdefault("daily", [])
    return data


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Atomic write so a crash mid-write never leaves a partial JSON file.
    fd, tmp_path = tempfile.mkstemp(
        prefix=".state.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(state, fh, separators=(",", ":"), sort_keys=False)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# -----------------------------------------------------------------------------
# Orchestration — wires the collectors + folder together for a single tick.
# -----------------------------------------------------------------------------


def collect_sample(
    *,
    now: datetime,
    services: Iterable[str],
    analytics_service: str,
    disk_mounts: Iterable[str],
    since_ts: float | None,
    prev_cpu_stat: CpuStat | None,
) -> tuple[Sample, CpuStat | None]:
    """Run every collector once.

    ``since_ts`` is the upper bound from the previous run (Unix epoch).
    On first run it's ``None`` — we then fall back to ``now - 60 s`` so
    we don't pull the entire journal on cold start.
    """
    until_ts = now.timestamp()
    if since_ts is None:
        # Cold start — pull a single minute so the first sample isn't a
        # 100 % zero (or a giant historical scrape).
        since_ts = until_ts - 60

    cpu_pct, new_cpu_stat = sample_cpu_percent(prev_cpu_stat)
    mem_pct = sample_memory_percent()
    disk_pcts = sample_disk_percents(disk_mounts)

    errors_by_service: dict[str, int] = {}
    warnings_by_service: dict[str, int] = {}
    cycle_times: list[float] = []
    for svc in services:
        lines = journalctl_lines(svc, since_ts, until_ts)
        err, warn = count_error_warning(lines)
        errors_by_service[svc] = err
        warnings_by_service[svc] = warn
        if svc == analytics_service:
            cycle_times.extend(extract_stage_timing_totals(lines))

    return (
        Sample(
            captured_at=now,
            cpu_pct=cpu_pct,
            mem_pct=mem_pct,
            disk_pcts=disk_pcts,
            cycle_times_s=cycle_times,
            errors_by_service=errors_by_service,
            warnings_by_service=warnings_by_service,
        ),
        new_cpu_stat,
    )


def run_once(
    state_path: Path,
    *,
    services: Iterable[str],
    analytics_service: str,
    disk_mounts: Iterable[str],
    hourly_retention: int,
    daily_retention: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    """One tick: read state, collect, fold, write. Returns the new state."""
    state = load_state(state_path)
    services = list(services)
    disk_mounts = list(disk_mounts)
    if now is None:
        now = datetime.now().astimezone()

    prev_cpu_dict = state.get("last_cpu_stat")
    prev_cpu_stat = CpuStat.from_dict(prev_cpu_dict) if prev_cpu_dict else None
    since_ts = state.get("last_sample_ts")

    sample, new_cpu_stat = collect_sample(
        now=now,
        services=services,
        analytics_service=analytics_service,
        disk_mounts=disk_mounts,
        since_ts=float(since_ts) if since_ts else None,
        prev_cpu_stat=prev_cpu_stat,
    )

    fold_sample(
        state,
        sample,
        disk_mounts=disk_mounts,
        services=services,
        hourly_retention=hourly_retention,
        daily_retention=daily_retention,
    )
    if new_cpu_stat is not None:
        state["last_cpu_stat"] = new_cpu_stat.to_dict()
    state["last_sample_ts"] = now.timestamp()
    state["last_sample_iso"] = now.isoformat()
    save_state(state_path, state)
    return state


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------


def _env_list(name: str, default: Iterable[str]) -> list[str]:
    raw = os.getenv(name)
    if not raw:
        return list(default)
    return [piece.strip() for piece in raw.split(",") if piece.strip()]


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture a one-minute system + application monitoring sample.",
    )
    parser.add_argument(
        "--state-file",
        default=os.getenv("MONITOR_STATE_FILE", DEFAULT_STATE_FILE),
        help="Path to the JSON state file (default: %(default)s).",
    )
    parser.add_argument(
        "--services",
        default=",".join(_env_list("MONITOR_SERVICES", DEFAULT_SERVICES)),
        help="Comma-separated systemd unit names to scan for error / warning counts.",
    )
    parser.add_argument(
        "--analytics-service",
        default=os.getenv("MONITOR_ANALYTICS_SERVICE", DEFAULT_ANALYTICS_SERVICE),
        help="Service to parse Stage timings from (default: %(default)s).",
    )
    parser.add_argument(
        "--disk-mounts",
        default=",".join(_env_list("MONITOR_DISK_MOUNTS", DEFAULT_DISK_MOUNTS)),
        help="Comma-separated mount points to track (default: %(default)s).",
    )
    parser.add_argument(
        "--hourly-retention",
        type=int,
        default=_env_int("MONITOR_HOURLY_RETENTION", DEFAULT_HOURLY_RETENTION),
        help="Number of hourly buckets to retain (default: %(default)s).",
    )
    parser.add_argument(
        "--daily-retention",
        type=int,
        default=_env_int("MONITOR_DAILY_RETENTION", DEFAULT_DAILY_RETENTION),
        help="Number of daily buckets to retain (default: %(default)s).",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Print the most recent hourly + daily aggregate to stdout instead of collecting.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="With --show, emit raw JSON instead of a human summary.",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable INFO-level logging to stderr.",
    )
    return parser.parse_args(argv)


def _resolve_state_path(raw: str) -> Path:
    return Path(os.path.expanduser(raw)).resolve()


def _summarise(state: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"State version: {state.get('version')}")
    lines.append(f"Last sample:   {state.get('last_sample_iso', '-')}")
    for label, key in (("Current hour", "hourly"), ("Current day", "daily")):
        entries = state.get(key) or []
        if not entries:
            lines.append(f"{label}: (no data yet)")
            continue
        latest = entries[-1]
        metrics = latest.get("metrics") or {}
        lines.append(f"{label} bucket: {latest.get('bucket_start')} "
                     f"(samples={metrics.get('sample_count')})")
        lines.append(f"  {'cpu_pct':<20s} max={_fmt(metrics.get('cpu_pct', {}).get('max'))} "
                     f"avg={_fmt(metrics.get('cpu_pct', {}).get('avg'))}")
        lines.append(f"  {'mem_pct':<20s} max={_fmt(metrics.get('mem_pct', {}).get('max'))} "
                     f"avg={_fmt(metrics.get('mem_pct', {}).get('avg'))}")
        cyc = metrics.get("cycle_time_s") or {}
        lines.append(f"  {'cycle_time_s':<20s} max={_fmt(cyc.get('max'))} "
                     f"avg={_fmt(cyc.get('avg'))} median={_fmt(cyc.get('median'))} "
                     f"(n={cyc.get('count') or 0})")
        for k, v in metrics.items():
            if not k.startswith("disk_"):
                continue
            lines.append(f"  {k:<20s} max={_fmt(v.get('max'))} avg={_fmt(v.get('avg'))}")
        errs = metrics.get("errors_by_service") or {}
        warns = metrics.get("warnings_by_service") or {}
        if errs or warns:
            lines.append("  errors:   " + ", ".join(f"{k}={v}" for k, v in sorted(errs.items())))
            lines.append("  warnings: " + ", ".join(f"{k}={v}" for k, v in sorted(warns.items())))
    return "\n".join(lines)


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    state_path = _resolve_state_path(args.state_file)
    services = [s.strip() for s in args.services.split(",") if s.strip()]
    disk_mounts = [m.strip() for m in args.disk_mounts.split(",") if m.strip()]

    if args.show:
        state = load_state(state_path)
        if args.json:
            json.dump(state, sys.stdout, indent=2, sort_keys=False)
            sys.stdout.write("\n")
        else:
            print(_summarise(state))
        return 0

    try:
        run_once(
            state_path,
            services=services,
            analytics_service=args.analytics_service,
            disk_mounts=disk_mounts,
            hourly_retention=args.hourly_retention,
            daily_retention=args.daily_retention,
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.error("system_monitor failed: %s", exc, exc_info=True)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
