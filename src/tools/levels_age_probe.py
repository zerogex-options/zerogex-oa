"""Measure how stale the dealer-positioning levels are by the time a chart
sees them, from any machine with an API key. No NinjaTrader required.

A tester reported levels that "move into position after price has come and
gone" and asked whether polling was to blame. Answering that needs one
number the chart cannot show and the engine does not log: how old a
snapshot already is at the moment it is published, and how that compares
with the wait until the next one. This tool measures both by polling the
levels endpoint far more often than any chart does and watching ``as_of``
advance.

What it reports, and how to read it:

* **cycle period** -- the gap between successive ``as_of`` values. Should
  sit at the analytics interval (60s). Longer means the cycle is overrunning
  (see the "Calculation took" warning in the engine log).
* **age at publish** -- ``age_seconds`` on the first sample that shows a new
  ``as_of``. The snapshot was born this old: the chain-to-cycle lag plus the
  whole snapshot-query-and-compute duration, because the engine stamps a
  snapshot with the chain timestamp it started from, not the time it
  finished. This is the number no poll interval can touch. Resolution is one
  probe interval, so the true value is up to ``--interval`` lower.
* **age seen by a random poll** -- percentiles of ``age_seconds`` over every
  sample. A chart polling at any interval lands at a random phase of the
  cycle, so this is the distribution its info panel shows before its own
  poll wait is added on top.

The v2 envelope is preferred because ``evaluated_at`` is the server's clock,
so the age needs no trust in the laptop's clock; the tool falls back to v1
(``as_of`` + ``age_seconds``, also server-computed) when v2 is not deployed.
On this endpoint ``generated_at`` equals ``as_of`` -- the API does not
expose when the analytics row was written -- so the API alone cannot split
"age at publish" into chain lag versus compute time. That split needs the
engine's own timing log.

Deliberately stdlib-only (urllib, no requests/httpx), like
``v2_envelope_check``, so it runs from a bare laptop or a deploy shell.

Exit codes:
    0 -- probe completed.
    2 -- could not reach the server, or the key was refused.

Usage:
    API_KEY=... python -m src.tools.levels_age_probe --symbol NQ --minutes 60
    API_KEY=... python -m src.tools.levels_age_probe --symbol NQ --once
    API_KEY=... python -m src.tools.levels_age_probe --symbol NQ --csv nq-age.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Sequence, Tuple

DEFAULT_BASE_URL = "https://api.zerogex.io"
USER_AGENT = "zerogex-levels-age-probe"

# Consecutive fetch failures tolerated before giving up. A single 5xx or a
# dropped connection should not end an hour-long probe; a dead server should.
MAX_CONSECUTIVE_FAILURES = 6

FetchFn = Callable[[str], Tuple[int, Optional[dict]]]


@dataclass(frozen=True)
class Sample:
    """One observation of the levels endpoint."""

    sample_at: datetime  # client clock, UTC
    evaluated_at: datetime  # server clock (v2) or client clock (v1)
    as_of: Optional[datetime]
    age_seconds: Optional[float]
    api_version: int
    advanced: bool  # as_of moved since the previous sample


@dataclass(frozen=True)
class Summary:
    samples: int
    snapshots: int  # distinct as_of values seen
    period_seconds: List[float]  # gaps between successive distinct as_of
    publish_age_seconds: List[float]  # age on first sight of each new as_of
    ages: List[float]  # age_seconds of every sample


def parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 timestamp to an aware UTC datetime.

    ``datetime.fromisoformat`` only learned the trailing ``Z`` in 3.11, and
    this runs on whatever Python a laptop has.
    """
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def levels_url(base_url: str, version: int, symbol: str) -> str:
    # strikes=1 keeps the payload to the headline levels: the profile is
    # dozens of rows the probe never reads.
    return f"{base_url.rstrip('/')}/api/v{version}/levels/{symbol.upper()}?strikes=1"


def fetch_json(url: str, api_key: str, timeout: float = 10.0) -> Tuple[int, Optional[dict]]:
    """GET ``url`` with the bearer key; ``(status, body)`` or ``(status, None)``.

    A network-level failure (no route, refused, timeout) is reported as
    status 0 rather than raised, so the probe loop can count it like any
    other bad answer.
    """
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": "Bearer " + api_key,
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, None
    except (urllib.error.URLError, OSError, ValueError):
        return 0, None


def take_sample(
    body: dict, sample_at: datetime, api_version: int, previous: Optional[Sample]
) -> Sample:
    """Reduce one response body to the few fields the probe reasons about."""
    if api_version == 2:
        freshness = body.get("freshness") or {}
        data = body.get("data") or {}
        evaluated_raw = freshness.get("evaluated_at")
        evaluated_at = parse_iso(evaluated_raw) if evaluated_raw else sample_at
        as_of_raw = data.get("as_of") or freshness.get("source_timestamp")
        age = freshness.get("age_seconds")
        if age is None:
            age = data.get("age_seconds")
    else:
        evaluated_at = sample_at
        as_of_raw = body.get("as_of")
        age = body.get("age_seconds")

    as_of = parse_iso(as_of_raw) if as_of_raw else None
    if age is None and as_of is not None:
        age = (evaluated_at - as_of).total_seconds()

    advanced = previous is not None and as_of is not None and as_of != previous.as_of
    return Sample(
        sample_at=sample_at,
        evaluated_at=evaluated_at,
        as_of=as_of,
        age_seconds=float(age) if age is not None else None,
        api_version=api_version,
        advanced=advanced,
    )


def summarize(samples: Sequence[Sample]) -> Summary:
    """Turn the sample series into cycle periods and publish ages.

    The first ``as_of`` seen is excluded from the publish ages: the probe
    joined that cycle mid-way, so its age says nothing about when it was
    born. Only an advance observed during the run counts.
    """
    periods: List[float] = []
    publish_ages: List[float] = []
    ages: List[float] = []
    last_as_of: Optional[datetime] = None
    snapshots = 0

    for sample in samples:
        if sample.age_seconds is not None:
            ages.append(sample.age_seconds)
        if sample.as_of is None:
            continue
        if last_as_of is None or sample.as_of != last_as_of:
            snapshots += 1
            if last_as_of is not None:
                periods.append((sample.as_of - last_as_of).total_seconds())
                if sample.age_seconds is not None:
                    publish_ages.append(sample.age_seconds)
            last_as_of = sample.as_of

    return Summary(
        samples=len(samples),
        snapshots=snapshots,
        period_seconds=periods,
        publish_age_seconds=publish_ages,
        ages=ages,
    )


def percentile(values: Sequence[float], pct: float) -> float:
    """Linear-interpolated percentile; ``values`` must be non-empty."""
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * pct / 100.0
    lower = math.floor(rank)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def _stats(values: Sequence[float]) -> str:
    if not values:
        return "n/a"
    return "min {:.0f}s  median {:.0f}s  p90 {:.0f}s  max {:.0f}s".format(
        min(values), statistics.median(values), percentile(values, 90), max(values)
    )


def format_report(summary: Summary, interval: float) -> str:
    lines = [
        f"samples: {summary.samples}   distinct snapshots: {summary.snapshots}",
        f"cycle period (as_of to as_of):   {_stats(summary.period_seconds)}",
        f"age at publish (first sight):    {_stats(summary.publish_age_seconds)}"
        f"   [resolution {interval:.0f}s: true value up to that much lower]",
        f"age seen by a random poll:       {_stats(summary.ages)}",
    ]
    if summary.snapshots < 3:
        lines.append(
            "fewer than three snapshots observed; run longer than two cycles "
            "for the period and publish-age lines to mean anything"
        )
    return "\n".join(lines)


def summary_json(summary: Summary, interval: float) -> Dict[str, object]:
    def block(values: Sequence[float]) -> Optional[Dict[str, float]]:
        if not values:
            return None
        return {
            "min": min(values),
            "median": statistics.median(values),
            "p90": percentile(values, 90),
            "max": max(values),
            "n": len(values),
        }

    return {
        "samples": summary.samples,
        "snapshots": summary.snapshots,
        "probe_interval_seconds": interval,
        "cycle_period_seconds": block(summary.period_seconds),
        "age_at_publish_seconds": block(summary.publish_age_seconds),
        "age_seen_by_random_poll_seconds": block(summary.ages),
    }


def run_probe(
    fetch: FetchFn,
    base_url: str,
    symbol: str,
    interval: float,
    duration_seconds: float,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    sleep: Callable[[float], None] = time.sleep,
    on_sample: Optional[Callable[[Sample], None]] = None,
    log: Callable[[str], None] = lambda message: print(message, file=sys.stderr),
) -> Tuple[List[Sample], int]:
    """Poll until ``duration_seconds`` elapse; ``(samples, exit_code)``.

    Tries v2 first for its server-clock ``evaluated_at`` and drops to v1 for
    the rest of the run on a 404, which is what an API without the v2 mirror
    answers. A refused key ends the run immediately: every later sample
    would fail the same way.
    """
    version = 2
    samples: List[Sample] = []
    previous: Optional[Sample] = None
    failures = 0
    started = clock()

    while True:
        now = clock()
        try:
            status, body = fetch(levels_url(base_url, version, symbol))
        except KeyboardInterrupt:
            # An hour of samples is worth a report even if the trader got
            # bored at fifty minutes.
            log("interrupted; summarizing what was collected")
            return samples, 0

        if status == 404 and version == 2:
            log("v2 not available here; continuing with v1")
            version = 1
            continue
        if status == 404:
            log(f"HTTP 404 from v1 as well: no levels for {symbol.upper()} (check --symbol)")
            return samples, 2
        if status in (401, 403):
            log(f"key refused (HTTP {status}); nothing to measure")
            return samples, 2
        if body is None:
            failures += 1
            log(f"fetch failed (HTTP {status}); {failures} consecutive")
            if failures >= MAX_CONSECUTIVE_FAILURES:
                log("giving up")
                return samples, 2
        else:
            failures = 0
            sample = take_sample(body, now, version, previous)
            samples.append(sample)
            previous = sample
            if on_sample is not None:
                on_sample(sample)

        if (clock() - started).total_seconds() + interval > duration_seconds:
            return samples, 0
        try:
            sleep(interval)
        except KeyboardInterrupt:
            log("interrupted; summarizing what was collected")
            return samples, 0


def _print_sample(sample: Sample) -> None:
    as_of = sample.as_of.strftime("%H:%M:%S") if sample.as_of else "-"
    age = f"{sample.age_seconds:6.1f}s" if sample.age_seconds is not None else "     -"
    marker = "  NEW SNAPSHOT" if sample.advanced else ""
    print(
        f"{sample.sample_at.strftime('%H:%M:%S')}  as_of {as_of}  "
        f"age {age}  v{sample.api_version}{marker}"
    )


def _write_csv(path: str, samples: Sequence[Sample]) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["sample_at", "evaluated_at", "as_of", "age_seconds", "api_version", "advanced"]
        )
        for s in samples:
            writer.writerow(
                [
                    s.sample_at.isoformat(),
                    s.evaluated_at.isoformat(),
                    s.as_of.isoformat() if s.as_of else "",
                    "" if s.age_seconds is None else f"{s.age_seconds:.3f}",
                    s.api_version,
                    int(s.advanced),
                ]
            )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base-url", default=os.getenv("ZEROGEX_API_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--symbol", default="NQ", help="underlying to probe (default: NQ)")
    parser.add_argument(
        "--api-key",
        default=os.getenv("API_KEY") or os.getenv("ZEROGEX_API_KEY"),
        help="bearer key; or set API_KEY / ZEROGEX_API_KEY",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="seconds between samples (default 5; this is also the resolution)",
    )
    parser.add_argument("--minutes", type=float, default=30.0, help="how long to run (default 30)")
    parser.add_argument("--once", action="store_true", help="take one sample and exit")
    parser.add_argument("--csv", metavar="PATH", help="write every sample to this CSV")
    parser.add_argument("--json", action="store_true", help="print the summary as JSON")
    parser.add_argument("--quiet", action="store_true", help="do not print per-sample lines")
    args = parser.parse_args(argv)

    if not args.api_key:
        print("no API key: pass --api-key or set API_KEY", file=sys.stderr)
        return 2
    if args.interval <= 0:
        print("--interval must be positive", file=sys.stderr)
        return 2

    duration = 0.0 if args.once else args.minutes * 60.0

    def fetch(url: str) -> Tuple[int, Optional[dict]]:
        return fetch_json(url, args.api_key)

    samples, code = run_probe(
        fetch=fetch,
        base_url=args.base_url,
        symbol=args.symbol,
        interval=args.interval,
        duration_seconds=duration,
        on_sample=None if args.quiet else _print_sample,
    )

    if args.csv and samples:
        _write_csv(args.csv, samples)

    summary = summarize(samples)
    if args.json:
        print(json.dumps(summary_json(summary, args.interval), indent=2))
    elif samples:
        print()
        print(format_report(summary, args.interval))
    return code


if __name__ == "__main__":
    sys.exit(main())
