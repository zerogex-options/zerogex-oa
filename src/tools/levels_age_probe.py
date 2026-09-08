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

Run it during the cash session. Outside one nothing advances -- the first
run of this tool spent an hour on Labor Day watching Friday's 15:59 ET
snapshot get older -- so when the first answer says the session is closed,
or the snapshot is already more than half an hour old, the tool says so and
stops (exit 3). ``--force`` polls anyway.

Two things bound what the API can tell you. The service sits behind a
5-second nginx response cache keyed by URL and key, so sampling faster than
5s buys nothing and every age here can trail the app by up to 5s. And on
this endpoint ``generated_at`` equals ``as_of`` -- the API does not expose
when the analytics row was written -- so the API alone cannot split "age at
publish" into chain lag versus compute time. That split needs the engine's
own timing log.

The v2 envelope is preferred because ``evaluated_at`` is the server's clock,
so the age needs no trust in the laptop's clock, and because it carries the
session status; the tool falls back to v1 (``as_of`` + ``age_seconds``, also
server-computed) when v2 is not deployed.

Deliberately stdlib-only (urllib, no requests/httpx), like
``v2_envelope_check``, so it runs from a bare laptop or a deploy shell.

Exit codes:
    0 -- probe completed.
    2 -- could not reach the server, or the key was refused.
    3 -- nothing to measure: the session is closed or the snapshot is
         frozen (override with --force).

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

# A first sample older than this means the market is closed or the engine is
# not publishing; either way an hour of polling would measure nothing. Half
# an hour is thirty cycles: no healthy session ever shows that.
STALE_AT_START_SECONDS = 30 * 60

# The v2 freshness_status that says the feed is not expected to move.
SESSION_CLOSED = "session_closed"

EXIT_OK = 0
EXIT_UNREACHABLE = 2
EXIT_NOTHING_TO_MEASURE = 3

# (status, body, cache_status) -- the third is nginx's X-Cache-Status header
# (HIT / MISS / EXPIRED / ...), or None when the server did not send one.
FetchFn = Callable[[str], Tuple[int, Optional[dict], Optional[str]]]


@dataclass(frozen=True)
class Sample:
    """One observation of the levels endpoint."""

    sample_at: datetime  # client clock, UTC
    evaluated_at: datetime  # server clock (v2) or client clock (v1)
    as_of: Optional[datetime]
    age_seconds: Optional[float]
    api_version: int
    advanced: bool  # as_of moved since the previous sample
    regressed: bool = False  # ...and moved BACKWARDS: an older snapshot served after a newer
    session: Optional[str] = None  # v2 market_session_status
    freshness: Optional[str] = None  # v2 freshness_status
    cache_status: Optional[str] = None  # nginx X-Cache-Status, when sent


@dataclass(frozen=True)
class Summary:
    samples: int
    snapshots: int  # distinct as_of values seen, counting only forward moves
    period_seconds: List[float]  # gaps between successive NEW as_of values
    publish_age_seconds: List[float]  # age on first sight of each new as_of
    ages: List[float]  # age_seconds of every sample
    regressions: int = 0  # samples where as_of went backwards


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


def fmt_age(seconds: Optional[float]) -> str:
    """Seconds as a person reads them: 63.2s, 12m 30s, 2d 21h 36m."""
    if seconds is None:
        return "-"
    sign = "-" if seconds < 0 else ""
    total = abs(seconds)
    if total < 600:
        return f"{sign}{total:.1f}s"
    minutes = int(total // 60)
    if minutes < 120:
        return f"{sign}{minutes}m {int(total - minutes * 60)}s"
    hours, minutes = divmod(minutes, 60)
    if hours < 48:
        return f"{sign}{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{sign}{days}d {hours}h {minutes}m"


def fmt_when(moment: Optional[datetime]) -> str:
    """A timestamp in UTC and, where the tz database allows, New York time."""
    if moment is None:
        return "-"
    text = moment.strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        from zoneinfo import ZoneInfo

        text += moment.astimezone(ZoneInfo("America/New_York")).strftime(" (%H:%M:%S ET)")
    except Exception:  # noqa: BLE001 - no tz database on this machine is fine
        pass
    return text


def levels_url(base_url: str, version: int, symbol: str, bust: Optional[str] = None) -> str:
    # strikes=1 keeps the payload to the headline levels: the profile is
    # dozens of rows the probe never reads. ``bust`` is a per-request token
    # appended as an extra query parameter; the nginx cache key is the full
    # request URI, so a unique token is a guaranteed cache miss.
    url = f"{base_url.rstrip('/')}/api/v{version}/levels/{symbol.upper()}?strikes=1"
    return f"{url}&probe={bust}" if bust else url


def fetch_json(
    url: str, api_key: str, timeout: float = 10.0
) -> Tuple[int, Optional[dict], Optional[str]]:
    """GET ``url`` with the bearer key; ``(status, body, cache_status)``.

    ``cache_status`` is nginx's ``X-Cache-Status`` header when present, so a
    sample can say whether it came from the edge cache or the app.

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
            cache_status = response.headers.get("X-Cache-Status")
            return response.status, json.loads(response.read().decode("utf-8")), cache_status
    except urllib.error.HTTPError as exc:
        return exc.code, None, None
    except (urllib.error.URLError, OSError, ValueError):
        return 0, None, None


def take_sample(
    body: dict,
    sample_at: datetime,
    api_version: int,
    previous: Optional[Sample],
    cache_status: Optional[str] = None,
) -> Sample:
    """Reduce one response body to the few fields the probe reasons about."""
    session = freshness = None
    if api_version == 2:
        envelope = body.get("freshness") or {}
        data = body.get("data") or {}
        evaluated_raw = envelope.get("evaluated_at")
        evaluated_at = parse_iso(evaluated_raw) if evaluated_raw else sample_at
        as_of_raw = data.get("as_of") or envelope.get("source_timestamp")
        age = envelope.get("age_seconds")
        if age is None:
            age = data.get("age_seconds")
        session = envelope.get("market_session_status")
        freshness = envelope.get("freshness_status")
    else:
        evaluated_at = sample_at
        as_of_raw = body.get("as_of")
        age = body.get("age_seconds")

    as_of = parse_iso(as_of_raw) if as_of_raw else None
    if age is None and as_of is not None:
        age = (evaluated_at - as_of).total_seconds()

    advanced = previous is not None and as_of is not None and as_of != previous.as_of
    regressed = advanced and previous.as_of is not None and as_of < previous.as_of
    return Sample(
        sample_at=sample_at,
        evaluated_at=evaluated_at,
        as_of=as_of,
        age_seconds=float(age) if age is not None else None,
        api_version=api_version,
        advanced=advanced,
        regressed=regressed,
        session=session,
        freshness=freshness,
        cache_status=cache_status,
    )


def nothing_to_measure(first: Sample) -> Optional[str]:
    """Why polling on from ``first`` would measure nothing, or None.

    v2 says it outright through ``freshness_status``. v1 has no such field,
    so the age has to speak: a snapshot already half an hour old at the
    start is a closed market or a silent engine, and an hour of samples of
    it is an hour of the same number.
    """
    if first.freshness == SESSION_CLOSED:
        return (
            f"the API reports the session closed (market_session_status="
            f"{first.session}); the newest snapshot is from {fmt_when(first.as_of)}"
        )
    if first.age_seconds is not None and first.age_seconds > STALE_AT_START_SECONDS:
        return (
            f"the newest snapshot is already {fmt_age(first.age_seconds)} old "
            f"(as_of {fmt_when(first.as_of)}): the market is closed or the "
            "engine is not publishing"
        )
    return None


def summarize(samples: Sequence[Sample]) -> Summary:
    """Turn the sample series into cycle periods and publish ages.

    The first ``as_of`` seen is excluded from the publish ages: the probe
    joined that cycle mid-way, so its age says nothing about when it was
    born. Only an advance observed during the run counts.

    A sample whose ``as_of`` is OLDER than the newest one seen so far is a
    regression: the API served a previous snapshot after a newer one had
    already been served, which is a serving bug rather than a cycle. The
    first live run saw four in an hour, ten seconds each. They are counted
    and reported on their own line, and kept out of the period and
    publish-age statistics, which would otherwise show a period of -60s
    and a "publish age" of 110s that no snapshot ever had.
    """
    periods: List[float] = []
    publish_ages: List[float] = []
    ages: List[float] = []
    newest: Optional[datetime] = None  # high-water mark
    snapshots = 0
    regressions = 0

    for sample in samples:
        if sample.age_seconds is not None:
            ages.append(sample.age_seconds)
        if sample.as_of is None:
            continue
        if newest is not None and sample.as_of < newest:
            regressions += 1
            continue
        if newest is None or sample.as_of != newest:
            snapshots += 1
            if newest is not None:
                periods.append((sample.as_of - newest).total_seconds())
                if sample.age_seconds is not None:
                    publish_ages.append(sample.age_seconds)
            newest = sample.as_of

    return Summary(
        samples=len(samples),
        snapshots=snapshots,
        period_seconds=periods,
        publish_age_seconds=publish_ages,
        ages=ages,
        regressions=regressions,
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
    return "min {}  median {}  p90 {}  max {}".format(
        fmt_age(min(values)),
        fmt_age(statistics.median(values)),
        fmt_age(percentile(values, 90)),
        fmt_age(max(values)),
    )


def format_report(summary: Summary, interval: float) -> str:
    lines = [
        f"samples: {summary.samples}   distinct snapshots: {summary.snapshots}",
        f"cycle period (as_of to as_of):   {_stats(summary.period_seconds)}",
        f"age at publish (first sight):    {_stats(summary.publish_age_seconds)}"
        f"   [resolution {interval:.0f}s plus the 5s edge cache: true value is lower]",
        f"age seen by a random poll:       {_stats(summary.ages)}",
    ]
    if summary.regressions:
        lines.append(
            f"as_of went BACKWARDS in {summary.regressions} sample(s): an older snapshot "
            "was served after a newer one. That is a serving bug, not latency; "
            "look for more than one API worker each holding its own cache."
        )
    if summary.snapshots == 1 and summary.samples > 1:
        lines.append(
            "as_of never advanced during the run: the market was closed or the "
            "engine is not publishing. Run this during the cash session "
            "(09:30-16:00 ET on a trading day)."
        )
    elif summary.snapshots < 3:
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
        "regressions": summary.regressions,
        "probe_interval_seconds": interval,
        "cycle_period_seconds": block(summary.period_seconds),
        "age_at_publish_seconds": block(summary.publish_age_seconds),
        "age_seen_by_random_poll_seconds": block(summary.ages),
    }


def should_print(sample: Sample, index: int, heartbeat_every: int, verbose: bool) -> bool:
    """Which samples earn a line: every one when verbose; otherwise the
    first, each advance, and one heartbeat per ``heartbeat_every``."""
    if verbose or sample.advanced:
        return True
    return index % max(1, heartbeat_every) == 0


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
    force: bool = False,
    bust_cache: bool = False,
) -> Tuple[List[Sample], int]:
    """Poll until ``duration_seconds`` elapse; ``(samples, exit_code)``.

    Tries v2 first for its server-clock ``evaluated_at`` and drops to v1 for
    the rest of the run on a 404, which is what an API without the v2 mirror
    answers. A refused key ends the run immediately: every later sample
    would fail the same way. So does a closed market, unless ``force``.
    """
    version = 2
    samples: List[Sample] = []
    previous: Optional[Sample] = None
    failures = 0
    started = clock()

    while True:
        now = clock()
        bust = f"{int(now.timestamp() * 1000)}-{len(samples)}" if bust_cache else None
        try:
            status, body, cache_status = fetch(levels_url(base_url, version, symbol, bust))
        except KeyboardInterrupt:
            # An hour of samples is worth a report even if the trader got
            # bored at fifty minutes.
            log("interrupted; summarizing what was collected")
            return samples, EXIT_OK

        if status == 404 and version == 2:
            log("v2 not available here; continuing with v1")
            version = 1
            continue
        if status == 404:
            log(f"HTTP 404 from v1 as well: no levels for {symbol.upper()} (check --symbol)")
            return samples, EXIT_UNREACHABLE
        if status in (401, 403):
            log(f"key refused (HTTP {status}); nothing to measure")
            return samples, EXIT_UNREACHABLE
        if body is None:
            failures += 1
            log(f"fetch failed (HTTP {status}); {failures} consecutive")
            if failures >= MAX_CONSECUTIVE_FAILURES:
                log("giving up")
                return samples, EXIT_UNREACHABLE
        else:
            failures = 0
            sample = take_sample(body, now, version, previous, cache_status)
            samples.append(sample)
            previous = sample
            if on_sample is not None:
                on_sample(sample)
            if len(samples) == 1 and duration_seconds > 0:
                reason = nothing_to_measure(sample)
                if reason is not None:
                    if force:
                        log(f"note: {reason}; polling anyway (--force)")
                    else:
                        log(f"stopping: {reason}. Run during the cash session, "
                            "or pass --force to poll anyway.")
                        return samples, EXIT_NOTHING_TO_MEASURE

        if (clock() - started).total_seconds() + interval > duration_seconds:
            return samples, EXIT_OK
        try:
            sleep(interval)
        except KeyboardInterrupt:
            log("interrupted; summarizing what was collected")
            return samples, EXIT_OK


def format_sample(sample: Sample) -> str:
    """One line per sample. ``eval`` is the server's own clock for this
    response, which is what separates "the app said this at :45" from "the
    edge handed back a copy from :35"."""
    as_of = sample.as_of.strftime("%H:%M:%S") if sample.as_of else "-"
    marker = ""
    if sample.regressed:
        marker = "  WENT BACKWARDS"
    elif sample.advanced:
        marker = "  NEW SNAPSHOT"
    status = ""
    if sample.session or sample.freshness:
        status = f"  [{sample.session or '?'}/{sample.freshness or '?'}]"
    cache = f"  {sample.cache_status}" if sample.cache_status else ""
    return (
        f"{sample.sample_at.strftime('%H:%M:%S')}  eval {sample.evaluated_at.strftime('%H:%M:%S')}"
        f"  as_of {as_of}  age {fmt_age(sample.age_seconds):>10}  "
        f"v{sample.api_version}{status}{cache}{marker}"
    )


def _write_csv(path: str, samples: Sequence[Sample]) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "sample_at",
                "evaluated_at",
                "as_of",
                "age_seconds",
                "api_version",
                "advanced",
                "regressed",
                "session",
                "freshness",
                "cache_status",
            ]
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
                    int(s.regressed),
                    s.session or "",
                    s.freshness or "",
                    s.cache_status or "",
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
        help=(
            "seconds between samples (default 5, which is also the edge cache; "
            "faster buys nothing)"
        ),
    )
    parser.add_argument("--minutes", type=float, default=30.0, help="how long to run (default 30)")
    parser.add_argument("--once", action="store_true", help="take one sample and exit")
    parser.add_argument("--force", action="store_true", help="keep polling on a closed market")
    parser.add_argument(
        "--bust-cache",
        action="store_true",
        help="add a unique query token per request so every sample bypasses the edge cache",
    )
    parser.add_argument("--csv", metavar="PATH", help="write every sample to this CSV")
    parser.add_argument("--json", action="store_true", help="print the summary as JSON")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="print every sample, not just advances and heartbeats",
    )
    parser.add_argument("--quiet", action="store_true", help="print no per-sample lines")
    args = parser.parse_args(argv)

    if not args.api_key:
        print("no API key: pass --api-key or set API_KEY", file=sys.stderr)
        return EXIT_UNREACHABLE
    if args.interval <= 0:
        print("--interval must be positive", file=sys.stderr)
        return EXIT_UNREACHABLE

    duration = 0.0 if args.once else args.minutes * 60.0
    heartbeat_every = max(1, round(60.0 / args.interval))
    counter = {"n": 0}

    def on_sample(sample: Sample) -> None:
        index = counter["n"]
        counter["n"] += 1
        if should_print(sample, index, heartbeat_every, args.verbose):
            print(format_sample(sample))

    def fetch(url: str) -> Tuple[int, Optional[dict], Optional[str]]:
        return fetch_json(url, args.api_key)

    if not args.once:
        print(
            f"probing {args.symbol.upper()} at {args.base_url} every {args.interval:g}s "
            f"for {args.minutes:g} min; printing advances and one line a minute"
            + (" (verbose)" if args.verbose else ""),
            file=sys.stderr,
        )

    samples, code = run_probe(
        fetch=fetch,
        base_url=args.base_url,
        symbol=args.symbol,
        interval=args.interval,
        duration_seconds=duration,
        on_sample=None if args.quiet else on_sample,
        force=args.force,
        bust_cache=args.bust_cache,
    )

    if args.csv and samples:
        _write_csv(args.csv, samples)

    if code == EXIT_NOTHING_TO_MEASURE:
        return code

    summary = summarize(samples)
    if args.json:
        print(json.dumps(summary_json(summary, args.interval), indent=2))
    elif samples:
        print()
        print(format_report(summary, args.interval))
    return code


if __name__ == "__main__":
    sys.exit(main())
