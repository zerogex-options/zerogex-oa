"""Verify ``component_normalizer_cache`` rows are fresh.

Catches the silent-failure mode where ``zerogex-oa-normalizer-refresh.timer``
has stopped firing (or always errors) but the rest of the system keeps
running on stale per-symbol magnitudes.  Designed to be called from the
shell, monitoring (Nagios/Prometheus textfile collector), or a follow-up
systemd unit.

Exit codes:
    0 — all expected (active symbol × field) cache rows are within the
        max-age window.
    1 — at least one row is older than ``--max-age-hours``.
        With ``--strict``, missing rows also fail.
    2 — DB connection or query error.

Missing rows do NOT fail the check by default.  The populator skips
fields with fewer than ``MIN_SAMPLES`` samples (see
:mod:`src.tools.normalizer_cache_refresh`), which is a legitimate state
for a thin underlying that hasn't accumulated enough history yet.
``--strict`` enforces full coverage when you want to alert on that.

Usage:
    python -m src.tools.normalizer_cache_healthcheck
    python -m src.tools.normalizer_cache_healthcheck --max-age-hours 48
    python -m src.tools.normalizer_cache_healthcheck --strict --json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence

from src.database.connection import db_connection
from src.tools.normalizer_cache_refresh import FIELD_SPECS

logger = logging.getLogger(__name__)

DEFAULT_MAX_AGE_HOURS = 36.0


@dataclass(frozen=True)
class FieldStatus:
    underlying: str
    field_name: str
    status: str  # "fresh" | "stale" | "missing"
    age_hours: float | None
    sample_size: int | None
    p95: float | None
    updated_at: datetime | None


def _evaluate(
    rows: Iterable[tuple],
    expected: Sequence[tuple[str, str]],
    max_age: timedelta,
    now: datetime,
) -> list[FieldStatus]:
    """Pure function over fetched data.

    ``rows`` is an iterable of ``(underlying, field_name, updated_at,
    sample_size, p95)`` tuples drawn from ``component_normalizer_cache``.
    Returns one :class:`FieldStatus` per (active symbol × expected field),
    in the order ``expected`` was given.
    """
    rowmap: dict[tuple[str, str], tuple] = {(r[0], r[1]): r for r in rows}
    out: list[FieldStatus] = []
    for underlying, field_name in expected:
        row = rowmap.get((underlying, field_name))
        if row is None:
            out.append(
                FieldStatus(
                    underlying=underlying,
                    field_name=field_name,
                    status="missing",
                    age_hours=None,
                    sample_size=None,
                    p95=None,
                    updated_at=None,
                )
            )
            continue
        _, _, updated_at, sample_size, p95 = row
        age = now - updated_at
        out.append(
            FieldStatus(
                underlying=underlying,
                field_name=field_name,
                status="stale" if age > max_age else "fresh",
                age_hours=age.total_seconds() / 3600.0,
                sample_size=int(sample_size) if sample_size is not None else None,
                p95=float(p95) if p95 is not None else None,
                updated_at=updated_at,
            )
        )
    return out


def _active_symbols(cur) -> list[str]:
    cur.execute(
        "SELECT symbol FROM symbols WHERE COALESCE(is_active, TRUE) = TRUE ORDER BY symbol"
    )
    return [r[0] for r in cur.fetchall()]


def run(
    conn,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    strict: bool = False,
) -> tuple[int, list[FieldStatus]]:
    """Execute the healthcheck against a connected DB.

    Returns ``(exit_code, statuses)`` so callers (CLI / tests / future
    monitoring wrappers) can introspect the same data.
    """
    expected_fields = [spec.name for spec in FIELD_SPECS]
    with conn.cursor() as cur:
        symbols = _active_symbols(cur)
        if not symbols:
            logger.warning("No active symbols in symbols table — nothing to check")
            return 0, []
        cur.execute(
            """
            SELECT underlying, field_name, updated_at, sample_size, p95
            FROM component_normalizer_cache
            WHERE underlying = ANY(%s) AND field_name = ANY(%s)
            """,
            (symbols, expected_fields),
        )
        rows = cur.fetchall()

    expected = [(s, f) for s in symbols for f in expected_fields]
    now = datetime.now(timezone.utc)
    statuses = _evaluate(rows, expected, timedelta(hours=max_age_hours), now)

    n_stale = sum(1 for s in statuses if s.status == "stale")
    n_missing = sum(1 for s in statuses if s.status == "missing")
    exit_code = 0
    if n_stale > 0:
        exit_code = 1
    if strict and n_missing > 0:
        exit_code = 1
    return exit_code, statuses


def _print_human(statuses: list[FieldStatus], max_age_hours: float, strict: bool) -> None:
    n_fresh = sum(1 for s in statuses if s.status == "fresh")
    n_stale = sum(1 for s in statuses if s.status == "stale")
    n_missing = sum(1 for s in statuses if s.status == "missing")

    print(
        f"Normalizer cache healthcheck "
        f"(max-age={max_age_hours:g}h, strict={strict})"
    )
    print(f"  fresh: {n_fresh}, stale: {n_stale}, missing: {n_missing}")

    issues = [
        s
        for s in statuses
        if s.status == "stale" or (strict and s.status == "missing")
    ]
    if issues:
        print()
        print("Issues:")
        for s in issues:
            if s.status == "stale":
                print(
                    f"  STALE   {s.underlying:<8s} {s.field_name:<24s} "
                    f"age={s.age_hours:.1f}h updated_at={s.updated_at}"
                )
            else:
                print(
                    f"  MISSING {s.underlying:<8s} {s.field_name:<24s} "
                    f"(no cache row at all)"
                )

    if not issues:
        print("STATUS: OK")
    else:
        print(
            "STATUS: STALE — run `make normalizer-cache-refresh` "
            "or check the timer (`make normalizer-cache-status`)."
        )


def _print_json(
    statuses: list[FieldStatus], max_age_hours: float, strict: bool, exit_code: int
) -> None:
    payload = {
        "max_age_hours": max_age_hours,
        "strict": strict,
        "exit_code": exit_code,
        "n_fresh": sum(1 for s in statuses if s.status == "fresh"),
        "n_stale": sum(1 for s in statuses if s.status == "stale"),
        "n_missing": sum(1 for s in statuses if s.status == "missing"),
        "statuses": [
            {
                **asdict(s),
                "updated_at": s.updated_at.isoformat() if s.updated_at else None,
            }
            for s in statuses
        ],
    }
    json.dump(payload, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=DEFAULT_MAX_AGE_HOURS,
        help=(
            f"Rows older than this many hours are flagged as stale "
            f"(default {DEFAULT_MAX_AGE_HOURS:g}: well past the 04:30 ET nightly run "
            f"plus jitter and a missed-cycle buffer)."
        ),
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat missing rows as failures too.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON instead of human-readable text.",
    )
    parser.add_argument("--log-level", default="WARNING")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.WARNING),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    if args.max_age_hours <= 0:
        parser.error("--max-age-hours must be positive")

    try:
        with db_connection() as conn:
            exit_code, statuses = run(
                conn,
                max_age_hours=args.max_age_hours,
                strict=args.strict,
            )
    except Exception as exc:
        logger.error("Healthcheck DB error: %s", exc)
        return 2

    if args.json:
        _print_json(statuses, args.max_age_hours, args.strict, exit_code)
    else:
        _print_human(statuses, args.max_age_hours, args.strict)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
