"""Phase 4 parity harness for the cash-session keying cutover.

Compares the legacy ET-calendar-date LAG-CASE classification against the
proposed cash-session-date classification, row-for-row, against a real
day of option_chains data.  Read-only: derives the volume_delta /
ask_vol_delta / bid_vol_delta columns via a SELECT-only mirror of the
production flow_contract_facts INSERT CTE, never writes back.

When the two formulations agree row-for-row, there is no divergence and
flipping ``USE_CASH_SESSION_KEYING`` on is operationally safe.

When they DISAGREE, the harness expects the divergences to cluster in
two narrow bands per ET session date:

* the 00:00 ET hour, where the legacy formulation treats midnight as
  a session boundary and the cash formulation does not (a row at 00:15
  ET belongs to the same cash session as the prior 23:45 ET row), and
* the 09:30 ET hour, where the cash formulation treats 09:30 as the
  session boundary and the legacy formulation does not (a row at 09:31
  ET starts a fresh cumulative against the cash-keyed prior at 09:29).

Any divergence OUTSIDE those bands is a real bug -- the helpers must
agree throughout RTH, the overnight extended-hours run, and the
pre-09:30 wee hours.  The test asserts exactly that.

Run this before flipping the flag in any environment:

    CASH_SESSION_KEYING_PARITY_DSN=postgres://... \\
    CASH_SESSION_KEYING_PARITY_SYMBOL=SPY \\
    CASH_SESSION_KEYING_PARITY_DATE=2026-05-22 \\
    pytest tests/test_cash_session_keying_parity.py -v

Date defaults to the most recent calendar day with option_chains data;
symbol defaults to SPY.  The CTE is identical to the production INSERT's
LAG-CASE pipeline, parameterized only on the same-session clause via
``_flow_lag_same_session_clause`` so this test exercises the same SQL
the production write path uses.
"""

from __future__ import annotations

import os
from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Tuple

import pytest

pytestmark = pytest.mark.integration

_DSN = os.getenv("CASH_SESSION_KEYING_PARITY_DSN")
_SYMBOL = os.getenv("CASH_SESSION_KEYING_PARITY_SYMBOL", "SPY")
_DATE_ENV = os.getenv("CASH_SESSION_KEYING_PARITY_DATE")

# Catch the common copy-paste failure where the operator pasted the
# docstring's literal ``postgres://...`` example unchanged, or otherwise
# handed us a DSN with no real host.  Without this guard, asyncpg gets
# as far as DNS-resolving ``...`` and dies inside the IDNA codec with
# ``UnicodeError: label empty or too long`` -- accurate, but actively
# misleading about what went wrong.
_PLACEHOLDER_DSN = _DSN is not None and (
    "..." in _DSN or _DSN.rstrip("/") in ("postgres:", "postgresql:")
)

if _PLACEHOLDER_DSN:
    pytest_skip_reason = (
        f"CASH_SESSION_KEYING_PARITY_DSN={_DSN!r} looks like a placeholder. "
        "Substitute real values, e.g. for the codebase's default pgpass "
        "provider:  PGPASSFILE=~/.pgpass "
        "CASH_SESSION_KEYING_PARITY_DSN="
        "postgresql://USER@HOST:PORT/DB?sslmode=require  (asyncpg will look "
        "up the password in ~/.pgpass).  Or inline the password: "
        "postgresql://USER:PASS@HOST:PORT/DB."
    )
else:
    pytest_skip_reason = (
        "CASH_SESSION_KEYING_PARITY_DSN not set — Phase 4 cash-session "
        "keying parity harness skipped.  See module docstring for usage."
    )


# The CTE mirrors src/api/database.py::_do_refresh_flow_cache exactly,
# minus the columns the LAG-CASE classification doesn't touch (strike,
# expiration, option_type, IV, delta, last/mid/bid/ask) and minus the
# INSERT/ON CONFLICT tail.  If the production CTE structure changes,
# this test will silently drift unless _flow_lag_same_session_clause
# itself is what changes -- in which case the gating tests catch it.
_PARITY_CTE = """
    WITH window_rows AS (
        SELECT
            oc.timestamp,
            oc.option_symbol,
            oc.volume,
            oc.ask_volume,
            oc.bid_volume
        FROM option_chains oc
        WHERE oc.underlying = $1
          AND oc.timestamp >= $2
          AND oc.timestamp <= $3
    ),
    active_symbols AS (
        SELECT DISTINCT option_symbol FROM window_rows
    ),
    seed_rows AS (
        SELECT
            oc.timestamp,
            oc.option_symbol,
            oc.volume,
            oc.ask_volume,
            oc.bid_volume
        FROM active_symbols s
        JOIN LATERAL (
            SELECT
                oc.timestamp,
                oc.option_symbol,
                oc.volume,
                oc.ask_volume,
                oc.bid_volume
            FROM option_chains oc
            WHERE oc.underlying = $1
              AND oc.option_symbol = s.option_symbol
              AND oc.timestamp < $2
            ORDER BY oc.timestamp DESC
            LIMIT 1
        ) oc ON TRUE
    ),
    source_rows AS (
        SELECT * FROM seed_rows
        UNION ALL
        SELECT * FROM window_rows
    ),
    with_prev AS (
        SELECT
            s.timestamp,
            s.option_symbol,
            CASE
                WHEN LAG(s.volume) OVER w IS NULL THEN COALESCE(s.volume, 0)
                WHEN {same_session}
                    THEN GREATEST(COALESCE(s.volume, 0) - COALESCE(LAG(s.volume) OVER w, 0), 0)
                ELSE COALESCE(s.volume, 0)
            END::bigint AS volume_delta,
            CASE
                WHEN LAG(s.ask_volume) OVER w IS NULL THEN COALESCE(s.ask_volume, 0)
                WHEN {same_session}
                    THEN GREATEST(
                        COALESCE(s.ask_volume, 0) - COALESCE(LAG(s.ask_volume) OVER w, 0), 0
                    )
                ELSE COALESCE(s.ask_volume, 0)
            END::bigint AS ask_vol_delta,
            CASE
                WHEN LAG(s.bid_volume) OVER w IS NULL THEN COALESCE(s.bid_volume, 0)
                WHEN {same_session}
                    THEN GREATEST(
                        COALESCE(s.bid_volume, 0) - COALESCE(LAG(s.bid_volume) OVER w, 0), 0
                    )
                ELSE COALESCE(s.bid_volume, 0)
            END::bigint AS bid_vol_delta
        FROM source_rows s
        WINDOW w AS (PARTITION BY s.option_symbol ORDER BY s.timestamp)
    )
    SELECT
        timestamp,
        option_symbol,
        volume_delta,
        ask_vol_delta,
        bid_vol_delta
    FROM with_prev
    WHERE timestamp >= $2
      AND volume_delta > 0
    ORDER BY timestamp, option_symbol
"""


def _resolve_window(date_str: str | None) -> Tuple[datetime, datetime, date]:
    """Resolve the (UTC start, UTC end, ET session date) window to derive.

    Accepts ``YYYY-MM-DD`` (treated as a calendar date in ET).  The
    window covers that ET calendar day from 00:00 ET to 23:59:59.999 ET
    so we sweep across BOTH the calendar-midnight and 09:30-open
    boundaries -- those are the exact two bands where the two
    formulations disagree.  Returns the UTC bounds and the ET date.
    """
    import pytz

    et = pytz.timezone("US/Eastern")
    if date_str is None:
        raise ValueError("date_str required")
    parts = date_str.split("-")
    if len(parts) != 3:
        raise ValueError(f"expected YYYY-MM-DD, got {date_str!r}")
    et_date = date(int(parts[0]), int(parts[1]), int(parts[2]))
    start_et = et.localize(datetime(et_date.year, et_date.month, et_date.day, 0, 0, 0))
    end_et = et.localize(datetime(et_date.year, et_date.month, et_date.day, 23, 59, 59, 999_000))
    return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc), et_date


@pytest.mark.skipif(_DSN is None or _PLACEHOLDER_DSN, reason=pytest_skip_reason)
def test_legacy_and_cash_session_keying_diverge_only_at_session_boundaries():
    import asyncio

    import asyncpg
    import pytz

    from src.api.database import _flow_lag_same_session_clause

    et = pytz.timezone("US/Eastern")

    async def _resolve_latest_date(conn: "asyncpg.Connection") -> str:
        row = await conn.fetchrow(
            """
            SELECT (timestamp AT TIME ZONE 'America/New_York')::date AS d
            FROM option_chains
            WHERE underlying = $1
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            _SYMBOL,
        )
        if row is None:
            return ""
        return row["d"].isoformat()

    async def _run() -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
        conn = await asyncpg.connect(_DSN)
        try:
            date_str = _DATE_ENV if _DATE_ENV is not None else await _resolve_latest_date(conn)
            if not date_str:
                return "", [], []
            start_utc, end_utc, _ = _resolve_window(date_str)
            legacy_sql = _PARITY_CTE.format(
                same_session=_flow_lag_same_session_clause(use_cash_keying=False)
            )
            cash_sql = _PARITY_CTE.format(
                same_session=_flow_lag_same_session_clause(use_cash_keying=True)
            )
            legacy = await conn.fetch(legacy_sql, _SYMBOL, start_utc, end_utc)
            cash = await conn.fetch(cash_sql, _SYMBOL, start_utc, end_utc)
            return date_str, [dict(r) for r in legacy], [dict(r) for r in cash]
        finally:
            await conn.close()

    resolved_date_str, legacy_rows, cash_rows = asyncio.run(_run())
    if not resolved_date_str:
        pytest.skip(f"no option_chains rows for {_SYMBOL}")
    _, _, session_date = _resolve_window(resolved_date_str)

    if not legacy_rows and not cash_rows:
        pytest.skip(
            f"no derived rows for {_SYMBOL} on {resolved_date_str} -- "
            "is option_chains backfilled for this day?"
        )

    # Both formulations operate on the same source rows.  Any (timestamp,
    # option_symbol) present in one but not the other would imply the
    # downstream WHERE volume_delta > 0 filter dropped a row in one
    # formulation -- which is exactly what we want to flag.
    legacy_idx = {(r["timestamp"], r["option_symbol"]): r for r in legacy_rows}
    cash_idx = {(r["timestamp"], r["option_symbol"]): r for r in cash_rows}

    legacy_only = sorted(set(legacy_idx) - set(cash_idx))
    cash_only = sorted(set(cash_idx) - set(legacy_idx))

    # Rows in both, but with different deltas.
    common_keys = set(legacy_idx) & set(cash_idx)
    delta_diffs: List[Dict[str, Any]] = []
    for key in sorted(common_keys):
        a, b = legacy_idx[key], cash_idx[key]
        if (
            a["volume_delta"] != b["volume_delta"]
            or a["ask_vol_delta"] != b["ask_vol_delta"]
            or a["bid_vol_delta"] != b["bid_vol_delta"]
        ):
            delta_diffs.append({"key": key, "legacy": a, "cash": b})

    # Bucket the divergences by ET hour-of-day for the assertion + report.
    def _et_hour(ts: datetime) -> int:
        return ts.astimezone(et).hour

    diff_keys: List[Tuple[datetime, str]] = (
        [(ts, sym) for ts, sym in legacy_only]
        + [(ts, sym) for ts, sym in cash_only]
        + [d["key"] for d in delta_diffs]
    )
    hour_buckets: Dict[int, int] = defaultdict(int)
    for ts, _sym in diff_keys:
        hour_buckets[_et_hour(ts)] += 1

    # Allowed bands: the calendar-midnight transition (hour 0) and the
    # cash-open transition (hour 9, since 09:30 is in hour 9).  Anything
    # else is a real bug.
    allowed_hours = {0, 9}
    bad_hour_buckets = {h: n for h, n in hour_buckets.items() if h not in allowed_hours and n > 0}

    diagnostic = (
        f"\nSymbol={_SYMBOL} ET session date={session_date}\n"
        f"  legacy rows={len(legacy_rows)}  cash rows={len(cash_rows)}\n"
        f"  legacy_only={len(legacy_only)}  cash_only={len(cash_only)}  "
        f"delta_diffs={len(delta_diffs)}\n"
        f"  divergences by ET hour-of-day: {dict(sorted(hour_buckets.items()))}\n"
        f"  allowed bands: {sorted(allowed_hours)} (00:00 = calendar-midnight, "
        f"09:00-09:59 = 09:30 cash open)\n"
    )

    if bad_hour_buckets:
        # Show up to 10 offending rows so the failure message is actionable
        # without dumping the whole differing-row set.
        sample: List[str] = []
        for d in delta_diffs[:10]:
            h = _et_hour(d["key"][0])
            if h in bad_hour_buckets:
                sample.append(
                    f"    {d['key'][0].astimezone(et).isoformat()} {d['key'][1]} "
                    f"legacy={d['legacy']['volume_delta']} cash={d['cash']['volume_delta']}"
                )
        for ts, sym in (legacy_only + cash_only)[:10]:
            h = _et_hour(ts)
            if h in bad_hour_buckets:
                side = "legacy-only" if (ts, sym) in legacy_only else "cash-only"
                sample.append(f"    {ts.astimezone(et).isoformat()} {sym} [{side}]")
        sample_str = "\n".join(sample) if sample else "    (no examples captured)"
        pytest.fail(
            "Cash-session keying parity FAILED -- divergence outside the "
            f"allowed 00:00/09:30 ET bands.\n{diagnostic}"
            f"  offending hours: {bad_hour_buckets}\n"
            f"  example offenders:\n{sample_str}"
        )

    # Pass -- print the diagnostic so the user can see how much divergence
    # there was even when it's all in the expected bands.  pytest -v shows
    # this; pytest without -v swallows it but the test still passes.
    print(diagnostic + "OK -- all divergences within expected boundary bands.")
