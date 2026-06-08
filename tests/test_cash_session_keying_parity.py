"""Phase 4 parity harness for the cash-session keying cutover.

Compares the legacy ET-calendar-date LAG-CASE classification against the
proposed cash-session-date classification, row-for-row, against a real
day of option_chains data.  Read-only: derives the volume_delta /
ask_vol_delta / bid_vol_delta columns via a SELECT-only mirror of the
production flow_contract_facts INSERT CTE, never writes back.

Cash-session keying is now the unconditional production path (the
``USE_CASH_SESSION_KEYING`` rollout flag has been removed). This harness
is retained as a regression guard: it confirms the production cash keying
still differs from the legacy calendar-date formulation ONLY at session
boundaries. When the two formulations agree row-for-row, there is no
divergence at all.

When they DISAGREE, every divergence MUST have a structural cause -- the
LAG and current timestamps for that row straddle exactly one of the two
session boundaries the formulations define differently:

* ``crosses_calendar_only`` -- LAG and current are in different ET
  calendar days but the same cash session.  Legacy sees a boundary
  (so it uses the absolute volume); cash sees continuation (delta from
  prior).  Happens on pre-09:30 ET rows whose predecessor was on the
  prior calendar day.  Cash is correct here: TradeStation does not
  reset cumulative volume at calendar midnight, so the delta IS the
  right value.

* ``crosses_cash_open_only`` -- LAG and current are in the same ET
  calendar day but different cash sessions.  Legacy sees continuation
  (delta); cash sees a boundary (absolute).  Happens for contracts
  whose first post-09:30 tick on day D has its LAG in the pre-09:30 ET
  hours of the same day D.  Cash is correct here too: TS resets at
  09:30, so the post-09:30 cumulative IS a fresh count.

* ``holiday_bridge_gap`` -- a sub-category of crosses_calendar_only
  where the LAG/current pair spans an NYSE-observed holiday (not just
  a weekend).  Cash correctly maps both ends to the same prior-trading-
  day session; legacy still sees a calendar boundary.  Broken out as
  its own category so a future regression in the SQL/Python holiday
  walk-back (e.g. NYSE_HOLIDAYS not propagating to one side) shows up
  visibly in the diagnostic rather than being mixed with normal
  weekend bridges.

* ``both_boundaries`` -- both formulations agree (no divergence).
* ``neither_boundary`` -- both formulations agree.  If a divergence
  shows up classified this way, something is wrong with the LAG-CASE
  rendering and the test fails.

The keying is sound when the parity harness returns ONLY
crosses_calendar_only, crosses_cash_open_only, and holiday_bridge_gap
divergences.  All three are by-design differences documented in the
rollout commit messages.  The diagnostic prints the count + a sampling
per cause so an operator auditing a real day can eyeball "how many rows
were affected" and "is the magnitude of those deltas sensible".

Run this against a real day of data to audit the keying:

    CASH_SESSION_KEYING_PARITY_DSN=postgresql://USER@HOST:PORT/DB \\
    CASH_SESSION_KEYING_PARITY_SYMBOL=SPY \\
    CASH_SESSION_KEYING_PARITY_DATE=2026-05-22 \\
    pytest tests/test_cash_session_keying_parity.py -v -s

Date defaults to the most recent calendar day with option_chains data;
symbol defaults to SPY.  The CTE is identical to the production INSERT's
LAG-CASE pipeline, parameterized only on the same-session clause via
``_flow_lag_same_session_clause`` so this test exercises the same SQL
the production write path uses.
"""

from __future__ import annotations

import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
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
# misleading about what went wrong.  Also catches the ``HOST:PORT``
# token form (e.g. ``postgresql://postgres@HOST:PORT/DB``) that the
# previous guard let through: asyncpg fails int-parsing ``PORT`` deep
# inside the URI parser, which is even less helpful than the IDNA
# error.
_PLACEHOLDER_TOKENS = ("...", "HOST", "PORT", "USER", "PASS", "DBNAME")
_PLACEHOLDER_DSN = _DSN is not None and (
    _DSN.rstrip("/") in ("postgres:", "postgresql:")
    or any(token in _DSN for token in _PLACEHOLDER_TOKENS)
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
# INSERT/ON CONFLICT tail.  It also DELIBERATELY drops production's
# ``AND timestamp > $5`` backfill-window bound: that bound only limits
# which already-computed rows one incremental refresh emits (it never
# changes a row's delta) and would restrict the legacy and cash result
# sets identically, so dropping it lets the harness sweep the whole ET
# day -- strictly more coverage, not less.  If the production CTE
# structure changes, this test will silently drift unless
# _flow_lag_same_session_clause itself is what changes -- in which case
# the gating tests catch it.
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
            LAG(s.timestamp) OVER w AS prior_ts,
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
        prior_ts,
        option_symbol,
        volume_delta,
        ask_vol_delta,
        bid_vol_delta
    FROM with_prev
    WHERE timestamp >= $2
      AND volume_delta > 0
    ORDER BY timestamp, option_symbol
"""


def _classify_divergence_cause(prior_ts, current_ts, et) -> str:
    """Why might the two formulations disagree on this LAG/current pair?

    See the module docstring for the four base cases.  ``no_prior``
    means the row has no LAG (first row in the contract's window
    after the seed); it cannot be the source of a divergence so it's
    classified separately and ignored by the failure check.

    ``crosses_calendar_only`` is further split into a weekend-bridge
    and a holiday-bridge sub-category so a regression that re-breaks
    only the holiday walk-back (e.g. SQL and Python falling out of
    sync on NYSE_HOLIDAYS) is visible in the output.  Both sub-
    categories are accepted divergence causes -- they're just
    sub-typed for diagnostic clarity.
    """
    from src.market_calendar import NYSE_HOLIDAYS
    from src.validation import cash_session_date

    if prior_ts is None:
        return "no_prior"
    prior_cal = prior_ts.astimezone(et).date()
    curr_cal = current_ts.astimezone(et).date()
    prior_cash = cash_session_date(prior_ts)
    curr_cash = cash_session_date(current_ts)
    legacy_same = prior_cal == curr_cal
    cash_same = prior_cash == curr_cash
    if legacy_same and not cash_same:
        return "crosses_cash_open_only"
    if not legacy_same and cash_same:
        # Distinguish whether the calendar-only crossing was spanned
        # by weekend-only or also by an NYSE holiday.  The bridge
        # range is all calendar dates strictly between prior_cal and
        # curr_cal inclusive of the endpoints; if any of those is in
        # NYSE_HOLIDAYS, this is a holiday_bridge_gap divergence.
        lo, hi = sorted([prior_cal, curr_cal])
        bridge_days = [lo + timedelta(days=i) for i in range((hi - lo).days + 1)]
        if any(d in NYSE_HOLIDAYS for d in bridge_days):
            return "holiday_bridge_gap"
        return "crosses_calendar_only"
    if not legacy_same and not cash_same:
        return "both_boundaries"
    return "neither_boundary"


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

    # Classify every divergent row by its structural cause.  See the
    # _classify_divergence_cause helper / module docstring for what each
    # cause means.  Divergences with cause ``neither_boundary`` are bugs
    # (both formulations should agree when neither sees a session
    # boundary between LAG and current); the test fails if any appear.
    # The other causes are by-design behavior changes documented in the
    # rollout commit message, and the test passes for those.
    def _et_hour(ts: datetime) -> int:
        return ts.astimezone(et).hour

    def _prior_ts_for(key: Tuple[datetime, str]):
        # Both formulations share the same WINDOW (PARTITION BY
        # option_symbol ORDER BY timestamp), so prior_ts is identical
        # for any key present in either index.  Prefer legacy_idx
        # because legacy_only rows are guaranteed to be there.
        row = legacy_idx.get(key) or cash_idx[key]
        return row["prior_ts"]

    all_divergent_keys: List[Tuple[datetime, str]] = sorted(
        set(legacy_only) | set(cash_only) | {d["key"] for d in delta_diffs}
    )

    cause_counts: Dict[str, int] = defaultdict(int)
    hour_buckets: Dict[int, int] = defaultdict(int)
    unexplained: List[Tuple[Tuple[datetime, str], Any]] = []
    samples_per_cause: Dict[str, List[str]] = defaultdict(list)

    for key in all_divergent_keys:
        prior_ts = _prior_ts_for(key)
        cause = _classify_divergence_cause(prior_ts, key[0], et)
        cause_counts[cause] += 1
        hour_buckets[_et_hour(key[0])] += 1
        # These three causes are all "the two formulations should have
        # emitted the SAME value here, but didn't" -- i.e. contradictions
        # that can only arise from a mis-rendered LAG-CASE or inconsistent
        # session helpers:
        #   neither_boundary -- neither sees a boundary, so both take the
        #                       intra-session delta branch: must agree.
        #   both_boundaries  -- each sees a boundary, so both take the
        #                       absolute-volume branch (which doesn't depend
        #                       on the session clause): must agree.
        #   no_prior         -- the LAG-NULL branch is identical in both
        #                       forms, so a divergent first-of-partition row
        #                       can only exist if the SQL is mis-rendered.
        if cause in ("neither_boundary", "both_boundaries", "no_prior"):
            unexplained.append((key, prior_ts))
        if len(samples_per_cause[cause]) < 3:
            # Three samples per cause is enough to eyeball "do the
            # deltas look sensible" without dumping the world.
            if key in legacy_idx and key in cash_idx:
                samples_per_cause[cause].append(
                    f"    {key[0].astimezone(et).isoformat()} {key[1]}  "
                    f"legacy_vol_delta={legacy_idx[key]['volume_delta']}  "
                    f"cash_vol_delta={cash_idx[key]['volume_delta']}  "
                    f"prior_ts={prior_ts.astimezone(et).isoformat() if prior_ts else 'NULL'}"
                )
            else:
                side = "legacy-only" if key in legacy_idx else "cash-only"
                row = legacy_idx.get(key) or cash_idx[key]
                samples_per_cause[cause].append(
                    f"    {key[0].astimezone(et).isoformat()} {key[1]}  [{side}]  "
                    f"vol_delta={row['volume_delta']}  "
                    f"prior_ts={prior_ts.astimezone(et).isoformat() if prior_ts else 'NULL'}"
                )

    diagnostic_lines: List[str] = [
        "",
        f"Symbol={_SYMBOL} ET session date={session_date}",
        f"  legacy rows={len(legacy_rows)}  cash rows={len(cash_rows)}",
        f"  legacy_only={len(legacy_only)}  cash_only={len(cash_only)}  "
        f"delta_diffs={len(delta_diffs)}  total_divergent={len(all_divergent_keys)}",
        f"  divergences by ET hour-of-day: {dict(sorted(hour_buckets.items()))}",
        f"  divergences by structural cause: {dict(sorted(cause_counts.items()))}",
    ]
    for cause in sorted(samples_per_cause):
        diagnostic_lines.append(f"  sample {cause}:")
        diagnostic_lines.extend(samples_per_cause[cause])
    diagnostic = "\n".join(diagnostic_lines) + "\n"

    if unexplained:
        # Every divergence here has a cause that should have produced
        # IDENTICAL values in both formulations (see the per-cause notes
        # above): neither_boundary, both_boundaries, or no_prior.  Their
        # presence implies the LAG-CASE SQL is mis-rendered or the date
        # helpers return inconsistent results.  Surface up to ten so the
        # failure message is actionable.
        sample = "\n".join(
            f"    {ts.astimezone(et).isoformat()} {sym}  "
            f"prior_ts={prior.astimezone(et).isoformat() if prior else 'NULL'}"
            for (ts, sym), prior in unexplained[:10]
        )
        pytest.fail(
            "Cash-session keying parity FAILED -- "
            f"{len(unexplained)} divergences whose cause should have produced "
            "identical values (LAG-CASE SQL mis-rendered or session helpers "
            f"inconsistent).\n"
            f"{diagnostic}"
            f"  example contradictory divergences:\n"
            f"{sample}"
        )

    # Pass: every divergence has a known structural cause.  Print the
    # breakdown so ``pytest -v -s`` shows it and the operator can
    # eyeball "is this a sensible amount of change before I flip the
    # flag in staging".  Without -s, pytest captures the print but the
    # test still passes.
    print(diagnostic + "OK -- all divergences explained by known boundary causes.")
