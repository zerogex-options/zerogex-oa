"""Deterministic fixture for ``tests/test_flow_series_parity.py``.

The parity test compares the canonical 8-CTE pipeline against
``flow_series_5min`` snapshot rows.  For the test to have any real bite
the snapshot rows must have been written by the **incremental** form
(``SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2``), because the canonical form
is what the parity test then runs as the oracle.  Same SQL on both
sides defeats the purpose.

This script:

  1. Drops any prior fixture data (idempotent reruns).
  2. Inserts ``SPY`` into ``symbols``.
  3. Inserts ``underlying_quotes`` for the prior session.
  4. Inserts ``flow_by_contract`` rows for ~6 contracts × N buckets on a
     fixed historical date (the "prior session").  Values are chosen so
     per-bucket deltas vary (some flat, some growing, some with
     net_volume / net_premium sign flips) — anything that doesn't
     exercise LAG over multiple values is not a real parity check.
  5. Inserts a single ``flow_by_contract`` row on the next ET date so
     ``_resolve_flow_series_session(session='prior')`` resolves to the
     fixture date.
  6. Invokes ``SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2`` once per
     (prev_bar, curr_bar) pair across the fixture session, mirroring
     what the live analytics engine would do across N cycles.

Usage::

    python -m tests.fixtures.seed_flow_series_parity \\
        --dsn "postgresql://zerogex_test:test@localhost:5432/zerogex_test"

After running this, ``make flow-series-parity
FLOW_SERIES_PARITY_DSN=... FLOW_SERIES_PARITY_SESSION=prior``
must pass — the incremental writes plus the canonical CTE read must
agree row-for-row on the closed bars.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Tuple
from zoneinfo import ZoneInfo

import psycopg2

# Wire the project src/ onto sys.path so the import below resolves when
# this file is executed as ``python tests/fixtures/seed_flow_series_parity.py``
# directly (without ``-m``).
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.flow_series_sql import (  # noqa: E402
    SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2,
    SNAPSHOT_UPSERT_PSYCOPG2,
)

_ET = ZoneInfo("America/New_York")

# Fixed historical date so the test is deterministic.  2025-04-15 is a
# Tuesday — regular trading day, no US market holidays in that week.
PRIOR_DATE = date(2025, 4, 15)
CURRENT_DATE = date(2025, 4, 16)

# Session window on the prior date.  A full RTH session is 09:30-16:15
# ET = 6h45m = 81 intervals of 5 min = 82 bars (bar_start values, the
# count `generate_series` produces inclusive of both endpoints).  The
# parity test compares the canonical CTE — which auto-fills the full
# session via generate_series — against the snapshot rows, so the
# fixture must also be dense across all 82 bars.
NUM_ACTIVE_BARS = 6   # first N bars have explicit varied flow values
TOTAL_BARS = 82       # session window (09:30 -> 16:15 ET)
SESSION_OPEN_ET = datetime(PRIOR_DATE.year, PRIOR_DATE.month, PRIOR_DATE.day, 9, 30, tzinfo=_ET)
SESSION_OPEN_UTC = SESSION_OPEN_ET.astimezone(timezone.utc)

SYMBOL = "SPY"

# Underlying close per 5-min bar for the first NUM_ACTIVE_BARS bars.
# Bars beyond that carry forward the last value (no new underlying
# bars after the contracts go quiet -- mirrors a thin extended-hours
# stretch where the underlying price stops moving).
UNDERLYING_CLOSES_ACTIVE = [540.00, 540.20, 540.10, 540.30, 540.50, 540.40]
assert len(UNDERLYING_CLOSES_ACTIVE) == NUM_ACTIVE_BARS

# Contracts.  Each tuple is:
#   (option_type, strike, expiration, [raw_volume per active bar], [net_volume per active bar])
# raw_volume is day-to-date cumulative, monotonically non-decreasing.
# net_volume can move either direction (buys-minus-sells).  ``None``
# in the first few entries marks "contract hadn't traded yet"; the
# fixture then carries forward the final explicit value across the
# rest of the session, mirroring what the analytics writer produces
# (HAVING SUM(volume_delta) > 0 from session_open through bucket_end).
CONTRACTS: list[Tuple[str, float, date, list, list]] = [
    # SPY 540C, 0DTE, steadily-growing call-buying pressure
    ("C", 540.0, PRIOR_DATE,
     [100, 250, 400, 600, 800, 950],
     [+60, +120, +180, +240, +290, +330]),
    # SPY 545C, 0DTE, late starter (no flow in bar 0/1)
    ("C", 545.0, PRIOR_DATE,
     [None, None, 50, 120, 200, 290],
     [None, None, +25, +60, +100, +145]),
    # SPY 540P, 0DTE, steady put-selling (net negative)
    ("P", 540.0, PRIOR_DATE, [80, 200, 320, 460, 600, 720], [-30, -75, -120, -170, -220, -260]),
    # SPY 535P, 0DTE, balanced flow (net ~0)
    ("P", 535.0, PRIOR_DATE, [60, 130, 200, 280, 370, 450], [-5, +10, +0, -5, +5, -10]),
    # SPY 545C, 2DTE -- different expiry, exercises (strike, expiration)
    # primary-key disambiguation in the LAG partition
    ("C", 545.0, date(2025, 4, 17),
     [40, 90, 140, 200, 270, 340],
     [+20, +45, +70, +100, +135, +170]),
    # SPY 540C, 2DTE -- second contract on same strike, different expiry
    ("C", 540.0, date(2025, 4, 17),
     [30, 70, 130, 200, 280, 360],
     [+15, +35, +65, +100, +140, +180]),
]


def _extend_carry_forward(values: list) -> list:
    """Carry forward the last non-None value across the rest of the session.

    Returns a list of length TOTAL_BARS.  Entries before the first
    non-None remain None (the contract hadn't traded yet); entries
    after the last explicit value get a copy of that last value
    (cumulative stays flat once trading stops -- matches what the
    analytics writer's SUM-from-session-open produces in subsequent
    buckets).
    """
    out: list = list(values)
    last_value = None
    for v in values:
        if v is not None:
            last_value = v
    # Pad with the last value across the remaining bars.
    if last_value is not None:
        out.extend([last_value] * (TOTAL_BARS - len(values)))
    else:
        out.extend([None] * (TOTAL_BARS - len(values)))
    return out


# raw_premium / net_premium per bar.  Premium per contract scales with
# raw_volume × an arbitrary per-contract average price (mid).  The
# parity test cares that incremental and canonical reproduce the same
# value, not that the value is realistic, so any deterministic mapping
# works.
def _premium_for(option_type: str, raw_vol: int) -> float:
    # Per-contract avg fill price — call ITM is more expensive than OTM put.
    if option_type == "C":
        per_unit = 35.5
    else:
        per_unit = 22.5
    return round(raw_vol * per_unit, 2)


def _bar_timestamps_utc() -> list[datetime]:
    return [SESSION_OPEN_UTC + timedelta(minutes=5 * i) for i in range(TOTAL_BARS)]


def _underlying_close_for_bar(i: int) -> float:
    if i < NUM_ACTIVE_BARS:
        return UNDERLYING_CLOSES_ACTIVE[i]
    return UNDERLYING_CLOSES_ACTIVE[-1]


def _truncate(cur) -> None:
    # ``CASCADE`` because flow_by_contract / flow_series_5min FK to
    # symbols.  Order ensures the truncate doesn't fight the FK.
    cur.execute(
        "TRUNCATE flow_series_5min, flow_by_contract, flow_contract_facts, "
        "underlying_quotes, gex_summary, gex_by_strike, gex_profile, "
        "signal_scores, signal_component_scores, signal_action_cards, "
        "signal_events, signal_trades, signal_calibration, "
        "playbook_pattern_stats, portfolio_snapshots, "
        "max_pain_oi_snapshot, max_pain_oi_snapshot_expiration, "
        "option_chains, vix_bars, tradestation_api_calls, "
        "component_normalizer_cache, symbols RESTART IDENTITY CASCADE"
    )


def _seed_symbol(cur) -> None:
    cur.execute(
        "INSERT INTO symbols (symbol, name, asset_type, is_active) "
        "VALUES (%s, %s, %s, TRUE) "
        "ON CONFLICT (symbol) DO NOTHING",
        (SYMBOL, "SPDR S&P 500 ETF Trust", "ETF"),
    )


def _seed_underlying_quotes(cur) -> None:
    bar_ts = _bar_timestamps_utc()
    for i, ts in enumerate(bar_ts):
        close = _underlying_close_for_bar(i)
        cur.execute(
            "INSERT INTO underlying_quotes "
            "(symbol, timestamp, open, high, low, close, up_volume, down_volume) "
            "VALUES (%s, %s, %s, %s, %s, %s, 0, 0)",
            (SYMBOL, ts, close, close, close, close),
        )


def _seed_flow_by_contract(cur) -> int:
    """Insert the prior-session rows plus one marker row on CURRENT_DATE.

    Returns the count of inserted rows for sanity logging.
    """
    bar_ts = _bar_timestamps_utc()
    inserted = 0
    for opt_type, strike, expiration, raw_vols_active, net_vols_active in CONTRACTS:
        # Extend explicit values to cover the full session via carry-
        # forward of the last non-None value (mirrors the analytics
        # writer's SUM-from-session-open through subsequent buckets).
        raw_vols = _extend_carry_forward(raw_vols_active)
        net_vols = _extend_carry_forward(net_vols_active)
        for i, (raw_vol, net_vol) in enumerate(zip(raw_vols, net_vols)):
            if raw_vol is None:
                # Contract hadn't traded yet this session.  Sparse-
                # at-the-front: omit the row entirely (matches the
                # analytics writer's HAVING SUM(volume_delta) > 0
                # filter -- no flow yet -> no row).
                continue
            raw_prem = _premium_for(opt_type, raw_vol)
            # net_premium is sign-correlated with net_volume; scale by
            # the same per-unit price.
            net_prem = _premium_for(opt_type, abs(net_vol))
            net_prem = net_prem if net_vol >= 0 else -net_prem
            cur.execute(
                "INSERT INTO flow_by_contract "
                "(timestamp, symbol, option_type, strike, expiration, "
                " raw_volume, raw_premium, net_volume, net_premium, underlying_price) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    bar_ts[i],
                    SYMBOL,
                    opt_type,
                    strike,
                    expiration,
                    raw_vol,
                    raw_prem,
                    net_vol,
                    net_prem,
                    _underlying_close_for_bar(i),
                ),
            )
            inserted += 1

    # Marker row on CURRENT_DATE so _resolve_flow_series_session
    # resolves PRIOR_DATE as the 'prior' session.  Value irrelevant —
    # the parity test windows on the prior session.
    current_open_et = datetime(
        CURRENT_DATE.year, CURRENT_DATE.month, CURRENT_DATE.day, 9, 30, tzinfo=_ET
    )
    cur.execute(
        "INSERT INTO flow_by_contract "
        "(timestamp, symbol, option_type, strike, expiration, "
        " raw_volume, raw_premium, net_volume, net_premium, underlying_price) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            current_open_et.astimezone(timezone.utc),
            SYMBOL,
            "C",
            540.0,
            CURRENT_DATE,
            1,
            35.5,
            1,
            35.5,
            541.00,
        ),
    )
    inserted += 1
    return inserted


def _populate_flow_series_5min_incremental(cur) -> int:
    """Drive the snapshot writes across every bar of the prior session,
    mirroring the live analytics engine cycle:

      * Bar 0 (prev_bar == session_start): use the **canonical full**
        upsert (``SNAPSHOT_UPSERT_PSYCOPG2``) — same guard the live
        engine uses at ``main_engine.py:2502`` (``prev_bar > session_start``).
      * Bars 1..N-1: use the **incremental** upsert
        (``SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2``) — what the engine
        runs on every steady-state cycle.  These are the bars whose
        parity against the canonical CTE is the actual check.

    Returns the number of bars covered for sanity logging.
    """
    bar_ts = _bar_timestamps_utc()
    written = 0

    # Bar 0: canonical full backfill writes the first bar.  Matches the
    # live engine guard ``if prev_bar_known and prev_bar > session_start``
    # at main_engine.py:2502 -- on the very first cycle of a session
    # prev_bar isn't yet present in flow_series_5min so the engine
    # falls back to the full canonical upsert.  The bar-0 write needs
    # session_end == bar_ts[0] so the generate_series in the CTE
    # produces exactly one bar.
    cur.execute(
        SNAPSHOT_UPSERT_PSYCOPG2,
        {
            "symbol": SYMBOL,
            "session_start": SESSION_OPEN_UTC,
            "session_end": bar_ts[0],
            "strikes": None,
            "expirations": None,
        },
    )
    written += 1

    # Bars 1..TOTAL_BARS-1: incremental form, one (prev_bar, curr_bar)
    # call per bar.  Mirrors what the live engine does over the course
    # of a session -- each cycle's incremental call refreshes the
    # current and prior 5-min bucket; iterating across all bars
    # eventually writes the whole session via the incremental path.
    # The prev_bar slot in each call is a no-op because the prior
    # iteration already wrote that row and the IS DISTINCT FROM guard
    # skips the rewrite.
    for i in range(1, len(bar_ts)):
        prev_bar = bar_ts[i - 1]
        curr_bar = bar_ts[i]
        cur.execute(
            SNAPSHOT_INCREMENTAL_UPSERT_PSYCOPG2,
            {
                "symbol": SYMBOL,
                "prev_bar": prev_bar,
                "curr_bar": curr_bar,
            },
        )
        written += 1

    return written


def seed(dsn: str) -> dict:
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            _truncate(cur)
            _seed_symbol(cur)
            _seed_underlying_quotes(cur)
            n_fbc = _seed_flow_by_contract(cur)
            n_bars = _populate_flow_series_5min_incremental(cur)

            # Verify the snapshot has rows for sanity.
            cur.execute(
                "SELECT COUNT(*) FROM flow_series_5min WHERE symbol = %s",
                (SYMBOL,),
            )
            n_snap_rows = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()
    return {
        "prior_session_date": PRIOR_DATE.isoformat(),
        "flow_by_contract_rows": n_fbc,
        "incremental_bars_written": n_bars,
        "flow_series_5min_rows": n_snap_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument(
        "--dsn",
        default=os.environ.get("FLOW_SERIES_PARITY_DSN")
        or os.environ.get("DATABASE_URL"),
        help="psycopg2 DSN (default: $FLOW_SERIES_PARITY_DSN or $DATABASE_URL)",
    )
    args = parser.parse_args()
    if not args.dsn:
        parser.error("--dsn or $FLOW_SERIES_PARITY_DSN required")

    stats = seed(args.dsn)
    print("Seeded flow-series parity fixture:")
    for k, v in stats.items():
        print(f"  {k} = {v}")

    if stats["flow_series_5min_rows"] == 0:
        print(
            "ERROR: flow_series_5min has zero rows after the incremental "
            "writes — the fixture failed to produce snapshot output.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
