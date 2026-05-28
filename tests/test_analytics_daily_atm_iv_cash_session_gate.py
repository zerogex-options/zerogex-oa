"""Cash-session gate for the daily_atm_iv writer.

``AnalyticsEngine._store_daily_atm_iv`` UPSERTs today's ATM call IV
into ``daily_atm_iv`` on every analytics cycle.  That table powers the
signals engine's iv_rank percentile — its "current" value must reflect
the live cash-session IV, not post-market drift.

The risk: SPX cash-settled options stop trading at 16:15 ET.  After
that the option_chains snapshot reflects wide quoted spreads and stale
IVs — the IV solver continues to compute values but they drift from
the true settlement.  Without the gate, an analytics cycle at 18:05 ET
would overwrite a clean 16:00 ET anchor with a post-market value
(observed in prod: SPX 0.1408 at 16:00 -> 0.1189 at 18:05).

This test pins the gate at 09:30-16:15 ET inclusive.  Outside that
window the function must return silently (no UPSERT), preserving
whatever value was last written during liquid hours.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytz

from src.analytics.main_engine import AnalyticsEngine


# Mid-session ATM call snapshot: enough rows for the function's
# ``atm_ivs`` aggregation to produce a non-zero result.  We don't care
# about the specific IV value here — only whether the UPSERT runs.
def _options() -> list[dict]:
    return [
        {"option_type": "C", "strike": 600.0, "implied_volatility": 0.15},
        {"option_type": "C", "strike": 601.0, "implied_volatility": 0.14},
        {"option_type": "C", "strike": 602.0, "implied_volatility": 0.16},
    ]


def _summary(ts_utc: datetime) -> dict:
    return {
        "underlying": "SPY",
        "timestamp": ts_utc,
        "underlying_price": 600.0,
    }


def _et(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    """Construct a UTC timestamp that lands at the given ET wall-clock."""
    et = pytz.timezone("America/New_York")
    return et.localize(datetime(year, month, day, hour, minute)).astimezone(timezone.utc)


def _engine() -> AnalyticsEngine:
    return AnalyticsEngine(underlying="SPY")


def test_upserts_during_cash_session():
    """09:30 ET through 16:15 ET inclusive should write."""
    cur = MagicMock()
    engine = _engine()
    for hour, minute in [(9, 30), (12, 0), (15, 59), (16, 15)]:
        cur.reset_mock()
        engine._store_daily_atm_iv(
            _options(),
            _summary(_et(2026, 5, 27, hour, minute)),
            cur,
        )
        assert cur.execute.called, (
            f"expected UPSERT at {hour:02d}:{minute:02d} ET (within cash session)"
        )


def test_skips_before_open():
    """Pre-09:30 ET (e.g. pre-market, overnight) must NOT write."""
    cur = MagicMock()
    engine = _engine()
    for hour, minute in [(4, 0), (8, 0), (9, 29)]:
        cur.reset_mock()
        engine._store_daily_atm_iv(
            _options(),
            _summary(_et(2026, 5, 27, hour, minute)),
            cur,
        )
        assert not cur.execute.called, (
            f"expected NO UPSERT at {hour:02d}:{minute:02d} ET (pre-open)"
        )


def test_skips_after_close():
    """Post-16:15 ET (post-market drift window) must NOT write.

    This is the bug that motivated the gate: at 18:05 ET the analytics
    cycle was overwriting today's row with a stale, wide-spread IV.
    """
    cur = MagicMock()
    engine = _engine()
    for hour, minute in [(16, 16), (18, 5), (20, 0), (23, 59)]:
        cur.reset_mock()
        engine._store_daily_atm_iv(
            _options(),
            _summary(_et(2026, 5, 27, hour, minute)),
            cur,
        )
        assert not cur.execute.called, (
            f"expected NO UPSERT at {hour:02d}:{minute:02d} ET (post-close)"
        )


def test_handles_naive_timestamp_as_utc():
    """If an upstream caller passes a naive datetime, the function
    should treat it as UTC rather than crash on the tz comparison.

    The DB schema stores TIMESTAMPTZ so this should never happen in
    practice, but the fallback keeps the writer robust against
    misconfiguration.
    """
    cur = MagicMock()
    engine = _engine()
    # 2026-05-27 14:00 UTC = 10:00 ET (within cash session)
    naive_ts = datetime(2026, 5, 27, 14, 0)
    engine._store_daily_atm_iv(_options(), _summary(naive_ts), cur)
    assert cur.execute.called
