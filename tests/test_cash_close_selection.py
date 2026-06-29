"""Regression tests for the asset-type-aware 16:00 ET cash-close rule.

The headline price on the header during AH / pre-market / closed / weekend
states comes from ``/api/market/session-closes.current_session_close``,
not the live tick on ``/api/market/quote``. The prior-day anchor used for
% change calculations comes from ``/api/market/session-closes
.prior_session_close`` and ``get_previous_close``.

TradeStation start-of-minute-stamps its 1-minute bars, so the bar
timestamped 16:00:00 spans the full 60 seconds 16:00:00–16:00:59 ET — a
window that begins with the closing auction print and ends with the first
~60s of after-hours trading. Pre-fix, the cash-close queries returned the
bar's ``close`` field, which captured the last AH tick in that minute and
drifted the displayed headline price away from the official close.

Verified against Friday 2026-06-26 vs published official cash closes:

                 15:59 close   16:00 open   16:00 close   official
  SPY (ETF)        731.66       729.01       731.54        728.99
  QQQ (ETF)        706.63       705.88       707.75        706.52
  SPX (INDEX)     7345.16      7335.70      7353.01        7354.03

For non-INDEX symbols the 16:00 bar's ``open`` (the closing auction
print) matches the official close. For INDEX symbols the 16:00 ``close``
(the post-auction settled level) matches. These tests pin the
asset-aware CASE substitution in ``get_session_closes`` (the canonical
owner) and ``get_previous_close`` (which references it).

The live tick endpoint ``get_latest_quote`` deliberately does NOT apply
this rule — the header's extended-hours row, the GEX live spot, and
chart tip-close merge all need real-time prints, not the cash-close
anchor.
"""

import asyncio
from contextlib import asynccontextmanager

import pytest

from src.api.database import DatabaseManager


class _RecordingConn:
    """Mock asyncpg connection that records queries and returns canned rows."""

    def __init__(self, fetchrow_result=None):
        self._fetchrow_result = fetchrow_result
        self.queries = []
        self.args = []

    async def fetchrow(self, query, *args):
        self.queries.append(query)
        self.args.append(args)
        return self._fetchrow_result


def _install_conn(db, conn):
    @asynccontextmanager
    async def _acquire():
        yield conn

    db._acquire_connection = _acquire  # type: ignore[method-assign]


def _new_db():
    db = DatabaseManager()
    # Force-disable the cache between cases so each test exercises the SQL.
    db._latest_quote_cache_ttl_seconds = 0.0
    return db


def _captured_sql(db, fn_name):
    conn = _RecordingConn(fetchrow_result=None)
    _install_conn(db, conn)
    asyncio.run(getattr(db, fn_name)("SPY"))
    return conn.queries[0]


# ---------------------------------------------------------------------------
# get_session_closes — canonical owner of the asset-aware cash-close rule
# ---------------------------------------------------------------------------


def test_session_closes_uses_asset_aware_close_for_1600_bar():
    """``current_session_close`` and ``prior_session_close`` drive the
    header price in the AH / closed / weekend states. The query must
    return the 16:00 bar's ``open`` for non-INDEX symbols (auction
    print) and ``close`` for INDEX symbols (settled level). Without
    this, the header keeps showing AH-contaminated prices."""
    sql = _captured_sql(_new_db(), "get_session_closes")

    # Inside the session_closes CTE the SELECT uses the asset-aware CASE.
    assert "CASE" in sql
    assert "TIME '16:00'" in sql
    assert "IS DISTINCT FROM 'INDEX'" in sql
    assert "THEN uq.open" in sql
    assert "ELSE uq.close" in sql

    # The symbols table must be joined so asset_type is in scope of the CASE.
    assert "LEFT JOIN symbols s" in sql


def test_session_closes_predicate_is_exact_1600_match():
    """The OPEN-substitution must bind on bar timestamp == 16:00 ET
    specifically — not a broader window. A 15:59 bar (or any earlier
    bar) returns ``close`` as usual, so the live in-session tick path
    is unaffected and a half-day's 13:00 early close returns its
    ``close`` correctly."""
    sql = _captured_sql(_new_db(), "get_session_closes")

    assert "= TIME '16:00'" in sql


def test_session_closes_filters_to_cash_session_weekdays():
    """Candidate bars must be cash-session weekday only — the SELECT
    DISTINCT ON picks the latest matching bar per date and the outer
    LIMIT 2 yields current + prior. Without these guards an AH bar or
    a stray weekend bar could win the per-date pick."""
    sql = _captured_sql(_new_db(), "get_session_closes")

    assert "BETWEEN '09:30' AND '16:00'" in sql
    assert "BETWEEN 1 AND 5" in sql  # weekday clamp


# ---------------------------------------------------------------------------
# get_previous_close — same rule, applied to the prior-day anchor
# ---------------------------------------------------------------------------


def test_previous_close_uses_asset_aware_close():
    """``get_previous_close`` returns the prior-day anchor used by %
    change calculations. Must follow the same rule as
    ``get_session_closes`` so the two endpoints can't drift apart."""
    sql = _captured_sql(_new_db(), "get_previous_close")

    # CASE appears in both the primary 16:00-exact CTE and the nearest-close
    # fallback CTE (which can match either the 16:00 bar or a 15:xx bar).
    assert sql.count("CASE") >= 2
    assert "IS DISTINCT FROM 'INDEX'" in sql
    assert "LEFT JOIN symbols s" in sql


def test_previous_close_nearest_ctE_substitutes_only_for_1600_bar():
    """The fallback CTE may match any bar between 15:00 and 16:00 ET.
    OPEN substitution must apply only to the exact 16:00 bar; for 15:xx
    bars the ``close`` is the normal last-tick-of-minute, not an
    auction print."""
    sql = _captured_sql(_new_db(), "get_previous_close")

    # The nearest_close CTE adds the explicit time predicate inside its
    # CASE. The primary CTE doesn't need it (its WHERE already pins
    # HOUR=16 AND MINUTE=0).
    assert "= TIME '16:00'" in sql


# ---------------------------------------------------------------------------
# get_latest_quote — the live tick path; deliberately unaware of the rule
# ---------------------------------------------------------------------------


def test_latest_quote_does_not_apply_cash_close_rule():
    """``/api/market/quote`` returns the live tick (any session). It
    must NOT apply the cash-close OPEN substitution — doing so freezes
    the displayed live AH/pre-market price at the cash close and breaks
    the header's extended-hours ticker, the GEX live spot, and every
    chart's tip-close merge. The user-visible symptom was 'pre-market
    price not updating, stuck at cash close'.

    Pinning the absence here so a future change that 'unifies' the rule
    across both endpoints fails loudly with this test."""
    sql = _captured_sql(_new_db(), "get_latest_quote")

    # No 16:00-time predicate, no OPEN-for-non-INDEX substitution, no
    # session window filter — just the latest bar by timestamp.
    assert "TIME '16:00'" not in sql
    assert "IS DISTINCT FROM 'INDEX'" not in sql
    assert "BETWEEN '09:30' AND '16:00'" not in sql
    # Verify it's still selecting the live ``close`` as the price field.
    assert "lq.close" in sql
