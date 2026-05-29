"""Tests for ``AnalyticsEngine._get_snapshot`` last-quote semantics.

The analytics snapshot must reflect each option contract's most recent
quote regardless of how old that quote is, only rolling a contract off
once it has cleared its 16:15 ET expiry-day settlement.  These tests pin
down that behavior so we don't reintroduce the time-based staleness
filter that previously emptied the chain after hours.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytz

from src.analytics import main_engine
from src.analytics.main_engine import AnalyticsEngine

ET = pytz.timezone("US/Eastern")


def _snapshot_query_call(cursor):
    """Return the cursor.execute call_args for the per-contract snapshot query.

    Located by SQL content ("DISTINCT ON") rather than a fixed positional
    index, so the cold-start path's extra ``SET LOCAL statement_timeout``
    execute doesn't shift the assertion onto the wrong call.
    """
    for call in cursor.execute.call_args_list:
        sql = call[0][0]
        if "DISTINCT ON" in sql:
            return call
    raise AssertionError("snapshot query (DISTINCT ON ...) was never executed")


def _row(option_symbol, strike, expiration, option_type, quote_ts, *, gamma=0.01, oi=100):
    """Construct a 15-tuple matching the SELECT column order in ``_get_snapshot``."""
    return (
        option_symbol,
        strike,
        expiration,
        option_type,
        1.0,  # last
        0.99,  # bid
        1.01,  # ask
        10,  # volume
        oi,  # open_interest
        0.5,  # delta
        gamma,
        -0.05,  # theta
        0.1,  # vega
        0.2,  # implied_volatility
        quote_ts,
    )


def _mock_db_connection(latest_ts, underlying_price, option_rows):
    """Return a context-manager-compatible mock that scripts the three
    sequential queries ``_get_snapshot`` issues."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (latest_ts,),  # query 1: latest option_chains timestamp
        (underlying_price, latest_ts),  # query 2: underlying close + ts
    ]
    cursor.fetchall.return_value = option_rows  # query 3: option rows

    conn = MagicMock()
    conn.cursor.return_value = cursor

    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, cursor


def test_old_quote_for_contract_is_still_returned():
    """A contract whose only quote is 30 minutes old must remain in the snapshot."""
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 18, 0)).astimezone(timezone.utc)
    old_quote_ts = snapshot_ts - timedelta(minutes=30)
    expiration = (snapshot_ts.astimezone(ET).date()) + timedelta(days=7)

    rows = [_row("SPY260520C00500000", 500.0, expiration, "C", old_quote_ts)]
    cm, _ = _mock_db_connection(snapshot_ts, 500.0, rows)

    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()

    assert result is not None
    assert len(result["options"]) == 1
    assert result["options"][0]["option_symbol"] == "SPY260520C00500000"


def test_expiration_filter_excludes_yesterday_expirations_when_before_1615():
    engine = AnalyticsEngine(underlying="SPY")
    # 14:00 ET (before 16:15) — yesterday's contracts should already be rolled off.
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 14, 0)).astimezone(timezone.utc)
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, [])

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    # The third execute() carries the min_expiration arg.
    options_call = _snapshot_query_call(cursor)
    params = options_call[0][1]
    min_expiration = params[3]
    # Before 16:15 ET: min_expiration = yesterday => today's expirations still pass.
    assert min_expiration == ET.localize(datetime(2026, 5, 12)).date()


def test_expiration_filter_rolls_off_today_after_1615():
    engine = AnalyticsEngine(underlying="SPY")
    # 16:30 ET (after 16:15) — today's expirations should be rolled off.
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 16, 30)).astimezone(timezone.utc)
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, [])

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    options_call = _snapshot_query_call(cursor)
    params = options_call[0][1]
    min_expiration = params[3]
    # After 16:15 ET: min_expiration = today => today's expirations are excluded.
    assert min_expiration == ET.localize(datetime(2026, 5, 13)).date()


def test_lookback_uses_configured_hours_not_minutes():
    engine = AnalyticsEngine(underlying="SPY")
    engine.snapshot_lookback_hours = 48
    # Skip the one-shot cold-start window so this test exercises steady-state.
    engine._snapshot_cold_start_consumed = True
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 10, 0)).astimezone(timezone.utc)
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, [])

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    options_call = _snapshot_query_call(cursor)
    params = options_call[0][1]
    lookback_start = params[2]
    expected = snapshot_ts - timedelta(hours=48)
    assert lookback_start == expected


def test_snapshot_query_includes_expiration_clause():
    """The expiration roll-off must be enforced in SQL, not Python — otherwise
    expired contracts could leak in via the per-contract DISTINCT ON."""
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 12, 0)).astimezone(timezone.utc)
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, [])

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    options_sql = _snapshot_query_call(cursor)[0][0]
    assert "oc.expiration >" in options_sql


def test_engine_no_longer_carries_freshness_attribute():
    """Guard against reintroducing the seconds-based staleness filter."""
    engine = AnalyticsEngine(underlying="SPY")
    assert not hasattr(engine, "snapshot_freshness_seconds")
    assert not hasattr(engine, "snapshot_lookback_minutes")


def test_default_cold_start_lookback_hours_covers_long_weekend():
    """The first-cycle cold-start window must reach across a 3-day weekend
    (Memorial Day, etc.) so the very first snapshot of the week still finds
    every contract's prior close.  Steady-state cycles use a narrow window."""
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.snapshot_cold_start_lookback_hours >= 72


def test_lookback_hours_env_var_overrides_default(monkeypatch):
    monkeypatch.setenv("ANALYTICS_SNAPSHOT_LOOKBACK_HOURS", "24")
    engine = AnalyticsEngine(underlying="SPY")
    assert engine.snapshot_lookback_hours == 24


def test_expiration_boundary_at_exactly_1615_rolls_off():
    """At precisely 16:15 ET, today's expirations should be considered rolled off."""
    engine = AnalyticsEngine(underlying="SPY")
    snapshot_ts = ET.localize(datetime(2026, 5, 13, 16, 15)).astimezone(timezone.utc)
    cm, cursor = _mock_db_connection(snapshot_ts, 500.0, [])

    with patch.object(main_engine, "db_connection", return_value=cm):
        engine._get_snapshot()

    options_call = _snapshot_query_call(cursor)
    min_expiration = options_call[0][1][3]
    # 16:15 is the boundary; per the rule (ts.time() < 16:15) is False, so roll off today.
    assert min_expiration == ET.localize(datetime(2026, 5, 13)).date()


# ---------------------------------------------------------------------------
# Session-aware underlying staleness gate
# ---------------------------------------------------------------------------
#
# These tests pin down the freeze semantic introduced alongside the
# underlying-staleness check: in-session a 4h gap is a hard fault and
# refuses the cycle; off-session the same gap is structural and the gate
# steps aside so the analytics engine can re-anchor to the frozen
# end-of-session option_chains snapshot.


def _mock_db_connection_with_stale_underlying(
    option_chain_ts, underlying_ts, underlying_price, option_rows, forward_row=None
):
    """Variant that scripts a deliberate gap between the option-chain
    anchor and the underlying-quote timestamp.

    ``forward_row`` scripts the optional close-stamp-skew re-anchor lookup
    (``SELECT close, timestamp ... timestamp > option_ts``) that
    ``_get_snapshot`` issues when the paired bar is more than one bucket
    old in-session. ``None`` (the default) means "no forward bar found",
    so a genuinely stale underlying still trips the gate; off-session
    cases never reach this query and leave the entry unconsumed."""
    cursor = MagicMock()
    cursor.fetchone.side_effect = [
        (option_chain_ts,),
        (underlying_price, underlying_ts),
        forward_row,
    ]
    cursor.fetchall.return_value = option_rows
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__.return_value = conn
    cm.__exit__.return_value = False
    return cm, cursor


def test_in_session_stale_underlying_refuses_cycle():
    """Wednesday noon: cash session, 1h underlying lag is a real fault."""
    engine = AnalyticsEngine(underlying="SPY")
    in_session = ET.localize(datetime(2026, 5, 13, 12, 0)).astimezone(timezone.utc)
    underlying_ts = in_session - timedelta(hours=1)  # 3,600s — over 900s default
    cm, _ = _mock_db_connection_with_stale_underlying(in_session, underlying_ts, 500.0, [])
    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()
    assert result is None  # refused


def test_in_session_fresh_underlying_proceeds():
    engine = AnalyticsEngine(underlying="SPY")
    in_session = ET.localize(datetime(2026, 5, 13, 12, 0)).astimezone(timezone.utc)
    underlying_ts = in_session - timedelta(seconds=30)  # well under 900s
    expiration = in_session.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260520C00500000", 500.0, expiration, "C", in_session)]
    cm, _ = _mock_db_connection_with_stale_underlying(in_session, underlying_ts, 500.0, rows)
    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()
    assert result is not None
    assert result["underlying_price"] == 500.0


def test_in_session_open_close_stamp_skew_reanchors_to_fresh_bar():
    """Cash open: the ``<= option_ts`` lookup returns yesterday's 16:00
    close because the day's first underlying bar is close-stamped one
    bucket AHEAD of the option bucket and isn't visible to that lookup
    yet. A fresh bar exists just past option_ts, so the snapshot must
    re-anchor onto it and proceed rather than refuse on a bogus ~17h gap.
    """
    engine = AnalyticsEngine(underlying="SPX")
    open_ts = ET.localize(datetime(2026, 5, 29, 9, 30)).astimezone(timezone.utc)
    # `<= option_ts` falls back across the session boundary to 16:00 close.
    prior_close_ts = ET.localize(datetime(2026, 5, 28, 16, 0)).astimezone(timezone.utc)
    # Day's first bar, close-stamped one minute ahead of the open bucket.
    fresh_ts = open_ts + timedelta(seconds=60)
    expiration = open_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPXW260605C05000000", 5000.0, expiration, "C", open_ts)]
    cm, cursor = _mock_db_connection_with_stale_underlying(
        open_ts, prior_close_ts, 4990.0, rows, forward_row=(5000.0, fresh_ts)
    )
    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()
    assert result is not None  # re-anchored, not refused
    assert result["underlying_price"] == 5000.0  # the fresh bar, not the 16:00 close

    # The forward lookup is bounded to ~2 buckets past option_ts so a
    # genuinely frozen feed can't be rescued by a far-future straggler.
    fwd_call = next(c for c in cursor.execute.call_args_list if "ORDER BY timestamp ASC" in c[0][0])
    lower, upper = fwd_call[0][1][1], fwd_call[0][1][2]
    assert lower == open_ts
    assert upper == open_ts + timedelta(seconds=120)


def test_in_session_stale_underlying_with_no_forward_bar_still_refuses():
    """The re-anchor must not mask a real outage: when the paired bar is
    stale in-session AND no fresh bar exists past option_ts, the cycle is
    still refused (forward_row defaults to None => lookup finds nothing)."""
    engine = AnalyticsEngine(underlying="SPX")
    in_session = ET.localize(datetime(2026, 5, 29, 12, 0)).astimezone(timezone.utc)
    underlying_ts = in_session - timedelta(hours=2)  # 7,200s, far over 900s
    cm, _ = _mock_db_connection_with_stale_underlying(in_session, underlying_ts, 5000.0, [])
    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()
    assert result is None  # genuine staleness still trips the gate


def test_offhours_stale_underlying_proceeds_to_freeze_last_snapshot():
    """Friday 21:00 ET (past SPY's 20:00 freeze): off-session, gate skipped.

    Outside the extended-hours window the gap between the latest option
    chain timestamp and the cash-close underlying is structural, not a
    fault. The gate must step aside so the engine can refresh
    ``gex_summary`` against the frozen end-of-session underlying — that
    snapshot is what API consumers see all weekend.
    """
    engine = AnalyticsEngine(underlying="SPY")
    # 21:00 ET on a Friday — past SPY's 20:00 extended-hours freeze.
    option_chain_ts = ET.localize(datetime(2026, 5, 15, 21, 0)).astimezone(timezone.utc)
    # Underlying frozen at the 16:00 cash close = 5h gap, well past 900s.
    underlying_ts = ET.localize(datetime(2026, 5, 15, 16, 0)).astimezone(timezone.utc)
    expiration = option_chain_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPY260522C00500000", 500.0, expiration, "C", option_chain_ts)]
    cm, _ = _mock_db_connection_with_stale_underlying(option_chain_ts, underlying_ts, 500.0, rows)
    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()
    assert result is not None  # NOT refused — freeze path is permitted
    assert result["underlying_price"] == 500.0


def test_etf_extended_hours_still_in_session_gate_active():
    """SPY at 19:00 ET is INSIDE the extended-hours window (closes at 20:00).

    A 3-hour underlying lag during extended hours IS a real fault for
    SPY (the SPY feed should still be printing), so the gate fires.
    """
    engine = AnalyticsEngine(underlying="SPY")
    option_chain_ts = ET.localize(datetime(2026, 5, 15, 19, 0)).astimezone(timezone.utc)
    underlying_ts = ET.localize(datetime(2026, 5, 15, 16, 0)).astimezone(timezone.utc)
    cm, _ = _mock_db_connection_with_stale_underlying(option_chain_ts, underlying_ts, 500.0, [])
    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()
    assert result is None  # gate fires — SPY's extended feed should be live


def test_cash_index_offhours_skips_gate_even_within_extended_window():
    """SPX freezes at 16:00 ET — 17:00 ET is already off-session for the
    index even though it would still be in-session for SPY."""
    engine = AnalyticsEngine(underlying="SPX")
    # 17:00 ET — inside SPY's extended-hours window but past SPX's close.
    option_chain_ts = ET.localize(datetime(2026, 5, 15, 17, 0)).astimezone(timezone.utc)
    underlying_ts = ET.localize(datetime(2026, 5, 15, 16, 0)).astimezone(timezone.utc)
    expiration = option_chain_ts.astimezone(ET).date() + timedelta(days=7)
    rows = [_row("SPXW260522C05000000", 5000.0, expiration, "C", option_chain_ts)]
    cm, _ = _mock_db_connection_with_stale_underlying(option_chain_ts, underlying_ts, 5000.0, rows)
    with patch.object(main_engine, "db_connection", return_value=cm):
        result = engine._get_snapshot()
    assert result is not None  # gate stepped aside for the cash-index off-session
