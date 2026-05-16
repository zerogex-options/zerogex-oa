"""Tests for the Playbook backtest harness (PR-14).

Covers ``compute_outcome`` (target/stop/time/no_data branches), the
aggregator (per-pattern stats with smoothing prior), and the
end-to-end ``run`` path against a stub psycopg2-style connection.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

from src.signals.playbook.backtest import (
    CardOutcome,
    CardRow,
    PatternStats,
    aggregate,
    compute_outcome,
    fetch_action_cards,
    fetch_quotes,
    run,
    upsert_pattern_stats,
)

# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _card(
    *,
    pattern: str = "call_wall_fade",
    direction: str = "bearish",
    confidence: float = 0.65,
    entry: float = 678.0,
    target: float = 675.0,
    stop: float = 680.0,
    max_hold_minutes: int = 90,
    target_kind: str = "level",
    stop_kind: str = "level",
    timestamp: Optional[datetime] = None,
) -> CardRow:
    ts = timestamp or datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc)
    payload: dict[str, Any] = {
        "underlying": "SPY",
        "timestamp": ts.isoformat(),
        "action": "SELL_CALL_SPREAD" if direction == "bearish" else "BUY_CALL_SPREAD",
        "pattern": pattern,
        "tier": "0DTE",
        "direction": direction,
        "confidence": confidence,
        "max_hold_minutes": max_hold_minutes,
        "entry": {"ref_price": entry, "trigger": "at_market"},
        "target": {"ref_price": target, "kind": target_kind, "level_name": "max_pain"},
        "stop": {"ref_price": stop, "kind": stop_kind, "level_name": "wall_break"},
    }
    return CardRow(
        underlying="SPY",
        timestamp=ts,
        pattern=pattern,
        action=payload["action"],
        tier="0DTE",
        direction=direction,
        confidence=confidence,
        payload=payload,
    )


def _quotes(
    base_ts: datetime, prices: list[float], step_minutes: int = 1
) -> list[tuple[datetime, float]]:
    return [(base_ts + timedelta(minutes=i * step_minutes), p) for i, p in enumerate(prices)]


# ----------------------------------------------------------------------
# compute_outcome
# ----------------------------------------------------------------------


def test_target_hit_in_bearish_card():
    """Bearish card: price drops to target before reaching stop."""
    card = _card(direction="bearish", entry=678.0, target=675.0, stop=680.0)
    quotes = _quotes(
        card.timestamp + timedelta(minutes=1),
        [678.0, 677.0, 676.5, 675.0, 674.0],
    )
    outcome = compute_outcome(card, quotes)
    assert outcome.outcome == "target_hit"
    assert outcome.target_hit_at is not None
    assert outcome.mfe_pct > 0


def test_stop_hit_in_bearish_card():
    """Bearish card: price moves up to stop first."""
    card = _card(direction="bearish", entry=678.0, target=675.0, stop=680.0)
    quotes = _quotes(
        card.timestamp + timedelta(minutes=1),
        [678.5, 679.0, 680.0, 681.0],
    )
    outcome = compute_outcome(card, quotes)
    assert outcome.outcome == "stop_hit"
    assert outcome.stop_hit_at is not None


def test_target_hit_in_bullish_card():
    card = _card(direction="bullish", entry=678.0, target=681.0, stop=676.5)
    quotes = _quotes(
        card.timestamp + timedelta(minutes=1),
        [678.5, 679.5, 680.0, 681.0],
    )
    outcome = compute_outcome(card, quotes)
    assert outcome.outcome == "target_hit"


def test_time_exit_when_neither_level_reached():
    card = _card(direction="bearish", entry=678.0, target=670.0, stop=685.0)
    quotes = _quotes(
        card.timestamp + timedelta(minutes=1),
        [678.0, 678.1, 677.9, 678.0, 678.2],
    )
    outcome = compute_outcome(card, quotes)
    assert outcome.outcome == "time_exit"
    # MFE / MAE captured even on time exit.
    assert -1.0 < outcome.mae_pct <= 0
    assert outcome.mfe_pct >= 0


def test_no_data_when_no_quotes():
    card = _card()
    outcome = compute_outcome(card, quotes=[])
    assert outcome.outcome == "no_data"


def test_quotes_outside_hold_window_are_ignored():
    """Quotes after deadline must not register a target hit."""
    card = _card(direction="bearish", entry=678.0, target=675.0, stop=680.0, max_hold_minutes=5)
    # First five minutes range bound; minute 7 hits target — should be skipped.
    quotes = [
        (card.timestamp + timedelta(minutes=1), 678.1),
        (card.timestamp + timedelta(minutes=2), 678.0),
        (card.timestamp + timedelta(minutes=3), 678.2),
        (card.timestamp + timedelta(minutes=4), 678.0),
        (card.timestamp + timedelta(minutes=7), 675.0),  # past deadline
    ]
    outcome = compute_outcome(card, quotes)
    assert outcome.outcome == "time_exit"


def test_non_level_target_falls_through_to_time_exit():
    card = _card(target_kind="signal_event", stop_kind="signal_event")
    quotes = _quotes(card.timestamp + timedelta(minutes=1), [678.0, 678.1, 678.2, 678.0])
    outcome = compute_outcome(card, quotes)
    assert outcome.outcome == "time_exit"
    assert "non-level" in outcome.note


def test_non_directional_card_returns_no_data():
    card = _card(direction="non_directional")
    quotes = _quotes(card.timestamp + timedelta(minutes=1), [678.0])
    outcome = compute_outcome(card, quotes)
    assert outcome.outcome == "no_data"


def test_single_close_at_target_resolves_target_hit():
    """Legacy 2-tuple close exactly at target (stop not touched at that
    scalar price) still resolves target_hit — back-compat preserved."""
    card = _card(direction="bearish", entry=678.0, target=675.0, stop=680.0)
    quotes = [(card.timestamp + timedelta(minutes=1), 675.0)]
    outcome = compute_outcome(card, quotes)
    assert outcome.outcome == "target_hit"


def test_intrabar_target_touch_counts_even_if_bar_closes_inside():
    """The bug: close-only resolution missed a target the bar traded
    through but closed back above. Bearish target 675; bar wicks to
    674.5 (low) but closes 677 — must be target_hit, not time_exit."""
    card = _card(direction="bearish", entry=678.0, target=675.0, stop=685.0, max_hold_minutes=5)
    quotes = [
        # ts, o, h, l, c — low pierces 675, close back at 677.
        (card.timestamp + timedelta(minutes=1), 678.0, 678.2, 674.5, 677.0),
    ]
    outcome = compute_outcome(card, quotes)
    assert outcome.outcome == "target_hit"
    assert outcome.target_hit_at is not None


def test_intrabar_stop_wick_counts_and_mae_uses_low():
    """Close-only missed a stop the bar wicked through, and understated
    MAE. Bullish stop 676; bar wicks to 675 then closes 678."""
    card = _card(direction="bullish", entry=678.0, target=690.0, stop=676.0, max_hold_minutes=5)
    quotes = [
        (card.timestamp + timedelta(minutes=1), 678.0, 678.5, 675.0, 678.0),
    ]
    outcome = compute_outcome(card, quotes)
    assert outcome.outcome == "stop_hit"
    # MAE reflects the true intrabar low (675), not the 678 close → ~ -0.44%.
    assert outcome.mae_pct < -0.004


def test_same_bar_both_touch_resolves_conservatively_to_stop():
    """When one bar's range spans BOTH target and stop, intrabar order
    is unknowable — must resolve to stop_hit (never inflate edge)."""
    card = _card(direction="bearish", entry=678.0, target=675.0, stop=680.0)
    quotes = [
        # high 681 (>= stop 680) AND low 674 (<= target 675) in one bar.
        (card.timestamp + timedelta(minutes=1), 678.0, 681.0, 674.0, 678.0),
    ]
    outcome = compute_outcome(card, quotes)
    assert outcome.outcome == "stop_hit"
    assert outcome.stop_hit_at is not None


# ----------------------------------------------------------------------
# Aggregation
# ----------------------------------------------------------------------


def test_aggregate_groups_by_pattern_and_computes_rates():
    base_ts = datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc)
    outcomes = [
        CardOutcome(
            card=_card(pattern="cwf", confidence=0.6, timestamp=base_ts),
            outcome="target_hit",
            mfe_pct=0.005,
            mae_pct=-0.001,
        ),
        CardOutcome(
            card=_card(pattern="cwf", confidence=0.7, timestamp=base_ts + timedelta(hours=1)),
            outcome="stop_hit",
            mfe_pct=0.001,
            mae_pct=-0.004,
        ),
        CardOutcome(
            card=_card(pattern="cwf", confidence=0.5, timestamp=base_ts + timedelta(hours=2)),
            outcome="time_exit",
            mfe_pct=0.002,
            mae_pct=-0.001,
        ),
        CardOutcome(
            card=_card(pattern="pwb", confidence=0.55, timestamp=base_ts),
            outcome="target_hit",
            mfe_pct=0.004,
            mae_pct=-0.001,
        ),
    ]
    stats = aggregate(
        outcomes,
        underlying="SPY",
        window_start=date(2026, 4, 1),
        window_end=date(2026, 5, 1),
    )
    cwf = next(s for s in stats if s.pattern == "cwf")
    pwb = next(s for s in stats if s.pattern == "pwb")
    assert cwf.n_emitted == 3
    assert cwf.n_resolved == 3
    assert cwf.n_target_hit == 1
    assert cwf.n_stop_hit == 1
    assert cwf.n_time_exit == 1
    assert abs(cwf.hit_rate - (1 / 3)) < 1e-6
    # Avg confidence = (0.6 + 0.7 + 0.5) / 3 = 0.6.
    assert abs((cwf.avg_confidence or 0) - 0.6) < 1e-6
    assert pwb.n_emitted == 1
    assert pwb.n_target_hit == 1
    assert pwb.hit_rate == 1.0


def test_proposed_base_smoothing_pulls_toward_50pct():
    """Single resolved trade shouldn't produce 100% empirical base."""
    base_ts = datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc)
    outcome = CardOutcome(
        card=_card(pattern="solo", timestamp=base_ts),
        outcome="target_hit",
    )
    stats = aggregate(
        [outcome], underlying="SPY", window_start=date(2026, 4, 1), window_end=date(2026, 5, 1)
    )
    proposed = stats[0].proposed_base
    # (1 + alpha=5) / (1 + alpha + beta) = 6/11 ≈ 0.545.
    assert proposed is not None
    assert 0.45 < proposed < 0.65


def test_aggregate_no_data_outcomes_count_in_emitted_only():
    base_ts = datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc)
    outcomes = [
        CardOutcome(
            card=_card(pattern="x", timestamp=base_ts),
            outcome="no_data",
        ),
        CardOutcome(
            card=_card(pattern="x", timestamp=base_ts),
            outcome="target_hit",
        ),
    ]
    stats = aggregate(
        outcomes, underlying="SPY", window_start=date(2026, 4, 1), window_end=date(2026, 5, 1)
    )
    assert stats[0].n_emitted == 2
    assert stats[0].n_resolved == 1
    assert stats[0].n_target_hit == 1


# ----------------------------------------------------------------------
# DB I/O — stub conn
# ----------------------------------------------------------------------


class _StubCursor:
    def __init__(self, fetch_returns: list[Any]):
        self._fetch_returns = list(fetch_returns)
        self.executions: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple) -> None:
        self.executions.append((sql, params))

    def fetchall(self):
        if not self._fetch_returns:
            return []
        return self._fetch_returns.pop(0)


class _StubConn:
    def __init__(self, fetch_returns: Optional[list[Any]] = None):
        self._cursor = _StubCursor(fetch_returns or [])
        self.commits = 0

    def cursor(self) -> _StubCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1


def test_fetch_action_cards_normalizes_payload_string():
    base_ts = datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc)
    payload_dict = {
        "underlying": "SPY",
        "max_hold_minutes": 90,
        "entry": {"ref_price": 678.0},
        "target": {"ref_price": 675.0, "kind": "level"},
        "stop": {"ref_price": 680.0, "kind": "level"},
    }
    rows = [
        (
            "SPY",
            base_ts,
            "call_wall_fade",
            "SELL_CALL_SPREAD",
            "0DTE",
            "bearish",
            0.65,
            json.dumps(payload_dict),  # asyncpg-style string payload
        )
    ]
    conn = _StubConn(fetch_returns=[rows])
    cards = fetch_action_cards(conn, "SPY", base_ts, base_ts + timedelta(days=1))
    assert len(cards) == 1
    assert cards[0].pattern == "call_wall_fade"
    assert cards[0].payload["max_hold_minutes"] == 90


def test_fetch_quotes_filters_nulls_and_returns_ohlc():
    base_ts = datetime(2026, 5, 1, 14, 0, tzinfo=timezone.utc)
    rows = [
        # ts, open, high, low, close
        (base_ts, 678.0, 678.4, 677.6, 678.2),
        (base_ts + timedelta(minutes=1), None, None, None, None),  # null close → dropped
        (base_ts + timedelta(minutes=2), None, None, None, 678.5),  # O/H/L→close fallback
    ]
    conn = _StubConn(fetch_returns=[rows])
    quotes = fetch_quotes(conn, "SPY", base_ts, base_ts + timedelta(hours=1))
    assert len(quotes) == 2
    assert quotes[0] == (base_ts, 678.0, 678.4, 677.6, 678.2)
    # NULL O/H/L coalesce to the close.
    assert quotes[1] == (base_ts + timedelta(minutes=2), 678.5, 678.5, 678.5, 678.5)


def test_upsert_executes_one_query_per_pattern_and_commits():
    s = PatternStats(
        pattern="cwf",
        underlying="SPY",
        window_start=date(2026, 4, 1),
        window_end=date(2026, 5, 1),
        n_emitted=3,
        n_resolved=3,
        n_target_hit=1,
    )
    conn = _StubConn()
    upsert_pattern_stats(conn, [s])
    cur = conn.cursor()
    assert len(cur.executions) == 1
    sql, params = cur.executions[0]
    assert "playbook_pattern_stats" in sql
    assert params[0] == "cwf"
    assert params[1] == "SPY"
    assert conn.commits == 1


# ----------------------------------------------------------------------
# run() integration
# ----------------------------------------------------------------------


def _run_stub_conn(card_rows, quote_rows):
    """Stub conn that returns cards on the first fetchall() and quotes on the second."""

    class _SeqConn:
        def __init__(self):
            self._fetch_responses = [card_rows, quote_rows]
            self._cur = self  # cursor returns self for simplicity
            self.executions: list[tuple[str, tuple]] = []
            self.commits = 0

        def cursor(self):
            return self

        def execute(self, sql, params):
            self.executions.append((sql, params))

        def fetchall(self):
            if not self._fetch_responses:
                return []
            return self._fetch_responses.pop(0)

        def commit(self):
            self.commits += 1

    return _SeqConn()


def test_run_end_to_end_writes_stats_when_cards_present():
    base_ts = datetime.now(timezone.utc) - timedelta(days=1)
    payload = {
        "underlying": "SPY",
        "max_hold_minutes": 60,
        "entry": {"ref_price": 678.0},
        "target": {"ref_price": 675.0, "kind": "level"},
        "stop": {"ref_price": 680.0, "kind": "level"},
    }
    card_rows = [
        ("SPY", base_ts, "call_wall_fade", "SELL_CALL_SPREAD", "0DTE", "bearish", 0.65, payload)
    ]
    # Quotes (OHLC): drop to 675 in 4 min → target hit.
    quote_rows = [
        (base_ts + timedelta(minutes=1), 678.0, 678.1, 677.9, 678.0),
        (base_ts + timedelta(minutes=2), 678.0, 678.0, 677.0, 677.0),
        (base_ts + timedelta(minutes=3), 677.0, 677.0, 676.0, 676.0),
        (base_ts + timedelta(minutes=4), 676.0, 676.0, 675.0, 675.0),
    ]
    conn = _run_stub_conn(card_rows, quote_rows)
    stats = run(underlying="SPY", days=2, conn=conn, write=True)
    assert len(stats) == 1
    assert stats[0].pattern == "call_wall_fade"
    assert stats[0].n_target_hit == 1
    assert conn.commits == 1


def test_run_returns_empty_when_no_cards():
    conn = _run_stub_conn([], [])
    stats = run(underlying="SPY", days=2, conn=conn, write=True)
    assert stats == []


def test_run_no_write_skips_persistence():
    base_ts = datetime.now(timezone.utc) - timedelta(days=1)
    payload = {
        "underlying": "SPY",
        "max_hold_minutes": 60,
        "entry": {"ref_price": 678.0},
        "target": {"ref_price": 675.0, "kind": "level"},
        "stop": {"ref_price": 680.0, "kind": "level"},
    }
    card_rows = [("SPY", base_ts, "cwf", "SELL_CALL_SPREAD", "0DTE", "bearish", 0.65, payload)]
    quote_rows = [
        # OHLC bar whose low reaches 675 → immediate target hit (bearish).
        (base_ts + timedelta(minutes=1), 678.0, 678.0, 675.0, 676.0),
    ]
    conn = _run_stub_conn(card_rows, quote_rows)
    stats = run(underlying="SPY", days=2, conn=conn, write=False)
    assert len(stats) == 1
    assert conn.commits == 0  # no upsert
