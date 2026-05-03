"""Cycle-integration tests for the Playbook (PR-13).

Verifies the sync helpers in ``src/signals/playbook/cycle.py``:
context built from in-memory cycle state, Action Card persistence to
``signal_action_cards``, and STAND_DOWN cards short-circuiting the
INSERT.  Uses a stub conn so tests don't need a live Postgres.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from types import SimpleNamespace

from src.signals.components.base import MarketContext


# Lightweight stand-in for AdvancedSignalResult so the tests don't trigger
# the eager src/signals/advanced/__init__.py import chain.
def _result(name: str, score: float, context: Optional[dict] = None):
    return SimpleNamespace(name=name, score=float(score), context=context or {})


from src.signals.playbook.cycle import (
    build_context_from_cycle,
    evaluate_and_persist,
    insert_action_card_sync,
)
from src.signals.playbook.engine import PlaybookEngine
from src.signals.playbook.types import ActionEnum

# ----------------------------------------------------------------------
# Test doubles
# ----------------------------------------------------------------------


@dataclass
class _FakeScore:
    composite_score: float = 0.0
    direction: str = "high_risk_reversal"
    components: dict[str, Any] = field(default_factory=dict)


class _FakeCursor:
    def __init__(self, executions: list[tuple[str, tuple]]):
        self._executions = executions

    def execute(self, sql: str, params: tuple) -> None:
        self._executions.append((sql, params))


class _FakeConn:
    def __init__(self) -> None:
        self.executions: list[tuple[str, tuple]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self.executions)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _FailingConn(_FakeConn):
    def cursor(self):
        raise RuntimeError("boom")


def _market_ctx() -> MarketContext:
    return MarketContext(
        timestamp=datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc),
        underlying="SPY",
        close=678.4,
        net_gex=7.1e9,
        gamma_flip=676.5,
        put_call_ratio=0.36,
        max_pain=675.0,
        smart_call=-765000.0,
        smart_put=-134000.0,
        recent_closes=[],
        iv_rank=None,
        extra={"vix_level": 16.7, "call_wall": 678.0, "put_wall": 674.0},
    )


def _bearish_flow_advanced() -> list:
    return [
        _result(
            name="trap_detection",
            score=-0.35,
            context={"signal": "bearish_fade", "triggered": True},
        ),
        _result(
            name="range_break_imminence",
            score=0.10,
            context={"label": "Range Fade"},
        ),
    ]


def _bearish_flow_basic() -> list:
    return [
        _result(
            name="tape_flow_bias",
            score=-0.50,
            context={},
        ),
        _result(
            name="positioning_trap",
            score=-0.30,
            context={},
        ),
    ]


def _empty_score() -> _FakeScore:
    return _FakeScore(
        composite_score=0.0,
        direction="high_risk_reversal",
        components={
            "net_gex_sign": {
                "score": -1.0,
                "max_points": 16,
                "contribution": -16,
                "context": {"net_gex": 7.1e9},
            }
        },
    )


# ----------------------------------------------------------------------
# build_context_from_cycle
# ----------------------------------------------------------------------


def test_build_context_synthesizes_snapshots_from_in_memory_state():
    ctx = build_context_from_cycle(
        market_context=_market_ctx(),
        score=_empty_score(),
        advanced_results=_bearish_flow_advanced(),
        basic_results=_bearish_flow_basic(),
    )
    # Advanced + basic snapshots present.
    assert ctx.signal("trap_detection") is not None
    assert ctx.signal("trap_detection").signal == "bearish_fade"
    assert ctx.signal("trap_detection").triggered is True
    assert ctx.signal("tape_flow_bias").score == -50.0
    # MSI dict carries the components.
    assert "net_gex_sign" in ctx.msi_components
    # Levels surfaced from market.extra.
    assert ctx.level("call_wall") == 678.0
    # No history loaded in cycle path.
    assert ctx.signal("trap_detection").score_history == []


def test_call_wall_fade_matches_via_cycle_path():
    """End-to-end: bearish-flow ctx synthesized from cycle state should match."""
    # Inject the pattern explicitly so the test doesn't rely on the
    # discovery cache, which is unstable across other tests that pop
    # `src.signals.playbook.*` from sys.modules.
    from src.signals.playbook.patterns.call_wall_fade import PATTERN as CWF

    engine = PlaybookEngine(patterns=[CWF])  # full pattern discovery
    ctx = build_context_from_cycle(
        market_context=_market_ctx(),
        score=_empty_score(),
        advanced_results=_bearish_flow_advanced(),
        basic_results=_bearish_flow_basic(),
    )
    card = engine.evaluate(ctx)
    assert card.action != ActionEnum.STAND_DOWN
    assert card.pattern == "call_wall_fade"


# ----------------------------------------------------------------------
# insert_action_card_sync
# ----------------------------------------------------------------------


def _trade_card_dict() -> dict[str, Any]:
    return {
        "underlying": "SPY",
        "timestamp": datetime(2026, 5, 1, 18, 30, tzinfo=timezone.utc),
        "action": "SELL_CALL_SPREAD",
        "pattern": "call_wall_fade",
        "tier": "0DTE",
        "direction": "bearish",
        "confidence": 0.65,
    }


def test_insert_action_card_sync_executes_and_commits():
    conn = _FakeConn()
    insert_action_card_sync(conn, _trade_card_dict())
    assert len(conn.executions) == 1
    sql, params = conn.executions[0]
    assert "signal_action_cards" in sql
    assert params[0] == "SPY"
    assert params[2] == "call_wall_fade"
    assert conn.commits == 1


def test_insert_action_card_sync_skips_stand_down():
    conn = _FakeConn()
    card = _trade_card_dict()
    card["action"] = "STAND_DOWN"
    insert_action_card_sync(conn, card)
    assert conn.executions == []
    assert conn.commits == 0


def test_insert_action_card_sync_swallows_db_errors():
    """A DB failure must NOT raise — cycle keeps running."""
    conn = _FailingConn()
    insert_action_card_sync(conn, _trade_card_dict())  # must not raise
    # Rollback is best-effort; we don't assert on it here since the
    # rollback itself could fail without breaking the contract.


# ----------------------------------------------------------------------
# evaluate_and_persist (full path)
# ----------------------------------------------------------------------


def test_evaluate_and_persist_writes_trade_card():
    conn = _FakeConn()
    # Inject the pattern explicitly so the test doesn't rely on the
    # discovery cache, which is unstable across other tests that pop
    # `src.signals.playbook.*` from sys.modules.
    from src.signals.playbook.patterns.call_wall_fade import PATTERN as CWF

    engine = PlaybookEngine(patterns=[CWF])
    card = evaluate_and_persist(
        engine=engine,
        market_context=_market_ctx(),
        score=_empty_score(),
        advanced_results=_bearish_flow_advanced(),
        basic_results=_bearish_flow_basic(),
        conn=conn,
    )
    assert card.pattern == "call_wall_fade"
    # Persistence happened.
    assert len(conn.executions) == 1


def test_evaluate_and_persist_does_not_persist_stand_down():
    conn = _FakeConn()
    # Inject the pattern explicitly so the test doesn't rely on the
    # discovery cache, which is unstable across other tests that pop
    # `src.signals.playbook.*` from sys.modules.
    from src.signals.playbook.patterns.call_wall_fade import PATTERN as CWF

    engine = PlaybookEngine(patterns=[CWF])
    # No flow signals → call_wall_fade fails on flow gate → STAND_DOWN.
    card = evaluate_and_persist(
        engine=engine,
        market_context=_market_ctx(),
        score=_empty_score(),
        advanced_results=[],
        basic_results=[],
        conn=conn,
    )
    assert card.action == ActionEnum.STAND_DOWN
    assert conn.executions == []


def test_evaluate_and_persist_works_without_conn():
    """conn=None branch: compute Card, skip persistence, return Card."""
    # Inject the pattern explicitly so the test doesn't rely on the
    # discovery cache, which is unstable across other tests that pop
    # `src.signals.playbook.*` from sys.modules.
    from src.signals.playbook.patterns.call_wall_fade import PATTERN as CWF

    engine = PlaybookEngine(patterns=[CWF])
    card = evaluate_and_persist(
        engine=engine,
        market_context=_market_ctx(),
        score=_empty_score(),
        advanced_results=_bearish_flow_advanced(),
        basic_results=_bearish_flow_basic(),
        conn=None,
    )
    assert card.pattern == "call_wall_fade"
