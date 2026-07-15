"""Wall-size-relative sizing: end-to-end wiring of wall strength.

Commit 1 makes the flagship Put/Call Wall Bouncer size by how large a wall
is *relative to the symbol's own history*. Before this, the sizing formula
had a ``wall_scale`` term but nothing ever populated ``wall_strength``, so
every wall trade sized at the neutral 1.0x. These tests lock the whole
chain:

  * ``compute_call_put_walls_with_strength`` returns the dollar-gamma
    magnitude at each wall (same ``γ × 100 × S² × 0.01`` scale the
    strike-profile ``abs_dollar_gex`` uses), and the strike-only
    ``compute_call_put_walls`` still returns the exact same strikes;
  * ``fetch_wall_strength_pctile`` reads the right per-side metric and
    interpolates the percentile, degrading to ``None`` (neutral) safely;
  * ``compute_contracts`` scales monotonically with wall strength, and a
    missing strength is identical to a neutral (0.5-percentile) wall;
  * the Bouncer feeds the side-appropriate percentile (0..1) into
    ``components_at_entry['wall_strength']`` — the exact key the engine
    hands to sizing;
  * ``build_snapshot`` surfaces the persisted strengths on the snapshot.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, List, Optional, Tuple

import pytest

from src.analytics.walls import (
    compute_call_put_walls,
    compute_call_put_walls_with_strength,
)
from src.tradeworkz.bots.put_call_wall_bouncer import PutCallWallBouncer
from src.tradeworkz.context import MarketSnapshot, build_snapshot
from src.tradeworkz.gex_stats import fetch_wall_strength_pctile
from src.tradeworkz.models import BotCapital, BotSpec
from src.tradeworkz.sizing import compute_contracts

# ----------------------------------------------------------------------
# walls: compute_call_put_walls_with_strength
# ----------------------------------------------------------------------

SPOT = 752.0
# dollar_scale = 100 * S^2 * 0.01 = S^2  (752^2 = 565504)
DOLLAR_SCALE = 100.0 * SPOT * SPOT * 0.01

# Above spot: 755 has the most call gamma -> call wall.
# Below spot: 745 has the most put gamma -> put wall.
ROWS = [
    {"strike": 750.0, "call_gamma": 1000.0, "put_gamma": 500.0},
    {"strike": 755.0, "call_gamma": 3000.0, "put_gamma": 100.0},
    {"strike": 745.0, "call_gamma": 0.0, "put_gamma": 4000.0},
    {"strike": 760.0, "call_gamma": 2000.0, "put_gamma": 0.0},
]


def test_with_strength_returns_walls_and_dollar_magnitudes():
    cw, pw, cs, ps = compute_call_put_walls_with_strength(ROWS, SPOT)
    assert cw == 755.0
    assert pw == 745.0
    assert cs == pytest.approx(3000.0 * DOLLAR_SCALE)
    assert ps == pytest.approx(4000.0 * DOLLAR_SCALE)


def test_strike_only_helper_agrees_on_strikes():
    """The 2-tuple public helper must stay byte-compatible for its callers."""
    cw, pw = compute_call_put_walls(ROWS, SPOT)
    cw2, pw2, _, _ = compute_call_put_walls_with_strength(ROWS, SPOT)
    assert (cw, pw) == (cw2, pw2) == (755.0, 745.0)


def test_strength_is_none_on_a_side_with_no_wall():
    # No positive call gamma above spot -> no call wall, strength None.
    rows = [{"strike": 745.0, "call_gamma": 0.0, "put_gamma": 4000.0}]
    cw, pw, cs, ps = compute_call_put_walls_with_strength(rows, SPOT)
    assert cw is None and cs is None
    assert pw == 745.0 and ps == pytest.approx(4000.0 * DOLLAR_SCALE)


def test_with_strength_degenerate_inputs():
    assert compute_call_put_walls_with_strength([], SPOT) == (None, None, None, None)
    assert compute_call_put_walls_with_strength(ROWS, 0.0) == (None, None, None, None)
    assert compute_call_put_walls_with_strength(ROWS, None) == (None, None, None, None)


def test_strength_uses_canonical_dollar_scale_at_each_spot():
    """Dollar strength = gamma × 100 × S² × 0.01 — verified at two spot
    levels that both keep strike 800 as the call wall (so only S² varies)."""
    rows = [{"strike": 800.0, "call_gamma": 10.0, "put_gamma": 0.0}]
    for spot in (700.0, 750.0):
        cw, _, cs, _ = compute_call_put_walls_with_strength(rows, spot)
        assert cw == 800.0
        assert cs == pytest.approx(10.0 * 100.0 * spot * spot * 0.01)


# ----------------------------------------------------------------------
# fetch_wall_strength_pctile
# ----------------------------------------------------------------------

DIST = (1.0e8, 5.0e8, 1.0e9, 2.0e9, 4.0e9)  # p05..p95
TS = datetime(2026, 7, 14, 15, 30, tzinfo=timezone.utc)


class _RecCursor:
    def __init__(self, row: Optional[tuple]):
        self._row = row
        self.params: Any = None
        self.executed = False

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed = True
        assert "gex_historical_stats" in sql
        self.params = params

    def fetchone(self):
        return self._row


class _RecConn:
    def __init__(self, row: Optional[tuple]):
        self._cur = _RecCursor(row)

    def cursor(self):
        return self._cur


def test_fetch_wall_pctile_call_side_uses_call_metric():
    conn = _RecConn(DIST)
    pct = fetch_wall_strength_pctile(conn, "SPY", "call", DIST[3], TS)  # p75 value
    assert pct == pytest.approx(75.0)
    # params = (underlying, metric, window, [bucket, -1], bucket)
    assert conn.cursor().params[1] == "call_wall_strength"


def test_fetch_wall_pctile_put_side_uses_put_metric():
    conn = _RecConn(DIST)
    pct = fetch_wall_strength_pctile(conn, "SPY", "put", DIST[1], TS)  # p25 value
    assert pct == pytest.approx(25.0)
    assert conn.cursor().params[1] == "put_wall_strength"


def test_fetch_wall_pctile_none_paths():
    # No history row -> None.
    assert fetch_wall_strength_pctile(_RecConn(None), "SPY", "call", 1.0e9, TS) is None
    # Missing value -> None, and we never even query.
    conn = _RecConn(DIST)
    assert fetch_wall_strength_pctile(conn, "SPY", "call", None, TS) is None
    assert conn.cursor().executed is False
    # Unknown side -> None, no query.
    conn2 = _RecConn(DIST)
    assert fetch_wall_strength_pctile(conn2, "SPY", "sideways", 1.0e9, TS) is None
    assert conn2.cursor().executed is False


# ----------------------------------------------------------------------
# sizing: wall_scale monotonicity
# ----------------------------------------------------------------------


def _capital() -> BotCapital:
    return BotCapital(
        bot_id="b",
        starting_capital=100_000.0,
        current_capital=100_000.0,
        peak_capital=100_000.0,
        max_heat_pct=0.06,
        kelly_fraction=0.5,
        daily_kill_pct=0.02,
    )


def _contracts(wall_strength: Optional[float]) -> int:
    return compute_contracts(
        capital=_capital(),
        entry_price=1.0,
        conviction=0.7,
        size_multiplier=1.0,
        wall_strength=wall_strength,
    )


def test_contracts_increase_with_wall_strength():
    small = _contracts(0.0)
    mid = _contracts(0.5)
    big = _contracts(1.0)
    assert small < mid < big


def test_missing_wall_strength_matches_neutral_half_percentile():
    """None (wall-agnostic) must size exactly like a median wall (scale 1.0)."""
    assert _contracts(None) == _contracts(0.5)


# ----------------------------------------------------------------------
# Bouncer wiring: components_at_entry['wall_strength'] is the engine's key
# ----------------------------------------------------------------------

MIDDAY = datetime(2026, 7, 14, 15, 30, tzinfo=timezone.utc)  # 11:30 ET


def _bot() -> PutCallWallBouncer:
    spec = BotSpec(
        id="put_call_wall_bouncer",
        display_name="Put/Call Wall Bouncer",
        strategy_class="PutCallWallBouncer",
        tier="0DTE",
        direction_mode="context",
        universe="SPY",
        tagline="",
        description="",
        params={"wall_proximity_pct": 0.002, "wall_break_pct": 0.003, "max_hold_minutes": 90},
    )
    return PutCallWallBouncer(spec, ml_state=None)


def _snap(**over) -> MarketSnapshot:
    base = dict(
        underlying="SPY",
        timestamp=MIDDAY,
        spot=755.0,
        call_wall=755.0,
        put_wall=750.0,
        gamma_flip=749.0,
        max_pain=752.0,
        net_gex=3.0e9,
    )
    base.update(over)
    return MarketSnapshot(**base)


def test_bouncer_feeds_call_wall_pctile_as_wall_strength():
    sig = _bot().open_criteria(_snap(spot=755.0, call_wall=755.0, call_wall_strength_pctile=90.0))
    assert sig is not None and sig.wall_ref_side == "call"
    assert sig.components_at_entry["wall_strength"] == pytest.approx(0.90)


def test_bouncer_feeds_put_wall_pctile_on_the_put_side():
    sig = _bot().open_criteria(
        _snap(spot=750.0, put_wall=750.0, max_pain=753.0, put_wall_strength_pctile=30.0)
    )
    assert sig is not None and sig.wall_ref_side == "put"
    assert sig.components_at_entry["wall_strength"] == pytest.approx(0.30)


def test_bouncer_wall_strength_none_when_no_history():
    """No percentile (new column, no history) -> None -> neutral sizing."""
    sig = _bot().open_criteria(_snap(spot=755.0, call_wall=755.0))
    assert sig is not None
    assert sig.components_at_entry["wall_strength"] is None


def test_bouncer_uses_only_the_traded_side_pctile():
    """A huge put wall must not inflate a call-wall (bearish) fade's size."""
    sig = _bot().open_criteria(
        _snap(
            spot=755.0,
            call_wall=755.0,
            call_wall_strength_pctile=20.0,
            put_wall_strength_pctile=99.0,  # irrelevant to a call-side fade
        )
    )
    assert sig is not None and sig.wall_ref_side == "call"
    assert sig.components_at_entry["wall_strength"] == pytest.approx(0.20)


# ----------------------------------------------------------------------
# build_snapshot surfaces the persisted strengths
# ----------------------------------------------------------------------


class _FakeCursor:
    def __init__(self):
        self._script: List[Tuple[str, List[tuple]]] = []
        self._pending: List[tuple] = []

    def program(self, sql_substring: str, rows: Iterable[tuple]) -> None:
        self._script.append((sql_substring, list(rows)))

    def execute(self, sql: str, params: Any = None) -> None:
        for matcher, rows in self._script:
            if matcher in sql:
                self._pending = list(rows)
                return
        self._pending = []

    def fetchone(self):
        return self._pending[0] if self._pending else None

    def fetchall(self):
        return list(self._pending)


class _FakeConn:
    def __init__(self):
        self._cursor = _FakeCursor()

    def cursor(self):
        return self._cursor


def _scripted_conn(call_strength: float, put_strength: float) -> _FakeConn:
    conn = _FakeConn()
    cur = conn.cursor()
    ts = datetime.now(timezone.utc)
    cur.program("SELECT timestamp, close\n        FROM underlying_quotes", [(ts, 752.0)])
    cur.program("SELECT MIN(low), MAX(high)", [(745.0, 760.0, 750.0)])
    cur.program(
        "FROM gex_summary",
        [
            (
                3.0e9,
                749.0,
                752.0,
                1.0,
                755.0,
                745.0,
                752.0,
                5.0e9,
                call_strength,
                put_strength,
            )
        ],
    )
    cur.program("FROM vix_bars", [(17.0,)])
    cur.program("FROM underlying_vwap_deviation", [(752.5, -0.1)])
    cur.program(
        "SELECT close FROM underlying_quotes\n        WHERE symbol", [(752.0,) for _ in range(30)]
    )
    return conn


def test_build_snapshot_populates_raw_wall_strengths():
    snap = build_snapshot(_scripted_conn(1.0e9, 8.0e8), "SPY")
    assert snap is not None
    assert snap.call_wall_strength == pytest.approx(1.0e9)
    assert snap.put_wall_strength == pytest.approx(8.0e8)
    # gex_historical_stats is unprogrammed here -> percentiles degrade to
    # None (neutral), exactly the no-history production path.
    assert snap.call_wall_strength_pctile is None
    assert snap.put_wall_strength_pctile is None
