"""Tests for skew_delta component."""

from datetime import datetime, timezone

from src.signals.components.base import MarketContext
from src.signals.basic.skew_delta import (
    SkewDeltaComponent,
    _SKEW_BASELINE,
    _SKEW_SATURATION,
)


def _ctx(**overrides) -> MarketContext:
    defaults = dict(
        timestamp=datetime(2026, 4, 14, 14, 0, tzinfo=timezone.utc),
        underlying="SPY",
        close=500.0,
        net_gex=0.0,
        gamma_flip=500.0,
        put_call_ratio=1.0,
        max_pain=500.0,
        smart_call=0.0,
        smart_put=0.0,
        recent_closes=[500.0] * 5,
        iv_rank=None,
    )
    defaults.update(overrides)
    return MarketContext(**defaults)


comp = SkewDeltaComponent()


def test_no_data_is_neutral():
    assert comp.compute(_ctx()) == 0.0


def test_baseline_spread_is_neutral():
    ctx = _ctx()
    ctx.extra["skew"] = {
        "otm_put_iv": 0.2 + _SKEW_BASELINE,
        "otm_call_iv": 0.2,
    }
    assert abs(comp.compute(ctx)) < 1e-9


def test_elevated_put_skew_is_bearish():
    ctx = _ctx()
    ctx.extra["skew"] = {
        "otm_put_iv": 0.2 + _SKEW_BASELINE + _SKEW_SATURATION,
        "otm_call_iv": 0.2,
    }
    assert comp.compute(ctx) <= -1.0 + 1e-9


def test_compressed_skew_is_bullish():
    ctx = _ctx()
    ctx.extra["skew"] = {
        "otm_put_iv": 0.2,
        "otm_call_iv": 0.2 + _SKEW_BASELINE,
    }
    # put-call = -baseline ... deviation = -2*baseline ...
    assert comp.compute(ctx) > 0


def test_partial_data_is_neutral():
    ctx = _ctx()
    ctx.extra["skew"] = {"otm_put_iv": 0.2}  # missing call
    assert comp.compute(ctx) == 0.0


def test_context_values_populated():
    ctx = _ctx()
    ctx.extra["skew"] = {"otm_put_iv": 0.22, "otm_call_iv": 0.18}
    cv = comp.context_values(ctx)
    assert cv["otm_put_iv"] == 0.22
    assert cv["otm_call_iv"] == 0.18
    assert cv["spread"] is not None


# ---------------------------------------------------------------------------
# Delta-band selection
# ---------------------------------------------------------------------------
def test_the_sampled_band_is_symmetric_around_the_target():
    """A risk reversal compares like for like. Sampling puts at one delta
    against calls at another puts the difference between those two deltas
    into the spread and calls it skew."""
    from src.signals.basic.skew_delta import (
        _SKEW_DELTA_BAND,
        _SKEW_DELTA_TARGET,
        delta_band,
    )

    lo, hi = delta_band()
    assert lo == _SKEW_DELTA_TARGET - _SKEW_DELTA_BAND
    assert hi == _SKEW_DELTA_TARGET + _SKEW_DELTA_BAND
    assert (lo + hi) / 2 == _SKEW_DELTA_TARGET


def test_the_band_stays_inside_the_range_delta_can_take(monkeypatch):
    """|delta| is bounded by [0, 1]; a band running off either end would
    select nothing and abstain forever -- silently, before this change.

    monkeypatch, not a hand-rolled set-and-reload: this test edits module
    globals, and restoring them by reloading leaves whatever imported the
    module earlier holding the old state. Letting pytest undo it is the
    difference between a local test and a session-wide one.
    """
    import src.signals.basic.skew_delta as sd

    for target, band in ((0.05, 0.10), (0.95, 0.10)):
        monkeypatch.setattr(sd, "_SKEW_DELTA_TARGET", target)
        monkeypatch.setattr(sd, "_SKEW_DELTA_BAND", band)
        lo, hi = sd.delta_band()
        assert 0.0 < lo < hi < 1.0, (target, band, lo, hi)


def test_the_selection_reaches_every_production_underlying():
    """The point of moving off percent-of-spot.

    The ingested chain reaches +/-1.29% on SPX and +/-1.71% on NDX (the
    strike-count cap binds before INGEST_STRIKE_PCT_RANGE does). The old
    2-5% band contained no ingested strike on either, so skew_delta had
    never produced a non-abstain reading there. Delta is tenor-invariant,
    so the sampled strike moves in with the tenor.

    Computed against the repo's own Black-Scholes rather than asserted.
    """
    from src.ingestion.greeks_calculator import GreeksCalculator
    from src.signals.basic.skew_delta import delta_band

    g = GreeksCalculator()
    lo, hi = delta_band()
    # (effective reach %, spot, representative 0-2 DTE ATM IV)
    book = {
        "SPY": (3.03, 660.0, 0.13),
        "QQQ": (2.68, 746.0, 0.17),
        "$NDXP.X": (1.71, 25000.0, 0.18),
        "$SPXW.X": (1.29, 6600.0, 0.12),
    }
    for underlying, (reach_pct, spot, iv) in book.items():
        for dte in (0.5, 1.0, 2.0):
            T = dte / 365.0
            for right in ("P", "C"):
                # Nearest moneyness whose |delta| equals the band's OUTER
                # edge -- the furthest strike the band can ask for.
                low, high = 0.0, 0.25
                for _ in range(200):
                    mid = (low + high) / 2
                    K = spot * (1 + mid) if right == "C" else spot * (1 - mid)
                    if abs(g.calculate_delta(spot, K, T, 0.04, iv, right)) > lo:
                        low = mid
                    else:
                        high = mid
                edge_pct = (low + high) / 2 * 100
                assert edge_pct < reach_pct, (
                    f"{underlying} {dte}DTE {right}: |delta|={lo} sits {edge_pct:.2f}% "
                    f"from spot, outside the {reach_pct}% the chain reaches"
                )
                # And the old band's NEAR edge was outside the reach on the
                # two index underlyings -- the regression being fixed.
                if underlying in ("$NDXP.X", "$SPXW.X"):
                    assert reach_pct < 2.0, underlying
    assert hi > lo


def test_an_abstain_is_published_even_though_the_score_cannot_say_so():
    """ComponentBase defines 0.0 as BOTH neutral and insufficient data, so
    the score alone cannot distinguish "skew is exactly normal" from "there
    was nothing to measure". Anything reading the score must be able to."""
    ctx = _ctx()
    assert comp.compute(ctx) == 0.0
    assert comp.context_values(ctx)["skew_available"] is False

    ctx.extra["skew"] = {"otm_put_iv": 0.2 + _SKEW_BASELINE, "otm_call_iv": 0.2}
    assert abs(comp.compute(ctx)) < 1e-9, "a genuine neutral also scores 0.0"
    assert comp.context_values(ctx)["skew_available"] is True


def test_what_was_sampled_is_published_for_audit():
    """The selection is only checkable after the fact if the rows say which
    contracts answered and where they sat."""
    ctx = _ctx()
    ctx.extra["skew"] = {
        "otm_put_iv": 0.24,
        "otm_call_iv": 0.19,
        "put_contracts": 3,
        "call_contracts": 4,
        "put_abs_delta": 0.2612,
        "call_abs_delta": 0.2447,
        "put_moneyness_pct": 0.61,
        "call_moneyness_pct": 0.66,
    }
    cv = comp.context_values(ctx)
    assert cv["put_contracts"] == 3 and cv["call_contracts"] == 4
    assert cv["put_abs_delta"] == 0.2612
    assert cv["put_moneyness_pct"] == 0.61
    assert cv["delta_band"] == list(__import__(
        "src.signals.basic.skew_delta", fromlist=["delta_band"]).delta_band())


def test_the_query_selects_on_delta_not_on_percent_of_spot():
    """The regression guard. The old band is what made this component inert
    on SPX and NDX; a revert to it must fail here rather than in production,
    where it looks exactly like a market with no skew."""
    import ast
    from pathlib import Path

    src = Path("src/signals/unified_signal_engine.py").read_text()
    skew_sql = [
        n.args[0].value
        for n in ast.walk(ast.parse(src))
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "execute"
        and n.args
        and isinstance(n.args[0], ast.Constant)
        and "ABS(delta) BETWEEN" in n.args[0].value
    ]
    assert len(skew_sql) == 1, "expected exactly one delta-banded skew query"
    sql = skew_sql[0]
    assert "delta IS NOT NULL" in sql, "a fallback-derived delta must not select a contract"
    assert "implied_volatility IS NOT NULL" in sql
    assert "0.95" not in sql and "1.05" not in sql, "percent-of-spot band is back"
