"""Make the projection allowlist fail CLOSED.

``PRICE_FIELDS`` is an allowlist, which means a price field nobody added to
it does not raise — it silently ships a cash-index number under an ES label,
on the same chart as fields that did move.  Two incompatible price axes in
one response is the worst failure mode this feature has, precisely because it
looks plausible.

Scope is the routes the middleware will actually project
(``_PROJECTABLE_PREFIXES``) — the same tuple it tests at runtime, imported
rather than restated so the two cannot drift. This test walks the response
models on those routes, collects every numeric field name, and asserts each
one is deliberately classified as either

  * projected           (``PRICE_FIELDS``),
  * explicitly denied   (``NEVER_PROJECT``), or
  * reviewed non-price  (``ACKNOWLEDGED_NON_PRICE`` below).

Adding a new numeric field to any response model therefore fails this test
until someone decides which bucket it belongs in.  That decision takes a
minute; finding out from a mis-drawn chart takes considerably longer.

If this test fails on a field you just added: it is a PRICE (a level, a
strike, a bar value, a price delta) → ``PRICE_FIELDS``.  It is a dollar
exposure, a count, a ratio, a score, a greek, or an already-futures price →
``NEVER_PROJECT`` if it could be mistaken for a price, otherwise the
acknowledged list here.
"""

import decimal
import inspect
import types
import typing

import pytest
from pydantic import BaseModel

from src.jobs.futures_projection import NEVER_PROJECT, PRICE_FIELDS

# Numeric response fields reviewed and confirmed NOT to live in price space.
# Grouped by why, because the grouping is the argument.
ACKNOWLEDGED_NON_PRICE: frozenset = frozenset(
    {
        # --- dollar exposures and notionals -------------------------------
        "gex",
        "exposure",
        "net_gamma",
        "call_gamma",
        "put_gamma",
        "charm_exposure",
        "vanna_exposure",
        "notional",
        "contribution",
        "net_premium",
        "raw_premium",
        "call_premium_cum",
        "put_premium_cum",
        "net_premium_cum",
        # --- contract counts and volumes ----------------------------------
        "call_oi",
        "put_oi",
        "total_call_oi",
        "total_put_oi",
        "call_volume",
        "put_volume",
        "net_volume",
        "up_volume",
        "down_volume",
        "call_volume_cum",
        "put_volume_cum",
        "net_volume_cum",
        "raw_volume",
        "raw_volume_cum",
        "call_position_cum",
        "put_position_cum",
        "contract_count",
        "sample_size",
        "configured_symbols",
        "eligible_symbols",
        # --- GEX distribution statistics (of exposure, not of price) ------
        # /api/gex/historical-context: percentiles of the GEX series itself.
        "p05",
        "p25",
        "p50",
        "p75",
        "p95",
        "mean",
        "std",
        "min",
        "max",
        "current",
        "percentile",
        "z_score",
        # --- scores, ratios and percentages -------------------------------
        "score",
        "flow_score",
        "gamma_score",
        "gamma_regime",
        "buy_pct",
        "period_buy_pct",
        "participation_pct",
        "bullish_breadth_pct",
        "bearish_breadth_pct",
        "neutral_breadth_pct",
        "span_pct",
        "gamma_flip_span_used",
        "amplifier",
        "weight",
        "flow",
        "opt_flow",
        "flow_direction",
        # --- greeks and their components ----------------------------------
        "charm_baseline",
        "charm_component",
        "charm_deviation",
        "charm_flow",
        "gamma_component",
        "vanna_component",
        "atm_iv",
        "call_iv",
        "put_iv",
        "skew",
        "vol_change_pts",  # volatility points, not price points
        "abs_dollar_gex",
        "close_flow_usd",
        "total_usd",
        # --- backtest / calibration statistics ----------------------------
        "baseline_rate",
        "hit_rate",
        "hit_rate_ci_high",
        "hit_rate_ci_low",
        "hits",
        "edge",
        "edge_p_value",
        "signal_mean_return",
        "signal_t_stat",
        "return_pct",
        "spot_move_pct",
        "residual",
        "predicted_dir",
        "realized_dir",
        "z",
        "total",
        "profiles",
        "span_used",
        "now_index",  # positional index into the session, not an index level
        # --- time and bookkeeping -----------------------------------------
        "available_max_dte",
        "available_strike_count",
        "days",
        "days_elapsed",
        "horizon_days",
        "horizons_days",
        "lookback_days",
        "session_days",
        "times_days",
        "evaluated_sessions",
        "total_sessions",
        "warmup_sessions",
        "skew_seconds",
        "min_to_close",
        "now_min_to_close",
        "session_open_min_to_close",
        "dte",
        "window_minutes",
        "data_age_seconds",
        "age_seconds",  # /api/v1/levels freshness, not a price
        "tod_bucket",
        "tod_bucket_used",
    }
)


def _is_numeric(annotation) -> bool:
    """True for float/int/Decimal, including Optional[...] and List[...]."""
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        return any(_is_numeric(a) for a in typing.get_args(annotation) if a is not type(None))
    if origin in (list, set, tuple, frozenset):
        return any(_is_numeric(a) for a in typing.get_args(annotation))
    return annotation in (float, int, decimal.Decimal)


def _nested_models(annotation):
    """Every BaseModel reachable from an annotation, one level of args deep."""
    for candidate in [annotation, *typing.get_args(annotation)]:
        if inspect.isclass(candidate) and issubclass(candidate, BaseModel):
            yield candidate
        for inner in typing.get_args(candidate):
            if inspect.isclass(inner) and issubclass(inner, BaseModel):
                yield inner


def _projectable_routes():
    """Every route the middleware will actually project, models included.

    ``app.routes`` alone is NOT enough: FastAPI materialises an included
    APIRouter as an opaque route object, so 19 routers — and roughly
    two-thirds of the response models — are invisible from there. An earlier
    version of this test walked only ``app.routes`` and therefore checked 24
    of 146 paths while claiming to be exhaustive.
    """
    import fastapi

    import src.api.main as main_module
    from src.api.futures_middleware import _PROJECTABLE_PREFIXES, _UNSUPPORTED_PREFIXES

    routes = list(main_module.app.routes)
    for value in vars(main_module).values():
        if isinstance(value, fastapi.APIRouter):
            routes.extend(value.routes)

    # Router routes already carry their prefix on ``path``, so a plain
    # startswith against the middleware's own tuple is the exact same test the
    # middleware performs at runtime — which is the point: this must not drift
    # from it.
    seen_paths = set()
    out = []
    for route in routes:
        path = getattr(route, "path", None)
        if not path or path in seen_paths:
            continue
        # Mirror the middleware exactly: unsupported wins over projectable,
        # in that order. Skipping this check here would make the test demand a
        # classification for fields on routes that never get projected.
        if path.startswith(_UNSUPPORTED_PREFIXES):
            continue
        if path.startswith(_PROJECTABLE_PREFIXES):
            seen_paths.add(path)
            out.append(route)
    return out


def _collect_numeric_fields() -> set:
    numeric: set = set()
    visited: set = set()

    def walk(model, depth=0):
        if model in visited or depth > 8:
            return
        if not (inspect.isclass(model) and issubclass(model, BaseModel)):
            return
        visited.add(model)
        for name, field in model.model_fields.items():
            if _is_numeric(field.annotation):
                numeric.add(name)
            for nested in _nested_models(field.annotation):
                walk(nested, depth + 1)

    for route in _projectable_routes():
        response_model = getattr(route, "response_model", None)
        if response_model is None:
            continue
        for model in _nested_models(response_model):
            walk(model)
    return numeric


def test_the_walk_actually_reaches_the_projectable_routes():
    """A silent zero here is how the previous version passed while blind."""
    routes = _projectable_routes()
    assert len(routes) >= 8, f"only matched {len(routes)} projectable routes: {routes}"


def test_every_numeric_response_field_is_classified():
    numeric = _collect_numeric_fields()
    assert numeric, "walked no response models — the introspection broke"

    unclassified = sorted(numeric - PRICE_FIELDS - NEVER_PROJECT - ACKNOWLEDGED_NON_PRICE)
    assert not unclassified, (
        "These numeric response fields are not classified for ES/NQ projection:\n  "
        + "\n  ".join(unclassified)
        + "\n\nDecide for each: a price/level/strike/bar/price-delta goes in "
        "PRICE_FIELDS; a dollar exposure, count, ratio, greek or already-futures "
        "price goes in NEVER_PROJECT; anything else goes in "
        "ACKNOWLEDGED_NON_PRICE in this test."
    )


def test_price_and_deny_lists_do_not_overlap():
    """A field in both would resolve one way and confuse the next reader."""
    assert PRICE_FIELDS.isdisjoint(NEVER_PROJECT)


def test_acknowledged_list_does_not_shadow_a_real_decision():
    """Nothing should sit in the acknowledged list AND a real list."""
    assert ACKNOWLEDGED_NON_PRICE.isdisjoint(PRICE_FIELDS)
    assert ACKNOWLEDGED_NON_PRICE.isdisjoint(NEVER_PROJECT)


@pytest.mark.parametrize(
    "field",
    [
        # Regressions the audit actually found: each of these was missing and
        # would have rendered a cash-index number on a futures chart.
        "settlement_price",
        "current_session_close",
        "prior_session_close",
        "orb_high",
        "orb_low",
        "charm_flip",
        "vanna_flip",
        "zero_flow_level",
        "magnet",
        "pivot",
        "flip",
        "grid",
        "prices",
        "spots",
        "distance_from_spot",
        "difference_from_underlying",
    ],
)
def test_known_price_levels_are_projected(field):
    assert field in PRICE_FIELDS


@pytest.mark.parametrize(
    "field",
    [
        "flip_distance",  # (spot - flip) / spot — a fraction
        "futures_close",  # already on the futures axis
        "futures_reference_close",
        "bid",  # option premium, not an index level
        "ask",
        "call_notional",
        "total_notional",
    ],
)
def test_look_alikes_are_denied(field):
    assert field in NEVER_PROJECT
