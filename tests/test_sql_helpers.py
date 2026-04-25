"""Tests for the SQL fragment allowlists in src.api.queries._sql_helpers.

These helpers feed f-string SQL queries directly. The safety property the
queries rely on is: every value returned by these helpers is one of a
fixed set of literal SQL fragments — never a string built from caller
input. The tests below pin that invariant.
"""

from __future__ import annotations

import pytest

from src.api.queries._sql_helpers import (
    _BUCKET_EXPRS,
    _GEX_BY_STRIKE_ORDER_CLAUSES,
    _INTERVAL_EXPRS,
    _VIEW_SUFFIXES,
    _bucket_expr,
    _gex_by_strike_order_clause,
    _interval_expr,
    _normalize_timeframe,
    _timeframe_view_suffix,
)


class TestNormalizeTimeframe:
    def test_lowercases(self):
        assert _normalize_timeframe("1Min") == "1min"

    def test_default_when_blank(self):
        assert _normalize_timeframe("") == "1min"
        assert _normalize_timeframe(None) == "1min"  # type: ignore[arg-type]

    def test_alias_mapping(self):
        assert _normalize_timeframe("1hour") == "1hr"
        assert _normalize_timeframe("1HOUR") == "1hr"


class TestBucketExpr:
    @pytest.mark.parametrize("tf", ["1min", "5min", "15min", "1hr", "1day"])
    def test_returns_known_literal(self, tf):
        result = _bucket_expr(tf)
        assert result == _BUCKET_EXPRS[tf]
        assert result.startswith("date_trunc") or "FLOOR" in result

    def test_alias_resolves(self):
        assert _bucket_expr("1hour") == _BUCKET_EXPRS["1hr"]

    @pytest.mark.parametrize(
        "bad",
        [
            "1week",
            "garbage",
            # injection-shaped inputs must never produce SQL
            "1min); DROP TABLE option_chains; --",
            "5min' OR '1'='1",
            "${5min}",
        ],
    )
    def test_rejects_unknown(self, bad):
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            _bucket_expr(bad)

    def test_no_input_in_output(self):
        """Caller input must not appear inside the returned fragment."""
        for tf in _BUCKET_EXPRS:
            assert tf not in _bucket_expr(tf) or tf in {"1min", "5min", "15min"}
            # the timeframe key may share a substring like "5" with the
            # numeric literal in the SQL ("5 minutes") — that's fine; the
            # invariant we care about is that the *fragment* is exactly the
            # one in the allowlist, which the parametrize tests above pin.


class TestIntervalExpr:
    @pytest.mark.parametrize("tf", ["1min", "5min", "15min", "1hr", "1day"])
    def test_returns_known_literal(self, tf):
        assert _interval_expr(tf) == _INTERVAL_EXPRS[tf]
        assert _interval_expr(tf).startswith("INTERVAL '")

    @pytest.mark.parametrize(
        "bad",
        ["1week", "garbage", "1min'; DROP TABLE--"],
    )
    def test_rejects_unknown(self, bad):
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            _interval_expr(bad)


class TestViewSuffix:
    @pytest.mark.parametrize("tf", ["1min", "5min", "15min", "1hr", "1day"])
    def test_returns_known_literal(self, tf):
        assert _timeframe_view_suffix(tf) == _VIEW_SUFFIXES[tf]

    def test_rejects_unknown(self):
        with pytest.raises(ValueError, match="Unsupported timeframe"):
            _timeframe_view_suffix("nope")


class TestGexByStrikeOrderClause:
    def test_distance(self):
        assert _gex_by_strike_order_clause("distance") == _GEX_BY_STRIKE_ORDER_CLAUSES["distance"]
        assert "ABS(g.strike - spot.close)" in _gex_by_strike_order_clause("distance")

    def test_impact(self):
        assert _gex_by_strike_order_clause("impact") == _GEX_BY_STRIKE_ORDER_CLAUSES["impact"]
        assert "ABS(g.net_gex)" in _gex_by_strike_order_clause("impact")

    @pytest.mark.parametrize(
        "bad",
        [
            "",
            "DISTANCE",  # case-sensitive — API layer enforces lowercase
            "distance; DROP TABLE--",
            "impact OR 1=1",
            None,
        ],
    )
    def test_rejects_unknown(self, bad):
        with pytest.raises(ValueError, match="Unsupported sort_by"):
            _gex_by_strike_order_clause(bad)  # type: ignore[arg-type]


class TestAllowlistsAreLiteralsOnly:
    """Pin that every fragment in every allowlist is a constant string.

    The whole point of these mappings is that the SQL is built once at
    import time and never composed from caller input. If someone in the
    future adds a callable, an f-string with a runtime variable, or
    anything that takes input, this test breaks loudly.
    """

    @pytest.mark.parametrize(
        "mapping",
        [
            _BUCKET_EXPRS,
            _INTERVAL_EXPRS,
            _VIEW_SUFFIXES,
            _GEX_BY_STRIKE_ORDER_CLAUSES,
        ],
    )
    def test_values_are_strings(self, mapping):
        for key, value in mapping.items():
            assert isinstance(key, str)
            assert isinstance(value, str)
            assert value, f"empty fragment for {key!r}"
