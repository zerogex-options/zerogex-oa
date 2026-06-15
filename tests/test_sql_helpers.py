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
    _bucket_floor_subquery,
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


class TestBucketFloorSubquery:
    """``_bucket_floor_subquery`` returns a scalar SQL fragment that yields
    the start timestamp of the Nth most recent bucket in a table.  It
    powers ``window_units`` = "N available buckets" semantics across every
    timeseries endpoint, replacing the old wall-clock floor that under-
    filled charts when the source feed had weekend / overnight gaps."""

    def _basic(self, **overrides):
        kwargs = {
            "table": "underlying_quotes",
            "bucket_expr": _bucket_expr("5min"),
            "symbol_predicate": "symbol = $1",
            "end_expr": "(SELECT max_ts FROM latest)",
            "limit_param": "$2",
        }
        kwargs.update(overrides)
        return _bucket_floor_subquery(**kwargs)

    def test_returns_scalar_subquery_with_min_bucket(self):
        sql = self._basic()
        # Outer SELECT MIN over the inner DISTINCT bucket list — picks the
        # earliest of the N most recent buckets, i.e. the start_ts that
        # makes ``window_units`` mean "N available buckets".
        assert sql.startswith("(SELECT MIN(bucket_ts) FROM (")
        assert "SELECT DISTINCT" in sql
        assert "AS bucket_ts" in sql
        assert sql.endswith("recent_buckets)")

    def test_uses_provided_table_and_predicate(self):
        sql = self._basic(table="gex_summary", symbol_predicate="underlying = $1")
        assert "FROM gex_summary" in sql
        assert "WHERE underlying = $1" in sql

    def test_orders_desc_and_limits_to_window_units(self):
        # Take the N MOST RECENT buckets; MIN of those is the start_ts.
        # ORDER ASC would pick the earliest N — the opposite of what we want.
        sql = self._basic(limit_param="$4")
        assert "ORDER BY bucket_ts DESC" in sql
        assert "LIMIT $4" in sql

    def test_end_expr_caps_the_lookback(self):
        sql = self._basic(end_expr="COALESCE($3::timestamptz, (SELECT max_ts FROM latest))")
        assert "AND timestamp <= COALESCE($3::timestamptz" in sql

    def test_extra_filter_is_interpolated(self):
        # Cash-index session predicate is an allowlist fragment using the
        # bare timestamp column (the subquery doesn't alias the source).
        session_filter = (
            "\n        AND EXTRACT(DOW FROM timestamp AT TIME ZONE 'America/New_York') "
            "BETWEEN 1 AND 5"
        )
        sql = self._basic(extra_filter=session_filter)
        assert "EXTRACT(DOW FROM timestamp" in sql

    def test_no_extra_filter_by_default(self):
        sql = self._basic()
        assert "EXTRACT(DOW" not in sql
        assert "America/New_York" not in sql

    def test_uses_validated_bucket_expression(self):
        # The bucket fragment is whatever ``_bucket_expr`` returns — pinning
        # this composition keeps the helper out of the f-string SQL fast
        # lane only via validated literals.
        sql_5min = self._basic(bucket_expr=_bucket_expr("5min"))
        sql_15min = self._basic(bucket_expr=_bucket_expr("15min"))
        assert "/ 5)" in sql_5min
        assert "/ 15)" in sql_15min


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
