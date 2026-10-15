"""The shared total-volume / uptick-share fragments, and their call sites.

Thirteen places computed an underlying bar's total volume as
``up_volume + down_volume``, and five answered ``50`` when the split was
absent. That is how one dormant conflation became four broken views and six
broken API endpoints at the vendor cutover: it was spelled out by hand
everywhere instead of named once. These tests pin the fragments and then
pin that every call site actually uses them.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from src.underlying_volume_sql import (
    TOTAL_VOLUME,
    TOTAL_VOLUME_LENIENT,
    UPTICK_SHARE_PCT,
    total_volume,
    total_volume_lenient,
    uptick_share_pct,
)

CALL_SITES = (
    "src/api/queries/technicals.py",
    "src/api/database.py",
    "src/signals/unified_signal_engine.py",
    "src/tradeworkz/flow_context.py",
)


# ---------------------------------------------------------------------------
# The fragments
# ---------------------------------------------------------------------------
def test_the_total_prefers_the_measured_column():
    assert TOTAL_VOLUME == "COALESCE(volume, up_volume + down_volume)"


def test_the_total_still_falls_back_to_the_split():
    """Rows written before the column existed carry NULL in it and a real
    split beside it. Dropping the fallback would blank every historical bar
    -- charts, backfills and the VWAP's own session accumulation."""
    assert "up_volume + down_volume" in TOTAL_VOLUME


def test_the_lenient_total_also_prefers_the_measured_column():
    assert TOTAL_VOLUME_LENIENT.startswith("COALESCE(volume,")


def test_the_lenient_total_counts_a_missing_side_as_zero():
    """Under SUM, the strict fragment voids a whole day's volume when one
    bar dropped an UpVolume: NULL + anything is NULL. The day-sum reads
    keep the pre-column history adding up exactly as it did, which is what
    tests/test_latest_quote_daily_volume.py's ZZQE case pins."""
    assert "COALESCE(up_volume, 0) + COALESCE(down_volume, 0)" in TOTAL_VOLUME_LENIENT


def test_the_two_totals_differ_only_in_that_leniency():
    """Not an alias. If these ever collapse into one, the day-sum reads go
    NULL on a half-classified bar and the per-bar reads start publishing a
    half split as a total -- opposite faults from the same edit."""
    assert TOTAL_VOLUME != TOTAL_VOLUME_LENIENT


def test_lenient_qualification_does_not_mangle_the_prefixed_columns():
    assert total_volume_lenient("v") == (
        "COALESCE(v.volume, COALESCE(v.up_volume, 0) + COALESCE(v.down_volume, 0))"
    )
    assert total_volume_lenient() == TOTAL_VOLUME_LENIENT
    assert "up_v.volume" not in total_volume_lenient("v")


def _func_src(path: str, name: str) -> str:
    """The source of one function, so an assertion about it cannot be
    satisfied -- or broken -- by an identical line in a different one."""
    src = Path(path).read_text()
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return ast.get_source_segment(src, node) or ""
    raise AssertionError(f"{name} not found in {path}")


def test_the_day_sum_reads_use_the_lenient_fragment():
    """The two reads the 2026-09 sweep missed, found on 2026-10-15 in the
    cutover rehearsal: every rehearsal bar carried a populated total and a
    NULL split, so get_latest_quote's day volume read 0 and the bucketed
    history's read NULL. Both spelled the split's sum out by hand, which is
    why neither showed up in the sweep's search for the fragments."""
    latest = _func_src("src/api/database.py", "get_latest_quote")
    assert "SUM({_total_volume_lenient('v.')})" in latest
    assert "COALESCE(v.up_volume, 0) + COALESCE(v.down_volume, 0)" not in latest

    history = _func_src("src/api/database.py", "get_historical_quotes")
    assert "SUM({_total_volume_lenient()})" in history
    assert "(SUM(up_volume) + SUM(down_volume))" not in history


def test_the_futures_reads_are_deliberately_left_on_the_split():
    """Not an oversight, and not to be swept with the others: these read
    futures_quotes, which never gained a `volume` column, so the fragment
    would name a column that does not exist. @ES/@NQ also have no provider
    seam -- they stay on TradeStation, which does supply the split."""
    for name in ("get_latest_future_quote", "get_historical_futures"):
        src = _func_src("src/api/database.py", name)
        assert "up_volume" in src and "down_volume" in src, name
        assert "_total_volume" not in src, name


def test_the_daily_volume_view_agrees_with_the_endpoint():
    """The view is the reference tests/test_latest_quote_daily_volume.py
    checks get_latest_quote against, so a fix to one is a fix to both. It
    is a VIEW, so it reaches production through `make schema-apply`, not a
    service restart."""
    schema = Path("setup/database/schema.sql").read_text()
    view = schema.split("CREATE VIEW underlying_daily_volume AS")[1]
    view = view.split("GROUP BY")[0]
    assert "COALESCE(volume, COALESCE(up_volume, 0) + COALESCE(down_volume, 0))" in view


def test_qualification_does_not_mangle_the_prefixed_columns():
    """A plain "volume" replacement also matches inside "up_volume" and
    "down_volume", which silently yields `up_q.volume` -- valid-looking SQL
    that names a column nothing has."""
    assert total_volume("q") == "COALESCE(q.volume, q.up_volume + q.down_volume)"
    assert total_volume("pv.") == "COALESCE(pv.volume, pv.up_volume + pv.down_volume)"
    assert total_volume() == TOTAL_VOLUME
    assert "up_q.volume" not in total_volume("q")
    assert "down_pv.volume" not in total_volume("pv")


def test_the_uptick_share_abstains_when_there_is_no_classification():
    frag = uptick_share_pct()
    assert frag.startswith("CASE WHEN up_volume IS NULL OR down_volume IS NULL THEN NULL")


def test_the_uptick_share_keeps_fifty_for_a_bar_that_did_not_trade():
    """The 50 was never wrong for its real case -- no ticks either way IS
    balanced. It was wrong as an answer to "this feed cannot classify"."""
    assert "* 100, 50" in UPTICK_SHARE_PCT
    assert "NULLIF((up_volume + down_volume)::numeric, 0)" in UPTICK_SHARE_PCT


def test_the_uptick_share_qualifies_too():
    assert uptick_share_pct("c").startswith("CASE WHEN c.up_volume IS NULL OR c.down_volume")


# ---------------------------------------------------------------------------
# The call sites
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("path", CALL_SITES)
def test_no_call_site_totals_volume_by_hand(path):
    """The regression this whole change exists to prevent: a fourteenth
    hand-written total that goes NULL at cutover and is noticed by a
    customer rather than by us.

    Scoped to TOTAL expressions. A bare `up_volume + down_volume` as the
    DENOMINATOR of an uptick ratio is about the split itself and is left
    alone -- `uptick_share_pct` contains exactly that.
    """
    src = Path(path).read_text()
    for node in ast.walk(ast.parse(src)):
        if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
            continue
        sql = node.value
        if "underlying_quotes" not in sql and "up_volume" not in sql:
            continue
        for shape in (
            "SUM(up_volume + down_volume)",
            "AVG(up_volume + down_volume)",
            "STDDEV_SAMP(up_volume + down_volume)",
            "SUM(close * (up_volume + down_volume))",
            "(up_volume + down_volume) AS volume",
            "(up_volume + down_volume) AS current_volume",
            "COALESCE(up_volume, 0) + COALESCE(down_volume, 0)",
        ):
            assert shape not in sql, f"{path}:{node.lineno} totals volume as {shape}"


@pytest.mark.parametrize("path", CALL_SITES)
def test_no_call_site_still_defaults_the_uptick_share_to_fifty(path):
    """`COALESCE(up / NULLIF(up + down, 0) * 100, 50)` reads as a measured
    neutral tape when the truth is that nothing measured it."""
    src = Path(path).read_text()
    pattern = re.compile(
        r"up_volume[^)]*?/\s*NULLIF\(\s*\(?[^)]*?up_volume\s*\+\s*down_volume.*?\*\s*100\s*,\s*50",
        re.S,
    )
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            assert not pattern.search(node.value), f"{path}:{node.lineno} still defaults to 50"


def test_the_index_proxy_vwap_reads_the_measured_total():
    """The sharpest case in the sweep: cash indices have no volume of their
    own, so SPX and NDX VWAP is computed from an ETF proxy's volume. On the
    split alone, COALESCE(pv.volume, 0) made cum_vol zero and VWAP came back
    NULL for both index underlyings."""
    src = Path("src/signals/unified_signal_engine.py").read_text()
    assert "{_total_volume()} AS volume" in src
    assert "(up_volume + down_volume) AS volume" not in src


def test_every_fragment_call_site_is_inside_an_f_string():
    """A call site that is NOT an f-string embeds the literal text
    `{_total_volume()}` into the SQL -- which is a syntax error at the
    database, but only for whoever hits that endpoint first."""
    for path in CALL_SITES:
        tree = ast.parse(Path(path).read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                assert "{_total_volume" not in node.value, f"{path}:{node.lineno}"
                assert "{_uptick_share_pct" not in node.value, f"{path}:{node.lineno}"


def test_the_response_model_allows_an_absent_split():
    """Returning NULL from SQL is only half the fix: a required Decimal on
    the Pydantic model turns the honest answer into a 500."""
    from src.api.models import FlowBuyingPressurePoint

    fields = FlowBuyingPressurePoint.model_fields
    assert not fields["buy_pct"].is_required()
    assert not fields["period_buy_pct"].is_required()
    assert fields["volume"].is_required(), "the total is measured, not inferred"
