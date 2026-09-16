"""Tests for the feed comparison harness.

The harness exists to answer one question during a vendor migration: would
a subscriber see a different number? These tests protect the properties
that make its answer trustworthy.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone

import pytest

from src.ingestion.providers.base import OptionQuote, ProviderCapabilities
from src.tools import feed_compare
from src.tools.feed_compare import (
    FeedSample,
    MetricComparison,
    _pct_diff,
    _quotes_to_option_rows,
    _verdict,
    compare_metrics,
)


def _sample(provider: str, quotes: dict, metadata: dict) -> FeedSample:
    return FeedSample(
        provider=provider,
        captured_at=datetime.now(timezone.utc),
        spot=650.0,
        quotes=quotes,
        metadata=metadata,
    )


# ---------------------------------------------------------------------------
# Diff arithmetic
# ---------------------------------------------------------------------------


def test_pct_diff_handles_zero_and_none():
    assert _pct_diff(None, 1.0) is None
    assert _pct_diff(1.0, None) is None
    assert _pct_diff(0.0, 0.0) is None
    assert _pct_diff(0.0, 5.0) == float("inf")
    assert _pct_diff(100.0, 101.0) == pytest.approx(1.0)
    # Sign of the base must not flip the sign of the difference.
    assert _pct_diff(-100.0, -90.0) == pytest.approx(10.0)


def test_exposure_gets_a_wider_tolerance_than_price():
    """Walls land on a strike ladder; net GEX scales with open interest.

    A 1% net-GEX difference is ordinary. A 1% wall difference means the two
    feeds picked different strikes, which is a real finding.
    """
    incumbent = {"net_gex": 1_000_000.0, "call_wall": 650.0}
    candidate = {"net_gex": 1_010_000.0, "call_wall": 655.0}
    results = {c.metric: c for c in compare_metrics(incumbent, candidate)}

    assert results["net_gex"].within_tolerance is True
    assert results["call_wall"].within_tolerance is False


def test_unresolved_metrics_are_noted_not_silently_equal():
    """Both feeds failing to resolve a flip is not agreement."""
    results = {c.metric: c for c in compare_metrics({"gamma_flip": None}, {"gamma_flip": None})}
    flip = results["gamma_flip"]
    assert flip.within_tolerance is None
    assert "both feeds unresolved" in flip.note


def test_one_sided_resolution_is_flagged():
    results = {c.metric: c for c in compare_metrics({"gamma_flip": 645.0}, {"gamma_flip": None})}
    assert results["gamma_flip"].note == "candidate unresolved"
    assert results["gamma_flip"].within_tolerance is None


def test_verdict_requires_at_least_one_comparable_metric():
    all_unresolved = compare_metrics(
        {m: None for m in ("spot", "net_gex")},
        {m: None for m in ("spot", "net_gex")},
    )
    assert _verdict(all_unresolved) == "incomparable"


def test_verdict_agree_and_diverge():
    agree = compare_metrics({"call_wall": 650.0}, {"call_wall": 650.0})
    assert _verdict(agree) == "agree"

    diverge = compare_metrics({"call_wall": 650.0}, {"call_wall": 700.0})
    assert _verdict(diverge) == "diverge"


def test_metric_comparison_row_renders_missing_values():
    row = MetricComparison(
        metric="gamma_flip",
        incumbent=None,
        candidate=645.0,
        abs_diff=None,
        pct_diff=None,
        within_tolerance=None,
        note="incumbent unresolved",
    ).as_row()
    assert "gamma_flip" in row
    assert "-" in row
    assert "incumbent unresolved" in row


# ---------------------------------------------------------------------------
# Row shaping
# ---------------------------------------------------------------------------


def test_quotes_without_metadata_are_dropped_not_guessed():
    """A mis-parsed strike would silently move a wall.

    Dropping an unmapped contract makes it show up as a coverage
    difference, which is visible, rather than as a wrong strike, which is
    not.
    """
    quotes = {
        "KNOWN": OptionQuote("KNOWN", bid=1.0, ask=1.2),
        "ORPHAN": OptionQuote("ORPHAN", bid=1.0, ask=1.2),
    }
    metadata = {"KNOWN": {"strike": 650.0, "expiration": date(2026, 9, 18), "option_type": "C"}}
    rows = _quotes_to_option_rows(quotes, metadata, datetime.now(timezone.utc))
    assert [r["option_symbol"] for r in rows] == ["KNOWN"]


def test_row_shaping_fills_analytics_contract():
    quotes = {"X": OptionQuote("X", bid=1.0, ask=1.5, last=1.2, volume=10, open_interest=99)}
    metadata = {"X": {"strike": 650.0, "expiration": date(2026, 9, 18), "option_type": "P"}}
    row = _quotes_to_option_rows(quotes, metadata, datetime.now(timezone.utc))[0]

    for key in (
        "option_symbol",
        "strike",
        "expiration",
        "option_type",
        "bid",
        "ask",
        "last",
        "mid",
        "volume",
        "open_interest",
        "implied_volatility",
    ):
        assert key in row, key
    assert row["mid"] == pytest.approx(1.25)
    assert row["strike"] == 650.0
    assert row["option_type"] == "P"


def test_missing_volume_and_oi_become_zero_in_rows():
    """Analytics sums these; None would raise. Zero is correct HERE only
    because the row is an analytics input, not a persisted record."""
    quotes = {"X": OptionQuote("X", bid=1.0, ask=1.5)}
    metadata = {"X": {"strike": 650.0, "expiration": date(2026, 9, 18), "option_type": "C"}}
    row = _quotes_to_option_rows(quotes, metadata, datetime.now(timezone.utc))[0]
    assert row["volume"] == 0
    assert row["open_interest"] == 0


# ---------------------------------------------------------------------------
# Coverage accounting
# ---------------------------------------------------------------------------


def test_sample_counts_distinguish_quoted_from_open_interest():
    """GEX is open-interest weighted.

    A candidate that quotes every contract but seeds no OI produces a
    plausible-looking empty gamma profile, and a spot check would never
    catch it. The counts are reported separately so it does.
    """
    quotes = {
        "A": OptionQuote("A", bid=1.0, ask=1.2, open_interest=500),
        "B": OptionQuote("B", bid=1.0, ask=1.2, open_interest=0),
        "C": OptionQuote("C", bid=None, ask=None, open_interest=700),
    }
    sample = _sample("candidate", quotes, {})
    assert sample.contract_count == 3
    assert sample.quoted_count == 2
    assert sample.oi_count == 2


def test_errored_sample_reports_zero_coverage():
    sample = FeedSample(
        provider="candidate",
        captured_at=datetime.now(timezone.utc),
        spot=None,
        quotes={},
        metadata={},
        error="no entitlement",
    )
    assert sample.contract_count == 0
    assert sample.oi_count == 0
    assert sample.error == "no entitlement"


# ---------------------------------------------------------------------------
# CLI guards
# ---------------------------------------------------------------------------


def test_cli_refuses_to_compare_a_feed_against_itself(monkeypatch, capsys):
    from src.tools import feed_compare

    monkeypatch.setenv("MARKET_DATA_PROVIDER", "tradestation")
    with pytest.raises(SystemExit):
        feed_compare.main(["--incumbent", "stub", "--candidate", "stub"])
    assert "proves nothing" in capsys.readouterr().err


def test_cli_requires_a_candidate(monkeypatch, capsys):
    from src.tools import feed_compare

    monkeypatch.setenv("MARKET_DATA_COMPARE_PROVIDER", "")
    with pytest.raises(SystemExit):
        feed_compare.main(["--underlying", "SPY"])
    assert "no candidate provider" in capsys.readouterr().err


def test_cli_rejects_a_candidate_that_cannot_serve_chains(monkeypatch, capsys):
    """A run of empty comparisons reads like an outage. Say why instead."""
    from src.ingestion.providers import _REGISTRY
    from src.tools import feed_compare
    from src.ingestion.providers.stub import StubProvider

    _REGISTRY["chainless"] = lambda **kw: StubProvider(ProviderCapabilities(underlying_bars=True))
    try:
        code = feed_compare.main(["--incumbent", "stub", "--candidate", "chainless"])
        assert code == 2
        assert "option_quotes=False" in capsys.readouterr().err
    finally:
        _REGISTRY.pop("chainless", None)


# ---------------------------------------------------------------------------
# Coupling guard
# ---------------------------------------------------------------------------


def test_harness_analytics_entry_points_still_exist():
    """Pin the analytics internals the harness drives.

    ``feed_compare`` deliberately calls ``AnalyticsEngine`` private methods
    rather than a public wrapper: the whole point is to run the candidate
    feed through the *exact* code the API serves from, and a wrapper would
    be a second code path that could diverge.

    The cost of that choice is coupling. This test converts it from a
    silent break during a migration -- precisely when the tooling matters
    most -- into a fast unit failure at refactor time.
    """
    import inspect

    from src.analytics.main_engine import AnalyticsEngine
    from src.analytics.walls import compute_call_put_walls

    expected = {
        "_calculate_gex_by_strike": ("options", "underlying_price", "timestamp"),
        "_resolve_gamma_flip": ("options", "spot", "timestamp"),
        "_calculate_max_pain_by_expiration": ("options",),
    }
    for name, required_params in expected.items():
        method = getattr(AnalyticsEngine, name, None)
        assert method is not None, f"AnalyticsEngine.{name} disappeared"
        params = list(inspect.signature(method).parameters)
        for required in required_params:
            assert required in params, (
                f"AnalyticsEngine.{name} no longer accepts {required!r}; "
                "src/tools/feed_compare.py calls it positionally"
            )

    walls_params = list(inspect.signature(compute_call_put_walls).parameters)
    assert walls_params[:2] == ["gex_by_strike", "spot_price"]


# ---------------------------------------------------------------------------
# Shadow schema / writer agreement
# ---------------------------------------------------------------------------


def _ddl_columns(table: str):
    import re

    ddl = open("setup/database/shadow_tables.sql").read()
    match = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\);", ddl, re.S)
    assert match, f"{table} missing from shadow_tables.sql"
    columns = []
    for line in match.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("--") or line.upper().startswith("PRIMARY KEY"):
            continue
        columns.append(line.split()[0])
    return columns


def _insert_columns(table: str):
    import re

    src = open("src/tools/feed_compare.py").read()
    match = re.search(rf"INSERT INTO {table} \((.*?)\) VALUES", src, re.S)
    assert match, f"nothing inserts into {table}"
    return [c.strip() for c in match.group(1).replace("\n", " ").split(",") if c.strip()]


def test_every_shadow_table_has_a_writer():
    """A table in the DDL with no INSERT is a silently empty evaluation record.

    ``underlying_quotes_shadow`` shipped in exactly that state: created by
    the schema, documented as persisted, and never written. The gap only
    shows up weeks later when you try to explain a GEX divergence and
    discover you never captured whether the feeds agreed on spot.
    """
    import re

    ddl = open("setup/database/shadow_tables.sql").read()
    src = open("src/tools/feed_compare.py").read()
    created = set(re.findall(r"CREATE TABLE IF NOT EXISTS ([a-z_]+)", ddl))
    written = set(re.findall(r"INSERT INTO ([a-z_]+)", src))
    assert created <= written, f"tables with no writer: {sorted(created - written)}"


@pytest.mark.parametrize(
    "table",
    ["option_chains_shadow", "underlying_quotes_shadow", "feed_comparisons"],
)
def test_insert_columns_exist_in_the_ddl(table):
    """A column-list drift fails at the worst moment, mid-evaluation."""
    ddl_cols = _ddl_columns(table)
    for column in _insert_columns(table):
        assert column in ddl_cols, f"{table}.{column} is not in the DDL"


@pytest.mark.parametrize(
    "table",
    ["option_chains_shadow", "underlying_quotes_shadow", "feed_comparisons"],
)
def test_insert_placeholder_count_matches_column_count(table):
    """Mismatched placeholders raise only when a row is actually written."""
    import re

    src = open("src/tools/feed_compare.py").read()
    n_columns = len(_insert_columns(table))
    block = re.search(rf"INSERT INTO {table} \(.*?\) VALUES\s*(%s|\((?P<ph>[^)]*)\))", src, re.S)
    assert block, f"no VALUES clause found for {table}"
    placeholders = block.group("ph")
    if placeholders is None:
        # execute_values form: a single %s stands in for the whole row list,
        # so the arity is carried by the tuples the caller builds instead.
        return
    assert placeholders.count("%s") == n_columns, (
        f"{table}: {n_columns} columns but " f"{placeholders.count('%s')} placeholders"
    )


def test_spot_bar_is_carried_on_the_sample():
    """The bar, not just the close, so the tape can be persisted."""
    from src.ingestion.providers.base import Bar

    bar = Bar(
        symbol="SPY",
        timestamp=datetime.now(timezone.utc),
        open=650.0,
        high=651.0,
        low=649.0,
        close=650.5,
    )
    sample = FeedSample(
        provider="p",
        captured_at=datetime.now(timezone.utc),
        spot=650.5,
        quotes={},
        metadata={},
        spot_bar=bar,
    )
    assert sample.spot_bar is bar
    assert sample.spot_bar.close == 650.5


def test_persist_underlying_bar_noops_without_a_bar():
    """A sample with no spot bar must not attempt a write."""
    from src.tools.feed_compare import persist_underlying_bar

    sample = FeedSample(
        provider="p",
        captured_at=datetime.now(timezone.utc),
        spot=None,
        quotes={},
        metadata={},
        error="no entitlement",
    )
    assert persist_underlying_bar(sample, "SPY") == 0


# ---------------------------------------------------------------------------
# Probe mode
# ---------------------------------------------------------------------------


def test_probe_needs_no_candidate(monkeypatch, capsys):
    """Probing is about sizing one provider's load, so requiring a second
    one would make it useless for the first thing you do."""
    from src.tools import feed_compare

    monkeypatch.setenv("MARKET_DATA_COMPARE_PROVIDER", "")
    code = feed_compare.main(["--probe", "--incumbent", "stub", "--underlying", "SPY"])
    out = capsys.readouterr().out
    assert "provider" in out
    assert "wall time" in out
    # The stub serves nothing, so a failed probe is the honest result.
    assert code == 1


def test_probe_allows_the_same_provider_twice(monkeypatch):
    """The self-comparison guard is about comparisons. Probing the same
    name twice is a legitimate way to measure variance between runs."""
    from src.tools import feed_compare

    code = feed_compare.main(
        ["--probe", "--incumbent", "stub", "--candidate", "stub", "--underlying", "SPY"]
    )
    assert code in (0, 1), "must not parser.error on a duplicate name"


def test_probe_reports_coverage_not_analytics():
    """A probe answers 'what does one cycle cost', so it must not pay for
    the Greeks pass that would dominate the timing."""
    from src.ingestion.providers.base import ProviderCapabilities
    from src.ingestion.providers.stub import StubProvider
    from src.tools import feed_compare

    provider = StubProvider(ProviderCapabilities())
    result = feed_compare.probe(
        provider,
        "SPY",
        num_expirations=1,
        strike_count_max=2,
        strike_pct_range=1.0,
    )
    for key in (
        "provider",
        "seconds",
        "contracts_requested",
        "contracts_returned",
        "two_sided",
        "with_open_interest",
    ):
        assert key in result, key
    # No analytics keys leak in.
    for key in ("net_gex", "call_wall", "gamma_flip", "max_pain"):
        assert key not in result


# ---------------------------------------------------------------------------
# Index spot routing (probe failure on $SPXW.X, 2026-09-14)
# ---------------------------------------------------------------------------


class _RoutingProvider:
    """Records which bar stream a caller reached for."""

    name = "routing-probe"

    def __init__(self):
        self.calls = []

    @property
    def capabilities(self):
        from src.ingestion.providers.base import ProviderCapabilities

        return ProviderCapabilities(underlying_bars=True, index_bars=True)

    def _stream(self, kind, symbol):
        self.calls.append((kind, symbol))

        class _S:
            last_error = None

            def start(self_inner):
                pass

            def stop(self_inner):
                pass

            def drain(self_inner):
                from datetime import datetime, timezone

                from src.ingestion.providers.base import Bar

                return Bar(symbol=symbol, timestamp=datetime.now(timezone.utc), close=100.0)

        return _S()

    def stream_underlying_bars(self, symbol, **kw):
        return self._stream("underlying", symbol)

    def stream_index_bars(self, symbol, **kw):
        return self._stream("index", symbol)


def test_index_spot_uses_the_index_feed_not_the_equity_one(monkeypatch):
    """SPX spot on the equity endpoint returns nothing at all.

    ThetaData's stock endpoint defaults to Nasdaq Basic, which has never
    heard of SPX. A probe on $SPXW.X died with "no spot price available"
    after a 30s wait -- for two of the four production underlyings.
    """
    monkeypatch.setenv("SYMBOL_ALIASES", "SPX=$SPXW.X,NDX=$NDXP.X")

    for raw in ("$SPXW.X", "$NDXP.X"):
        provider = _RoutingProvider()
        bar = feed_compare._spot_from_provider(provider, raw)
        assert bar is not None
        kind, symbol = provider.calls[0]
        assert kind == "index", f"{raw} was routed to the equity feed"
        assert not symbol.startswith("$"), "the index feed wants a bare root"


def test_index_routing_survives_an_unconfigured_checkout(monkeypatch):
    """`$XXX.X` is TradeStation index notation and needs no alias config."""
    monkeypatch.delenv("SYMBOL_ALIASES", raising=False)
    provider = _RoutingProvider()
    feed_compare._spot_from_provider(provider, "$VIX.X")
    assert provider.calls[0][0] == "index"


def test_equities_still_use_the_equity_feed(monkeypatch):
    monkeypatch.setenv("SYMBOL_ALIASES", "SPX=$SPXW.X")
    for raw in ("SPY", "QQQ"):
        provider = _RoutingProvider()
        feed_compare._spot_from_provider(provider, raw)
        assert provider.calls[0][0] == "underlying", f"{raw} was routed to the index feed"


def test_a_symbol_make_truncated_is_rejected_with_the_fix():
    """'PXW.X' reaches the tool when make eats the '$' in '$SPXW.X'."""
    assert feed_compare._detect_mangled_index_symbol("PXW.X")
    assert "UNDERLYING=" in feed_compare._detect_mangled_index_symbol("PXW.X")
    # Real symbols must not trip it.
    for good in ("SPY", "QQQ", "$SPXW.X", "$VIX.X", ""):
        assert feed_compare._detect_mangled_index_symbol(good) is None


def test_a_permanently_failing_spot_poll_gives_up_early():
    """A wrong endpoint will still be wrong in twenty seconds.

    The original probe waited out its full deadline and then reported "no
    spot price available", discarding the one line that said why.
    """
    import time as _time

    class _Failing(_RoutingProvider):
        def _stream(self, kind, symbol):
            class _S:
                last_error = "ValueError: No data found for: stock_snapshot_ohlc(PXW,nqb,None)"

                def start(self_inner):
                    pass

                def stop(self_inner):
                    pass

                def drain(self_inner):
                    return None

            return _S()

    started = _time.monotonic()
    with pytest.raises(RuntimeError, match="No data found"):
        feed_compare._spot_from_provider(_Failing(), "SPY")
    assert _time.monotonic() - started < 5, "waited out the deadline on a hard failure"


# ---------------------------------------------------------------------------
# The analytics path (pre-flight, 2026-09-14)
# ---------------------------------------------------------------------------


def _quote(symbol, **kw):
    base = dict(bid=1.20, ask=1.30, last=1.25, bid_size=5, ask_size=5, volume=10, open_interest=100)
    base.update(kw)
    return OptionQuote(option_symbol=symbol, timestamp=datetime.now(timezone.utc), **base)


def test_rows_carry_a_timestamp_or_greeks_never_compute():
    """GreeksCalculator treats a missing `timestamp` as a missing required
    field: it returns None for every Greek rather than raising, and the GEX
    sum downstream then dies on `None * open_interest`.

    Every --probe run passed because probes skip analytics entirely, so this
    only surfaced when the full comparison path was exercised.
    """
    as_of = datetime(2026, 9, 14, 15, 0, tzinfo=timezone.utc)
    quotes = {"SPY   260914C00650000": _quote("SPY   260914C00650000")}
    meta = {
        "SPY   260914C00650000": {
            "strike": 650.0,
            "expiration": date(2026, 9, 14),
            "option_type": "C",
        }
    }
    rows = feed_compare._quotes_to_option_rows(quotes, meta, as_of)
    assert rows, "contract was dropped"
    assert rows[0]["timestamp"] == as_of


def test_both_feeds_are_stamped_with_the_same_instant():
    """A per-feed clock would put a difference in time-to-expiry, and
    therefore in the Greeks, that belongs to the harness rather than to
    either vendor."""
    as_of = datetime(2026, 9, 14, 15, 0, tzinfo=timezone.utc)
    meta = {"X": {"strike": 650.0, "expiration": date(2026, 9, 14), "option_type": "C"}}
    a = feed_compare._quotes_to_option_rows({"X": _quote("X")}, meta, as_of)
    b = feed_compare._quotes_to_option_rows({"X": _quote("X", bid=9.9)}, meta, as_of)
    assert a[0]["timestamp"] == b[0]["timestamp"] == as_of


def test_vendor_iv_can_be_discarded_so_both_feeds_solve():
    """TradeStation ships IV on the quote; ThetaData sells it separately.

    Left alone, one feed uses the vendor's surface and the other solves from
    mid, so a GEX divergence cannot be attributed to the quotes.
    """
    as_of = datetime(2026, 9, 14, 15, 0, tzinfo=timezone.utc)
    meta = {"X": {"strike": 650.0, "expiration": date(2026, 9, 14), "option_type": "C"}}
    quotes = {"X": _quote("X", implied_volatility=0.42)}

    kept = feed_compare._quotes_to_option_rows(quotes, meta, as_of)
    assert kept[0]["implied_volatility"] == pytest.approx(0.42)

    solved = feed_compare._quotes_to_option_rows(quotes, meta, as_of, keep_vendor_iv=False)
    assert solved[0]["implied_volatility"] is None


def test_a_contract_with_no_greeks_is_dropped_not_passed_on():
    """enrich_option_data returns None Greeks rather than raising, and the
    GEX sum multiplies gamma by open interest. One unpriceable strike is a
    gap; it must not cost the whole run."""
    rows = [
        {"option_symbol": "GOOD", "strike": 650.0, "open_interest": 10},
        {"option_symbol": "BAD", "strike": 655.0, "open_interest": 10},
    ]

    class _Calc:
        def enrich_option_data(self, row, spot):
            row["gamma"] = 0.01 if row["option_symbol"] == "GOOD" else None
            return row

    import src.ingestion.greeks_calculator as gc

    original = gc.GreeksCalculator
    gc.GreeksCalculator = lambda **kw: _Calc()
    try:
        out = feed_compare._enrich(rows, 650.0, "SPY")
    finally:
        gc.GreeksCalculator = original

    assert [r["option_symbol"] for r in out] == ["GOOD"]
    assert all(r.get("gamma") is not None for r in out)


def test_spot_agreement_alone_is_not_agreement():
    """Spot comes from a bar, not from the chain.

    When every contract fails to price, spot still matches and every chain
    metric is None -- which the original verdict reported as "agree". That
    is the most dangerous possible output: it signs off a migration on a
    comparison that compared nothing. Found by mutating the timestamp fix
    and watching a fully broken run report agreement.
    """
    only_spot = compare_metrics(
        {"spot": 650.0, "net_gex": None, "call_wall": None, "max_pain": None},
        {"spot": 650.0, "net_gex": None, "call_wall": None, "max_pain": None},
    )
    assert _verdict(only_spot) == "incomparable"


def test_one_resolved_chain_metric_is_enough_to_judge():
    """The guard must not make a partially-resolved run unjudgeable."""
    partial = compare_metrics(
        {"spot": 650.0, "net_gex": 1_000_000.0, "gamma_flip": None},
        {"spot": 650.0, "net_gex": 1_005_000.0, "gamma_flip": None},
    )
    assert _verdict(partial) == "agree"

    diverging = compare_metrics(
        {"spot": 650.0, "call_wall": 650.0},
        {"spot": 650.0, "call_wall": 700.0},
    )
    assert _verdict(diverging) == "diverge"


# ---------------------------------------------------------------------------
# Reading a real run (first live comparison, 2026-09-14)
# ---------------------------------------------------------------------------


def test_strike_increment_is_the_modal_gap_not_the_smallest():
    """Chains mix $1 strikes near the money with $5 in the wings.

    The near-money spacing is the one walls and max pain land on, so the
    modal gap is the right ladder; the minimum would be distorted by a
    single tight pair and the mean by the wings.
    """
    wings_and_body = {}
    strikes = (
        [700.0, 705.0, 710.0]  # $5 wings below
        + [754.5]  # one odd half-dollar strike
        + [755.0 + i for i in range(20)]  # the $1 body around the money
        + [790.0, 795.0]  # $5 wings above
    )
    for i, k in enumerate(sorted(strikes)):
        wings_and_body[f"s{i}"] = {"strike": k}

    gaps = sorted({round(b - a, 4) for a, b in zip(sorted(strikes), sorted(strikes)[1:])})
    assert min(gaps) == 0.5, "fixture must contain a gap smaller than the ladder"
    assert 5.0 in gaps, "fixture must contain wider wing gaps"
    # Modal, so neither the lone half-dollar pair nor the $5 wings win.
    assert feed_compare.infer_strike_increment(wings_and_body) == 1.0

    assert feed_compare.infer_strike_increment({"a": {"strike": 650.0}}) is None
    assert feed_compare.infer_strike_increment({}) is None


def test_adjacent_strikes_are_distinguishable_from_distant_ones():
    """A percentage tolerance cannot express a strike ladder.

    SPY strikes are $1 apart near $765, so ONE strike is 0.13% and can never
    fall inside the 0.05% price band. The first live run reported max_pain
    "764 vs 765" as a tolerance failure indistinguishable from a wall
    twenty-six strikes away.
    """
    near = {
        c.metric: c
        for c in compare_metrics({"max_pain": 764.0}, {"max_pain": 765.0}, strike_increment=1.0)
    }["max_pain"]
    far = {
        c.metric: c
        for c in compare_metrics({"max_pain": 764.0}, {"max_pain": 790.0}, strike_increment=1.0)
    }["max_pain"]

    assert "1 strike apart" in near.note
    assert "26 strikes apart" in far.note
    assert near.within_tolerance is False and far.within_tolerance is False

    same = {
        c.metric: c
        for c in compare_metrics({"call_wall": 765.0}, {"call_wall": 765.0}, strike_increment=1.0)
    }["call_wall"]
    assert same.within_tolerance is True
    assert "strike" not in same.note


def test_summary_contrasts_self_variance_with_the_gap_between_feeds():
    """The number that decides a migration is not how far the feeds sit
    apart, but how that compares to how far each moves from itself.

    Replays the shape of the first live run: the incumbent's net GEX spanned
    9.3% across ten consecutive samples while the feeds never differed by
    more than 6.1%.
    """
    incumbent_values = [-2_827.27e6, -2_575.92e6, -2_700.0e6]
    candidate_values = [-2_732.48e6, -2_732.72e6, -2_732.91e6]
    results = []
    for a, b in zip(incumbent_values, candidate_values):
        results.append(
            {
                "comparisons": [
                    {
                        "metric": "net_gex",
                        "incumbent": a,
                        "candidate": b,
                        "pct_diff": (b - a) / abs(a) * 100,
                    }
                ]
            }
        )

    summary = feed_compare.summarise_run(results)["net_gex"]
    assert summary["samples"] == 3
    assert summary["incumbent_self_span_pct"] > 9.0
    assert summary["candidate_self_span_pct"] < 0.1
    assert summary["incumbent_self_span_pct"] > summary["max_cross_feed_pct"]


def test_summary_needs_two_samples_to_say_anything(capsys):
    """One sample has no variance to report; saying 0% would be a lie."""
    one = [
        {
            "comparisons": [
                {"metric": "net_gex", "incumbent": 1.0, "candidate": 1.0, "pct_diff": 0.0}
            ]
        }
    ]
    feed_compare._print_summary(feed_compare.summarise_run(one), "a", "b")
    assert capsys.readouterr().out == ""


def test_summary_warns_when_the_incumbent_is_noisier_than_the_difference(capsys):
    results = []
    for a in (-2_827.0e6, -2_575.0e6):
        results.append(
            {
                "comparisons": [
                    {
                        "metric": "net_gex",
                        "incumbent": a,
                        "candidate": -2_732.0e6,
                        "pct_diff": (-2_732.0e6 - a) / abs(a) * 100,
                    }
                ]
            }
        )
    feed_compare._print_summary(feed_compare.summarise_run(results), "tradestation", "thetadata")
    out = capsys.readouterr().out
    assert "net_gex" in out
    assert "ITSELF" in out


def test_ctrl_c_survives_an_analytics_engine_construction():
    """AnalyticsEngine installs its own SIGINT handler in __init__.

    That is right for the production daemon and wrong here, where the engine
    is built and discarded once per sample: the handler sets running=False
    on an object nobody keeps, swallows the signal, and leaves the
    comparison loop unable to see KeyboardInterrupt. A live run could only
    be stopped by killing it from another shell.
    """
    import signal as _signal

    def _mine(signum, frame):  # pragma: no cover - never invoked
        pass

    original = _signal.getsignal(_signal.SIGINT)
    _signal.signal(_signal.SIGINT, _mine)
    try:
        with feed_compare._preserve_signal_handlers():
            # Stand in for the engine constructor stealing the signal.
            _signal.signal(_signal.SIGINT, _signal.SIG_IGN)
        assert _signal.getsignal(_signal.SIGINT) is _mine
    finally:
        _signal.signal(_signal.SIGINT, original)


def _flip_fixture_chain(
    spot: float, increment: float, n_strikes: int, now: datetime, *, calls: bool
) -> list:
    """A chain shaped in moneyness, so the ladder is the only variable.

    ``calls=False`` builds the puts-only chain: dealer gamma is negative
    everywhere, so no crossing exists at any rung and the resolver is
    correct to return NULL.
    """
    import math

    rows = []
    base = round(spot / increment) * increment
    half = n_strikes // 2
    for dte in (0, 2, 4):
        expiration = (now + timedelta(days=dte)).date()
        for i in range(n_strikes):
            strike = base + (i - half) * increment
            moneyness = (strike - spot) / spot
            for option_type in (("C", "P") if calls else ("P",)):
                centre = -0.012 if option_type == "P" else 0.010
                peak = 8000 if option_type == "P" else 6000
                rows.append(
                    {
                        "option_symbol": None,
                        "strike": float(strike),
                        "expiration": expiration,
                        "option_type": option_type,
                        "open_interest": int(
                            peak * math.exp(-((moneyness - centre) ** 2) / (2 * 0.010**2)) + 200
                        ),
                        "implied_volatility": 0.16 + 0.9 * abs(moneyness),
                        "volume": 0,
                        "bid": 1.0,
                        "ask": 1.1,
                        "mid": 1.05,
                        "gamma": 0.01,
                        "timestamp": now,
                    }
                )
    return rows


def test_an_unresolved_gamma_flip_says_why(caplog):
    """A NULL flip prints as a bare "-" on both feeds -- same as agreement.

    ``_resolve_gamma_flip`` returns None for four distinct reasons (IV
    spike, 0DTE-dominant chain, stale IV at the 0.20 default, one-sided
    chain), and during a migration the first question is whether the
    candidate feed caused it. The engine already builds the diagnostic
    that separates them; the harness has to surface it, or a whole run
    reports a metric it cannot explain.
    """
    now = datetime.now(timezone.utc).replace(hour=17, minute=30, second=0, microsecond=0)
    rows = _flip_fixture_chain(6600.0, 5.0, 40, now, calls=False)

    with caplog.at_level("WARNING", logger="src.tools.feed_compare"):
        out = feed_compare._compute_analytics(rows, 6600.0, "$SPXW.X", now, label="thetadata_mv")

    assert out["gamma_flip"] is None, "fixture must leave the flip unresolved"
    diagnostics = [
        r.getMessage() for r in caplog.records if "gamma_flip unresolved" in r.getMessage()
    ]
    assert diagnostics, "an unresolved flip was reported with no explanation"
    message = diagnostics[0]
    assert "thetadata_mv" in message, "the diagnostic must name which feed"
    # The one-sided cause has to be readable straight off the line.
    assert "calls=0" in message and "puts=120" in message


def test_a_resolved_gamma_flip_stays_quiet(caplog):
    """The diagnostic is for the NULL path only; on a healthy chain it is noise."""
    now = datetime.now(timezone.utc).replace(hour=17, minute=30, second=0, microsecond=0)
    rows = _flip_fixture_chain(6600.0, 5.0, 40, now, calls=True)

    with caplog.at_level("WARNING", logger="src.tools.feed_compare"):
        out = feed_compare._compute_analytics(rows, 6600.0, "$SPXW.X", now, label="thetadata")

    assert out["gamma_flip"] is not None
    assert not [r for r in caplog.records if "gamma_flip unresolved" in r.getMessage()]


def test_a_240_contract_spx_ladder_can_still_resolve_a_flip():
    """The SPX comparison chain being narrower than SPY's does not, by itself,
    cost the flip.

    40 strikes at $5 on a 6600 spot spans +/-1.5%; the same 40 strikes at $1
    on a 660 spot spans +/-3%. That asymmetry is the obvious suspect when one
    underlying resolves a flip and the other does not, and it is wrong -- so
    an unresolved SPX run has to be diagnosed, not explained away by the
    ladder.
    """
    now = datetime.now(timezone.utc).replace(hour=17, minute=30, second=0, microsecond=0)
    spx = feed_compare._compute_analytics(
        _flip_fixture_chain(6600.0, 5.0, 40, now, calls=True), 6600.0, "$SPXW.X", now
    )
    spy = feed_compare._compute_analytics(
        _flip_fixture_chain(660.0, 1.0, 40, now, calls=True), 660.0, "SPY", now
    )
    assert spx["gamma_flip"] is not None
    assert spy["gamma_flip"] is not None


def test_the_two_feeds_are_not_sampled_simultaneously_and_the_run_says_so(capsys):
    """The feeds are polled one after the other, so a live 'feeds apart'
    figure is the vendor's adjustment PLUS whatever the market did in
    between.

    Left unreported, the whole difference reads as the candidate's. The
    gap has to be on the page next to the number it qualifies, or the
    column gets quoted as a measurement of the candidate when it is an
    upper bound.
    """
    result = {
        "underlying": "SPY",
        "captured_at": "2026-09-15T20:08:48+00:00",
        "sampling_skew_seconds": 1.4,
        "incumbent": {
            "provider": "thetadata",
            "contracts": 240,
            "two_sided": 201,
            "with_oi": 238,
            "error": None,
        },
        "candidate": {
            "provider": "thetadata_mv",
            "contracts": 240,
            "two_sided": 201,
            "with_oi": 238,
            "error": None,
        },
        "comparisons": [],
        "verdict": "agree",
    }
    feed_compare._print_report(result)
    assert "1.4s apart" in capsys.readouterr().out


def test_the_summary_calls_feeds_apart_an_upper_bound_when_there_is_skew(capsys):
    summary = {
        "net_gex": {
            "incumbent_self_span_pct": 2.45,
            "candidate_self_span_pct": 2.53,
            "max_cross_feed_pct": 0.08,
            "samples": 3,
        }
    }
    feed_compare._print_summary(summary, "thetadata", "thetadata_mv", max_skew_seconds=1.4)
    out = capsys.readouterr().out
    assert "UPPER" in out and "1.4s apart" in out

    # No skew recorded (an older result, or a single failed sample): say
    # nothing rather than print a caveat that does not apply.
    feed_compare._print_summary(summary, "thetadata", "thetadata_mv")
    assert "UPPER" not in capsys.readouterr().out


class _SlowChainProvider:
    """Instant spot and discovery; the chain snapshot is the only slow phase."""

    name = "slow-chain"
    SNAPSHOT_SECONDS = 0.15

    @property
    def capabilities(self):
        return ProviderCapabilities(
            underlying_bars=True,
            option_chain_discovery=True,
            option_quotes=True,
        )

    def get_option_expirations(self, underlying, strike_price=None):
        return [date(2026, 9, 16)]

    def get_option_strikes(self, underlying, expiration=None):
        return [650.0]

    def build_option_symbol(self, underlying, expiration, strike, option_type):
        return f"{underlying}{expiration:%y%m%d}{option_type}{int(strike * 1000):08d}"

    def snapshot_option_quotes(self, symbols):
        time.sleep(self.SNAPSHOT_SECONDS)
        return {s: OptionQuote(option_symbol=s, bid=1.0, ask=1.1) for s in symbols}


def test_chain_at_is_stamped_after_the_snapshot_not_before():
    """``chain_at`` is what makes the skew measurable, and it is only
    meaningful if it marks when the quotes actually landed.

    Stamped before the fetch it would read as near-simultaneous sampling
    no matter how long the two feeds really took, which is precisely the
    error the field exists to prevent.
    """
    provider = _SlowChainProvider()
    sample = feed_compare.sample_provider(
        provider,
        "SPY",
        num_expirations=1,
        strike_count_max=1,
        strike_pct_range=3.0,
        spot_hint=650.0,
    )
    assert sample.error is None, sample.error
    assert sample.chain_at is not None
    elapsed = (sample.chain_at - sample.captured_at).total_seconds()
    assert elapsed >= provider.SNAPSHOT_SECONDS, (
        f"chain_at is only {elapsed:.3f}s after the sample started, less than the "
        f"{provider.SNAPSHOT_SECONDS}s the snapshot took -- it was stamped before the fetch"
    )


def _net_gex_run(pairs):
    return [
        {
            "comparisons": [
                {
                    "metric": "net_gex",
                    "incumbent": a,
                    "candidate": b,
                    "abs_diff": b - a,
                    "pct_diff": (b - a) / abs(a) * 100.0,
                }
            ]
        }
        for a, b in pairs
    ]


def test_a_metric_that_changes_sign_has_no_meaningful_percentage(capsys):
    """QQQ net_gex ran from -605M to +399M in half an hour.

    Every percentage in that row divides by a mean sitting near zero, so
    the run reported a 535% self-span and a 25% cross-feed gap while the
    feeds were never more than 22M apart on a metric that travelled 1,005M
    by itself. Printed bare, those percentages read as a measurement of the
    candidate and convict it of nothing it did.
    """
    # The widest gap here is the candidate BELOW the incumbent, so a max()
    # over signed differences would miss it and report a smaller number
    # than the run actually saw.
    results = _net_gex_run([(-605.86e6, -630.00e6), (-75.66e6, -56.31e6), (399.38e6, 408.40e6)])
    summary = feed_compare.summarise_run(results)
    row = summary["net_gex"]

    assert row["crosses_zero"] is True
    assert row["max_cross_feed_abs"] == pytest.approx(24.14e6, rel=1e-3)
    assert row["incumbent_range"] == pytest.approx(1005.24e6, rel=1e-3)

    feed_compare._print_summary(summary, "thetadata", "thetadata_mv")
    out = capsys.readouterr().out
    assert "changed sign" in out, "a sign change has to be called out"
    assert "-605.86M" in out and "399.38M" in out, "show the span that broke the ratio"
    assert "24.14M" in out, "the absolute gap is the figure that means something"


def test_a_metric_that_keeps_its_sign_is_reported_normally(capsys):
    """The caveat is for the degenerate case only; elsewhere it is noise."""
    results = _net_gex_run([(-605.86e6, -607.45e6), (-400.00e6, -395.00e6), (-300.00e6, -302.00e6)])
    summary = feed_compare.summarise_run(results)
    assert summary["net_gex"]["crosses_zero"] is False

    feed_compare._print_summary(summary, "thetadata", "thetadata_mv")
    assert "changed sign" not in capsys.readouterr().out


class _IndexBlockedProvider:
    """Spot is unentitled; the options chain underneath it is fine.

    The exact shape ThetaData returns for $NDXP.X: the NDX index level needs
    a Nasdaq GIDS licence, while NDX options are OPRA under the root NDXP.
    """

    name = "index-blocked"

    def __init__(self):
        self.chain_was_asked = False

    @property
    def capabilities(self):
        return ProviderCapabilities(
            index_bars=True,
            option_chain_discovery=True,
            option_quotes=True,
            option_open_interest=True,
        )

    def stream_index_bars(self, symbol, **kw):
        raise RuntimeError(
            "Requesting data for NASDAQ GIDs symbols: [NDX] without proper permissions."
        )

    def stream_underlying_bars(self, symbol, **kw):
        return self.stream_index_bars(symbol, **kw)

    def get_option_expirations(self, underlying, strike_price=None):
        return [date(2026, 9, 18)]

    def get_option_strikes(self, underlying, expiration=None):
        return [24000.0 + 25.0 * i for i in range(40)]

    def build_option_symbol(self, underlying, expiration, strike, option_type):
        return f"NDXP{expiration:%y%m%d}{option_type}{int(strike * 1000):08d}"

    def snapshot_option_quotes(self, symbols):
        self.chain_was_asked = True
        return {
            s: OptionQuote(option_symbol=s, bid=1.0, ask=1.2, open_interest=10) for s in symbols
        }

    def describe_columns(self, underlying):
        return {}

    def close(self):
        pass


def test_a_probe_reports_the_chain_even_when_spot_is_unentitled(capsys):
    """A feed can serve a symbol's OPTIONS and not its INDEX level.

    $NDXP.X failed on the Nasdaq GIDS index call and the probe gave up
    there, reporting zero contracts for a chain it never asked for. Read
    off the output that is indistinguishable from the whole underlying
    being unavailable -- and the two cost very different amounts to fix.
    """
    provider = _IndexBlockedProvider()
    result = feed_compare.probe(
        provider, "$NDXP.X", num_expirations=1, strike_count_max=10, strike_pct_range=3.0
    )

    assert provider.chain_was_asked, "a failed spot call must not skip the chain"
    assert result["contracts_returned"] == 20
    assert result["spot"] is None
    assert "NASDAQ GIDs" in (result["spot_error"] or "")

    feed_compare._print_probe(result)
    out = capsys.readouterr().out
    assert "UNAVAILABLE" in out
    assert "options reachable" in out, "the two halves must be reported separately"


def test_a_comparison_still_refuses_to_run_without_spot():
    """The probe's leniency must not leak into the comparison.

    Every chain metric prices against spot; a sample taken without one
    would compare two feeds on Greeks computed from nothing.
    """
    provider = _IndexBlockedProvider()
    sample = feed_compare.sample_provider(
        provider, "$NDXP.X", num_expirations=1, strike_count_max=10, strike_pct_range=3.0
    )
    assert sample.error is not None
    assert not sample.quotes
    assert not provider.chain_was_asked


def test_a_chain_with_no_spot_centres_on_the_ladder():
    """Without spot there is no percentage window, so take the ladder's middle."""
    provider = _IndexBlockedProvider()
    _symbols, metadata = feed_compare._resolve_chain(
        provider,
        "$NDXP.X",
        num_expirations=1,
        strike_count_max=4,
        strike_pct_range=3.0,
        spot=None,
    )
    strikes = sorted({m["strike"] for m in metadata.values()})
    ladder = provider.get_option_strikes("$NDXP.X")
    median = sorted(ladder)[len(ladder) // 2]
    assert len(strikes) == 4
    assert min(strikes) <= median <= max(strikes), "the picked strikes must bracket the median"
