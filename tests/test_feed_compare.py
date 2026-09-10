"""Tests for the feed comparison harness.

The harness exists to answer one question during a vendor migration: would
a subscriber see a different number? These tests protect the properties
that make its answer trustworthy.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.ingestion.providers.base import OptionQuote, ProviderCapabilities
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
    rows = _quotes_to_option_rows(quotes, metadata)
    assert [r["option_symbol"] for r in rows] == ["KNOWN"]


def test_row_shaping_fills_analytics_contract():
    quotes = {"X": OptionQuote("X", bid=1.0, ask=1.5, last=1.2, volume=10, open_interest=99)}
    metadata = {"X": {"strike": 650.0, "expiration": date(2026, 9, 18), "option_type": "P"}}
    row = _quotes_to_option_rows(quotes, metadata)[0]

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
    row = _quotes_to_option_rows(quotes, metadata)[0]
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
