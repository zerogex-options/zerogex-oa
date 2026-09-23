"""The one thing the chain comparison never measured.

Every metric the feed comparison diffs -- spot, walls, max pain, gamma
flip, net GEX -- is an AVERAGE or a SUM. Averages absorb a penny of quote
noise, which is why the chain evidence came back so clean.

Lee-Ready is not an average. It is a THRESHOLD: buyer- or
seller-initiated by where the trade price sits relative to the quote. A
penny of randomisation on a five-cent-wide option is a material fraction
of that decision, and nothing in the evidence base had tested it.
"""

from __future__ import annotations

from datetime import datetime, timezone

from src.ingestion.providers.base import OptionQuote
from src.tools.feed_compare import FeedSample, compare_flow_classification

NOW = datetime(2026, 9, 23, 14, 0, tzinfo=timezone.utc)


def _sample(provider: str, quotes: dict) -> FeedSample:
    return FeedSample(
        provider=provider,
        captured_at=NOW,
        spot=660.0,
        quotes=quotes,
        metadata={},
    )


def _q(symbol, *, bid, ask, last, volume):
    return OptionQuote(
        option_symbol=symbol, timestamp=NOW, bid=bid, ask=ask, last=last, volume=volume
    )


def test_identical_quotes_never_disagree():
    """The control. Any disagreement here is the harness, not the feed."""
    q = {"S1": _q("S1", bid=1.20, ask=1.25, last=1.25, volume=100)}
    out = compare_flow_classification(_sample("rt", q), _sample("mv", dict(q)))
    assert out["contracts_compared"] == 1
    assert out["contracts_disagreed"] == 0
    assert out["volume_disagreement_pct"] == 0.0


def test_a_trade_at_the_ask_survives_a_penny_of_quote_noise():
    """The reassuring case, and the reason not to panic about this.

    Options mostly trade AT the quote. A print at the ask stays on the ask
    side even after the quote moves a cent, because it is still above the
    band around the mid.
    """
    rt = {"S1": _q("S1", bid=1.20, ask=1.30, last=1.30, volume=500)}
    mv = {"S1": _q("S1", bid=1.21, ask=1.29, last=1.30, volume=500)}
    out = compare_flow_classification(_sample("rt", rt), _sample("mv", mv))
    assert out["contracts_disagreed"] == 0


def test_a_midpoint_fill_can_flip_sides():
    """The exposed case. Price-improved fills sit exactly where the band
    decides, and that is where a penny of quote movement changes the
    answer."""
    # Trade at 1.25. Real quote 1.20/1.30 -> mid 1.25, squarely mid.
    rt = {"S1": _q("S1", bid=1.20, ask=1.30, last=1.25, volume=400)}
    # Randomised quote 1.10/1.20 shifts the mid to 1.15, so the same trade
    # is now above the ask -- buyer-initiated.
    mv = {"S1": _q("S1", bid=1.10, ask=1.20, last=1.25, volume=400)}
    out = compare_flow_classification(_sample("rt", rt), _sample("mv", mv))
    assert out["contracts_disagreed"] == 1
    assert out["shifts"] == {"mid->ask": 1}


def test_disagreement_is_volume_weighted_not_just_counted():
    """A thousand one-lot far-OTM disagreements matter less to a published
    flow figure than one on a heavily traded strike, so the headline number
    has to be weighted."""
    rt = {
        "BIG": _q("BIG", bid=1.20, ask=1.30, last=1.25, volume=10_000),
        "TINY": _q("TINY", bid=1.20, ask=1.30, last=1.30, volume=1),
    }
    mv = {
        "BIG": _q("BIG", bid=1.10, ask=1.20, last=1.25, volume=10_000),
        "TINY": _q("TINY", bid=1.21, ask=1.29, last=1.30, volume=1),
    }
    out = compare_flow_classification(_sample("rt", rt), _sample("mv", mv))
    assert out["contracts_disagreed"] == 1
    # One of two contracts, but essentially all of the volume.
    assert out["contract_disagreement_pct"] == 50.0
    assert out["volume_disagreement_pct"] > 99.0


def test_contracts_that_did_not_trade_are_excluded_not_counted_as_agreeing():
    """Nothing to classify is not the same as classifying the same way. A
    chain is mostly untraded contracts, so folding them in would divide the
    disagreement rate by the size of the universe and report near-zero
    however bad it was."""
    rt = {
        "NOTRADE": _q("NOTRADE", bid=1.20, ask=1.30, last=None, volume=0),
        "TRADED": _q("TRADED", bid=1.20, ask=1.30, last=1.25, volume=100),
    }
    mv = {
        "NOTRADE": _q("NOTRADE", bid=1.21, ask=1.29, last=None, volume=0),
        "TRADED": _q("TRADED", bid=1.10, ask=1.20, last=1.25, volume=100),
    }
    out = compare_flow_classification(_sample("rt", rt), _sample("mv", mv))
    assert out["contracts_compared"] == 1
    assert out["contracts_without_a_trade"] == 1
    assert out["contract_disagreement_pct"] == 100.0


def test_the_trade_itself_is_taken_from_one_side_only():
    """Both feeds read last and volume from the same non-Market-Value
    endpoint, so they are identical by construction. Taking each feed's own
    copy would let a stale row masquerade as a classification difference and
    charge it to the randomisation."""
    rt = {"S1": _q("S1", bid=1.20, ask=1.30, last=1.30, volume=500)}
    mv = {"S1": _q("S1", bid=1.20, ask=1.30, last=1.21, volume=999_999)}
    out = compare_flow_classification(_sample("rt", rt), _sample("mv", mv))
    assert out["volume_compared"] == 500, "candidate's volume leaked in"
    assert out["contracts_disagreed"] == 0, "candidate's last leaked in"
