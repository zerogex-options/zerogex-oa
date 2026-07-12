"""Phase 4 -- the Charm-into-Close headline in the pre-market Bulletin tweet.

A forecast with a deadline: time decay alone forces this much dealer stock
trading by the bell if spot holds. Rendered only above a $1M floor.
"""

from src.jobs.bulletin_tweet import SymbolBulletin, _fmt_dollars, _symbol_block


def _b(**kw):
    return SymbolBulletin(symbol="SPY", spot=744.5, gamma_flip=740.0, **kw)


def test_charm_line_rendered_buy():
    block = _symbol_block(_b(charm_close_flow=52_000_000.0))
    assert "Charm into close" in block
    assert "buy $52M" in block
    assert "by 4pm ET if SPY holds here" in block


def test_charm_line_rendered_sell():
    block = _symbol_block(_b(charm_close_flow=-18_000_000.0))
    assert "sell $18M" in block


def test_charm_line_elided_below_floor():
    block = _symbol_block(_b(charm_close_flow=500_000.0))
    assert "Charm into close" not in block


def test_charm_line_elided_when_none():
    block = _symbol_block(_b(charm_close_flow=None))
    assert "Charm into close" not in block


def test_fmt_dollars_scales():
    assert _fmt_dollars(1_200_000_000) == "$1.20B"
    assert _fmt_dollars(-340_000_000) == "$340M"
    assert _fmt_dollars(12_000) == "$12K"
    assert _fmt_dollars(500) == "$500"
