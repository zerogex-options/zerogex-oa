"""Tests for the chain-depth sweep.

The tool exists to answer one question -- does the gamma flip converge as
the option chain deepens, or does it just track how much chain you bought
-- and its answer is only trustworthy if every depth reads the same quotes
at the same instant.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timedelta, timezone

from src.ingestion.providers.base import OptionQuote, ProviderCapabilities
from src.tools import chain_depth_sweep


class _CountingProvider:
    """Serves a 12-expiration chain and counts how often it is asked."""

    name = "counting-probe"

    def __init__(self):
        self.snapshot_calls = 0
        self.expiration_calls = 0
        self.symbols_requested = 0

    @property
    def capabilities(self):
        return ProviderCapabilities(
            underlying_bars=True, option_chain_discovery=True, option_quotes=True
        )

    def _stream(self, symbol, **kw):
        class _S:
            last_error = None

            def start(self_inner):
                pass

            def stop(self_inner):
                pass

            def drain(self_inner):
                from src.ingestion.providers.base import Bar

                return Bar(symbol=symbol, timestamp=datetime.now(timezone.utc), close=660.0)

        return _S()

    def stream_underlying_bars(self, symbol, **kw):
        return self._stream(symbol, **kw)

    def get_option_expirations(self, underlying, strike_price=None):
        self.expiration_calls += 1
        today = datetime.now(timezone.utc).date()
        return [today + timedelta(days=d) for d in range(1, 13)]

    def get_option_strikes(self, underlying, expiration=None):
        return [640.0 + i for i in range(41)]

    def build_option_symbol(self, underlying, expiration, strike, option_type):
        return f"SPY{expiration:%y%m%d}{option_type}{int(strike * 1000):08d}"

    def snapshot_option_quotes(self, symbols):
        self.snapshot_calls += 1
        self.symbols_requested += len(symbols)
        out = {}
        for s in symbols:
            strike = int(s[-8:]) / 1000.0
            right = s[-9]
            moneyness = (strike - 660.0) / 660.0
            centre = -0.012 if right == "P" else 0.010
            peak = 8000 if right == "P" else 6000
            oi = int(peak * math.exp(-((moneyness - centre) ** 2) / (2 * 0.010**2)) + 200)
            out[s] = OptionQuote(
                option_symbol=s,
                bid=1.0,
                ask=1.1,
                open_interest=oi,
                implied_volatility=0.16 + 0.9 * abs(moneyness),
            )
        return out

    def close(self):
        pass


def test_every_depth_reads_one_fetch_at_one_instant():
    """Running the depths as separate fetches is the whole trap.

    SPX moves further in the minutes a serial sweep takes than the effect
    being measured, so a flip difference between depths would be market
    drift wearing the costume of a finding. One fetch, analysed repeatedly,
    is what makes the comparison mean anything.
    """
    provider = _CountingProvider()
    result = chain_depth_sweep.sweep_once(
        provider, "SPY", depths=(3, 6, 12), strike_count_max=10, strike_pct_range=3.0
    )

    assert result["error"] is None
    assert provider.snapshot_calls == 1, "the chain must be fetched exactly once"
    assert provider.expiration_calls == 1

    # ...and at the DEEPEST depth, so the shallower ones are subsets of it.
    assert result["depths"][12]["expirations"] == 12
    assert result["depths"][6]["expirations"] == 6
    assert result["depths"][3]["expirations"] == 3
    assert result["depths"][3]["contracts"] < result["depths"][12]["contracts"]


def test_a_shallower_depth_is_a_prefix_of_a_deeper_one():
    """Nearest-N is a subset of nearest-M, so the depths nest."""
    now = datetime.now(timezone.utc)
    rows = [
        {"expiration": date(2026, 9, 21 + d), "strike": 100.0 + i, "timestamp": now}
        for d in range(5)
        for i in range(3)
    ]
    three = chain_depth_sweep._rows_for_depth(rows, 3)
    five = chain_depth_sweep._rows_for_depth(rows, 5)
    assert {r["expiration"] for r in three} == {date(2026, 9, 21 + d) for d in range(3)}
    assert all(r in five for r in three)
    # A depth past the end of the chain takes everything, not nothing.
    assert len(chain_depth_sweep._rows_for_depth(rows, 99)) == len(rows)


def _round(depths_to_flip, spot=660.0):
    return {
        "spot": spot,
        "error": None,
        "depths": {
            d: {
                "contracts": 80 * d,
                "expirations": d,
                "gamma_flip": f,
                "flip_distance": None if f is None else (spot - f) / spot,
                "net_gex": -1.0e9,
                "call_wall": 665.0,
                "put_wall": 655.0,
            }
            for d, f in depths_to_flip.items()
        },
    }


def test_shrinking_steps_read_as_convergence(capsys):
    """Each added block moving the flip less than the last means the shallow
    chain was truncation -- there is a right answer and it can be reached."""
    rounds = [_round({3: 700.0, 6: 720.0, 9: 728.0, 12: 730.0})]
    summary = chain_depth_sweep.summarise(rounds, (3, 6, 9, 12))

    assert summary[3]["step_from_previous"] is None
    assert summary[6]["step_from_previous"] == 20.0
    assert summary[12]["step_from_previous"] == 2.0

    chain_depth_sweep._print_summary(summary, "$SPXW.X", 1)
    assert "CONVERGING" in capsys.readouterr().out


def test_steady_steps_read_as_no_convergence(capsys):
    """A flip that keeps marching is a function of the config, not the market."""
    rounds = [_round({3: 700.0, 6: 720.0, 9: 740.0, 12: 760.0})]
    summary = chain_depth_sweep.summarise(rounds, (3, 6, 9, 12))
    chain_depth_sweep._print_summary(summary, "$SPXW.X", 1)
    out = capsys.readouterr().out
    assert "NOT CONVERGING" in out
    assert "is truncation" not in out, "the converging verdict must not also appear"


def test_an_unresolved_chain_gives_no_verdict_rather_than_a_wrong_one(capsys):
    """2026-09-21: SPX was so long-gamma the crossing sat ~30% above spot,
    outside the 8% actionable band, and NO depth resolved. Inventing a
    convergence verdict from that would be worse than saying nothing."""
    rounds = [_round({3: None, 6: None, 9: None, 12: None})]
    summary = chain_depth_sweep.summarise(rounds, (3, 6, 9, 12))
    assert all(r["resolved"] == 0 for r in summary.values())
    assert all(r["attempted"] == 1 for r in summary.values())

    chain_depth_sweep._print_summary(summary, "$SPXW.X", 1)
    out = capsys.readouterr().out
    assert "NO VERDICT" in out
    assert "CONVERGING" not in out


def test_a_partially_resolved_sweep_still_counts_what_resolved():
    """Depth 3 unresolved and depth 12 resolved is itself the finding."""
    rounds = [_round({3: None, 12: 730.0}), _round({3: None, 12: 734.0})]
    summary = chain_depth_sweep.summarise(rounds, (3, 12))
    assert summary[3]["resolved"] == 0 and summary[3]["attempted"] == 2
    assert summary[12]["resolved"] == 2
    assert summary[12]["mean_flip"] == 732.0
