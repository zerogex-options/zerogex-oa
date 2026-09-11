"""Side-by-side feed comparison for a market data vendor migration.

Implements step 14 of ``docs/compliance/market-data-remediation-runbook.md``
("Check the numbers actually match"): run the incumbent and candidate
providers against the same contracts at the same moment, push both through
the *same* IV, Greeks and analytics code, and report where the published
numbers diverge.

Why compare analytics rather than raw quotes
--------------------------------------------

Two feeds never agree tick for tick.  They time and batch differently,
they sample the NBBO at different instants, and one may carry a venue the
other does not.  A raw-quote diff therefore reports thousands of
differences that mean nothing.

What subscribers actually see is the derived layer: gamma exposure by
strike, the call and put walls, the gamma flip, and max pain.  Those are
the numbers that must agree, and they are far more robust than the quotes
underneath them because a wall is the argmax over a strike ladder, not a
price.  So this tool holds the analytics code constant and varies only the
feed, which makes any surviving difference genuinely attributable to the
vendor.

The runbook's acceptance bar is that the differences are *written down and
explained*, not that they are zero.

Usage
-----

    # one-shot comparison, printed
    python -m src.tools.feed_compare --underlying SPY --candidate stub

    # repeated sampling for a session, persisted to shadow tables
    python -m src.tools.feed_compare --underlying SPY --candidate databento \\
        --duration-minutes 390 --interval-seconds 60 --persist

    # JSON for a report
    python -m src.tools.feed_compare --underlying SPY --candidate thetadata --json

Shadow tables
-------------

``--persist`` writes each feed's normalised quotes to ``option_chains_shadow``,
the spot bar each priced against to ``underlying_quotes_shadow``, and the
run's metric diff to ``feed_comparisons`` -- all keyed by a ``provider``
column. The spot table matters more than it looks: when two feeds disagree
on GEX the first question is whether they disagreed on the price underneath
it, and that is unanswerable after the fact without the bar.

They are deliberately SEPARATE tables rather than a
``source`` column on ``option_chains``: the live tables feed the analytics
engine, the API and every signal, and a candidate feed's rows must not be
able to reach any of that while it is under evaluation.  Create them with
``setup/database/shadow_tables.sql``.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.ingestion.providers import MarketDataProvider, get_provider
from src.ingestion.providers.base import Bar, OptionQuote
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)

#: Metrics compared, in report order. Each maps to a key in the dict
#: returned by :func:`_compute_analytics`.
_METRICS = (
    "spot",
    "net_gex",
    "call_wall",
    "put_wall",
    "gamma_flip",
    "max_pain",
)

#: Relative tolerance below which a difference is reported as agreement.
#: Price-space metrics (walls, flip, max pain, spot) land on a strike
#: ladder, so anything under a tick is noise. Dollar-exposure metrics
#: (net_gex) scale with open interest and move more, hence the wider band.
_DEFAULT_PRICE_TOLERANCE_PCT = 0.05
_DEFAULT_EXPOSURE_TOLERANCE_PCT = 2.0


@dataclass
class MetricComparison:
    """One metric's value under each feed, and how far apart they are."""

    metric: str
    incumbent: Optional[float]
    candidate: Optional[float]
    abs_diff: Optional[float]
    pct_diff: Optional[float]
    within_tolerance: Optional[bool]
    note: str = ""

    def as_row(self) -> str:
        def fmt(v: Optional[float]) -> str:
            if v is None:
                return "-"
            if abs(v) >= 1_000_000:
                return f"{v / 1_000_000:,.2f}M"
            return f"{v:,.2f}"

        verdict = (
            "-" if self.within_tolerance is None else ("ok" if self.within_tolerance else "DIFF")
        )
        pct = "-" if self.pct_diff is None else f"{self.pct_diff:+.2f}%"
        return (
            f"{self.metric:<12} {fmt(self.incumbent):>14} {fmt(self.candidate):>14} "
            f"{pct:>10} {verdict:>6}  {self.note}"
        )


def _pct_diff(a: Optional[float], b: Optional[float]) -> Optional[float]:
    """Percentage difference of ``b`` from ``a``, or ``None`` if undefined."""
    if a is None or b is None:
        return None
    if a == 0:
        return None if b == 0 else float("inf")
    return (b - a) / abs(a) * 100.0


def _quotes_to_option_rows(
    quotes: Dict[str, OptionQuote],
    metadata: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Shape normalised quotes into the dicts the analytics engine expects.

    ``metadata`` supplies strike / expiration / option_type per contract,
    which the provider's chain discovery already resolved. Contracts with
    no metadata are dropped rather than guessed: a mis-parsed strike would
    silently move a wall.
    """
    rows: List[Dict[str, Any]] = []
    for symbol, q in quotes.items():
        meta = metadata.get(symbol)
        if not meta:
            continue
        rows.append(
            {
                "option_symbol": symbol,
                "strike": float(meta["strike"]),
                "expiration": meta["expiration"],
                "option_type": meta["option_type"],
                "bid": q.bid,
                "ask": q.ask,
                "last": q.last,
                "mid": q.effective_mid(),
                "volume": q.volume or 0,
                "open_interest": q.open_interest or 0,
                "implied_volatility": q.implied_volatility,
            }
        )
    return rows


def _enrich(rows: List[Dict[str, Any]], spot: float, underlying: str) -> List[Dict[str, Any]]:
    """Run the production IV and Greeks calculators over ``rows``.

    Using the real calculators, not a reimplementation, is the point: the
    comparison must isolate the feed as the only variable.
    """
    from src.config import RISK_FREE_RATE, resolve_dividend_yield
    from src.ingestion.greeks_calculator import GreeksCalculator

    calculator = GreeksCalculator(
        risk_free_rate=RISK_FREE_RATE,
        dividend_yield=resolve_dividend_yield(get_canonical_symbol(underlying)),
    )
    enriched = []
    for row in rows:
        try:
            enriched.append(calculator.enrich_option_data(dict(row), spot))
        except Exception as e:  # noqa: BLE001 - one bad contract must not
            # abort the comparison; note it and continue.
            logger.debug("enrichment failed for %s: %s", row.get("option_symbol"), e)
    return enriched


def _compute_analytics(
    rows: List[Dict[str, Any]], spot: float, underlying: str, now: datetime
) -> Dict[str, Optional[float]]:
    """Compute the published metrics from one feed's contracts.

    Calls the same ``AnalyticsEngine`` and ``walls`` code the API serves
    from, so a difference here is a difference a subscriber would see.
    """
    from src.analytics.main_engine import AnalyticsEngine
    from src.analytics.walls import compute_call_put_walls

    out: Dict[str, Optional[float]] = {m: None for m in _METRICS}
    out["spot"] = spot
    if not rows:
        return out

    engine = AnalyticsEngine(underlying=underlying)

    gex_by_strike = engine._calculate_gex_by_strike(rows, spot, now)
    if gex_by_strike:
        out["net_gex"] = float(sum(r.get("net_gex", 0.0) for r in gex_by_strike))
        call_wall, put_wall = compute_call_put_walls(gex_by_strike, spot)
        out["call_wall"] = call_wall
        out["put_wall"] = put_wall

    _profile, flip, _span = engine._resolve_gamma_flip(rows, spot, now)
    out["gamma_flip"] = flip

    # Max pain partitions by expiration internally; compare the nearest
    # expiration, which is what the headline figure tracks.
    by_expiry = engine._calculate_max_pain_by_expiration(rows)
    if by_expiry:
        nearest = min(by_expiry)
        out["max_pain"] = by_expiry[nearest]

    return out


def compare_metrics(
    incumbent: Dict[str, Optional[float]],
    candidate: Dict[str, Optional[float]],
    *,
    price_tolerance_pct: float = _DEFAULT_PRICE_TOLERANCE_PCT,
    exposure_tolerance_pct: float = _DEFAULT_EXPOSURE_TOLERANCE_PCT,
) -> List[MetricComparison]:
    """Diff two analytics dicts into a reportable comparison."""
    results: List[MetricComparison] = []
    for metric in _METRICS:
        a = incumbent.get(metric)
        b = candidate.get(metric)
        tolerance = exposure_tolerance_pct if metric == "net_gex" else price_tolerance_pct
        note = ""
        if a is None and b is None:
            note = "both feeds unresolved"
        elif a is None:
            note = "incumbent unresolved"
        elif b is None:
            note = "candidate unresolved"
        pct = _pct_diff(a, b)
        within = None if pct is None else abs(pct) <= tolerance
        results.append(
            MetricComparison(
                metric=metric,
                incumbent=a,
                candidate=b,
                abs_diff=None if (a is None or b is None) else b - a,
                pct_diff=pct,
                within_tolerance=within,
                note=note,
            )
        )
    return results


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------


@dataclass
class FeedSample:
    """One provider's view of the chain at one instant."""

    provider: str
    captured_at: datetime
    spot: Optional[float]
    quotes: Dict[str, OptionQuote]
    metadata: Dict[str, Dict[str, Any]]
    error: Optional[str] = None
    #: The bar ``spot`` was taken from, kept so the underlying tape can be
    #: persisted alongside the chain. When two feeds disagree on GEX the
    #: first question is always whether they disagreed on spot, and that is
    #: unanswerable after the fact without the bar.
    spot_bar: Optional[Bar] = None

    @property
    def contract_count(self) -> int:
        return len(self.quotes)

    @property
    def quoted_count(self) -> int:
        """Contracts with a two-sided quote, the ones GEX can actually use."""
        return sum(1 for q in self.quotes.values() if q.bid is not None and q.ask is not None)

    @property
    def oi_count(self) -> int:
        """Contracts carrying positive open interest.

        Reported separately because GEX is open-interest weighted: a feed
        that quotes every contract but seeds no OI produces a plausible
        looking but empty gamma profile, which a spot-price check would
        never catch.
        """
        return sum(1 for q in self.quotes.values() if (q.open_interest or 0) > 0)


def _resolve_chain(
    provider: MarketDataProvider,
    underlying: str,
    *,
    num_expirations: int,
    strike_count_max: int,
    strike_pct_range: float,
    spot: float,
) -> Tuple[List[str], Dict[str, Dict[str, Any]]]:
    """Pick the contracts to compare, and their strike/expiry metadata.

    Selection mirrors ``StreamManager``: the nearest ``num_expirations``
    expirations, and every strike within ``strike_pct_range`` percent of
    spot, capped at ``strike_count_max`` per expiration. Both feeds are
    handed the SAME contract list so a coverage difference shows up as a
    missing quote rather than as a different universe.
    """
    expirations = provider.get_option_expirations(underlying)[:num_expirations]
    symbols: List[str] = []
    metadata: Dict[str, Dict[str, Any]] = {}
    lo = spot * (1 - strike_pct_range / 100.0)
    hi = spot * (1 + strike_pct_range / 100.0)

    for expiration in expirations:
        exp_str = expiration.isoformat() if isinstance(expiration, date) else str(expiration)
        strikes = [s for s in provider.get_option_strikes(underlying, exp_str) if lo <= s <= hi]
        # Trim from the furthest-from-spot strikes inward, as the engine does.
        strikes.sort(key=lambda s: abs(s - spot))
        strikes = sorted(strikes[:strike_count_max])
        for strike in strikes:
            for option_type in ("C", "P"):
                symbol = provider.build_option_symbol(underlying, expiration, strike, option_type)
                symbols.append(symbol)
                metadata[symbol] = {
                    "strike": strike,
                    "expiration": expiration,
                    "option_type": option_type,
                }
    return symbols, metadata


def sample_provider(
    provider: MarketDataProvider,
    underlying: str,
    *,
    num_expirations: int,
    strike_count_max: int,
    strike_pct_range: float,
    spot_hint: Optional[float] = None,
) -> FeedSample:
    """Take one snapshot of ``underlying``'s chain from ``provider``.

    Uses the snapshot path rather than the streams: a comparison wants a
    consistent instant across both feeds, and a streaming accumulator's
    contents depend on how long it has been running.
    """
    captured_at = datetime.now(timezone.utc)
    try:
        spot_bar = None
        spot = spot_hint
        if spot is None:
            spot_bar = _spot_from_provider(provider, underlying)
            spot = float(spot_bar.close) if spot_bar and spot_bar.close else None
        if not spot or spot <= 0:
            return FeedSample(
                provider=provider.name,
                captured_at=captured_at,
                spot=None,
                quotes={},
                metadata={},
                error="no spot price available",
            )
        symbols, metadata = _resolve_chain(
            provider,
            underlying,
            num_expirations=num_expirations,
            strike_count_max=strike_count_max,
            strike_pct_range=strike_pct_range,
            spot=spot,
        )
        quotes = provider.snapshot_option_quotes(symbols)
        return FeedSample(
            provider=provider.name,
            captured_at=captured_at,
            spot=spot,
            quotes=quotes,
            metadata=metadata,
            spot_bar=spot_bar,
        )
    except Exception as e:  # noqa: BLE001 - a failing feed is a RESULT here,
        # not a crash: "the candidate could not answer" is exactly what the
        # evaluation needs to record.
        logger.warning("sampling %s failed: %s", provider.name, e)
        return FeedSample(
            provider=provider.name,
            captured_at=captured_at,
            spot=None,
            quotes={},
            metadata={},
            error=str(e),
        )


def _spot_from_provider(provider: MarketDataProvider, underlying: str) -> Optional[Bar]:
    """Best available spot bar for ``underlying`` from this provider.

    Briefly runs the underlying bar stream rather than assuming a quote
    endpoint, because that is the path a migration actually depends on.
    Returns the whole bar rather than just the close so the caller can
    persist the tape it priced against.
    """
    if not provider.capabilities.underlying_bars:
        return None
    stream = provider.stream_underlying_bars(underlying, db_symbol=get_canonical_symbol(underlying))
    try:
        stream.start()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            bar = stream.drain()
            if bar and bar.close:
                return bar
            time.sleep(0.5)
    finally:
        stream.stop()
    return None


# ---------------------------------------------------------------------------
# Persistence (optional; see setup/database/shadow_tables.sql)
# ---------------------------------------------------------------------------


def persist_sample(sample: FeedSample, underlying: str) -> int:
    """Write one feed's quotes to ``option_chains_shadow``. Returns rows written.

    Never raises: a persistence failure must not abort an evaluation run
    that is otherwise producing usable comparisons on stdout.
    """
    if not sample.quotes:
        return 0
    db_symbol = get_canonical_symbol(underlying)
    rows = []
    for symbol, q in sample.quotes.items():
        meta = sample.metadata.get(symbol)
        if not meta:
            continue
        rows.append(
            (
                sample.provider,
                symbol,
                sample.captured_at,
                db_symbol,
                meta["strike"],
                meta["expiration"],
                meta["option_type"],
                q.bid,
                q.ask,
                q.last,
                q.effective_mid(),
                q.bid_size,
                q.ask_size,
                q.volume,
                q.open_interest,
                q.implied_volatility,
                q.timestamp,
            )
        )
    if not rows:
        return 0
    try:
        from psycopg2.extras import execute_values

        from src.database import db_connection

        with db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO option_chains_shadow (
                        provider, option_symbol, captured_at, underlying,
                        strike, expiration, option_type,
                        bid, ask, last, mid, bid_size, ask_size,
                        volume, open_interest, implied_volatility,
                        quote_timestamp
                    ) VALUES %s
                    ON CONFLICT (provider, option_symbol, captured_at)
                    DO NOTHING
                    """,
                    rows,
                )
        return len(rows)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "shadow persist failed for %s (is shadow_tables.sql applied?): %s",
            sample.provider,
            e,
        )
        return 0


def persist_underlying_bar(sample: FeedSample, underlying: str) -> int:
    """Write one feed's spot bar to ``underlying_quotes_shadow``.

    Separate from :func:`persist_sample` because the two answer different
    questions. The chain table explains a wall that moved; this one
    explains whether the feeds even agreed on the price the walls were
    measured against, which is the first thing to check and the thing you
    cannot reconstruct afterwards.

    Never raises: losing the tape must not abort a run that is still
    producing usable chain comparisons.
    """
    bar = sample.spot_bar
    if bar is None:
        return 0
    try:
        from src.database import db_connection

        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO underlying_quotes_shadow (
                        provider, symbol, captured_at, bar_timestamp,
                        open, high, low, close, volume, up_volume, down_volume
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (provider, symbol, captured_at) DO NOTHING
                    """,
                    (
                        sample.provider,
                        get_canonical_symbol(underlying),
                        sample.captured_at,
                        bar.timestamp,
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.volume,
                        # None stays None: a feed that cannot report a signed
                        # split is a different fact from one reporting zero,
                        # and the column is nullable precisely to hold that.
                        bar.up_volume,
                        bar.down_volume,
                    ),
                )
        return 1
    except Exception as e:  # noqa: BLE001
        logger.warning("underlying shadow persist failed for %s: %s", sample.provider, e)
        return 0


def persist_comparison(
    comparisons: Sequence[MetricComparison],
    *,
    underlying: str,
    incumbent: FeedSample,
    candidate: FeedSample,
) -> int:
    """Write one comparison run to ``feed_comparisons``. Never raises."""
    rows = [
        (
            candidate.captured_at,
            get_canonical_symbol(underlying),
            incumbent.provider,
            candidate.provider,
            c.metric,
            c.incumbent,
            c.candidate,
            None if c.pct_diff in (None, float("inf")) else c.pct_diff,
            c.within_tolerance,
            c.note or None,
            incumbent.contract_count,
            candidate.contract_count,
            incumbent.oi_count,
            candidate.oi_count,
        )
        for c in comparisons
    ]
    try:
        from psycopg2.extras import execute_values

        from src.database import db_connection

        with db_connection() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO feed_comparisons (
                        captured_at, underlying, incumbent, candidate, metric,
                        incumbent_value, candidate_value, pct_diff,
                        within_tolerance, note,
                        incumbent_contracts, candidate_contracts,
                        incumbent_oi_contracts, candidate_oi_contracts
                    ) VALUES %s
                    """,
                    rows,
                )
        return len(rows)
    except Exception as e:  # noqa: BLE001
        logger.warning("comparison persist failed: %s", e)
        return 0


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


def run_once(
    incumbent_provider: MarketDataProvider,
    candidate_provider: MarketDataProvider,
    underlying: str,
    *,
    num_expirations: int,
    strike_count_max: int,
    strike_pct_range: float,
    persist: bool,
    price_tolerance_pct: float,
    exposure_tolerance_pct: float,
) -> Dict[str, Any]:
    """One paired sample plus its analytics diff."""
    incumbent = sample_provider(
        incumbent_provider,
        underlying,
        num_expirations=num_expirations,
        strike_count_max=strike_count_max,
        strike_pct_range=strike_pct_range,
    )
    # Reuse the incumbent's spot so both feeds are compared over the SAME
    # strike window. Letting each pick its own spot would let a small
    # price difference select different contracts, and the resulting
    # divergence would be an artefact of the harness, not the feed.
    candidate = sample_provider(
        candidate_provider,
        underlying,
        num_expirations=num_expirations,
        strike_count_max=strike_count_max,
        strike_pct_range=strike_pct_range,
        spot_hint=incumbent.spot,
    )

    now = datetime.now(timezone.utc)
    analytics: Dict[str, Dict[str, Optional[float]]] = {}
    for sample in (incumbent, candidate):
        if sample.error or not sample.spot:
            analytics[sample.provider] = {m: None for m in _METRICS}
            continue
        rows = _quotes_to_option_rows(sample.quotes, sample.metadata)
        enriched = _enrich(rows, sample.spot, underlying)
        analytics[sample.provider] = _compute_analytics(enriched, sample.spot, underlying, now)

    comparisons = compare_metrics(
        analytics[incumbent.provider],
        analytics[candidate.provider],
        price_tolerance_pct=price_tolerance_pct,
        exposure_tolerance_pct=exposure_tolerance_pct,
    )

    if persist:
        persist_sample(incumbent, underlying)
        persist_sample(candidate, underlying)
        persist_underlying_bar(incumbent, underlying)
        persist_underlying_bar(candidate, underlying)
        persist_comparison(
            comparisons,
            underlying=underlying,
            incumbent=incumbent,
            candidate=candidate,
        )

    return {
        "captured_at": candidate.captured_at.isoformat(),
        "underlying": underlying,
        "incumbent": {
            "provider": incumbent.provider,
            "contracts": incumbent.contract_count,
            "two_sided": incumbent.quoted_count,
            "with_oi": incumbent.oi_count,
            "error": incumbent.error,
        },
        "candidate": {
            "provider": candidate.provider,
            "contracts": candidate.contract_count,
            "two_sided": candidate.quoted_count,
            "with_oi": candidate.oi_count,
            "error": candidate.error,
        },
        "comparisons": [asdict(c) for c in comparisons],
        "verdict": _verdict(comparisons),
    }


def _verdict(comparisons: Sequence[MetricComparison]) -> str:
    """One-word summary: agree, diverge, or incomparable."""
    evaluated = [c for c in comparisons if c.within_tolerance is not None]
    if not evaluated:
        return "incomparable"
    return "agree" if all(c.within_tolerance for c in evaluated) else "diverge"


def _print_report(result: Dict[str, Any]) -> None:
    inc, cand = result["incumbent"], result["candidate"]
    print(f"\n{result['underlying']}  @  {result['captured_at']}")
    print(
        f"  incumbent {inc['provider']:<14} contracts={inc['contracts']:<6} "
        f"two-sided={inc['two_sided']:<6} with-OI={inc['with_oi']:<6}"
        + (f"  ERROR: {inc['error']}" if inc["error"] else "")
    )
    print(
        f"  candidate {cand['provider']:<14} contracts={cand['contracts']:<6} "
        f"two-sided={cand['two_sided']:<6} with-OI={cand['with_oi']:<6}"
        + (f"  ERROR: {cand['error']}" if cand["error"] else "")
    )
    print(f"\n  {'metric':<12} {'incumbent':>14} {'candidate':>14} " f"{'diff':>10} {'':>6}")
    print("  " + "-" * 62)
    for c in result["comparisons"]:
        print("  " + MetricComparison(**c).as_row())
    print(f"\n  verdict: {result['verdict']}\n")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compare two market data providers through the same analytics.",
    )
    parser.add_argument("--underlying", default="SPY")
    parser.add_argument(
        "--incumbent",
        default=None,
        help="incumbent provider name (default: MARKET_DATA_PROVIDER)",
    )
    parser.add_argument(
        "--candidate",
        default=None,
        help=(
            "candidate provider name (default: MARKET_DATA_COMPARE_PROVIDER). "
            "Must already be implemented and registered in "
            "src/ingestion/providers/__init__.py -- a vendor you have "
            "credentials for but no provider module for is not yet comparable."
        ),
    )
    parser.add_argument("--expirations", type=int, default=3)
    parser.add_argument("--strike-count-max", type=int, default=40)
    parser.add_argument("--strike-pct-range", type=float, default=3.0)
    parser.add_argument(
        "--duration-minutes",
        type=float,
        default=0.0,
        help="repeat for this long; 0 runs a single comparison",
    )
    parser.add_argument("--interval-seconds", type=float, default=60.0)
    parser.add_argument(
        "--persist",
        action="store_true",
        help="write both feeds to the shadow tables (see setup/database/shadow_tables.sql)",
    )
    parser.add_argument("--price-tolerance-pct", type=float, default=_DEFAULT_PRICE_TOLERANCE_PCT)
    parser.add_argument(
        "--exposure-tolerance-pct", type=float, default=_DEFAULT_EXPOSURE_TOLERANCE_PCT
    )
    parser.add_argument("--json", action="store_true", help="emit JSON, one object per sample")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    from src.config import MARKET_DATA_COMPARE_PROVIDER, MARKET_DATA_PROVIDER

    incumbent_name = args.incumbent or MARKET_DATA_PROVIDER
    candidate_name = args.candidate or MARKET_DATA_COMPARE_PROVIDER
    if not candidate_name:
        parser.error(
            "no candidate provider: pass --candidate or set " "MARKET_DATA_COMPARE_PROVIDER"
        )
    if candidate_name == incumbent_name:
        parser.error(
            f"incumbent and candidate are both {incumbent_name!r}; "
            "a feed compared against itself proves nothing"
        )

    incumbent_provider = get_provider(incumbent_name)
    try:
        candidate_provider = get_provider(candidate_name)
    except ValueError as e:
        # The registry is deliberately fatal on an unknown name, but the
        # common case here is "I have trial credentials for a vendor nobody
        # has written a module for yet", which is a build step rather than a
        # typo. Say which.
        print(
            f"{e}\n\n"
            f"If {candidate_name!r} is a vendor you have credentials for, its "
            "provider module does not exist yet. Copy "
            "src/ingestion/providers/stub.py, implement the six operations "
            "against their client, and register it in "
            "src/ingestion/providers/__init__.py. See "
            "docs/design/market-data-provider-abstraction.md.",
            file=sys.stderr,
        )
        return 2

    # Fail loudly on a candidate that cannot serve the option chain at all,
    # rather than reporting a run of empty comparisons that reads like a
    # transient outage.
    if not candidate_provider.capabilities.option_quotes:
        print(
            f"candidate {candidate_name!r} declares option_quotes=False; "
            "it cannot back the chain and there is nothing to compare.",
            file=sys.stderr,
        )
        return 2

    deadline = time.monotonic() + args.duration_minutes * 60
    exit_code = 0
    try:
        while True:
            result = run_once(
                incumbent_provider,
                candidate_provider,
                args.underlying,
                num_expirations=args.expirations,
                strike_count_max=args.strike_count_max,
                strike_pct_range=args.strike_pct_range,
                persist=args.persist,
                price_tolerance_pct=args.price_tolerance_pct,
                exposure_tolerance_pct=args.exposure_tolerance_pct,
            )
            if args.json:
                print(json.dumps(result, default=str))
            else:
                _print_report(result)
            if result["verdict"] == "diverge":
                exit_code = 1
            if time.monotonic() >= deadline:
                break
            time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
    finally:
        incumbent_provider.close()
        candidate_provider.close()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
