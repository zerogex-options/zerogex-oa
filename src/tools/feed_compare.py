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
import contextlib
import logging
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.ingestion.providers import MarketDataProvider, get_provider
from src.ingestion.providers.base import Bar, OptionQuote
from src.symbols import get_canonical_symbol, is_cash_index
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

#: Metrics whose value is a strike, so the smallest possible non-zero
#: difference is one strike increment. A percentage tolerance cannot express
#: that: SPY strikes are $1 apart around $765, so ONE strike is 0.13% and no
#: adjacent-strike disagreement can ever fall inside a 0.05% band. The
#: percentage is still reported, but the judgement and the note are made in
#: strikes, where "adjacent" and "nowhere near" stop looking identical.
_STRIKE_QUANTISED = ("call_wall", "put_wall", "max_pain")


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
    as_of: datetime,
    *,
    keep_vendor_iv: bool = True,
) -> List[Dict[str, Any]]:
    """Shape normalised quotes into the dicts the analytics engine expects.

    ``metadata`` supplies strike / expiration / option_type per contract,
    which the provider's chain discovery already resolved. Contracts with
    no metadata are dropped rather than guessed: a mis-parsed strike would
    silently move a wall.

    ``as_of`` is stamped on every row and is REQUIRED: ``GreeksCalculator``
    treats a missing ``timestamp`` as a missing required field, returns
    ``None`` for every Greek, and ``_calculate_gex_by_strike`` then fails on
    ``None * open_interest``. Both feeds are stamped with the same instant
    so time-to-expiry is identical across them -- a per-feed clock would
    put a difference in the Greeks that belongs to the harness, not the
    vendor.

    ``keep_vendor_iv=False`` discards a vendor-supplied implied vol so the
    solver runs for both feeds. That matters because the feeds are not
    symmetric: TradeStation ships IV on the quote and ThetaData sells it
    separately (and this deployment does not buy it), so passing vendor IV
    through means one side uses the vendor's surface and the other solves
    from mid. A divergence then cannot be attributed to the quotes.
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
                "implied_volatility": q.implied_volatility if keep_vendor_iv else None,
                "timestamp": as_of,
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
    dropped = 0
    for row in rows:
        try:
            out = calculator.enrich_option_data(dict(row), spot)
        except Exception as e:  # noqa: BLE001 - one bad contract must not
            # abort the comparison; note it and continue.
            logger.debug("enrichment failed for %s: %s", row.get("option_symbol"), e)
            continue
        # enrich_option_data does NOT raise on a missing required field: it
        # returns the row with every Greek set to None. Passing that on
        # kills the whole comparison downstream, where the GEX sum
        # multiplies gamma by open interest. Drop the contract instead --
        # one unpriceable strike is a gap, not a reason to lose the run.
        if out.get("gamma") is None:
            dropped += 1
            continue
        enriched.append(out)
    if dropped:
        logger.warning(
            "%d of %d contracts produced no Greeks and were dropped "
            "(they cannot contribute to GEX)",
            dropped,
            len(rows),
        )
    return enriched


@contextlib.contextmanager
def _preserve_signal_handlers():
    """Keep Ctrl-C working across an ``AnalyticsEngine`` construction.

    The engine is a daemon in production and installs its own SIGINT and
    SIGTERM handlers in ``__init__`` (main_engine.py). Those set
    ``running = False`` on the engine instance -- correct for the daemon,
    useless here, where the engine is built and discarded once per sample.
    The effect is that Ctrl-C is swallowed: the handler runs, the loop never
    sees KeyboardInterrupt, and the only way to stop a run is to kill it
    from another shell. Observed on a live comparison 2026-09-15.

    Saving and restoring around construction leaves the engine untouched
    and gives the signal back to whoever owned it.
    """
    saved = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            saved[sig] = signal.getsignal(sig)
        except (ValueError, OSError):  # pragma: no cover - platform dependent
            pass
    try:
        yield
    finally:
        for sig, handler in saved.items():
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):  # pragma: no cover
                pass


def _compute_analytics(
    rows: List[Dict[str, Any]],
    spot: float,
    underlying: str,
    now: datetime,
    *,
    label: str = "feed",
) -> Dict[str, Optional[float]]:
    """Compute the published metrics from one feed's contracts.

    Calls the same ``AnalyticsEngine`` and ``walls`` code the API serves
    from, so a difference here is a difference a subscriber would see.

    ``label`` names the feed in the unresolved-flip diagnostic below.
    """
    from src.analytics.main_engine import AnalyticsEngine
    from src.analytics.walls import compute_call_put_walls

    # See _preserve_signal_handlers: constructing the engine steals SIGINT.

    out: Dict[str, Optional[float]] = {m: None for m in _METRICS}
    out["spot"] = spot
    if not rows:
        return out

    with _preserve_signal_handlers():

        engine = AnalyticsEngine(underlying=underlying)

    gex_by_strike = engine._calculate_gex_by_strike(rows, spot, now)
    if gex_by_strike:
        out["net_gex"] = float(sum(r.get("net_gex", 0.0) for r in gex_by_strike))
        call_wall, put_wall = compute_call_put_walls(gex_by_strike, spot)
        out["call_wall"] = call_wall
        out["put_wall"] = put_wall

    profile, flip, span = engine._resolve_gamma_flip(rows, spot, now)
    out["gamma_flip"] = flip
    if flip is None:
        # A NULL flip is the engine's honest "no actionable crossing", and
        # the comparison prints it as a bare "-" on both feeds -- which is
        # indistinguishable from the two feeds agreeing. The engine already
        # builds the diagnostic that separates the four documented causes
        # (IV spike / 0DTE-dominant chain / stale IV at the 0.20 default /
        # one-sided chain); surface it here so an unresolved run says why
        # instead of leaving it to be re-derived from the chain by hand.
        diag = engine._gamma_flip_unresolved_diagnostics(rows, profile, spot, now)
        logger.warning(
            "%s: gamma_flip unresolved through span=%.2f -- "
            "usable=%d (calls=%d puts=%d) profile pos/neg/zero=%d/%d/%d "
            "peak=%.3g reference=%.3g floor=%.3g "
            "iv p50=%.3f p90=%.3f max=%.3f at_default=%.0f%% "
            "oi_share 0dte=%.0f%% weighted 0dte=%.0f%%",
            label,
            span,
            diag["usable_total"],
            diag["usable_calls"],
            diag["usable_puts"],
            diag["profile_pos_pts"],
            diag["profile_neg_pts"],
            diag["profile_zero_pts"],
            diag["profile_peak"],
            diag["profile_reference"],
            diag["structural_floor"],
            diag["iv_p50"],
            diag["iv_p90"],
            diag["iv_max"],
            100.0 * diag["iv_at_default_share"],
            100.0 * diag["oi_share_0dte"],
            100.0 * diag["weighted_oi_share_0dte"],
        )

    # Max pain partitions by expiration internally; compare the nearest
    # expiration, which is what the headline figure tracks.
    by_expiry = engine._calculate_max_pain_by_expiration(rows)
    if by_expiry:
        nearest = min(by_expiry)
        out["max_pain"] = by_expiry[nearest]

    return out


def infer_strike_increment(metadata: Dict[str, Dict[str, Any]]) -> Optional[float]:
    """The ladder spacing for this chain, from the strikes actually sampled.

    The modal gap rather than the minimum: a chain routinely mixes $1
    strikes near the money with $5 out in the wings, and the near-money
    spacing is the one the walls and max pain land on.
    """
    strikes = sorted({float(m["strike"]) for m in metadata.values() if m.get("strike")})
    if len(strikes) < 2:
        return None
    gaps: Dict[float, int] = {}
    for lo, hi in zip(strikes, strikes[1:]):
        gap = round(hi - lo, 4)
        if gap > 0:
            gaps[gap] = gaps.get(gap, 0) + 1
    if not gaps:
        return None
    return max(gaps.items(), key=lambda kv: kv[1])[0]


def compare_metrics(
    incumbent: Dict[str, Optional[float]],
    candidate: Dict[str, Optional[float]],
    *,
    price_tolerance_pct: float = _DEFAULT_PRICE_TOLERANCE_PCT,
    exposure_tolerance_pct: float = _DEFAULT_EXPOSURE_TOLERANCE_PCT,
    strike_increment: Optional[float] = None,
) -> List[MetricComparison]:
    """Diff two analytics dicts into a reportable comparison.

    ``strike_increment`` lets the strike-quantised metrics be described in
    strikes. Without it they are judged on percentage, which for a $1 ladder
    means every adjacent-strike disagreement reads as a tolerance failure
    indistinguishable from a wall fifty strikes away.
    """
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
        if metric in _STRIKE_QUANTISED and strike_increment and a is not None and b is not None:
            steps = abs(b - a) / strike_increment
            within = steps < 0.5
            if not within:
                note = (note + " " if note else "") + (
                    f"{steps:.0f} strike" + ("s" if round(steps) != 1 else "") + " apart"
                )
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
    #: Seconds per phase. Discovery (expirations + strikes) runs once at
    #: startup and on strike recalibration; the chain fetch runs every poll.
    #: Reporting one total conflates a one-off cost with the recurring one,
    #: which is the only number that has to fit inside the poll interval.
    timings: Dict[str, float] = field(default_factory=dict)

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
        spot_started = time.monotonic()
        if spot is None:
            spot_bar = _spot_from_provider(provider, underlying)
            spot = float(spot_bar.close) if spot_bar and spot_bar.close else None
        spot_seconds = time.monotonic() - spot_started
        if not spot or spot <= 0:
            return FeedSample(
                provider=provider.name,
                captured_at=captured_at,
                spot=None,
                quotes={},
                metadata={},
                error="no spot price available",
            )
        discovery_started = time.monotonic()
        symbols, metadata = _resolve_chain(
            provider,
            underlying,
            num_expirations=num_expirations,
            strike_count_max=strike_count_max,
            strike_pct_range=strike_pct_range,
            spot=spot,
        )
        discovery = time.monotonic() - discovery_started

        chain_started = time.monotonic()
        quotes = provider.snapshot_option_quotes(symbols)
        chain = time.monotonic() - chain_started

        return FeedSample(
            provider=provider.name,
            captured_at=captured_at,
            spot=spot,
            quotes=quotes,
            metadata=metadata,
            spot_bar=spot_bar,
            timings={
                "spot": round(spot_seconds, 2),
                "discovery": round(discovery, 2),
                "chain": round(chain, 2),
            },
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


def _is_index_underlying(underlying: str, canonical: str) -> bool:
    """True when this symbol's spot comes from an index feed, not an equity one.

    A cash index and an ETF are different products on different feeds, and
    asking the wrong one does not degrade gracefully -- it returns nothing.
    ThetaData's equity endpoint defaults to Nasdaq Basic, which has never
    heard of SPX.

    ``is_cash_index`` is the codebase's source of truth, but it keys on the
    canonical symbol, which needs ``SYMBOL_ALIASES`` configured. The
    ``$XXX.X`` form is TradeStation's own index notation and is unambiguous
    without any config, so it stands in as a structural fallback: a probe
    run against an unconfigured checkout should still reach the right feed.
    """
    if is_cash_index(canonical):
        return True
    raw = (underlying or "").strip().upper()
    return raw.startswith("$") and raw.endswith(".X")


def _detect_mangled_index_symbol(underlying: str) -> Optional[str]:
    """Explain a symbol that make ate the ``$`` from, or return ``None``.

    Index symbols are spelled ``$SPXW.X``, and ``make feed-probe
    UNDERLYING='$SPXW.X'`` silently delivers ``PXW.X``: make expands ``$S``
    as an empty variable of its own before the recipe ever runs. The result
    is a symbol that simply does not exist, whose only previous symptom was
    a half-minute wait and "no spot price available".

    A trailing ``.X`` with no leading ``$`` is not a real ticker in any
    convention this codebase uses, so it is safe to reject outright.
    """
    raw = (underlying or "").strip()
    if not raw.upper().endswith(".X") or raw.startswith("$"):
        return None
    return (
        f"{raw!r} is not a symbol. It looks like an index symbol that make "
        f"truncated: make reads '$X' as a variable of its own and expands it "
        f"to nothing, so '$SPXW.X' arrives here as 'PXW.X'. The original "
        f"cannot be reconstructed from what survived, so pass it again, "
        f"through the environment:\n"
        f"    UNDERLYING='$SPXW.X' make feed-probe PROVIDER=<name>\n"
        f"or double the '$' in a make assignment: UNDERLYING='$$SPXW.X'."
    )


def _spot_from_provider(provider: MarketDataProvider, underlying: str) -> Optional[Bar]:
    """Best available spot bar for ``underlying`` from this provider.

    Briefly runs a bar stream rather than assuming a quote endpoint,
    because that is the path a migration actually depends on. Returns the
    whole bar rather than just the close so the caller can persist the tape
    it priced against.

    Routes indices to the index feed. Sending SPX to the equity endpoint
    returns nothing at all, which surfaces as "no spot price available" and
    aborts the whole comparison -- for two of the four production
    underlyings.
    """
    canonical = get_canonical_symbol(underlying)
    caps = provider.capabilities

    if _is_index_underlying(underlying, canonical):
        if not caps.index_bars:
            return None
        stream = provider.stream_index_bars(canonical, db_symbol=canonical)
    else:
        if not caps.underlying_bars:
            return None
        stream = provider.stream_underlying_bars(underlying, db_symbol=canonical)

    try:
        stream.start()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            bar = stream.drain()
            if bar and bar.close:
                return bar
            # A poll that is failing for a structural reason -- wrong
            # endpoint, no entitlement -- will still be failing in twenty
            # seconds. Waiting out the deadline turns a one-line diagnosis
            # into a half-minute of silence.
            error = getattr(stream, "last_error", None)
            if error:
                raise RuntimeError(f"{underlying} spot unavailable: {error}")
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
    keep_vendor_iv: bool = True,
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
        rows = _quotes_to_option_rows(
            sample.quotes, sample.metadata, now, keep_vendor_iv=keep_vendor_iv
        )
        enriched = _enrich(rows, sample.spot, underlying)
        if rows and not enriched:
            # Every contract failed to price. Left alone this yields None for
            # each chain metric while spot still matches, which reads as
            # agreement. Say so instead.
            logger.error(
                "%s: all %d contracts failed to price; no chain metric can be "
                "computed from this sample",
                sample.provider,
                len(rows),
            )
            analytics[sample.provider] = {m: None for m in _METRICS}
            analytics[sample.provider]["spot"] = sample.spot
            continue
        analytics[sample.provider] = _compute_analytics(
            enriched, sample.spot, underlying, now, label=sample.provider
        )

    comparisons = compare_metrics(
        analytics[incumbent.provider],
        analytics[candidate.provider],
        price_tolerance_pct=price_tolerance_pct,
        exposure_tolerance_pct=exposure_tolerance_pct,
        # Taken from the incumbent, which defines the contract set both
        # feeds were asked for.
        strike_increment=infer_strike_increment(incumbent.metadata),
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


def probe(
    provider: MarketDataProvider,
    underlying: str,
    *,
    num_expirations: int,
    strike_count_max: int,
    strike_pct_range: float,
) -> Dict[str, Any]:
    """One timed fetch, so the cost of a real run is measured not guessed.

    Deliberately skips analytics and persistence. The question a probe
    answers is "what does one cycle cost me in seconds and coverage", and
    the Greeks pass would dominate the timing while telling you nothing
    about the vendor.

    The number to watch is wall time. The polling streams issue their calls
    sequentially, so a chain poll has to finish inside
    ``THETADATA_POLL_SECONDS`` or cycles start overlapping. If this comes
    back slow, raise the poll interval or narrow the strike range before
    pointing production at it.
    """
    started = time.monotonic()
    sample = sample_provider(
        provider,
        underlying,
        num_expirations=num_expirations,
        strike_count_max=strike_count_max,
        strike_pct_range=strike_pct_range,
    )
    elapsed = time.monotonic() - started

    # Providers whose response columns are server-supplied can report what
    # they actually saw. Without this a wrong column guess and a closed
    # market both print "contracts returned 0", and you cannot tell which.
    columns: Dict[str, Any] = {}
    describe = getattr(provider, "describe_columns", None)
    if callable(describe):
        try:
            columns = describe(underlying)
        except Exception as e:  # noqa: BLE001 - a diagnostic never fails a probe
            columns = {"error": {"detail": f"{type(e).__name__}: {e}"}}

    return {
        "provider": provider.name,
        "underlying": underlying,
        "seconds": round(elapsed, 2),
        "spot": sample.spot,
        "contracts_requested": len(sample.metadata),
        "contracts_returned": sample.contract_count,
        "two_sided": sample.quoted_count,
        "with_open_interest": sample.oi_count,
        "error": sample.error,
        "timings": sample.timings,
        "columns": columns,
    }


def _print_probe(result: Dict[str, Any]) -> None:
    print(f"\n  provider           {result['provider']}")
    print(f"  underlying         {result['underlying']}")
    print(f"  spot               {result['spot']}")
    timings = result.get("timings") or {}
    print(f"  wall time          {result['seconds']}s")
    if timings:
        # Only the chain fetch repeats every poll; the rest is startup.
        print(
            f"    spot {timings.get('spot', 0)}s"
            f"  + discovery {timings.get('discovery', 0)}s  (once, at startup)"
        )
        print(
            f"    chain {timings.get('chain', 0)}s"
            f"  <- this is the per-poll cost; must fit THETADATA_POLL_SECONDS"
        )
    print(f"  contracts asked    {result['contracts_requested']}")
    print(f"  contracts returned {result['contracts_returned']}")
    print(f"  two-sided quotes   {result['two_sided']}")
    print(f"  with open interest {result['with_open_interest']}")
    if result["error"]:
        print(f"  ERROR              {result['error']}")
    elif result["contracts_requested"]:
        coverage = result["contracts_returned"] / result["contracts_requested"]
        print(f"  coverage           {coverage:.1%}")
    columns = dict(result.get("columns") or {})
    # Not a response shape: an inventory of which Market Value endpoints the
    # installed client wraps. Printed on its own so it is not mistaken for
    # a call that returned no rows.
    inventory = columns.pop("market_value_endpoints", None)
    if inventory:
        print("\n  --- market value endpoints on this client ---")
        for name in sorted(inventory):
            print(f"  {name:<34} {inventory[name]}")
    if columns:
        print("\n  --- raw response columns (paste this back) ---")
        for endpoint in sorted(columns):
            info = columns[endpoint]
            if "error" in info:
                print(f"  {endpoint:<22} ERROR {info['error']}")
                continue
            print(f"  {endpoint:<22} rows={info.get('rows', 0)}")
            print(f"  {'':<22} {info.get('columns')}")
            sample = info.get("sample") or {}
            if sample:
                print(f"  {'':<22} sample={sample}")
    print()


#: Metrics that come from the option chain. `spot` is deliberately excluded:
#: it is read from a bar, not derived from the chain, so it agrees even when
#: every contract failed to price.
_CHAIN_METRICS = tuple(m for m in _METRICS if m != "spot")


def summarise_run(results: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Each feed's variability against the gap between them.

    A single sample cannot tell a real disagreement from ordinary jitter,
    and a run of them is unreadable by eye. The number that decides a
    migration is not how far the feeds sit apart -- it is how that distance
    compares to how far each feed moves from itself minute to minute.

    A feed whose own consecutive samples span 9% cannot be used to convict
    another feed of a 6% difference. Reporting the two side by side makes
    that visible instead of leaving it to be worked out by hand.
    """
    per_metric: Dict[str, Dict[str, Any]] = {}
    for metric in _METRICS:
        inc: List[float] = []
        cand: List[float] = []
        gaps: List[float] = []
        for r in results:
            for c in r.get("comparisons", []):
                if c.get("metric") != metric:
                    continue
                a, b, pct = c.get("incumbent"), c.get("candidate"), c.get("pct_diff")
                if a is not None:
                    inc.append(a)
                if b is not None:
                    cand.append(b)
                if pct is not None and pct not in (float("inf"), float("-inf")):
                    gaps.append(abs(pct))

        def _span_pct(values: List[float]) -> Optional[float]:
            if len(values) < 2:
                return None
            mean = sum(values) / len(values)
            if mean == 0:
                return None
            return (max(values) - min(values)) / abs(mean) * 100.0

        per_metric[metric] = {
            "incumbent_self_span_pct": _span_pct(inc),
            "candidate_self_span_pct": _span_pct(cand),
            "max_cross_feed_pct": max(gaps) if gaps else None,
            "samples": len(results),
        }
    return per_metric


def _print_summary(summary: Dict[str, Any], incumbent: str, candidate: str) -> None:
    samples = next((v["samples"] for v in summary.values()), 0)
    if samples < 2:
        return
    print(f"\n  === across {samples} samples ===")
    print(
        f"  {'metric':<12} {incumbent[:12] + ' self':>19} "
        f"{candidate[:12] + ' self':>19} {'feeds apart':>13}"
    )
    print("  " + "-" * 66)
    for metric, row in summary.items():

        def fmt(v):
            return "-" if v is None else f"{v:.2f}%"

        print(
            f"  {metric:<12} {fmt(row['incumbent_self_span_pct']):>19} "
            f"{fmt(row['candidate_self_span_pct']):>19} "
            f"{fmt(row['max_cross_feed_pct']):>13}"
        )
    noisy = [
        m
        for m, r in summary.items()
        if r["incumbent_self_span_pct"] is not None
        and r["max_cross_feed_pct"] is not None
        and r["incumbent_self_span_pct"] > r["max_cross_feed_pct"]
    ]
    if noisy:
        print(
            f"\n  NOTE: on {', '.join(noisy)} the incumbent moved further from\n"
            f"  ITSELF between samples than the two feeds ever differed. A\n"
            f"  divergence smaller than that is not evidence about the candidate."
        )
    print()


def _verdict(comparisons: Sequence[MetricComparison]) -> str:
    """One-word summary: agree, diverge, or incomparable.

    "agree" requires at least one CHAIN metric to have been evaluated, not
    merely one metric. Spot comes from a bar rather than from the contracts,
    so a run in which every contract failed to price still matches on spot --
    and would otherwise report "agree" while having compared nothing that
    matters. A crash is recoverable; a confident false agreement is what
    gets a migration signed off on no evidence.
    """
    evaluated = [c for c in comparisons if c.within_tolerance is not None]
    if not evaluated:
        return "incomparable"
    if not any(c.metric in _CHAIN_METRICS for c in evaluated):
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
    parser.add_argument(
        "--probe",
        action="store_true",
        help=(
            "measure ONE fetch per provider and exit: wall time and coverage, "
            "with no analytics and no database writes. Run this before the "
            "first real comparison to size the load."
        ),
    )
    parser.add_argument(
        "--solve-iv-both",
        action="store_true",
        help=(
            "discard vendor-supplied implied vol and solve it from price for "
            "BOTH feeds. The feeds are not symmetric -- TradeStation ships IV "
            "on the quote, ThetaData sells it separately -- so by default one "
            "side uses the vendor's surface and the other solves. Use this to "
            "tell a quote difference apart from an IV-source difference when a "
            "run reports 'diverge'."
        ),
    )
    parser.add_argument("--json", action="store_true", help="emit JSON, one object per sample")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args(argv)

    mangled = _detect_mangled_index_symbol(args.underlying)
    if mangled:
        parser.error(mangled)

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    from src.config import MARKET_DATA_COMPARE_PROVIDER, MARKET_DATA_PROVIDER

    incumbent_name = args.incumbent or MARKET_DATA_PROVIDER
    candidate_name = args.candidate or MARKET_DATA_COMPARE_PROVIDER
    if not candidate_name and not args.probe:
        parser.error(
            "no candidate provider: pass --candidate or set " "MARKET_DATA_COMPARE_PROVIDER"
        )
    if candidate_name and candidate_name == incumbent_name and not args.probe:
        parser.error(
            f"incumbent and candidate are both {incumbent_name!r}; "
            "a feed compared against itself proves nothing"
        )

    if args.probe:
        names = [n for n in (incumbent_name, candidate_name) if n]
        exit_code = 0
        for name in names:
            try:
                provider = get_provider(name)
            except ValueError as e:
                print(f"{name}: {e}", file=sys.stderr)
                exit_code = 2
                continue
            try:
                result = probe(
                    provider,
                    args.underlying,
                    num_expirations=args.expirations,
                    strike_count_max=args.strike_count_max,
                    strike_pct_range=args.strike_pct_range,
                )
            finally:
                provider.close()
            if args.json:
                print(json.dumps(result, default=str))
            else:
                _print_probe(result)
            if result["error"]:
                exit_code = 1
        return exit_code

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
    collected: List[Dict[str, Any]] = []
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
                keep_vendor_iv=not args.solve_iv_both,
            )
            collected.append(result)
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
    # Printed even after an interrupt: a run stopped early still carries the
    # samples it took, and those are what the decision rests on.
    if collected and not args.json:
        _print_summary(summarise_run(collected), incumbent_name, candidate_name)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
