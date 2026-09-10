"""Quoted-spread / liquidity statistics for an option chain snapshot.

Every other analytics module in this package asks what the book *means*:
where dealer gamma sits, which strike pins, how the surface shifted.  This
one asks a question none of them can answer — **can you actually get filled
in it?**  A gamma wall three points away is worth nothing to a trader whose
put is quoted 12.40 x 15.80.

The inputs are the ``bid`` / ``ask`` columns ZeroGEX already ingests on
every ``option_chains`` row.  Nothing new is fetched; the whole module is a
reduction over quotes that were always there but never summarized.

--------------------------------------------------------------------------
The four things measured, and why each is separate
--------------------------------------------------------------------------

**1. Width in dollars** (:attr:`ContractSpread.spread`) — ``ask - bid``.  The
literal toll for crossing, per share; multiply by the contract multiplier
for the per-contract cost.  It is the only unit a fill is actually paid in,
and it is useless for comparison: a $0.40 wide SPX put and a $0.40 wide SPY
put are different worlds.

**2. Width as a share of the premium** (``relative_spread_pct``) —
``100 * (ask - bid) / mid``.  "What fraction of what I pay is the toll."
This is the headline number, because it is what makes a cheap option
untradeable: a put quoted 0.05 x 0.35 is 150% wide, and no edge survives
that.  It is comparable across strikes within a chain, and roughly
comparable across products.

**3. Width in basis points of the underlying** (``spread_bps_underlying``) —
``10_000 * (ask - bid) / spot``.  The cross-product normalizer.  SPX trades
near 6,800 and NDX near 25,000, so their dollar widths are not on the same
scale and never will be; expressed against the index level they are.  This
is the number that answers "is NDX worse than SPX today, or just bigger?"

**4. Whether a two-sided market exists at all** (:class:`QuoteState`).  The
most severe liquidity failure does not show up in any width statistic,
because a width cannot be computed for it.  A contract quoted ``0.00 x
2.40`` has no bid: there is nothing to sell into, at any price.  Averaging
it in as "240% wide" would be a fabrication (the real answer is "no market
exists"), and dropping it silently would make a chain look *tighter* the
worse it got, since the untradeable contracts leave the sample.  So they are
counted, in their own bucket, and reported alongside the widths as coverage
percentages.  A chain whose median width is unchanged but whose zero-bid
share has doubled has deteriorated, and only the coverage number says so.

--------------------------------------------------------------------------
Why median and p90, never mean
--------------------------------------------------------------------------

Chain-wide quote quality is not normally distributed and has no upper bound.
One stale far-wing strike quoted 0.05 x 4.00 is 3900% wide, and a mean over
a few hundred contracts will happily report that one contract as if it were
the state of the chain.  The median says where the typical contract sits;
the p90 says how bad the bad ones are.  Both are needed and they answer
different questions — the complaint that starts "spreads are untradeable"
is usually a p90 observation, and the reply "looks fine to me" is usually a
median one.  Reporting both is what makes the disagreement resolvable.

--------------------------------------------------------------------------
What this module deliberately does NOT claim
--------------------------------------------------------------------------

* **Not effective spread.**  Effective spread compares fills to the midpoint
  at the time of the fill.  That needs per-trade prints with timestamps and
  an NBBO to measure against; ZeroGEX stores neither.  Everything here is
  *quoted* spread, and the surfaces that render it must say so.
* **Not depth.**  The feed carries no bid/ask sizes, so "1,000 up" and "1 up"
  at the same width are indistinguishable here.  A tight quote for one
  contract is not liquidity, and this module cannot tell you which you have.
* **Not a venue or routing statistic.**  These are consolidated quotes, so
  they describe the market, not any particular broker's execution.

Every function is pure: no DB, no clock, no ``market_calendar``.  The caller
supplies the rows and the spot price, which keeps the whole reduction
deterministic and unit-testable, and lets the same code serve the live API
snapshot and the historical backfill without a second implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

__all__ = [
    "QuoteState",
    "ContractSpread",
    "SpreadAggregate",
    "DEFAULT_MONEYNESS_EDGES",
    "percentile",
    "classify_quote",
    "contract_spread",
    "contract_spreads",
    "aggregate",
    "aggregate_by_option_type",
    "aggregate_by_moneyness",
    "aggregate_by_expiration",
    "moneyness_bucket_label",
    "percentile_rank",
]


class QuoteState(str, Enum):
    """The condition of a single contract's two-sided market.

    Ordered by severity, loosest first.  Only :attr:`TWO_SIDED` contracts
    contribute to any width statistic — see the module docstring for why the
    rest are counted rather than averaged in or dropped.
    """

    #: bid > 0 and ask > bid — a real market; the only state with a width.
    TWO_SIDED = "two_sided"
    #: ask > 0 but bid <= 0 — nothing to sell into. The "untradeable" state.
    ZERO_BID = "zero_bid"
    #: ask == bid > 0. Legal and transient, but not a market you work inside.
    LOCKED = "locked"
    #: ask < bid. Almost always stale data rather than a real arbitrage.
    CROSSED = "crossed"
    #: no usable ask at all (missing, null, or <= 0). Contract isn't quoted.
    NO_QUOTE = "no_quote"


#: Signed distance from spot, in percent, used to bucket the moneyness curve.
#: Negative is below spot, so for puts the leftmost buckets are the OTM wings
#: — the region the "index put spreads have gone bonkers" complaint is about.
#: Edges are bucket BOUNDARIES: n edges produce n-1 buckets.
DEFAULT_MONEYNESS_EDGES: Tuple[float, ...] = (
    -10.0, -5.0, -3.0, -1.5, -0.5, 0.5, 1.5, 3.0, 5.0, 10.0,
)


def _as_float(value: Any) -> Optional[float]:
    """Coerce a DB numeric / string / None to float, or None if unusable.

    asyncpg hands back ``Decimal`` for NUMERIC columns and psycopg2 can hand
    back ``str``; both paths feed this module, so the coercion lives here
    once rather than at each call site.
    """
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out or out in (float("inf"), float("-inf")):  # NaN / inf
        return None
    return out


def percentile(values: Sequence[float], pct: float) -> Optional[float]:
    """Linear-interpolated percentile of ``values`` (``pct`` in 0..100).

    Stdlib only and deliberately so: this runs inside the analytics engine's
    per-cycle path and inside the API request path, and neither should pull
    numpy in for a sort and a lerp.  Matches numpy's default ``linear``
    interpolation so a reading computed here and one computed in a notebook
    agree.
    """
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pct = max(0.0, min(100.0, pct))
    position = (len(ordered) - 1) * (pct / 100.0)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def percentile_rank(value: float, population: Sequence[float]) -> Optional[float]:
    """Where ``value`` sits within ``population``, as a 0-100 percentile.

    Used to answer "is today unusual?" against a trailing history: the share
    of past readings at or below today's.  Returns None for an empty
    population rather than a meaningless 0 or 50 — "no history yet" and "the
    tightest day on record" must not render identically.
    """
    if not population:
        return None
    at_or_below = sum(1 for p in population if p <= value)
    return round(100.0 * at_or_below / len(population), 1)


@dataclass(frozen=True)
class ContractSpread:
    """One contract's quote quality.

    The width fields are None for every state except
    :attr:`QuoteState.TWO_SIDED`, which is the type-level expression of the
    module's central rule: a contract without a two-sided market has no
    width, and inventing one for it is the mistake this whole module exists
    to avoid.
    """

    option_symbol: Optional[str]
    strike: float
    option_type: str
    expiration: Any
    state: QuoteState
    bid: Optional[float]
    ask: Optional[float]
    #: ask - bid, per share. Multiply by the multiplier for per-contract cost.
    spread: Optional[float] = None
    mid: Optional[float] = None
    #: 100 * spread / mid — the toll as a share of the premium.
    relative_spread_pct: Optional[float] = None
    #: 10_000 * spread / spot — comparable across SPX / NDX / SPY / QQQ.
    spread_bps_underlying: Optional[float] = None
    #: Signed % distance of the strike from spot; negative is below spot.
    moneyness_pct: Optional[float] = None
    open_interest: int = 0
    volume: int = 0

    @property
    def is_tradable(self) -> bool:
        return self.state is QuoteState.TWO_SIDED


def classify_quote(bid: Optional[float], ask: Optional[float]) -> QuoteState:
    """Classify a raw (bid, ask) pair into a :class:`QuoteState`.

    Order matters.  ``NO_QUOTE`` is checked first so a row with no ask at all
    never reports as ``ZERO_BID`` — "not quoted" and "quoted with no bid" are
    different failures, and only the second one means a market maker is
    showing an offer nobody will bid against.
    """
    if ask is None or ask <= 0:
        return QuoteState.NO_QUOTE
    if bid is None or bid <= 0:
        return QuoteState.ZERO_BID
    if ask < bid:
        return QuoteState.CROSSED
    if ask == bid:
        return QuoteState.LOCKED
    return QuoteState.TWO_SIDED


def contract_spread(row: Dict[str, Any], spot: float) -> Optional[ContractSpread]:
    """Reduce one ``option_chains``-shaped row to a :class:`ContractSpread`.

    Returns None only when the row is structurally unusable (no strike, or a
    non-positive spot).  A row that is merely *badly quoted* is never
    dropped — it comes back with the state that says so, because the count
    of those is itself the signal.
    """
    strike = _as_float(row.get("strike"))
    if strike is None or strike <= 0 or spot <= 0:
        return None

    bid = _as_float(row.get("bid"))
    ask = _as_float(row.get("ask"))
    state = classify_quote(bid, ask)

    moneyness_pct = 100.0 * (strike - spot) / spot

    common = {
        "option_symbol": row.get("option_symbol"),
        "strike": strike,
        "option_type": (row.get("option_type") or "").upper(),
        "expiration": row.get("expiration"),
        "state": state,
        "bid": bid,
        "ask": ask,
        "moneyness_pct": round(moneyness_pct, 4),
        "open_interest": int(_as_float(row.get("open_interest")) or 0),
        "volume": int(_as_float(row.get("volume")) or 0),
    }

    if state is not QuoteState.TWO_SIDED or bid is None or ask is None:
        return ContractSpread(**common)

    spread = ask - bid
    mid = (ask + bid) / 2.0
    return ContractSpread(
        **common,
        spread=round(spread, 4),
        mid=round(mid, 4),
        # mid > 0 is guaranteed here: TWO_SIDED requires bid > 0.
        relative_spread_pct=round(100.0 * spread / mid, 4),
        spread_bps_underlying=round(10_000.0 * spread / spot, 4),
    )


def contract_spreads(
    rows: Iterable[Dict[str, Any]], spot: float
) -> List[ContractSpread]:
    """Map :func:`contract_spread` across rows, skipping unusable ones."""
    out: List[ContractSpread] = []
    for row in rows:
        item = contract_spread(row, spot)
        if item is not None:
            out.append(item)
    return out


@dataclass(frozen=True)
class SpreadAggregate:
    """Chain-wide (or bucket-wide) quote quality.

    ``contract_count`` counts every contract considered, including the ones
    with no market; ``tradable_count`` counts only those a width could be
    computed from.  Reporting both is what stops a chain from appearing to
    tighten as its wings go no-bid and leave the width sample.
    """

    contract_count: int
    tradable_count: int
    #: Share of contracts with a real two-sided market, 0-100.
    two_sided_pct: float
    #: Share quoted with no bid — offered, but with nothing to sell into.
    zero_bid_pct: float
    #: Share locked or crossed; almost always stale marks rather than edge.
    crossed_or_locked_pct: float
    #: Share carrying no usable quote at all.
    no_quote_pct: float
    median_spread: Optional[float] = None
    median_relative_spread_pct: Optional[float] = None
    p90_relative_spread_pct: Optional[float] = None
    median_spread_bps_underlying: Optional[float] = None
    p90_spread_bps_underlying: Optional[float] = None
    #: Summed OI / volume of the contracts considered — thin books quote wide,
    #: and a width read without them invites the wrong conclusion.
    total_open_interest: int = 0
    total_volume: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contract_count": self.contract_count,
            "tradable_count": self.tradable_count,
            "two_sided_pct": self.two_sided_pct,
            "zero_bid_pct": self.zero_bid_pct,
            "crossed_or_locked_pct": self.crossed_or_locked_pct,
            "no_quote_pct": self.no_quote_pct,
            "median_spread": self.median_spread,
            "median_relative_spread_pct": self.median_relative_spread_pct,
            "p90_relative_spread_pct": self.p90_relative_spread_pct,
            "median_spread_bps_underlying": self.median_spread_bps_underlying,
            "p90_spread_bps_underlying": self.p90_spread_bps_underlying,
            "total_open_interest": self.total_open_interest,
            "total_volume": self.total_volume,
        }


def _pct(count: int, total: int) -> float:
    return round(100.0 * count / total, 2) if total else 0.0


def aggregate(spreads: Sequence[ContractSpread]) -> SpreadAggregate:
    """Reduce contract-level readings to one bucket summary.

    An all-empty input returns a zeroed aggregate with None widths rather
    than raising: an expiration with no quoted contracts is a legitimate
    state, and the caller renders "no data" from the None, not from an
    exception.
    """
    total = len(spreads)
    if total == 0:
        return SpreadAggregate(
            contract_count=0,
            tradable_count=0,
            two_sided_pct=0.0,
            zero_bid_pct=0.0,
            crossed_or_locked_pct=0.0,
            no_quote_pct=0.0,
        )

    tradable = [s for s in spreads if s.is_tradable]
    zero_bid = sum(1 for s in spreads if s.state is QuoteState.ZERO_BID)
    crossed_locked = sum(
        1 for s in spreads if s.state in (QuoteState.CROSSED, QuoteState.LOCKED)
    )
    no_quote = sum(1 for s in spreads if s.state is QuoteState.NO_QUOTE)

    widths = [s.spread for s in tradable if s.spread is not None]
    relatives = [
        s.relative_spread_pct for s in tradable if s.relative_spread_pct is not None
    ]
    bps = [
        s.spread_bps_underlying
        for s in tradable
        if s.spread_bps_underlying is not None
    ]

    def _round(value: Optional[float], places: int) -> Optional[float]:
        return None if value is None else round(value, places)

    return SpreadAggregate(
        contract_count=total,
        tradable_count=len(tradable),
        two_sided_pct=_pct(len(tradable), total),
        zero_bid_pct=_pct(zero_bid, total),
        crossed_or_locked_pct=_pct(crossed_locked, total),
        no_quote_pct=_pct(no_quote, total),
        median_spread=_round(percentile(widths, 50), 4),
        median_relative_spread_pct=_round(percentile(relatives, 50), 3),
        p90_relative_spread_pct=_round(percentile(relatives, 90), 3),
        median_spread_bps_underlying=_round(percentile(bps, 50), 3),
        p90_spread_bps_underlying=_round(percentile(bps, 90), 3),
        total_open_interest=sum(s.open_interest for s in spreads),
        total_volume=sum(s.volume for s in spreads),
    )


def aggregate_by_option_type(
    spreads: Sequence[ContractSpread],
) -> Dict[str, SpreadAggregate]:
    """Split into calls, puts and the combined chain.

    The put/call split is the point of the page rather than a nicety: a
    chain whose calls are unchanged and whose puts have doubled in width is
    a crash-hedge bid, and a blended median reports roughly half of it.
    """
    calls = [s for s in spreads if s.option_type == "C"]
    puts = [s for s in spreads if s.option_type == "P"]
    return {
        "calls": aggregate(calls),
        "puts": aggregate(puts),
        "all": aggregate(list(spreads)),
    }


def moneyness_bucket_label(low: float, high: float) -> str:
    """Human label for a signed-moneyness bucket, e.g. ``-5% to -3%``."""
    return f"{low:+.1f}% to {high:+.1f}%"


def aggregate_by_moneyness(
    spreads: Sequence[ContractSpread],
    edges: Sequence[float] = DEFAULT_MONEYNESS_EDGES,
    option_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """The spread curve across strike distance from spot.

    Bucketing is by SIGNED distance, so the result reads left-to-right the
    way a skew chart does: downside strikes on the left, upside on the
    right.  Combined with an ``option_type`` filter this is what shows the
    OTM-put wing blowing out while everything else stays orderly — the shape
    a blended, unsigned "distance from ATM" bucketing would average away.

    Buckets are half-open ``[low, high)`` except the last, which includes its
    upper edge so a strike exactly at the outer boundary is not silently
    dropped.
    """
    pool = (
        [s for s in spreads if s.option_type == option_type.upper()]
        if option_type
        else list(spreads)
    )

    out: List[Dict[str, Any]] = []
    for index in range(len(edges) - 1):
        low, high = float(edges[index]), float(edges[index + 1])
        is_last = index == len(edges) - 2
        bucket = [
            s
            for s in pool
            if s.moneyness_pct is not None
            and s.moneyness_pct >= low
            and (s.moneyness_pct <= high if is_last else s.moneyness_pct < high)
        ]
        out.append(
            {
                "moneyness_low_pct": low,
                "moneyness_high_pct": high,
                "label": moneyness_bucket_label(low, high),
                **aggregate(bucket).to_dict(),
            }
        )
    return out


def aggregate_by_expiration(
    spreads: Sequence[ContractSpread],
    dte_of: Dict[Any, int],
) -> List[Dict[str, Any]]:
    """Spread quality per EXPIRATION, calls and puts kept apart.

    Deliberately per-expiration rather than per-DTE-bucket.  A bucket like
    "2-7 DTE" is not something anyone trades: it blends Wednesday's expiry
    with Friday's, and those routinely differ by more than the change the
    page is trying to show.  "Today's puts are 8% wide and Friday's are 3%"
    is a sentence a trader can act on; "2-7 DTE puts are 5% wide" is not.

    ``dte_of`` maps each contract's ``expiration`` value to its integer DTE.
    Resolving that needs a trading calendar and a timezone, which this module
    deliberately holds neither of — passing it in is what keeps every
    function here deterministic.  Contracts whose expiration the caller
    could not resolve are skipped rather than pooled into an "unknown"
    slice that would silently widen whichever row it landed in.

    Sorted nearest-expiry first, which is both the reading order and the
    order of interest: the near-dated rows are where quotes go first.
    """
    by_exp: Dict[Any, List[ContractSpread]] = {}
    for item in spreads:
        if item.expiration not in dte_of:
            continue
        by_exp.setdefault(item.expiration, []).append(item)

    out: List[Dict[str, Any]] = []
    for expiration in sorted(by_exp, key=lambda e: dte_of[e]):
        bucket = by_exp[expiration]
        split = aggregate_by_option_type(bucket)
        out.append(
            {
                "expiration": expiration,
                "dte": dte_of[expiration],
                "calls": split["calls"].to_dict(),
                "puts": split["puts"].to_dict(),
                "all": split["all"].to_dict(),
            }
        )
    return out
