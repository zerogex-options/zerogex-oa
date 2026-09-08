"""Model B — Aggressor-Inferred MM positioning (the assumption under test).

ZeroGEX's ingestion classifies every option print against the contemporaneous
NBBO (a Lee-Ready prior-tick rule with a mid band; see
``src/ingestion/main_engine.py::IngestionEngine._classify_volume_chunk``) and
persists the result as three SESSION-CUMULATIVE counters on every
``option_chains`` minute row:

* ``ask_volume`` — volume that printed at or near the ask: **buyer-initiated**
* ``bid_volume`` — volume that printed at or near the bid: **seller-initiated**
* ``mid_volume`` — inside the band, the opening auction, or a print with no
  usable quote: **unclassified**

That classification establishes which side *initiated* a trade.  It says
nothing about *who* was on either side.  A common shortcut — used by a number
of positioning products — adds an assumption on top of it::

    buyer-initiated  print  ->  the aggressor was a customer who bought,
                                so the passive market maker SOLD
    seller-initiated print  ->  the aggressor was a customer who sold,
                                so the passive market maker BOUGHT

This module implements exactly that shortcut, so it can be measured.  It is
**Aggressor-Inferred MM positioning** (equivalently *Aggressor-Assumption
GEX*).  It is never "observed dealer flow", "attributed dealer flow" or
"dealer positioning": the participant identity is assumed, not classified.
The whole point of building it is to find out how often the assumption is
right, by comparing it against exchange-classified Market Maker activity
(:mod:`~research.mm_attributed_gex.attribution`) and by running it through
the same market-outcome battery as the other arms.

Two variants, both session-scoped
---------------------------------
**B1 — flow since open** (:data:`FLOW_ONLY_REASON`).  Every session starts at
zero.  For each classified bucket the assumed MM signed quantity changes by
``+seller_initiated − buyer_initiated``; unclassified volume changes nothing.
This is NOT an inventory level — it is the inferred *change* in MM option
quantity since the cash open, and it is labelled that way everywhere.

**B2 — production-anchored** (:data:`ANCHORED_REASON`).  The B1 change is
added to the quantity ZeroGEX's production convention already assumes:
``+open_interest`` for a call, ``−open_interest`` for a put, taken from the
same chain snapshot the production reading used.  The starting inventory is
therefore Model A, not an observed MM inventory, and the arm is a hybrid by
construction.  It exists so the aggressor assumption can be compared to the
other arms as a full profile (flip, walls, gamma at spot).

Sign conventions, stated once
-----------------------------
The option type never enters the side mapping.  A long put and a long call
both carry positive gamma; a short put and a short call both carry negative
gamma.  The contract's identity decides its gamma; the *assumed MM buy/sell
side* decides the sign of the quantity.  :func:`assumed_mm_delta` is the only
place that mapping lives.

Causality
---------
An ``option_chains`` row is stamped with the start of its one-minute bucket and
its counters cover every snapshot that arrived inside that minute, so the row
is fully known one minute after its stamp.  :class:`AggressorBucket.timestamp`
is that *known-at* instant, and the replay only consumes buckets stamped at or
before the snapshot it is pricing.  Nothing here uses next-day open interest,
end-of-day totals, or any later row.

Source fidelity
---------------
``option_chains`` carries the unclassified share explicitly and is the honest
source.  ``flow_contract_facts`` stores per-bucket deltas whose buy/sell split
has had the mid-classified portion redistributed pro-rata, so the unclassified
share is unrecoverable from it; buckets built from it are marked
``extrapolated`` and the label travels through every report.
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Sequence

from src.validation import cash_session_date

from research.mm_attributed_gex.gex import ChainQuote, MMContract, join_positions_to_chain
from research.mm_attributed_gex.inventory import MMPosition, settlement_instant
from research.mm_attributed_gex.schema import Interval, SeriesKey

__all__ = [
    "ARM_LABELS",
    "FLOW_ONLY_REASON",
    "ANCHORED_REASON",
    "SOURCE_OPTION_CHAINS",
    "SOURCE_FLOW_FACTS",
    "AggressorBucket",
    "AggressorSeriesState",
    "AggressorFlowBook",
    "AggressorTimeline",
    "AggressorGateConfig",
    "SessionCoverage",
    "AnchorDiagnostics",
    "assumed_mm_delta",
    "option_root_from_symbol",
    "coverage_by_session",
    "flow_positions_to_contracts",
    "production_anchored_contracts",
    "interval_bucket_end",
    "aggregate_to_interval",
    "write_aggressor_jsonl",
    "read_aggressor_jsonl",
]

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - defensive
    import pytz

    ET = pytz.timezone("US/Eastern")  # type: ignore[assignment]


#: Mandatory arm terminology.  Reports and column prefixes derive from these.
ARM_LABELS: Mapping[str, str] = {
    "A": "Production Modeled GEX",
    "B1": "Aggressor-Inferred MM GEX — flow since open",
    "B2": "Aggressor-Inferred MM GEX — production-anchored",
    "C": "Market-Maker Attributed GEX",
}

#: ``MMPosition.left_censor_reason`` for a B1 (flow-only) position.
FLOW_ONLY_REASON = "aggressor_inferred_session_flow"
#: ``MMPosition.left_censor_reason`` for a B2 (production-anchored) position.
ANCHORED_REASON = "production_anchored_aggressor_mm"

SOURCE_OPTION_CHAINS = "option_chains"
SOURCE_FLOW_FACTS = "flow_contract_facts"

_ROOT_RE = re.compile(r"^([A-Z]+)")


def option_root_from_symbol(option_symbol: Optional[str], symbol: str) -> Optional[str]:
    """The listed root (``SPX`` / ``SPXW``) from a TradeStation option symbol.

    ``"SPXW 260619C06000000"`` → ``"SPXW"``.  The root decides AM vs PM
    settlement downstream, so it is carried rather than re-derived.  Falls back
    to the underlying when the symbol carries no recognisable root.
    """
    if not option_symbol:
        return symbol or None
    head = option_symbol.strip().split(" ")[0].upper()
    m = _ROOT_RE.match(head)
    return m.group(1) if m else (symbol or None)


def assumed_mm_delta(buyer_initiated: float, seller_initiated: float) -> float:
    """The assumption under test, in one line.

    ``buyer-initiated → assumed MM sold → negative``;
    ``seller-initiated → assumed MM bought → positive``.  Unclassified volume
    is not an argument on purpose: it contributes no signed change.
    """
    return float(seller_initiated) - float(buyer_initiated)


@dataclass(frozen=True, slots=True)
class AggressorBucket:
    """Classified volume for one option series over one persisted bucket.

    ``timestamp`` is the instant at which the bucket is fully known (its END),
    tz-aware UTC.  ``trading_date`` is the ET cash-session date.  Volumes are
    non-negative counts; direction is carried by which field they sit in.
    """

    symbol: str
    option_symbol: str
    expiration: date
    strike: float
    option_type: str
    timestamp: datetime
    trading_date: date
    buyer_initiated: int
    seller_initiated: int
    unclassified: int
    source: str = SOURCE_OPTION_CHAINS
    extrapolated: bool = False
    gamma: Optional[float] = None
    implied_volatility: Optional[float] = None
    quote_locked: bool = False
    quote_crossed: bool = False
    quote_missing: bool = False
    first_row_of_session: bool = False

    def __post_init__(self) -> None:
        if min(self.buyer_initiated, self.seller_initiated, self.unclassified) < 0:
            raise ValueError("classified volumes must be non-negative counts")
        if self.option_type not in ("C", "P"):
            raise ValueError(f"option_type must be 'C' or 'P' (got {self.option_type!r})")
        if self.strike <= 0:
            raise ValueError(f"strike must be positive (got {self.strike})")
        if self.timestamp.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware (UTC)")

    @property
    def key(self) -> SeriesKey:
        return (self.symbol, self.expiration, float(self.strike), self.option_type)

    @property
    def option_root(self) -> Optional[str]:
        return option_root_from_symbol(self.option_symbol, self.symbol)

    @property
    def classified(self) -> int:
        return self.buyer_initiated + self.seller_initiated

    @property
    def total(self) -> int:
        return self.classified + self.unclassified

    @property
    def assumed_mm_delta(self) -> float:
        return assumed_mm_delta(self.buyer_initiated, self.seller_initiated)

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "option_symbol": self.option_symbol,
            "expiration": self.expiration.isoformat(),
            "strike": self.strike,
            "option_type": self.option_type,
            "timestamp": self.timestamp.isoformat(),
            "trading_date": self.trading_date.isoformat(),
            "buyer_initiated": self.buyer_initiated,
            "seller_initiated": self.seller_initiated,
            "unclassified": self.unclassified,
            "source": self.source,
            "extrapolated": self.extrapolated,
            "gamma": self.gamma,
            "implied_volatility": self.implied_volatility,
            "quote_locked": self.quote_locked,
            "quote_crossed": self.quote_crossed,
            "quote_missing": self.quote_missing,
            "first_row_of_session": self.first_row_of_session,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "AggressorBucket":
        ts = datetime.fromisoformat(str(payload["timestamp"]))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return cls(
            symbol=str(payload["symbol"]),
            option_symbol=str(payload.get("option_symbol") or ""),
            expiration=date.fromisoformat(str(payload["expiration"])),
            strike=float(payload["strike"]),
            option_type=str(payload["option_type"]),
            timestamp=ts,
            trading_date=date.fromisoformat(str(payload["trading_date"])),
            buyer_initiated=int(payload.get("buyer_initiated") or 0),
            seller_initiated=int(payload.get("seller_initiated") or 0),
            unclassified=int(payload.get("unclassified") or 0),
            source=str(payload.get("source") or SOURCE_OPTION_CHAINS),
            extrapolated=bool(payload.get("extrapolated", False)),
            gamma=(None if payload.get("gamma") is None else float(payload["gamma"])),
            implied_volatility=(
                None
                if payload.get("implied_volatility") is None
                else float(payload["implied_volatility"])
            ),
            quote_locked=bool(payload.get("quote_locked", False)),
            quote_crossed=bool(payload.get("quote_crossed", False)),
            quote_missing=bool(payload.get("quote_missing", False)),
            first_row_of_session=bool(payload.get("first_row_of_session", False)),
        )


# ---------------------------------------------------------------------------
# Session coverage and gates
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AggressorGateConfig:
    """Minimum-data gates a session must pass to enter the headline B arms.

    Predeclared, not tuned: a session whose tape was mostly unclassifiable
    cannot say anything about the aggressor assumption, and letting it into
    the headline numbers would let the data quality masquerade as a
    methodology result.
    """

    #: Share of classified (buyer + seller) volume in the session's total.
    min_classified_share: float = 0.50
    #: Distinct (series, minute) buckets observed in the session.
    min_buckets: int = 50
    #: Distinct series with any classified volume in the session.
    min_series: int = 10
    #: Refuse sessions built from a source that redistributed the mid volume
    #: unless explicitly allowed — the unclassified share is then unknowable.
    allow_extrapolated: bool = False


@dataclass
class SessionCoverage:
    """What the classified tape looked like for one session."""

    trading_date: date
    buckets: int = 0
    series: set[SeriesKey] = field(default_factory=set)
    buyer_initiated: int = 0
    seller_initiated: int = 0
    unclassified: int = 0
    locked_quote_buckets: int = 0
    crossed_quote_buckets: int = 0
    missing_quote_buckets: int = 0
    first_row_volume: int = 0
    extrapolated_buckets: int = 0
    sources: set[str] = field(default_factory=set)

    @property
    def classified(self) -> int:
        return self.buyer_initiated + self.seller_initiated

    @property
    def total(self) -> int:
        return self.classified + self.unclassified

    @property
    def classified_share(self) -> float:
        return self.classified / self.total if self.total else 0.0

    @property
    def buyer_share(self) -> float:
        return self.buyer_initiated / self.total if self.total else 0.0

    @property
    def seller_share(self) -> float:
        return self.seller_initiated / self.total if self.total else 0.0

    @property
    def unclassified_share(self) -> float:
        return self.unclassified / self.total if self.total else 0.0

    @property
    def extrapolated(self) -> bool:
        return self.extrapolated_buckets > 0

    def add(self, bucket: AggressorBucket) -> None:
        self.buckets += 1
        self.series.add(bucket.key)
        self.buyer_initiated += bucket.buyer_initiated
        self.seller_initiated += bucket.seller_initiated
        self.unclassified += bucket.unclassified
        self.locked_quote_buckets += int(bucket.quote_locked)
        self.crossed_quote_buckets += int(bucket.quote_crossed)
        self.missing_quote_buckets += int(bucket.quote_missing)
        if bucket.first_row_of_session:
            self.first_row_volume += bucket.total
        self.extrapolated_buckets += int(bucket.extrapolated)
        self.sources.add(bucket.source)

    def gate(self, config: AggressorGateConfig = AggressorGateConfig()) -> tuple[bool, list[str]]:
        """``(passed, reasons)`` against the predeclared minimums."""
        reasons: list[str] = []
        if self.buckets < config.min_buckets:
            reasons.append(f"buckets {self.buckets} < {config.min_buckets}")
        if len(self.series) < config.min_series:
            reasons.append(f"series {len(self.series)} < {config.min_series}")
        if self.classified_share < config.min_classified_share:
            reasons.append(
                f"classified share {self.classified_share:.2f} < {config.min_classified_share:.2f}"
            )
        if self.extrapolated and not config.allow_extrapolated:
            reasons.append("built from an extrapolated buy/sell split (unclassified share unknown)")
        return (not reasons), reasons

    def as_dict(self, config: Optional[AggressorGateConfig] = None) -> dict[str, Any]:
        out: dict[str, Any] = {
            "trading_date": self.trading_date.isoformat(),
            "buckets": self.buckets,
            "series": len(self.series),
            "buyer_initiated": self.buyer_initiated,
            "seller_initiated": self.seller_initiated,
            "unclassified": self.unclassified,
            "total": self.total,
            "classified_share": self.classified_share,
            "buyer_share": self.buyer_share,
            "seller_share": self.seller_share,
            "unclassified_share": self.unclassified_share,
            "locked_quote_buckets": self.locked_quote_buckets,
            "crossed_quote_buckets": self.crossed_quote_buckets,
            "missing_quote_buckets": self.missing_quote_buckets,
            "first_row_volume": self.first_row_volume,
            "extrapolated": self.extrapolated,
            "sources": sorted(self.sources),
        }
        if config is not None:
            passed, reasons = self.gate(config)
            out["gate_passed"] = passed
            out["gate_reasons"] = reasons
        return out


def coverage_by_session(buckets: Iterable[AggressorBucket]) -> dict[date, SessionCoverage]:
    """Classification coverage per session — the first pass over the stream."""
    out: dict[date, SessionCoverage] = {}
    for b in buckets:
        cov = out.get(b.trading_date)
        if cov is None:
            cov = SessionCoverage(trading_date=b.trading_date)
            out[b.trading_date] = cov
        cov.add(b)
    return out


# ---------------------------------------------------------------------------
# B1 — flow since open
# ---------------------------------------------------------------------------


@dataclass
class AggressorSeriesState:
    """Running aggressor-inferred MM change for one series within a session."""

    key: SeriesKey
    symbol: str
    option_symbol: str
    expiration: date
    strike: float
    option_type: str
    option_root: Optional[str]
    trading_date: date
    assumed_mm_buys: float = 0.0  # seller-initiated volume
    assumed_mm_sells: float = 0.0  # buyer-initiated volume
    unclassified: float = 0.0
    buckets: int = 0
    first_timestamp: Optional[datetime] = None
    last_timestamp: Optional[datetime] = None
    extrapolated: bool = False

    @property
    def signed_change(self) -> float:
        """Assumed MM signed quantity change since the session open."""
        return self.assumed_mm_buys - self.assumed_mm_sells

    @property
    def classified(self) -> float:
        return self.assumed_mm_buys + self.assumed_mm_sells

    def apply(self, bucket: AggressorBucket) -> None:
        # The mapping lives in assumed_mm_delta; this just books the legs.
        self.assumed_mm_buys += float(bucket.seller_initiated)
        self.assumed_mm_sells += float(bucket.buyer_initiated)
        self.unclassified += float(bucket.unclassified)
        self.buckets += 1
        self.extrapolated = self.extrapolated or bucket.extrapolated
        if self.first_timestamp is None or bucket.timestamp < self.first_timestamp:
            self.first_timestamp = bucket.timestamp
        if self.last_timestamp is None or bucket.timestamp > self.last_timestamp:
            self.last_timestamp = bucket.timestamp

    def to_mm_position(self) -> MMPosition:
        """Encode the signed change as an :class:`MMPosition` for the pricing path.

        ``long`` carries the assumed MM buys and ``short`` the assumed MM sells
        so ``net_contracts`` is the signed change.  ``left_censored`` is False
        because the quantity starts from a known zero at the open — it is a
        change, not an inventory, and the reason code says so.
        """
        return MMPosition(
            key=self.key,
            symbol=self.symbol,
            expiration=self.expiration,
            strike=self.strike,
            option_type=self.option_type,
            option_root=self.option_root,
            long_contracts=self.assumed_mm_buys,
            short_contracts=self.assumed_mm_sells,
            opening_buys=self.assumed_mm_buys,
            opening_sells=self.assumed_mm_sells,
            first_activity=self.first_timestamp,
            last_activity=self.last_timestamp,
            first_activity_date=self.trading_date,
            last_activity_date=self.trading_date,
            active_sessions={self.trading_date},
            buckets_observed=self.buckets,
            left_censored=False,
            left_censor_reason=FLOW_ONLY_REASON,
        )


class AggressorFlowBook:
    """Session-scoped accumulation of aggressor-inferred MM change (B1).

    Buckets must arrive chronologically.  A bucket from a later session
    resets the book — every session starts from zero — and a bucket from an
    earlier session is an ordering bug, so it raises rather than silently
    corrupting the running state.
    """

    __slots__ = ("symbol", "session_date", "_series", "coverage", "buckets_consumed")

    def __init__(self, symbol: str = "SPX") -> None:
        self.symbol = symbol
        self.session_date: Optional[date] = None
        self._series: dict[SeriesKey, AggressorSeriesState] = {}
        self.coverage: Optional[SessionCoverage] = None
        self.buckets_consumed = 0

    def __len__(self) -> int:
        return len(self._series)

    def reset(self, session: date) -> None:
        self.session_date = session
        self._series = {}
        self.coverage = SessionCoverage(trading_date=session)

    def consume(self, buckets: Iterable[AggressorBucket]) -> "AggressorFlowBook":
        for b in buckets:
            if self.session_date is None or b.trading_date > self.session_date:
                self.reset(b.trading_date)
            elif b.trading_date < self.session_date:
                raise ValueError(
                    f"aggressor buckets must be chronological: got {b.trading_date} "
                    f"after {self.session_date}"
                )
            state = self._series.get(b.key)
            if state is None:
                state = AggressorSeriesState(
                    key=b.key,
                    symbol=b.symbol,
                    option_symbol=b.option_symbol,
                    expiration=b.expiration,
                    strike=float(b.strike),
                    option_type=b.option_type,
                    option_root=b.option_root,
                    trading_date=b.trading_date,
                )
                self._series[b.key] = state
            state.apply(b)
            assert self.coverage is not None
            self.coverage.add(b)
            self.buckets_consumed += 1
        return self

    def states(self) -> list[AggressorSeriesState]:
        return list(self._series.values())

    def positions(self, as_of: Optional[datetime] = None) -> list[MMPosition]:
        """B1 positions with a non-zero signed change, dropping settled series."""
        out: list[MMPosition] = []
        for state in self._series.values():
            if state.signed_change == 0.0:
                continue
            if as_of is not None and as_of >= settlement_instant(
                state.symbol, state.expiration, state.option_root
            ):
                continue
            out.append(state.to_mm_position())
        return out


class AggressorTimeline:
    """Replays classified buckets and freezes the B1 book at requested instants.

    Same single-pass shape as :class:`~research.mm_attributed_gex.dataset.InventoryTimeline`:
    ``O(buckets + snapshots)``.  A bucket stamped exactly at the snapshot is
    included (its minute is fully known at that instant); anything later is
    held back.  A snapshot whose cash session the book has not yet seen gets an
    empty list — nothing has been observed in that session, and the previous
    session's flow must not leak across the open.
    """

    def __init__(self, symbol: str = "SPX") -> None:
        self.symbol = symbol
        self.book = AggressorFlowBook(symbol=symbol)

    def replay(
        self,
        buckets: Iterable[AggressorBucket],
        timestamps: Sequence[datetime],
    ) -> Iterator[tuple[datetime, list[MMPosition], Optional[SessionCoverage]]]:
        stamps = sorted(timestamps)
        if not stamps:
            return
        it = iter(buckets)
        pending: Optional[AggressorBucket] = None
        for target in stamps:
            while True:
                b = pending if pending is not None else next(it, None)
                pending = None
                if b is None:
                    break
                if b.timestamp > target:
                    pending = b
                    break
                self.book.consume((b,))
            yield target, self._freeze(target), self._coverage_for(target)

    def _coverage_for(self, as_of: datetime) -> Optional[SessionCoverage]:
        if self.book.session_date != cash_session_date(as_of):
            return None
        return self.book.coverage

    def _freeze(self, as_of: datetime) -> list[MMPosition]:
        if self.book.session_date != cash_session_date(as_of):
            return []
        return self.book.positions(as_of)


# ---------------------------------------------------------------------------
# Joining to the chain — B1 priced, B2 anchored
# ---------------------------------------------------------------------------


@dataclass
class AnchorDiagnostics:
    """What happened when the B1 change was laid over the production anchor."""

    chain_contracts: int = 0
    anchored_contracts: int = 0
    flow_series: int = 0
    flow_series_matched: int = 0
    flow_series_unmatched: int = 0
    flow_contracts_unmatched: float = 0.0
    flow_series_unpriceable: int = 0
    anchor_contracts_total: float = 0.0
    flow_contracts_total: float = 0.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "chain_contracts": self.chain_contracts,
            "anchored_contracts": self.anchored_contracts,
            "flow_series": self.flow_series,
            "flow_series_matched": self.flow_series_matched,
            "flow_series_unmatched": self.flow_series_unmatched,
            "flow_contracts_unmatched": self.flow_contracts_unmatched,
            "flow_series_unpriceable": self.flow_series_unpriceable,
            "anchor_contracts_total": self.anchor_contracts_total,
            "flow_contracts_total": self.flow_contracts_total,
        }


def flow_positions_to_contracts(
    positions: Sequence[MMPosition], chain: Sequence[ChainQuote]
) -> tuple[list[MMContract], list[MMPosition]]:
    """Price B1 positions against the chain.  ``(priced, unpriceable)``.

    Delegates to the same join the attributed arm uses, so a B1 series is
    matched, priced and dropped by exactly the rules a C series is.
    """
    return join_positions_to_chain(positions, chain, use_net_flow=False)


def production_anchored_contracts(
    chain: Sequence[ChainQuote],
    flow_positions: Sequence[MMPosition],
) -> tuple[list[MMContract], AnchorDiagnostics]:
    """B2: the production convention's quantity plus the aggressor-inferred change.

    For every chain contract with a usable implied vol::

        anchor = +open_interest  (call)  |  −open_interest  (put)
        qty    = anchor + signed_change_since_open  (0 when the series saw no
                 classified volume)

    The anchor is the open interest ZeroGEX's production reading used at this
    very snapshot — published once per session, so it is the start-of-day
    figure and never a next-day value.  With no classified volume at all, the
    contracts reduce exactly to the production rows, which is the parity a
    test pins.
    """
    diag = AnchorDiagnostics(chain_contracts=len(chain), flow_series=len(flow_positions))
    flow_by_key: dict[tuple[date, float, str], MMPosition] = {
        (p.expiration, float(p.strike), p.option_type): p for p in flow_positions
    }
    diag.flow_contracts_total = sum(p.net_contracts for p in flow_positions)
    seen: set[tuple[date, float, str]] = set()
    out: list[MMContract] = []
    for q in chain:
        k = (q.expiration, float(q.strike), q.option_type)
        flow = flow_by_key.get(k)
        priceable = bool(q.implied_volatility) and (q.implied_volatility or 0.0) > 0
        if flow is not None:
            seen.add(k)
            if not priceable:
                diag.flow_series_unpriceable += 1
        if not priceable:
            continue
        anchor = float(q.open_interest) * (1.0 if q.option_type == "C" else -1.0)
        if q.open_interest > 0:
            diag.anchored_contracts += 1
            diag.anchor_contracts_total += anchor
        delta = flow.net_contracts if flow is not None else 0.0
        qty = anchor + delta
        if qty == 0.0:
            continue
        position = MMPosition(
            key=("", q.expiration, float(q.strike), q.option_type),
            symbol=flow.symbol if flow is not None else "",
            expiration=q.expiration,
            strike=float(q.strike),
            option_type=q.option_type,
            option_root=option_root_from_symbol(q.option_symbol, ""),
            long_contracts=max(qty, 0.0),
            short_contracts=max(-qty, 0.0),
            left_censored=False,
            left_censor_reason=ANCHORED_REASON,
        )
        out.append(MMContract(position=position, quote=q, net_contracts=qty))
    diag.flow_series_matched = len(seen)
    unmatched = [
        p for p in flow_positions if (p.expiration, float(p.strike), p.option_type) not in seen
    ]
    diag.flow_series_unmatched = len(unmatched)
    diag.flow_contracts_unmatched = sum(abs(p.net_contracts) for p in unmatched)
    return out, diag


# ---------------------------------------------------------------------------
# Interval aggregation — for the attribution comparison
# ---------------------------------------------------------------------------

_INTERVAL_MINUTES: Mapping[Interval, int] = {
    Interval.MINUTE_1: 1,
    Interval.MINUTE_5: 5,
    Interval.MINUTE_10: 10,
    Interval.MINUTE_30: 30,
}


def interval_bucket_end(ts: datetime, interval: Interval, trading_date: date) -> datetime:
    """The exchange-interval bucket a known-at instant belongs to (its END, UTC).

    Computed on the ET wall clock so the grid is DST-safe: a 10-minute grid
    ends at 09:40, 09:50, … ET on EST and EDT days alike.  An instant exactly
    on a boundary belongs to the bucket ending there (it is known at that
    instant).  Session-summary feeds are stamped 16:00 ET on the trading date,
    matching ``cboe.loader._bucket_timestamp``.
    """
    minutes = _INTERVAL_MINUTES.get(interval)
    if minutes is None:
        naive = datetime.combine(trading_date, time(16, 0))
        return naive.replace(tzinfo=ET).astimezone(timezone.utc)
    et = ts.astimezone(ET)
    since_midnight = et.hour * 60 + et.minute + (1 if (et.second or et.microsecond) else 0)
    end_minutes = -(-since_midnight // minutes) * minutes  # ceil to the grid
    day = et.date()
    naive = datetime.combine(day, time(0, 0)) + timedelta(minutes=end_minutes)
    return naive.replace(tzinfo=ET).astimezone(timezone.utc)


@dataclass
class AggregatedAggressor:
    """B volume folded into one exchange-interval bucket for one series."""

    bucket_end: datetime
    trading_date: date
    key: SeriesKey
    option_symbol: str
    buyer_initiated: float = 0.0
    seller_initiated: float = 0.0
    unclassified: float = 0.0
    gamma: Optional[float] = None
    implied_volatility: Optional[float] = None
    rows: int = 0
    extrapolated: bool = False

    @property
    def signed(self) -> float:
        return assumed_mm_delta(self.buyer_initiated, self.seller_initiated)


def aggregate_to_interval(
    buckets: Iterable[AggressorBucket], interval: Interval
) -> dict[tuple[datetime, SeriesKey], AggregatedAggressor]:
    """Fold minute buckets into ``(bucket_end, series)`` cells of ``interval``.

    A minute row never lands in a bucket that ends before the row is known,
    so nothing leaks backward.  The last row's gamma in the cell is kept as
    the cell's gamma.
    """
    out: dict[tuple[datetime, SeriesKey], AggregatedAggressor] = {}
    for b in buckets:
        end = interval_bucket_end(b.timestamp, interval, b.trading_date)
        cell = out.get((end, b.key))
        if cell is None:
            cell = AggregatedAggressor(
                bucket_end=end,
                trading_date=b.trading_date,
                key=b.key,
                option_symbol=b.option_symbol,
            )
            out[(end, b.key)] = cell
        cell.buyer_initiated += b.buyer_initiated
        cell.seller_initiated += b.seller_initiated
        cell.unclassified += b.unclassified
        cell.rows += 1
        cell.extrapolated = cell.extrapolated or b.extrapolated
        if b.gamma is not None:
            cell.gamma = b.gamma
        if b.implied_volatility is not None:
            cell.implied_volatility = b.implied_volatility
    return out


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def write_aggressor_jsonl(buckets: Iterable[AggressorBucket], path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for b in buckets:
            fh.write(json.dumps(b.as_dict()) + "\n")
    return p


def read_aggressor_jsonl(path: str | Path) -> Iterator[AggressorBucket]:
    """Stream buckets back, in file order (which the writer keeps chronological)."""
    with Path(path).open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                yield AggressorBucket.from_dict(json.loads(line))


def make_bucket_factory(path: str | Path) -> Callable[[], Iterator[AggressorBucket]]:
    """A re-openable stream, for the two-pass builders."""

    def factory() -> Iterator[AggressorBucket]:
        return read_aggressor_jsonl(path)

    return factory


def session_open_instant(session: date) -> datetime:
    """09:30 ET on ``session`` as a UTC instant — the B1 zero point."""
    return datetime.combine(session, time(9, 30)).replace(tzinfo=ET).astimezone(timezone.utc)


def group_by_session(
    buckets: Iterable[AggressorBucket],
) -> dict[date, list[AggressorBucket]]:
    out: dict[date, list[AggressorBucket]] = defaultdict(list)
    for b in buckets:
        out[b.trading_date].append(b)
    return dict(out)
