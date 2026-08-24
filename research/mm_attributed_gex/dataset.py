"""Side-by-side research dataset: existing ZeroGEX values next to MM-attributed ones.

One row per snapshot, carrying both methodologies plus enough diagnostics to
tell *"the methodology failed"* apart from *"the inventory reconstruction was
not complete enough"*.  That distinction is the reason half the columns exist.

Replay
------
MM inventory is a running state, so the dataset is produced by replay rather
than by point queries.  Two passes over the record stream:

1. **Classification pass** — consume the whole window and finalize, which is
   the only point at which left-censoring and session gaps can be decided (you
   cannot know a contract's first trade fell at the window edge until you have
   seen the window).
2. **Replay pass** — walk the records chronologically again, and whenever the
   cursor crosses a requested snapshot timestamp, freeze the running inventory
   and price it.

Censoring verdicts from pass 1 are stamped onto pass 2 snapshots.  They are
properties of the series over the whole window, not of the snapshot, so this is
correct — and it is also the conservative direction: a series whose history
turns out to be incomplete is marked incomplete at *every* snapshot, including
the ones before the evidence appeared.

Causality
---------
The replay only ever uses records whose bucket timestamp is ``<=`` the
snapshot.  For a session-summary feed those buckets are stamped at the session
close (see ``cboe.loader._bucket_timestamp``), so an end-of-day file can never
inform a mid-session snapshot of the same day.  This is what keeps the forward
tests in :mod:`~.backtest` honest.
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Sequence

from src.analytics.main_engine import AnalyticsEngine

from research.mm_attributed_gex.confidence import (
    ConfidenceWeights,
    UniverseConfidence,
    score_universe,
)
from research.mm_attributed_gex.gex import (
    DEFAULT_UNIVERSES,
    ChainQuote,
    ExpirationUniverse,
    build_engine,
    compute_mm_gex,
    gamma_by_key,
    join_positions_to_chain,
    production_profile_rows,
)
from research.mm_attributed_gex.inventory import (
    InventoryReconstructor,
    MMPosition,
    settlement_instant,
)
from research.mm_attributed_gex.schema import ParticipantActivity, SeriesKey
from research.mm_attributed_gex.walls import build_strike_structure

logger = logging.getLogger(__name__)

__all__ = [
    "DatasetSpec",
    "SnapshotRow",
    "InventoryTimeline",
    "build_dataset",
    "write_jsonl",
    "write_csv",
    "read_jsonl",
]

RecordsFactory = Callable[[], Iterable[ParticipantActivity]]
ChainProvider = Callable[[datetime], Sequence[ChainQuote]]
SummaryProvider = Callable[[datetime], Optional[Mapping[str, Any]]]


@dataclass(frozen=True)
class DatasetSpec:
    """What to build."""

    symbol: str = "SPX"
    universes: tuple[ExpirationUniverse, ...] = DEFAULT_UNIVERSES
    #: The universe whose readings fill the headline ``mm_attributed_*`` columns.
    headline_universe: str = "near_term"
    #: Restrict MM positions to cleanly reconstructed series for the headline
    #: numbers.  The partial-data arm is produced alongside either way.
    clean_only: bool = True
    apply_horizon_weighting: bool = True
    use_net_flow_estimator: bool = False
    confidence_weights: ConfidenceWeights = field(default_factory=ConfidenceWeights)
    recompute_existing: bool = True


@dataclass
class SnapshotRow:
    """One timestamp of the side-by-side comparison."""

    timestamp: str
    trading_date: str
    session_minute: int
    spot: float

    # Existing ZeroGEX (persisted where available, else recomputed).
    existing_dealer_gamma_at_spot: Optional[float] = None
    existing_gamma_flip: Optional[float] = None
    existing_gamma_flip_raw: Optional[float] = None
    existing_call_wall: Optional[float] = None
    existing_put_wall: Optional[float] = None
    existing_call_wall_strength: Optional[float] = None
    existing_put_wall_strength: Optional[float] = None
    existing_net_gex: Optional[float] = None
    existing_composite_score: Optional[float] = None
    existing_max_pain: Optional[float] = None
    existing_put_call_ratio: Optional[float] = None
    existing_source: str = "none"
    #: Relative gap between the persisted production reading and this
    #: harness's recomputation of it from the same chain. Near zero proves
    #: the research path reproduces production; a large value means the two
    #: saw different inputs and the comparison is not yet apples-to-apples.
    existing_recompute_parity_at_spot: Optional[float] = None
    existing_recompute_flip_delta: Optional[float] = None

    # Market-Maker Attributed (headline universe).
    mm_attributed_gamma_at_spot: Optional[float] = None
    mm_attributed_gamma_flip: Optional[float] = None
    mm_attributed_gamma_flip_raw: Optional[float] = None
    mm_attributed_gamma_at_spot_unweighted: Optional[float] = None
    mm_attributed_net_gex: Optional[float] = None
    mm_attributed_call_wall: Optional[float] = None
    mm_attributed_put_wall: Optional[float] = None
    mm_attributed_call_wall_strength: Optional[float] = None
    mm_attributed_put_wall_strength: Optional[float] = None
    mm_attributed_b_call_wall: Optional[float] = None
    mm_attributed_b_put_wall: Optional[float] = None
    mm_accelerant_up: Optional[float] = None
    mm_accelerant_down: Optional[float] = None
    mm_flip_unresolved: bool = True
    mm_regime: Optional[str] = None

    # Diagnostics — the "did the methodology fail, or the data?" columns.
    number_of_contracts: int = 0
    number_of_cleanly_reconstructed_contracts: int = 0
    number_of_unpriceable_contracts: int = 0
    percent_of_gamma_universe_reconstructed: float = 0.0
    clean_gamma_share: float = 0.0
    contribution_0dte: float = 0.0
    contribution_weekly: float = 0.0
    contribution_monthly: float = 0.0
    contribution_leaps: float = 0.0
    inventory_confidence: float = 0.0
    inventory_confidence_band: str = "none"
    mean_position_confidence: float = 0.0
    data_completeness: float = 0.0
    mm_net_contracts_total: float = 0.0
    mm_gross_contracts_total: float = 0.0
    mm_positive_gamma_share: float = 0.0
    mm_negative_gamma_share: float = 0.0
    mm_concentration_hhi: float = 0.0
    estimator_disagreement_contracts: float = 0.0
    universe_gamma_abs: float = 0.0

    # Controls / metadata the backtests split on.
    dte_front: Optional[int] = None
    is_opex: bool = False
    is_month_end: bool = False
    vix_close: Optional[float] = None

    # Per-universe detail, so a 0DTE-only conclusion is reachable without a rerun.
    universes: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------


class InventoryTimeline:
    """Replays classified flow and freezes the inventory at requested instants.

    Streaming and single-pass over the (chronologically ordered) record source:
    ``O(records + snapshots)``, with no re-scan per snapshot.  That is what
    makes a multi-month, minute-resolution study feasible.
    """

    def __init__(
        self,
        symbol: str,
        censoring: Mapping[SeriesKey, tuple[bool, str]],
        listing_dates: Optional[Mapping[SeriesKey, date]] = None,
        missing_sessions: Optional[Mapping[SeriesKey, tuple[date, ...]]] = None,
    ) -> None:
        self.symbol = symbol
        self.censoring = dict(censoring)
        self.listing_dates = dict(listing_dates or {})
        self.missing_sessions = dict(missing_sessions or {})
        self._reconstructor = InventoryReconstructor(symbol=symbol)

    def replay(
        self,
        records: Iterable[ParticipantActivity],
        timestamps: Sequence[datetime],
    ) -> Iterator[tuple[datetime, list[MMPosition]]]:
        """Yield ``(timestamp, live positions)`` for each requested instant.

        Records at exactly the snapshot instant are included: an Open-Close
        bucket stamped 15:30 covers activity up to and including 15:30, so it
        is known at 15:30.
        """
        stamps = sorted(timestamps)
        if not stamps:
            return
        idx = 0
        rec_iter = iter(records)
        pending: Optional[ParticipantActivity] = None

        while idx < len(stamps):
            target = stamps[idx]
            # Drain records up to and including the target instant.
            while True:
                rec = pending if pending is not None else next(rec_iter, None)
                pending = None
                if rec is None:
                    break
                if rec.timestamp > target:
                    pending = rec
                    break
                self._reconstructor.consume((rec,))
            yield target, self._freeze(target)
            idx += 1

    def _freeze(self, as_of: datetime) -> list[MMPosition]:
        """Positions live at ``as_of``, stamped with their window-wide verdicts.

        Expired series are excluded via ZeroGEX's settlement calendar — SPX is
        European and cash-settled, so a settled contract carries no residual
        position and must not keep contributing gamma.
        """
        out: list[MMPosition] = []
        for pos in self._reconstructor.book:
            if as_of >= settlement_instant(pos.symbol, pos.expiration, pos.option_root):
                continue
            if pos.net_contracts == 0.0 and pos.net_flow_contracts == 0.0:
                continue
            verdict = self.censoring.get(pos.key)
            if verdict is not None:
                pos.left_censored, pos.left_censor_reason = verdict
            pos.listing_date = self.listing_dates.get(pos.key, pos.listing_date)
            pos.missing_sessions = self.missing_sessions.get(pos.key, pos.missing_sessions)
            # Detach: the book keeps mutating these objects as the replay
            # advances, so a caller holding this list across snapshots would
            # otherwise read every snapshot as the latest one.
            out.append(pos.snapshot())
        return out


# ---------------------------------------------------------------------------
# Dataset construction
# ---------------------------------------------------------------------------


def _third_friday(d: date) -> bool:
    return d.weekday() == 4 and 15 <= d.day <= 21


def _session_minute(ts: datetime) -> int:
    """Minutes since 09:30 ET.  Negative pre-open, >390 after the close."""
    try:
        from zoneinfo import ZoneInfo

        et = ts.astimezone(ZoneInfo("America/New_York"))
    except Exception:  # pragma: no cover
        import pytz

        et = ts.astimezone(pytz.timezone("US/Eastern"))
    return (et.hour - 9) * 60 + (et.minute - 30)


def _existing_from_chain(
    engine: AnalyticsEngine,
    chain: Sequence[ChainQuote],
    spot: float,
    ts: datetime,
) -> dict[str, Any]:
    """Recompute the production readings from the same chain snapshot.

    Used when ``gex_summary`` has no row for a timestamp, and as the control
    arm generally: same contracts, same IVs, same kernels, only the positioning
    attribution differs.  Values are produced by the unmodified production
    methods, so a recomputed row is directly comparable to a persisted one.
    """
    from src.analytics.walls import compute_call_put_walls_with_strength

    rows = production_profile_rows(chain)
    profile, flip, span = engine._resolve_gamma_flip(rows, spot, ts)
    at_spot = engine._net_gex_at_spot(profile, spot)
    raw_profile = engine._gamma_exposure_profile(
        rows, spot, ts, span_pct=span, apply_dte_weight=False
    )
    flip_raw = engine._calculate_gamma_flip_point(raw_profile, spot)
    strike_rows = engine._calculate_gex_by_strike(rows, spot, ts, recompute_gamma=True)
    call_wall, put_wall, cw_str, pw_str = compute_call_put_walls_with_strength(strike_rows, spot)
    return {
        "existing_dealer_gamma_at_spot": at_spot,
        "existing_gamma_flip": flip,
        "existing_gamma_flip_raw": flip_raw,
        "existing_net_gex": sum(r["net_gex"] for r in strike_rows),
        "existing_call_wall": call_wall,
        "existing_put_wall": put_wall,
        "existing_call_wall_strength": cw_str,
        "existing_put_wall_strength": pw_str,
        "_universe_gamma_abs": sum(abs(r["net_gex"]) for r in strike_rows),
        "existing_source": "recomputed",
    }


def _universe_gamma_abs(
    engine: AnalyticsEngine, chain: Sequence[ChainQuote], spot: float, ts: datetime
) -> float:
    """Total ``|dollar gamma|`` of the FULL production chain — the denominator.

    Turns "we reconstructed 900 series" into "we reconstructed 41% of the gamma
    that exists", which is the only version of that statement worth reporting.
    """
    rows = production_profile_rows(chain)
    strike_rows = engine._calculate_gex_by_strike(rows, spot, ts, recompute_gamma=True)
    return sum(abs(r["net_gex"]) for r in strike_rows)


def build_dataset(
    records_factory: RecordsFactory,
    timestamps: Sequence[datetime],
    chain_provider: ChainProvider,
    spot_provider: Callable[[datetime], Optional[float]],
    *,
    spec: DatasetSpec = DatasetSpec(),
    summary_provider: Optional[SummaryProvider] = None,
    composite_provider: Optional[Callable[[datetime], Optional[float]]] = None,
    vix_provider: Optional[Callable[[datetime], Optional[float]]] = None,
    listing_dates: Optional[Mapping[SeriesKey, date]] = None,
    engine: Optional[AnalyticsEngine] = None,
    progress: Optional[Callable[[int, int], None]] = None,
) -> tuple[list[SnapshotRow], dict[str, Any]]:
    """Build the side-by-side dataset.

    Providers are injected rather than imported so the whole builder is
    testable with synthetic data and no database — the same code path runs in
    tests and in production research.

    Returns ``(rows, provenance)``.  ``provenance`` carries the reconstruction
    summary, so a result set always travels with a description of the data it
    rests on.
    """
    eng = engine or build_engine(spec.symbol)
    stamps = sorted(timestamps)

    # Pass 1 — classify censoring and gaps over the whole window.
    classifier = InventoryReconstructor(symbol=spec.symbol, listing_dates=listing_dates)
    classifier.consume(records_factory()).finalize()
    censoring = {p.key: (p.left_censored, p.left_censor_reason) for p in classifier.book}
    missing = {p.key: p.missing_sessions for p in classifier.book}
    reconstruction_summary = classifier.summary()

    # Data completeness: the share of expected sessions the feed actually
    # covered.  A single number that answers "was the history there at all?"
    rep = classifier.report
    expected_n = len(rep.observed_sessions) + len(rep.session_gaps)
    data_completeness = (len(rep.observed_sessions) / expected_n) if expected_n else 0.0

    # Pass 2 — replay.
    timeline = InventoryTimeline(
        symbol=spec.symbol,
        censoring=censoring,
        listing_dates=listing_dates,
        missing_sessions=missing,
    )

    rows: list[SnapshotRow] = []
    total = len(stamps)
    for i, (ts, positions) in enumerate(timeline.replay(records_factory(), stamps)):
        if progress is not None:
            progress(i + 1, total)
        spot = spot_provider(ts)
        if not spot or spot <= 0:
            continue
        chain = list(chain_provider(ts))
        if not chain:
            continue

        row = _build_row(
            ts=ts,
            spot=float(spot),
            positions=positions,
            chain=chain,
            spec=spec,
            engine=eng,
            summary_provider=summary_provider,
            composite_provider=composite_provider,
            vix_provider=vix_provider,
            data_completeness=data_completeness,
        )
        if row is not None:
            rows.append(row)

    provenance = {
        "spec": {
            "symbol": spec.symbol,
            "headline_universe": spec.headline_universe,
            "clean_only": spec.clean_only,
            "apply_horizon_weighting": spec.apply_horizon_weighting,
            "use_net_flow_estimator": spec.use_net_flow_estimator,
            "universes": [u.name for u in spec.universes],
        },
        "reconstruction": reconstruction_summary,
        "data_completeness": data_completeness,
        "snapshots_requested": len(stamps),
        "snapshots_built": len(rows),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    return rows, provenance


def _build_row(
    *,
    ts: datetime,
    spot: float,
    positions: Sequence[MMPosition],
    chain: Sequence[ChainQuote],
    spec: DatasetSpec,
    engine: AnalyticsEngine,
    summary_provider: Optional[SummaryProvider],
    composite_provider: Optional[Callable[[datetime], Optional[float]]],
    vix_provider: Optional[Callable[[datetime], Optional[float]]],
    data_completeness: float,
) -> Optional[SnapshotRow]:
    as_of = ts.date()
    row = SnapshotRow(
        timestamp=ts.isoformat(),
        trading_date=as_of.isoformat(),
        session_minute=_session_minute(ts),
        spot=spot,
        data_completeness=data_completeness,
    )

    # --- existing methodology -------------------------------------------
    persisted = summary_provider(ts) if summary_provider else None
    universe_abs: Optional[float] = None
    if persisted:
        for key, value in persisted.items():
            if key.startswith("existing_") and hasattr(row, key):
                setattr(row, key, value)
        row.existing_source = "persisted"
    if persisted is None or spec.recompute_existing:
        recomputed = _existing_from_chain(engine, chain, spot, ts)
        universe_abs = recomputed.pop("_universe_gamma_abs", None)
        if persisted is None:
            for key, value in recomputed.items():
                if hasattr(row, key):
                    setattr(row, key, value)
        else:
            # Both available: keep the persisted values as the comparand (they
            # are what the live system actually published) and record how far
            # the recomputation lands from them. This is the integrity check
            # that the research harness reproduces production.
            persisted_spot = row.existing_dealer_gamma_at_spot
            recomputed_spot = recomputed.get("existing_dealer_gamma_at_spot")
            if persisted_spot is not None and recomputed_spot is not None:
                denom = max(abs(persisted_spot), 1e-9)
                row.existing_recompute_parity_at_spot = (recomputed_spot - persisted_spot) / denom
            persisted_flip = row.existing_gamma_flip
            recomputed_flip = recomputed.get("existing_gamma_flip")
            if persisted_flip is not None and recomputed_flip is not None:
                row.existing_recompute_flip_delta = recomputed_flip - persisted_flip
    if universe_abs is None:
        universe_abs = _universe_gamma_abs(engine, chain, spot, ts)
    row.universe_gamma_abs = universe_abs

    if composite_provider is not None:
        row.existing_composite_score = composite_provider(ts)
    if vix_provider is not None:
        row.vix_close = vix_provider(ts)

    # --- MM-attributed ----------------------------------------------------
    selected_positions = (
        [p for p in positions if not p.left_censored] if spec.clean_only else list(positions)
    )
    contracts, unpriceable = join_positions_to_chain(
        selected_positions, chain, use_net_flow=spec.use_net_flow_estimator
    )
    row.number_of_contracts = len(positions)
    row.number_of_cleanly_reconstructed_contracts = sum(1 for p in positions if not p.left_censored)
    row.number_of_unpriceable_contracts = len(unpriceable)
    row.estimator_disagreement_contracts = sum(p.estimator_disagreement for p in positions)

    if not contracts:
        row.universes = {}
        return row

    by_name: dict[str, Any] = {}
    headline_key = spec.headline_universe
    for universe in spec.universes:
        result = compute_mm_gex(
            contracts,
            spot,
            ts,
            engine=engine,
            universe=universe,
            apply_horizon_weighting=spec.apply_horizon_weighting,
            n_unpriceable=len(unpriceable),
        )
        struct = build_strike_structure(result.strike_rows, spot, universe=universe.name)
        by_name[universe.name] = {**result.as_dict(), **struct.as_dict()}
        if universe.name == headline_key:
            _apply_headline(row, result, struct)
    row.universes = by_name

    # --- confidence -------------------------------------------------------
    live_positions = [c.position for c in contracts]
    gkeys = gamma_by_key(contracts, engine, spot, ts)
    conf: UniverseConfidence = score_universe(
        live_positions,
        gamma_by_key=gkeys,
        universe_gamma_abs=universe_abs,
        weights=spec.confidence_weights,
    )
    row.inventory_confidence = conf.gamma_weighted_confidence
    row.inventory_confidence_band = conf.band
    row.mean_position_confidence = conf.mean_position_confidence
    row.percent_of_gamma_universe_reconstructed = conf.pct_gamma_universe_reconstructed

    # --- controls ---------------------------------------------------------
    expirations = sorted({c.expiration for c in contracts})
    if expirations:
        row.dte_front = (expirations[0] - as_of).days
    row.is_opex = _third_friday(as_of)
    row.is_month_end = _is_last_trading_day_of_month(as_of)
    return row


def _is_last_trading_day_of_month(d: date) -> bool:
    """True on the final NYSE session of the month (SPX EOM expiration day).

    Uses ZeroGEX's own holiday table rather than a weekday rule, so a month
    ending on a holiday Friday resolves to the preceding Thursday.
    """
    from src.market_calendar import load_nyse_holidays

    holidays = load_nyse_holidays()
    if d.weekday() >= 5 or d in holidays:
        return False
    cursor = d + timedelta(days=1)
    while cursor.month == d.month:
        if cursor.weekday() < 5 and cursor not in holidays:
            return False
        cursor += timedelta(days=1)
    return True


def _apply_headline(row: SnapshotRow, result: Any, struct: Any) -> None:
    row.mm_attributed_gamma_at_spot = result.mm_attributed_gamma_at_spot
    row.mm_attributed_gamma_flip = result.mm_attributed_gamma_flip
    row.mm_attributed_gamma_flip_raw = result.mm_attributed_gamma_flip_raw
    row.mm_attributed_gamma_at_spot_unweighted = result.mm_attributed_gamma_at_spot_unweighted
    row.mm_attributed_net_gex = result.mm_attributed_net_gex
    row.mm_flip_unresolved = result.flip_unresolved
    row.mm_regime = result.regime
    row.mm_net_contracts_total = result.mm_net_contracts_total
    row.mm_gross_contracts_total = result.mm_gross_contracts_total
    row.clean_gamma_share = (
        result.abs_gamma_clean / result.abs_gamma_total if result.abs_gamma_total else 0.0
    )
    row.contribution_0dte = result.contribution_by_bucket.get("0dte", 0.0)
    row.contribution_weekly = result.contribution_by_bucket.get("weekly", 0.0)
    row.contribution_monthly = result.contribution_by_bucket.get("monthly", 0.0)
    row.contribution_leaps = result.contribution_by_bucket.get("leaps", 0.0)

    row.mm_attributed_call_wall = struct.definition_a.call_wall
    row.mm_attributed_put_wall = struct.definition_a.put_wall
    row.mm_attributed_call_wall_strength = struct.definition_a.call_wall_strength
    row.mm_attributed_put_wall_strength = struct.definition_a.put_wall_strength
    row.mm_attributed_b_call_wall = struct.definition_b.call_wall
    row.mm_attributed_b_put_wall = struct.definition_b.put_wall
    row.mm_accelerant_up = struct.accelerant_up
    row.mm_accelerant_down = struct.accelerant_down
    row.mm_positive_gamma_share = struct.positive_gamma_share
    row.mm_negative_gamma_share = struct.negative_gamma_share
    row.mm_concentration_hhi = struct.concentration_hhi


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def write_jsonl(rows: Iterable[SnapshotRow], path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row.as_dict(), default=str) + "\n")
    return p


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    with p.open("r", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


def write_csv(rows: Sequence[SnapshotRow], path: str | Path) -> Path:
    """Flat CSV of the scalar columns.

    The nested per-universe detail is dropped here by design — CSV is for the
    headline side-by-side comparison; use the JSONL for the full structure.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        p.write_text("", encoding="utf-8")
        return p
    fields = [k for k, v in rows[0].as_dict().items() if not isinstance(v, (dict, list))]
    with p.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: v for k, v in row.as_dict().items() if k in fields})
    return p
