"""Point-in-time gamma structure, and confluence against a price.

This is the module the whole study's credibility rests on.  Its one job is to
answer *"what gamma structure was already there, before price arrived?"* in a
way that cannot accidentally answer *"what gamma structure is there now that
price has arrived?"* — because those two questions have very different answers
and only the first one is tradeable.

Three hazards, and what is done about each.

1. **``gex_summary.timestamp`` is not when the level existed.**
   It is the option-chain instant the levels were computed FROM
   (``src/analytics/main_engine.py:3953``).  The engine then computes, then
   writes (``created_at``), and the client polls up to 30 s later.  So a frame
   stamped 10:13:00 was not on anyone's chart at 10:13:00.
   :class:`GammaSnapshot` carries all three instants and
   :class:`GammaTimeline` orders and searches on ``available_at``, chosen by
   :attr:`~research.or_gamma_confluence.config.ResearchConfig.availability_clock`.

2. **Walls and GEX ranks re-centre on spot.**  Production's own definition:
   the Call Wall is the biggest call-gamma strike *above spot*
   (``src/analytics/walls.py:249``), so when price trades through a strike
   that strike becomes the Put Wall and the Call Wall re-points upward.  A
   ladder read at the touch is therefore partly CAUSED by the touch.  Ranks
   are recomputed here against the spot in force **at the snapshot**, never
   the touch spot, and each level kind is tagged :data:`RECENTERING_KINDS` so
   the "does this survive if we only use non-chasing levels?" control is one
   filter rather than a separate study.

3. **A missing or suspicious frame must fail closed.**  No ``created_at``
   under a publish-based clock is unusable, not "assume it was fine".  A
   publish lag beyond
   :attr:`~research.or_gamma_confluence.config.ResearchConfig.max_publish_lag_seconds`
   means the row was backfilled — its ``created_at`` is the backfill time, not
   a publish time — and the whole session is refused rather than measured
   against a fictional clock.

Nothing here interpolates.  A step function is the only honest reading of a
level that is republished on a cadence and holds in between, which is exactly
what :class:`src.analytics.wall_breaks.StepSeries` implements; the lookup here
is the same right-continuous search with a lead-time offset applied first.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping, Optional, Sequence

from research.or_gamma_confluence.config import CLOCK_DATA, CLOCK_PUBLISHED, ResearchConfig
from src.analytics.walls import align_wall_ladder, compute_wall_ladder

__all__ = [
    "KIND_CALL_WALL",
    "KIND_PUT_WALL",
    "KIND_GAMMA_FLIP",
    "KIND_GAMMA_FLIP_RAW",
    "KIND_MAX_PAIN",
    "KIND_PIN_STRIKE",
    "KIND_MAX_GAMMA",
    "KIND_GEX_CALL_RANK",
    "KIND_GEX_PUT_RANK",
    "LEVEL_KINDS",
    "RECENTERING_KINDS",
    "GammaLevel",
    "GammaSnapshot",
    "GammaTimeline",
    "Confluence",
    "SnapshotRejected",
    "build_snapshot",
    "build_timeline",
    "TimelineBuild",
    "build_timeline_tolerant",
    "confluence_at",
]

KIND_CALL_WALL = "call_wall"
KIND_PUT_WALL = "put_wall"
KIND_GAMMA_FLIP = "gamma_flip"
KIND_GAMMA_FLIP_RAW = "gamma_flip_raw"
KIND_MAX_PAIN = "max_pain"
KIND_PIN_STRIKE = "pin_strike"
KIND_MAX_GAMMA = "max_gamma_strike"
KIND_GEX_CALL_RANK = "gex_call_rank"
KIND_GEX_PUT_RANK = "gex_put_rank"

LEVEL_KINDS: tuple[str, ...] = (
    KIND_CALL_WALL,
    KIND_PUT_WALL,
    KIND_GAMMA_FLIP,
    KIND_GAMMA_FLIP_RAW,
    KIND_MAX_PAIN,
    KIND_PIN_STRIKE,
    KIND_MAX_GAMMA,
    KIND_GEX_CALL_RANK,
    KIND_GEX_PUT_RANK,
)

#: Level kinds whose DEFINITION is relative to spot, so they migrate as price
#: moves.  From the source email: *"Call Wall is 'the biggest call gamma strike
#: above spot'.  When price goes through a strike, that strike flips to Put
#: Wall and the Call Wall re-points to the next one up. ... Max Pain and VWAP
#: are the two that don't chase."*
#:
#: These are the kinds where "the level was there first" is a claim that has to
#: be enforced rather than assumed, and restricting a cohort to the complement
#: is the cleanest available control for the whole re-centring hazard.
RECENTERING_KINDS: frozenset[str] = frozenset(
    {
        KIND_CALL_WALL,
        KIND_PUT_WALL,
        KIND_GEX_CALL_RANK,
        KIND_GEX_PUT_RANK,
        KIND_MAX_GAMMA,
    }
)


class SnapshotRejected(RuntimeError):
    """A frame (or a session) cannot be reconstructed point-in-time.

    Raised rather than returned so a caller cannot ignore it by accident.  The
    dataset layer catches it per session and records the reason; that is the
    fail-closed path the brief requires.
    """


@dataclass(frozen=True)
class GammaLevel:
    """One structural price level published by a gamma snapshot."""

    kind: str
    price: float
    #: 1-based rank within its side, for the ranked GEX ladder.  ``None`` for
    #: singleton levels (flip, max pain, pin).
    rank: Optional[int] = None
    #: Dollar-gamma magnitude where the source supplies one.  ``None`` is
    #: "not published", never zero.
    strength: Optional[float] = None
    #: Display label: ``C1``/``P4`` for the ladder, the kind otherwise.
    label: str = ""

    @property
    def recenters(self) -> bool:
        return self.kind in RECENTERING_KINDS

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "price": self.price,
            "rank": self.rank,
            "strength": self.strength,
            "label": self.label or self.kind,
            "recenters": self.recenters,
        }


@dataclass(frozen=True)
class GammaSnapshot:
    """One gamma frame, with every clock it can be judged by."""

    #: ``gex_summary.timestamp`` — the option-chain instant.
    data_ts: datetime
    #: ``gex_summary.created_at`` — the database write.  ``None`` on a
    #: deployment that predates the column being populated.
    created_at: Optional[datetime]
    #: The instant this frame counts as knowable, per the configured clock.
    available_at: datetime
    #: Which clock produced ``available_at`` — persisted on every event so a
    #: result can be re-read under the assumption that produced it.
    clock: str
    #: Spot in force at ``data_ts``, used to rank the GEX ladder.  ``None``
    #: when no bar is available, which disables the ranked levels for this
    #: frame rather than ranking against the wrong price.
    spot: Optional[float]

    levels: tuple[GammaLevel, ...] = ()

    # Regime scalars, read straight from the frame.  Dollar quantities are
    # NEVER projected onto a futures axis (src/jobs/futures_projection.py:94).
    total_net_gex: Optional[float] = None
    net_gex_at_spot: Optional[float] = None
    flip_distance: Optional[float] = None
    local_gex: Optional[float] = None
    convexity_risk: Optional[float] = None
    call_wall_strength: Optional[float] = None
    put_wall_strength: Optional[float] = None
    pin_score: Optional[float] = None
    pin_confidence: Optional[float] = None

    #: False when ``gex_by_strike`` had no rows for this frame — the ranked
    #: ladder is then simply absent, and every cohort that does not need it
    #: still uses the frame.
    gex_ranks_available: bool = False

    @property
    def publish_lag_seconds(self) -> Optional[float]:
        if self.created_at is None:
            return None
        return (self.created_at - self.data_ts).total_seconds()

    def level_prices(self) -> dict[str, float]:
        return {lv.label or lv.kind: lv.price for lv in self.levels}

    def gamma_flip(self) -> Optional[float]:
        for lv in self.levels:
            if lv.kind == KIND_GAMMA_FLIP:
                return lv.price
        return None

    def regime_sign(self) -> Optional[int]:
        """Sign of dealer gamma at spot: ``+1`` / ``-1`` / ``0``.

        Prefers ``net_gex_at_spot`` — the schema is explicit that this is "the
        regime-correct 'dealer gamma here' figure", while ``total_net_gex`` is
        the whole-chain endpoint and "can carry the opposite sign when far-OTM
        strikes dominate the tail".  Both are stored so the choice can be
        revisited; only this one drives the regime cohorts.
        """
        value = self.net_gex_at_spot
        if value is None:
            value = self.total_net_gex
        if value is None:
            return None
        return 0 if value == 0 else (1 if value > 0 else -1)

    def to_audit(self) -> dict[str, Any]:
        """The provenance blob persisted on every event that used this frame."""
        return {
            "gamma_data_ts": self.data_ts.isoformat(),
            "gamma_created_at": self.created_at.isoformat() if self.created_at else None,
            "gamma_available_at": self.available_at.isoformat(),
            "gamma_clock": self.clock,
            "gamma_publish_lag_s": self.publish_lag_seconds,
            "gamma_spot": self.spot,
            "gex_ranks_available": self.gex_ranks_available,
        }


def _f(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None  # NaN -> None


def _available_at(
    data_ts: datetime, created_at: Optional[datetime], cfg: ResearchConfig
) -> datetime:
    """When this frame counts as knowable, per the configured clock.

    Raises :class:`SnapshotRejected` rather than silently degrading to the
    optimistic clock when the publish instant is required but missing — that
    degradation is precisely the bug the clock exists to prevent.
    """
    if cfg.availability_clock == CLOCK_DATA:
        return data_ts
    if created_at is None:
        raise SnapshotRejected(
            f"created_at missing at {data_ts.isoformat()}; "
            f"clock={cfg.availability_clock} requires a publish instant"
        )
    lag = (created_at - data_ts).total_seconds()
    if lag < 0:
        raise SnapshotRejected(
            f"created_at precedes timestamp by {-lag:.0f}s at {data_ts.isoformat()}"
        )
    if lag > cfg.max_publish_lag_seconds:
        raise SnapshotRejected(
            f"publish lag {lag:.0f}s exceeds max {cfg.max_publish_lag_seconds}s at "
            f"{data_ts.isoformat()} — row looks backfilled, so created_at is not "
            f"a publish time"
        )
    if cfg.availability_clock == CLOCK_PUBLISHED:
        return created_at
    return created_at + timedelta(seconds=cfg.client_poll_lag_seconds)


def build_snapshot(
    row: Mapping[str, Any],
    cfg: ResearchConfig,
    *,
    spot: Optional[float] = None,
    strike_rows: Optional[Sequence[Mapping[str, Any]]] = None,
) -> GammaSnapshot:
    """One ``gex_summary`` row (+ optional per-strike rows) as a frozen frame.

    ``spot`` must be the underlying's price **at this frame's own timestamp**.
    It is used only to rank the GEX ladder, and passing the touch-time spot
    instead would reintroduce exactly the re-centring look-ahead this module
    exists to prevent — so it is a required-by-contract argument rather than
    something derived here from ambient state.
    """
    data_ts = row["timestamp"]
    created_at = row.get("created_at")
    available_at = _available_at(data_ts, created_at, cfg)

    levels: list[GammaLevel] = []

    def _add(kind: str, value: Any, *, strength: Any = None, label: str = "") -> None:
        price = _f(value)
        if price is None or price <= 0:
            return
        levels.append(
            GammaLevel(
                kind=kind,
                price=price,
                strength=_f(strength),
                label=label or kind.upper(),
            )
        )

    _add(KIND_CALL_WALL, row.get("call_wall"), strength=row.get("call_wall_strength"))
    _add(KIND_PUT_WALL, row.get("put_wall"), strength=row.get("put_wall_strength"))
    _add(KIND_GAMMA_FLIP, row.get("gamma_flip_point"))
    _add(KIND_GAMMA_FLIP_RAW, row.get("gamma_flip_raw"))
    _add(KIND_MAX_PAIN, row.get("max_pain"))
    _add(KIND_PIN_STRIKE, row.get("pin_strike"))
    _add(KIND_MAX_GAMMA, row.get("max_gamma_strike"))

    ranks_available = False
    if cfg.use_gex_ranks and strike_rows and spot is not None and spot > 0:
        call_ladder, put_ladder = compute_wall_ladder(
            strike_rows, float(spot), depth=cfg.gex_ladder_depth
        )
        # Reconcile C1/P1 with the frame's own published wall.  Production does
        # the same (``align_wall_ladder``) because the engine's wall was
        # computed against the spot IT held, which can differ from the bar
        # close we use here by enough to move a strike across spot.  Without
        # this the ladder can disagree with the wall drawn beside it.
        call_ladder = align_wall_ladder(
            call_ladder, _f(row.get("call_wall")), "call", depth=cfg.gex_ladder_depth
        )
        put_ladder = align_wall_ladder(
            put_ladder, _f(row.get("put_wall")), "put", depth=cfg.gex_ladder_depth
        )
        for kind, ladder in ((KIND_GEX_CALL_RANK, call_ladder), (KIND_GEX_PUT_RANK, put_ladder)):
            for entry in ladder:
                price = _f(entry.get("strike"))
                if price is None or price <= 0:
                    continue
                levels.append(
                    GammaLevel(
                        kind=kind,
                        price=price,
                        rank=int(entry["rank"]),
                        strength=_f(entry.get("strength")),
                        label=str(entry.get("label") or ""),
                    )
                )
        ranks_available = bool(call_ladder or put_ladder)

    return GammaSnapshot(
        data_ts=data_ts,
        created_at=created_at,
        available_at=available_at,
        clock=cfg.availability_clock,
        spot=_f(spot),
        levels=tuple(levels),
        total_net_gex=_f(row.get("total_net_gex")),
        net_gex_at_spot=_f(row.get("net_gex_at_spot")),
        flip_distance=_f(row.get("flip_distance")),
        local_gex=_f(row.get("local_gex")),
        convexity_risk=_f(row.get("convexity_risk")),
        call_wall_strength=_f(row.get("call_wall_strength")),
        put_wall_strength=_f(row.get("put_wall_strength")),
        pin_score=_f(row.get("pin_score")),
        pin_confidence=_f(row.get("pin_confidence")),
        gex_ranks_available=ranks_available,
    )


class GammaTimeline:
    """Availability-ordered snapshots with a lead-enforcing as-of lookup.

    Ordered and searched on ``available_at``, not ``data_ts``.  Those two
    orderings normally agree, and where they do not it is the availability
    order that describes what a trader could have known — which is the only
    ordering this study is entitled to use.
    """

    __slots__ = ("_stamps", "_snapshots")

    def __init__(self, snapshots: Iterable[GammaSnapshot]) -> None:
        ordered = sorted(snapshots, key=lambda s: s.available_at)
        self._snapshots: list[GammaSnapshot] = ordered
        self._stamps: list[datetime] = [s.available_at for s in ordered]

    def __len__(self) -> int:
        return len(self._snapshots)

    def __iter__(self):
        return iter(self._snapshots)

    @property
    def snapshots(self) -> list[GammaSnapshot]:
        return list(self._snapshots)

    def as_of(self, ts: datetime, min_lead_seconds: int) -> Optional[GammaSnapshot]:
        """The newest frame available at least ``min_lead_seconds`` before ``ts``.

        ``None`` means no qualifying frame exists — the caller must then treat
        the observation as having NO pre-existing gamma information, never
        reach for the next frame.  ``bisect_right`` on a cutoff is what makes
        that guarantee structural: a frame stamped exactly at the cutoff
        qualifies, one stamped a microsecond later cannot be reached.
        """
        cutoff = ts - timedelta(seconds=max(0, int(min_lead_seconds)))
        idx = bisect.bisect_right(self._stamps, cutoff) - 1
        return self._snapshots[idx] if idx >= 0 else None

    def coverage(self) -> dict[str, Any]:
        """Frame density and clock diagnostics for the run's provenance."""
        if not self._snapshots:
            return {"frames": 0}
        lags = [s.publish_lag_seconds for s in self._snapshots if s.publish_lag_seconds is not None]
        lags.sort()
        return {
            "frames": len(self._snapshots),
            "first_available_at": self._stamps[0].isoformat(),
            "last_available_at": self._stamps[-1].isoformat(),
            "with_gex_ranks": sum(1 for s in self._snapshots if s.gex_ranks_available),
            "publish_lag_median_s": lags[len(lags) // 2] if lags else None,
            "publish_lag_max_s": lags[-1] if lags else None,
        }


@dataclass(frozen=True)
class Confluence:
    """Every gamma level near one price, with distances kept raw.

    Distances are stored in the instrument's own price units and NOT bucketed.
    Thresholds are a reporting decision applied in :mod:`.cohorts`, so no
    rebuild is needed to ask whether the answer depends on calling it 10
    points versus 9 — which the brief asks specifically to be able to check.
    """

    #: ``(level, signed_distance)`` — signed so "the wall is ABOVE the
    #: extension" is recoverable.  Ascending by absolute distance.
    matches: tuple[tuple[GammaLevel, float], ...]

    @property
    def nearest(self) -> Optional[tuple[GammaLevel, float]]:
        return self.matches[0] if self.matches else None

    def within(
        self, distance: float, *, kinds: Optional[frozenset[str]] = None
    ) -> list[GammaLevel]:
        """Levels within ``distance`` points, optionally restricted by kind."""
        return [
            lv
            for lv, d in self.matches
            if abs(d) <= distance and (kinds is None or lv.kind in kinds)
        ]

    def count_within(self, distance: float, *, kinds: Optional[frozenset[str]] = None) -> int:
        return len(self.within(distance, kinds=kinds))

    def distinct_kinds_within(self, distance: float) -> int:
        """How many DIFFERENT structures agree here.

        The email's example — Call Wall, Max Pain, Pin Strike and GEX 1 all on
        29530 — is four kinds on one strike, which is a stronger statement
        than four ranked GEX levels that happen to be adjacent.  Counting
        kinds rather than levels is what separates those two.
        """
        return len({lv.kind for lv, d in self.matches if abs(d) <= distance})

    def to_dict(self, buckets: Sequence[float]) -> dict[str, Any]:
        near = self.nearest
        out: dict[str, Any] = {
            "nearest_gamma_level_type": near[0].kind if near else None,
            "nearest_gamma_level_label": (near[0].label or near[0].kind) if near else None,
            "nearest_gamma_level_price": near[0].price if near else None,
            "nearest_gamma_distance": abs(near[1]) if near else None,
            "nearest_gamma_distance_signed": near[1] if near else None,
            "nearest_gamma_rank": near[0].rank if near else None,
            "nearest_gamma_strength": near[0].strength if near else None,
            "nearest_gamma_recenters": near[0].recenters if near else None,
            "gamma_levels_total": len(self.matches),
        }
        for b in buckets:
            key = f"{b:g}".replace(".", "p")
            out[f"gamma_confluence_count_{key}"] = self.count_within(b)
            out[f"gamma_confluence_kinds_{key}"] = self.distinct_kinds_within(b)
        return out


def confluence_at(
    snapshot: GammaSnapshot,
    price: float,
    *,
    max_distance: Optional[float] = None,
) -> Confluence:
    """Distances from ``price`` to every level in ``snapshot``.

    ``max_distance`` trims the stored list for size; it is a storage bound,
    not a confluence threshold.  Leave it generous — the point of keeping raw
    distances is that the threshold is chosen later.
    """
    matches: list[tuple[GammaLevel, float]] = []
    for lv in snapshot.levels:
        d = lv.price - price
        if max_distance is not None and abs(d) > max_distance:
            continue
        matches.append((lv, d))
    matches.sort(key=lambda m: abs(m[1]))
    return Confluence(matches=tuple(matches))


def build_timeline(
    rows: Sequence[Mapping[str, Any]],
    cfg: ResearchConfig,
    *,
    spot_at: Any = None,
    strikes_at: Any = None,
) -> GammaTimeline:
    """Build a timeline from ``gex_summary`` rows, strictly.

    ``spot_at(ts) -> float | None`` and ``strikes_at(ts) -> rows`` are
    callables the caller supplies so this module stays free of the database.
    Both are asked for the FRAME's timestamp, never a touch timestamp.

    Any :class:`SnapshotRejected` propagates.  Use
    :func:`build_timeline_tolerant` for the dataset path, which drops isolated
    bad frames and fails the session only when too many are unusable.
    """
    return GammaTimeline(
        build_snapshot(
            row,
            cfg,
            spot=spot_at(row["timestamp"]) if spot_at is not None else None,
            strike_rows=strikes_at(row["timestamp"]) if strikes_at is not None else None,
        )
        for row in rows
        if row.get("timestamp") is not None
    )


@dataclass(frozen=True)
class TimelineBuild:
    """A timeline plus an account of what could not be used."""

    timeline: GammaTimeline
    accepted: int
    rejected: int
    #: Reason string -> count, for the run's provenance.
    reasons: dict[str, int]

    @property
    def total(self) -> int:
        return self.accepted + self.rejected

    @property
    def rejected_frac(self) -> float:
        return (self.rejected / self.total) if self.total else 0.0


def _reason_key(exc: SnapshotRejected) -> str:
    """Collapse a rejection message to a countable category."""
    text = str(exc)
    for needle, key in (
        ("created_at missing", "created_at_missing"),
        ("precedes timestamp", "created_at_before_timestamp"),
        ("publish lag", "publish_lag_backfill"),
    ):
        if needle in text:
            return key
    return "other"


def build_timeline_tolerant(
    rows: Sequence[Mapping[str, Any]],
    cfg: ResearchConfig,
    *,
    spot_at: Any = None,
    strikes_at: Any = None,
) -> TimelineBuild:
    """Build a timeline, dropping individual frames that cannot be trusted.

    Isolated bad frames are DROPPED rather than taken as evidence against the
    whole session.  A step function is exactly the right structure for that:
    the previous frame simply stays in force across the gap, which is what a
    consumer would have seen anyway if the publish had failed.

    The session-level judgement is left to the caller, on
    :attr:`TimelineBuild.rejected_frac` against
    ``cfg.max_rejected_frame_frac`` — a clock that is broken for a few frames
    is a stall, one that is broken for a fifth of the day is not a clock.

    Measured on production, this distinction is worth roughly a session per
    symbol: SPY carries a single 1618-second publish and QQQ a single
    negative-lag row across ~59 sessions each.  Failing the whole session for
    one frame would discard ~3% of the longest history available, and for a
    reason ("the level was published late") that the availability clock
    already handles correctly.
    """
    snapshots: list[GammaSnapshot] = []
    reasons: dict[str, int] = {}
    rejected = 0
    for row in rows:
        if row.get("timestamp") is None:
            continue
        try:
            snapshots.append(
                build_snapshot(
                    row,
                    cfg,
                    spot=spot_at(row["timestamp"]) if spot_at is not None else None,
                    strike_rows=(strikes_at(row["timestamp"]) if strikes_at is not None else None),
                )
            )
        except SnapshotRejected as exc:
            rejected += 1
            key = _reason_key(exc)
            reasons[key] = reasons.get(key, 0) + 1
    return TimelineBuild(
        timeline=GammaTimeline(snapshots),
        accepted=len(snapshots),
        rejected=rejected,
        reasons=reasons,
    )
