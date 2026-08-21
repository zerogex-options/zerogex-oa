"""Declarative column mapping for exchange Open-Close files.

A :class:`ColumnProfile` is data, not code: it says which source column holds
the strike, which holds the expiration, and — the part that matters — which
columns hold which ``(participant, side, position_effect)`` volume bucket.
Swapping in a corrected mapping once a real Cboe file arrives is a JSON edit,
not a code change.

Why no hard-coded Cboe header
-----------------------------
No Cboe Open-Close sample was available in this repository or environment when
this module was written.  Guessing column names would produce code that looks
finished and silently mis-attributes flow the first time it meets a real file —
the single worst outcome for an experiment whose entire purpose is measuring
attribution quality.  Instead:

* :data:`ALIAS_CANDIDATES` lists the token spellings each concept plausibly
  appears under.  These are *search hints for the proposer*, not assertions
  about the real schema.
* :func:`research.mm_attributed_gex.cboe.inspect.propose_profile` reads an
  actual header and emits a candidate profile plus an explicit list of columns
  it could not map.
* A profile is only usable once :meth:`ColumnProfile.validate` passes, which
  requires every mapped column to exist in the file.

Two built-in profiles are provided.  ``cboe_open_close_v1`` is a *template*:
its column names come from Cboe's published Open-Close Volume Summary field
descriptions as commonly documented, and it is marked ``confirmed=False`` until
a human validates it against a delivered file.  ``generic_long`` handles the
already-tidy shape (one row per participant/side/effect) some vendors emit.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from research.mm_attributed_gex.schema import (
    Exchange,
    Interval,
    ParticipantType,
    PositionEffect,
    Side,
)

__all__ = [
    "VolumeColumn",
    "ColumnProfile",
    "ALIAS_CANDIDATES",
    "PARTICIPANT_TOKENS",
    "SIDE_TOKENS",
    "EFFECT_TOKENS",
    "builtin_profiles",
    "get_profile",
    "profile_to_dict",
    "profile_from_dict",
    "load_profile_file",
    "save_profile_file",
]


@dataclass(frozen=True, slots=True)
class VolumeColumn:
    """One source column that holds a contract count for a single bucket.

    ``column`` is the header name in the file.  The triple
    ``(participant, side, position_effect)`` is what that column means.
    ``size_bucket`` carries Cboe's customer order-size split (``"lt100"``,
    ``"100to199"``, ``"gt199"``) when the feed breaks customer or professional-
    customer volume out that way; it is preserved as provenance and never
    changes the arithmetic, since all three buckets sum into one participant.
    """

    column: str
    participant: ParticipantType
    side: Side
    position_effect: PositionEffect
    size_bucket: Optional[str] = None

    def describe(self) -> str:
        base = f"{self.participant.value}/{self.side.value}/{self.position_effect.value}"
        return f"{base}[{self.size_bucket}]" if self.size_bucket else base


#: Token spellings the proposer searches for.  Ordered most-specific first so
#: ``professional_customer`` is not swallowed by ``customer``.
PARTICIPANT_TOKENS: tuple[tuple[ParticipantType, tuple[str, ...]], ...] = (
    (
        ParticipantType.PROFESSIONAL_CUSTOMER,
        ("professional_customer", "prof_customer", "procust", "professional", "prof"),
    ),
    (
        ParticipantType.MARKET_MAKER,
        ("market_maker", "marketmaker", "mktmaker", "market maker", "maker", "mm"),
    ),
    (
        ParticipantType.BROKER_DEALER,
        ("broker_dealer", "brokerdealer", "broker dealer", "bd"),
    ),
    (ParticipantType.FIRM, ("firm",)),
    (ParticipantType.CUSTOMER, ("customer", "cust")),
)

SIDE_TOKENS: tuple[tuple[Side, tuple[str, ...]], ...] = (
    (Side.BUY, ("buy", "bought", "b")),
    (Side.SELL, ("sell", "sold", "s")),
)

EFFECT_TOKENS: tuple[tuple[PositionEffect, tuple[str, ...]], ...] = (
    (PositionEffect.OPEN, ("open", "opening", "o")),
    (PositionEffect.CLOSE, ("close", "closing", "c")),
)

#: Non-volume field aliases.  Same caveat: hints for the proposer only.
ALIAS_CANDIDATES: Mapping[str, tuple[str, ...]] = {
    "symbol": ("underlying_symbol", "underlying", "root_symbol", "symbol", "ticker"),
    "option_root": ("option_root", "root", "osi_root", "series_root", "product"),
    "expiration": ("expiration", "expiration_date", "expiry", "exp_date", "maturity"),
    "strike": ("strike", "strike_price", "exercise_price"),
    "option_type": ("option_type", "call_put", "call_put_flag", "put_call", "type", "cp_flag"),
    "trading_date": ("trade_date", "date", "trading_date", "business_date", "as_of_date"),
    "interval_end": (
        "interval_end",
        "time",
        "interval",
        "bucket_end",
        "end_time",
        "interval_end_time",
        "timestamp",
    ),
    # Long-format-only columns.
    "participant": ("participant", "participant_type", "account_type", "origin", "customer_type"),
    "side": ("side", "buy_sell", "buy_sell_indicator"),
    "position_effect": ("position_effect", "open_close", "open_close_indicator", "oc_flag"),
    "contracts": ("contracts", "volume", "quantity", "size", "contract_volume"),
}


@dataclass(frozen=True, slots=True)
class ColumnProfile:
    """Everything the loader needs to turn a file's rows into records.

    ``layout`` picks the shape:

    ``"wide"``
        one row per (series, interval) with many volume columns, each named
        for a participant/side/effect bucket.  This is the shape exchange
        Open-Close summaries use.  ``volume_columns`` drives the parse.

    ``"long"``
        one row per (series, interval, participant, side, effect) with a
        single ``contracts`` column.  ``participant_col`` / ``side_col`` /
        ``effect_col`` drive the parse and their cell values are mapped via
        the same token tables.

    ``confirmed`` is the honesty flag.  ``False`` means "this mapping has not
    been checked against a real delivered file"; the loader refuses to run an
    unconfirmed profile unless the caller passes ``allow_unconfirmed=True``,
    so an unverified guess can never quietly become a research result.
    """

    name: str
    layout: str  # "wide" | "long"
    exchange: Exchange = Exchange.CBOE_C1
    interval: Interval = Interval.UNKNOWN
    confirmed: bool = False

    # Series identity columns.
    symbol_col: Optional[str] = None
    option_root_col: Optional[str] = None
    expiration_col: str = "expiration"
    strike_col: str = "strike"
    option_type_col: str = "option_type"
    trading_date_col: str = "trade_date"
    interval_end_col: Optional[str] = None

    # Wide layout.
    volume_columns: tuple[VolumeColumn, ...] = ()

    # Long layout.
    participant_col: Optional[str] = None
    side_col: Optional[str] = None
    effect_col: Optional[str] = None
    contracts_col: Optional[str] = None

    # Parsing knobs.
    date_formats: tuple[str, ...] = ("%Y-%m-%d", "%m/%d/%Y", "%Y%m%d", "%d-%b-%Y")
    time_formats: tuple[str, ...] = ("%H:%M:%S", "%H:%M", "%H%M%S", "%H%M")
    #: Multiply strike by this before use.  Some feeds publish strikes in
    #: thousandths (OSI style).  1.0 = strike column is already in dollars.
    strike_scale: float = 1.0
    #: Fixed symbol when the file has no symbol column (single-underlying file).
    default_symbol: Optional[str] = None
    #: Rows whose symbol is not in this set are dropped.  Empty = keep all.
    symbol_filter: frozenset[str] = frozenset()
    notes: str = ""
    aliases: Mapping[str, str] = field(default_factory=dict, compare=False, hash=False)

    # -- introspection -------------------------------------------------

    def required_columns(self) -> tuple[str, ...]:
        """Columns that must exist in the file for this profile to work."""
        cols: list[str] = [
            self.expiration_col,
            self.strike_col,
            self.option_type_col,
            self.trading_date_col,
        ]
        if self.symbol_col:
            cols.append(self.symbol_col)
        if self.option_root_col:
            cols.append(self.option_root_col)
        if self.interval_end_col:
            cols.append(self.interval_end_col)
        if self.layout == "wide":
            cols.extend(vc.column for vc in self.volume_columns)
        else:
            cols.extend(
                c
                for c in (
                    self.participant_col,
                    self.side_col,
                    self.effect_col,
                    self.contracts_col,
                )
                if c
            )
        # Preserve order, drop duplicates.
        seen: set[str] = set()
        out: list[str] = []
        for c in cols:
            if c not in seen:
                seen.add(c)
                out.append(c)
        return tuple(out)

    def market_maker_columns(self) -> tuple[VolumeColumn, ...]:
        return tuple(
            vc for vc in self.volume_columns if vc.participant is ParticipantType.MARKET_MAKER
        )

    def validate(self, header: Sequence[str]) -> list[str]:
        """Return the list of problems with this profile against ``header``.

        Empty list means usable.  Checks structural coherence first (a wide
        profile with no volume columns is broken regardless of the file), then
        column presence.
        """
        problems: list[str] = []
        if self.layout not in ("wide", "long"):
            problems.append(f"layout must be 'wide' or 'long' (got {self.layout!r})")
        if self.layout == "wide" and not self.volume_columns:
            problems.append("wide layout requires at least one volume column")
        if self.layout == "long":
            missing_spec = [
                name
                for name, value in (
                    ("participant_col", self.participant_col),
                    ("side_col", self.side_col),
                    ("contracts_col", self.contracts_col),
                )
                if not value
            ]
            if missing_spec:
                problems.append(f"long layout requires {', '.join(missing_spec)}")
        if not self.symbol_col and not self.default_symbol:
            problems.append("either symbol_col or default_symbol must be set")

        present = {h.strip() for h in header}
        for col in self.required_columns():
            if col not in present:
                problems.append(f"column {col!r} not found in file header")
        if self.layout == "wide" and not any(
            vc.participant is ParticipantType.MARKET_MAKER for vc in self.volume_columns
        ):
            problems.append(
                "no MARKET_MAKER volume column mapped — this experiment cannot "
                "run without Market Maker classified volume"
            )
        return problems

    def with_confirmed(self, confirmed: bool = True) -> "ColumnProfile":
        """Return a copy flagged as human-confirmed against a real file."""
        return replace(self, confirmed=confirmed)


# ---------------------------------------------------------------------------
# Built-in profiles
# ---------------------------------------------------------------------------


def _cboe_wide_volume_columns() -> tuple[VolumeColumn, ...]:
    """Template MM/customer/firm/BD volume columns in ``<who>_<effect><side>`` form.

    UNCONFIRMED.  Generated from the participant x side x effect cross-product
    using the naming shape Cboe's Open-Close Volume Summary documentation
    describes.  Every one of these names must be checked against a delivered
    file before use — that is what ``confirmed=False`` on the profile means and
    why the loader refuses to run it by default.
    """
    prefixes: tuple[tuple[str, ParticipantType], ...] = (
        ("market_maker", ParticipantType.MARKET_MAKER),
        ("customer", ParticipantType.CUSTOMER),
        ("professional_customer", ParticipantType.PROFESSIONAL_CUSTOMER),
        ("broker_dealer", ParticipantType.BROKER_DEALER),
        ("firm", ParticipantType.FIRM),
    )
    effects: tuple[tuple[str, PositionEffect], ...] = (
        ("open", PositionEffect.OPEN),
        ("close", PositionEffect.CLOSE),
    )
    sides: tuple[tuple[str, Side], ...] = (("buy", Side.BUY), ("sell", Side.SELL))
    return tuple(
        VolumeColumn(
            column=f"{prefix}_{effect_name}_{side_name}",
            participant=participant,
            side=side,
            position_effect=effect,
        )
        for prefix, participant in prefixes
        for effect_name, effect in effects
        for side_name, side in sides
    )


def builtin_profiles() -> dict[str, ColumnProfile]:
    """Profiles shipped with the experiment, keyed by name."""
    return {
        "cboe_open_close_v1": ColumnProfile(
            name="cboe_open_close_v1",
            layout="wide",
            exchange=Exchange.CBOE_C1,
            # Cboe delivers Open-Close as an end-of-day summary or as 1-minute
            # / 10-minute intraday snapshots.  Left UNKNOWN so the operator
            # states which one the delivered files are rather than inheriting
            # a guess that would mislabel every record.
            interval=Interval.UNKNOWN,
            confirmed=False,
            symbol_col="underlying_symbol",
            option_root_col="option_root",
            expiration_col="expiration",
            strike_col="strike",
            option_type_col="call_put",
            trading_date_col="trade_date",
            interval_end_col="interval_end",
            volume_columns=_cboe_wide_volume_columns(),
            default_symbol="SPX",
            symbol_filter=frozenset({"SPX", "SPXW", "$SPX.X"}),
            notes=(
                "TEMPLATE — column names are unconfirmed. Run "
                "`python -m research.mm_attributed_gex.cli inspect-cboe <file>` "
                "against a delivered file, confirm the proposed mapping, then "
                "save it with confirmed=true."
            ),
        ),
        "generic_long": ColumnProfile(
            name="generic_long",
            layout="long",
            exchange=Exchange.UNKNOWN,
            interval=Interval.UNKNOWN,
            confirmed=False,
            symbol_col="underlying_symbol",
            expiration_col="expiration",
            strike_col="strike",
            option_type_col="option_type",
            trading_date_col="trade_date",
            interval_end_col="interval_end",
            participant_col="participant",
            side_col="side",
            effect_col="position_effect",
            contracts_col="contracts",
            notes="Tidy one-row-per-bucket layout; useful for hand-built fixtures.",
        ),
    }


def get_profile(name: str) -> ColumnProfile:
    profiles = builtin_profiles()
    try:
        return profiles[name]
    except KeyError:
        raise KeyError(
            f"Unknown profile {name!r}; built-ins are {sorted(profiles)}. "
            "Pass a JSON profile path instead to use a custom mapping."
        ) from None


# ---------------------------------------------------------------------------
# JSON round-trip — a confirmed mapping is config, and lives in a file
# ---------------------------------------------------------------------------


def profile_to_dict(profile: ColumnProfile) -> dict[str, Any]:
    return {
        "name": profile.name,
        "layout": profile.layout,
        "exchange": profile.exchange.value,
        "interval": profile.interval.value,
        "confirmed": profile.confirmed,
        "symbol_col": profile.symbol_col,
        "option_root_col": profile.option_root_col,
        "expiration_col": profile.expiration_col,
        "strike_col": profile.strike_col,
        "option_type_col": profile.option_type_col,
        "trading_date_col": profile.trading_date_col,
        "interval_end_col": profile.interval_end_col,
        "participant_col": profile.participant_col,
        "side_col": profile.side_col,
        "effect_col": profile.effect_col,
        "contracts_col": profile.contracts_col,
        "date_formats": list(profile.date_formats),
        "time_formats": list(profile.time_formats),
        "strike_scale": profile.strike_scale,
        "default_symbol": profile.default_symbol,
        "symbol_filter": sorted(profile.symbol_filter),
        "notes": profile.notes,
        "volume_columns": [
            {
                "column": vc.column,
                "participant": vc.participant.value,
                "side": vc.side.value,
                "position_effect": vc.position_effect.value,
                "size_bucket": vc.size_bucket,
            }
            for vc in profile.volume_columns
        ],
    }


def profile_from_dict(payload: Mapping[str, Any]) -> ColumnProfile:
    """Rebuild a profile from its JSON form, validating enum members."""
    volume_columns = tuple(
        VolumeColumn(
            column=str(vc["column"]),
            participant=ParticipantType(vc["participant"]),
            side=Side(vc["side"]),
            position_effect=PositionEffect(vc["position_effect"]),
            size_bucket=vc.get("size_bucket"),
        )
        for vc in payload.get("volume_columns", ())
    )
    defaults = ColumnProfile(name="_", layout="wide")
    return ColumnProfile(
        name=str(payload.get("name", "custom")),
        layout=str(payload.get("layout", "wide")),
        exchange=Exchange(payload.get("exchange", Exchange.CBOE_C1.value)),
        interval=Interval(payload.get("interval", Interval.UNKNOWN.value)),
        confirmed=bool(payload.get("confirmed", False)),
        symbol_col=payload.get("symbol_col"),
        option_root_col=payload.get("option_root_col"),
        expiration_col=str(payload.get("expiration_col", defaults.expiration_col)),
        strike_col=str(payload.get("strike_col", defaults.strike_col)),
        option_type_col=str(payload.get("option_type_col", defaults.option_type_col)),
        trading_date_col=str(payload.get("trading_date_col", defaults.trading_date_col)),
        interval_end_col=payload.get("interval_end_col"),
        volume_columns=volume_columns,
        participant_col=payload.get("participant_col"),
        side_col=payload.get("side_col"),
        effect_col=payload.get("effect_col"),
        contracts_col=payload.get("contracts_col"),
        date_formats=tuple(payload.get("date_formats", defaults.date_formats)),
        time_formats=tuple(payload.get("time_formats", defaults.time_formats)),
        strike_scale=float(payload.get("strike_scale", 1.0)),
        default_symbol=payload.get("default_symbol"),
        symbol_filter=frozenset(payload.get("symbol_filter", ()) or ()),
        notes=str(payload.get("notes", "")),
    )


def load_profile_file(path: str | Path) -> ColumnProfile:
    """Load a profile from JSON, or resolve a built-in name."""
    p = Path(path)
    if not p.exists():
        return get_profile(str(path))
    with p.open("r", encoding="utf-8") as fh:
        return profile_from_dict(json.load(fh))


def save_profile_file(profile: ColumnProfile, path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as fh:
        json.dump(profile_to_dict(profile), fh, indent=2, sort_keys=True)
        fh.write("\n")
    return p


def unmapped_columns(profile: ColumnProfile, header: Iterable[str]) -> list[str]:
    """Columns in ``header`` the profile ignores.

    Surfaced by the inspector so a reviewer can see at a glance whether a
    participant category or size bucket was silently dropped.
    """
    mapped = set(profile.required_columns())
    return [h for h in header if h.strip() not in mapped]
