"""End-to-end plumbing check on synthetic data.

Proves the layers connect: a file is parsed, inventory is reconstructed,
gamma is computed with the production kernels, a side-by-side dataset is built,
the experiment battery runs and a report renders.

**This is not evidence about the methodology.**  The inputs are invented, so
every number it produces is a property of the generator, not of the market.
The return payload carries ``"synthetic": True`` and the CLI prints a warning,
because a plumbing check that gets quoted as a result is worse than no check
at all.

The generator is deliberately crude and *stationary in its flow process*: MM
positioning is drawn independently of the price path, so the battery run over
it should find nothing.  If a run of this ever reports a strong effect, the
harness has a leak.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional

import numpy as np

from research.mm_attributed_gex.backtest import ExperimentConfig, run_experiment
from research.mm_attributed_gex.dataset import DatasetSpec, build_dataset
from research.mm_attributed_gex.gex import ChainQuote, build_engine
from research.mm_attributed_gex.outcomes import Bar, BarSeries
from research.mm_attributed_gex.report import decide, render_markdown
from research.mm_attributed_gex.schema import (
    Exchange,
    Interval,
    ParticipantActivity,
    ParticipantType,
    PositionEffect,
    Side,
)

__all__ = ["synthetic_records", "synthetic_chain", "synthetic_bars", "run_pipeline_check"]

try:
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    import pytz

    ET = pytz.timezone("US/Eastern")  # type: ignore[assignment]


def _et(d: date, hh: int, mm: int) -> datetime:
    return datetime.combine(d, time(hh, mm), tzinfo=ET).astimezone(timezone.utc)


def synthetic_records(
    sessions: list[date],
    expirations: list[date],
    strikes: list[float],
    *,
    symbol: str = "SPX",
    seed: int = 20260821,
    buckets_per_session: int = 13,
) -> list[ParticipantActivity]:
    """Invented Open-Close activity with a zero-sum customer counterparty.

    Every MM bucket is mirrored by an opposite-side Customer bucket, so the
    zero-sum reconciliation check has something coherent to verify — the point
    of the check is that the harness detects violations, which it can only
    demonstrate against data that satisfies the identity.
    """
    rng = np.random.default_rng(seed)
    out: list[ParticipantActivity] = []
    for session in sessions:
        for bucket in range(buckets_per_session):
            stamp = _et(session, 9, 30) + timedelta(minutes=30 * bucket)
            for expiration in expirations:
                if expiration < session:
                    continue
                for strike in strikes:
                    for option_type in ("C", "P"):
                        for effect, side in (
                            (PositionEffect.OPEN, Side.BUY),
                            (PositionEffect.OPEN, Side.SELL),
                            (PositionEffect.CLOSE, Side.BUY),
                            (PositionEffect.CLOSE, Side.SELL),
                        ):
                            qty = int(rng.integers(0, 60))
                            if qty == 0:
                                continue

                            def _make(
                                participant: ParticipantType, rec_side: Side
                            ) -> ParticipantActivity:
                                return ParticipantActivity(
                                    exchange=Exchange.CBOE_C1,
                                    symbol=symbol,
                                    expiration=expiration,
                                    strike=strike,
                                    option_type=option_type,
                                    participant=participant,
                                    side=rec_side,
                                    position_effect=effect,
                                    contracts=qty,
                                    timestamp=stamp,
                                    trading_date=session,
                                    interval=Interval.MINUTE_30,
                                    option_root="SPXW",
                                )

                            out.append(_make(ParticipantType.MARKET_MAKER, side))
                            # Mirrored customer leg with the opposite side and
                            # the same position effect, so the zero-sum and
                            # open-interest checks have coherent data to verify.
                            out.append(
                                _make(
                                    ParticipantType.CUSTOMER,
                                    Side.SELL if side is Side.BUY else Side.BUY,
                                )
                            )
    out.sort(key=lambda r: r.timestamp)
    return out


def synthetic_chain(
    expirations: list[date],
    strikes: list[float],
    *,
    iv: float = 0.16,
    open_interest: int = 5_000,
) -> list[ChainQuote]:
    """A chain snapshot with a crude smile, so gamma is not flat across strikes."""
    out: list[ChainQuote] = []
    mid = float(np.median(strikes))
    for expiration in expirations:
        stamp = expiration.strftime("%y%m%d")
        for strike in strikes:
            moneyness = abs(strike - mid) / mid
            local_iv = iv * (1.0 + 2.0 * moneyness)
            for option_type in ("C", "P"):
                out.append(
                    ChainQuote(
                        option_symbol=f"SPXW {stamp}{option_type}{int(strike):08d}",
                        strike=strike,
                        expiration=expiration,
                        option_type=option_type,
                        implied_volatility=local_iv,
                        gamma=None,
                        open_interest=open_interest,
                        volume=1_000,
                    )
                )
    return out


def synthetic_bars(
    sessions: list[date],
    *,
    start_price: float = 6000.0,
    seed: int = 4242,
    minutes: int = 390,
) -> list[Bar]:
    """A random-walk price path, independent of the synthetic flow by design."""
    rng = np.random.default_rng(seed)
    bars: list[Bar] = []
    price = start_price
    for session in sessions:
        base = _et(session, 9, 30)
        for i in range(minutes):
            step = float(rng.normal(0.0, 1.6))
            close = max(1.0, price + step)
            high = max(price, close) + abs(float(rng.normal(0.0, 0.6)))
            low = min(price, close) - abs(float(rng.normal(0.0, 0.6)))
            bars.append(Bar(base + timedelta(minutes=i), price, high, low, close))
            price = close
    return bars


def run_pipeline_check(symbol: str = "SPX", *, n_sessions: int = 6) -> dict[str, Any]:
    """Run every layer once on synthetic inputs and report what happened."""
    base = date(2026, 6, 1)
    sessions = [
        base + timedelta(days=i)
        for i in range(n_sessions)
        if (base + timedelta(days=i)).weekday() < 5
    ]
    expirations = [sessions[-1], sessions[-1] + timedelta(days=7)]
    strikes = [5900.0 + 25.0 * i for i in range(9)]

    records = synthetic_records(sessions, expirations, strikes, symbol=symbol)
    chain = synthetic_chain(expirations, strikes)
    bars = synthetic_bars(sessions)
    series = BarSeries(bars)
    engine = build_engine(symbol)

    stamps: list[datetime] = []
    for session in sessions:
        for minute in range(0, 360, 15):
            stamps.append(_et(session, 9, 45) + timedelta(minutes=minute))

    spot_lookup = {b.ts: b.close for b in bars}
    sorted_ts = sorted(spot_lookup)

    def spot_provider(ts: datetime) -> Optional[float]:
        import bisect

        i = bisect.bisect_right(sorted_ts, ts) - 1
        return spot_lookup[sorted_ts[i]] if i >= 0 else None

    def chain_provider(ts: datetime):
        return [q for q in chain if q.expiration >= ts.date()]

    rows, provenance = build_dataset(
        lambda: iter(records),
        stamps,
        chain_provider,
        spot_provider,
        spec=DatasetSpec(symbol=symbol, headline_universe="all", clean_only=False),
        engine=engine,
    )

    result = run_experiment(
        [r.as_dict() for r in rows],
        series,
        config=ExperimentConfig(horizons=(15, 30), n_boot=200),
        sampling_minutes=15,
    )
    verdict = decide(result)
    markdown = render_markdown(result, provenance=provenance)

    non_null_mm = sum(1 for r in rows if r.mm_attributed_gamma_at_spot is not None)
    return {
        "ok": bool(rows) and non_null_mm > 0 and bool(markdown),
        "synthetic": True,
        "records": len(records),
        "chain_quotes": len(chain),
        "bars": len(bars),
        "snapshots_requested": len(stamps),
        "dataset_rows": len(rows),
        "rows_with_mm_gamma": non_null_mm,
        "rows_with_mm_flip": sum(1 for r in rows if r.mm_attributed_gamma_flip is not None),
        "rows_with_existing_gamma": sum(
            1 for r in rows if r.existing_dealer_gamma_at_spot is not None
        ),
        "scored_observations": result.n_scored,
        "verdict_code": verdict.code,
        "verdict_label": verdict.label,
        "report_chars": len(markdown),
        "provenance": provenance,
        "warning": (
            "Synthetic inputs. This validates plumbing only and is not evidence "
            "about Market-Maker Attributed GEX."
        ),
    }
