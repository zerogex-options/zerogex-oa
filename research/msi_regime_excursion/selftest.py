"""End-to-end check of the pipeline against worlds whose answer is known.

**This is not evidence about the market.** Every number here is invented. The
only thing it establishes is that the machinery reports what is actually in the
data: that it finds a relationship when one was built in, reports a null when
none was, and calls an inverted relationship inverted rather than quietly
reporting its absolute value.

Three synthetic worlds, each 40 sessions of minute bars with an MSI attached:

``signal``
    Excursion is drawn with a scale that rises with the MSI. The study must
    return ``supported``.
``null``
    Excursion is drawn independently of the MSI. The study must NOT return
    ``supported`` -- and specifically must not be fooled by the fact that
    readings a minute apart are strongly autocorrelated.
``inverted``
    Excursion FALLS as the MSI rises. The study must say so, not report a
    strong relationship of unspecified sign.

Run::

    python -m research.msi_regime_excursion.cli selftest
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from research.msi_regime_excursion.excursion import ET, Bar, BarSeries
from research.msi_regime_excursion.report import verdict_for
from research.msi_regime_excursion.sources import Instrument, Reading
from research.msi_regime_excursion.study import build_rows, run_study

__all__ = ["build_world", "run_selftest"]

_INSTRUMENT = Instrument("SELFTEST", "SELFTEST", "cash", "SELFTEST", (1.0, 2.0))


def build_world(
    mode: str,
    *,
    sessions: int = 40,
    minutes: int = 300,
    seed: int = 4242,
    base_price: float = 5000.0,
) -> tuple[list[Reading], BarSeries]:
    """Synthesize readings and bars with a known relationship between them.

    The MSI is generated as a slow random walk within each session, which is
    how the real thing behaves -- it does not jump around minute to minute --
    so the autocorrelation the block bootstrap exists to handle is present.
    """
    rng = random.Random(seed)
    readings: list[Reading] = []
    bars: list[Bar] = []
    price = base_price
    day = datetime(2026, 4, 1, 13, 30, tzinfo=timezone.utc)  # 09:30 ET

    for s in range(sessions):
        start = day + timedelta(days=s)
        # Skip weekends so ET session dates stay distinct trading days.
        if start.astimezone(ET).weekday() >= 5:
            continue
        msi = rng.uniform(20.0, 80.0)
        for m in range(minutes):
            ts = start + timedelta(minutes=m)
            # Slow walk, reflected at the edges: adjacent readings are close.
            msi = min(95.0, max(5.0, msi + rng.gauss(0.0, 0.8)))

            if mode == "signal":
                scale = 0.4 + 1.6 * (msi / 100.0)
            elif mode == "inverted":
                scale = 0.4 + 1.6 * (1.0 - msi / 100.0)
            elif mode == "null":
                scale = 1.2
            else:
                raise ValueError(f"unknown mode {mode!r}")

            step = rng.gauss(0.0, scale)
            high = price + abs(rng.gauss(0.0, scale))
            low = price - abs(rng.gauss(0.0, scale))
            close = price + step
            high = max(high, close, price)
            low = min(low, close, price)
            bars.append(Bar(ts=ts, open=price, high=high, low=low, close=close))
            price = close

            readings.append(
                Reading(timestamp=ts, msi=msi, persisted_band=None, components={})
            )
    return readings, BarSeries(bars)


def run_selftest(
    *,
    horizons: tuple[int, ...] = (15,),
    iterations: int = 400,
    verbose: bool = True,
) -> bool:
    """Run all three worlds. Returns True when every expectation holds."""
    expectations = {
        "signal": lambda v: v["verdict"] == "supported",
        "null": lambda v: v["verdict"] != "supported" and not v["inverted"],
        "inverted": lambda v: v["verdict"].startswith("INVERTED"),
    }
    ok = True
    for mode, check in expectations.items():
        readings, series = build_world(mode)
        rows = build_rows(
            readings, series, horizons=horizons, include_rest_of_session=False
        )
        result = run_study(
            _INSTRUMENT, rows, horizons=horizons, iterations=iterations,
            variants=("msi",),
        )
        v = verdict_for(result, horizons[0])
        passed = check(v)
        ok = ok and passed
        if verbose:
            rho = v["rho"]
            print(
                f"  {mode:9} rows={result.n_rows:6,} sessions={result.n_sessions:3}  "
                f"rho={rho:+.4f}  ordered={v['ordered_as_claimed']}  "
                f"inverted={v['inverted']}  -> {v['verdict']:<45} "
                f"[{'PASS' if passed else 'FAIL'}]"
            )
    return ok


def main() -> int:
    print("Pipeline self-test — synthetic worlds, invented numbers, NOT a market result.\n")
    ok = run_selftest()
    print()
    print("All expectations met." if ok else "SELF-TEST FAILED — the machinery is wrong.")
    return 0 if ok else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
