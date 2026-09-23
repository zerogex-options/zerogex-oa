"""Is the futures carry fallback configured correctly?

``theoretical_ratio`` is the basis used whenever no concurrent index/futures
print pair is available — which is most of the overnight session, exactly when
the futures display swap is live. It is only as good as ``RISK_FREE_RATE`` and
the per-symbol dividend yield, and both are easy to leave at defaults that are
silently wrong: ``DIVIDEND_YIELD`` ships at 0.0, and the
``DIVIDEND_YIELD_BY_SYMBOL`` example in ``.env.example`` historically listed
QQQ and SPY without the NDX and SPX keys this actually resolves by.

This prints what the configuration implies, beside what the tape says, so the
gap is visible. The measured side is the same median-of-concurrent-pairs the
basis engine uses, over the last few days of ``futures_quotes`` against
``underlying_quotes``.

Run it with ``make futures-carry-check``. Read-only.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from math import log

from src.config import RISK_FREE_RATE, resolve_dividend_yield
from src.database import db_connection
from src.jobs.futures_projection import active_contract_expiry, theoretical_ratio
from src.symbols import get_index_futures

_MEASURE_SQL = """
    SELECT f.close::numeric / u.close::numeric AS ratio
    FROM futures_quotes f
    JOIN underlying_quotes u
      ON u.symbol = f.index_symbol AND u.timestamp = f.timestamp
    WHERE f.index_symbol = %s
      AND f.timestamp >= NOW() - interval '4 days'
      AND u.close > 0
    ORDER BY f.timestamp DESC
    LIMIT 500
"""


def _measured_ratio(index_symbol: str):
    """Median of the newest concurrent print pairs, or None if the tape is dry."""
    try:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute(_MEASURE_SQL, (index_symbol,))
            ratios = sorted(float(r[0]) for r in cur.fetchall())
    except Exception as e:  # a dead read must not fail the whole report
        # First line only: connection errors here carry multi-line setup
        # instructions that bury the table this report exists to print.
        first_line = str(e).strip().splitlines()[0] if str(e).strip() else type(e).__name__
        print(f"  ({index_symbol}: could not read futures_quotes — {first_line})")
        return None
    if not ratios:
        return None
    mid = len(ratios) // 2
    return ratios[mid] if len(ratios) % 2 else (ratios[mid - 1] + ratios[mid]) / 2


def main() -> int:
    now = datetime.now(timezone.utc)
    expiry = active_contract_expiry(now)
    years = max((expiry - now.date()).days, 0) / 365.0

    print(f"Active contract expiry: {expiry}  ({years:.4f}y out)")
    print(f"RISK_FREE_RATE        : {RISK_FREE_RATE:.4f}\n")
    print(
        f"{'index':6} {'q':>7} {'carry r-q':>10} {'theoretical':>12} "
        f"{'measured':>10} {'gap (bps)':>10}  verdict"
    )

    worst = 0.0
    for index_symbol in sorted(get_index_futures()):
        q = resolve_dividend_yield(index_symbol)
        theo = theoretical_ratio(index_symbol, now)
        meas = _measured_ratio(index_symbol)
        if meas is None:
            print(
                f"{index_symbol:6} {q:>7.4f} {RISK_FREE_RATE - q:>10.4f} {theo:>12.5f} "
                f"{'--':>10} {'--':>10}  no concurrent prints to compare"
            )
            continue
        gap_bps = (theo - meas) * 10000
        worst = max(worst, abs(gap_bps))
        if abs(gap_bps) < 25:
            verdict = "ok"
        elif q == 0.0:
            verdict = f"CHECK — q is 0; set DIVIDEND_YIELD_BY_SYMBOL['{index_symbol}']"
        else:
            verdict = "CHECK — r or q is off; the tape disagrees"
        implied = log(meas) / years if years > 0 else float("nan")
        print(
            f"{index_symbol:6} {q:>7.4f} {RISK_FREE_RATE - q:>10.4f} {theo:>12.5f} "
            f"{meas:>10.5f} {gap_bps:>10.1f}  {verdict}"
        )
        print(f"{'':6} {'':>7} {'tape implies r-q =':>10} {implied:>7.4f}")

    print(
        "\nThe measured column is what the basis engine uses when a concurrent "
        "pair exists.\nThe theoretical column is what it falls back to when one "
        "does not — most of\nthe overnight session. A large gap means the "
        "fallback publishes levels on a\ndifferent axis than the live path does."
    )
    return 1 if worst >= 25 else 0


if __name__ == "__main__":
    sys.exit(main())
