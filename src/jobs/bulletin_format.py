"""How the bulletin X-posts write numbers: rounded the way a person says them.

One place for both the text Python assembles (:mod:`src.jobs.bulletin_tweet`:
the key-levels lists) and the numbers the writer is told to quote
(:mod:`src.jobs.bulletin_llm`), so the list and the prose can't round
differently.

  * Prices and levels: the nearest whole dollar, with "~" in front when that
    rounds something off ("745", "~747" for 747.29, "~7,483" for 7,482.71).
  * Dollar amounts (net gamma, charm flow): whole units of the largest scale
    ("$6B" for 6.28 billion, "$340M", "$12K").
  * Percent changes: one decimal ("+0.4%" for 0.39%).

Rounding is half up on the exact value, as the website's card rounds.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

_SCALES = ((Decimal(10**9), "B"), (Decimal(10**6), "M"), (Decimal(10**3), "K"))


def _half_up(value: Decimal, step: str = "1") -> Decimal:
    return value.quantize(Decimal(step), rounding=ROUND_HALF_UP)


def price(value: float) -> str:
    """A price or level: "745", "~747" (747.29), "~7,483" (7,482.71).

    The "~" goes on only when the rounding drops at least a cent, so float
    noise on a whole strike (744.996) still reads as an exact "745"."""
    exact = Decimal(value)
    whole = _half_up(exact)
    approx = "" if _half_up(exact, "0.01") == whole else "~"
    return f"{approx}{whole:,}"


def dollars(value: float) -> str:
    """An unsigned dollar amount in whole units: "$6B", "$340M", "$12K", "$500".

    A value that rounds up into the next scale is written in it: 999.6 million
    is "$1B", not "$1000M"."""
    amount = abs(Decimal(value))
    for i, (scale, suffix) in enumerate(_SCALES):
        if amount >= scale:
            units = _half_up(amount / scale)
            if units >= 1000 and i > 0:
                bigger, bigger_suffix = _SCALES[i - 1]
                return f"${_half_up(amount / bigger)}{bigger_suffix}"
            return f"${units}{suffix}"
    units = _half_up(amount)
    if units >= 1000:
        return "$1K"
    return f"${units}"


def signed_dollars(value: float) -> str:
    """:func:`dollars` with a plain "+" or "-" in front: "+$6B", "-$340M"."""
    return f"{'+' if value >= 0 else '-'}{dollars(value)}"


def pct(value: float) -> str:
    """A percent change to one decimal, signed: "+0.4%", "-1.2%", "0.0%"."""
    rounded = _half_up(Decimal(value), "0.1")
    if rounded == 0:
        return "0.0%"
    return f"{'+' if rounded > 0 else '-'}{abs(rounded)}%"
