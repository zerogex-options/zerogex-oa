"""Single source of truth for reading an underlying bar's volume.

``underlying_quotes`` carries two different facts that used to be one.

``up_volume`` / ``down_volume`` are a **classification** -- a tick test,
run by the feed, on its own consolidated bar stream. ``schema.sql`` says so
where it defines ``underlying_buying_pressure``, and the "Buying" /
"Selling" labels were already walked back once for claiming more precision
than a tick test has.

``volume`` is the **total** traded during the bar. Until 2026-09 there was
no such column, so every caller that wanted the total wrote
``up_volume + down_volume`` -- correct only for as long as one vendor
happened to supply both numbers. ThetaData supplies the total and cannot
supply the split, which turned a dormant conflation into four broken views
and six broken API endpoints at the vendor cutover.

Hence two fragments, and the discipline of importing them rather than
spelling either one out again:

``TOTAL_VOLUME``
    The total. Prefers the measured column and falls back to the split's
    sum, so rows written before the column existed keep reading.

``UPTICK_SHARE_PCT``
    The classification's share, as a percentage -- and ``NULL`` when there
    is no classification to take a share of.

The second one carries the sharper lesson. It used to read
``COALESCE(up / NULLIF(up + down, 0) * 100, 50)``, which answers ``50``
both when nothing traded (fair -- no ticks either way is balanced) and when
the feed cannot classify at all (not fair -- that publishes a measured,
confident, permanently neutral tape on a column nobody would think to
re-check). The 50 is kept for the first case and refused for the second.
See ``docs/compliance/signal-component-inert-gate-sweep-2026-09.md`` for
the other three places this same shape was found.

Every string here is a literal SQL fragment with no user input in it, the
same rule :mod:`src.api.queries._sql_helpers` states: a caller picks a
column prefix, never supplies SQL.
"""

from __future__ import annotations

import re
from typing import Final

#: Total volume traded during the bar, for an unprefixed ``underlying_quotes``.
TOTAL_VOLUME: Final[str] = "COALESCE(volume, up_volume + down_volume)"

#: Uptick share of classified volume, or NULL when the feed cannot classify.
UPTICK_SHARE_PCT: Final[str] = (
    "CASE WHEN up_volume IS NULL OR down_volume IS NULL THEN NULL "
    "ELSE ROUND(COALESCE("
    "up_volume::numeric / NULLIF((up_volume + down_volume)::numeric, 0) * 100, 50"
    "), 2) END"
)


def total_volume(prefix: str = "") -> str:
    """:data:`TOTAL_VOLUME` qualified by a table alias, e.g. ``total_volume("q.")``."""
    return _qualify(TOTAL_VOLUME, prefix)


def uptick_share_pct(prefix: str = "") -> str:
    """:data:`UPTICK_SHARE_PCT` qualified by a table alias."""
    return _qualify(UPTICK_SHARE_PCT, prefix)


#: Word-anchored on purpose. A plain ``"volume"`` replacement also matches
#: inside ``up_volume`` and ``down_volume`` and yields ``up_q.volume``; ``\b``
#: does not match between ``_`` and ``v``, both being word characters, so the
#: three names stay distinct.
_COLUMN_RE: Final = re.compile(r"\b(?:up_volume|down_volume|volume)\b")


def _qualify(fragment: str, prefix: str) -> str:
    if not prefix:
        return fragment
    if not prefix.endswith("."):
        prefix += "."
    return _COLUMN_RE.sub(lambda m: prefix + m.group(0), fragment)
