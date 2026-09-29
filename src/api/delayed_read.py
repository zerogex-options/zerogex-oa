"""The ceiling a delayed read is capped at.

The website's free, no-login surfaces (the gamma-levels pages, the embed
widget, llms.txt, /mcp and the public /chart) are sold as delayed.  Until
these reads existed they were delayed only by the website's 15-minute page
cache, so a visitor saw data anywhere from a few seconds to fifteen minutes
old, never the "at least fifteen minutes" every one of those pages states.

A read made with ``delay_minutes`` answers from the database with the newest
data that is at least that old, so the delay holds however the caller caches.
A read without it is the live read, unchanged.

Every table these reads touch is filed in one-minute buckets stamped at the
START of the minute: ``gex_summary.timestamp`` is "the minute bucket" (see
its column comment in schema.sql), the chain rows behind it are "minute
buckets rewritten in place every few seconds", and the 1-minute bars are
start-of-minute stamped.  A bucket stamped T can therefore carry data from as
late as T + 59 s, so capping at ``now - delay`` would let a bucket through
whose newest print is only ``delay - 1`` minutes old.  The ceiling sits one
bucket further back: every bucket at or before it had closed a full
``delay_minutes`` ago.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

#: Largest ``delay_minutes`` a caller may ask for.  A day covers any public
#: display delay; the cap only stops a typo from reaching back through the
#: whole retention window.
MAX_DELAY_MINUTES = 24 * 60

#: Width of the buckets a delayed read selects from.
_BUCKET = timedelta(minutes=1)


def delayed_ceiling(delay_minutes: int, now: Optional[datetime] = None) -> Optional[datetime]:
    """The newest bucket timestamp a read delayed by ``delay_minutes`` may return.

    ``None`` for 0 (or less), which is the live read.  ``now`` is for tests;
    callers leave it unset.
    """
    if delay_minutes <= 0:
        return None
    if now is None:
        now = datetime.now(timezone.utc)
    return now - timedelta(minutes=delay_minutes) - _BUCKET
