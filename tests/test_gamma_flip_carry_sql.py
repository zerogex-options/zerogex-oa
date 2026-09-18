"""The gamma-flip observation query, against a real Postgres.

The rest of the carry suite mocks the cursor, so the part that actually decides
whether a bar counts as MEASURED is never run: the 5-minute bucketing, the
in-bar "newest row wins" pick, the session bounds, and -- the load-bearing one
-- ``(ARRAY_AGG(gamma_flip_point ORDER BY timestamp DESC))[1]`` keeping a NULL
rather than skipping to the newest non-NULL value beneath it.

That last point is the whole distinction :mod:`src.analytics.gamma_flip_carry`
exists to draw, and it is one line of SQL away from being lost: ``MAX()``, or
any ``IGNORE NULLS``-flavoured pick, would reach past the NULL to an earlier
crossing in the same bar and turn "the profile is one-signed" back into a
level -- exactly the over-correction the carry must not make. Only a real
database can say whether the query written has the semantics argued for.

Four claims, each asserted against real rows:

1. A bar with no ``gex_summary`` row is ABSENT from the result -- that absence
   is how a missing window is told apart from a measured NULL.
2. A bar whose newest row has a NULL ``gamma_flip_point`` is PRESENT with NULL,
   even when an earlier row in the same bar carried a value.
3. Newest-in-bar wins, so the bar reports the profile as it last stood.
4. The window is closed at both ends: a row before the open cannot leak in, and
   the last bar's own five minutes are in scope.

Run it against a SCRATCH database:

    make gamma-flip-carry-sql GAMMA_FLIP_CARRY_DSN=postgres://...

The DSN's database must already have ``setup/database/schema.sql`` applied. It
is deliberately NOT auto-derived from ``.env``: this harness WRITES, and a
target that silently defaults to production is one `make` away from seeding
rows into it. It seeds under a sentinel symbol and removes everything it
created, including the ``symbols`` row the ``gex_summary`` foreign key needs.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from src.analytics.gamma_flip_carry import GAMMA_FLIP_OBSERVATIONS_SQL, resolve_session_flips

pytestmark = pytest.mark.integration

_DSN = os.getenv("GAMMA_FLIP_CARRY_DSN")
_SYMBOL = os.getenv("GAMMA_FLIP_CARRY_SYMBOL", "ZZFLIP")
_ET = ZoneInfo("America/New_York")
_SESSION = datetime(2026, 6, 11, 9, 30, tzinfo=_ET).astimezone(timezone.utc)

_SKIP_REASON = (
    "GAMMA_FLIP_CARRY_DSN not set — integration harness skipped "
    "(run via `make gamma-flip-carry-sql`)."
)


def _bars(n):
    return [_SESSION + timedelta(minutes=5 * i) for i in range(n)]


@pytest.fixture()
def observations():
    """Seed one session with every shape the resolver has to tell apart."""
    import psycopg2

    bars = _bars(8)
    rows = [
        # bar 0: three rows; the newest one is what the bar reports.
        (bars[0] + timedelta(seconds=30), 688.0),
        (bars[0] + timedelta(minutes=2), 689.0),
        (bars[0] + timedelta(minutes=4), 690.0),
        # bar 1: nothing at all -- the missing window.
        # bar 2: one row that measured no crossing.
        (bars[2] + timedelta(minutes=1), None),
        # bar 3: a value, then a NULL. Newest wins, so the bar is NULL.
        (bars[3] + timedelta(minutes=1), 695.0),
        (bars[3] + timedelta(minutes=3), None),
        # bar 5: an ordinary reading.
        (bars[5] + timedelta(minutes=2), 701.5),
        # bar 7 is the last bar; a row inside its own five minutes must count.
        (bars[7] + timedelta(minutes=4), 704.0),
        # Out of scope on both sides.
        (_SESSION - timedelta(minutes=30), 111.0),
        (bars[7] + timedelta(minutes=6), 999.0),
    ]

    conn = psycopg2.connect(_DSN)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO symbols (symbol) VALUES (%s) ON CONFLICT DO NOTHING", (_SYMBOL,)
            )
            cur.executemany(
                """
                INSERT INTO gex_summary (underlying, timestamp, gamma_flip_point)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                """,
                [(_SYMBOL, ts, value) for ts, value in rows],
            )
        with conn, conn.cursor() as cur:
            cur.execute(
                GAMMA_FLIP_OBSERVATIONS_SQL,
                {"symbol": _SYMBOL, "session_start": bars[0], "session_end": bars[7]},
            )
            yield bars, cur.fetchall()
    finally:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM gex_summary WHERE underlying = %s", (_SYMBOL,))
            cur.execute("DELETE FROM symbols WHERE symbol = %s", (_SYMBOL,))
        conn.close()


@pytest.mark.skipif(_DSN is None, reason=_SKIP_REASON)
def test_only_bars_with_a_source_row_are_emitted(observations):
    bars, rows = observations

    assert [bar for bar, _ in rows] == [bars[0], bars[2], bars[3], bars[5], bars[7]]


@pytest.mark.skipif(_DSN is None, reason=_SKIP_REASON)
def test_the_newest_row_in_the_bar_wins_and_a_null_is_kept(observations):
    """The claim the whole distinction rests on. Bar 3 holds 695.0 followed by
    a NULL: the bar must report NULL, because that is what the profile last
    said, not 695.0 because it is the newest value that happens to exist."""
    _, rows = observations

    assert [value for _, value in rows] == [690.0, None, None, 701.5, 704.0]


@pytest.mark.skipif(_DSN is None, reason=_SKIP_REASON)
def test_the_window_is_closed_at_both_ends(observations):
    """A reading from before the open would be yesterday's market; one after
    the last bar's own five minutes would be a bar that has not happened."""
    bars, rows = observations
    values = [value for _, value in rows]

    assert 111.0 not in values and 999.0 not in values
    assert rows[-1] == (bars[7], 704.0), "the last bar's own window was excluded"


@pytest.mark.skipif(_DSN is None, reason=_SKIP_REASON)
def test_the_resolution_over_real_rows_matches_the_mocked_suite(observations):
    """End to end: the query's output, fed to the resolver, produces the carry
    the unit tests describe."""
    bars, rows = observations
    resolved = resolve_session_flips(bars, rows)

    assert resolved[bars[1]].flip == 690.0 and resolved[bars[1]].carried
    assert resolved[bars[2]].flip is None and resolved[bars[2]].measured
    assert resolved[bars[4]].flip is None and resolved[bars[4]].carried
    assert resolved[bars[6]].flip == 701.5 and resolved[bars[6]].stale_bars == 1
