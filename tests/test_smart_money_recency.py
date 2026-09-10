"""Freshness of a ranked (top-N-by-size) response.

``/api/flow/smart-money`` returns the fifty largest-notional prints of the
session, ``ORDER BY ABS(notional) DESC ... LIMIT 50``. It is a leaderboard,
not a recency window, so the newest ``timestamp`` among the returned rows is
whichever of the biggest prints landed last — for the index names, usually
the opening burst.

That makes ``MAX(row timestamp)`` a meaningless freshness signal, and an
integrator's automated review hit exactly this: SPX/SPY/QQQ flagged stale
every afternoon on a perfectly healthy feed. The generic envelope scan made
the same wrong assumption, so it reproduced the false positive rather than
fixing it.

The query now also returns ``session_latest_at`` — the max timestamp across
the whole filtered session, computed by a window function BEFORE the ORDER
BY/LIMIT truncates — and the scan treats that as the observation.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytz

from src.api import freshness as fr

ET = pytz.timezone("US/Eastern")


def _et(h, m):
    return ET.localize(datetime(2026, 8, 21, h, m)).astimezone(timezone.utc)


def _leaderboard(session_latest):
    """Biggest prints clustered at the open — the real-world shape."""
    return [
        {
            "timestamp": _et(9, 30) + timedelta(seconds=i * 7),
            "notional": 9_000_000 - i,
            "session_latest_at": session_latest,
        }
        for i in range(50)
    ]


def test_a_healthy_but_top_heavy_leaderboard_is_not_stale_mid_session():
    """The reported false positive. Before the fix this read `stale` at 13:00
    because the largest prints were from the open."""
    now = _et(13, 0)
    f = fr.build_freshness(
        _leaderboard(now - timedelta(seconds=20)),
        profile=fr.resolve_profile("/api/flow/smart-money"),
        now=now,
    )
    assert f.freshness_status is fr.FreshnessStatus.FRESH
    assert f.latest_event_at == now - timedelta(seconds=20)


def test_a_genuinely_stalled_flow_feed_is_still_caught():
    """The fix must not simply suppress the signal — a feed that really has
    stopped mid-session still has to report stale."""
    f = fr.build_freshness(
        _leaderboard(_et(9, 35)),
        profile=fr.resolve_profile("/api/flow/smart-money"),
        now=_et(13, 0),
    )
    assert f.freshness_status is fr.FreshnessStatus.STALE


def test_session_recency_outranks_the_selected_rows():
    """session_latest_at is always >= any row timestamp, so it wins the max()
    without needing its own precedence tier."""
    now = _et(13, 0)
    _, latest = fr._scan_timestamps(_leaderboard(now - timedelta(seconds=5)), now)
    assert latest == now - timedelta(seconds=5)


def test_payloads_without_the_field_are_unaffected():
    """Every other endpoint keeps its existing behaviour."""
    now = _et(13, 0)
    f = fr.build_freshness(
        [{"timestamp": now - timedelta(seconds=30)}],
        profile=fr.resolve_profile("/api/flow/smart-money"),
        now=now,
    )
    assert f.freshness_status is fr.FreshnessStatus.FRESH


def test_the_window_function_precedes_the_limit_in_the_sql():
    """Load-bearing: a window function is evaluated before ORDER BY/LIMIT, so
    MAX(timestamp) OVER () spans the whole session. Move it into a subquery
    that is itself truncated, or into the ORDER BY's scope, and it would
    silently degrade to the max of the fifty ranked rows — reintroducing the
    bug with the field still present and looking correct."""
    sql = Path("src/api/database.py").read_text()
    block = sql[sql.index("async def get_smart_money_flow") :][:8000]
    assert "MAX(timestamp) OVER () AS session_latest_at" in block
    select_at = block.index("MAX(timestamp) OVER ()")
    # "ORDER BY ABS(notional) DESC" matches only the real clause — the comment
    # above the window function says "ORDER BY ABS(notional) means ...".
    order_at = block.index("ORDER BY ABS(notional) DESC")
    limit_at = block.index("LIMIT $4")
    assert select_at < order_at < limit_at, "window function must sit in the pre-LIMIT select"


def test_every_session_spanning_query_can_report_recency():
    """Guard for the class, not just the instance.

    The flaw needs three things at once: rows spanning MANY timestamps, a
    ranking that is not by time, and a LIMIT that truncates. Nearly every
    query here fails the first condition — it pins to a single snapshot
    (``AND gbs.timestamp = ls.timestamp``), so all rows share a timestamp and
    MAX() is trivially correct.

    The ones that genuinely span time are exactly those taking flow session
    bounds. Rather than trying to re-derive "does this span time?" from SQL
    text — which produced false positives on Python comments and on JOIN-style
    snapshot pins — key the invariant off that marker: a session-spanning
    query must either be ordered by time, or return session_latest_at.
    """
    src = Path("src/api/database.py").read_text()
    lines = src.splitlines()

    callers = [
        i
        for i, line in enumerate(lines)
        if "_get_flow_session_bounds(" in line and "def " not in line
    ]
    assert callers, "marker moved — this guard is no longer watching anything"

    for idx in callers:
        start = next(
            j for j in range(idx, -1, -1) if re.match(r"^\s*(?:async\s+)?def\s+\w+", lines[j])
        )
        name = re.match(r"^\s*(?:async\s+)?def\s+(\w+)", lines[start]).group(1)
        end = next(
            (j for j in range(idx, len(lines)) if re.match(r"^    async def ", lines[j])),
            len(lines),
        )
        body = "\n".join(lines[start:end])

        orders = [
            ln.strip()
            for ln in lines[start:end]
            if "ORDER BY" in ln and not ln.strip().startswith(("#", "--"))
        ]
        if not orders:
            continue
        first_key = orders[0].split("ORDER BY", 1)[1].split(",")[0].strip()
        time_ordered = re.search(r"timestamp|_time|_date|created|updated", first_key, re.I)

        assert time_ordered or "session_latest_at" in body, (
            f"{name} spans a flow session and ranks by {first_key!r} rather than time, "
            f"so MAX(row timestamp) is not its recency. It needs a "
            f"session_latest_at column like get_smart_money_flow."
        )
