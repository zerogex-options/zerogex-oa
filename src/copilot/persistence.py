"""Sync persistence + input-fetching helpers for the Copilot.

The Playbook cycle runs synchronously with psycopg2 connections (see
``src/signals/playbook/cycle.py``). This module mirrors that convention
so the regime classifier can fetch its inputs and persist its output
without round-tripping through the async stack.

Every function here follows the established convention from
``insert_action_card_sync``: log on failure, never raise — a DB hiccup
must not break the signal cycle.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from typing import Optional

from .regime_narrative import RegimeNarrative

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def insert_regime_narrative_sync(conn, narrative: RegimeNarrative) -> None:
    """psycopg2 INSERT for regime_narratives — best-effort.

    Mirrors the shape of ``insert_action_card_sync``: never raises, logs a
    warning on failure, and rolls back the connection so a transactional
    error doesn't poison the surrounding cycle.

    The ``payload`` JSONB column carries the full ``to_dict()`` projection
    so downstream consumers (chat agent, replay UI) get the same structure
    as the live ``/api/copilot/regime/{symbol}`` endpoint.
    """
    if narrative is None:
        return
    if not narrative.symbol or narrative.timestamp is None:
        return
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO regime_narratives
                (underlying, timestamp, label, confidence, spot,
                 msi_regime, payload)
            SELECT %s, %s, %s, %s, %s, %s, %s::jsonb
            WHERE NOT EXISTS (
                SELECT 1 FROM regime_narratives
                WHERE underlying = %s AND timestamp = %s
            )
            """,
            (
                narrative.symbol,
                narrative.timestamp,
                narrative.label,
                float(narrative.confidence),
                float(narrative.spot),
                narrative.msi_regime or "",
                json.dumps(narrative.to_dict(), default=str),
                narrative.symbol,
                narrative.timestamp,
            ),
        )
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning(
            "insert_regime_narrative_sync failed (%s): %s", narrative.symbol, exc
        )


# ---------------------------------------------------------------------------
# Input-fetching helpers
#
# Each helper takes a sync psycopg2 connection plus the parameters the
# classifier needs. They never raise — failures return None so the
# classifier falls through to the UNDEFINED branch.
# ---------------------------------------------------------------------------


def query_prior_regime_sync(conn, symbol: str) -> Optional[RegimeNarrative]:
    """Fetch the most recent ``RegimeNarrative`` for ``symbol``.

    Used for the hysteresis check in ``classify_regime``. Returns ``None``
    on any failure (missing row, JSON error, DB error) — the classifier
    treats ``prior=None`` as no-prior, so this fail-open default is safe.

    Only the fields ``classify_regime`` actually reads are reconstructed
    (``label``, ``confidence``). The rest are placeholders — callers
    must not depend on ``inputs_snapshot`` or ``what_would_flip_it`` here.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT label, confidence, spot, msi_regime, timestamp, payload
            FROM regime_narratives
            WHERE underlying = %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
        if not row:
            return None
        label, confidence, spot, msi_regime, ts, payload = row
        return RegimeNarrative(
            timestamp=ts,
            symbol=symbol,
            label=str(label),
            confidence=float(confidence),
            spot=float(spot),
            expected_behavior="",
            favored_patterns=[],
            avoid=[],
            what_would_flip_it="",
            msi_regime=str(msi_regime or ""),
            inputs_snapshot={},
        )
    except Exception as exc:
        logger.debug("query_prior_regime_sync failed: %s", exc)
        return None


def query_vix_change_pct_sync(conn) -> Optional[float]:
    """Day-over-day VIX change as a percent.

    Reads from ``vix_bars`` if available; falls back silently when the
    table or rows are missing. Returns ``None`` when there isn't enough
    history for a comparison.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT close
            FROM vix_bars
            ORDER BY timestamp DESC
            LIMIT 2
            """,
        )
        rows = cur.fetchall()
        if len(rows) < 2:
            return None
        latest = float(rows[0][0])
        prior = float(rows[1][0])
        if prior == 0.0:
            return None
        return ((latest - prior) / prior) * 100.0
    except Exception as exc:
        logger.debug("query_vix_change_pct_sync failed: %s", exc)
        return None


def query_realized_vol_30m_sync(conn, symbol: str) -> Optional[float]:
    """Annualized realized vol from the last 30 minutes of 1-min closes.

    Uses ``underlying_quotes``; returns ``None`` when we don't have at
    least 10 bars (a tighter floor than strictly necessary, but it
    suppresses noisy estimates at session open).
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT last
            FROM underlying_quotes
            WHERE symbol = %s
              AND timestamp >= NOW() - INTERVAL '30 minutes'
            ORDER BY timestamp ASC
            """,
            (symbol,),
        )
        closes = [float(r[0]) for r in cur.fetchall() if r[0] is not None]
        if len(closes) < 10:
            return None
        log_returns: list[float] = []
        for i in range(1, len(closes)):
            prev = closes[i - 1]
            curr = closes[i]
            if prev > 0 and curr > 0:
                log_returns.append(math.log(curr / prev))
        if len(log_returns) < 5:
            return None
        mean = sum(log_returns) / len(log_returns)
        variance = sum((r - mean) ** 2 for r in log_returns) / len(log_returns)
        stdev = math.sqrt(variance)
        # Annualize: 252 trading days * 390 minutes per session.
        return stdev * math.sqrt(252.0 * 390.0)
    except Exception as exc:
        logger.debug("query_realized_vol_30m_sync failed: %s", exc)
        return None


def query_max_pain_convergence_sync(
    conn,
    symbol: str,
    *,
    lookback_minutes: int = 10,
) -> Optional[float]:
    """Signed change in ``|spot - max_pain|`` over the last N minutes.

    Negative ⇒ spot converging toward max pain. Returns ``None`` when
    the lookback window doesn't have a baseline row.

    Reads from ``regime_narratives`` directly — its ``payload.inputs``
    contains both fields at every prior cycle, so we don't need a
    separate join.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT spot, payload
            FROM regime_narratives
            WHERE underlying = %s
              AND timestamp >= NOW() - (%s || ' minutes')::interval
            ORDER BY timestamp ASC
            LIMIT 1
            """,
            (symbol, str(int(lookback_minutes))),
        )
        baseline_row = cur.fetchone()
        if not baseline_row:
            return None

        cur.execute(
            """
            SELECT spot, payload
            FROM regime_narratives
            WHERE underlying = %s
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (symbol,),
        )
        latest_row = cur.fetchone()
        if not latest_row:
            return None

        def _distance(spot, payload) -> Optional[float]:
            try:
                inputs = (payload or {}).get("inputs") or {}
                mp = inputs.get("max_pain")
                if mp is None or spot is None:
                    return None
                return abs(float(spot) - float(mp))
            except (TypeError, ValueError, AttributeError):
                return None

        baseline_distance = _distance(baseline_row[0], baseline_row[1])
        latest_distance = _distance(latest_row[0], latest_row[1])
        if baseline_distance is None or latest_distance is None:
            return None
        return latest_distance - baseline_distance
    except Exception as exc:
        logger.debug("query_max_pain_convergence_sync failed: %s", exc)
        return None
