"""Best-effort fetchers for the edge metrics the retired fleet never used.

The original :class:`MarketSnapshot` only carried FIRST-ORDER, static
positioning levels — walls, gamma flip, VWAP, max-pain, net-GEX. The whole
retired fleet traded those levels and the backtest found no edge in any of
them. ZeroGEX, however, *also* computes three richer layers that never
reached the bot tier:

* **Aggressor-classified option order flow** (``flow_series_5min`` — signed
  buy/sell premium and volume). Real money leaning a direction before price
  confirms it.
* **Second-order forced dealer flow** (``gex_by_strike.dealer_vanna_exposure``
  / ``dealer_charm_exposure`` and ``forced_flow_profile.close_charm_flow``).
  The *mechanical* hedging dealers must do as time passes (charm) and as
  implied vol moves (vanna) — a scheduled, direction-known flow, not a
  narrative.
* **Pin Strike** (``gex_summary.pin_strike`` — the gamma-restoring,
  reachability-filtered magnet, distinct from the OI-based max-pain the
  fleet used).

Every fetch here mirrors the semantics of
:func:`src.tradeworkz.context._fetch_trade_bias`:

* it is **best-effort** — any failure (missing table, thin history, aborted
  read) returns ``None`` fields and NEVER aborts the snapshot build;
* it is **as-of aware** — every read is bounded to
  ``timestamp <= COALESCE(as_of, NOW())`` so the backtest harness (which
  rebuilds the snapshot at a past instant) can never see a future row;
* it is **transaction-isolated** — each read runs inside its own SAVEPOINT
  and rolls back to it on error, so a hiccup here can only null these
  fields, never poison the caller's connection.

These are inputs to the *candidate* edge bots in
``src/tradeworkz/bots/`` (charm_close_magnet, vanna_vol_crush_rider,
aggressor_flow_divergence, gamma_regime_shift_rider). They are additive:
existing bots ignore the new fields and behave exactly as before.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_ET = ZoneInfo("America/New_York")


def _savepoint(cur: Any, name: str) -> bool:
    try:
        cur.execute(f"SAVEPOINT {name}")
        return True
    except Exception:  # pragma: no cover - defensive
        return False


def _rollback(cur: Any, name: str, armed: bool) -> None:
    if not armed:
        return
    try:
        cur.execute(f"ROLLBACK TO SAVEPOINT {name}")
    except Exception:  # pragma: no cover - defensive
        pass


def _release(cur: Any, name: str, armed: bool) -> None:
    if not armed:
        return
    try:
        cur.execute(f"RELEASE SAVEPOINT {name}")
    except Exception:  # pragma: no cover - defensive
        pass


def _f(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def fetch_forced_flow(
    conn: Any, underlying: str, as_of: Optional[datetime] = None
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Latest modeled forced-hedging levels for ``underlying``.

    Returns ``(close_charm_flow, close_charm_flow_raw, close_charm_flow_smooth,
    charm_flip, vanna_flip)`` from the freshest ``forced_flow_profile`` row
    at-or-before ``as_of``:

    * ``close_charm_flow`` — dollars of stock dealers must trade by the cash
      close if spot holds (the Bulletin headline figure). Prefers the
      ``close_charm_flow_smooth`` first-order value (drops the noisy same-day
      expiry resolution) and falls back to the raw ``close_charm_flow``.
      **Sign convention: positive => dealers must BUY into the close.**
    * ``close_charm_flow_raw`` / ``close_charm_flow_smooth`` — the two source
      figures SEPARATELY. The raw value is the exact full-reprice flow
      *including* same-day 0DTE strikes resolving to intrinsic at the close;
      the smooth value is the first-order charm drift only. Their difference
      isolates the pure settlement-unwind leg (see ``settlement_flow_snap``).
    * ``charm_flip`` — spot level where time-decay hedging flips sign.
    * ``vanna_flip`` — spot level where vol-driven hedging flips sign.

    All ``None`` when the table has no row for this symbol or the read errors.
    """
    cur = conn.cursor()
    armed = _savepoint(cur, "tw_forced_flow")
    try:
        cur.execute(
            """
            SELECT close_charm_flow, close_charm_flow_smooth, charm_flip, vanna_flip
            FROM forced_flow_profile
            WHERE underlying = %s AND timestamp <= COALESCE(%s::timestamptz, NOW())
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (underlying, as_of),
        )
        row = cur.fetchone()
        _release(cur, "tw_forced_flow", armed)
    except Exception:
        _rollback(cur, "tw_forced_flow", armed)
        logger.debug("fetch_forced_flow failed for %s", underlying, exc_info=True)
        return (None, None, None, None, None)
    if not row:
        return (None, None, None, None, None)
    raw, smooth, charm_flip, vanna_flip = row
    raw_f, smooth_f = _f(raw), _f(smooth)
    close_charm = smooth_f if smooth_f is not None else raw_f
    return (close_charm, raw_f, smooth_f, _f(charm_flip), _f(vanna_flip))


def fetch_second_order_totals(
    conn: Any, underlying: str, as_of: Optional[datetime] = None
) -> Tuple[Optional[float], Optional[float]]:
    """Aggregate dealer vanna / charm exposure for the front expiration.

    Sums ``gex_by_strike.dealer_vanna_exposure`` and
    ``dealer_charm_exposure`` across every strike of the NEAREST (front)
    expiration at the latest snapshot timestamp at-or-before ``as_of`` — the
    same ``SUM(dealer_*_exposure)`` quantity the platform's
    ``vanna_charm_flow`` component and its normalizer cache use.

    Sign convention (dealer perspective, per the schema comments):
      * ``dealer_vanna_exposure > 0`` => dealers BUY underlying as IV rises
        (and sell as IV falls);
      * ``dealer_charm_exposure > 0`` => dealers BUY underlying as time passes.

    Legacy rows without the dealer-sign columns fall back to negating the
    market-aggregate ``vanna_exposure`` / ``charm_exposure`` (the documented
    market->dealer convention). Restricting to the front expiration keeps the
    read focused on the near-dated book that actually drives intraday hedging
    rather than summing far-dated exposure that never gets traded today.

    Returns ``(dealer_vanna_total, dealer_charm_total)``, or ``(None, None)``
    when there is no per-strike data or the read errors.
    """
    cur = conn.cursor()
    armed = _savepoint(cur, "tw_second_order")
    try:
        cur.execute(
            """
            SELECT MAX(timestamp)
            FROM gex_by_strike
            WHERE underlying = %s AND timestamp <= COALESCE(%s::timestamptz, NOW())
            """,
            (underlying, as_of),
        )
        row = cur.fetchone()
        ts0 = row[0] if row else None
        if ts0 is None:
            _release(cur, "tw_second_order", armed)
            return (None, None)
        # Front expiration = the nearest expiration that has not yet passed as
        # of this snapshot's ET calendar date. Computed in Python so we avoid a
        # SQL timezone cast and keep the query portable.
        front_date = (
            ts0.astimezone(_ET).date()
            if isinstance(ts0, datetime)
            else datetime.now(timezone.utc).astimezone(_ET).date()
        )
        cur.execute(
            """
            SELECT
                COALESCE(SUM(COALESCE(dealer_vanna_exposure, -vanna_exposure)), 0.0),
                COALESCE(SUM(COALESCE(dealer_charm_exposure, -charm_exposure)), 0.0),
                COUNT(*)
            FROM gex_by_strike
            WHERE underlying = %s AND timestamp = %s
              AND expiration = (
                  SELECT MIN(expiration)
                  FROM gex_by_strike
                  WHERE underlying = %s AND timestamp = %s AND expiration >= %s
              )
            """,
            (underlying, ts0, underlying, ts0, front_date),
        )
        srow = cur.fetchone()
        _release(cur, "tw_second_order", armed)
    except Exception:
        _rollback(cur, "tw_second_order", armed)
        logger.debug("fetch_second_order_totals failed for %s", underlying, exc_info=True)
        return (None, None)
    if not srow or not srow[2]:
        return (None, None)
    return (_f(srow[0]), _f(srow[1]))


def fetch_recent_option_flow(
    conn: Any, underlying: str, as_of: Optional[datetime] = None
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Latest aggressor-classified option order flow for ``underlying``.

    Reads the two most recent ``flow_series_5min`` bars at-or-before
    ``as_of`` and returns ``(net_premium, net_premium_prev, net_volume)``:

    * ``net_premium`` — day-to-date cumulative SIGNED premium
      (``call_premium_cum - put_premium_cum``, buys minus sells baked in);
      **positive => call-led / bullish aggression.**
    * ``net_premium_prev`` — the same figure one 5-minute bucket earlier, so a
      bot can recover the per-bucket flow *delta* (the freshest lean) without
      a second query.
    * ``net_volume`` — day-to-date cumulative SIGNED volume; positive =
      call-led / bullish.

    All ``None`` on missing data or error. ``net_premium_prev`` is ``None``
    when only one bar exists yet (early session).
    """
    cur = conn.cursor()
    armed = _savepoint(cur, "tw_recent_flow")
    try:
        cur.execute(
            """
            SELECT net_premium_cum, net_volume_cum
            FROM flow_series_5min
            WHERE symbol = %s AND bar_start <= COALESCE(%s::timestamptz, NOW())
            ORDER BY bar_start DESC
            LIMIT 2
            """,
            (underlying, as_of),
        )
        rows = cur.fetchall()
        _release(cur, "tw_recent_flow", armed)
    except Exception:
        _rollback(cur, "tw_recent_flow", armed)
        logger.debug("fetch_recent_option_flow failed for %s", underlying, exc_info=True)
        return (None, None, None)
    if not rows:
        return (None, None, None)
    net_premium = _f(rows[0][0])
    net_volume = _f(rows[0][1])
    net_premium_prev = _f(rows[1][0]) if len(rows) > 1 else None
    return (net_premium, net_premium_prev, net_volume)


def fetch_recent_flow_window(
    conn: Any,
    underlying: str,
    window_buckets: int = 3,
    as_of: Optional[datetime] = None,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """FRESH aggressor flow over the last ``window_buckets`` 5-min buckets.

    The shelved ``aggressor_flow_divergence`` keyed on the day-to-date
    CUMULATIVE net premium — by mid-afternoon that is dominated by the morning's
    flow and lags badly (PF 0.31 on 404 trades). This returns the net signed
    flow over a short recent WINDOW instead, recovered as a cumulative-difference
    over ``flow_series_5min`` (``net_*_cum`` is day-to-date; a k-bucket delta is
    the net flow of the last k buckets):

        recent = cum(t) − cum(t − window_buckets)

    Returns ``(recent_premium, recent_volume, prior_window_premium)`` where
    ``prior_window_premium`` is the SAME-length window immediately before
    (``cum(t−w) − cum(t−2w)``) so a bot can measure *acceleration* (a genuine
    burst = recent window bigger than the prior). Signs follow the source:
    positive = call-led / bullish aggression. ``None`` fields when there is not
    enough history yet (early session) or on error. Best-effort + as-of bounded.
    """
    w = max(1, int(window_buckets))
    need = 2 * w + 1
    cur = conn.cursor()
    armed = _savepoint(cur, "tw_flow_window")
    try:
        cur.execute(
            """
            SELECT net_premium_cum, net_volume_cum
            FROM flow_series_5min
            WHERE symbol = %s AND bar_start <= COALESCE(%s::timestamptz, NOW())
            ORDER BY bar_start DESC
            LIMIT %s
            """,
            (underlying, as_of, need),
        )
        rows = cur.fetchall()
        _release(cur, "tw_flow_window", armed)
    except Exception:
        _rollback(cur, "tw_flow_window", armed)
        logger.debug("fetch_recent_flow_window failed for %s", underlying, exc_info=True)
        return (None, None, None)
    if not rows or len(rows) < w + 1:
        return (None, None, None)
    latest_p, latest_v = _f(rows[0][0]), _f(rows[0][1])
    w_ago_p, w_ago_v = _f(rows[w][0]), _f(rows[w][1])
    if latest_p is None or w_ago_p is None:
        return (None, None, None)
    recent_p = latest_p - w_ago_p
    recent_v = (latest_v - w_ago_v) if (latest_v is not None and w_ago_v is not None) else None
    prior_p: Optional[float] = None
    if len(rows) >= 2 * w + 1:
        w2_ago_p = _f(rows[2 * w][0])
        if w2_ago_p is not None:
            prior_p = w_ago_p - w2_ago_p
    return (recent_p, recent_v, prior_p)


def fetch_second_order_by_bucket(
    conn: Any, underlying: str, as_of: Optional[datetime] = None
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Dealer charm / vanna exposure split by expiration bucket.

    ``fetch_second_order_totals`` collapses the book to a front-expiration
    total; this returns the ladder the ``weekly_charm_grind`` bot needs —
    the same-day book vs the 1–7 DTE weekly book, whose time-decay rebalance
    is the dominant midday forced flow when gamma hedging is quiet:

        (dealer_charm_0dte, dealer_charm_weekly,
         dealer_vanna_0dte, dealer_vanna_weekly)

    Buckets come from ``gex_by_strike.expiration_bucket`` ('0dte' / 'weekly'),
    summed at the latest snapshot timestamp at-or-before ``as_of``. Dealer
    sign convention matches the source columns: positive charm => dealers BUY
    as time passes. Legacy rows without the dealer-sign columns fall back to
    negating the market-aggregate exposures (documented market->dealer
    convention). All ``None`` when the bucket column has no data or on error.
    """
    cur = conn.cursor()
    armed = _savepoint(cur, "tw_so_bucket")
    try:
        cur.execute(
            """
            SELECT MAX(timestamp)
            FROM gex_by_strike
            WHERE underlying = %s AND timestamp <= COALESCE(%s::timestamptz, NOW())
            """,
            (underlying, as_of),
        )
        row = cur.fetchone()
        ts0 = row[0] if row else None
        if ts0 is None:
            _release(cur, "tw_so_bucket", armed)
            return (None, None, None, None)
        cur.execute(
            """
            SELECT expiration_bucket,
                   SUM(COALESCE(dealer_charm_exposure, -charm_exposure)),
                   SUM(COALESCE(dealer_vanna_exposure, -vanna_exposure)),
                   COUNT(*)
            FROM gex_by_strike
            WHERE underlying = %s AND timestamp = %s
              AND expiration_bucket IN ('0dte', 'weekly')
            GROUP BY expiration_bucket
            """,
            (underlying, ts0),
        )
        rows = cur.fetchall()
        _release(cur, "tw_so_bucket", armed)
    except Exception:
        _rollback(cur, "tw_so_bucket", armed)
        logger.debug("fetch_second_order_by_bucket failed for %s", underlying, exc_info=True)
        return (None, None, None, None)
    charm_0dte = charm_weekly = vanna_0dte = vanna_weekly = None
    for bucket, charm, vanna, n in rows or []:
        if not n:
            continue
        if bucket == "0dte":
            charm_0dte, vanna_0dte = _f(charm), _f(vanna)
        elif bucket == "weekly":
            charm_weekly, vanna_weekly = _f(charm), _f(vanna)
    return (charm_0dte, charm_weekly, vanna_0dte, vanna_weekly)


def fetch_hedge_impulse(
    conn: Any,
    underlying: str,
    window_min: int = 15,
    as_of: Optional[datetime] = None,
) -> Tuple[Optional[float], Optional[float]]:
    """Pending dealer hedge obligation from the last ``window_min`` of flow.

    When customers aggressively buy options the dealer inherits the opposite
    delta and must neutralize it in stock within minutes. The obligation in
    SHARES is exactly ``Σ delta_i · (buy_volume_i − sell_volume_i) · 100``
    over ``flow_contract_facts`` (per-contract delta and the Lee-Ready
    buy/sell split are both persisted per row). Delta-weighting is the point:
    a $500k sweep of 0.03-delta lottos creates almost no hedge demand — it is
    exactly the noise that polluted the raw-premium flow bots.

    Returns ``(hedge_impulse_shares, tape_volume_shares)`` where the tape
    volume is the same-window ``underlying_quotes`` up+down volume, so the
    caller can normalize the obligation against what the tape can absorb.
    Sign: positive => dealers must BUY stock (call buying / put selling).
    ``None`` fields on missing data or error.
    """
    w = max(1, int(window_min))
    cur = conn.cursor()
    armed = _savepoint(cur, "tw_hedge_impulse")
    try:
        cur.execute(
            """
            SELECT SUM(delta * (buy_volume - sell_volume) * 100.0), COUNT(*)
            FROM flow_contract_facts
            WHERE symbol = %s
              AND timestamp <= COALESCE(%s::timestamptz, NOW())
              AND timestamp > COALESCE(%s::timestamptz, NOW())
                              - (%s * INTERVAL '1 minute')
              AND delta IS NOT NULL
            """,
            (underlying, as_of, as_of, w),
        )
        frow = cur.fetchone()
        cur.execute(
            """
            SELECT SUM(COALESCE(up_volume, 0) + COALESCE(down_volume, 0))
            FROM underlying_quotes
            WHERE symbol = %s
              AND timestamp <= COALESCE(%s::timestamptz, NOW())
              AND timestamp > COALESCE(%s::timestamptz, NOW())
                              - (%s * INTERVAL '1 minute')
            """,
            (underlying, as_of, as_of, w),
        )
        vrow = cur.fetchone()
        _release(cur, "tw_hedge_impulse", armed)
    except Exception:
        _rollback(cur, "tw_hedge_impulse", armed)
        logger.debug("fetch_hedge_impulse failed for %s", underlying, exc_info=True)
        return (None, None)
    if not frow or not frow[1]:
        return (None, None)
    impulse = _f(frow[0])
    tape = _f(vrow[0]) if vrow else None
    return (impulse, tape)


def fetch_put_panic_window(
    conn: Any,
    underlying: str,
    window_min: int = 15,
    as_of: Optional[datetime] = None,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Put-side aggressor capitulation metrics over the last ``window_min``.

    Reads per-type Lee-Ready flow from ``flow_contract_facts`` and returns:

    * ``put_net_buy_premium`` — Σ(buy_premium − sell_premium) over PUT rows in
      the window. Large positive = a one-way rush INTO puts.
    * ``put_dominance`` — put gross premium / (put + call gross premium) in
      the window, in [0, 1]. High = the burst is fear, not two-way repricing.
    * ``session_baseline`` — median of the per-``window_min``-bucket put net
      buy premium since today's 09:30 ET session open (excluding the current
      window). The burst multiple against this baseline is self-scaling
      across SPY/SPX. ``None`` until at least 3 completed prior buckets.

    All ``None`` on missing data or error; best-effort + as-of bounded.
    """
    w = max(1, int(window_min))
    cur = conn.cursor()
    armed = _savepoint(cur, "tw_put_panic")
    try:
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN option_type = 'P' THEN buy_premium - sell_premium ELSE 0 END),
                SUM(CASE WHEN option_type = 'P' THEN buy_premium + sell_premium ELSE 0 END),
                SUM(buy_premium + sell_premium),
                COUNT(*)
            FROM flow_contract_facts
            WHERE symbol = %s
              AND timestamp <= COALESCE(%s::timestamptz, NOW())
              AND timestamp > COALESCE(%s::timestamptz, NOW())
                              - (%s * INTERVAL '1 minute')
            """,
            (underlying, as_of, as_of, w),
        )
        wrow = cur.fetchone()
        # Per-bucket put net buy premium since the ET session open, excluding
        # the in-flight window. Bucketing is relative to as_of so the windows
        # tile back from "now" exactly like the live window above.
        cur.execute(
            """
            SELECT FLOOR(EXTRACT(EPOCH FROM (COALESCE(%s::timestamptz, NOW()) - timestamp))
                         / (%s * 60.0)) AS bucket_back,
                   SUM(CASE WHEN option_type = 'P' THEN buy_premium - sell_premium ELSE 0 END)
            FROM flow_contract_facts
            WHERE symbol = %s
              AND timestamp <= COALESCE(%s::timestamptz, NOW())
              AND timestamp > (COALESCE(%s::timestamptz, NOW()) AT TIME ZONE
                               'America/New_York')::date::timestamp
                              AT TIME ZONE 'America/New_York' + INTERVAL '9 hours 30 minutes'
            GROUP BY bucket_back
            HAVING FLOOR(EXTRACT(EPOCH FROM (COALESCE(%s::timestamptz, NOW()) - timestamp))
                         / (%s * 60.0)) >= 1
            """,
            (as_of, w, underlying, as_of, as_of, as_of, w),
        )
        brows = cur.fetchall()
        _release(cur, "tw_put_panic", armed)
    except Exception:
        _rollback(cur, "tw_put_panic", armed)
        logger.debug("fetch_put_panic_window failed for %s", underlying, exc_info=True)
        return (None, None, None)
    if not wrow or not wrow[3]:
        return (None, None, None)
    put_net = _f(wrow[0])
    put_gross = _f(wrow[1]) or 0.0
    all_gross = _f(wrow[2]) or 0.0
    dominance = (put_gross / all_gross) if all_gross > 0 else None
    baseline: Optional[float] = None
    vals = sorted(v for _, raw in (brows or []) if (v := _f(raw)) is not None)
    if len(vals) >= 3:
        mid = len(vals) // 2
        baseline = vals[mid] if len(vals) % 2 else (vals[mid - 1] + vals[mid]) / 2.0
    return (put_net, dominance, baseline)


def fetch_profile_geometry(
    conn: Any,
    underlying: str,
    spot: float,
    as_of: Optional[datetime] = None,
    probe_pct: float = 0.005,
    window_pct: float = 0.015,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Local geometry of the spot-shift dealer gamma curve (``gex_profile``).

    The persisted profile IS the dealers' forced order schedule: the signed
    dollar GEX they would carry at each hypothetical price. Its local shape —
    a steep one-sided negative shelf just off spot — is the pre-condition for
    a hedging cascade, and no bot has ever consumed it. Returns:

        (gex_down, gex_up, trough_price, trough_gex)

    * ``gex_down`` / ``gex_up`` — curve value interpolated at
      ``spot·(1∓probe_pct)``.
    * ``trough_price`` / ``trough_gex`` — the most negative grid point within
      ``spot·(1±window_pct)`` (where forced selling/buying exhausts because
      the curve flattens back toward zero).

    All ``None`` when there is no profile row, the JSONB is empty/malformed,
    or the read errors. Best-effort + as-of bounded.
    """
    if spot is None or spot <= 0:
        return (None, None, None, None)
    cur = conn.cursor()
    armed = _savepoint(cur, "tw_profile_geo")
    try:
        cur.execute(
            """
            SELECT profile
            FROM gex_profile
            WHERE underlying = %s AND timestamp <= COALESCE(%s::timestamptz, NOW())
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (underlying, as_of),
        )
        row = cur.fetchone()
        _release(cur, "tw_profile_geo", armed)
    except Exception:
        _rollback(cur, "tw_profile_geo", armed)
        logger.debug("fetch_profile_geometry failed for %s", underlying, exc_info=True)
        return (None, None, None, None)
    if not row or not row[0]:
        return (None, None, None, None)
    points = row[0]
    if isinstance(points, str):
        try:
            import json

            points = json.loads(points)
        except (TypeError, ValueError):
            return (None, None, None, None)
    grid: list[Tuple[float, float]] = []
    try:
        for p in points:
            price, gex = _f(p.get("price")), _f(p.get("gex"))
            if price is not None and gex is not None and price > 0:
                grid.append((price, gex))
    except (AttributeError, TypeError):
        return (None, None, None, None)
    if len(grid) < 3:
        return (None, None, None, None)
    grid.sort(key=lambda pg: pg[0])

    def _interp(target: float) -> Optional[float]:
        if target < grid[0][0] or target > grid[-1][0]:
            return None
        for (p_a, g_a), (p_b, g_b) in zip(grid, grid[1:]):
            if p_a <= target <= p_b:
                if p_b == p_a:
                    return g_a
                frac = (target - p_a) / (p_b - p_a)
                return g_a + frac * (g_b - g_a)
        return None

    gex_down = _interp(spot * (1.0 - probe_pct))
    gex_up = _interp(spot * (1.0 + probe_pct))
    lo, hi = spot * (1.0 - window_pct), spot * (1.0 + window_pct)
    in_window = [(p, g) for p, g in grid if lo <= p <= hi]
    trough_price = trough_gex = None
    if in_window:
        trough_price, trough_gex = min(in_window, key=lambda pg: pg[1])
    return (gex_down, gex_up, trough_price, trough_gex)


def fetch_vix_lookback(
    conn: Any, lookback_minutes: int = 30, as_of: Optional[datetime] = None
) -> Optional[float]:
    """VIX close ``lookback_minutes`` before ``as_of`` (default 30m).

    Paired with the snapshot's current ``vix`` to form a session-scale
    ΔVIX — the input the vanna vol-crush bot needs (vanna flow is driven by
    the *change* in implied vol, not its level). Returns ``None`` when no bar
    exists that far back or on error.
    """
    cur = conn.cursor()
    armed = _savepoint(cur, "tw_vix_lookback")
    try:
        cur.execute(
            """
            SELECT close
            FROM vix_bars
            WHERE timestamp <= COALESCE(%s::timestamptz, NOW())
                              - (%s * INTERVAL '1 minute')
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (as_of, int(lookback_minutes)),
        )
        row = cur.fetchone()
        _release(cur, "tw_vix_lookback", armed)
    except Exception:
        _rollback(cur, "tw_vix_lookback", armed)
        logger.debug("fetch_vix_lookback failed", exc_info=True)
        return None
    if not row:
        return None
    return _f(row[0])
