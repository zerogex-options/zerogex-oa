"""Unified signal + portfolio reconciliation engine.

This engine is fully self-contained under src/signals and does not depend on
src/analytics modules.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Optional
import os
import json
from datetime import timedelta

from src.config import SIGNALS_GEX_STALE_BUFFER_SECONDS
from src.database import db_connection
from src.signals.basic.dealer_delta_pressure import DealerDeltaPressureComponent
from src.signals.components.base import MarketContext
from src.signals.components.flip_distance import FlipDistanceComponent
from src.signals.components.local_gamma import LocalGammaComponent
from src.signals.components.net_gex_sign import NetGexSignComponent
from src.signals.components.order_flow_imbalance import OrderFlowImbalanceComponent
from src.signals.components.price_vs_max_gamma import PriceVsMaxGammaComponent
from src.signals.components.put_call_ratio_state import PutCallRatioStateComponent
from src.signals.components.volatility_regime import VolatilityRegimeComponent
from src.signals.advanced import AdvancedSignalEngine
from src.signals.basic import BasicSignalEngine
from src.signals.portfolio_engine import PortfolioEngine
from src.signals.scoring_engine import ScoringEngine
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)


class UnifiedSignalEngine:
    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)
        components = [
            NetGexSignComponent(),
            FlipDistanceComponent(),
            LocalGammaComponent(),
            PutCallRatioStateComponent(),
            PriceVsMaxGammaComponent(),
            VolatilityRegimeComponent(),
            # Phase 3.1: leading-indicator components added to MSI.
            OrderFlowImbalanceComponent(),
            DealerDeltaPressureComponent(),
        ]

        self.scoring_engine = ScoringEngine(
            underlying=self.db_symbol,
            components=components,
        )

        self.portfolio_engine = PortfolioEngine(self.underlying)
        self.advanced_signal_engine = AdvancedSignalEngine()
        self.basic_signal_engine = BasicSignalEngine()
        # Per-signal hysteresis/dedupe state, keyed by advanced signal name.
        # Initialized eagerly so helper methods can assume the dict exists.
        self._advanced_state: dict[str, dict] = {}
        self._iv_rank_enabled = os.getenv("SIGNAL_IV_RANK_ENABLED", "false").lower() == "true"
        if not self._iv_rank_enabled:
            logger.info(
                "UnifiedSignalEngine [%s]: IV-rank query disabled (set SIGNAL_IV_RANK_ENABLED=true to enable)",
                self.db_symbol,
            )

    @staticmethod
    @contextmanager
    def _use_conn(conn=None):
        """Yield *conn* if provided, otherwise acquire a fresh one from the pool."""
        if conn is not None:
            yield conn
        else:
            with db_connection() as new_conn:
                yield new_conn

    @staticmethod
    def _reset_tx(conn) -> None:
        """Rollback any aborted transaction so later queries on the same
        connection can proceed. Safe for read-only work paths because no
        successful write state is lost on rollback."""
        try:
            conn.rollback()
        except Exception:
            pass

    def _fetch_market_context(self, conn=None) -> Optional[dict]:
        # Phase 2.5: defensive look-ahead guard.  Option quotes are aggregated
        # into 1-minute buckets stamped with the bucket start time, so a
        # gex_summary row stamped 14:00 was actually computed from quotes
        # arriving up to 14:00:59.  In live trading this is invisible — every
        # cycle naturally consumes the most recent data — but a backtest
        # replay at second-resolution would observe values computed up to
        # ~60s in the future relative to replay time.  Subtracting
        # SIGNALS_GEX_STALE_BUFFER_SECONDS from the anchor lets a backtest
        # operator set the buffer to one bucket width (60s) without code
        # changes; default 0 is a no-op for live.
        gex_buffer_seconds = int(SIGNALS_GEX_STALE_BUFFER_SECONDS)
        with self._use_conn(conn) as conn:
            with conn.cursor() as cur:
                # Fetch underlying quote + latest gex_summary ROW including
                # the gex_summary timestamp so the prev-gex lookup below can
                # use the *gex_summary* ts (not underlying ts).  Fixes C1 —
                # previously the "prev" query re-selected the same row,
                # forcing net_gex_delta=0 on every cycle.
                cur.execute(
                    """
                    SELECT uq.timestamp,
                           uq.close,
                           gs.timestamp AS gs_ts,
                           gs.total_net_gex,
                           gs.gamma_flip_point,
                           gs.flip_distance,
                           gs.local_gex,
                           gs.convexity_risk,
                           gs.put_call_ratio,
                           gs.max_pain,
                           gs.total_call_oi,
                           gs.total_put_oi
                    FROM underlying_quotes uq
                    LEFT JOIN LATERAL (
                        SELECT timestamp, total_net_gex, gamma_flip_point, flip_distance, local_gex, convexity_risk,
                               put_call_ratio, max_pain,
                               total_call_oi, total_put_oi
                        FROM gex_summary
                        WHERE underlying = %s
                          AND timestamp <= uq.timestamp - (%s * INTERVAL '1 second')
                        ORDER BY timestamp DESC
                        LIMIT 1
                    ) gs ON TRUE
                    WHERE uq.symbol = %s
                    ORDER BY uq.timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol, gex_buffer_seconds, self.db_symbol),
                )
                row = cur.fetchone()
                if not row:
                    return None
                (
                    ts,
                    close,
                    gs_ts,
                    net_gex,
                    gamma_flip,
                    flip_distance,
                    local_gex,
                    convexity_risk,
                    pcr,
                    max_pain,
                    total_call_oi,
                    total_put_oi,
                ) = row
                close_f = float(close)

                # Open-interest-based PCR is structurally more stable than the
                # flow-volume ratio (which inverts in vol events). We prefer
                # the OI ratio when available and fall back to the existing
                # volume ratio otherwise.
                oi_pcr = None
                try:
                    if total_call_oi and int(total_call_oi) > 0:
                        oi_pcr = float(total_put_oi or 0) / float(total_call_oi)
                except (TypeError, ValueError, ZeroDivisionError):
                    oi_pcr = None
                effective_pcr = oi_pcr if oi_pcr is not None and oi_pcr > 0 else float(pcr or 1.0)

                cur.execute(
                    """
                    SELECT vwap, vwap_deviation_pct
                    FROM underlying_vwap_deviation
                    WHERE symbol = %s
                      AND timestamp <= %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol, ts),
                )
                vwap_row = cur.fetchone()
                vwap = float(vwap_row[0]) if vwap_row and vwap_row[0] is not None else None
                vwap_deviation_pct = (
                    float(vwap_row[1]) if vwap_row and vwap_row[1] is not None else None
                )

                # Single pass over gex_by_strike: fetch the per-strike exposure
                # window around spot (used by gex_gradient, dealer_delta_pressure,
                # vanna_charm_flow, eod_pressure) and derive call_wall and
                # max_gamma_strike in Python from the same rows.  Also fetches
                # dealer-sign charm/vanna (C3) and expiration_bucket for EOD
                # pressure expiry-weighting (S2).
                gex_strike_rows: list[dict] = []
                gex_strike_by_bucket: dict[str, list[dict]] = {}
                call_wall: Optional[float] = None
                max_gamma_strike: Optional[float] = None
                try:
                    # Single query at (strike, bucket) granularity.  We build
                    # both the strike-level aggregate view (gex_strike_rows)
                    # and the per-bucket view (gex_strike_by_bucket) from the
                    # same result set — halves DB roundtrips per cycle.
                    cur.execute(
                        """
                        WITH latest AS (
                            SELECT MAX(timestamp) AS ts
                            FROM gex_by_strike
                            WHERE underlying = %s AND timestamp <= %s
                        )
                        SELECT strike,
                               COALESCE(expiration_bucket, 'monthly') AS bucket,
                               SUM(COALESCE(net_gex, 0))                 AS net_gex,
                               SUM(COALESCE(call_oi, 0))                 AS call_oi,
                               SUM(COALESCE(put_oi, 0))                  AS put_oi,
                               SUM(COALESCE(vanna_exposure, 0))          AS vanna_exposure,
                               SUM(COALESCE(charm_exposure, 0))          AS charm_exposure,
                               SUM(COALESCE(dealer_vanna_exposure, -vanna_exposure, 0)) AS dealer_vanna,
                               SUM(COALESCE(dealer_charm_exposure, -charm_exposure, 0)) AS dealer_charm
                        FROM gex_by_strike g, latest
                        WHERE g.underlying = %s
                          AND g.timestamp = latest.ts
                          AND g.strike BETWEEN %s AND %s
                        GROUP BY strike, bucket
                        ORDER BY strike
                        """,
                        (
                            self.db_symbol,
                            # Phase 2.5 stale-buffer: see _fetch_market_context
                            # docstring for rationale.  Default 0 is a no-op.
                            ts - timedelta(seconds=gex_buffer_seconds)
                            if gex_buffer_seconds
                            else ts,
                            self.db_symbol,
                            close_f * 0.90,
                            close_f * 1.10,
                        ),
                    )
                    per_strike: dict[float, dict] = {}
                    for r in cur.fetchall():
                        strike = float(r[0])
                        bucket = r[1] or "monthly"
                        net_gex_b = float(r[2] or 0.0)
                        call_oi_b = int(r[3] or 0)
                        put_oi_b = int(r[4] or 0)
                        vanna_b = float(r[5] or 0.0)
                        charm_b = float(r[6] or 0.0)
                        dealer_vanna_b = float(r[7] or 0.0)
                        dealer_charm_b = float(r[8] or 0.0)

                        gex_strike_by_bucket.setdefault(bucket, []).append(
                            {
                                "strike": strike,
                                "dealer_charm_exposure": dealer_charm_b,
                                "dealer_vanna_exposure": dealer_vanna_b,
                            }
                        )

                        agg = per_strike.setdefault(
                            strike,
                            {
                                "strike": strike,
                                "net_gex": 0.0,
                                "call_oi": 0,
                                "put_oi": 0,
                                "vanna_exposure": 0.0,
                                "charm_exposure": 0.0,
                                "dealer_vanna_exposure": 0.0,
                                "dealer_charm_exposure": 0.0,
                                "expiration_bucket": "monthly",
                            },
                        )
                        agg["net_gex"] += net_gex_b
                        agg["call_oi"] += call_oi_b
                        agg["put_oi"] += put_oi_b
                        agg["vanna_exposure"] += vanna_b
                        agg["charm_exposure"] += charm_b
                        agg["dealer_vanna_exposure"] += dealer_vanna_b
                        agg["dealer_charm_exposure"] += dealer_charm_b
                        # Preserve the old "COALESCE(MAX(bucket))" semantic:
                        # pick the alphabetically-greatest bucket label.  The
                        # field is stored but not consumed downstream; this
                        # just matches prior behavior for any external reader.
                        if bucket > agg["expiration_bucket"]:
                            agg["expiration_bucket"] = bucket

                    gex_strike_rows = sorted(per_strike.values(), key=lambda row: row["strike"])
                    if gex_strike_rows:
                        above_spot = [r for r in gex_strike_rows if r["strike"] >= close_f]
                        if above_spot:
                            # call_wall = highest call_oi strike >= spot, ties broken by
                            # nearest-to-spot (lowest strike above spot).
                            call_wall = max(
                                above_spot,
                                key=lambda r: (r["call_oi"], -r["strike"]),
                            )["strike"]
                        max_gamma_row = max(
                            gex_strike_rows,
                            key=lambda r: (abs(r["net_gex"]), -r["strike"]),
                        )
                        max_gamma_strike = max_gamma_row["strike"]
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning(
                        "UnifiedSignalEngine [%s]: gex_by_strike fetch failed: %s",
                        self.db_symbol,
                        exc,
                    )

                # Lee-Ready-classified flow per option type for the most recent
                # 15-minute window. Powers tape_flow_bias. Sources from
                # flow_contract_facts now that the per-minute flow_by_type
                # cache has been merged into the unified flow_by_contract
                # rollup.
                flow_by_type_rows: list[dict] = []
                try:
                    cur.execute(
                        """
                        SELECT option_type,
                               SUM(COALESCE(buy_volume, 0))    AS buy_volume,
                               SUM(COALESCE(sell_volume, 0))   AS sell_volume,
                               SUM(COALESCE(buy_premium, 0))   AS buy_premium,
                               SUM(COALESCE(sell_premium, 0))  AS sell_premium
                        FROM flow_contract_facts
                        WHERE symbol = %s
                          AND timestamp BETWEEN %s - INTERVAL '15 minutes' AND %s
                        GROUP BY option_type
                        """,
                        (self.db_symbol, ts, ts),
                    )
                    for r in cur.fetchall():
                        flow_by_type_rows.append(
                            {
                                "option_type": r[0],
                                "buy_volume": int(r[1] or 0),
                                "sell_volume": int(r[2] or 0),
                                "buy_premium": float(r[3] or 0.0),
                                "sell_premium": float(r[4] or 0.0),
                            }
                        )
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning(
                        "UnifiedSignalEngine [%s]: per-type flow fetch failed: %s",
                        self.db_symbol,
                        exc,
                    )

                # Short-dated OTM put vs OTM call IV for skew_delta.
                skew_info: dict = {}
                try:
                    cur.execute(
                        """
                        SELECT option_type,
                               AVG(implied_volatility) AS iv
                        FROM option_chains
                        WHERE underlying = %s
                          AND implied_volatility IS NOT NULL
                          AND implied_volatility > 0
                          AND timestamp >= %s - INTERVAL '30 minutes'
                          AND (
                                (option_type = 'P' AND strike BETWEEN %s AND %s)
                             OR (option_type = 'C' AND strike BETWEEN %s AND %s)
                          )
                        GROUP BY option_type
                        """,
                        (
                            self.db_symbol,
                            ts,
                            close_f * 0.95,
                            close_f * 0.98,  # OTM puts: ~2-5% OTM
                            close_f * 1.02,
                            close_f * 1.05,  # OTM calls: ~2-5% OTM
                        ),
                    )
                    for r in cur.fetchall():
                        if r[0] == "P":
                            skew_info["otm_put_iv"] = float(r[1]) if r[1] is not None else None
                        elif r[0] == "C":
                            skew_info["otm_call_iv"] = float(r[1]) if r[1] is not None else None
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning(
                        "UnifiedSignalEngine [%s]: skew fetch failed: %s",
                        self.db_symbol,
                        exc,
                    )

                # C7: signed smart-money imbalance.  The previous aggregation
                # summed ``total_premium`` which is gross notional and doesn't
                # distinguish buyers from sellers — heavy call-selling + heavy
                # put-selling shows up as "balanced" when the directional bias
                # is clearly bearish.  Prefer signed buy-sell flow from
                # flow_contract_facts (which has Lee-Ready buy/sell splits)
                # and fall back to flow_smart_money's gross totals only when
                # signed flow is unavailable.
                sm_call = 0.0
                sm_put = 0.0
                sm_call_gross = 0.0
                sm_put_gross = 0.0
                try:
                    cur.execute(
                        """
                        SELECT option_type,
                               SUM(COALESCE(buy_premium, 0) - COALESCE(sell_premium, 0)) AS net_premium,
                               SUM(COALESCE(buy_premium, 0) + COALESCE(sell_premium, 0)) AS gross_premium
                        FROM flow_contract_facts
                        WHERE symbol = %s
                          AND timestamp BETWEEN %s - INTERVAL '30 minutes' AND %s
                        GROUP BY option_type
                        """,
                        (self.db_symbol, ts, ts),
                    )
                    for r in cur.fetchall():
                        if r[0] == "C":
                            sm_call = float(r[1] or 0.0)
                            sm_call_gross = float(r[2] or 0.0)
                        elif r[0] == "P":
                            sm_put = float(r[1] or 0.0)
                            sm_put_gross = float(r[2] or 0.0)
                except Exception as exc:
                    logger.warning(
                        "UnifiedSignalEngine [%s]: signed smart-money fetch failed: %s",
                        self.db_symbol,
                        exc,
                    )
                    cur.execute(
                        """
                        SELECT COALESCE(SUM(CASE WHEN option_type='C' THEN total_premium ELSE 0 END), 0),
                               COALESCE(SUM(CASE WHEN option_type='P' THEN total_premium ELSE 0 END), 0)
                        FROM flow_smart_money
                        WHERE symbol = %s
                          AND timestamp BETWEEN %s - INTERVAL '30 minutes' AND %s
                        """,
                        (self.db_symbol, ts, ts),
                    )
                    sm_call, sm_put = cur.fetchone() or (0.0, 0.0)
                    sm_call_gross, sm_put_gross = abs(sm_call), abs(sm_put)

                # C6: true 0DTE flow by option_type and moneyness. Filters
                # flow_contract_facts to today's expiration and splits by
                # strike so the advanced 0dte-position-imbalance signal can
                # weight OTM/ATM moneyness independently (S5 scoring).
                zero_dte_flow: list[dict] = []
                try:
                    cur.execute(
                        """
                        SELECT option_type,
                               strike,
                               SUM(COALESCE(buy_premium, 0))  AS buy_premium,
                               SUM(COALESCE(sell_premium, 0)) AS sell_premium,
                               SUM(COALESCE(buy_volume, 0))   AS buy_volume,
                               SUM(COALESCE(sell_volume, 0))  AS sell_volume
                        FROM flow_contract_facts
                        WHERE symbol = %s
                          AND timestamp BETWEEN %s - INTERVAL '30 minutes' AND %s
                          AND expiration = ((%s AT TIME ZONE 'America/New_York')::date)
                        GROUP BY option_type, strike
                        """,
                        (self.db_symbol, ts, ts, ts),
                    )
                    for r in cur.fetchall():
                        zero_dte_flow.append(
                            {
                                "option_type": r[0],
                                "strike": float(r[1]),
                                "buy_premium": float(r[2] or 0.0),
                                "sell_premium": float(r[3] or 0.0),
                                "buy_volume": int(r[4] or 0),
                                "sell_volume": int(r[5] or 0),
                            }
                        )
                except Exception as exc:
                    logger.warning(
                        "UnifiedSignalEngine [%s]: 0dte flow fetch failed: %s",
                        self.db_symbol,
                        exc,
                    )

                # Call/put premium flow acceleration over two consecutive windows.
                # Positive delta means demand is increasing in the latest window.
                call_flow_delta = 0.0
                put_flow_delta = 0.0
                try:
                    cur.execute(
                        """
                        WITH windows AS (
                            SELECT
                                CASE
                                    WHEN timestamp > %s - INTERVAL '15 minutes' THEN 'recent'
                                    WHEN timestamp > %s - INTERVAL '30 minutes' THEN 'prior'
                                    ELSE NULL
                                END AS bucket,
                                option_type,
                                SUM(COALESCE(buy_premium, 0) - COALESCE(sell_premium, 0)) AS net_premium
                            FROM flow_contract_facts
                            WHERE symbol = %s
                              AND timestamp BETWEEN %s - INTERVAL '30 minutes' AND %s
                            GROUP BY 1, 2
                        )
                        SELECT
                            COALESCE(SUM(CASE WHEN bucket='recent' AND option_type='C' THEN net_premium END), 0)
                            - COALESCE(SUM(CASE WHEN bucket='prior' AND option_type='C' THEN net_premium END), 0),
                            COALESCE(SUM(CASE WHEN bucket='recent' AND option_type='P' THEN net_premium END), 0)
                            - COALESCE(SUM(CASE WHEN bucket='prior' AND option_type='P' THEN net_premium END), 0)
                        FROM windows
                        """,
                        (ts, ts, self.db_symbol, ts, ts),
                    )
                    flow_delta_row = cur.fetchone() or (0.0, 0.0)
                    call_flow_delta = float(flow_delta_row[0] or 0.0)
                    put_flow_delta = float(flow_delta_row[1] or 0.0)
                except Exception as exc:
                    logger.warning(
                        "UnifiedSignalEngine [%s]: flow acceleration fetch failed: %s",
                        self.db_symbol,
                        exc,
                    )

                # C1: use gex_summary's own timestamp for the "previous" row,
                # not underlying_quotes.timestamp.  Previously this compared
                # ``timestamp < uq.ts`` — but ``uq.ts`` is almost always
                # strictly greater than the latest ``gex_summary.ts`` (signals
                # run 1Hz; analytics writes gex_summary every 60s), so the
                # LATERAL and the "prev" query both returned the same row and
                # net_gex_delta was structurally 0 on every cycle.
                prev_net_gex = None
                if gs_ts is not None:
                    try:
                        cur.execute(
                            """
                            SELECT total_net_gex
                            FROM gex_summary
                            WHERE underlying = %s
                              AND timestamp < %s
                            ORDER BY timestamp DESC
                            LIMIT 1
                            """,
                            (self.db_symbol, gs_ts),
                        )
                        prev_row = cur.fetchone()
                        if prev_row and prev_row[0] is not None:
                            prev_net_gex = float(prev_row[0])
                    except Exception as exc:
                        logger.warning(
                            "UnifiedSignalEngine [%s]: previous net gex fetch failed: %s",
                            self.db_symbol,
                            exc,
                        )

                # Any of the try/except-wrapped queries above may have failed
                # and left the transaction in an aborted state. Reset it so
                # the unguarded queries that follow can run cleanly. Safe for
                # this entirely read-only context fetch.
                self._reset_tx(conn)

                # C4: extend the close history to 2h of 1-minute bars so
                # components can compute realized-sigma normalized momentum.
                cur.execute(
                    """
                    SELECT close
                    FROM underlying_quotes
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 120
                    """,
                    (self.db_symbol,),
                )
                closes = [float(r[0]) for r in cur.fetchall()]

                # Latest VIX level for optional term-structure gating.  We only
                # have spot VIX; vix_9d / vix_3m would need a separate ingest.
                vix_level: Optional[float] = None
                try:
                    cur.execute("""
                        SELECT close
                        FROM vix_bars
                        ORDER BY timestamp DESC
                        LIMIT 1
                        """)
                    vix_row = cur.fetchone()
                    if vix_row and vix_row[0] is not None:
                        vix_level = float(vix_row[0])
                except Exception:
                    vix_level = None

                # Historical call_wall strike (~30min ago) so trap_detection
                # can detect "wall migration" — the single strongest predictor
                # of a genuine breakout vs. a failed pop.
                prior_call_wall: Optional[float] = None
                try:
                    cur.execute(
                        """
                        WITH window_max AS (
                            SELECT timestamp
                            FROM gex_by_strike
                            WHERE underlying = %s
                              AND timestamp <= %s - INTERVAL '25 minutes'
                              AND timestamp >= %s - INTERVAL '60 minutes'
                            ORDER BY timestamp DESC
                            LIMIT 1
                        )
                        SELECT strike
                        FROM gex_by_strike g, window_max w
                        WHERE g.underlying = %s
                          AND g.timestamp = w.timestamp
                          AND g.strike >= %s
                        ORDER BY g.call_oi DESC NULLS LAST, g.strike ASC
                        LIMIT 1
                        """,
                        (self.db_symbol, ts, ts, self.db_symbol, close_f),
                    )
                    wrow = cur.fetchone()
                    if wrow and wrow[0] is not None:
                        prior_call_wall = float(wrow[0])
                except Exception as exc:
                    logger.debug(
                        "UnifiedSignalEngine [%s]: prior call_wall fetch failed: %s",
                        self.db_symbol,
                        exc,
                    )

                # C5: per-symbol normalization constants from the rolling
                # cache so GEX/charm/flow scales aren't hardcoded for SPY.
                normalizers: dict[str, float] = {}
                try:
                    cur.execute(
                        """
                        SELECT field_name, p05, p50, p95, std
                        FROM component_normalizer_cache
                        WHERE underlying = %s
                        """,
                        (self.db_symbol,),
                    )
                    for r in cur.fetchall():
                        field = r[0]
                        p95 = float(r[3]) if r[3] is not None else None
                        std = float(r[4]) if r[4] is not None else None
                        p50 = float(r[2]) if r[2] is not None else None
                        # Conservatively prefer p95 magnitude; fall back to
                        # std-based scale (roughly 2-sigma).
                        if p95 is not None and p95 > 0:
                            normalizers[field] = p95
                        elif std is not None and std > 0:
                            normalizers[field] = 2.0 * std
                        elif p50 is not None and p50 > 0:
                            normalizers[field] = p50
                except Exception:
                    normalizers = {}

                iv_rank = None
                if self._iv_rank_enabled:
                    # IV rank: compare current ATM IV to its 30-day daily range.
                    try:
                        cur.execute(
                            """
                            WITH current_atm AS (
                                SELECT AVG(implied_volatility) AS current_iv
                                FROM option_chains
                                WHERE underlying = %s
                                  AND ABS(strike - %s) / NULLIF(%s, 0) < 0.01
                                  AND option_type = 'C'
                                  AND implied_volatility IS NOT NULL
                                  AND implied_volatility > 0
                                  AND timestamp >= %s - INTERVAL '2 hours'
                            ),
                            daily_iv AS (
                                SELECT DATE_TRUNC('day', timestamp) AS day,
                                       AVG(implied_volatility) AS avg_iv
                                FROM option_chains
                                WHERE underlying = %s
                                  AND ABS(strike - %s) / NULLIF(%s, 0) < 0.01
                                  AND option_type = 'C'
                                  AND implied_volatility IS NOT NULL
                                  AND implied_volatility > 0
                                  AND timestamp >= NOW() - INTERVAL '30 days'
                                GROUP BY DATE_TRUNC('day', timestamp)
                            )
                            SELECT
                                (SELECT current_iv FROM current_atm),
                                MIN(avg_iv),
                                MAX(avg_iv)
                            FROM daily_iv
                            """,
                            (
                                self.db_symbol,
                                close_f,
                                close_f,
                                ts,
                                self.db_symbol,
                                close_f,
                                close_f,
                            ),
                        )
                        iv_row = cur.fetchone()
                        if (
                            iv_row
                            and iv_row[0] is not None
                            and iv_row[1] is not None
                            and iv_row[2] is not None
                        ):
                            current_iv, iv_low, iv_high = (
                                float(iv_row[0]),
                                float(iv_row[1]),
                                float(iv_row[2]),
                            )
                            iv_range = max(iv_high - iv_low, 0.001)
                            iv_rank = round(min(1.0, max(0.0, (current_iv - iv_low) / iv_range)), 4)
                    except Exception as exc:
                        # IV rank is supplemental; do not block signal generation if unavailable.
                        logger.debug(
                            "UnifiedSignalEngine [%s]: iv_rank query failed: %s",
                            self.db_symbol,
                            exc,
                        )

                net_gex_f = float(net_gex or 0.0)
                flip_distance_f = (
                    float(flip_distance)
                    if flip_distance is not None
                    else (
                        ((close_f - float(gamma_flip)) / close_f)
                        if (gamma_flip is not None and close_f > 0)
                        else None
                    )
                )
                local_gex_f = float(local_gex or 0.0)
                convexity_risk_f = (
                    float(convexity_risk)
                    if convexity_risk is not None
                    else (
                        abs(net_gex_f) / max(abs(flip_distance_f), 1e-6)
                        if flip_distance_f is not None
                        else None
                    )
                )
                if prev_net_gex is not None:
                    net_gex_delta_raw = net_gex_f - prev_net_gex
                    # Normalize by prior magnitude (C1 note): a delta of
                    # 100M is enormous on a 200M book but trivial on a 5B
                    # book.  Expose both raw and normalized so components
                    # can pick the scale they need.
                    denom = max(abs(prev_net_gex), 1.0e6)
                    net_gex_delta_pct = net_gex_delta_raw / denom
                else:
                    net_gex_delta_raw = 0.0
                    net_gex_delta_pct = 0.0

                return {
                    "timestamp": ts,
                    "gex_summary_ts": gs_ts,
                    "close": close_f,
                    "net_gex": net_gex_f,
                    "prev_net_gex": prev_net_gex,
                    "gamma_flip": float(gamma_flip) if gamma_flip is not None else None,
                    "flip_distance": flip_distance_f,
                    "local_gex": local_gex_f,
                    "convexity_risk": convexity_risk_f,
                    "put_call_ratio": effective_pcr,
                    "put_call_ratio_source": (
                        "oi" if oi_pcr is not None and oi_pcr > 0 else "volume"
                    ),
                    "put_call_ratio_volume": float(pcr or 1.0),
                    "put_call_ratio_oi": oi_pcr,
                    "max_pain": float(max_pain) if max_pain is not None else None,
                    "smart_call": float(sm_call or 0.0),
                    "smart_put": float(sm_put or 0.0),
                    "smart_call_net": float(sm_call or 0.0),
                    "smart_put_net": float(sm_put or 0.0),
                    "smart_call_gross": float(sm_call_gross or 0.0),
                    "smart_put_gross": float(sm_put_gross or 0.0),
                    "recent_closes": list(reversed(closes)),
                    "iv_rank": iv_rank,
                    "vwap": vwap,
                    "vwap_deviation_pct": vwap_deviation_pct,
                    "call_wall": call_wall,
                    "prior_call_wall": prior_call_wall,
                    "max_gamma_strike": max_gamma_strike,
                    "gex_by_strike": gex_strike_rows,
                    "gex_by_strike_bucket": gex_strike_by_bucket,
                    "flow_by_type": flow_by_type_rows,
                    "flow_zero_dte": zero_dte_flow,
                    "skew": skew_info,
                    "call_flow_delta": call_flow_delta,
                    "put_flow_delta": put_flow_delta,
                    "net_gex_delta": net_gex_delta_raw,
                    "net_gex_delta_pct": net_gex_delta_pct,
                    "vix_level": vix_level,
                    "normalizers": normalizers,
                }

    def _build_market_context(self, ctx: dict) -> MarketContext:
        """Convert the dict returned by _fetch_market_context() into a MarketContext."""
        return MarketContext(
            timestamp=ctx["timestamp"],
            underlying=self.db_symbol,
            close=ctx["close"],
            net_gex=ctx["net_gex"],
            gamma_flip=ctx["gamma_flip"],
            put_call_ratio=ctx["put_call_ratio"],
            max_pain=ctx["max_pain"],
            smart_call=ctx["smart_call"],
            smart_put=ctx["smart_put"],
            recent_closes=ctx["recent_closes"],
            iv_rank=ctx.get("iv_rank"),
            vwap=ctx.get("vwap"),
            vwap_deviation_pct=ctx.get("vwap_deviation_pct"),
            extra={
                "call_wall": ctx.get("call_wall"),
                "prior_call_wall": ctx.get("prior_call_wall"),
                "max_gamma_strike": ctx.get("max_gamma_strike"),
                "gex_by_strike": ctx.get("gex_by_strike") or [],
                "gex_by_strike_bucket": ctx.get("gex_by_strike_bucket") or {},
                "flow_by_type": ctx.get("flow_by_type") or [],
                "flow_zero_dte": ctx.get("flow_zero_dte") or [],
                "skew": ctx.get("skew") or {},
                "put_call_ratio_source": ctx.get("put_call_ratio_source"),
                "put_call_ratio_volume": ctx.get("put_call_ratio_volume"),
                "put_call_ratio_oi": ctx.get("put_call_ratio_oi"),
                "flip_distance": ctx.get("flip_distance"),
                "local_gex": ctx.get("local_gex"),
                "convexity_risk": ctx.get("convexity_risk"),
                "call_flow_delta": ctx.get("call_flow_delta"),
                "put_flow_delta": ctx.get("put_flow_delta"),
                "net_gex_delta": ctx.get("net_gex_delta"),
                "net_gex_delta_pct": ctx.get("net_gex_delta_pct"),
                "smart_call_gross": ctx.get("smart_call_gross"),
                "smart_put_gross": ctx.get("smart_put_gross"),
                "smart_call_net": ctx.get("smart_call_net"),
                "smart_put_net": ctx.get("smart_put_net"),
                "vix_level": ctx.get("vix_level"),
                "normalizers": ctx.get("normalizers") or {},
            },
        )

    # Per-signal hysteresis state.  Keyed by component_name, stores the last
    # triggered-flag and the last score actually persisted.  Used to:
    #   * Dedupe upserts when the score + trigger flag didn't meaningfully
    #     change between cycles (C8).
    #   * Require ``SIGNAL_HYSTERESIS_CYCLES`` consecutive cycles of
    #     triggered=true before emitting a signal_events row (prevents
    #     single-bar flickers from firing alerts).
    _HYSTERESIS_CYCLES = max(1, int(os.getenv("SIGNAL_HYSTERESIS_CYCLES", "2")))
    _SCORE_DEDUPE_EPSILON = float(os.getenv("SIGNAL_SCORE_DEDUPE_EPSILON", "0.01"))

    def _persist_advanced_signals(self, market_context: MarketContext) -> list:
        results = self.advanced_signal_engine.evaluate(market_context)
        if not results:
            return []

        with db_connection() as conn:
            with conn.cursor() as cur:
                for result in results:
                    state = self._advanced_state.setdefault(
                        result.name,
                        {
                            "last_score": None,
                            "last_triggered": False,
                            "streak": 0,
                            "event_emitted": False,
                        },
                    )

                    triggered = bool(result.context.get("triggered", False))
                    prev_score = state["last_score"]
                    score_delta = (
                        abs(result.score - prev_score) if prev_score is not None else float("inf")
                    )

                    should_persist = (
                        prev_score is None
                        or score_delta >= self._SCORE_DEDUPE_EPSILON
                        or triggered != state["last_triggered"]
                    )

                    if should_persist:
                        cur.execute(
                            """
                            INSERT INTO signal_component_scores (
                                underlying, timestamp, component_name, clamped_score, weighted_score, weight, context_values
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                            ON CONFLICT (underlying, timestamp, component_name) DO UPDATE SET
                                clamped_score = EXCLUDED.clamped_score,
                                weighted_score = EXCLUDED.weighted_score,
                                weight = EXCLUDED.weight,
                                context_values = EXCLUDED.context_values
                            """,
                            (
                                market_context.underlying,
                                market_context.timestamp,
                                result.name,
                                result.score,
                                0.0,
                                0.0,
                                json.dumps(result.context, default=str),
                            ),
                        )
                        state["last_score"] = result.score
                        state["last_triggered"] = triggered

                    # Hysteresis: accumulate streak of consecutive triggered
                    # cycles; only emit an event on the streak threshold.
                    if triggered:
                        state["streak"] += 1
                    else:
                        state["streak"] = 0
                        state["event_emitted"] = False

                    if (
                        triggered
                        and state["streak"] >= self._HYSTERESIS_CYCLES
                        and not state["event_emitted"]
                    ):
                        # SAVEPOINT-wrap the events INSERT so a failure (e.g.
                        # a too-long direction string, FK violation) doesn't
                        # poison the transaction and cascade
                        # InFailedSqlTransaction into the next iteration's
                        # signal_component_scores INSERT.
                        cur.execute("SAVEPOINT sig_event")
                        try:
                            cur.execute(
                                """
                                INSERT INTO signal_events (
                                    underlying, timestamp, signal_name, direction, score,
                                    context_values, close_at_emit
                                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)
                                """,
                                (
                                    market_context.underlying,
                                    market_context.timestamp,
                                    result.name,
                                    # signal_events.direction is VARCHAR(16);
                                    # truncate so longer signal labels (e.g.
                                    # "trend_expansion") don't blow up the
                                    # INSERT.
                                    str(result.context.get("signal", "neutral"))[:16],
                                    result.score,
                                    json.dumps(result.context, default=str),
                                    market_context.close,
                                ),
                            )
                            cur.execute("RELEASE SAVEPOINT sig_event")
                            state["event_emitted"] = True
                        except Exception as exc:
                            cur.execute("ROLLBACK TO SAVEPOINT sig_event")
                            cur.execute("RELEASE SAVEPOINT sig_event")
                            logger.warning(
                                "signal_events insert failed for %s: %s",
                                result.name,
                                exc,
                            )
            conn.commit()
        return results

    def _persist_basic_signals(self, market_context: MarketContext) -> list:
        """Evaluate and persist the continuous basic signals.

        These share ``signal_component_scores`` with the MSI components and
        Advanced Signals but carry weight=0 (they do not contribute to the
        composite MSI). No ``signal_events`` emission — they are continuous
        directional reads, not discrete triggers.
        """
        results = self.basic_signal_engine.evaluate(market_context)
        if not results:
            return []

        if not hasattr(self, "_basic_state"):
            self._basic_state: dict[str, dict] = {}

        with db_connection() as conn:
            with conn.cursor() as cur:
                for result in results:
                    state = self._basic_state.setdefault(result.name, {"last_score": None})
                    prev_score = state["last_score"]
                    score_delta = (
                        abs(result.score - prev_score) if prev_score is not None else float("inf")
                    )
                    if prev_score is not None and score_delta < self._SCORE_DEDUPE_EPSILON:
                        continue
                    cur.execute(
                        """
                        INSERT INTO signal_component_scores (
                            underlying, timestamp, component_name, clamped_score, weighted_score, weight, context_values
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (underlying, timestamp, component_name) DO UPDATE SET
                            clamped_score = EXCLUDED.clamped_score,
                            weighted_score = EXCLUDED.weighted_score,
                            weight = EXCLUDED.weight,
                            context_values = EXCLUDED.context_values
                        """,
                        (
                            market_context.underlying,
                            market_context.timestamp,
                            result.name,
                            result.score,
                            0.0,
                            0.0,
                            json.dumps(result.context, default=str),
                        ),
                    )
                    state["last_score"] = result.score
            conn.commit()
        return results

    def run_cycle(self) -> bool:
        # Each phase acquires and releases its own connection to avoid
        # holding a connection "idle in transaction" during CPU-bound work.
        # The _use_conn(conn=None) pattern in each callee handles this.

        # Phase 1: fetch market context (read-only, releases connection)
        ctx = self._fetch_market_context()
        if not ctx:
            logger.warning("UnifiedSignalEngine [%s]: missing market context", self.db_symbol)
            return False

        # Phase 2: score and persist (acquires connection, writes, commits, releases)
        # NOTE: score_and_persist must run before portfolio reconciliation.
        # _score_trend_confirmation inside PortfolioEngine.compute_target uses
        # `timestamp < current` — changing this order would allow the current
        # score to confirm itself.
        market_context = self._build_market_context(ctx)
        score = self.scoring_engine.score_and_persist(market_context)
        advanced_results = self._persist_advanced_signals(market_context)
        basic_results = self._persist_basic_signals(market_context)

        # Phase 3: reconcile portfolio (acquires connection for reads+writes)
        # Legacy optimizer-cache plumbing is no longer needed because MSI
        # components do not fetch option snapshots.
        cached_option_rows = None
        target = self.portfolio_engine.compute_target_with_advanced_signals(
            score,
            ctx,
            advanced_results=advanced_results,
            basic_results=basic_results,
            cached_option_rows=cached_option_rows,
        )
        action = self.portfolio_engine.reconcile(target)

        logger.info(
            "UnifiedSignalEngine [%s] score=%.3f norm=%.3f dir=%s action=%s",
            self.db_symbol,
            score.composite_score,
            score.normalized_score,
            score.direction,
            action,
        )
        return True
