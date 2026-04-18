"""Unified signal + portfolio reconciliation engine.

This engine is fully self-contained under src/signals and does not depend on
src/analytics modules.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Optional
import os

from src.database import db_connection
from src.signals.components.base import MarketContext
from src.signals.components.dealer_delta_pressure import DealerDeltaPressureComponent
from src.signals.components.dealer_regime import DealerRegimeComponent
from src.signals.components.eod_pressure import EODPressureComponent
from src.signals.components.exhaustion import ExhaustionComponent
from src.signals.components.gamma_flip import GammaFlipComponent
from src.signals.components.gex_gradient import GexGradientComponent
from src.signals.components.gex_regime import GexRegimeComponent
from src.signals.components.intraday_regime import IntradayRegimeComponent
from src.signals.components.opportunity_quality import OpportunityQualityComponent
from src.signals.components.positioning_trap import PositioningTrapComponent
from src.signals.components.put_call_ratio import PutCallRatioComponent
from src.signals.components.skew_delta import SkewDeltaComponent
from src.signals.components.smart_money import SmartMoneyComponent
from src.signals.components.tape_flow_bias import TapeFlowBiasComponent
from src.signals.components.vanna_charm_flow import VannaCharmFlowComponent
from src.signals.components.vol_expansion import VolExpansionComponent
from src.signals.portfolio_engine import PortfolioEngine
from src.signals.scoring_engine import ScoringEngine
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)


class UnifiedSignalEngine:
    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)

        self.scoring_engine = ScoringEngine(
            underlying=self.db_symbol,
            components=[
                GexRegimeComponent(),
                GammaFlipComponent(),
                DealerRegimeComponent(),
                PutCallRatioComponent(),
                SmartMoneyComponent(),
                PositioningTrapComponent(),
                VolExpansionComponent(),
                ExhaustionComponent(),
                OpportunityQualityComponent(self.underlying),
                GexGradientComponent(),
                DealerDeltaPressureComponent(),
                VannaCharmFlowComponent(),
                TapeFlowBiasComponent(),
                SkewDeltaComponent(),
                IntradayRegimeComponent(),
                EODPressureComponent(),
            ],
        )

        self.portfolio_engine = PortfolioEngine(self.underlying)
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

    def _fetch_market_context(self, conn=None) -> Optional[dict]:
        with self._use_conn(conn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT uq.timestamp,
                           uq.close,
                           gs.total_net_gex,
                           gs.gamma_flip_point,
                           gs.put_call_ratio,
                           gs.max_pain,
                           gs.total_call_oi,
                           gs.total_put_oi
                    FROM underlying_quotes uq
                    LEFT JOIN LATERAL (
                        SELECT total_net_gex, gamma_flip_point, put_call_ratio, max_pain,
                               total_call_oi, total_put_oi
                        FROM gex_summary
                        WHERE underlying = %s AND timestamp <= uq.timestamp
                        ORDER BY timestamp DESC
                        LIMIT 1
                    ) gs ON TRUE
                    WHERE uq.symbol = %s
                    ORDER BY uq.timestamp DESC
                    LIMIT 1
                    """,
                    (self.db_symbol, self.db_symbol),
                )
                row = cur.fetchone()
                if not row:
                    return None
                ts, close, net_gex, gamma_flip, pcr, max_pain, total_call_oi, total_put_oi = row
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
                effective_pcr = (
                    oi_pcr if oi_pcr is not None and oi_pcr > 0 else float(pcr or 1.0)
                )

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
                # max_gamma_strike in Python from the same rows.
                gex_strike_rows: list[dict] = []
                call_wall: Optional[float] = None
                max_gamma_strike: Optional[float] = None
                try:
                    cur.execute(
                        """
                        WITH latest AS (
                            SELECT MAX(timestamp) AS ts
                            FROM gex_by_strike
                            WHERE underlying = %s AND timestamp <= %s
                        )
                        SELECT strike,
                               SUM(COALESCE(net_gex, 0))         AS net_gex,
                               SUM(COALESCE(call_oi, 0))         AS call_oi,
                               SUM(COALESCE(put_oi, 0))          AS put_oi,
                               SUM(COALESCE(vanna_exposure, 0))  AS vanna_exposure,
                               SUM(COALESCE(charm_exposure, 0))  AS charm_exposure
                        FROM gex_by_strike g, latest
                        WHERE g.underlying = %s
                          AND g.timestamp = latest.ts
                          AND g.strike BETWEEN %s AND %s
                        GROUP BY strike
                        ORDER BY strike
                        """,
                        (
                            self.db_symbol,
                            ts,
                            self.db_symbol,
                            close_f * 0.90,
                            close_f * 1.10,
                        ),
                    )
                    for r in cur.fetchall():
                        gex_strike_rows.append(
                            {
                                "strike": float(r[0]),
                                "net_gex": float(r[1] or 0.0),
                                "call_oi": int(r[2] or 0),
                                "put_oi": int(r[3] or 0),
                                "vanna_exposure": float(r[4] or 0.0),
                                "charm_exposure": float(r[5] or 0.0),
                            }
                        )
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
                # 15-minute window. Powers tape_flow_bias.
                flow_by_type_rows: list[dict] = []
                try:
                    cur.execute(
                        """
                        SELECT option_type,
                               SUM(COALESCE(buy_volume, 0))    AS buy_volume,
                               SUM(COALESCE(sell_volume, 0))   AS sell_volume,
                               SUM(COALESCE(buy_premium, 0))   AS buy_premium,
                               SUM(COALESCE(sell_premium, 0))  AS sell_premium
                        FROM flow_by_type
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
                        "UnifiedSignalEngine [%s]: flow_by_type fetch failed: %s",
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

                cur.execute(
                    """
                    SELECT close
                    FROM underlying_quotes
                    WHERE symbol = %s
                    ORDER BY timestamp DESC
                    LIMIT 20
                    """,
                    (self.db_symbol,),
                )
                closes = [float(r[0]) for r in cur.fetchall()]

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
                            (self.db_symbol, close_f, close_f, ts, self.db_symbol, close_f, close_f),
                        )
                        iv_row = cur.fetchone()
                        if iv_row and iv_row[0] is not None and iv_row[1] is not None and iv_row[2] is not None:
                            current_iv, iv_low, iv_high = float(iv_row[0]), float(iv_row[1]), float(iv_row[2])
                            iv_range = max(iv_high - iv_low, 0.001)
                            iv_rank = round(min(1.0, max(0.0, (current_iv - iv_low) / iv_range)), 4)
                    except Exception as exc:
                        # IV rank is supplemental; do not block signal generation if unavailable.
                        logger.debug(
                            "UnifiedSignalEngine [%s]: iv_rank query failed: %s",
                            self.db_symbol,
                            exc,
                        )

                return {
                    "timestamp": ts,
                    "close": close_f,
                    "net_gex": float(net_gex or 0.0),
                    "gamma_flip": float(gamma_flip) if gamma_flip is not None else None,
                    "put_call_ratio": effective_pcr,
                    "put_call_ratio_source": "oi" if oi_pcr is not None and oi_pcr > 0 else "volume",
                    "put_call_ratio_volume": float(pcr or 1.0),
                    "put_call_ratio_oi": oi_pcr,
                    "max_pain": float(max_pain) if max_pain is not None else None,
                    "smart_call": float(sm_call or 0.0),
                    "smart_put": float(sm_put or 0.0),
                    "recent_closes": list(reversed(closes)),
                    "iv_rank": iv_rank,
                    "vwap": vwap,
                    "vwap_deviation_pct": vwap_deviation_pct,
                    "call_wall": call_wall,
                    "max_gamma_strike": max_gamma_strike,
                    "gex_by_strike": gex_strike_rows,
                    "flow_by_type": flow_by_type_rows,
                    "skew": skew_info,
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
                "max_gamma_strike": ctx.get("max_gamma_strike"),
                "gex_by_strike": ctx.get("gex_by_strike") or [],
                "flow_by_type": ctx.get("flow_by_type") or [],
                "skew": ctx.get("skew") or {},
                "put_call_ratio_source": ctx.get("put_call_ratio_source"),
                "put_call_ratio_volume": ctx.get("put_call_ratio_volume"),
                "put_call_ratio_oi": ctx.get("put_call_ratio_oi"),
            },
        )

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

        # Phase 3: reconcile portfolio (acquires connection for reads+writes)
        # Pass cached option rows from OpportunityQualityComponent to avoid
        # a duplicate fetch_option_snapshot query in _select_optimizer_candidate.
        cached_option_rows = None
        for component in self.scoring_engine.components:
            if hasattr(component, '_cached_option_rows_key') and component._cached_option_rows is not None:
                cached_option_rows = (component._cached_option_rows_key, component._cached_option_rows)
                break
        target = self.portfolio_engine.compute_target(score, ctx, cached_option_rows=cached_option_rows)
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
