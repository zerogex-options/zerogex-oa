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
from src.signals.components.exhaustion import ExhaustionComponent
from src.signals.components.gamma_flip import GammaFlipComponent
from src.signals.components.gex_regime import GexRegimeComponent
from src.signals.components.opportunity_quality import OpportunityQualityComponent
from src.signals.components.positioning_trap import PositioningTrapComponent
from src.signals.components.put_call_ratio import PutCallRatioComponent
from src.signals.components.smart_money import SmartMoneyComponent
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
                PutCallRatioComponent(),
                SmartMoneyComponent(),
                PositioningTrapComponent(),
                VolExpansionComponent(),
                ExhaustionComponent(),
                OpportunityQualityComponent(self.underlying),
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
                           gs.max_pain
                    FROM underlying_quotes uq
                    LEFT JOIN LATERAL (
                        SELECT total_net_gex, gamma_flip_point, put_call_ratio, max_pain
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
                ts, close, net_gex, gamma_flip, pcr, max_pain = row
                close_f = float(close)

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
                            iv_range = iv_high - iv_low
                            if iv_range > 0.001:
                                iv_rank = round(min(1.0, max(0.0, (current_iv - iv_low) / iv_range)), 4)
                    except Exception:
                        pass  # IV rank is supplemental; do not block signal generation if unavailable

                return {
                    "timestamp": ts,
                    "close": close_f,
                    "net_gex": float(net_gex or 0.0),
                    "gamma_flip": float(gamma_flip) if gamma_flip is not None else None,
                    "put_call_ratio": float(pcr or 1.0),
                    "max_pain": float(max_pain) if max_pain is not None else None,
                    "smart_call": float(sm_call or 0.0),
                    "smart_put": float(sm_put or 0.0),
                    "recent_closes": list(reversed(closes)),
                    "iv_rank": iv_rank,
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
