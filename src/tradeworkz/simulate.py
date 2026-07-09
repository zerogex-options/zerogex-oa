"""Deterministic historical-trade simulator for TradeWorkz.

Populates ``tw_trades`` + ``tw_equity_curve_daily`` + ``tw_bot_metrics_daily``
+ ``tw_ml_state`` with realistic-looking synthetic trade history so the
/trading-signals dashboard has something to render before real ticks
accumulate. Each bot has a distinct performance personality (see
:data:`BOT_PERSONALITY`) so the leaderboard actually differentiates them.

The simulator is deterministic — a ``(bot_id, session_date, seed)`` triple
seeds Python's ``random.Random`` and the same call produces the same output.
It's idempotent — before inserting, it wipes the tables for any bot the
simulation touches. Callable from the admin API or from a shell script.
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# -- Per-bot performance personality --------------------------------------
#
# Numbers picked to produce a leaderboard where the wall-bouncer / gamma-flip
# defender lead, the vwap scalper is a steady middle, the vix breakout is a
# high-variance high-return outlier, and one bot (opening_range_hunter) is
# marginally negative to make the "worst bot" callout meaningful. Ordered by
# expected return, high-to-low, so the fleet feels differentiated.

BOT_PERSONALITY: Dict[str, Dict[str, float]] = {
    "put_call_wall_bouncer": {
        "trades_per_day": 2.6,
        "win_rate": 0.63,
        "avg_win_pct": 0.09,
        "avg_loss_pct": -0.11,
        "win_stdev": 0.03,
        "loss_stdev": 0.04,
    },
    "gamma_flip_defender": {
        "trades_per_day": 1.9,
        "win_rate": 0.60,
        "avg_win_pct": 0.08,
        "avg_loss_pct": -0.10,
        "win_stdev": 0.03,
        "loss_stdev": 0.03,
    },
    "vwap_reversion_scalper": {
        "trades_per_day": 4.5,
        "win_rate": 0.68,
        "avg_win_pct": 0.04,
        "avg_loss_pct": -0.07,
        "win_stdev": 0.015,
        "loss_stdev": 0.025,
    },
    "eod_pin_drifter": {
        "trades_per_day": 0.7,
        "win_rate": 0.58,
        "avg_win_pct": 0.07,
        "avg_loss_pct": -0.09,
        "win_stdev": 0.03,
        "loss_stdev": 0.03,
    },
    "max_pain_gravitator": {
        "trades_per_day": 0.4,
        "win_rate": 0.52,
        "avg_win_pct": 0.14,
        "avg_loss_pct": -0.12,
        "win_stdev": 0.05,
        "loss_stdev": 0.04,
    },
    "gamma_flip_breaker": {
        "trades_per_day": 1.3,
        "win_rate": 0.44,
        "avg_win_pct": 0.19,
        "avg_loss_pct": -0.10,
        "win_stdev": 0.08,
        "loss_stdev": 0.03,
    },
    "vix_regime_breakout": {
        "trades_per_day": 0.9,
        "win_rate": 0.42,
        "avg_win_pct": 0.24,
        "avg_loss_pct": -0.11,
        "win_stdev": 0.10,
        "loss_stdev": 0.04,
    },
    "dealer_delta_pressure_rider": {
        "trades_per_day": 1.4,
        "win_rate": 0.50,
        "avg_win_pct": 0.07,
        "avg_loss_pct": -0.08,
        "win_stdev": 0.03,
        "loss_stdev": 0.03,
    },
    "opening_range_hunter": {
        "trades_per_day": 1.6,
        "win_rate": 0.46,
        "avg_win_pct": 0.06,
        "avg_loss_pct": -0.09,
        "win_stdev": 0.025,
        "loss_stdev": 0.03,
    },
    # v2 bots — personalities picked so the leaderboard tells a coherent
    # story: the bullish spread bot is a steady positive-EV grinder, the
    # iron condor is high-hit-rate low-magnitude, the straddle is
    # low-hit-rate high-magnitude, and the QQQ mirrors track their SPY
    # siblings but with slightly different profiles reflecting QQQ's
    # generally higher realized vol.
    "bull_momentum_climber": {
        "trades_per_day": 1.1,
        "win_rate": 0.57,
        "avg_win_pct": 0.11,
        "avg_loss_pct": -0.09,
        "win_stdev": 0.03,
        "loss_stdev": 0.03,
    },
    "range_iron_condor": {
        "trades_per_day": 0.8,
        "win_rate": 0.72,
        "avg_win_pct": 0.05,
        "avg_loss_pct": -0.13,
        "win_stdev": 0.015,
        "loss_stdev": 0.04,
    },
    "vol_expansion_straddle": {
        "trades_per_day": 0.6,
        "win_rate": 0.38,
        "avg_win_pct": 0.28,
        "avg_loss_pct": -0.10,
        "win_stdev": 0.12,
        "loss_stdev": 0.03,
    },
    "qqq_gamma_flip_breaker": {
        "trades_per_day": 1.5,
        "win_rate": 0.45,
        "avg_win_pct": 0.20,
        "avg_loss_pct": -0.11,
        "win_stdev": 0.08,
        "loss_stdev": 0.035,
    },
    "qqq_dealer_delta_pressure_rider": {
        "trades_per_day": 1.6,
        "win_rate": 0.51,
        "avg_win_pct": 0.08,
        "avg_loss_pct": -0.09,
        "win_stdev": 0.035,
        "loss_stdev": 0.035,
    },
}

_CLOSE_REASONS = ("target", "stop", "time_stop", "signal", "wall_shift")

# ET session bounds used when synthesizing timestamps.
_SESSION_OPEN_MINUTE = 9 * 60 + 45   # 09:45 ET
_SESSION_CLOSE_MINUTE = 15 * 60 + 55  # 15:55 ET


def _seed_for(bot_id: str, session_date: date, master_seed: int) -> int:
    """Stable hash → deterministic RNG per (bot_id, date)."""
    key = f"{bot_id}|{session_date.isoformat()}|{master_seed}".encode()
    return int(hashlib.blake2b(key, digest_size=8).hexdigest(), 16)


def _trading_dates(days: int, end: date) -> List[date]:
    """Roll ``days`` calendar days back from ``end`` and keep only weekdays."""
    out: List[date] = []
    cursor = end
    while len(out) < days:
        if cursor.weekday() < 5:
            out.append(cursor)
        cursor -= timedelta(days=1)
    out.sort()
    return out


def _sample_trade(
    rng: random.Random,
    session_date: date,
    starting_capital: float,
    personality: Dict[str, float],
    scale: float,
) -> Dict[str, Any]:
    """One synthesized closed-trade row."""
    won = rng.random() < personality["win_rate"]
    if won:
        pct = rng.gauss(personality["avg_win_pct"], personality["win_stdev"])
        outcome = "win"
    else:
        pct = rng.gauss(personality["avg_loss_pct"], personality["loss_stdev"])
        outcome = "loss"
    pct *= scale
    # Scale the dollar move roughly to a fraction of the sleeve — small
    # enough that no single trade blows out the capital curve but big
    # enough that the equity chart has visible daily swings.
    per_trade_notional = starting_capital * 0.015
    realized_pnl = pct * per_trade_notional

    open_min = rng.randint(_SESSION_OPEN_MINUTE, _SESSION_CLOSE_MINUTE - 30)
    hold_min = rng.randint(10, 90)
    close_min = min(_SESSION_CLOSE_MINUTE, open_min + hold_min)

    opened_at = datetime(
        session_date.year, session_date.month, session_date.day,
        open_min // 60, open_min % 60, tzinfo=timezone.utc,
    ) - timedelta(hours=4)  # rough ET->UTC (dashboard is display-only)
    closed_at = datetime(
        session_date.year, session_date.month, session_date.day,
        close_min // 60, close_min % 60, tzinfo=timezone.utc,
    ) - timedelta(hours=4)

    direction = rng.choice(("bullish", "bearish"))
    # Entry price: pick a plausible single-leg option premium so pnl_pct
    # ties back to a per-share number the user would read as sane.
    entry_price = round(rng.uniform(1.5, 6.0), 2)
    exit_price = round(max(0.05, entry_price * (1 + pct)), 2)
    quantity = rng.randint(2, 8)

    if outcome == "win":
        reason = rng.choice(("target", "signal", "wall_shift"))
    else:
        reason = rng.choice(("stop", "time_stop", "wall_shift"))

    legs = [
        {
            "option_symbol": f"SPY{session_date.strftime('%y%m%d')}"
            f"{'C' if direction == 'bullish' else 'P'}"
            f"{int(rng.uniform(400, 600) * 1000):08d}",
            "side": "long",
            "option_type": "call" if direction == "bullish" else "put",
            "strike": round(rng.uniform(400, 600), 2),
            "expiration": session_date.isoformat(),
        }
    ]

    return {
        "opened_at": opened_at,
        "closed_at": closed_at,
        "direction": direction,
        "strategy_type": "BUY_CALL_DEBIT" if direction == "bullish" else "BUY_PUT_DEBIT",
        "legs": legs,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "quantity": quantity,
        "realized_pnl": round(realized_pnl, 4),
        "pnl_percent": round(pct, 4),
        "outcome": outcome,
        "close_reason": reason,
        "entry_conviction": round(rng.uniform(0.55, 0.85), 4),
    }


def _wipe_bot_history(conn: Any, bot_ids: List[str]) -> Dict[str, int]:
    """Delete simulated AND live history for the given bots.

    Returns a per-table count of rows deleted so the admin surface can
    show exactly what was wiped (useful when an operator wants to know
    "did that clear actually do anything?"). We wipe both sim and live
    rows in one pass — a caller invoking this is asking for a full reset
    to the starting sleeve, not a selective purge; if we ever want
    "delete only sim rows," add a separate helper that filters on
    ``components_at_entry->>'origin' = 'simulate'``.
    """
    cur = conn.cursor()
    tables = [
        "tw_notifications_log",
        "tw_trades",
        "tw_positions",
        "tw_equity_curve_daily",
        "tw_bot_metrics_daily",
        "tw_ml_state",
    ]
    deleted: Dict[str, int] = {}
    for table in tables:
        cur.execute(f"DELETE FROM {table} WHERE bot_id = ANY(%s)", (bot_ids,))
        deleted[table] = cur.rowcount if cur.rowcount is not None else 0
    return deleted


def simulate(
    conn: Any,
    *,
    days: int = 60,
    master_seed: int = 42,
    scale: float = 1.0,
    end_date: Optional[date] = None,
    bot_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Populate tw_trades / equity / metrics / ml_state.

    Returns a summary payload safe to return from the API handler.
    """
    cur = conn.cursor()
    cur.execute("SELECT id FROM tw_bots WHERE enabled = TRUE ORDER BY id")
    all_bot_ids = [row[0] for row in cur.fetchall()]
    if bot_ids is None:
        target_bots = all_bot_ids
    else:
        target_bots = [b for b in bot_ids if b in all_bot_ids]
    if not target_bots:
        return {"status": "no-op", "reason": "no enabled bots to simulate"}

    _wipe_bot_history(conn, target_bots)

    cur.execute(
        "SELECT bot_id, starting_capital FROM tw_bot_capital WHERE bot_id = ANY(%s)",
        (target_bots,),
    )
    starting_capital = {row[0]: float(row[1]) for row in cur.fetchall()}

    end = end_date or date.today()
    sessions = _trading_dates(days, end)

    total_trades = 0
    per_bot_summary: Dict[str, Any] = {}

    for bot_id in target_bots:
        personality = BOT_PERSONALITY.get(bot_id) or {
            "trades_per_day": 1.5,
            "win_rate": 0.50,
            "avg_win_pct": 0.06,
            "avg_loss_pct": -0.08,
            "win_stdev": 0.02,
            "loss_stdev": 0.03,
        }
        sleeve = starting_capital.get(bot_id, 100_000.0)

        session_pnl: Dict[date, Tuple[float, int, int, int]] = {}
        # (realized_pnl, n_trades, wins, losses) per session

        for sd in sessions:
            rng = random.Random(_seed_for(bot_id, sd, master_seed))
            expected = personality["trades_per_day"]
            # Bernoulli-ish trade count: mean = expected, floor 0, cap 8.
            n_trades = max(0, min(8, int(round(rng.gauss(expected, expected * 0.4)))))
            session_realized = 0.0
            session_wins = 0
            session_losses = 0
            for _ in range(n_trades):
                t = _sample_trade(rng, sd, sleeve, personality, scale)
                cur.execute(
                    """
                    INSERT INTO tw_trades (
                        bot_id, underlying, opened_at, closed_at, direction,
                        strategy_type, legs, entry_price, exit_price, quantity,
                        realized_pnl, pnl_percent, outcome, close_reason,
                        entry_conviction, components_at_entry, components_at_exit
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s::jsonb, %s::jsonb
                    )
                    """,
                    (
                        bot_id,
                        "SPY",
                        t["opened_at"],
                        t["closed_at"],
                        t["direction"],
                        t["strategy_type"],
                        json.dumps(t["legs"]),
                        t["entry_price"],
                        t["exit_price"],
                        t["quantity"],
                        t["realized_pnl"],
                        t["pnl_percent"],
                        t["outcome"],
                        t["close_reason"],
                        t["entry_conviction"],
                        # `origin` is the canonical provenance marker the
                        # audit surface reads; `simulated` is kept for
                        # backward-compat with existing filters.
                        json.dumps({"simulated": True, "origin": "simulate"}),
                        json.dumps(
                            {
                                "simulated": True,
                                "origin": "simulate",
                                "reason": t["close_reason"],
                            }
                        ),
                    ),
                )
                session_realized += t["realized_pnl"]
                total_trades += 1
                if t["outcome"] == "win":
                    session_wins += 1
                elif t["outcome"] == "loss":
                    session_losses += 1
            session_pnl[sd] = (session_realized, n_trades, session_wins, session_losses)

        # Roll up equity curve + daily metrics for this bot.
        starting_nav = sleeve
        running = sleeve
        peak = sleeve
        lifetime_wins = 0
        lifetime_losses = 0
        gross_win = 0.0
        gross_loss = 0.0
        for sd in sessions:
            realized, n, wins, losses = session_pnl[sd]
            open_nav = running
            running += realized
            peak = max(peak, running)
            cur.execute(
                """
                INSERT INTO tw_equity_curve_daily (
                    bot_id, session_date, starting_nav, ending_nav,
                    realized_pnl, unrealized_pnl, heat_pct, n_trades
                ) VALUES (%s, %s, %s, %s, %s, 0, 0, %s)
                """,
                (bot_id, sd, open_nav, running, realized, n),
            )
            lifetime_wins += wins
            lifetime_losses += losses
            # Attribution of gross win/loss dollars for profit_factor rollup.
            #   avg_win_dollars ≈ personality_avg_win_pct × per_trade_notional
            avg_win_dollars = personality["avg_win_pct"] * sleeve * 0.015
            avg_loss_dollars = personality["avg_loss_pct"] * sleeve * 0.015
            gross_win += wins * avg_win_dollars
            gross_loss += losses * abs(avg_loss_dollars)

            if n > 0:
                wr = wins / n if n else None
                cur.execute(
                    """
                    INSERT INTO tw_bot_metrics_daily (
                        bot_id, session_date, trades_count, wins, losses,
                        win_rate, avg_win_pnl, avg_loss_pnl, profit_factor
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        bot_id,
                        sd,
                        n,
                        wins,
                        losses,
                        wr,
                        avg_win_dollars,
                        avg_loss_dollars,
                        (avg_win_dollars / abs(avg_loss_dollars))
                        if avg_loss_dollars < 0 else None,
                    ),
                )

        # Update the bot's live capital sleeve to reflect the simulation.
        cur.execute(
            """
            UPDATE tw_bot_capital
            SET current_capital = %s,
                peak_capital = %s,
                updated_at = NOW()
            WHERE bot_id = %s
            """,
            (running, peak, bot_id),
        )

        # Seed a plausible ml_state so the ML calibration panel isn't empty.
        n_lifetime = lifetime_wins + lifetime_losses
        hit_rate = (lifetime_wins / n_lifetime) if n_lifetime else None
        profit_factor = (gross_win / gross_loss) if gross_loss > 0 else None
        size_mult = 0.75 if (profit_factor or 0) < 1.0 else (
            1.25 if (profit_factor or 0) >= 1.4 else 1.0
        )
        confidence_base = round(max(0.40, min(0.85, hit_rate or 0.55)), 4)
        cur.execute(
            """
            INSERT INTO tw_ml_state (
                bot_id, n_samples, n_wins, hit_rate, confidence_base,
                confidence_threshold, size_multiplier, weights, feature_mean,
                feature_var, last_win_rate_7d, last_win_rate_30d,
                last_profit_factor, computed_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, NOW()
            )
            """,
            (
                bot_id,
                n_lifetime,
                lifetime_wins,
                hit_rate,
                confidence_base,
                round(max(0.45, min(0.75, 1.0 - (hit_rate or 0.5) + 0.30)), 4),
                size_mult,
                json.dumps({}),
                json.dumps({}),
                json.dumps({}),
                hit_rate,
                hit_rate,
                profit_factor,
            ),
        )

        per_bot_summary[bot_id] = {
            "trades": n_lifetime,
            "wins": lifetime_wins,
            "losses": lifetime_losses,
            "hit_rate": hit_rate,
            "profit_factor": profit_factor,
            "final_nav": round(running, 2),
            "return_pct": (running / sleeve - 1.0) if sleeve else None,
        }

    return {
        "status": "ok",
        "days_simulated": len(sessions),
        "bots_simulated": target_bots,
        "total_trades": total_trades,
        "per_bot": per_bot_summary,
        "seed": master_seed,
        "scale": scale,
    }


def clear(conn: Any, bot_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Wipe ALL history (sim + live) and reset every sleeve to starting_capital.

    Full fleet reset. After this returns, every bot in ``bot_ids`` (or
    every bot if ``bot_ids`` is None) is at Day Zero: no trades, no
    positions, no notifications, no equity/metrics, no ML state, and
    capital = starting_capital. Live ticks resume immediately if the
    engine is running — the sleeve just starts from scratch.

    The response includes per-table deletion counts so callers can see
    exactly what was removed. This is the endpoint you hit after a
    P&L math bug corrupts the sleeve with bogus wins.
    """
    cur = conn.cursor()
    if bot_ids is None:
        cur.execute("SELECT id FROM tw_bots ORDER BY id")
        bot_ids = [row[0] for row in cur.fetchall()]
    if not bot_ids:
        return {"status": "no-op", "reason": "no bots to clear"}
    deleted = _wipe_bot_history(conn, bot_ids)
    cur.execute(
        """
        UPDATE tw_bot_capital
        SET current_capital = starting_capital,
            peak_capital = starting_capital,
            updated_at = NOW()
        WHERE bot_id = ANY(%s)
        """,
        (bot_ids,),
    )
    sleeves_reset = cur.rowcount if cur.rowcount is not None else 0
    return {
        "status": "ok",
        "cleared": bot_ids,
        "deleted_rows": deleted,
        "sleeves_reset": sleeves_reset,
    }
