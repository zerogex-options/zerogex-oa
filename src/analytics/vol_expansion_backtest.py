"""Specific historical backtest harness for volatility-expansion signals."""

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Optional

import pytz

from src.analytics.vol_expansion_engine import VolExpansionEngine
from src.database import db_connection
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)
ET = pytz.timezone("US/Eastern")


@dataclass
class BacktestTrade:
    signal_timestamp: datetime
    trade_date: date
    expected_direction: str
    confidence: str
    catalyst_type: str
    move_probability: float
    expected_magnitude_pct: float
    actual_close_to_close_pct: float
    actual_intraday_move_pct: float
    actual_direction: str
    direction_correct: bool
    hit_large_move: bool
    straddle_return_pct: float
    direction_spread_return_pct: float


@dataclass
class BacktestSummary:
    underlying: str
    start_date: str
    end_date: str
    threshold: float
    signal_count: int
    large_move_hit_rate: float
    direction_accuracy: float
    avg_straddle_return_pct: float
    avg_direction_spread_return_pct: float
    profit_factor_straddle: Optional[float]
    profit_factor_directional: Optional[float]


class VolExpansionBacktester:
    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)
        self.engine = VolExpansionEngine(underlying=self.underlying)

    def _generate_signal_for_timestamp(self, timestamp: datetime) -> Optional[dict]:
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT timestamp, composite_score, move_probability, expected_direction,
                           expected_magnitude_pct, confidence, catalyst_type
                    FROM volatility_expansion_signals
                    WHERE underlying = %s AND timestamp = %s
                    """,
                    (self.db_symbol, timestamp),
                )
                persisted = cur.fetchone()
                if persisted:
                    return {
                        "timestamp": persisted[0],
                        "composite_score": int(persisted[1]),
                        "move_probability": float(persisted[2]),
                        "expected_direction": persisted[3],
                        "expected_magnitude_pct": float(persisted[4]),
                        "confidence": persisted[5],
                        "catalyst_type": persisted[6],
                    }
        except Exception as exc:
            logger.error("Failed loading persisted vol signal %s: %s", timestamp, exc)

        ctx = self.engine._fetch_context(as_of=timestamp)
        if ctx is None:
            return None
        signal = self.engine.compute_signal(ctx)
        return {
            "timestamp": signal.timestamp,
            "composite_score": signal.composite_score,
            "move_probability": signal.move_probability,
            "expected_direction": signal.expected_direction,
            "expected_magnitude_pct": signal.expected_magnitude_pct,
            "confidence": signal.confidence,
            "catalyst_type": signal.catalyst_type,
        }

    def _get_outcome(self, trade_date: date) -> Optional[dict]:
        next_date = trade_date + timedelta(days=1)
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    WITH day_quotes AS (
                        SELECT close, high, low, timestamp
                        FROM underlying_quotes
                        WHERE symbol = %s
                          AND DATE(timestamp AT TIME ZONE 'America/New_York') = %s
                    )
                    SELECT
                        (SELECT close FROM day_quotes ORDER BY timestamp ASC LIMIT 1),
                        (SELECT close FROM day_quotes ORDER BY timestamp DESC LIMIT 1),
                        (SELECT MAX(high) FROM day_quotes),
                        (SELECT MIN(low) FROM day_quotes)
                    """,
                    (self.db_symbol, next_date),
                )
                row = cur.fetchone()
                if not row or row[0] is None or row[1] is None:
                    return None
                open_px, close_px, high_px, low_px = map(float, row)
                close_to_close = ((close_px - open_px) / open_px) * 100.0
                intraday_move = max(abs(high_px - open_px), abs(low_px - open_px)) / open_px * 100.0
                return {
                    "close_to_close_pct": round(close_to_close, 4),
                    "intraday_move_pct": round(intraday_move, 4),
                    "direction": "up" if close_to_close > 0 else ("down" if close_to_close < 0 else "neutral"),
                }
        except Exception as exc:
            logger.error("Failed loading outcome for %s: %s", next_date, exc)
            return None

    @staticmethod
    def _estimate_straddle_return(move_probability: float, intraday_move_pct: float) -> float:
        implied_break_even = max(0.35, move_probability * 0.75)
        return round((intraday_move_pct - implied_break_even) * 100.0, 2)

    @staticmethod
    def _estimate_directional_return(expected_direction: str, actual_direction: str, close_to_close_pct: float) -> float:
        if expected_direction == "neutral":
            return 0.0
        aligned = expected_direction == actual_direction
        magnitude = abs(close_to_close_pct)
        if aligned:
            return round(min(150.0, magnitude * 120.0), 2)
        return round(-min(100.0, max(25.0, magnitude * 90.0)), 2)

    def run(self, start_date: date, end_date: date, threshold: float, export_dir: Optional[Path]) -> BacktestSummary:
        trades: list[BacktestTrade] = []
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT timestamp
                    FROM gex_summary
                    WHERE underlying = %s
                      AND DATE(timestamp AT TIME ZONE 'America/New_York') BETWEEN %s AND %s
                    ORDER BY timestamp ASC
                    """,
                    (self.db_symbol, start_date, end_date),
                )
                timestamps = [row[0] for row in cur.fetchall()]
        except Exception as exc:
            logger.error("Failed to load backtest timestamps: %s", exc)
            timestamps = []

        for timestamp in timestamps:
            signal = self._generate_signal_for_timestamp(timestamp)
            if not signal or signal["move_probability"] < threshold:
                continue
            trade_date = timestamp.astimezone(ET).date() if timestamp.tzinfo else timestamp.date()
            outcome = self._get_outcome(trade_date)
            if not outcome:
                continue
            hit_large_move = outcome["intraday_move_pct"] >= 0.5 or abs(outcome["close_to_close_pct"]) >= 0.5
            direction_correct = signal["expected_direction"] == outcome["direction"] if signal["expected_direction"] != "neutral" else hit_large_move
            trades.append(
                BacktestTrade(
                    signal_timestamp=timestamp,
                    trade_date=trade_date,
                    expected_direction=signal["expected_direction"],
                    confidence=signal["confidence"],
                    catalyst_type=signal["catalyst_type"],
                    move_probability=signal["move_probability"],
                    expected_magnitude_pct=signal["expected_magnitude_pct"],
                    actual_close_to_close_pct=outcome["close_to_close_pct"],
                    actual_intraday_move_pct=outcome["intraday_move_pct"],
                    actual_direction=outcome["direction"],
                    direction_correct=direction_correct,
                    hit_large_move=hit_large_move,
                    straddle_return_pct=self._estimate_straddle_return(signal["move_probability"], outcome["intraday_move_pct"]),
                    direction_spread_return_pct=self._estimate_directional_return(
                        signal["expected_direction"], outcome["direction"], outcome["close_to_close_pct"]
                    ),
                )
            )

        summary = self._summarize(trades, start_date, end_date, threshold)
        if export_dir:
            self._export(export_dir, summary, trades)
        return summary

    def _summarize(self, trades: list[BacktestTrade], start_date: date, end_date: date, threshold: float) -> BacktestSummary:
        if not trades:
            return BacktestSummary(
                underlying=self.db_symbol,
                start_date=str(start_date),
                end_date=str(end_date),
                threshold=threshold,
                signal_count=0,
                large_move_hit_rate=0.0,
                direction_accuracy=0.0,
                avg_straddle_return_pct=0.0,
                avg_direction_spread_return_pct=0.0,
                profit_factor_straddle=None,
                profit_factor_directional=None,
            )
        straddle_pnl = [t.straddle_return_pct for t in trades]
        direction_pnl = [t.direction_spread_return_pct for t in trades]
        return BacktestSummary(
            underlying=self.db_symbol,
            start_date=str(start_date),
            end_date=str(end_date),
            threshold=threshold,
            signal_count=len(trades),
            large_move_hit_rate=round(sum(t.hit_large_move for t in trades) / len(trades), 4),
            direction_accuracy=round(sum(t.direction_correct for t in trades) / len(trades), 4),
            avg_straddle_return_pct=round(mean(straddle_pnl), 2),
            avg_direction_spread_return_pct=round(mean(direction_pnl), 2),
            profit_factor_straddle=self._profit_factor(straddle_pnl),
            profit_factor_directional=self._profit_factor(direction_pnl),
        )

    @staticmethod
    def _profit_factor(pnls: list[float]) -> Optional[float]:
        gross_profit = sum(p for p in pnls if p > 0)
        gross_loss = abs(sum(p for p in pnls if p < 0))
        if gross_loss == 0:
            return None if gross_profit == 0 else round(gross_profit, 2)
        return round(gross_profit / gross_loss, 4)

    @staticmethod
    def _export(export_dir: Path, summary: BacktestSummary, trades: list[BacktestTrade]) -> None:
        export_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        summary_path = export_dir / f"vol_expansion_summary_{stamp}.json"
        trades_path = export_dir / f"vol_expansion_trades_{stamp}.csv"
        summary_path.write_text(json.dumps(asdict(summary), indent=2, default=str))
        with trades_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(asdict(trades[0]).keys()) if trades else [])
            if trades:
                writer.writeheader()
                for trade in trades:
                    writer.writerow(asdict(trade))


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest stored volatility expansion signals")
    parser.add_argument("--underlying", default="SPY")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--threshold", type=float, default=0.65)
    parser.add_argument("--export-dir", default=None)
    args = parser.parse_args()

    backtester = VolExpansionBacktester(underlying=args.underlying)
    summary = backtester.run(
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        threshold=args.threshold,
        export_dir=Path(args.export_dir) if args.export_dir else None,
    )
    print(json.dumps(asdict(summary), indent=2, default=str))


if __name__ == "__main__":
    main()
