"""Historical backtest harness for the position optimizer engine."""

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from statistics import mean
from typing import Optional

from src.analytics.position_optimizer_engine import PositionOptimizerEngine
from src.database import db_connection
from src.symbols import get_canonical_symbol
from src.utils import get_logger

logger = get_logger(__name__)


@dataclass
class PositionOptimizerBacktestTrade:
    signal_timestamp: datetime
    signal_direction: str
    signal_timeframe: str
    strategy_type: str
    strikes: str
    probability_of_profit: float
    expected_value: float
    realized_return_pct: float
    profitable: bool


@dataclass
class PositionOptimizerBacktestSummary:
    underlying: str
    start_date: str
    end_date: str
    min_pop: float
    signal_count: int
    hit_rate: float
    avg_expected_value: float
    avg_realized_return_pct: float
    avg_probability_of_profit: float
    profit_factor: Optional[float]


class PositionOptimizerBacktester:
    def __init__(self, underlying: str = "SPY"):
        self.underlying = underlying.upper()
        self.db_symbol = get_canonical_symbol(self.underlying)
        self.engine = PositionOptimizerEngine(underlying=self.underlying)

    def _generate_signal_for_timestamp(self, timestamp: datetime) -> Optional[dict]:
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT timestamp, signal_direction, signal_timeframe, top_strategy_type, candidates
                    FROM position_optimizer_signals
                    WHERE underlying = %s AND timestamp = %s
                    """,
                    (self.db_symbol, timestamp),
                )
                row = cur.fetchone()
                if row:
                    candidates = json.loads(row[4]) if isinstance(row[4], str) else (row[4] or [])
                    return {
                        "timestamp": row[0],
                        "signal_direction": row[1],
                        "signal_timeframe": row[2],
                        "top_strategy_type": row[3],
                        "candidate": candidates[0] if candidates else None,
                    }
        except Exception as exc:
            logger.error("Failed loading stored position optimizer signal %s: %s", timestamp, exc)

        ctx = self.engine._fetch_context(as_of=timestamp)
        if ctx is None:
            return None
        signal = self.engine.compute_signal(ctx)
        if signal is None or not signal.candidates:
            return None
        return {
            "timestamp": signal.timestamp,
            "signal_direction": signal.signal_direction,
            "signal_timeframe": signal.signal_timeframe,
            "top_strategy_type": signal.top_strategy_type,
            "candidate": asdict(signal.candidates[0]),
        }

    def run(self, start_date: date, end_date: date, min_pop: float, export_dir: Optional[Path]) -> PositionOptimizerBacktestSummary:
        trades: list[PositionOptimizerBacktestTrade] = []
        try:
            with db_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT DISTINCT timestamp
                    FROM trade_signals
                    WHERE underlying = %s
                      AND DATE(timestamp AT TIME ZONE 'America/New_York') BETWEEN %s AND %s
                    ORDER BY timestamp ASC
                    """,
                    (self.db_symbol, start_date, end_date),
                )
                timestamps = [row[0] for row in cur.fetchall()]
        except Exception as exc:
            logger.error("Failed loading timestamps for position optimizer backtest: %s", exc)
            timestamps = []

        for timestamp in timestamps:
            signal = self._generate_signal_for_timestamp(timestamp)
            if not signal or not signal.get("candidate"):
                continue
            candidate = signal["candidate"]
            if float(candidate.get("probability_of_profit") or 0.0) < min_pop:
                continue
            snapshot = self.engine._snapshot_accuracy(timestamp, candidate)
            if snapshot is None:
                continue
            trades.append(
                PositionOptimizerBacktestTrade(
                    signal_timestamp=timestamp,
                    signal_direction=signal["signal_direction"],
                    signal_timeframe=signal["signal_timeframe"],
                    strategy_type=candidate.get("strategy_type", signal["top_strategy_type"]),
                    strikes=candidate.get("strikes", ""),
                    probability_of_profit=float(candidate.get("probability_of_profit") or 0.0),
                    expected_value=float(candidate.get("expected_value") or 0.0),
                    realized_return_pct=float(snapshot.proxy_return_pct),
                    profitable=bool(snapshot.profitable),
                )
            )

        summary = self._summarize(trades, start_date, end_date, min_pop)
        if export_dir:
            self._export(export_dir, summary, trades)
        return summary

    def _summarize(self, trades: list[PositionOptimizerBacktestTrade], start_date: date, end_date: date, min_pop: float) -> PositionOptimizerBacktestSummary:
        if not trades:
            return PositionOptimizerBacktestSummary(
                underlying=self.db_symbol,
                start_date=str(start_date),
                end_date=str(end_date),
                min_pop=min_pop,
                signal_count=0,
                hit_rate=0.0,
                avg_expected_value=0.0,
                avg_realized_return_pct=0.0,
                avg_probability_of_profit=0.0,
                profit_factor=None,
            )
        realized = [t.realized_return_pct for t in trades]
        return PositionOptimizerBacktestSummary(
            underlying=self.db_symbol,
            start_date=str(start_date),
            end_date=str(end_date),
            min_pop=min_pop,
            signal_count=len(trades),
            hit_rate=round(sum(t.profitable for t in trades) / len(trades), 4),
            avg_expected_value=round(mean(t.expected_value for t in trades), 2),
            avg_realized_return_pct=round(mean(realized), 2),
            avg_probability_of_profit=round(mean(t.probability_of_profit for t in trades), 4),
            profit_factor=self._profit_factor(realized),
        )

    @staticmethod
    def _profit_factor(values: list[float]) -> Optional[float]:
        gross_profit = sum(v for v in values if v > 0)
        gross_loss = abs(sum(v for v in values if v < 0))
        if gross_loss == 0:
            return None if gross_profit == 0 else round(gross_profit, 2)
        return round(gross_profit / gross_loss, 4)

    @staticmethod
    def _export(export_dir: Path, summary: PositionOptimizerBacktestSummary, trades: list[PositionOptimizerBacktestTrade]) -> None:
        export_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        summary_path = export_dir / f"position_optimizer_summary_{stamp}.json"
        trades_path = export_dir / f"position_optimizer_trades_{stamp}.csv"
        summary_path.write_text(json.dumps(asdict(summary), indent=2, default=str))
        with trades_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(asdict(trades[0]).keys()) if trades else [])
            if trades:
                writer.writeheader()
                for trade in trades:
                    writer.writerow(asdict(trade))


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest stored position optimizer signals")
    parser.add_argument("--underlying", default="SPY")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--min-pop", type=float, default=0.55)
    parser.add_argument("--export-dir", default=None)
    args = parser.parse_args()

    backtester = PositionOptimizerBacktester(underlying=args.underlying)
    summary = backtester.run(
        start_date=date.fromisoformat(args.start_date),
        end_date=date.fromisoformat(args.end_date),
        min_pop=args.min_pop,
        export_dir=Path(args.export_dir) if args.export_dir else None,
    )
    print(json.dumps(asdict(summary), indent=2, default=str))


if __name__ == "__main__":
    main()
