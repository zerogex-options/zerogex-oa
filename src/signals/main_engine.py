"""Standalone Signal Engine service.

Runs always-on per-symbol scoring plus hypothetical signal/trade lifecycle
management as a dedicated process.
"""

from __future__ import annotations

import argparse
import signal
import time
from multiprocessing import Process

from src.config import SIGNALS_UNDERLYINGS
from src.signals.signal_scoring_engine import SignalScoringEngine
from src.signals.hypothetical_trade_engine import HypotheticalTradeEngine
from src.symbols import parse_underlyings
from src.utils import get_logger

logger = get_logger(__name__)


class SignalEngineService:
    def __init__(self, underlying: str = "SPY", interval_seconds: int = 300):
        self.underlying = underlying.upper()
        self.interval_seconds = max(30, int(interval_seconds))
        self.running = False
        self.scoring_engine = SignalScoringEngine(underlying=self.underlying)
        self.trade_engine = HypotheticalTradeEngine(underlying=self.underlying)

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logger.info("SignalEngineService received signal %s - stopping", signum)
        self.running = False

    def run_cycle(self) -> None:
        score = self.scoring_engine.run_cycle()
        scoring_ok = score is not None
        trades_ok = self.trade_engine.run_cycle(score)
        logger.info(
            "SignalEngineService cycle [%s] complete | score=%s trades=%s",
            self.underlying,
            scoring_ok,
            trades_ok,
        )

    def run(self) -> None:
        self.running = True
        logger.info(
            "Starting SignalEngineService underlying=%s interval=%ss",
            self.underlying,
            self.interval_seconds,
        )
        while self.running:
            started = time.time()
            try:
                self.run_cycle()
            except Exception as exc:
                logger.error("SignalEngineService cycle failed: %s", exc, exc_info=True)
            elapsed = time.time() - started
            sleep_for = max(1.0, self.interval_seconds - elapsed)
            time.sleep(sleep_for)


def _run_for_symbol(symbol: str, interval: int) -> None:
    SignalEngineService(underlying=symbol, interval_seconds=interval).run()


def main() -> None:
    parser = argparse.ArgumentParser(description="ZeroGEX Signal Engine service")
    parser.add_argument("--underlying", default=SIGNALS_UNDERLYINGS)
    parser.add_argument("--interval", type=int, default=300)
    args = parser.parse_args()

    symbols = parse_underlyings(args.underlying)
    if len(symbols) == 1:
        SignalEngineService(underlying=symbols[0], interval_seconds=args.interval).run()
        return

    processes: list[Process] = []
    for symbol in symbols:
        process = Process(target=_run_for_symbol, args=(symbol, args.interval), name=f"signals-{symbol}")
        process.start()
        processes.append(process)

    for process in processes:
        process.join()


if __name__ == "__main__":
    main()
