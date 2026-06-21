"""Standalone backtest worker (Phase 4).

Drains queued ``backtest_runs`` and executes them out-of-process, so long runs
don't tie up an API thread and survive API restarts. Enable by running this
service AND setting ``BACKTEST_WORKER_ENABLED=1`` (which stops the API from
also executing runs in-process via BackgroundTasks).

Run:
    python -m src.backtesting.worker
    BACKTEST_WORKER_POLL_SECONDS=2 python -m src.backtesting.worker

Multiple instances are safe: claims use ``FOR UPDATE SKIP LOCKED``.
"""

from __future__ import annotations

import logging
import signal
import time

from src import config
from src.backtesting.runner import claim_next_queued_run, execute_run, requeue_stale_runs
from src.database.connection import close_db_connection, get_db_connection

logger = logging.getLogger(__name__)


class BacktestWorker:
    def __init__(self, poll_interval: float | None = None):
        self.poll_interval = (
            poll_interval if poll_interval is not None
            else config.BACKTEST_WORKER_POLL_SECONDS
        )
        self.running = False
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, signum, frame):
        logger.info("BacktestWorker received signal %s — stopping after current run", signum)
        self.running = False

    def _claim(self):
        conn = get_db_connection()
        try:
            conn.autocommit = True
            return claim_next_queued_run(conn)
        finally:
            close_db_connection(conn)

    def _recover_stale(self) -> None:
        conn = get_db_connection()
        try:
            conn.autocommit = True
            n = requeue_stale_runs(
                conn, older_than_minutes=config.BACKTEST_WORKER_STALE_MINUTES
            )
            if n:
                logger.warning("BacktestWorker requeued %d stale run(s)", n)
        except Exception:  # noqa: BLE001 - recovery is best-effort
            logger.warning("BacktestWorker stale-run recovery failed", exc_info=True)
        finally:
            close_db_connection(conn)

    def run(self) -> None:
        self.running = True
        logger.info("BacktestWorker started (poll=%.1fs)", self.poll_interval)
        if not config.BACKTEST_WORKER_ENABLED:
            logger.warning(
                "BACKTEST_WORKER_ENABLED is not set — the API is ALSO executing runs "
                "in-process, so this worker may double-process. Set "
                "BACKTEST_WORKER_ENABLED=1 to make the worker the sole executor."
            )
        self._recover_stale()
        while self.running:
            try:
                run_id = self._claim()
            except Exception:  # noqa: BLE001 - never let a transient DB error kill the worker
                logger.warning("BacktestWorker claim failed; backing off", exc_info=True)
                time.sleep(self.poll_interval)
                continue
            if run_id is None:
                time.sleep(self.poll_interval)
                continue
            logger.info("BacktestWorker executing run %s", run_id)
            execute_run(run_id)  # opens its own connection; captures its own errors
        logger.info("BacktestWorker stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    BacktestWorker().run()


if __name__ == "__main__":
    main()
