"""
Database connection management for PostgreSQL
"""

import os
import psycopg2
from psycopg2 import pool
from psycopg2 import extensions
import time
from typing import Optional
from contextlib import contextmanager
from src.database.password_providers import get_db_password
from src.utils import get_logger

logger = get_logger(__name__)

# Connection pool
_connection_pool: Optional[pool.SimpleConnectionPool] = None


def get_db_connection():
    """
    Get a database connection from the pool

    Returns:
        psycopg2 connection object

    Raises:
        Exception: If connection cannot be established
    """
    global _connection_pool

    # Initialize pool if not already done
    if _connection_pool is None:
        _initialize_connection_pool()

    try:
        conn = _connection_pool.getconn()
        logger.debug("Retrieved connection from pool")
        return conn
    except Exception as e:
        logger.error(f"Failed to get connection from pool: {e}")
        raise


def close_db_connection(conn):
    """
    Return a connection to the pool

    Args:
        conn: psycopg2 connection object
    """
    global _connection_pool

    if _connection_pool and conn:
        try:
            tx_status = conn.get_transaction_status()
            if tx_status != extensions.TRANSACTION_STATUS_IDLE:
                # Ensure we never return a connection "idle in transaction"
                # to the pool (this can hold locks and bloat table churn).
                conn.rollback()
                logger.warning("Rolled back open transaction before returning DB connection")
        except Exception:
            logger.warning("Failed to inspect/reset transaction state before pool return", exc_info=True)
        _connection_pool.putconn(conn)
        logger.debug("Returned connection to pool")


@contextmanager
def db_connection():
    """
    Context manager for database connections

    Usage:
        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
    """
    conn = None
    try:
        conn = get_db_connection()
        yield conn
    except Exception:
        if conn:
            try:
                conn.rollback()
            except Exception:
                logger.warning("Failed to rollback DB connection after error", exc_info=True)
        raise
    finally:
        if conn:
            close_db_connection(conn)


def _initialize_connection_pool():
    """Initialize the database connection pool"""
    global _connection_pool

    logger.info("Initializing database connection pool...")

    # Get database configuration from environment
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = int(os.getenv('DB_PORT', '5432'))
    db_name = os.getenv('DB_NAME', 'zerogex')
    db_user = os.getenv('DB_USER', 'postgres')
    # Conservative defaults prevent "too many clients" when multiple engines
    # and API workers are running concurrently against the same DB.
    min_connections = int(os.getenv('DB_POOL_MIN', '1'))
    max_connections = int(os.getenv('DB_POOL_MAX', '2'))
    if min_connections > max_connections:
        min_connections = max_connections
    connect_timeout = int(os.getenv('DB_CONNECT_TIMEOUT_SECONDS', '20'))
    connect_retries = int(os.getenv('DB_CONNECT_RETRIES', '5'))
    retry_base_delay = float(os.getenv('DB_CONNECT_RETRY_DELAY_SECONDS', '1.5'))
    sslmode = os.getenv('DB_SSLMODE', '').strip()

    # Get password from configured provider
    # Note: For .pgpass, this returns None (psycopg2 reads .pgpass automatically)
    try:
        db_password = get_db_password()
    except Exception as e:
        logger.error(f"Failed to retrieve database password: {e}")
        raise

    logger.info(
        "Connecting to PostgreSQL: %s@%s:%s/%s (pool min=%d, max=%d)",
        db_user,
        db_host,
        db_port,
        db_name,
        min_connections,
        max_connections,
    )

    # Build connection parameters
    conn_params = {
        'minconn': min_connections,
        'maxconn': max_connections,
        'host': db_host,
        'port': db_port,
        'database': db_name,
        'user': db_user,
        'connect_timeout': connect_timeout,
        # Improve resilience to stale network links / RDS edge cases.
        'keepalives': 1,
        'keepalives_idle': int(os.getenv('DB_KEEPALIVES_IDLE_SECONDS', '30')),
        'keepalives_interval': int(os.getenv('DB_KEEPALIVES_INTERVAL_SECONDS', '10')),
        'keepalives_count': int(os.getenv('DB_KEEPALIVES_COUNT', '5')),
    }

    # Only add password if it's not None (i.e., not using .pgpass)
    if db_password is not None:
        conn_params['password'] = db_password
    if sslmode:
        conn_params['sslmode'] = sslmode

    last_error = None
    for attempt in range(1, connect_retries + 1):
        try:
            _connection_pool = pool.SimpleConnectionPool(**conn_params)

            # Test the connection
            test_conn = _connection_pool.getconn()
            cursor = test_conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"✅ Connected to PostgreSQL: {version[0][:50]}...")
            cursor.close()
            _connection_pool.putconn(test_conn)
            return

        except psycopg2.OperationalError as e:
            last_error = e
            logger.error(
                "Failed to connect to database (attempt %d/%d): %s",
                attempt,
                connect_retries,
                e,
            )
        except Exception as e:
            last_error = e
            logger.error(
                "Unexpected error initializing connection pool (attempt %d/%d): %s",
                attempt,
                connect_retries,
                e,
                exc_info=True,
            )

        if _connection_pool is not None:
            try:
                _connection_pool.closeall()
            except Exception:
                pass
            _connection_pool = None

        if attempt < connect_retries:
            delay = retry_base_delay * attempt
            logger.warning("Retrying database pool initialization in %.1fs...", delay)
            time.sleep(delay)

    raise last_error


def close_connection_pool():
    """Close all connections in the pool"""
    global _connection_pool

    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("Closed database connection pool")
