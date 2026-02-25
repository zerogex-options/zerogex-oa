"""
Database connection management for PostgreSQL
"""

import os
import psycopg2
from psycopg2 import pool
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
    db_name = os.getenv('DB_NAME', 'zerogexdb')
    db_user = os.getenv('DB_USER', 'postgres')
    min_connections = int(os.getenv('DB_POOL_MIN', '1'))
    max_connections = int(os.getenv('DB_POOL_MAX', '10'))

    # Get password from configured provider
    # Note: For .pgpass, this returns None (psycopg2 reads .pgpass automatically)
    try:
        db_password = get_db_password()
    except Exception as e:
        logger.error(f"Failed to retrieve database password: {e}")
        raise

    logger.info(f"Connecting to PostgreSQL: {db_user}@{db_host}:{db_port}/{db_name}")

    try:
        # Build connection parameters
        conn_params = {
            'minconn': min_connections,
            'maxconn': max_connections,
            'host': db_host,
            'port': db_port,
            'database': db_name,
            'user': db_user,
        }

        # Only add password if it's not None (i.e., not using .pgpass)
        if db_password is not None:
            conn_params['password'] = db_password

        _connection_pool = pool.SimpleConnectionPool(**conn_params)

        # Test the connection
        test_conn = _connection_pool.getconn()
        cursor = test_conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info(f"✅ Connected to PostgreSQL: {version[0][:50]}...")
        cursor.close()
        _connection_pool.putconn(test_conn)

    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to database: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error initializing connection pool: {e}", exc_info=True)
        raise


def close_connection_pool():
    """Close all connections in the pool"""
    global _connection_pool

    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("Closed database connection pool")
