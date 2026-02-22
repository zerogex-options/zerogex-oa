"""
Database utilities for ZeroGEX platform

Components:
- Connection management with connection pooling
- Password providers (AWS Secrets Manager, environment variables)
- Context managers for safe database operations

Usage:
    from src.database import db_connection
    
    # Context manager (recommended)
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM underlying_quotes LIMIT 1")
        result = cursor.fetchone()
    
    # Manual connection management
    conn = get_db_connection()
    try:
        # ... use connection
    finally:
        close_db_connection(conn)
"""

from src.database.connection import (
    get_db_connection,
    close_db_connection,
    db_connection,
    close_connection_pool,
)
from src.database.password_providers import get_db_password

__all__ = [
    "get_db_connection",
    "close_db_connection",
    "db_connection",
    "close_connection_pool",
    "get_db_password",
]
