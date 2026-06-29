"""Saved & shareable backtest configurations (Phase 6).

A "config" is a named, validated :class:`BacktestSpec` the user can reload into
the form or share. Reads and writes are synchronous psycopg2 (like
``queries.py`` / ``runner.py``) and the async router dispatches them through
``asyncio.to_thread``.

Ownership mirrors ``backtest_runs``: a config owned by an end-user is only
listable / mutable by that same end-user; an anonymous config (NULL
``end_user``) belongs to the anonymous pool. Sharing is orthogonal to
ownership — anyone holding a config's ``share_token`` can load its spec
read-only via :func:`get_shared_config`, but cannot enumerate or mutate the
owner's other configs.
"""

from __future__ import annotations

import json
import secrets
from typing import Optional

from src.database.connection import close_db_connection, db_connection, get_db_connection


def _new_share_token() -> str:
    """A short, URL-safe, unguessable token for shareable config links."""
    return secrets.token_urlsafe(16)[:22]


def _row_to_summary(row) -> dict:
    return {
        "id": int(row[0]),
        "name": row[1],
        "underlying": row[2],
        "share_token": row[3],
        "created_at": row[4].isoformat() if row[4] else None,
        "updated_at": row[5].isoformat() if row[5] else None,
    }


_SUMMARY_COLS = "id, name, underlying, share_token, created_at, updated_at"


def save_config(spec_dict: dict, *, name: str, underlying: str,
                end_user: Optional[str]) -> dict:
    """Insert a new saved config and return its summary (incl. share_token)."""
    conn = get_db_connection()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            f"""
            INSERT INTO backtest_configs (end_user, name, underlying, spec, share_token)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING {_SUMMARY_COLS}
            """,
            (end_user, name, underlying, json.dumps(spec_dict), _new_share_token()),
        )
        return _row_to_summary(cur.fetchone())
    finally:
        close_db_connection(conn)


def list_configs(*, end_user: Optional[str], limit: int = 100) -> list[dict]:
    """Saved configs for this end-user (or the anonymous pool when unauthenticated)."""
    with db_connection() as conn:
        cur = conn.cursor()
        if end_user:
            cur.execute(
                f"SELECT {_SUMMARY_COLS} FROM backtest_configs "
                "WHERE end_user = %s ORDER BY updated_at DESC LIMIT %s",
                (end_user, limit),
            )
        else:
            cur.execute(
                f"SELECT {_SUMMARY_COLS} FROM backtest_configs "
                "WHERE end_user IS NULL ORDER BY updated_at DESC LIMIT %s",
                (limit,),
            )
        return [_row_to_summary(r) for r in cur.fetchall()]


def get_config(config_id: int, *, end_user: Optional[str]) -> Optional[dict]:
    """Fetch one config (incl. spec), scoped to its owner. None if absent/foreign."""
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT {_SUMMARY_COLS}, spec, end_user FROM backtest_configs WHERE id = %s",
            (config_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        owner = row[7]
        if owner is not None and owner != end_user:
            return None
        out = _row_to_summary(row)
        out["spec"] = row[6]
        return out


def update_config(config_id: int, *, end_user: Optional[str],
                  name: Optional[str] = None,
                  spec_dict: Optional[dict] = None,
                  underlying: Optional[str] = None) -> Optional[dict]:
    """Rename and/or replace a config's spec (owner only). None if absent/foreign."""
    conn = get_db_connection()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT end_user FROM backtest_configs WHERE id = %s", (config_id,))
        row = cur.fetchone()
        if row is None:
            return None
        owner = row[0]
        if owner is not None and owner != end_user:
            return None

        sets, params = [], []
        if name is not None:
            sets.append("name = %s")
            params.append(name)
        if underlying is not None:
            sets.append("underlying = %s")
            params.append(underlying)
        if spec_dict is not None:
            sets.append("spec = %s")
            params.append(json.dumps(spec_dict))
        sets.append("updated_at = NOW()")
        params.append(config_id)
        cur.execute(
            f"UPDATE backtest_configs SET {', '.join(sets)} WHERE id = %s "
            f"RETURNING {_SUMMARY_COLS}",
            params,
        )
        return _row_to_summary(cur.fetchone())
    finally:
        close_db_connection(conn)


def delete_config(config_id: int, *, end_user: Optional[str]) -> bool:
    """Delete a config (owner only). Returns True if a row was removed."""
    conn = get_db_connection()
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT end_user FROM backtest_configs WHERE id = %s", (config_id,))
        row = cur.fetchone()
        if row is None:
            return False
        owner = row[0]
        if owner is not None and owner != end_user:
            return False
        cur.execute("DELETE FROM backtest_configs WHERE id = %s", (config_id,))
        return True
    finally:
        close_db_connection(conn)


def get_shared_config(share_token: str) -> Optional[dict]:
    """Public, read-only fetch by share token: returns ``{name, underlying, spec}``.

    No owner scoping — possession of the token is the authorization. Only the
    fields needed to clone the config into a fresh form are returned.
    """
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT name, underlying, spec FROM backtest_configs WHERE share_token = %s",
            (share_token,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return {"name": row[0], "underlying": row[1], "spec": row[2]}
