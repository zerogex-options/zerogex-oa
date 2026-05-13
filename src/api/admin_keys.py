"""Admin CLI for managing per-user API keys.

The CLI talks directly to PostgreSQL using the same credential resolution
the API server uses (``~/.pgpass`` first, then ``DB_*`` env vars).  Keys
are issued as ``secrets.token_urlsafe(32)`` (~43 chars, ~256 bits) and
stored as SHA-256 hashes; the raw secret is printed exactly once.

Usage
-----
::

    # Create a key for a user.  The raw secret is printed once — copy it now.
    python -m src.api.admin_keys create alice@example.com --name "alice-laptop"

    # List all keys (or filter to one user, or hide revoked ones).
    python -m src.api.admin_keys list
    python -m src.api.admin_keys list --user-id alice@example.com
    python -m src.api.admin_keys list --active

    # Revoke a key by its numeric id (active keys only).
    python -m src.api.admin_keys revoke 7
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import secrets
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import asyncpg

_PREFIX_LEN = 8


def _hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _load_db_credentials() -> Dict[str, Any]:
    """Mirror DatabaseManager._load_credentials: .pgpass first, env-vars next."""
    pgpass = Path.home() / ".pgpass"
    if pgpass.exists():
        with open(pgpass) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(":")
                if len(parts) >= 5:
                    return {
                        "host": parts[0],
                        "port": int(parts[1]),
                        "database": parts[2],
                        "user": parts[3],
                        "password": parts[4],
                    }
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "database": os.getenv("DB_NAME", "zerogex"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
    }


async def _connect() -> asyncpg.Connection:
    creds = _load_db_credentials()
    ssl_mode = os.getenv("DB_SSLMODE", "").strip().lower()
    ssl = True if ssl_mode in {"require", "verify-ca", "verify-full"} else None
    return await asyncpg.connect(ssl=ssl, **creds)


async def _create(user_id: str, name: str, scopes: Optional[List[str]]) -> int:
    raw = secrets.token_urlsafe(32)
    prefix = raw[:_PREFIX_LEN]
    key_hash = _hash_key(raw)
    conn = await _connect()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO api_keys (user_id, name, key_hash, prefix, scopes)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, created_at
            """,
            user_id,
            name,
            key_hash,
            prefix,
            scopes or [],
        )
    finally:
        await conn.close()

    print(f"Created key id={row['id']} user_id={user_id} name={name!r} prefix={prefix}")
    print()
    print("API key (shown once — copy it now):")
    print(f"  {raw}")
    print()
    print("Send the key in either header on every request:")
    print(f"  Authorization: Bearer {raw}")
    return 0


async def _list(user_id: Optional[str], active_only: bool) -> int:
    filters: List[str] = []
    params: List[Any] = []
    if user_id:
        params.append(user_id)
        filters.append(f"user_id = ${len(params)}")
    if active_only:
        filters.append("revoked_at IS NULL")
    where = f"WHERE {' AND '.join(filters)}" if filters else ""

    conn = await _connect()
    try:
        rows = await conn.fetch(
            f"""
            SELECT id, user_id, name, prefix, scopes,
                   created_at, last_used_at, revoked_at
            FROM api_keys
            {where}
            ORDER BY id
            """,
            *params,
        )
    finally:
        await conn.close()

    if not rows:
        print("(no keys)")
        return 0

    def _fmt_utc(dt: Optional[datetime]) -> str:
        if dt is None:
            return "-"
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    headers = (
        "id",
        "user_id",
        "name",
        "prefix",
        "state",
        "created_at (UTC)",
        "last_used_at (UTC)",
    )
    aligns = (">", "<", "<", "<", "<", "<", "<")
    cells = [
        (
            str(r["id"]),
            r["user_id"],
            r["name"],
            r["prefix"],
            "revoked" if r["revoked_at"] else "active",
            _fmt_utc(r["created_at"]),
            _fmt_utc(r["last_used_at"]),
        )
        for r in rows
    ]
    widths = [max(len(h), *(len(row[i]) for row in cells)) for i, h in enumerate(headers)]
    fmt = "  ".join(f"{{:{a}{w}}}" for a, w in zip(aligns, widths))
    total_width = sum(widths) + 2 * (len(widths) - 1)
    print(fmt.format(*headers).rstrip())
    print("-" * total_width)
    for row in cells:
        print(fmt.format(*row).rstrip())
    return 0


async def _revoke(key_id: int) -> int:
    conn = await _connect()
    try:
        result = await conn.execute(
            """
            UPDATE api_keys
            SET revoked_at = NOW()
            WHERE id = $1 AND revoked_at IS NULL
            """,
            key_id,
        )
    finally:
        await conn.close()

    if result == "UPDATE 0":
        print(
            f"No active key with id={key_id} (already revoked or doesn't exist)",
            file=sys.stderr,
        )
        return 1
    print(f"Revoked key id={key_id}")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="admin_keys",
        description="Manage per-user API keys for the ZeroGEX API.",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_create = sub.add_parser("create", help="Issue a new API key for a user")
    p_create.add_argument(
        "user_id",
        help="User identifier (free-form: email, username, integration name)",
    )
    p_create.add_argument(
        "--name",
        required=True,
        help="Friendly label for the key (e.g. 'alice-laptop', 'ci-bot')",
    )
    p_create.add_argument(
        "--scope",
        action="append",
        default=None,
        dest="scopes",
        # Stored in api_keys.scopes (text[]); not enforced anywhere in the
        # request path today. Useful as an audit record of intended scope.
        # Real enforcement would require a per-endpoint "required scopes"
        # declaration and a check in api_key_auth after the DB lookup.
        help="Optional scope tag, repeat for multiple. Advisory only — stored but not enforced.",
    )

    p_list = sub.add_parser("list", help="List API keys")
    p_list.add_argument("--user-id", default=None, help="Filter by user_id")
    p_list.add_argument(
        "--active",
        action="store_true",
        help="Show only active keys (omit revoked)",
    )

    p_revoke = sub.add_parser("revoke", help="Revoke an API key by id")
    p_revoke.add_argument("key_id", type=int)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "create":
        return asyncio.run(_create(args.user_id, args.name, args.scopes))
    if args.cmd == "list":
        return asyncio.run(_list(args.user_id, args.active))
    if args.cmd == "revoke":
        return asyncio.run(_revoke(args.key_id))
    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
