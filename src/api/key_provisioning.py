"""Pool-based per-user API key provisioning for the admin HTTP router.

This shares the on-disk key format with the ``admin_keys`` CLI — a
``secrets.token_urlsafe(32)`` raw secret, an 8-char clear ``prefix``, and a
SHA-256 hash stored in ``api_keys.key_hash`` — but operates over the live
application connection pool instead of opening its own connection.  That
lets the FastAPI ``/api/admin/api-keys`` endpoints mint, list, and revoke
keys in-process (and invalidate the auth cache) on behalf of the website
BFF's self-service "Generate API Key" button.

The raw secret is returned exactly once, by :func:`provision_user_key`.  It
is never stored and never re-derivable from the hash, mirroring the CLI's
"shown once" contract.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Reuse the canonical hash the auth path checks against (security.key_store
# looks keys up by this exact digest) so a key minted here authenticates.
from src.api.security import _hash_key

# Length of the clear-text prefix retained on each row so a key can be
# referenced in logs / the account UI without exposing the secret. Matches
# ``admin_keys._PREFIX_LEN``.
_PREFIX_LEN = 8


def _iso(dt: Optional[datetime]) -> Optional[str]:
    """UTC ISO-8601 string for a timestamptz, or ``None``."""
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat()


def _rows_affected(command_tag: Any) -> int:
    """Parse the trailing count out of an asyncpg command tag (``"UPDATE 3"``)."""
    if isinstance(command_tag, str):
        parts = command_tag.split()
        if parts:
            try:
                return int(parts[-1])
            except ValueError:
                return 0
    return 0


def next_key_name(base_name: str, existing: set) -> str:
    """Return ``base_name`` unless it collides, else ``base_name<N>``.

    The website derives ``base_name`` from the local-part of the user's
    email (everything before ``@``). If a key with that exact name already
    exists for the user — active or revoked — we tack on the smallest
    positive integer that is free (``alice`` → ``alice1`` → ``alice2`` …),
    matching the behaviour the product spec asked for.
    """
    base = base_name.strip() or "key"
    if base not in existing:
        return base
    suffix = 1
    while f"{base}{suffix}" in existing:
        suffix += 1
    return f"{base}{suffix}"


async def revoke_all_for_user(conn: Any, *, user_id: str) -> int:
    """Revoke every currently-active key owned by ``user_id``.

    Idempotent: revoking a user with no active keys is a no-op that returns
    ``0``. Used both for the "regenerate" flow (deprecate the old key before
    minting a new one) and for automatic deprovisioning when a member drops
    below the Pro tier.
    """
    tag = await conn.execute(
        """
        UPDATE api_keys
        SET revoked_at = NOW()
        WHERE user_id = $1 AND revoked_at IS NULL
        """,
        user_id,
    )
    return _rows_affected(tag)


async def provision_user_key(
    conn: Any,
    *,
    user_id: str,
    base_name: str,
    scopes: Optional[List[str]] = None,
    revoke_existing: bool = True,
) -> Dict[str, Any]:
    """Mint a fresh API key for ``user_id`` and return it (raw secret once).

    When ``revoke_existing`` is true (the default) any active keys for the
    user are revoked first, so a user has at most one active key — the model
    the account UI presents ("Generate" → "Regenerate"). The new key's
    ``name`` is derived from ``base_name`` via :func:`next_key_name` against
    the user's full key history so names never repeat.

    Callers should run this inside a transaction (the router does) so the
    revoke + name scan + insert are atomic.
    """
    revoked_previous = 0
    if revoke_existing:
        revoked_previous = await revoke_all_for_user(conn, user_id=user_id)

    existing_rows = await conn.fetch(
        "SELECT name FROM api_keys WHERE user_id = $1",
        user_id,
    )
    existing = {r["name"] for r in existing_rows}
    name = next_key_name(base_name, existing)

    stored_scopes = sorted(set(scopes)) if scopes else []
    raw = secrets.token_urlsafe(32)
    prefix = raw[:_PREFIX_LEN]
    key_hash = _hash_key(raw)

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
        stored_scopes,
    )

    return {
        "id": row["id"],
        "user_id": user_id,
        "name": name,
        "prefix": prefix,
        "scopes": stored_scopes,
        "created_at": _iso(row["created_at"]),
        "revoked_previous": revoked_previous,
        # The raw secret — returned exactly once, never persisted.
        "api_key": raw,
    }


async def list_user_keys(conn: Any, *, user_id: str, active_only: bool) -> List[Dict[str, Any]]:
    """Return key metadata for ``user_id`` (never the raw secret or hash)."""
    where = "WHERE user_id = $1"
    if active_only:
        where += " AND revoked_at IS NULL"
    rows = await conn.fetch(
        f"""
        SELECT id, user_id, name, prefix, scopes,
               created_at, last_used_at, revoked_at
        FROM api_keys
        {where}
        ORDER BY id
        """,
        user_id,
    )
    return [
        {
            "id": r["id"],
            "user_id": r["user_id"],
            "name": r["name"],
            "prefix": r["prefix"],
            "scopes": list(r["scopes"] or []),
            "created_at": _iso(r["created_at"]),
            "last_used_at": _iso(r["last_used_at"]),
            "revoked_at": _iso(r["revoked_at"]),
        }
        for r in rows
    ]
