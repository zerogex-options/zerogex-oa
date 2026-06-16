"""Self-serve developer-portal API.

These endpoints back the Next.js "Developers" UI. The website BFF calls
them on behalf of a logged-in SaaS user, passing:

* its own bearer key (a ``TIER_FULL`` / wildcard key holding the
  ``dev_portal`` scope), which authenticates the caller; and
* an ``X-End-User-Token`` JWT whose ``sub`` is the SaaS user's id, which
  identifies *which developer* the operation is for.

The end-user-token's ``sub`` becomes the ``api_keys.user_id`` for every
key minted through this surface. That couples a B2B key 1:1 to a SaaS
account so the portal's list/create/rotate/revoke operations naturally
scope to "the keys the logged-in user owns" — there is no separate
permission model, the JWT *is* the ownership claim.

Security posture:

* **DEV_PORTAL scope** — gates the whole router. A regular ``analytics``
  key does not carry it and cannot reach these endpoints even with a
  hand-forged end-user token; only the website BFF can.
* **End-user-token required** — even with the right scope, an absent or
  invalid token returns ``400`` rather than acting on no user. We never
  fall back to the caller_user_id for ownership, because the caller is
  the BFF, not the developer.
* **Per-developer key cap** — bounded by env (``DEV_PORTAL_MAX_KEYS``)
  so a misbehaving frontend can't fill ``api_keys`` for one user.

Returns the raw key string exactly once on creation/rotation, mirroring
the ``admin_keys`` CLI semantics.
"""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Path, Query, Request, status
from pydantic import BaseModel, Field, field_validator

from ..database import DatabaseManager
from ..scopes import TIERS, expand_tier

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dev", tags=["Developer Portal"])

_PREFIX_LEN = 8
_MAX_KEYS_PER_USER = int(os.getenv("DEV_PORTAL_MAX_KEYS", "5") or "5")

# Sellable tiers via the self-serve portal. ``full`` is internal-only — it
# carries MARKET_RAW and DEV_PORTAL, which we never want to vend through a
# self-serve UI. ``signals`` is the higher-priced add-on. ``analytics`` is
# the default product.
_SELLABLE_TIERS = ("analytics", "signals")


def _hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _require_end_user(request: Request) -> str:
    """Return the end-user id from the verified JWT, or 400.

    A 400 is the right code here: this is a contract violation by the
    BFF (missing/invalid token), not an authorization failure by the
    end-user. The auth layer already handled identity authentication.
    """
    identity = getattr(request.state, "identity", None)
    end_user_id = getattr(identity, "end_user_id", None) if identity else None
    if not end_user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing or invalid X-End-User-Token; dev portal "
            "endpoints require an end-user attribution token.",
        )
    return end_user_id


def _db(request: Request) -> DatabaseManager:
    """Resolve the live ``DatabaseManager`` from the parent app module."""
    from .. import main as api_main

    return api_main._db()


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class KeyInfo(BaseModel):
    """Public view of an ``api_keys`` row — never includes the raw secret."""

    id: int
    name: str
    prefix: str = Field(..., description="First 8 chars of the raw key for visual ID.")
    tier: Optional[str] = Field(
        None,
        description="Best-guess tier name inferred from the scope set, or "
        "``null`` for keys that don't match any standard bundle.",
    )
    scopes: List[str]
    created_at: datetime
    last_used_at: Optional[datetime]
    revoked_at: Optional[datetime]
    active: bool


class KeyCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    tier: str = Field(
        "analytics",
        description="Tier bundle to grant. One of: " + ", ".join(_SELLABLE_TIERS),
    )

    @field_validator("name")
    @classmethod
    def _strip_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("name cannot be blank")
        return value

    @field_validator("tier")
    @classmethod
    def _check_tier(cls, value: str) -> str:
        if value not in _SELLABLE_TIERS:
            raise ValueError(
                "tier must be one of " + ", ".join(_SELLABLE_TIERS) + f"; got {value!r}"
            )
        return value


class KeyCreateResponse(BaseModel):
    key: KeyInfo
    raw_key: str = Field(
        ...,
        description="The raw API key. Returned **exactly once** — store it "
        "now; the server keeps only the SHA-256 hash.",
    )


class KeyListResponse(BaseModel):
    keys: List[KeyInfo]
    limit_per_user: int = Field(
        ..., description="Hard cap on simultaneously-active keys per developer."
    )


class UsageDayPoint(BaseModel):
    day: date
    request_count: int
    error_count: int


class UsageResponse(BaseModel):
    points: List[UsageDayPoint]
    total_requests: int
    total_errors: int
    window_days: int


class UsageSummaryResponse(BaseModel):
    current_month_requests: int
    last_month_requests: int
    last_30_days_requests: int
    last_seen_at: Optional[datetime]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _infer_tier(scopes: List[str]) -> Optional[str]:
    """Reverse the tier->scopes map; return the tier name with the exact set."""
    if not scopes:
        return None
    if "*" in scopes:
        return "full"
    scope_set = frozenset(scopes)
    for tier_name, tier_scopes in TIERS.items():
        if scope_set == tier_scopes:
            return tier_name
    return None


def _row_to_keyinfo(row: Dict[str, Any]) -> KeyInfo:
    scopes = list(row.get("scopes") or [])
    return KeyInfo(
        id=row["id"],
        name=row["name"],
        prefix=row["prefix"],
        tier=_infer_tier(scopes),
        scopes=scopes,
        created_at=row["created_at"],
        last_used_at=row.get("last_used_at"),
        revoked_at=row.get("revoked_at"),
        active=row.get("revoked_at") is None,
    )


async def _count_active_keys(pool: Any, user_id: str) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS n FROM api_keys " "WHERE user_id = $1 AND revoked_at IS NULL",
            user_id,
        )
    return int(row["n"])


async def _fetch_owned_key(pool: Any, user_id: str, key_id: int) -> Optional[Dict[str, Any]]:
    """Fetch a key by id only if it belongs to ``user_id``.

    Two-column WHERE — never trust a key_id from the request without
    also verifying ownership against the JWT-derived user.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, user_id, name, prefix, scopes,
                   created_at, last_used_at, revoked_at
            FROM api_keys
            WHERE id = $1 AND user_id = $2
            """,
            key_id,
            user_id,
        )
    return dict(row) if row else None


async def _list_keys(pool: Any, user_id: str) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, user_id, name, prefix, scopes,
                   created_at, last_used_at, revoked_at
            FROM api_keys
            WHERE user_id = $1
            ORDER BY id DESC
            """,
            user_id,
        )
    return [dict(r) for r in rows]


async def _insert_key(pool: Any, user_id: str, name: str, tier: str) -> Dict[str, Any]:
    raw = secrets.token_urlsafe(32)
    prefix = raw[:_PREFIX_LEN]
    key_hash = _hash_key(raw)
    scopes = expand_tier(tier)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO api_keys (user_id, name, key_hash, prefix, scopes)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, user_id, name, prefix, scopes,
                      created_at, last_used_at, revoked_at
            """,
            user_id,
            name,
            key_hash,
            prefix,
            scopes,
        )
    # The new raw secret is carried out of band — never logged or
    # persisted in any form other than the SHA-256 ``key_hash`` above.
    return {"row": dict(row), "raw": raw}


async def _revoke_key(pool: Any, user_id: str, key_id: int) -> bool:
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE api_keys
            SET revoked_at = NOW()
            WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL
            """,
            key_id,
            user_id,
        )
    return result != "UPDATE 0"


def _invalidate_caches() -> None:
    """Drop the security key-store cache so a freshly-revoked key 401's
    on the next request instead of riding out its existing cache entry."""
    try:
        from .. import security

        security.key_store.invalidate()
    except Exception:
        # Cache invalidation must not break the mutation path.
        logger.debug("key_store invalidate failed", exc_info=True)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/keys", response_model=KeyListResponse)
async def list_keys(request: Request) -> KeyListResponse:
    """List every ``api_keys`` row owned by the calling developer.

    Revoked keys are returned alongside active ones (with ``active=False``
    and a ``revoked_at`` timestamp) so the UI can render an audit-style
    history. The raw secret is never included — keys are stored as a
    SHA-256 hash and the original is shown only once at creation.
    """
    user_id = _require_end_user(request)
    pool = _db(request).pool
    rows = await _list_keys(pool, user_id)
    return KeyListResponse(
        keys=[_row_to_keyinfo(r) for r in rows],
        limit_per_user=_MAX_KEYS_PER_USER,
    )


@router.post(
    "/keys",
    response_model=KeyCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_key(payload: KeyCreateRequest, request: Request) -> KeyCreateResponse:
    """Issue a new API key for the calling developer.

    Returns the raw key string **exactly once** — the server keeps only
    the SHA-256 hash. The caller is expected to render it once, advise
    the user to copy it, and never persist it on the client.

    Capped at ``DEV_PORTAL_MAX_KEYS`` active keys per developer (default
    5) so a buggy frontend can't fill the table.
    """
    user_id = _require_end_user(request)
    pool = _db(request).pool

    active = await _count_active_keys(pool, user_id)
    if active >= _MAX_KEYS_PER_USER:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Active key limit reached ({active}/{_MAX_KEYS_PER_USER}). "
                "Revoke an unused key before issuing a new one."
            ),
        )

    created = await _insert_key(pool, user_id, payload.name, payload.tier)
    _invalidate_caches()
    logger.info(
        "dev_portal key created user=%s tier=%s id=%s",
        user_id,
        payload.tier,
        created["row"]["id"],
    )
    return KeyCreateResponse(
        key=_row_to_keyinfo(created["row"]),
        raw_key=created["raw"],
    )


@router.post(
    "/keys/{key_id}/rotate",
    response_model=KeyCreateResponse,
)
async def rotate_key(
    request: Request,
    key_id: int = Path(..., ge=1),
) -> KeyCreateResponse:
    """Rotate a key: mint a new one carrying the same scopes, revoke the
    old one — all in a single transaction so the customer never has zero
    valid keys mid-rotation. The new raw key is returned exactly once.

    The new key's ``name`` carries a ``(rotated)`` suffix so the UI can
    surface the lineage without breaking older clients that may still be
    referencing the previous key by id.
    """
    user_id = _require_end_user(request)
    pool = _db(request).pool
    existing = await _fetch_owned_key(pool, user_id, key_id)
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No such key for this developer.",
        )
    if existing.get("revoked_at") is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot rotate a key that has already been revoked. " "Issue a new key instead.",
        )

    raw = secrets.token_urlsafe(32)
    prefix = raw[:_PREFIX_LEN]
    key_hash = _hash_key(raw)
    scopes = list(existing.get("scopes") or [])
    new_name = f"{existing['name']} (rotated)"[:128]

    async with pool.acquire() as conn:
        async with conn.transaction():
            new_row = await conn.fetchrow(
                """
                INSERT INTO api_keys (user_id, name, key_hash, prefix, scopes)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING id, user_id, name, prefix, scopes,
                          created_at, last_used_at, revoked_at
                """,
                user_id,
                new_name,
                key_hash,
                prefix,
                scopes,
            )
            await conn.execute(
                "UPDATE api_keys SET revoked_at = NOW() WHERE id = $1",
                key_id,
            )

    _invalidate_caches()
    logger.info(
        "dev_portal key rotated user=%s old_id=%s new_id=%s",
        user_id,
        key_id,
        new_row["id"],
    )
    return KeyCreateResponse(
        key=_row_to_keyinfo(dict(new_row)),
        raw_key=raw,
    )


@router.delete(
    "/keys/{key_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def revoke_key(
    request: Request,
    key_id: int = Path(..., ge=1),
) -> None:
    """Revoke one of the calling developer's keys.

    Returns 204 on success and 404 if the key doesn't exist *for this
    developer* — the two-column WHERE means a guess at someone else's
    key id never reveals whether it exists.
    """
    user_id = _require_end_user(request)
    pool = _db(request).pool
    revoked = await _revoke_key(pool, user_id, key_id)
    if not revoked:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No such active key for this developer.",
        )
    _invalidate_caches()
    logger.info("dev_portal key revoked user=%s id=%s", user_id, key_id)


@router.get("/usage", response_model=UsageResponse)
async def usage_history(
    request: Request,
    days: int = Query(30, ge=1, le=180),
) -> UsageResponse:
    """Daily request/error counts for the calling developer over ``days``.

    Reads ``api_usage_daily`` which is the durable cross-worker source of
    truth populated by ``UsageMeter`` (off until ``API_USAGE_METERING_
    ENABLED=1``; before that, this endpoint returns zeros). Missing days
    are omitted rather than zero-filled — the UI is responsible for
    rendering gaps.
    """
    user_id = _require_end_user(request)
    pool = _db(request).pool
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=days - 1)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT day::date AS day,
                   SUM(request_count)::BIGINT AS request_count,
                   SUM(error_count)::BIGINT AS error_count
            FROM api_usage_daily
            WHERE caller_user_id = $1 AND day >= $2 AND day <= $3
            GROUP BY day
            ORDER BY day
            """,
            user_id,
            start,
            today,
        )
    points = [
        UsageDayPoint(
            day=r["day"],
            request_count=int(r["request_count"] or 0),
            error_count=int(r["error_count"] or 0),
        )
        for r in rows
    ]
    return UsageResponse(
        points=points,
        total_requests=sum(p.request_count for p in points),
        total_errors=sum(p.error_count for p in points),
        window_days=days,
    )


@router.get("/usage/summary", response_model=UsageSummaryResponse)
async def usage_summary(request: Request) -> UsageSummaryResponse:
    """Headline usage numbers for the developer dashboard card.

    Same data source as :func:`usage_history`; this just packages three
    common roll-ups (current month, last month, last 30 days) into one
    round trip so the overview card is a single query.
    """
    user_id = _require_end_user(request)
    pool = _db(request).pool
    today = datetime.now(timezone.utc).date()
    first_of_month = today.replace(day=1)
    # First-of-last-month = (first_of_month - 1 day) snapped to day 1.
    last_month_end = first_of_month - timedelta(days=1)
    first_of_last_month = last_month_end.replace(day=1)
    last_30_start = today - timedelta(days=29)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                COALESCE(SUM(
                    CASE WHEN day >= $2 THEN request_count END
                ), 0)::BIGINT AS current_month,
                COALESCE(SUM(
                    CASE WHEN day >= $3 AND day <= $4 THEN request_count END
                ), 0)::BIGINT AS last_month,
                COALESCE(SUM(
                    CASE WHEN day >= $5 THEN request_count END
                ), 0)::BIGINT AS last_30,
                MAX(last_seen_at) AS last_seen_at
            FROM api_usage_daily
            WHERE caller_user_id = $1
              AND day >= $3
            """,
            user_id,
            first_of_month,
            first_of_last_month,
            last_month_end,
            last_30_start,
        )

    return UsageSummaryResponse(
        current_month_requests=int(row["current_month"] or 0),
        last_month_requests=int(row["last_month"] or 0),
        last_30_days_requests=int(row["last_30"] or 0),
        last_seen_at=row["last_seen_at"],
    )
