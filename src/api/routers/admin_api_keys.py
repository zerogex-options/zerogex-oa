"""Admin HTTP endpoints for per-user API key lifecycle.

These back the website's self-service "Generate API Key" button. The
website BFF (never a browser directly) calls them server-to-server with
its bearer key *and* the ``X-Admin-Token`` shared secret; see
``security.require_admin`` for the gate. The whole router sits behind that
gate, so every route needs the admin token in addition to the global
API-key auth.

Endpoints (prefix ``/api/admin/api-keys``):

* ``POST /provision``   — mint a key for a user (optionally revoking their
  existing active keys first). Returns the raw secret exactly once.
* ``POST /revoke-all``  — revoke every active key for a user. Used both by
  "regenerate" and by automatic deprovisioning when a member drops below
  the Pro tier.
* ``GET  ``             — list a user's keys (metadata only, never secrets).

All mutations invalidate the in-process ``key_store`` cache so a revoked
key stops authenticating immediately rather than lingering for the cache
TTL.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from src.api import key_provisioning as kp
from src.api.database import DatabaseManager
from src.api.scopes import TIERS, expand_tier
from src.api.security import key_store, require_admin

logger = logging.getLogger(__name__)

# The whole router is admin-gated. require_admin fails closed (503) when
# KEY_ADMIN_TOKEN is unset and 403s on a missing/incorrect X-Admin-Token.
router = APIRouter(
    prefix="/api/admin/api-keys",
    tags=["Admin"],
    dependencies=[Depends(require_admin)],
)


def get_db() -> DatabaseManager:
    # Import lazily (mirrors the other routers) so the module imports cleanly
    # before the lifespan has constructed db_manager.
    from src.api.main import db_manager

    if db_manager is None:
        raise HTTPException(status_code=503, detail="Database not initialized")
    return db_manager


def _merge_scopes(tier: Optional[str], scopes: Optional[List[str]]) -> List[str]:
    """Merge an optional tier bundle with explicit scopes (like the CLI).

    Raises 400 on an unknown tier so a typo fails loudly instead of minting
    a key with no scopes.
    """
    merged: List[str] = list(scopes or [])
    if tier:
        if tier not in TIERS:
            raise HTTPException(
                status_code=400,
                detail=f"unknown tier {tier!r}; known tiers: {sorted(TIERS)}",
            )
        merged.extend(expand_tier(tier))
    return sorted(set(merged))


class ProvisionRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=128)
    # Base label for the key; the smallest free "<base>", "<base>1", … is
    # chosen server-side against the user's key history.
    base_name: str = Field(..., min_length=1, max_length=128)
    # Optional forward-looking scope grant (scope enforcement is off today).
    tier: Optional[str] = None
    scopes: Optional[List[str]] = None
    # Default true → at most one active key per user ("regenerate" semantics).
    revoke_existing: bool = True


class RevokeAllRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=128)


@router.post("/provision")
async def provision(
    body: ProvisionRequest, db: DatabaseManager = Depends(get_db)
) -> Dict[str, Any]:
    scopes = _merge_scopes(body.tier, body.scopes)
    user_id = body.user_id.strip()
    async with db.pool.acquire() as conn:
        # Transaction so revoke + name scan + insert are atomic.
        async with conn.transaction():
            result = await kp.provision_user_key(
                conn,
                user_id=user_id,
                base_name=body.base_name.strip(),
                scopes=scopes,
                revoke_existing=body.revoke_existing,
            )
    # A just-revoked prior key must stop authenticating at once.
    key_store.invalidate()
    logger.info(
        "admin: provisioned key id=%s user_id=%s name=%s (revoked_previous=%s)",
        result["id"],
        user_id,
        result["name"],
        result["revoked_previous"],
    )
    return result


@router.post("/revoke-all")
async def revoke_all(
    body: RevokeAllRequest, db: DatabaseManager = Depends(get_db)
) -> Dict[str, Any]:
    user_id = body.user_id.strip()
    async with db.pool.acquire() as conn:
        revoked = await kp.revoke_all_for_user(conn, user_id=user_id)
    key_store.invalidate()
    logger.info("admin: revoked %s active key(s) for user_id=%s", revoked, user_id)
    return {"user_id": user_id, "revoked": revoked}


@router.get("")
async def list_keys(
    user_id: str = Query(..., min_length=1, max_length=128),
    active_only: bool = Query(False),
    db: DatabaseManager = Depends(get_db),
) -> Dict[str, Any]:
    uid = user_id.strip()
    async with db.pool.acquire() as conn:
        keys = await kp.list_user_keys(conn, user_id=uid, active_only=active_only)
    return {"user_id": uid, "keys": keys}
