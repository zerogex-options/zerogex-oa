"""End-user attribution for website-proxied requests.

The website's Next.js server authenticates to this API with a single
shared per-user key (``user_id=zerogex-web``) on behalf of every logged-in
human, so the caller identity alone cannot say *which* end-user a request
is for. This module adds an optional second factor: the website mints a
short-lived signed token naming the end-user and sends it in the
``X-End-User-Token`` header; here we verify it (pure crypto, shared
secret) and surface the end-user on a typed :class:`RequestIdentity`
used for audit logging and (later) rate limiting.

Design constraints (binding wire contract — see ``API_Guide.md``):

* The token is a standard JWT, ``alg=HS256``. Any other algorithm —
  including ``none`` — is rejected (algorithm-confusion guard).
* Verification is **pure crypto**: it never touches the DB and never
  raises into the request path.
* **Fail-open / purely additive**: no token, no secret configured, or
  any invalid/expired/forged token simply means "no end-user". A bad
  token must never turn a 200 into a 4xx/5xx.

Activation is a single env var on the API host: ``END_USER_TOKEN_SECRET``.
Unset ⇒ attribution disabled ⇒ callers authenticate exactly as before.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional, Tuple

from fastapi import Request

logger = logging.getLogger(__name__)

# Read at import time so a misconfigured secret can't be hot-swapped under
# a running worker and so the disabled-path is a cheap identity check.
_SECRET: Optional[str] = (os.getenv("END_USER_TOKEN_SECRET") or "").strip() or None
_LEEWAY: int = int(os.getenv("END_USER_TOKEN_LEEWAY_SECONDS", "60") or "60")
_MAX_AGE: int = int(os.getenv("END_USER_TOKEN_MAX_AGE_SECONDS", "900") or "900")
_HEADER = "X-End-User-Token"
_MAX_SUB_LEN = 256

if _SECRET is None:
    logger.info(
        "End-user attribution disabled (END_USER_TOKEN_SECRET not set); "
        "requests authenticate as the caller only and any X-End-User-Token "
        "is ignored without rejecting the request."
    )


@dataclass(frozen=True)
class RequestIdentity:
    """Who a request is for — the caller, and optionally the end-user.

    ``caller_*`` describes the authenticated API key (static break-glass
    or per-user DB key). ``end_user_*`` is populated only when a valid
    ``X-End-User-Token`` accompanied the request. Frozen so it can be
    stashed on ``request.state`` and read by middleware/handlers without
    risk of mutation mid-request.
    """

    caller_kind: str  # "static" | "db" | "anonymous"
    caller_user_id: Optional[str] = None
    caller_key_id: Optional[int] = None
    caller_name: Optional[str] = None
    caller_scopes: Tuple[str, ...] = ()
    end_user_id: Optional[str] = None
    end_user_source: Optional[str] = None

    @property
    def subject(self) -> str:
        """Most-specific stable identity string for audit/rate-limit keys."""
        if self.end_user_id:
            return f"end_user:{self.end_user_id}"
        if self.caller_user_id:
            return f"caller:{self.caller_user_id}"
        return f"caller_kind:{self.caller_kind}"


ANONYMOUS = RequestIdentity(caller_kind="anonymous")


def is_enabled() -> bool:
    """True when a verification secret is configured."""
    return _SECRET is not None


def _b64url_decode(segment: str) -> bytes:
    """base64url-decode a JWT segment, restoring stripped ``=`` padding."""
    return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))


def verify_end_user_token(token: Optional[str]) -> Optional[str]:
    """Verify a website end-user token; return its ``sub`` or ``None``.

    Never raises. Returns ``None`` when attribution is disabled, no token
    is present, or the token is malformed/forged/expired in any way. The
    HS256-only check is an explicit algorithm-confusion guard.
    """
    if _SECRET is None or not token:
        return None
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        h_b64, p_b64, s_b64 = parts

        header = json.loads(_b64url_decode(h_b64))
        if not isinstance(header, dict) or header.get("alg") != "HS256":
            return None

        expected_sig = hmac.new(
            _SECRET.encode("utf-8"),
            f"{h_b64}.{p_b64}".encode("ascii"),
            hashlib.sha256,
        ).digest()
        presented_sig = _b64url_decode(s_b64)
        if not hmac.compare_digest(expected_sig, presented_sig):
            return None

        payload = json.loads(_b64url_decode(p_b64))
        if not isinstance(payload, dict):
            return None

        now = int(time.time())

        exp = payload.get("exp")
        if not isinstance(exp, (int, float)) or isinstance(exp, bool):
            return None
        if now > int(exp) + _LEEWAY:
            return None

        iat = payload.get("iat")
        if iat is not None:
            if not isinstance(iat, (int, float)) or isinstance(iat, bool):
                return None
            if int(iat) > now + _LEEWAY:
                return None
            if now - int(iat) > _MAX_AGE + _LEEWAY:
                return None

        sub = payload.get("sub")
        if not isinstance(sub, str):
            return None
        sub = sub.strip()
        if not sub or len(sub) > _MAX_SUB_LEN:
            return None
        return sub
    except (ValueError, binascii.Error, TypeError, json.JSONDecodeError):
        return None
    except Exception:
        logger.warning(
            "Unexpected error verifying end-user token; treating as no end-user",
            exc_info=True,
        )
        return None


def resolve_end_user(request: Request) -> Tuple[Optional[str], Optional[str]]:
    """Return ``(end_user_id, source)`` from the request, or ``(None, None)``."""
    sub = verify_end_user_token(request.headers.get(_HEADER))
    if sub:
        return sub, "web-token"
    return None, None


def current_identity(request: Request) -> RequestIdentity:
    """FastAPI dependency: the :class:`RequestIdentity` for this request."""
    return getattr(request.state, "identity", ANONYMOUS)
