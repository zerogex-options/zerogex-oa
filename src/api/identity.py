"""End-user identity attribution.

The API authenticates the *caller* (a service, or a direct consumer with
its own ``api_keys`` row).  When the caller is the website's Next.js
backend it presents a single per-user key (``user_id=zerogex-web``) on
behalf of *many* logged-in humans, so the caller identity alone cannot say
*which* end-user a request is for.

This module adds an optional second factor: the website mints a
short-lived HMAC-signed token naming the end-user and forwards it in the
``X-End-User-Token`` header.  The API verifies the signature with a shared
secret (``END_USER_TOKEN_SECRET``) and, on success, attaches the end-user
id to the request's :class:`RequestIdentity`.

Design constraints honored here:

* **Purely additive.**  No token — or no secret configured — means the
  request behaves exactly as before (caller-only identity).  Existing
  static-key and ``api_keys`` callers are unaffected, and a missing or
  bad token never *rejects* a request.
* **Pure crypto, no database.**  Token verification can never turn a DB
  outage into a 500 and never adds DB load.
* **Never raises into the request path.**  Any malformed, expired, or
  badly-signed token is treated as "no end-user", not an error.

The token is a minimal JWT (HS256) so the website side is a one-liner
with any standard JWT library, e.g. Node ``jsonwebtoken``::

    jwt.sign({ sub: userId }, process.env.END_USER_TOKEN_SECRET,
             { algorithm: "HS256", expiresIn: "5m" })
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

_SECRET: Optional[str] = (os.getenv("END_USER_TOKEN_SECRET") or "").strip() or None
# Clock-skew tolerance when checking exp/iat (seconds).
_LEEWAY: int = int(os.getenv("END_USER_TOKEN_LEEWAY_SECONDS", "60") or "60")
# Hard ceiling on the token lifetime we will honor even if the website
# sets a more generous ``exp`` — bounds the blast radius of a single
# leaked token regardless of what the minting side chose.
_MAX_AGE: int = int(os.getenv("END_USER_TOKEN_MAX_AGE_SECONDS", "900") or "900")

_HEADER = "X-End-User-Token"
_MAX_SUB_LEN = 256

if _SECRET is None:
    logger.info(
        "End-user attribution disabled (END_USER_TOKEN_SECRET not set); "
        "requests will carry caller identity only."
    )


@dataclass(frozen=True)
class RequestIdentity:
    """Who is behind a request.

    ``caller_*`` describes the authenticated API caller (a service or a
    direct consumer).  ``end_user_id`` is the website end-user the caller
    is acting for, set only when a valid ``X-End-User-Token`` accompanied
    the request; ``None`` for direct consumers and service-to-service
    calls.
    """

    caller_kind: str  # "static" | "db" | "anonymous"
    caller_user_id: Optional[str] = None
    caller_key_id: Optional[int] = None
    caller_name: Optional[str] = None
    caller_scopes: Tuple[str, ...] = ()
    end_user_id: Optional[str] = None
    end_user_source: Optional[str] = None  # "web-token" when resolved

    @property
    def subject(self) -> str:
        """Most specific principal available — for log/rate-limit keys."""
        if self.end_user_id:
            return f"end_user:{self.end_user_id}"
        if self.caller_user_id:
            return f"caller:{self.caller_user_id}"
        return f"caller_kind:{self.caller_kind}"


ANONYMOUS = RequestIdentity(caller_kind="anonymous")


def _b64url_decode(segment: str) -> bytes:
    pad = "=" * (-len(segment) % 4)
    return base64.urlsafe_b64decode(segment + pad)


def is_enabled() -> bool:
    """True when a signing secret is configured (attribution is active)."""
    return _SECRET is not None


def verify_end_user_token(token: Optional[str]) -> Optional[str]:
    """Return the end-user id from a valid token, else ``None``.

    Never raises.  Returns ``None`` when attribution is disabled, or the
    token is absent, malformed, signed with the wrong key/algorithm,
    expired, issued in the future, or whose honored lifetime exceeds the
    configured ceiling.
    """
    if _SECRET is None or not token:
        return None
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header_b64, payload_b64, sig_b64 = parts

        header = json.loads(_b64url_decode(header_b64))
        # Reject "none" and any non-HS256 alg up front: this is what
        # blocks the classic algorithm-confusion / signature-stripping
        # attack on hand-rolled JWT verifiers.
        if not isinstance(header, dict) or header.get("alg") != "HS256":
            return None

        signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
        expected = hmac.new(_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
        presented = _b64url_decode(sig_b64)
        if not hmac.compare_digest(expected, presented):
            return None

        payload = json.loads(_b64url_decode(payload_b64))
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
                return None  # issued in the future
            if now - int(iat) > _MAX_AGE + _LEEWAY:
                return None  # honored lifetime ceiling exceeded

        sub = payload.get("sub")
        if not isinstance(sub, str):
            return None
        sub = sub.strip()
        if not sub or len(sub) > _MAX_SUB_LEN:
            return None
        return sub
    except (ValueError, binascii.Error, TypeError, json.JSONDecodeError):
        return None
    except Exception:  # never let token handling break a request
        logger.warning("unexpected error verifying end-user token", exc_info=True)
        return None


def resolve_end_user(request: Request) -> Tuple[Optional[str], Optional[str]]:
    """Extract + verify the end-user from the request.

    Returns ``(end_user_id, source)`` on success, else ``(None, None)``.
    """
    sub = verify_end_user_token(request.headers.get(_HEADER))
    if sub is None:
        return None, None
    return sub, "web-token"


def current_identity(request: Request) -> RequestIdentity:
    """FastAPI dependency: the resolved identity for the current request.

    Handlers that need the end-user (or caller) declare
    ``identity: RequestIdentity = Depends(current_identity)``.  Falls back
    to :data:`ANONYMOUS` if a request errored before ``api_key_auth``
    populated ``request.state``.
    """
    return getattr(request.state, "identity", ANONYMOUS)
