"""Which TradeStation USER is this deployment authenticated as?

Market-data entitlements are attached to a TradeStation **username**, not to
an API application. The OAuth credentials split those two apart, which is easy
to miss:

    TRADESTATION_CLIENT_ID / _SECRET   identify the APP
    TRADESTATION_REFRESH_TOKEN        identifies the USER who authorised it

So an account holder with two usernames can add the real-time CME package to
one of them, restart everything, and still receive delayed quotes forever —
because the refresh token in ``.env`` was minted by the OTHER username, and
nothing in the API's behaviour says so. Streams connect, bars arrive, prices
are simply old. That is exactly the failure Step 1a of the ES/NQ runbook
describes, and this tool is how you tell the two usernames apart.

It mints an access token the normal way and decodes the identity claims out of
it. The token is a JWT: the payload is base64url, readable without the signing
key and without a second API call. **The token itself is never printed** — only
who it belongs to and when it expires.

Usage::

    python -m src.tools.tradestation_whoami

Reading the output: match the username / subject against the TradeStation
username that carries the entitlement. If they differ, no amount of restarting
will help — re-run the OAuth authorisation flow while signed in as the
entitled username and replace TRADESTATION_REFRESH_TOKEN with the new value.
"""

from __future__ import annotations

import base64
import json
import os
import sys
from typing import Any, Dict, Optional

from dotenv import load_dotenv

from src.utils import get_logger

logger = get_logger(__name__)

# Claims worth showing, in the order they are most useful. Auth0 (which
# TradeStation signs with) namespaces its custom claims, so the username can
# arrive under any of several keys depending on how the app is configured.
_IDENTITY_CLAIMS = (
    "preferred_username",
    "username",
    "name",
    "email",
    "sub",
    "aud",
    "iss",
    "scp",
    "scope",
    "exp",
    "iat",
)


def decode_jwt_payload(token: str) -> Dict[str, Any]:
    """Decode a JWT's payload WITHOUT verifying it.

    Verification would need TradeStation's signing key and buys nothing here:
    the token came straight from their token endpoint over TLS, and this is a
    read-only identity check, not an authorisation decision.
    """
    parts = token.split(".")
    if len(parts) < 2:
        raise ValueError("not a JWT (expected three dot-separated segments)")
    payload = parts[1]
    # base64url, and JWTs drop the '=' padding.
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload).decode("utf-8"))


def describe(claims: Dict[str, Any]) -> str:
    """Human-readable identity summary. Never includes the token."""
    lines = []
    for key in _IDENTITY_CLAIMS:
        if key not in claims:
            continue
        value = claims[key]
        if key in ("exp", "iat"):
            from datetime import datetime, timezone

            try:
                stamp = datetime.fromtimestamp(int(value), tz=timezone.utc)
                value = f"{value}  ({stamp.isoformat()})"
            except (TypeError, ValueError, OSError):
                pass
        lines.append(f"  {key:20} {value}")

    # Anything else the token carries, so a renamed claim can't hide the
    # username from this report.
    extra = sorted(set(claims) - set(_IDENTITY_CLAIMS))
    if extra:
        lines.append("")
        lines.append("  other claims present:")
        for key in extra:
            lines.append(f"    {key:18} {claims[key]}")
    return "\n".join(lines)


def username_of(claims: Dict[str, Any]) -> Optional[str]:
    """The TradeStation username, wherever the token happens to carry it."""
    for key, value in claims.items():
        if key.rsplit("/", 1)[-1] in ("username", "preferred_username"):
            return str(value)
    return None


def _identify(label: str, credentials: tuple) -> Optional[str]:
    """Print one credential's identity. Returns the username, or None."""
    from src.ingestion.tradestation_auth import TradeStationAuth

    client_id, client_secret, refresh_token = credentials
    print("=" * 72)
    print(label)
    print("=" * 72)
    if not (client_id and client_secret and refresh_token):
        print("  (not configured)")
        return None
    try:
        claims = decode_jwt_payload(
            TradeStationAuth(client_id, client_secret, refresh_token).get_access_token()
        )
    except Exception as e:
        print(f"  could not identify: {e}")
        return None
    print(describe(claims))
    return username_of(claims)


def main(argv: Optional[list] = None) -> int:
    load_dotenv()
    from src.config import futures_credentials_are_separate, futures_tradestation_credentials

    main_creds = (
        os.getenv("TRADESTATION_CLIENT_ID", ""),
        os.getenv("TRADESTATION_CLIENT_SECRET", ""),
        os.getenv("TRADESTATION_REFRESH_TOKEN", ""),
    )
    if not all(main_creds):
        print(
            "TRADESTATION_CLIENT_ID / _SECRET / _REFRESH_TOKEN must all be set in .env",
            file=sys.stderr,
        )
        return 2

    main_user = _identify("MAIN credential — options, equities, indexes, VIX/VXN", main_creds)

    separate = futures_credentials_are_separate()
    print()
    if separate:
        futures_user = _identify(
            "FUTURES credential (TRADESTATION_FUTURES_REFRESH_TOKEN) — ES / NQ",
            futures_tradestation_credentials(),
        )
    else:
        futures_user = main_user
        print("=" * 72)
        print("FUTURES credential — ES / NQ")
        print("=" * 72)
        print("  TRADESTATION_FUTURES_REFRESH_TOKEN is unset, so the futures feeds")
        print(f"  run under the MAIN credential ({main_user or 'unknown'}).")

    print()
    print("=" * 72)
    print("What to check")
    print("=" * 72)
    print("  Market-data entitlements attach to a USERNAME, not to the API")
    print("  application, and the refresh token is what carries the username.")
    print(f"    real-time CME (ES/NQ)  must be on: {futures_user or 'unknown'}")
    print(f"    equity / index / OPRA  must be on: {main_user or 'unknown'}")
    if not separate and main_user:
        print()
        print("  Both are the same username here. If your CME entitlement sits on a")
        print("  DIFFERENT username, do not repoint the main token at it — that one")
        print("  token also drives option chains, equity bars and every backfill.")
        print("  Mint a token for the CME-entitled username and set")
        print("  TRADESTATION_FUTURES_REFRESH_TOKEN instead, which also splits the")
        print("  per-account stream cap across the two.")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
