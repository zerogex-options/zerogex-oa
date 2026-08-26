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


def main(argv: Optional[list] = None) -> int:
    load_dotenv()
    from src.ingestion.tradestation_auth import TradeStationAuth

    client_id = os.getenv("TRADESTATION_CLIENT_ID", "")
    client_secret = os.getenv("TRADESTATION_CLIENT_SECRET", "")
    refresh_token = os.getenv("TRADESTATION_REFRESH_TOKEN", "")
    if not (client_id and client_secret and refresh_token):
        print(
            "TRADESTATION_CLIENT_ID / _SECRET / _REFRESH_TOKEN must all be set in .env",
            file=sys.stderr,
        )
        return 2

    auth = TradeStationAuth(client_id, client_secret, refresh_token)
    try:
        token = auth.get_access_token()
    except Exception as e:
        print(f"Could not obtain an access token: {e}", file=sys.stderr)
        return 1

    try:
        claims = decode_jwt_payload(token)
    except Exception as e:
        print(f"Access token is not a decodable JWT ({e}) — cannot identify the user.")
        return 1

    print("=" * 72)
    print("TradeStation identity for this deployment")
    print("=" * 72)
    print(describe(claims))
    print()
    print("Market-data entitlements attach to the USERNAME above, not to the API")
    print("application. If that is not the username carrying the real-time CME")
    print("package, re-run the OAuth authorisation flow signed in as the entitled")
    print("username and replace TRADESTATION_REFRESH_TOKEN with the new value —")
    print("restarting will not move the entitlement across.")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
