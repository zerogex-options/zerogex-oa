"""The username behind TRADESTATION_REFRESH_TOKEN.

Entitlements attach to a TradeStation username; OAuth splits the app identity
(client id/secret) from the user identity (refresh token). An account holder
with two usernames can therefore buy real-time CME on one, restart everything,
and receive delayed quotes indefinitely because the refresh token was minted
by the other one — with nothing in the API's behaviour to say so.

Two properties are pinned: the decode must survive real JWT encoding, and the
report must never leak the token it was decoded from.
"""

import base64
import json

import pytest

from src.tools.tradestation_whoami import decode_jwt_payload, describe


def _jwt(payload: dict) -> str:
    def seg(obj) -> str:
        raw = json.dumps(obj).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")  # JWTs drop padding

    return f"{seg({'alg': 'RS256'})}.{seg(payload)}.c2lnbmF0dXJl"


def test_the_payload_survives_stripped_base64url_padding():
    """JWT segments drop '=' padding, and b64decode raises without it. Payload
    lengths vary, so this only fails for some tokens — exactly the kind of bug
    that passes locally and breaks on the deployment you needed it for."""
    for extra in range(6):  # walk every payload length mod 4
        claims = {"sub": "auth0|abc", "preferred_username": "user" + "x" * extra}
        assert decode_jwt_payload(_jwt(claims)) == claims


def test_a_non_jwt_is_rejected_rather_than_half_decoded():
    with pytest.raises(ValueError):
        decode_jwt_payload("not-a-jwt")


def test_the_username_is_reported():
    out = describe(decode_jwt_payload(_jwt({"preferred_username": "trader_b", "sub": "auth0|1"})))
    assert "trader_b" in out


def test_an_unexpected_claim_name_cannot_hide_the_identity():
    """Auth0 namespaces custom claims, so the username may not arrive under a
    key this tool knows. Everything unrecognised is still printed."""
    out = describe(decode_jwt_payload(_jwt({"https://ts.io/user": "trader_b"})))
    assert "trader_b" in out


def test_the_report_never_contains_the_token():
    token = _jwt({"sub": "auth0|1", "preferred_username": "trader_b"})
    out = describe(decode_jwt_payload(token))
    assert token not in out
    for segment in token.split("."):
        assert segment not in out


def test_expiry_is_rendered_as_a_readable_instant():
    out = describe(decode_jwt_payload(_jwt({"exp": 1787000000})))
    assert "2026-" in out
