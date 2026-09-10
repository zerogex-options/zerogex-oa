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


# --- two usernames, two entitlements ---------------------------------------
#
# The account holder runs two TradeStation usernames against one API app,
# deliberately, to split the per-account stream cap. Entitlements followed the
# usernames rather than the app: real-time CME landed on one, while the equity
# / index / OPRA entitlements the rest of the platform depends on stayed on the
# other. One global refresh token cannot see both.


def test_the_futures_feeds_fall_back_to_the_main_credential(monkeypatch):
    """The single-username deployment must be unchanged by any of this."""
    from src.config import futures_credentials_are_separate, futures_tradestation_credentials

    monkeypatch.setenv("TRADESTATION_CLIENT_ID", "app")
    monkeypatch.setenv("TRADESTATION_CLIENT_SECRET", "secret")
    monkeypatch.setenv("TRADESTATION_REFRESH_TOKEN", "main-token")
    monkeypatch.delenv("TRADESTATION_FUTURES_REFRESH_TOKEN", raising=False)

    assert futures_tradestation_credentials() == ("app", "secret", "main-token")
    assert futures_credentials_are_separate() is False


def test_only_the_refresh_token_has_to_be_overridden(monkeypatch):
    """Both usernames sit under the SAME API application, so the id/secret are
    shared and the second identity is one extra line in .env."""
    from src.config import futures_credentials_are_separate, futures_tradestation_credentials

    monkeypatch.setenv("TRADESTATION_CLIENT_ID", "app")
    monkeypatch.setenv("TRADESTATION_CLIENT_SECRET", "secret")
    monkeypatch.setenv("TRADESTATION_REFRESH_TOKEN", "main-token")
    monkeypatch.setenv("TRADESTATION_FUTURES_REFRESH_TOKEN", "cme-token")

    assert futures_tradestation_credentials() == ("app", "secret", "cme-token")
    assert futures_credentials_are_separate() is True


def test_a_separate_api_application_is_also_supported(monkeypatch):
    """Less common, but each field falls back independently rather than
    forcing all-or-nothing."""
    from src.config import futures_tradestation_credentials

    monkeypatch.setenv("TRADESTATION_CLIENT_ID", "app")
    monkeypatch.setenv("TRADESTATION_CLIENT_SECRET", "secret")
    monkeypatch.setenv("TRADESTATION_REFRESH_TOKEN", "main-token")
    monkeypatch.setenv("TRADESTATION_FUTURES_CLIENT_ID", "app2")
    monkeypatch.setenv("TRADESTATION_FUTURES_REFRESH_TOKEN", "cme-token")

    assert futures_tradestation_credentials() == ("app2", "secret", "cme-token")


def test_the_same_token_repeated_is_not_treated_as_separate(monkeypatch):
    """Pasting the main token into the futures var is a no-op, not a second
    account — it would otherwise report two identities that are one."""
    from src.config import futures_credentials_are_separate

    monkeypatch.setenv("TRADESTATION_REFRESH_TOKEN", "same")
    monkeypatch.setenv("TRADESTATION_FUTURES_REFRESH_TOKEN", "same")
    assert futures_credentials_are_separate() is False


def test_a_blank_futures_token_falls_back_rather_than_authenticating_as_nobody(monkeypatch):
    """An empty var in .env must not blank the credential and take ES/NQ down."""
    from src.config import futures_credentials_are_separate, futures_tradestation_credentials

    monkeypatch.setenv("TRADESTATION_CLIENT_ID", "app")
    monkeypatch.setenv("TRADESTATION_CLIENT_SECRET", "secret")
    monkeypatch.setenv("TRADESTATION_REFRESH_TOKEN", "main-token")
    monkeypatch.setenv("TRADESTATION_FUTURES_REFRESH_TOKEN", "")

    assert futures_tradestation_credentials() == ("app", "secret", "main-token")
    assert futures_credentials_are_separate() is False


def test_the_username_is_found_under_a_namespaced_claim():
    """TradeStation carries it as http://tradestation.com/username, which is
    how this deployment turned out to be running as the wrong account."""
    from src.tools.tradestation_whoami import username_of

    assert username_of({"http://tradestation.com/username": "mikejb124"}) == "mikejb124"
    assert username_of({"preferred_username": "mikejb124b"}) == "mikejb124b"
    assert username_of({"sub": "auth0|1"}) is None
