"""The ``?api_key=`` query-parameter credential, and its blast radius.

``/api/v1/levels`` accepts a key in the query string as well as in a header.
It exists for one caller — the Sierra Chart (ACSIL) charting study, whose
portable HTTP call (``sc.MakeHTTPRequest``) cannot attach request headers, so
a header-only endpoint is unreachable from that platform at all.

A credential in a URL is materially weaker than one in a header: proxies see
it, access logs record it (nginx is configured to redact it — see
``deploy/steps/120.nginx_api``), and it survives in ``Referer``. That is an
acceptable trade for the levels routes, whose entire response is derived,
redistributable analytics, and NOT an acceptable trade anywhere else.

So the tests that matter here are the negative ones: the accommodation must
not have widened past the routes it was added for, and a header must still
win when both are present.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

TEST_KEY = "s3cret-test-key"


def _build_app(monkeypatch: pytest.MonkeyPatch, *, api_key: str | None = TEST_KEY):
    """Reload src.api with ``API_KEY`` in place.

    ``API_KEY`` and ``ENVIRONMENT`` are read at module-import time in
    ``src.api.security``, so the whole subtree has to be flushed and
    re-imported — same approach as tests/test_api_auth_and_cors.py.
    """
    for name in ("API_KEY", "ENVIRONMENT"):
        monkeypatch.delenv(name, raising=False)
    if api_key is not None:
        monkeypatch.setenv("API_KEY", api_key)
    monkeypatch.setenv("ENVIRONMENT", "development")

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod  # noqa: E402

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)
    dbmod.DatabaseManager.get_latest_gex_summary = AsyncMock(
        return_value={
            "timestamp": datetime(2026, 7, 6, 15, 0, tzinfo=timezone.utc),
            "spot_price": 6000.0,
            "net_gex_at_spot": 1.5e9,
            "gamma_flip": 5950.0,
            "call_wall": 6100.0,
            "put_wall": 5900.0,
            "max_pain": 5975.0,
            "pin_strike": None,
            "pin_score": None,
            "pin_confidence": None,
            "pin_strike_reason": "REASON_NO_PIN",
        }
    )
    dbmod.DatabaseManager.get_latest_strike_gamma_profile = AsyncMock(return_value=[])

    from src.api.main import app  # noqa: E402
    from src.api import security  # noqa: E402

    # /api/health is public in production so probes need no credentials.
    # Clearing the allowlist makes it usable as an auth canary — every
    # request then reaches api_key_auth.
    monkeypatch.setattr(security, "_PUBLIC_PATHS", set())

    return app


# ---------------------------------------------------------------------------
# The accommodation itself
# ---------------------------------------------------------------------------


def test_levels_accepts_key_in_query_string(monkeypatch: pytest.MonkeyPatch):
    """The whole point: a headerless GET authenticates on the levels route."""
    app = _build_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(f"/api/v1/levels/SPX?strikes=1&api_key={TEST_KEY}")
    assert response.status_code == 200, response.text
    assert response.json()["levels"]["gamma_flip"] == 5950.0


def test_levels_rejects_wrong_key_in_query_string(monkeypatch: pytest.MonkeyPatch):
    """Accepting the channel is not accepting the value."""
    app = _build_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/api/v1/levels/SPX?api_key=wrong-key")
    assert response.status_code == 401


def test_levels_still_rejects_a_request_with_no_credential(monkeypatch: pytest.MonkeyPatch):
    app = _build_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/api/v1/levels/SPX")
    assert response.status_code == 401


def test_levels_still_accepts_the_header_forms(monkeypatch: pytest.MonkeyPatch):
    """The query parameter is additive — it must not have displaced the
    headers the NinjaTrader indicator and every server-side caller use."""
    app = _build_app(monkeypatch)
    with TestClient(app) as client:
        bearer = client.get("/api/v1/levels/SPX", headers={"Authorization": f"Bearer {TEST_KEY}"})
        x_api_key = client.get("/api/v1/levels/SPX", headers={"X-API-Key": TEST_KEY})
    assert bearer.status_code == 200, bearer.text
    assert x_api_key.status_code == 200, x_api_key.text


def test_header_wins_over_query_parameter(monkeypatch: pytest.MonkeyPatch):
    """A caller that can send a header is never downgraded to the weaker
    channel because a stale URL still carries the parameter — so a good
    header authenticates even alongside a junk ``?api_key=``."""
    app = _build_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(
            "/api/v1/levels/SPX?api_key=stale-and-wrong",
            headers={"Authorization": f"Bearer {TEST_KEY}"},
        )
    assert response.status_code == 200, response.text


# ---------------------------------------------------------------------------
# Blast radius — the tests this file exists for
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [
        "/api/health",
        "/api/gex/summary?symbol=SPY",
        "/api/gex/by-strike?symbol=SPY",
    ],
)
def test_query_key_is_refused_outside_the_levels_routes(monkeypatch: pytest.MonkeyPatch, path: str):
    """A valid key in the query string authenticates NOTHING else.

    If this ever passes on a non-levels path, the allowlist has been widened
    and every endpoint it now covers can be called with a credential in a URL.
    """
    app = _build_app(monkeypatch)
    separator = "&" if "?" in path else "?"
    with TestClient(app) as client:
        response = client.get(f"{path}{separator}api_key={TEST_KEY}")
    assert response.status_code == 401, (
        f"{path} authenticated from the query string; the query-key allowlist "
        f"has leaked past /api/*/levels"
    )


def test_allowlist_covers_only_the_levels_routes(monkeypatch: pytest.MonkeyPatch):
    """Pin the allowlist's contents, not just its behaviour.

    A prefix like ``/api`` or ``/`` would quietly make the entire paid
    surface query-authenticable while every behavioural test above still
    passed on the paths it happens to check.
    """
    _build_app(monkeypatch)
    from src.api import security  # noqa: E402

    assert set(security._QUERY_KEY_PATH_PREFIXES) == {
        "/api/v1/levels",
        "/api/v2/levels",
    }
    for prefix in security._QUERY_KEY_PATH_PREFIXES:
        assert prefix.endswith("/levels"), (
            f"{prefix!r} is broader than a levels route — a credential in a URL "
            f"must not reach raw quotes, flow, or the key-admin surface"
        )


def test_query_key_helper_is_prefix_matched_not_substring(
    monkeypatch: pytest.MonkeyPatch,
):
    """``/api/v1/levelsomething`` is a prefix match and fine; a path that
    merely *contains* the string must not be, or an attacker-chosen path
    segment could opt itself in."""
    _build_app(monkeypatch)
    from src.api import security  # noqa: E402

    assert security._query_key_allowed("/api/v1/levels/SPX") is True
    assert security._query_key_allowed("/api/v2/levels/SPX") is True
    assert security._query_key_allowed("/api/gex/summary") is False
    assert security._query_key_allowed("/api/v1/levels") is True
    # The dangerous shape: levels appearing anywhere but the front.
    assert security._query_key_allowed("/api/gex/api/v1/levels") is False
