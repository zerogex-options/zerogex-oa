"""End-to-end shape tests for /api/gex/strike-profile-timeseries.

These tests run through the full FastAPI request → response → JSON path
(not just the SQL query shape), so they pin the public contract the
frontend Strike-Profile rewind chart depends on:

  * top-level shape is a list of bucket objects
  * each bucket carries timestamp, symbol, OHLC, flip/walls, and a
    ``strikes`` array using exactly the names ``call_gamma``,
    ``put_gamma``, ``net_gamma``, ``call_oi``, ``put_oi`` (the request
    payload the frontend type-checks against)
  * Decimals serialize as floats so the chart can do arithmetic on the
    values without dragging in a Decimal shim
  * empty data path returns 200 + [] (matches every other list
    endpoint — see test_api_empty_list_responses)
  * the ``expirations`` query param accepts ``all`` and a YYYY-MM-DD
    date; anything else is a 400
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient


def _build_app_with_mocked_method(
    monkeypatch: pytest.MonkeyPatch, returns
) -> tuple[TestClient, AsyncMock]:
    """Reload src.api.main with get_strike_profile_timeseries patched.

    Same pattern test_api_empty_list_responses uses: patch the class
    before importing main so the module-level db_manager picks up the
    mock.
    """
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")

    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)

    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    dbmod.DatabaseManager.get_latest_quote = AsyncMock(return_value=None)
    mock = AsyncMock(return_value=returns)
    dbmod.DatabaseManager.get_strike_profile_timeseries = mock

    from src.api.main import app

    return TestClient(app), mock


def _sample_bucket(ts: str, strike: float = 505.0) -> dict:
    """Bucket dict shaped like the SQL grouping output."""
    return {
        "timestamp": datetime.fromisoformat(ts.replace("Z", "+00:00")),
        "symbol": "SPY",
        "open": 512.30,
        "high": 513.10,
        "low": 511.85,
        "close": 512.80,
        "gamma_flip": 510.0,
        "call_wall": 515.0,
        "put_wall": 505.0,
        "strikes": [
            {
                "strike": strike,
                "call_gamma": 1234.5,
                "put_gamma": -2345.6,
                "net_gamma": -1111.1,
                "call_oi": 8200,
                "put_oi": 9100,
            }
        ],
    }


# ---------------------------------------------------------------------------
# Happy path: response shape + JSON encoding
# ---------------------------------------------------------------------------


def test_response_shape_is_list_of_buckets(monkeypatch: pytest.MonkeyPatch):
    client, _ = _build_app_with_mocked_method(
        monkeypatch,
        returns=[
            _sample_bucket("2026-06-08T14:30:00+00:00", strike=505.0),
            _sample_bucket("2026-06-08T14:31:00+00:00", strike=510.0),
        ],
    )
    with client:
        response = client.get("/api/gex/strike-profile-timeseries?symbol=SPY")

    assert response.status_code == 200, response.text
    body = response.json()
    assert isinstance(body, list)
    assert len(body) == 2

    bucket = body[0]
    # Top-level field shape pinned to the request payload.
    for field in (
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "gamma_flip",
        "call_wall",
        "put_wall",
        "strikes",
    ):
        assert field in bucket, field

    # Numerics arrive as floats (Decimal → float via the model's
    # json_encoders).  The chart relies on this to skip a Decimal shim.
    assert isinstance(bucket["close"], float)
    assert bucket["close"] == 512.80

    # Strikes shape uses exactly the names the frontend type-checks
    # against (call_gamma / put_gamma / net_gamma, not call_gex et al.).
    strike_row = bucket["strikes"][0]
    for field in ("strike", "call_gamma", "put_gamma", "net_gamma", "call_oi", "put_oi"):
        assert field in strike_row, field
    assert isinstance(strike_row["call_oi"], int)
    assert strike_row["call_gamma"] == 1234.5
    assert strike_row["put_gamma"] == -2345.6


def test_buckets_with_no_strikes_serialize_with_empty_array(monkeypatch: pytest.MonkeyPatch):
    """A bucket whose rep_ts had no gex_by_strike rows must still appear
    in the response with strikes=[] — the chart's rewindIndex relies on
    the 1:1 alignment with the bucket grid."""
    bucket = _sample_bucket("2026-06-08T14:30:00+00:00")
    bucket["strikes"] = []
    client, _ = _build_app_with_mocked_method(monkeypatch, returns=[bucket])
    with client:
        response = client.get("/api/gex/strike-profile-timeseries?symbol=SPY")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["strikes"] == []


# ---------------------------------------------------------------------------
# Expirations param validation
# ---------------------------------------------------------------------------


def test_expirations_all_passed_as_none(monkeypatch: pytest.MonkeyPatch):
    """``expirations=all`` (the default) must reach the DB layer as
    ``expiration=None`` so the SQL's IS NULL predicate fires and every
    expiration's strikes get summed in."""
    client, mock = _build_app_with_mocked_method(monkeypatch, returns=[])
    with client:
        response = client.get("/api/gex/strike-profile-timeseries?symbol=SPY&expirations=all")

    assert response.status_code == 200
    mock.assert_awaited_once()
    args, kwargs = mock.call_args
    # The 4th positional arg is `expiration` (after symbol, timeframe, window_units).
    assert args[3] is None


def test_expirations_date_passed_through(monkeypatch: pytest.MonkeyPatch):
    """A single YYYY-MM-DD expiration must reach the DB layer as a
    ``datetime.date`` so the SQL binds it via $3::date."""
    client, mock = _build_app_with_mocked_method(monkeypatch, returns=[])
    with client:
        response = client.get(
            "/api/gex/strike-profile-timeseries?symbol=SPY&expirations=2026-06-19"
        )

    assert response.status_code == 200
    mock.assert_awaited_once()
    args, _ = mock.call_args
    assert args[3] == date(2026, 6, 19)


def test_expirations_invalid_returns_400(monkeypatch: pytest.MonkeyPatch):
    """Anything other than ``all`` or a parseable YYYY-MM-DD date is a
    client error.  The chart's expiry dropdown only sends valid values,
    so this is defense-in-depth against SQL-injection-as-date — never
    interpolate the raw string into the SQL."""
    client, _ = _build_app_with_mocked_method(monkeypatch, returns=[])
    with client:
        response = client.get("/api/gex/strike-profile-timeseries?symbol=SPY&expirations=tomorrow")
    assert response.status_code == 400
    assert "expirations" in response.json()["detail"].lower()


# ---------------------------------------------------------------------------
# Query-param defaults
# ---------------------------------------------------------------------------


def test_defaults_match_request_spec(monkeypatch: pytest.MonkeyPatch):
    """symbol=SPY, timeframe=1min, window_units=78, expirations=all."""
    client, mock = _build_app_with_mocked_method(monkeypatch, returns=[])
    with client:
        response = client.get("/api/gex/strike-profile-timeseries")

    assert response.status_code == 200
    args, _ = mock.call_args
    assert args[0] == "SPY"
    assert args[1] == "1min"
    assert args[2] == 78
    assert args[3] is None


def test_window_units_respects_upper_bound(monkeypatch: pytest.MonkeyPatch):
    """window_units > 480 is rejected by FastAPI's query-param validator
    (the chart only ever asks for 480; anything larger would be an
    accidental fetch and we want a clear 422 not a slow DB scan)."""
    client, _ = _build_app_with_mocked_method(monkeypatch, returns=[])
    with client:
        response = client.get("/api/gex/strike-profile-timeseries?symbol=SPY&window_units=10000")
    assert response.status_code == 422


def test_timeframe_only_accepts_minute_intervals(monkeypatch: pytest.MonkeyPatch):
    """The chart's timeframe selector only offers 1min/5min/15min — and
    the rewind UX assumes minute-bucketed buckets.  Hour/day timeframes
    are out of scope for this endpoint; reject them up front so a typo
    in the frontend doesn't silently produce an unusable response."""
    client, _ = _build_app_with_mocked_method(monkeypatch, returns=[])
    with client:
        response = client.get("/api/gex/strike-profile-timeseries?symbol=SPY&timeframe=1hr")
    assert response.status_code == 422
