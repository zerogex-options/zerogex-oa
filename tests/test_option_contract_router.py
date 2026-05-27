"""Tests for the /api/option/contract endpoint."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient


def _setup_app():
    """Build the FastAPI app with a stubbed-out DatabaseManager."""
    from src.api import main as api_main

    api_main.db_manager = AsyncMock()
    return api_main.app


@pytest.fixture
def client():
    app = _setup_app()
    with patch("src.api.main.api_key_auth", lambda *_args, **_kwargs: None):
        yield TestClient(app)


def test_option_contract_returns_504_on_timeout(client):
    """Bare TimeoutError (asyncpg command_timeout / Postgres statement_timeout)
    must surface as 504, not a generic 500 with an empty-string detail.

    Pre-fix the router caught all Exception and logged ``f"...: {e}"`` —
    str(TimeoutError()) is "" so the message had nothing after the colon,
    and the client got the unactionable "Internal server error" 500.
    """
    from src.api import main as api_main

    api_main.db_manager.get_option_contract_history = AsyncMock(side_effect=asyncio.TimeoutError())

    resp = client.get(
        "/api/option/contract",
        params={
            "underlying": "SPY",
            "strike": 500,
            "expiration": "2026-05-08",
            "option_type": "C",
        },
    )
    assert resp.status_code == 504, resp.text
    assert resp.json()["detail"] == "Database query timed out"


def test_option_contract_returns_504_on_bare_timeout_error(client):
    """In Python 3.11+ asyncio.TimeoutError is aliased to the builtin
    TimeoutError; both must be caught the same way."""
    from src.api import main as api_main

    api_main.db_manager.get_option_contract_history = AsyncMock(side_effect=TimeoutError())

    resp = client.get(
        "/api/option/contract",
        params={
            "underlying": "SPY",
            "strike": 500,
            "expiration": "2026-05-08",
            "option_type": "C",
        },
    )
    assert resp.status_code == 504, resp.text


def test_option_contract_returns_404_when_no_rows(client):
    from src.api import main as api_main

    api_main.db_manager.get_option_contract_history = AsyncMock(return_value=[])

    resp = client.get(
        "/api/option/contract",
        params={
            "underlying": "SPY",
            "strike": 500,
            "expiration": "2026-05-08",
            "option_type": "C",
        },
    )
    assert resp.status_code == 404, resp.text


def test_option_contract_returns_500_with_repr_in_log_on_other_errors(client, caplog):
    """Non-timeout errors still get a 500, but the log must contain the
    exception ``repr`` so the type is visible even when ``str(e)`` is empty.
    """
    from src.api import main as api_main

    class _SilentError(Exception):
        def __str__(self):
            return ""

    api_main.db_manager.get_option_contract_history = AsyncMock(side_effect=_SilentError())

    with caplog.at_level("ERROR", logger="src.api.routers.option_contract"):
        resp = client.get(
            "/api/option/contract",
            params={
                "underlying": "SPY",
                "strike": 500,
                "expiration": "2026-05-08",
                "option_type": "C",
            },
        )
    assert resp.status_code == 500, resp.text
    # repr surfaces the exception type even when str(e) is empty.
    assert any("_SilentError" in rec.message for rec in caplog.records), [
        rec.message for rec in caplog.records
    ]


def test_option_contract_returns_rows_on_success(client):
    from src.api import main as api_main

    api_main.db_manager.get_option_contract_history = AsyncMock(
        return_value=[
            {
                "timestamp": datetime(2026, 5, 6, 19, 30, tzinfo=timezone.utc),
                "underlying": "SPY",
                "strike": Decimal("500"),
                "expiration": datetime(2026, 5, 8).date(),
                "option_type": "C",
                "last": Decimal("4.95"),
                "bid": Decimal("4.95"),
                "ask": Decimal("5.05"),
                "mid": Decimal("5.00"),
                "volume": 1000,
                "open_interest": 5000,
            }
        ]
    )

    resp = client.get(
        "/api/option/contract",
        params={
            "underlying": "SPY",
            "strike": 500,
            "expiration": "2026-05-08",
            "option_type": "C",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert len(body) == 1
    assert body[0]["underlying"] == "SPY"
    assert body[0]["mid"] == 5.00
