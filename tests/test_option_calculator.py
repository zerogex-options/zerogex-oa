"""Tests for the /api/tools/option-calculator endpoint."""

from __future__ import annotations

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


def _spy_put_730_5dollar_history():
    """Latest row mid=$5.00 for SPY 730 P expiring 2026-05-08."""
    return [
        {
            "timestamp": datetime(2026, 5, 6, 19, 30, tzinfo=timezone.utc),
            "underlying": "SPY",
            "strike": Decimal("730"),
            "expiration": datetime(2026, 5, 8).date(),
            "option_type": "P",
            "last": Decimal("4.95"),
            "bid": Decimal("4.95"),
            "ask": Decimal("5.05"),
            "mid": Decimal("5.00"),
            "volume": 1000,
            "open_interest": 5000,
        }
    ]


def test_option_calculator_matches_spy_put_example(client):
    """The 50-contract SPY 730 P example from the spec round-trips exactly."""
    from src.api import main as api_main

    api_main.db_manager.get_option_contract_history = AsyncMock(
        return_value=_spy_put_730_5dollar_history()
    )
    api_main.db_manager.get_latest_quote = AsyncMock(
        return_value={
            "timestamp": datetime(2026, 5, 6, 19, 30, tzinfo=timezone.utc),
            "symbol": "SPY",
            "close": Decimal("736.23"),
        }
    )

    resp = client.get(
        "/api/tools/option-calculator",
        params={
            "underlying": "SPY",
            "strike": 730,
            "expiration": "2026-05-08",
            "option_type": "P",
            "num_contracts": 50,
            "steps": 20,
            "step_pct": 0.001,
            "fee_per_contract": 0.5,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["spot_price"] == 736.23
    assert body["entry_price"] == 5.00
    assert body["entry_price_source"] == "mid"
    assert body["total_fees"] == 25.0
    # 5 * 50 * 100 + 25 = 25025
    assert body["total_cost"] == 25025.0
    assert body["breakeven_price"] == 725.0
    # -5 / 730 ≈ -0.006849
    assert abs(body["pct_move_to_breakeven"] - (-5.0 / 730.0)) < 1e-6

    scenarios = body["scenarios"]
    assert len(scenarios) == 20

    # First step at -0.10% — entirely OTM, full premium loss.
    first = scenarios[0]
    assert first["pct_move"] == -0.001
    assert abs(first["underlying_price"] - 735.4946) < 1e-3
    assert first["intrinsic_per_contract"] == 0.0
    assert first["pnl"] == -25025.0

    # Last step at -2.00% — comfortably ITM.
    last = scenarios[-1]
    assert last["pct_move"] == -0.02
    assert abs(last["underlying_price"] - 721.5054) < 1e-3
    # strike - underlying ≈ 730 - 721.5054 = 8.4946
    assert abs(last["intrinsic_per_contract"] - 8.4946) < 1e-3
    # 8.4946 * 50 * 100 - 25025 ≈ 17448
    assert abs(last["pnl"] - 17448.0) < 1.0


def test_option_calculator_call_walks_up(client):
    """Call options walk the underlying upward in step_pct increments."""
    from src.api import main as api_main

    api_main.db_manager.get_option_contract_history = AsyncMock(
        return_value=[
            {
                "timestamp": datetime(2026, 5, 6, 19, 30, tzinfo=timezone.utc),
                "underlying": "SPY",
                "strike": Decimal("740"),
                "expiration": datetime(2026, 5, 8).date(),
                "option_type": "C",
                "last": None,
                "bid": Decimal("2.95"),
                "ask": Decimal("3.05"),
                "mid": Decimal("3.00"),
                "volume": 100,
                "open_interest": 100,
            }
        ]
    )
    api_main.db_manager.get_latest_quote = AsyncMock(
        return_value={
            "timestamp": datetime(2026, 5, 6, 19, 30, tzinfo=timezone.utc),
            "symbol": "SPY",
            "close": Decimal("736.23"),
        }
    )

    resp = client.get(
        "/api/tools/option-calculator",
        params={
            "underlying": "SPY",
            "strike": 740,
            "expiration": "2026-05-08",
            "option_type": "C",
            "num_contracts": 1,
            "steps": 5,
            "step_pct": 0.005,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["breakeven_price"] == 743.0
    # +3 / 740 ≈ +0.004054
    assert abs(body["pct_move_to_breakeven"] - (3.0 / 740.0)) < 1e-6
    # Steps walk upward.
    assert body["scenarios"][0]["pct_move"] == 0.005
    assert body["scenarios"][-1]["pct_move"] == 0.025


def test_option_calculator_validates_step_pct_max(client):
    from src.api import main as api_main

    api_main.db_manager.get_option_contract_history = AsyncMock(return_value=[])
    api_main.db_manager.get_latest_quote = AsyncMock(return_value=None)

    resp = client.get(
        "/api/tools/option-calculator",
        params={
            "underlying": "SPY",
            "strike": 500,
            "expiration": "2026-05-08",
            "option_type": "P",
            "step_pct": 0.05,  # exceeds 1.0% max
        },
    )
    assert resp.status_code == 422


def test_option_calculator_validates_steps_max(client):
    from src.api import main as api_main

    api_main.db_manager.get_option_contract_history = AsyncMock(return_value=[])
    api_main.db_manager.get_latest_quote = AsyncMock(return_value=None)

    resp = client.get(
        "/api/tools/option-calculator",
        params={
            "underlying": "SPY",
            "strike": 500,
            "expiration": "2026-05-08",
            "option_type": "P",
            "steps": 200,  # exceeds 100 max
        },
    )
    assert resp.status_code == 422


def test_option_calculator_handles_missing_contract(client):
    from src.api import main as api_main

    api_main.db_manager.get_option_contract_history = AsyncMock(return_value=[])
    api_main.db_manager.get_latest_quote = AsyncMock(return_value=None)

    resp = client.get(
        "/api/tools/option-calculator",
        params={
            "underlying": "SPY",
            "strike": 500,
            "expiration": "2026-05-08",
            "option_type": "P",
        },
    )
    assert resp.status_code == 404
