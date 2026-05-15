"""Tests for the read-only db_query_cli (C2: Makefile SQL -> one place).

The CLI must route each subcommand to the canonical DatabaseManager
query method (so the SQL lives only in src/api/database.py) and render a
table without blowing up on the real return shapes.
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from src.tools import db_query_cli

TS = datetime(2026, 5, 15, 18, 30, tzinfo=timezone.utc)


class _FakeDB:
    """Stands in for DatabaseManager; records the canonical calls."""

    instance = None

    def __init__(self):
        _FakeDB.instance = self
        self.connect = AsyncMock()
        self.disconnect = AsyncMock()
        self.get_latest_gex_summary = AsyncMock(
            return_value={
                "symbol": "SPY",
                "timestamp": TS,
                "spot_price": 500.12,
                "net_gex": 1_234_567.0,
                "gamma_flip": 498.5,
                "put_call_ratio": 1.1,
                "max_pain": 505.0,
                "call_wall": 510.0,
                "put_wall": 495.0,
            }
        )
        self.get_flow_buying_pressure = AsyncMock(
            return_value=[
                {
                    "timestamp": TS,
                    "symbol": "SPY",
                    "price": 500.12,
                    "volume": 12345,
                    "buy_pct": 55.5,
                    "period_buy_pct": 60.0,
                    "price_chg": -0.25,
                    "momentum": "✅ Buying",
                }
            ]
        )
        self.get_max_pain_current = AsyncMock(
            return_value={
                "symbol": "SPY",
                "timestamp": TS,
                "underlying_price": 500.0,
                "max_pain": 505.0,
                "difference": 5.0,
                "expirations": [
                    {
                        "expiration": "2026-05-16",
                        "max_pain": 505.0,
                        "difference_from_underlying": 5.0,
                        "strikes": [
                            {
                                "settlement_price": 505.0,
                                "call_notional": 1000.0,
                                "put_notional": 2000.0,
                                "total_notional": 3000.0,
                            },
                            {
                                "settlement_price": 500.0,
                                "call_notional": 1500.0,
                                "put_notional": 1200.0,
                                "total_notional": 2700.0,
                            },
                        ],
                    },
                    {
                        "expiration": "2026-05-23",
                        "max_pain": 506.0,
                        "difference_from_underlying": 6.0,
                        "strikes": [],
                    },
                ],
            }
        )


@pytest.fixture
def fake_db(monkeypatch):
    monkeypatch.setattr(db_query_cli, "DatabaseManager", _FakeDB)
    return _FakeDB


def test_gex_summary_routes_to_canonical_query(fake_db, capsys):
    assert db_query_cli.main(["gex-summary", "spy"]) == 0
    db = _FakeDB.instance
    db.get_latest_gex_summary.assert_awaited_once_with("SPY")
    db.connect.assert_awaited_once()
    db.disconnect.assert_awaited_once()
    out = capsys.readouterr().out
    assert "net_gex" in out and "1,234,567" in out


def test_gex_summary_handles_missing_row(monkeypatch, capsys):
    class _NoneDB(_FakeDB):
        def __init__(self):
            super().__init__()
            self.get_latest_gex_summary = AsyncMock(return_value=None)

    monkeypatch.setattr(db_query_cli, "DatabaseManager", _NoneDB)
    assert db_query_cli.main(["gex-summary", "QQQ"]) == 0
    assert "no GEX summary for QQQ" in capsys.readouterr().out


def test_flow_buying_pressure_passes_limit(fake_db, capsys):
    assert db_query_cli.main(["flow-buying-pressure", "spy", "20"]) == 0
    db = _FakeDB.instance
    db.get_flow_buying_pressure.assert_awaited_once_with("SPY", 20)
    out = capsys.readouterr().out
    assert "✅ Buying" in out and "period_buy_pct" in out


def test_max_pain_current_routes_to_canonical_query(fake_db, capsys):
    assert db_query_cli.main(["max-pain-current", "spy"]) == 0
    _FakeDB.instance.get_max_pain_current.assert_awaited_once_with("SPY")
    out = capsys.readouterr().out
    assert "num_expirations" in out and "505.00" in out


def test_max_pain_expirations_lists_each_expiration(fake_db, capsys):
    assert db_query_cli.main(["max-pain-expirations", "spy"]) == 0
    out = capsys.readouterr().out
    assert "2026-05-16" in out and "2026-05-23" in out
    assert "(2 rows)" in out


def test_max_pain_strikes_uses_nearest_expiration_sorted(fake_db, capsys):
    assert db_query_cli.main(["max-pain-strikes", "spy", "20"]) == 0
    out = capsys.readouterr().out
    # Nearest expiration is the first (ascending order from the query).
    assert "Nearest expiration: 2026-05-16" in out
    # Strikes sorted ascending by settlement_price -> 500 before 505.
    assert out.index("500.00") < out.index("505.00")


def test_unknown_command_is_rejected(fake_db):
    with pytest.raises(SystemExit):
        db_query_cli.main(["not-a-command", "SPY"])
