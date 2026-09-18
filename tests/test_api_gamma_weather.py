"""Tests for GET /api/gex/weather.

The classifier is tested in tests/test_gamma_weather.py. What matters here is
the join: the endpoint reads two independently materialised series and has to
describe ONE bar. A sentence mixing this bar's pressure with last bar's
structure would be quietly wrong and would never look wrong.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

UTC = timezone.utc
T0 = datetime(2026, 4, 24, 13, 30, tzinfo=UTC)


def _ts(i: int) -> datetime:
    return T0 + timedelta(minutes=5 * i)


def _flow(i: int, net: float) -> Dict[str, Any]:
    return {"bar_start": _ts(i), "net_flow_usd": net}


def _regime(
    i: int,
    *,
    lean: float = 2.0e8,
    stability: float = 2.0e8,
    spot: Optional[float] = 700.0,
    flip: Optional[float] = 690.0,
) -> Dict[str, Any]:
    return {
        "bar_start": _ts(i),
        "spot": spot,
        "gamma_flip": flip,
        "rolling_lean": lean,
        "rolling_stability": stability,
        "anchored_stability": stability,
    }


def _build_app(monkeypatch: pytest.MonkeyPatch):
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

    from src.api.main import app
    from src.api import main as mainmod

    return app, mainmod


def _attach(mainmod, flow: Optional[List[dict]], regime: Optional[List[dict]]):
    """Patch both reads at the class level so the mock survives the lifespan's
    own ``db_manager = DatabaseManager()`` reassignment."""
    from src.api import database as dbmod

    mainmod.db_manager = mainmod.db_manager or mainmod.DatabaseManager()
    for target in (dbmod.DatabaseManager, mainmod.db_manager):
        setattr(target, "get_hedging_flow_series", AsyncMock(return_value=flow))
        setattr(target, "get_gamma_regime_series", AsyncMock(return_value=regime))


def _series(n: int, net: float, **regime_kw):
    """n bars of both series, newest-first, as the DB returns them."""
    flow = [_flow(i, net) for i in range(n)][::-1]
    regime = [_regime(i, **regime_kw) for i in range(n)][::-1]
    return flow, regime


# --------------------------------------------------------------------------- #
# The read
# --------------------------------------------------------------------------- #
def test_http_classifies_the_latest_bar(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app(monkeypatch)
    flow, regime = _series(6, 5.0e8)

    with TestClient(app) as client:
        _attach(mainmod, flow, regime)
        payload = client.get("/api/gex/weather?symbol=SPY").json()

    assert payload["state"] == "STABLE_BID"
    assert payload["pressure"] == "BUYING"
    assert payload["structure"] == "PINNING"
    assert payload["sentence"].startswith("Stable bid.")
    assert payload["bar_start"] == "2026-04-24T13:55:00Z"


def test_http_pairs_on_a_bar_both_series_have(monkeypatch: pytest.MonkeyPatch):
    """Structure is a bar ahead of flow. The read must describe the newest bar
    they share, not silently pair this bar's structure with last bar's push."""
    app, mainmod = _build_app(monkeypatch)
    flow = [_flow(i, 5.0e8) for i in range(5)][::-1]
    regime = [_regime(i) for i in range(6)][::-1]

    with TestClient(app) as client:
        _attach(mainmod, flow, regime)
        payload = client.get("/api/gex/weather?symbol=SPY").json()

    assert payload["bar_start"] == "2026-04-24T13:50:00Z"


def test_http_returns_components_for_auditing(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app(monkeypatch)
    flow, regime = _series(6, -5.0e8)

    with TestClient(app) as client:
        _attach(mainmod, flow, regime)
        payload = client.get("/api/gex/weather?symbol=SPY").json()

    c = payload["components"]
    assert c["pressure_bar_usd"] == -5.0e8
    assert c["pressure_avg_usd"] == pytest.approx(-5.0e8)
    assert c["spot"] == 700.0
    assert c["gamma_flip"] == 690.0
    assert c["cushion_state"] == "THIN"


def test_http_carries_the_disclosure(monkeypatch: pytest.MonkeyPatch):
    """The read inherits the estimated-not-observed caveat from the flow it
    consumes; combining inputs does not upgrade it to observed."""
    app, mainmod = _build_app(monkeypatch)
    flow, regime = _series(6, 5.0e8)

    with TestClient(app) as client:
        _attach(mainmod, flow, regime)
        payload = client.get("/api/gex/weather?symbol=SPY").json()

    assert payload["basis"] == "aggressor_inferred"
    assert "not observed dealer flow" in payload["disclosure"].lower()


def test_http_cushion_is_a_modifier_not_the_state(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app(monkeypatch)
    # Cushion collapsing from 20 points to 2 while pressure and structure hold.
    flow = [_flow(i, 5.0e8) for i in range(6)][::-1]
    regime = [_regime(i, spot=710.0 - i * 3.0, flip=690.0) for i in range(6)][::-1]

    with TestClient(app) as client:
        _attach(mainmod, flow, regime)
        payload = client.get("/api/gex/weather?symbol=SPY").json()

    assert payload["state"] == "STABLE_BID"
    assert payload["cushion"] in ("TRANSITION_RISK", "NARROWING")
    assert "cushion" in payload["sentence"]


# --------------------------------------------------------------------------- #
# Edges
# --------------------------------------------------------------------------- #
def test_http_no_shared_bar_is_409_not_a_guess(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [], [])
        assert client.get("/api/gex/weather?symbol=SPY").status_code == 409


def test_http_unknown_symbol_is_404(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, None, None)
        assert client.get("/api/gex/weather?symbol=NOPE").status_code == 404


def test_http_rejects_bad_symbol(monkeypatch: pytest.MonkeyPatch):
    app, mainmod = _build_app(monkeypatch)

    with TestClient(app) as client:
        _attach(mainmod, [], [])
        assert client.get("/api/gex/weather?symbol=SP%20Y").status_code == 400


def test_http_missing_flip_still_classifies(monkeypatch: pytest.MonkeyPatch):
    """No gamma flip is a real condition, not a reason to fail the read."""
    app, mainmod = _build_app(monkeypatch)
    flow, regime = _series(6, 5.0e8, flip=None)

    with TestClient(app) as client:
        _attach(mainmod, flow, regime)
        payload = client.get("/api/gex/weather?symbol=SPY").json()

    assert payload["state"] == "STABLE_BID"
    assert payload["cushion"] == "NONE"
    assert "no gamma flip" in payload["sentence"]


def test_http_emits_both_ladders_as_code_and_label(monkeypatch: pytest.MonkeyPatch):
    """The panel reads the wording off the payload rather than keeping its own
    copy of the maps. It used to keep one, and a rename of these rungs is
    exactly what silently breaks that: an unmatched code falls through to the
    raw value and puts PERSISTENT in front of a user."""
    app, mainmod = _build_app(monkeypatch)
    # 15 bars, not 12: the first two read MIXED until the three-bar average
    # fills, so the run that reaches MATURE starts at bar 2.
    flow, regime = _series(15, 5.0e8)

    with TestClient(app) as client:
        _attach(mainmod, flow, regime)
        payload = client.get("/api/gex/weather?symbol=SPY").json()

    assert payload["persistence"] == "PERSISTENT"
    assert payload["persistence_label"] == "Persistent"
    assert payload["age"] == "MATURE"
    assert payload["age_label"] == "Mature"


def test_http_keeps_the_two_ladders_distinguishable(monkeypatch: pytest.MonkeyPatch):
    """Both fields ride in one payload, so a value appearing in both would be
    ambiguous to anything reading it. They used to share DEVELOPING and
    ESTABLISHED, which is the collision Barrie caught from the live panel."""
    app, mainmod = _build_app(monkeypatch)
    flow, regime = _series(4, 5.0e8)

    with TestClient(app) as client:
        _attach(mainmod, flow, regime)
        payload = client.get("/api/gex/weather?symbol=SPY").json()

    assert payload["persistence"] != payload["age"]
    assert payload["persistence_label"] != payload["age_label"]
