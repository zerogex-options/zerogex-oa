"""Integration tests for GET /api/cone/* — the intraday cone read surface.

Stubs the DB layer; no live Postgres.

The reliability endpoint carries most of the weight here.  It is the claim
the whole feature rests on, and the ways it can be wrong are all quiet: a
baseline comparison pointing the wrong way, a thin sample rendered as though
it were a track record, or an abandoned claim leaking in as a win.
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

ET = ZoneInfo("America/New_York")


def _build_app(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")
    for mod in list(sys.modules):
        if mod.startswith("src.api") or mod.startswith("src.signals.playbook"):
            sys.modules.pop(mod, None)
    from src.api import database as dbmod  # noqa: E402

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    from src.api.main import app  # noqa: E402

    return app, dbmod


def _claim(minute=0, horizon=30, held=None, graded=False, hold_prob="0.7300"):
    ts = datetime(2026, 9, 21, 11, minute, tzinfo=ET)
    return {
        "symbol": "SPY",
        "session_date": date(2026, 9, 21),
        "forecast_ts": ts,
        "horizon_min": horizon,
        "target_ts": datetime(2026, 9, 21, 11 + (minute + horizon) // 60,
                              (minute + horizon) % 60, tzinfo=ET),
        "anchor_spot": Decimal("600.00"),
        "band_low": Decimal("598.00"),
        "band_high": Decimal("602.00"),
        "hold_prob": Decimal(hold_prob),
        "sigma": Decimal("1.0600"),
        "call_wall": Decimal("606.00"),
        "put_wall": Decimal("594.00"),
        "gamma_flip": Decimal("601.00"),
        "net_gex_at_spot": Decimal("400000000"),
        "gamma_mult": Decimal("1.0640"),
        "elapsed_min": 90,
        "model_version": "cone_v1_0",
        "graded_at": datetime(2026, 9, 21, 12, 0, tzinfo=ET) if graded else None,
        "window_low": Decimal("598.80") if graded else None,
        "window_high": Decimal("601.40") if graded else None,
        "held": held,
        "brier": Decimal("0.072900") if graded else None,
    }


def _graded(hold_prob: float, held: bool, horizon: int = 30, day=None):
    return {
        "session_date": day or date(2026, 9, 21),
        "forecast_ts": datetime(2026, 9, 21, 11, 0, tzinfo=ET),
        "horizon_min": horizon,
        "hold_prob": Decimal(str(hold_prob)),
        "held": held,
        "brier": Decimal("0.05"),
    }


# ---------------------------------------------------------------------------
# /session
# ---------------------------------------------------------------------------


def test_session_groups_claims_into_one_cone_per_anchor(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    rows = [_claim(minute=0, horizon=h) for h in (30, 60, 90, 120)]
    rows += [_claim(minute=15, horizon=h) for h in (30, 60)]
    dbmod.DatabaseManager.get_intraday_cones_for_session = AsyncMock(return_value=rows)

    with TestClient(app) as client:
        body = client.get("/api/cone/session/2026-09-21?symbol=SPY").json()

    assert body["n_fires"] == 2
    assert body["n_claims"] == 6
    assert [len(f["horizons"]) for f in body["fires"]] == [4, 2]
    first = body["fires"][0]
    # Anchor-level fields live on the fire, per-horizon fields on the legs.
    assert first["anchor_spot"] == 600.0
    assert first["call_wall"] == 606.0
    assert [h["horizon_min"] for h in first["horizons"]] == [30, 60, 90, 120]
    assert "anchor_spot" not in first["horizons"][0]


def test_session_counts_only_resolved_verdicts(monkeypatch):
    """An abandoned claim carries graded_at but held NULL.  It must not be
    counted as either outcome — that is the whole reason for the third state."""
    app, dbmod = _build_app(monkeypatch)
    rows = [
        _claim(minute=0, horizon=30, graded=True, held=True),
        _claim(minute=0, horizon=60, graded=True, held=False),
        _claim(minute=0, horizon=90, graded=True, held=None),   # abandoned
        _claim(minute=0, horizon=120),                          # still pending
    ]
    dbmod.DatabaseManager.get_intraday_cones_for_session = AsyncMock(return_value=rows)

    with TestClient(app) as client:
        body = client.get("/api/cone/session/2026-09-21").json()

    assert body["n_claims"] == 4
    assert body["n_graded"] == 2
    assert body["n_held"] == 1


def test_session_rejects_a_malformed_date(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_intraday_cones_for_session = AsyncMock(return_value=[])
    with TestClient(app) as client:
        assert client.get("/api/cone/session/21-09-2026").status_code == 400


def test_session_with_no_cones_is_empty_not_an_error(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_intraday_cones_for_session = AsyncMock(return_value=[])
    with TestClient(app) as client:
        body = client.get("/api/cone/session/2026-09-21").json()
    assert body["n_fires"] == 0 and body["fires"] == []


# ---------------------------------------------------------------------------
# /latest
# ---------------------------------------------------------------------------


def test_latest_returns_the_most_recent_fire(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    rows = [_claim(minute=0, horizon=30), _claim(minute=30, horizon=30)]
    dbmod.DatabaseManager.get_intraday_cones_for_session = AsyncMock(return_value=rows)

    with TestClient(app) as client:
        body = client.get("/api/cone/latest?symbol=SPY&session_date=2026-09-21").json()

    assert body["fire"]["forecast_ts"].startswith("2026-09-21T11:30")


def test_latest_says_why_when_there_is_nothing(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_intraday_cones_for_session = AsyncMock(return_value=[])
    with TestClient(app) as client:
        body = client.get("/api/cone/latest?session_date=2026-09-21").json()
    assert body["fire"] is None
    assert "no cone committed" in body["reason"]


# ---------------------------------------------------------------------------
# /reliability — the receipt
# ---------------------------------------------------------------------------


def test_reliability_reports_a_well_calibrated_cone_as_such(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    # 100 claims at 0.70, of which exactly 70 held.
    rows = [_graded(0.70, True) for _ in range(70)]
    rows += [_graded(0.70, False) for _ in range(30)]
    dbmod.DatabaseManager.get_graded_cone_history = AsyncMock(return_value=rows)

    with TestClient(app) as client:
        body = client.get("/api/cone/reliability?symbol=SPY").json()

    overall = body["overall"]
    assert overall["n"] == 100
    assert overall["hold_rate"] == pytest.approx(0.70)
    assert overall["calibration_error"] == pytest.approx(0.0, abs=1e-9)
    assert overall["sufficient_sample"] is True
    assert len(overall["reliability"]) == 1


def test_reliability_says_plainly_when_the_cone_loses_to_the_baseline(monkeypatch):
    """A cone that cannot beat 'always predict the base rate' has shown
    nothing about the market, and the endpoint has to say so rather than
    presenting a respectable-looking Brier score on its own."""
    app, dbmod = _build_app(monkeypatch)
    # Confidently wrong: says 0.90 but only half hold.
    rows = [_graded(0.90, i % 2 == 0) for i in range(100)]
    dbmod.DatabaseManager.get_graded_cone_history = AsyncMock(return_value=rows)

    with TestClient(app) as client:
        overall = client.get("/api/cone/reliability").json()["overall"]

    assert overall["beats_baseline"] is False
    assert overall["brier"] > overall["baseline_brier"]
    assert overall["calibration_error"] == pytest.approx(0.40, abs=1e-9)


def test_reliability_withholds_a_verdict_on_a_thin_sample(monkeypatch):
    """With a handful of claims a bucket can only read 0% or 100%, which looks
    like precision and is noise."""
    app, dbmod = _build_app(monkeypatch)
    rows = [_graded(0.70, True) for _ in range(5)]
    dbmod.DatabaseManager.get_graded_cone_history = AsyncMock(return_value=rows)

    with TestClient(app) as client:
        overall = client.get("/api/cone/reliability").json()["overall"]

    assert overall["n"] == 5
    assert overall["sufficient_sample"] is False
    assert overall["beats_baseline"] is None


def test_reliability_breaks_out_every_horizon(monkeypatch):
    """The term structure carries the one frankly empirical constant in the
    model, so a miss concentrated in one horizon has to be visible."""
    app, dbmod = _build_app(monkeypatch)
    rows = []
    # +30m well calibrated, +120m badly overconfident.
    rows += [_graded(0.75, True, horizon=30) for _ in range(45)]
    rows += [_graded(0.75, False, horizon=30) for _ in range(15)]
    rows += [_graded(0.75, True, horizon=120) for _ in range(20)]
    rows += [_graded(0.75, False, horizon=120) for _ in range(40)]
    dbmod.DatabaseManager.get_graded_cone_history = AsyncMock(return_value=rows)

    with TestClient(app) as client:
        by_h = client.get("/api/cone/reliability").json()["by_horizon"]

    assert by_h["30"]["hold_rate"] == pytest.approx(0.75)
    assert by_h["30"]["calibration_error"] == pytest.approx(0.0, abs=1e-9)
    assert by_h["120"]["hold_rate"] == pytest.approx(1 / 3, abs=1e-4)
    assert by_h["120"]["calibration_error"] > 0.4
    # Horizons with no graded claims report an empty block, not a fake zero.
    assert by_h["60"]["n"] == 0
    assert by_h["60"]["beats_baseline"] is None


def test_reliability_trims_to_the_requested_session_count(monkeypatch):
    """The DB window is in calendar days; the response must be in SESSIONS so
    a long weekend cannot silently shorten the sample."""
    app, dbmod = _build_app(monkeypatch)
    rows = []
    for d in (18, 19, 20, 21):
        rows += [_graded(0.7, True, day=date(2026, 9, d)) for _ in range(10)]
    dbmod.DatabaseManager.get_graded_cone_history = AsyncMock(return_value=rows)

    with TestClient(app) as client:
        body = client.get("/api/cone/reliability?window=2").json()

    assert body["sessions_covered"] == 2
    assert body["first_session"] == "2026-09-20"
    assert body["last_session"] == "2026-09-21"
    assert body["overall"]["n"] == 20


def test_reliability_ships_the_definition_of_held(monkeypatch):
    """The meaning of the number travels with the number."""
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_graded_cone_history = AsyncMock(return_value=[])
    with TestClient(app) as client:
        body = client.get("/api/cone/reliability").json()
    assert "never left the committed band" in body["definition"]
    assert "closed back inside did not hold" in body["definition"]


def test_reliability_with_no_history_is_empty_not_an_error(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_graded_cone_history = AsyncMock(return_value=[])
    with TestClient(app) as client:
        body = client.get("/api/cone/reliability").json()
    assert body["overall"]["n"] == 0
    assert body["overall"]["brier"] is None
    assert body["sessions_covered"] == 0
