"""Integration tests for /api/replay/* — the public read surface for the
GEX Replay scrubber. Stubs the DB layer so no live Postgres is needed."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from urllib.parse import quote
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient


def _build_app(monkeypatch: pytest.MonkeyPatch):
    # Force anonymous auth by disabling the static-key gate. Set API_KEY to
    # "" (which src/api/security.py normalises to None) rather than deleting
    # it: src/config.py's module-level load_dotenv() runs on the first app
    # import and would refill a *missing* API_KEY from an operator/server
    # .env, 401-ing the first test on a configured box while CI (no .env)
    # stays green. load_dotenv(override=False) leaves a present-but-empty var
    # untouched, so "" holds.
    monkeypatch.setenv("API_KEY", "")
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


def _summary(ts: datetime, **overrides) -> dict:
    base = {
        "timestamp": ts,
        "spot_price": Decimal("600.00"),
        "call_wall": Decimal("606.00"),
        "put_wall": Decimal("594.00"),
        "gamma_flip": Decimal("600.50"),
        "max_pain": Decimal("599.00"),
        "pin_strike": Decimal("601.00"),
        "pin_score": Decimal("1500000000"),
        "pin_confidence": Decimal("0.42"),
        "pin_strike_reason": None,
        "net_gex": Decimal("12345.6789"),
        "net_gex_at_spot": Decimal("2345.0"),
        "put_call_ratio": Decimal("1.23"),
    }
    base.update(overrides)
    return base


def _strikes(spot: float = 600.0):
    return [
        {"timestamp": None, "strike": Decimal(str(spot + d)), "call_gex": Decimal(str(1000 - 5 * d)),
         "put_gex": Decimal(str(-800 + 4 * d)),
         "net_gex": Decimal(str(200 - d)),
         "distance_from_spot": Decimal(str(d))}
        for d in (-5, -3, -1, 0, 1, 3, 5)
    ]


def test_sessions_returns_recent_dates(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_replay_session_dates = AsyncMock(return_value=[
        {"session_date": date(2026, 6, 29), "bar_count": 390,
         "first_ts": datetime(2026, 6, 29, 13, 30, tzinfo=timezone.utc),
         "last_ts": datetime(2026, 6, 29, 20, 0, tzinfo=timezone.utc)},
        {"session_date": date(2026, 6, 26), "bar_count": 95,
         "first_ts": datetime(2026, 6, 26, 13, 30, tzinfo=timezone.utc),
         "last_ts": datetime(2026, 6, 26, 15, 5, tzinfo=timezone.utc)},
    ])
    with TestClient(app) as client:
        r = client.get("/api/replay/sessions?symbol=SPY&limit=10")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["count"] == 2
    assert body["sessions"][0]["date"] == "2026-06-29"
    assert body["sessions"][0]["bar_count"] == 390
    assert body["sessions"][1]["bar_count"] == 95


def test_frame_returns_summary_plus_strikes(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    frame_ts = datetime(2026, 6, 29, 14, 30, tzinfo=timezone.utc)
    dbmod.DatabaseManager.get_gex_summary_at_ts = AsyncMock(return_value=_summary(frame_ts))
    dbmod.DatabaseManager.get_gex_by_strike_at_ts = AsyncMock(return_value=_strikes())
    with TestClient(app) as client:
        r = client.get("/api/replay/frame?symbol=SPY&ts=2026-06-29T14:35:00Z")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["frame_ts"] == frame_ts.isoformat()
    assert body["requested_ts"] == "2026-06-29T14:35:00+00:00"
    assert body["summary"]["spot"] == pytest.approx(600.0)
    assert body["summary"]["call_wall"] == pytest.approx(606.0)
    # Pin Strike (+ strength metadata) rides along on the frame summary so the
    # Strike Profile snapshot can draw the pin line.
    assert body["summary"]["pin_strike"] == pytest.approx(601.0)
    assert body["summary"]["pin_confidence"] == pytest.approx(0.42)
    assert body["summary"]["pin_strike_reason"] is None
    assert len(body["strikes"]) == 7
    # Strikes carry the four expected fields.
    assert all({"strike", "call_gex", "put_gex", "net_gex"} <= row.keys() for row in body["strikes"])


def test_frame_accepts_session_baseline_strike_limit(monkeypatch):
    """The Pair Comparison ladder's Δ-since-open indicator fetches one frame
    per symbol per session with a large ``strike_limit`` — the limit counts
    (strike, expiration) rows, so a dense chain needs far more than the old
    200-row cap to span the ladder's ±20-strike window across expirations."""
    app, dbmod = _build_app(monkeypatch)
    frame_ts = datetime(2026, 6, 29, 13, 30, tzinfo=timezone.utc)
    dbmod.DatabaseManager.get_gex_summary_at_ts = AsyncMock(return_value=_summary(frame_ts))
    dbmod.DatabaseManager.get_gex_by_strike_at_ts = AsyncMock(return_value=_strikes())
    with TestClient(app) as client:
        r = client.get("/api/replay/frame?symbol=SPY&ts=2026-06-29T13:30:00Z&strike_limit=4000")
        assert r.status_code == 200, r.text
        dbmod.DatabaseManager.get_gex_by_strike_at_ts.assert_awaited_with(
            "SPY", frame_ts, limit=4000
        )
        # Above the ceiling still 422s — the cap moved, it didn't vanish.
        r = client.get("/api/replay/frame?symbol=SPY&ts=2026-06-29T13:30:00Z&strike_limit=4001")
        assert r.status_code == 422


def test_frame_returns_404_when_no_data(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_gex_summary_at_ts = AsyncMock(return_value=None)
    with TestClient(app) as client:
        r = client.get("/api/replay/frame?symbol=SPY&ts=2024-01-01T14:35:00Z")
    assert r.status_code == 404


def test_frame_rejects_invalid_ts(monkeypatch):
    app, _ = _build_app(monkeypatch)
    with TestClient(app) as client:
        r = client.get("/api/replay/frame?symbol=SPY&ts=not-a-timestamp")
    assert r.status_code == 422


def test_range_returns_session_frames_by_date(monkeypatch):
    """The range endpoint MUST fetch by the requested date, not just
    return the latest N buckets. Regression against the bug where
    ``get_gex_heatmap`` (latest-N-anchored) was used in place of a
    date-scoped query — that shipped a session picker showing every
    date but only delivering data for today."""
    app, dbmod = _build_app(monkeypatch)
    bar_a = datetime(2026, 6, 29, 13, 30, tzinfo=timezone.utc)
    bar_b = datetime(2026, 6, 29, 14, 30, tzinfo=timezone.utc)
    dbmod.DatabaseManager.get_gex_frames_for_session = AsyncMock(return_value=[
        {"timestamp": bar_a, "gamma_flip": Decimal("600.5"),
         "call_wall": Decimal("606"), "put_wall": Decimal("594"),
         "max_pain": Decimal("599"),
         "pin_strike": Decimal("601"), "pin_confidence": Decimal("0.42"),
         "strikes": [{"strike": Decimal("600"), "net_gex": Decimal("1234.5"),
                      "call_gex": Decimal("2000.5"), "put_gex": Decimal("-766.0")}]},
        {"timestamp": bar_b, "gamma_flip": Decimal("601"),
         "call_wall": Decimal("607"), "put_wall": Decimal("595"),
         "max_pain": Decimal("599.5"),
         "pin_strike": Decimal("602"), "pin_confidence": Decimal("0.55"),
         "strikes": [{"strike": Decimal("600"), "net_gex": Decimal("2222.5"),
                      "call_gex": Decimal("3000.5"), "put_gex": Decimal("-778.0")}]},
    ])
    dbmod.DatabaseManager.get_underlying_candles_for_session = AsyncMock(return_value=[
        {"timestamp": bar_a, "open": Decimal("600.10"), "high": Decimal("600.50"),
         "low": Decimal("599.80"), "close": Decimal("600.40"),
         "up_volume": 1200, "down_volume": 300, "volume": 1500},
        {"timestamp": bar_b, "open": Decimal("600.40"), "high": Decimal("601.20"),
         "low": Decimal("600.30"), "close": Decimal("601.00"),
         "up_volume": 800, "down_volume": 200, "volume": 1000},
    ])
    with TestClient(app) as client:
        r = client.get("/api/replay/range?symbol=SPY&date=2026-06-29")
    body = r.json()
    assert r.status_code == 200, r.text
    assert body["date"] == "2026-06-29"
    assert body["count"] == 2
    # Chronological order preserved by the DB helper.
    assert body["frames"][0]["timestamp"] == bar_a.isoformat()
    assert body["frames"][1]["timestamp"] == bar_b.isoformat()
    assert body["frames"][0]["strikes"][0]["strike"] == pytest.approx(600.0)
    # Per-strike call/put split rides along so the scrubber can render the
    # Split / Combined gamma views (calls right, puts left, net overlaid)
    # exactly like the Strike Profile chart.
    s0 = body["frames"][0]["strikes"][0]
    assert s0["net_gex"] == pytest.approx(1234.5)
    assert s0["call_gex"] == pytest.approx(2000.5)
    assert s0["put_gex"] == pytest.approx(-766.0)
    assert body["frames"][1]["strikes"][0]["call_gex"] == pytest.approx(3000.5)
    assert body["frames"][1]["strikes"][0]["put_gex"] == pytest.approx(-778.0)
    # Call/put walls ride along on each frame so the scrubber can draw the
    # same level lines the snapshot view shows for that minute.
    assert body["frames"][0]["call_wall"] == pytest.approx(606.0)
    assert body["frames"][0]["put_wall"] == pytest.approx(594.0)
    assert body["frames"][1]["call_wall"] == pytest.approx(607.0)
    assert body["frames"][1]["put_wall"] == pytest.approx(595.0)
    # Max pain rides along on each frame too, so the replay scrubber can draw
    # the same five-level set (spot/flip/walls/max-pain) the live view shows.
    assert body["frames"][0]["max_pain"] == pytest.approx(599.0)
    assert body["frames"][1]["max_pain"] == pytest.approx(599.5)
    # Pin Strike (+ confidence) rides along per frame too, so the scrubber can
    # draw the pin line alongside the walls/flip/max-pain for each minute.
    assert body["frames"][0]["pin_strike"] == pytest.approx(601.0)
    assert body["frames"][0]["pin_confidence"] == pytest.approx(0.42)
    assert body["frames"][1]["pin_strike"] == pytest.approx(602.0)
    assert body["frames"][1]["pin_confidence"] == pytest.approx(0.55)
    # Confirm the endpoint routed to the date-scoped helper, not the
    # latest-N heatmap (the actual bug we're guarding against).
    call = dbmod.DatabaseManager.get_gex_frames_for_session.call_args
    assert call.args[0] == "SPY"
    assert call.args[1] == date(2026, 6, 29)
    # Underlying candles come back on the same axis as the frames so
    # the replay scrubber can render price action next to the strike
    # profile without a second round-trip.
    assert len(body["candles"]) == 2
    assert body["candles"][0]["timestamp"] == bar_a.isoformat()
    assert body["candles"][0]["open"] == pytest.approx(600.10)
    assert body["candles"][0]["close"] == pytest.approx(600.40)
    assert body["candles"][0]["volume"] == 1500
    assert body["candles"][1]["high"] == pytest.approx(601.20)


def test_range_returns_empty_when_date_has_no_data(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    dbmod.DatabaseManager.get_gex_frames_for_session = AsyncMock(return_value=[])
    dbmod.DatabaseManager.get_underlying_candles_for_session = AsyncMock(return_value=[])
    with TestClient(app) as client:
        r = client.get("/api/replay/range?symbol=SPY&date=2020-01-01")
    body = r.json()
    assert r.status_code == 200
    assert body["count"] == 0
    assert body["frames"] == []
    assert body["candles"] == []


def test_diff_computes_strike_deltas(monkeypatch):
    app, dbmod = _build_app(monkeypatch)
    ts_a = datetime(2026, 6, 29, 14, 0, tzinfo=timezone.utc)
    ts_b = datetime(2026, 6, 29, 15, 30, tzinfo=timezone.utc)
    dbmod.DatabaseManager.get_gex_summary_at_ts = AsyncMock(side_effect=[
        _summary(ts_a, spot_price=Decimal("600.0")),
        _summary(ts_b, spot_price=Decimal("602.0")),
    ])
    dbmod.DatabaseManager.get_gex_by_strike_at_ts = AsyncMock(side_effect=[
        [{"strike": Decimal("600"), "net_gex": Decimal("100"), "call_gex": Decimal("60"),
          "put_gex": Decimal("-40"), "distance_from_spot": Decimal("0")}],
        [{"strike": Decimal("600"), "net_gex": Decimal("150"), "call_gex": Decimal("90"),
          "put_gex": Decimal("-60"), "distance_from_spot": Decimal("-2")},
         {"strike": Decimal("605"), "net_gex": Decimal("50"), "call_gex": Decimal("30"),
          "put_gex": Decimal("-20"), "distance_from_spot": Decimal("3")}],
    ])
    with TestClient(app) as client:
        # URL-encode the timestamps because ``+`` is a reserved query-string
        # delimiter that decodes to a literal space in the route handler.
        r = client.get(
            f"/api/replay/diff?symbol=SPY&ts_a={quote(ts_a.isoformat())}&ts_b={quote(ts_b.isoformat())}"
        )
    body = r.json()
    assert r.status_code == 200, r.text
    deltas = {row["strike"]: row for row in body["deltas"]}
    assert deltas[600.0]["delta"] == pytest.approx(50.0)   # 150 − 100
    assert deltas[605.0]["delta"] == pytest.approx(50.0)   # 50 − 0 (new strike)


def test_diff_rejects_identical_timestamps(monkeypatch):
    app, _ = _build_app(monkeypatch)
    ts = "2026-06-29T14:00:00Z"
    with TestClient(app) as client:
        r = client.get(f"/api/replay/diff?symbol=SPY&ts_a={ts}&ts_b={ts}")
    assert r.status_code == 422


def test_clip_endpoint_returns_503_v1_status(monkeypatch):
    """v1 doesn't ship the MP4 worker — endpoint exists so clients can
    feature-detect, but must return 503 with a stable status string."""
    app, _ = _build_app(monkeypatch)
    with TestClient(app) as client:
        r = client.post("/api/replay/clip")
    assert r.status_code == 503
    body = r.json()
    detail = body.get("detail")
    assert isinstance(detail, dict)
    assert detail["status"] == "not_implemented_v1"
    assert "MP4" in detail["message"]


# ── Expiry colour gradient ───────────────────────────────────────────────────
# The scrubber and the shareable snapshot colour-grade each strike bar by
# time-to-expiry (nearest expiration boldest, fanning out to the faintest on
# the furthest), the same ramp the GEX Strike Profile and the Gamma Chart's
# rail draw. That needs the expiration dimension to survive the API — these
# pin the two places it used to be dropped.


def test_frame_strikes_carry_expiration(monkeypatch):
    """/replay/frame must label each row with its expiration.

    ``get_gex_by_strike_at_ts`` has always SELECTed ``g.expiration`` (the table
    is keyed per (strike, expiration)), but the router's shaper dropped it, so
    the snapshot chart received several rows for one strike with no way to tell
    them apart and could only sum them.
    """
    app, dbmod = _build_app(monkeypatch)
    ts = datetime(2026, 6, 29, 14, 30, tzinfo=timezone.utc)
    dbmod.DatabaseManager.get_gex_summary_at_ts = AsyncMock(return_value=_summary(ts))
    dbmod.DatabaseManager.get_gex_by_strike_at_ts = AsyncMock(return_value=[
        {"strike": Decimal("600"), "expiration": date(2026, 6, 29),
         "call_gex": Decimal("2000"), "put_gex": Decimal("-500"),
         "net_gex": Decimal("1500"), "distance_from_spot": Decimal("0")},
        # Same strike, later expiration — the row multiplicity the chart sees.
        {"strike": Decimal("600"), "expiration": date(2026, 7, 17),
         "call_gex": Decimal("1000"), "put_gex": Decimal("-250"),
         "net_gex": Decimal("750"), "distance_from_spot": Decimal("0")},
    ])
    with TestClient(app) as client:
        r = client.get(f"/api/replay/frame?symbol=SPY&ts={quote(ts.isoformat())}")
    assert r.status_code == 200, r.text
    strikes = r.json()["strikes"]
    assert [s["expiration"] for s in strikes] == ["2026-06-29", "2026-07-17"]
    # The existing fields are untouched.
    assert strikes[0]["call_gex"] == pytest.approx(2000.0)


def test_range_omits_expiration_mix_by_default(monkeypatch):
    """The shares are opt-in: an unmodified caller's payload is unchanged.

    The pair-comparison scrubber buffers a whole session and does not draw the
    gradient, so it must not pay for a second session-wide scan.
    """
    app, dbmod = _build_app(monkeypatch)
    bar = datetime(2026, 6, 29, 13, 30, tzinfo=timezone.utc)
    dbmod.DatabaseManager.get_gex_frames_for_session = AsyncMock(return_value=[
        {"timestamp": bar, "gamma_flip": Decimal("600.5"),
         "call_wall": None, "put_wall": None, "max_pain": None,
         "pin_strike": None, "pin_confidence": None,
         "strikes": [{"strike": Decimal("600"), "net_gex": Decimal("1234.5"),
                      "call_gex": Decimal("2000.5"), "put_gex": Decimal("-766.0")}]},
    ])
    dbmod.DatabaseManager.get_underlying_candles_for_session = AsyncMock(return_value=[])
    shares = AsyncMock()
    dbmod.DatabaseManager.get_gex_expiration_shares_for_session = shares
    with TestClient(app) as client:
        r = client.get("/api/replay/range?symbol=SPY&date=2026-06-29")
    body = r.json()
    assert r.status_code == 200, r.text
    assert "expirations" not in body
    s0 = body["frames"][0]["strikes"][0]
    assert "call_shares" not in s0 and "put_shares" not in s0
    # Not merely absent from the payload — never queried at all.
    shares.assert_not_awaited()


def test_range_attaches_expiration_shares_when_requested(monkeypatch):
    """include_expirations=true adds the legend + per-strike shares.

    Shares only SUBDIVIDE the call/put totals, so the aggregate bar values must
    come back byte-identical to the default response.
    """
    app, dbmod = _build_app(monkeypatch)
    bar = datetime(2026, 6, 29, 13, 30, tzinfo=timezone.utc)
    dbmod.DatabaseManager.get_gex_frames_for_session = AsyncMock(return_value=[
        {"timestamp": bar, "gamma_flip": Decimal("600.5"),
         "call_wall": None, "put_wall": None, "max_pain": None,
         "pin_strike": None, "pin_confidence": None,
         "strikes": [
             {"strike": Decimal("600"), "net_gex": Decimal("1234.5"),
              "call_gex": Decimal("2000.5"), "put_gex": Decimal("-766.0")},
             # A strike the shares lookup doesn't cover — must degrade to a
             # plain bar rather than dropping out of the ladder.
             {"strike": Decimal("605"), "net_gex": Decimal("10.0"),
              "call_gex": Decimal("10.0"), "put_gex": Decimal("0.0")},
         ]},
    ])
    dbmod.DatabaseManager.get_underlying_candles_for_session = AsyncMock(return_value=[])
    dbmod.DatabaseManager.get_gex_expiration_shares_for_session = AsyncMock(return_value={
        "expirations": ["2026-06-29", "2026-07-17"],
        "far_bucket": True,
        "rows": {(bar, 600.0): {"call": [0.6, 0.3, 0.1], "put": [0.5, 0.5, 0.0]}},
    })
    with TestClient(app) as client:
        r = client.get(
            "/api/replay/range?symbol=SPY&date=2026-06-29&include_expirations=true"
        )
    body = r.json()
    assert r.status_code == 200, r.text
    # Legend is nearest-first with the catch-all bucket last, so ranking by
    # position puts the faintest shade on the furthest expirations.
    assert body["expirations"] == ["2026-06-29", "2026-07-17", "far"]
    s0, s1 = body["frames"][0]["strikes"]
    assert s0["call_shares"] == [0.6, 0.3, 0.1]
    assert sum(s0["call_shares"]) == pytest.approx(1.0)
    assert s0["put_shares"] == [0.5, 0.5, 0.0]
    # Totals untouched by the split.
    assert s0["call_gex"] == pytest.approx(2000.5)
    assert s0["net_gex"] == pytest.approx(1234.5)
    # Uncovered strike keeps its bar, just without a split.
    assert s1["strike"] == pytest.approx(605.0)
    assert "call_shares" not in s1 and "put_shares" not in s1


def test_range_survives_a_failed_shares_lookup(monkeypatch):
    """A shares fetch that returns nothing degrades to plain bars.

    ``get_gex_expiration_shares_for_session`` swallows its own errors and
    returns empty structures; the replay payload must still render.
    """
    app, dbmod = _build_app(monkeypatch)
    bar = datetime(2026, 6, 29, 13, 30, tzinfo=timezone.utc)
    dbmod.DatabaseManager.get_gex_frames_for_session = AsyncMock(return_value=[
        {"timestamp": bar, "gamma_flip": None,
         "call_wall": None, "put_wall": None, "max_pain": None,
         "pin_strike": None, "pin_confidence": None,
         "strikes": [{"strike": Decimal("600"), "net_gex": Decimal("1234.5"),
                      "call_gex": Decimal("2000.5"), "put_gex": Decimal("-766.0")}]},
    ])
    dbmod.DatabaseManager.get_underlying_candles_for_session = AsyncMock(return_value=[])
    dbmod.DatabaseManager.get_gex_expiration_shares_for_session = AsyncMock(
        return_value={"expirations": [], "far_bucket": False, "rows": {}}
    )
    with TestClient(app) as client:
        r = client.get(
            "/api/replay/range?symbol=SPY&date=2026-06-29&include_expirations=true"
        )
    body = r.json()
    assert r.status_code == 200, r.text
    assert body["expirations"] == []
    assert body["frames"][0]["strikes"][0]["net_gex"] == pytest.approx(1234.5)
    assert "call_shares" not in body["frames"][0]["strikes"][0]
