"""Endpoint tests for the Gamma Regime Shift surface.

Covers the three routes the overhauled Gamma Shift page reads:
``/api/gex/regime-shift``, ``/api/gex/expiry-rolloff`` and
``/api/gex/regime-history``.

The maths is unit-tested in ``test_regime_shift.py`` and the storage
orchestration in ``test_regime_session.py``; what these tests pin down is the
*contract* — the shapes the frontend binds to, the refusals that must be
explicit rather than silently substituted, and the two disclosure fields
(``expiry_scope`` and ``read.normalization``) that let the card be honest
about what it measured and how confident the magnitude is.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")

DAY = date(2026, 8, 21)  # a Friday
PRIOR = date(2026, 8, 20)


def _et(day: date, hh: int, mm: int) -> datetime:
    return datetime.combine(day, time(hh, mm), tzinfo=ET).astimezone(UTC)


def _rows(strikes: Dict[float, float], exp: str) -> List[Dict[str, Any]]:
    return [
        {
            "strike": k,
            "expiration": exp,
            "net_gex": v,
            "call_gex": v,
            "put_gex": 0.0,
            "call_oi": 100,
            "put_oi": 0,
        }
        for k, v in strikes.items()
    ]


#: Snapshot fixture: two sessions, with a tranche that expires overnight.
#:
#: PRIOR carries an 2026-08-20 expiry (which is gone by DAY) plus a shared
#: 2026-08-28 expiry.  DAY builds gamma at 99 — BELOW a spot of 100, so the
#: read must come out supportive.
_SNAPSHOTS: Dict[datetime, tuple] = {
    _et(PRIOR, 12, 0): (
        100.0,
        _rows({99: 0.0, 100: 0.0, 101: 0.0}, "2026-08-28")
        + _rows({100: 900.0}, "2026-08-20"),
    ),
    _et(PRIOR, 16, 0): (
        100.0,
        _rows({99: 0.0, 100: 0.0, 101: 0.0}, "2026-08-28")
        + _rows({100: 900.0}, "2026-08-20"),
    ),
    _et(DAY, 9, 30): (100.0, _rows({99: 0.0, 100: 0.0, 101: 0.0}, "2026-08-28")),
    _et(DAY, 12, 0): (
        100.0,
        _rows({99: 800.0, 100: 50.0, 101: 0.0}, "2026-08-28")
        + _rows({99: 400.0, 100: 100.0}, "2026-08-21"),
    ),
}


async def _fake_chain_at_ts(self, symbol, at_ts, max_rows=6000):
    candidates = [ts for ts in _SNAPSHOTS if ts <= at_ts]
    if not candidates:
        return {"timestamp": None, "spot": None, "rows": []}
    ts = max(candidates)
    spot, rows = _SNAPSHOTS[ts]
    return {"timestamp": ts, "spot": spot, "rows": rows}


async def _fake_summary_at_ts(self, symbol, at_ts):
    return {
        "timestamp": at_ts,
        "spot_price": 100.0,
        "gamma_flip": 101.0 if at_ts < _et(DAY, 10, 0) else 98.5,
        "call_wall": 105.0,
        "put_wall": 95.0,
        "max_pain": 100.0,
        "net_gex_at_spot": 250.0,
    }


async def _fake_session_dates(self, symbol, limit=30):
    return [
        {"session_date": DAY, "bar_count": 390},
        {"session_date": PRIOR, "bar_count": 390},
    ]


def _stored_sessions(n: int, start: date = PRIOR) -> List[Dict[str, Any]]:
    """``n`` prior sessions with a spread of raw scores (a usable sigma)."""
    out = []
    for i in range(n):
        day = start - timedelta(days=i)
        if day.weekday() >= 5:
            continue
        out.append(
            {
                "session_date": day,
                "lean_raw": float((i % 7) - 3) * 100.0,
                "stability_raw": float((i % 5) - 2) * 150.0,
                "lean_z": 0.0,
                "stability_z": 0.0,
                "magnitude": 0.0,
                "state": "QUIET",
                "net_shift": 10.0,
                "spot_open": 99.0,
                "spot_close": 100.0,
                "band_low": 99.0,
                "band_high": 101.0,
                "band_resolved": True,
                "rolloff_share": 0.10 + (i % 9) * 0.05,
                "rolloff_expiration": day,
                "gamma_flip_open": 100.0,
                "gamma_flip_close": 100.0,
                "expired_expiration_count": 1,
            }
        )
    return out


def _build_app(monkeypatch: pytest.MonkeyPatch, sessions: List[Dict[str, Any]]):
    """Reload src.api with the DB methods this surface uses patched.

    Mirrors ``test_api_empty_list_responses._build_app_with_mocked_method``:
    patch at CLASS level before importing main, so the module-level
    ``db_manager`` instance picks the mocks up.
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
    # Assigned at class level, so these are BOUND methods: each takes `self`
    # before the real arguments.  (An AsyncMock ignores its args and hides
    # this; a real stub does not.)
    dbmod.DatabaseManager.get_gex_chain_at_ts = _fake_chain_at_ts
    dbmod.DatabaseManager.get_gex_summary_at_ts = _fake_summary_at_ts
    dbmod.DatabaseManager.get_replay_session_dates = _fake_session_dates
    dbmod.DatabaseManager.get_gex_historical_context = AsyncMock(return_value=None)

    async def _sessions(self, symbol, limit=60, before_date=None):
        rows = sessions
        if before_date is not None:
            rows = [r for r in rows if r["session_date"] < before_date]
        return sorted(rows, key=lambda r: r["session_date"], reverse=True)[:limit]

    dbmod.DatabaseManager.get_regime_sessions = _sessions

    from src.api.main import app

    return app


def _evict_api_modules() -> None:
    """Drop the mocked ``src.api`` tree from the module cache.

    ``_build_app`` patches DatabaseManager at class level and re-imports
    ``src.api.main``; leaving that behind means the NEXT test file to import
    ``src.api.main`` gets this file's mocks instead of the real class.  Evicting
    on teardown as well as on setup keeps the damage inside this module.
    """
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)


@pytest.fixture
def client_with_history(monkeypatch):
    app = _build_app(monkeypatch, _stored_sessions(40))
    with TestClient(app) as client:
        yield client
    _evict_api_modules()


@pytest.fixture
def client_no_history(monkeypatch):
    app = _build_app(monkeypatch, [])
    with TestClient(app) as client:
        yield client
    _evict_api_modules()


# --------------------------------------------------------------------------- #
# /api/gex/regime-shift
# --------------------------------------------------------------------------- #
class TestRegimeShift:
    def test_since_open_returns_the_whole_card(self, client_with_history):
        r = client_with_history.get("/api/gex/regime-shift?symbol=SPY&lookback=session")
        assert r.status_code == 200, r.text
        body = r.json()

        assert body["symbol"] == "SPY"
        assert body["lookback"] == "session"
        assert body["lookback_label"] == "Since the open"
        for key in ("from", "to", "read", "scores", "band", "strikes", "expiry_scope"):
            assert key in body, key
        assert body["from"]["timestamp"] and body["to"]["timestamp"]
        assert body["from"]["timestamp"] < body["to"]["timestamp"]

    def test_a_build_below_spot_reads_supportive(self, client_with_history):
        body = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=session"
        ).json()

        assert body["scores"]["lean"] > 0
        assert body["scores"]["stability"] > 0
        assert body["read"]["state"] in ("FIRMING", "QUIET")

    def test_levels_come_through_for_both_ends(self, client_with_history):
        body = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=session"
        ).json()

        assert body["from"]["gamma_flip"] == 101.0
        assert body["to"]["gamma_flip"] == 98.5
        assert body["to"]["call_wall"] == 105.0

    def test_day_over_day_excludes_the_expired_tranche(self, client_with_history):
        """The headline correctness property, end to end.

        PRIOR carries a 2026-08-20 expiry worth +900 that no longer exists on
        DAY.  A naive diff books it as a -900 shed and prints DETERIORATING;
        this endpoint must exclude it, report it under ``expiry_scope``, and
        still read the real (supportive) repositioning underneath.
        """
        body = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=prev_same_time"
        ).json()

        scope = body["expiry_scope"]
        assert scope["expired"] == ["2026-08-20"]
        assert scope["expired_net_gex"] == pytest.approx(900.0)
        assert "2026-08-28" in scope["common"]
        # The expired tranche is NOT in the per-strike rows...
        assert all(abs(s["d_net"]) < 900 for s in body["strikes"])
        # ...and the read is supportive, not the false DETERIORATING.
        assert body["scores"]["lean"] > 0

    def test_newly_listed_expiration_is_disclosed_too(self, client_with_history):
        body = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=prev_same_time"
        ).json()
        assert body["expiry_scope"]["added"] == ["2026-08-21"]

    def test_strikes_come_back_in_strike_order(self, client_with_history):
        body = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=session"
        ).json()
        strikes = [s["strike"] for s in body["strikes"]]
        assert strikes == sorted(strikes)

    def test_normalization_is_trailing_when_history_exists(self, client_with_history):
        body = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=session"
        ).json()
        assert body["read"]["normalization"] == "trailing"
        assert body["read"]["sessions_in_window"] > 0

    def test_normalization_bootstraps_off_the_chain_without_history(
        self, client_no_history
    ):
        """A symbol with no stored sessions still gets a real read.

        The bootstrap denominator comes from the chain the shift was measured
        on, so it is the same kind of quantity as the score.  The regression
        this guards: it used to borrow the dispersion of a stored LEVEL,
        which had no fixed relationship to a proximity-weighted change and in
        practice collapsed every z toward zero — so a freshly-deployed symbol
        printed QUIET on every session no matter what the book did.
        """
        body = client_no_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=session"
        ).json()
        assert body["read"]["normalization"] == "proxy"
        assert body["read"]["sessions_in_window"] == 0
        # 800 of gamma built one point below a spot of 100 is not "quiet".
        assert body["read"]["state"] == "FIRMING"
        assert body["read"]["magnitude"] > 0

    def test_the_window_is_normalized_over_the_hours_it_actually_spans(
        self, client_with_history
    ):
        """Every stored read is a full since-open session, so a shorter
        window has to be measured against a shorter yardstick or it reads
        QUIET on the clock rather than on the book."""
        body = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=session"
        ).json()
        # 09:30 -> 12:00 ET.
        assert body["scores"]["window_hours"] == pytest.approx(2.5)

    def test_positioning_lens_is_selectable(self, client_with_history):
        body = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=session&lens=positioning"
        ).json()
        assert body["lens"] == "positioning"

    def test_reports_whether_the_window_can_see_repositioning_at_all(
        self, client_with_history
    ):
        """Open interest is a once-a-day settlement figure, so an intraday
        A->B pair has the identical OI at every strike and the positioning
        lens is structurally empty.  The payload says so rather than letting
        the client draw a flat line that reads as "dealers did nothing"."""
        body = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=session"
        ).json()
        assert body["positioning"]["resolved"] is False
        assert body["positioning"]["oi_moved_strikes"] == 0
        assert body["positioning"]["strike_count"] > 0

    def test_rolloff_block_rides_along(self, client_with_history):
        body = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=session"
        ).json()
        assert body["rolloff"]["expiration"] == "2026-08-21"
        assert 0 < body["rolloff"]["share"] <= 1

    @pytest.mark.parametrize("lookback", ["nonsense", "1d", ""])
    def test_unknown_lookback_is_422(self, client_with_history, lookback):
        r = client_with_history.get(
            f"/api/gex/regime-shift?symbol=SPY&lookback={lookback}"
        )
        assert r.status_code == 422

    def test_unknown_lens_is_422(self, client_with_history):
        r = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=session&lens=sideways"
        )
        assert r.status_code == 422

    def test_malformed_expirations_is_400(self, client_with_history):
        r = client_with_history.get(
            "/api/gex/regime-shift?symbol=SPY&lookback=session&expirations=not-a-date"
        )
        assert r.status_code == 400

    def test_unreachable_lookback_is_409_not_a_substituted_window(
        self, client_with_history
    ):
        """A week back does not exist in the fixture.  The endpoint must refuse
        rather than quietly comparing against something else — a chart that
        silently changes its own time window is worse than one that errors."""
        r = client_with_history.get("/api/gex/regime-shift?symbol=SPY&lookback=week")
        assert r.status_code == 409
        assert "week" in r.json()["detail"]

    def test_no_data_at_all_is_404(self, monkeypatch):
        app = _build_app(monkeypatch, [])
        from src.api import database as dbmod

        async def _empty(self, symbol, at_ts, max_rows=6000):
            return {"timestamp": None, "spot": None, "rows": []}

        dbmod.DatabaseManager.get_gex_chain_at_ts = _empty
        try:
            with TestClient(app) as client:
                r = client.get("/api/gex/regime-shift?symbol=ZZZZ&lookback=session")
        finally:
            _evict_api_modules()
        assert r.status_code == 404


# --------------------------------------------------------------------------- #
# /api/gex/expiry-rolloff
# --------------------------------------------------------------------------- #
class TestExpiryRolloff:
    def test_reports_the_nearest_tranche_and_the_term_ladder(self, client_with_history):
        r = client_with_history.get("/api/gex/expiry-rolloff?symbol=SPY")
        assert r.status_code == 200, r.text
        body = r.json()

        assert body["next"]["expiration"] == "2026-08-21"
        assert body["next"]["dte"] == 0
        assert body["next"]["abs_gex"] == pytest.approx(500.0)
        assert [t["expiration"] for t in body["tranches"]] == [
            "2026-08-21",
            "2026-08-28",
        ]
        assert sum(t["share"] for t in body["tranches"]) == pytest.approx(1.0)

    def test_share_is_absolute_gamma_over_the_whole_chain(self, client_with_history):
        body = client_with_history.get("/api/gex/expiry-rolloff?symbol=SPY").json()
        # 500 expiring of 1350 total |gamma|.
        assert body["total_abs_gex"] == pytest.approx(1350.0)
        assert body["next"]["share"] == pytest.approx(500.0 / 1350.0)

    def test_next_by_strike_is_strike_ordered(self, client_with_history):
        body = client_with_history.get("/api/gex/expiry-rolloff?symbol=SPY").json()
        strikes = [s["strike"] for s in body["next_by_strike"]]
        assert strikes == sorted(strikes)
        assert strikes == [99.0, 100.0]

    def test_context_carries_a_percentile_and_a_plain_word(self, client_with_history):
        body = client_with_history.get("/api/gex/expiry-rolloff?symbol=SPY").json()
        ctx = body["context"]
        assert 0.0 <= ctx["percentile"] <= 1.0
        assert ctx["verdict"] in (
            "very light",
            "light",
            "typical",
            "above average",
            "heavy",
        )

    def test_context_is_null_without_enough_history(self, client_no_history):
        """The raw share still stands on its own; only the ranking is withheld."""
        body = client_no_history.get("/api/gex/expiry-rolloff?symbol=SPY").json()
        assert body["context"]["percentile"] is None
        assert body["context"]["verdict"] is None
        assert body["next"]["share"] > 0

    def test_no_data_is_404(self, monkeypatch):
        app = _build_app(monkeypatch, [])
        from src.api import database as dbmod

        async def _empty(self, symbol, at_ts, max_rows=6000):
            return {"timestamp": None, "spot": None, "rows": []}

        dbmod.DatabaseManager.get_gex_chain_at_ts = _empty
        try:
            with TestClient(app) as client:
                r = client.get("/api/gex/expiry-rolloff?symbol=ZZZZ")
        finally:
            _evict_api_modules()
        assert r.status_code == 404


class TestRolloffVerdict:
    @pytest.mark.parametrize(
        "pct,word",
        [
            (0.02, "very light"),
            (0.20, "light"),
            (0.50, "typical"),
            (0.80, "above average"),
            (0.97, "heavy"),
            (1.00, "heavy"),
        ],
    )
    def test_ladder(self, pct, word):
        from src.api.routers.gamma_shift import rolloff_verdict

        assert rolloff_verdict(pct) == word

    def test_none_percentile_has_no_verdict(self):
        from src.api.routers.gamma_shift import rolloff_verdict

        assert rolloff_verdict(None) is None


# --------------------------------------------------------------------------- #
# /api/gex/regime-history
# --------------------------------------------------------------------------- #
class TestRegimeHistory:
    def test_returns_sessions_newest_first(self, client_with_history):
        r = client_with_history.get("/api/gex/regime-history?symbol=SPY&limit=10")
        assert r.status_code == 200, r.text
        body = r.json()

        dates = [s["session_date"] for s in body["sessions"]]
        assert len(dates) == 10
        assert dates == sorted(dates, reverse=True)

    def test_z_scores_are_recomputed_against_one_shared_window(
        self, client_with_history
    ):
        """Rows written on different days were normalized against different
        window sizes.  Serving them as written would put a strip on screen
        whose bars are not comparable — the one thing a strip exists for.

        Every row here carries the same raw score pattern, so identical raw
        values must produce identical z values across the whole strip.
        """
        body = client_with_history.get(
            "/api/gex/regime-history?symbol=SPY&limit=30"
        ).json()

        assert body["normalization"] == "trailing"
        by_raw: Dict[float, set] = {}
        for s in body["sessions"]:
            by_raw.setdefault(s["lean_raw"], set()).add(round(s["lean_z"], 9))
        assert all(len(zs) == 1 for zs in by_raw.values())
        # ...and the stored (all-zero) z values were genuinely replaced.
        assert any(s["lean_z"] != 0 for s in body["sessions"])

    def test_states_are_reclassified_from_the_recomputed_z(
        self, client_with_history
    ):
        body = client_with_history.get(
            "/api/gex/regime-history?symbol=SPY&limit=30"
        ).json()
        states = {s["state"] for s in body["sessions"]}
        assert states - {"QUIET"}, "every row stayed QUIET — z was not recomputed"

    def test_empty_history_is_an_empty_list_not_a_404(self, client_no_history):
        r = client_no_history.get("/api/gex/regime-history?symbol=SPY")
        assert r.status_code == 200
        assert r.json() == {
            "symbol": "SPY",
            "sessions": [],
            "normalization": "none",
            "sessions_in_window": 0,
        }

    def test_limit_is_respected(self, client_with_history):
        body = client_with_history.get(
            "/api/gex/regime-history?symbol=SPY&limit=3"
        ).json()
        assert len(body["sessions"]) == 3
