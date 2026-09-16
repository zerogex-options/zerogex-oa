"""API tests for GET /api/market/spreads/surface.

The view answers one question — "are spreads unusually wide right now, and
WHERE" — and every way of getting that wrong is a way of inventing a number.
So these pin the refusals as hard as the answers:

* the rank is withheld below a minimum session count rather than computed
  from a handful of days;
* the baseline's session count and its date range describe the SAME
  population, so a thin scope cannot print a months-long range beside a zero;
* today's own rollup never ranks today;
* a moneyness slice with a current reading but no history is a gap, not a
  zero and not an interpolation;
* a band or DTE universe we hold no history for is a 400, not a comparison
  against nothing.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from src.analytics import spread_stats as ss


SESSION_DATE = date(2026, 9, 10)
# 15:42 ET -> the 15:30 bucket, inside the cash session.
SNAPSHOT_TS = datetime(2026, 9, 10, 19, 42, tzinfo=timezone.utc)
SPOT = 6000.0

# Wide enough that the smallest slice still clears the bucket floor.
PER_STRIKE = 8
STRIKE_PCTS = (-4.0, -2.0, -1.0, 0.0, 1.0, 2.0, 4.0)
# One expiry in every disjoint DTE bucket, so the by-expiry ranking is
# fully populated and a None there means a real refusal, not a hole in
# the fixture.
DTES = (0, 1, 3, 5, 10)


def _build_app(monkeypatch):
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setenv("ENVIRONMENT", "development")
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.check_health = AsyncMock(return_value=True)
    return dbmod


def _chain(*, put_0dte_width_pct: float = 6.0, ts: datetime = SNAPSHOT_TS):
    """Four expiries; 0DTE puts carry whatever width the caller asks for."""
    rows: List[Dict[str, Any]] = []
    for dte in DTES:
        expiration = SESSION_DATE + timedelta(days=dte)
        for pct in STRIKE_PCTS:
            strike = round(SPOT * (1 + pct / 100.0), 2)
            for option_type in ("P", "C"):
                base = 6.0 + abs(pct)          # a smile, in % of mid
                if option_type == "P" and dte == 0:
                    base = put_0dte_width_pct + abs(pct)
                mid = 20.0
                half = mid * base / 200.0
                for i in range(PER_STRIKE):
                    rows.append(
                        {
                            "option_symbol": f"T{dte}_{pct}_{option_type}_{i}",
                            "strike": strike,
                            "option_type": option_type,
                            "expiration": expiration,
                            "bid": mid - half,
                            "ask": mid + half,
                            "open_interest": 100,
                            "volume": 10,
                            "snapshot_ts": ts,
                            "session_date": SESSION_DATE,
                        }
                    )
    return {
        "spot_price": SPOT,
        "spot_timestamp": ts,
        "snapshot_ts": ts,
        "session_date": SESSION_DATE,
        "rows": rows,
    }


def _baseline_medians(option_type: str, band: float) -> Dict[tuple, float]:
    """What a NORMAL day of this fixture chain measures, cell by cell.

    Seeding history from a hand-picked constant is how a fixture lies: pick
    one a hair tighter than the chain's own quiet reading and every scope
    ranks 100th, which looks exactly like the blowout the view is supposed to
    isolate. Deriving the baseline from an unblown chain means "normal" means
    normal, and only the cell the test actually widened can rank extreme.
    """
    chain = _chain()
    spreads = [
        s
        for s in ss.contract_spreads(chain["rows"], SPOT)
        if s.option_type == option_type
    ]
    dte_of = {SESSION_DATE + timedelta(days=dte): dte for dte in DTES}
    return {
        (cell.dte_scope, cell.money_bucket): cell.aggregate.median_relative_spread_pct
        for cell in ss.surface_scopes(spreads, dte_of)
        if cell.band_pct == band
        and cell.aggregate.median_relative_spread_pct is not None
    }


def _surface_rows(
    *,
    sessions: int = 30,
    option_type: str = "P",
    band: float = 5.0,
    first_date: date = date(2026, 8, 1),
    scopes: Optional[List[tuple]] = None,
) -> List[Dict[str, Any]]:
    """A stored window: one row per (session, scope), oldest first.

    Each cell wanders ±1% around what a quiet day of this chain measures —
    enough spread for a percentile to be a real rank rather than a tie.
    """
    medians = _baseline_medians(option_type, band)
    if scopes is None:
        scopes = sorted(medians)
    out = []
    for day in range(sessions):
        trading_date = first_date + timedelta(days=day)
        # Deterministic, symmetric, and not monotonic in the date, so the
        # newest session is not automatically the widest.
        jitter = 1.0 + ((day % 5) - 2) * 0.005
        for dte_scope, money_bucket in scopes:
            median = medians.get((dte_scope, money_bucket), 6.0) * jitter
            out.append(
                {
                    "trading_date": trading_date,
                    "dte_scope": dte_scope,
                    "money_bucket": money_bucket,
                    "spot_price": SPOT,
                    "contract_count": 200,
                    "tradable_count": 195,
                    "two_sided_pct": 97.5,
                    "zero_bid_pct": 2.5,
                    "crossed_or_locked_pct": 0.0,
                    "median_relative_spread_pct": median,
                    "p90_relative_spread_pct": median * 2,
                    "median_spread": 0.4,
                    "source_timestamp": SNAPSHOT_TS,
                }
            )
    return out


class _StubDb:
    def __init__(self, chain, window, latest_bucket=None):
        self._chain = chain
        self._window = window
        self._latest_bucket = latest_bucket
        self.window_calls: List[tuple] = []

    async def get_spread_snapshot_chain(self, symbol, dte_max, band):
        if isinstance(self._chain, Exception):
            raise self._chain
        return self._chain

    async def get_spread_surface_window(
        self, symbol, option_type, band_pct, bucket_start_min, days
    ):
        self.window_calls.append(
            (symbol, option_type, band_pct, bucket_start_min, days)
        )
        if isinstance(self._window, Exception):
            raise self._window
        return self._window or []

    async def get_spread_surface_latest_bucket(self, symbol, option_type):
        return self._latest_bucket


def _client(monkeypatch, *, chain=None, window=None, latest_bucket=None):
    _build_app(monkeypatch)
    from src.api.main import app
    from src.api.routers import spread_liquidity as mod

    stub = _StubDb(_chain() if chain is None else chain, window, latest_bucket)
    app.dependency_overrides[mod.get_db] = lambda: stub
    mod._cache.clear()
    return TestClient(app), stub


def _get(client, **params) -> Dict[str, Any]:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    response = client.get(f"/api/market/spreads/surface?{query}")
    assert response.status_code == 200, response.text
    return response.json()


# ---------------------------------------------------------------------------
# The question the view exists for
# ---------------------------------------------------------------------------


def test_a_0dte_put_blowout_shows_up_as_0dte_and_not_as_everything(monkeypatch):
    """"NDX broadly looks normal, but 0DTE is blowing out" must be readable.

    A chain where only the 0DTE puts have widened has to rank 0DTE extreme
    and every other expiry ordinary. An implementation that pooled expiries
    would smear the blowout across the whole surface and show a mild
    elevation everywhere, which is the opposite of the finding.
    """
    client, _ = _client(
        monkeypatch,
        chain=_chain(put_0dte_width_pct=18.0),
        window=_surface_rows(),
    )
    body = _get(client, symbol="SPX", option_type="P", dte_max=0, moneyness_band_pct=5)

    assert body["summary"]["vs_normal"] > 2.0
    assert body["summary"]["percentile"] == 100.0

    ranks = {row["dte_scope"]: row for row in body["by_dte"]}
    assert ranks["b0"]["percentile"] == 100.0
    for key in ("b1", "b2_3", "b4_7", "b8_30"):
        # Sitting in the body of its own distribution — an unmoved expiry
        # reads as ordinary, not as a milder version of the blowout.
        assert 25.0 <= ranks[key]["percentile"] <= 75.0, key
        assert ranks[key]["insufficient_history"] is False


def test_the_untouched_side_of_the_book_still_reads_normal(monkeypatch):
    """The put reading is only meaningful against a side that has not moved."""
    client, _ = _client(
        monkeypatch,
        chain=_chain(put_0dte_width_pct=18.0),
        window=_surface_rows(option_type="C"),
    )
    calls = _get(
        client, symbol="SPX", option_type="C", dte_max=0, moneyness_band_pct=5
    )
    assert calls["summary"]["vs_normal"] < 1.5
    assert calls["summary"]["percentile"] < 100.0


def test_puts_and_calls_are_never_blended(monkeypatch):
    client, stub = _client(
        monkeypatch,
        chain=_chain(put_0dte_width_pct=18.0),
        window=_surface_rows(),
    )
    puts = _get(client, symbol="SPX", option_type="P", dte_max=0)
    calls = _get(client, symbol="SPX", option_type="C", dte_max=0)
    assert puts["summary"]["current_pct"] > calls["summary"]["current_pct"] * 2
    assert {call[1] for call in stub.window_calls} == {"P", "C"}


# ---------------------------------------------------------------------------
# Never invent a comparison
# ---------------------------------------------------------------------------


def test_too_little_history_withholds_the_rank_instead_of_guessing(monkeypatch):
    from src.config import SPREAD_SURFACE_MIN_SESSIONS

    client, _ = _client(
        monkeypatch,
        window=_surface_rows(sessions=int(SPREAD_SURFACE_MIN_SESSIONS) - 1),
    )
    body = _get(client, symbol="SPX", option_type="P", dte_max=0)

    assert body["summary"]["percentile"] is None
    # The median is still honest arithmetic on the days we have; only the
    # RANK is unsafe, because a rank implies a distribution.
    assert body["summary"]["normal_pct"] is not None
    assert body["summary"]["sessions"] == int(SPREAD_SURFACE_MIN_SESSIONS) - 1
    assert all(row["insufficient_history"] for row in body["by_dte"])


def test_no_history_at_all_still_renders_today(monkeypatch):
    client, _ = _client(monkeypatch, window=[])
    body = _get(client, symbol="SPX", option_type="P", dte_max=0)

    assert body["summary"]["current_pct"] > 0
    assert body["summary"]["normal_pct"] is None
    assert body["summary"]["vs_normal"] is None
    assert body["baseline"]["sessions"] == 0
    assert body["baseline"]["time_matched"] is False
    assert body["baseline"]["earliest_date"] is None


def test_the_session_count_and_the_date_range_describe_the_same_population(
    monkeypatch,
):
    """A thin scope must not print a months-long range beside a zero count.

    The window is read for every scope at once, so a date range taken from
    the whole response body would advertise history the selected scope does
    not have — the "implies 60 sessions when 36 are comparable" failure,
    inverted.
    """
    scopes = [("u7", ss.BAND_WIDE), ("u30", ss.BAND_WIDE)]
    client, _ = _client(
        monkeypatch, window=_surface_rows(sessions=30, scopes=scopes)
    )
    body = _get(client, symbol="SPX", option_type="P", dte_max=0)

    assert body["baseline"]["sessions"] == 0
    assert body["baseline"]["earliest_date"] is None
    assert body["baseline"]["latest_date"] is None

    wide = _get(client, symbol="SPX", option_type="P", dte_max=7)
    assert wide["baseline"]["sessions"] == 30
    assert wide["baseline"]["earliest_date"] == "2026-08-01"


def test_todays_own_rollup_never_ranks_today(monkeypatch):
    """Otherwise the reading drags its own baseline on the day it matters."""
    rows = _surface_rows(sessions=30)
    today = [dict(row, trading_date=SESSION_DATE) for row in rows[-20:]]
    client, _ = _client(monkeypatch, window=rows + today)
    body = _get(client, symbol="SPX", option_type="P", dte_max=0)

    assert body["baseline"]["sessions"] == 30
    assert body["baseline"]["latest_date"] < SESSION_DATE.isoformat()


def test_a_slice_with_no_history_is_a_gap_not_a_zero(monkeypatch):
    """The curve must break, not dive to the floor and back."""
    keep = ss.moneyness_bucket_key(-0.5, 0.5)
    scopes = [("u0", ss.BAND_WIDE), ("u0", keep)]
    client, _ = _client(monkeypatch, window=_surface_rows(scopes=scopes))
    curve = _get(client, symbol="SPX", option_type="P", dte_max=0)["curve"]

    by_bucket = {point["money_bucket"]: point for point in curve}
    assert by_bucket[keep]["historical_median_pct"] is not None

    others = [p for b, p in by_bucket.items() if b != keep]
    assert others, "expected slices outside the stored one"
    for point in others:
        assert point["current_pct"] is not None
        assert point["historical_median_pct"] is None
        assert point["historical_p25_pct"] is None
        assert point["percentile"] is None
        assert point["sessions"] == 0


# ---------------------------------------------------------------------------
# Scope, time matching and degradation
# ---------------------------------------------------------------------------


def test_only_the_enumerated_scopes_are_comparable(monkeypatch):
    client, _ = _client(monkeypatch, window=_surface_rows())

    bad_dte = client.get("/api/market/spreads/surface?symbol=SPX&dte_max=3")
    assert bad_dte.status_code == 400
    assert "0, 1, 7, 30" in bad_dte.json()["detail"]

    bad_band = client.get(
        "/api/market/spreads/surface?symbol=SPX&moneyness_band_pct=4"
    )
    assert bad_band.status_code == 400
    assert "history is stored per band" in bad_band.json()["detail"]


def test_history_is_matched_to_the_current_half_hour(monkeypatch):
    client, stub = _client(monkeypatch, window=_surface_rows())
    body = _get(client, symbol="SPX", option_type="P", dte_max=0)

    # 15:42 ET -> 15:30, and the read asks for that bucket only.
    assert stub.window_calls[0][3] == 15 * 60 + 30
    assert body["baseline"]["time_bucket_label"] == "15:30-16:00 ET"
    assert body["baseline"]["time_matched"] is True
    assert body["baseline"]["fell_back_to_last_bucket"] is False


def test_outside_the_session_the_fallback_is_disclosed(monkeypatch):
    """A Saturday reading ranked against Friday's close says so."""
    after_hours = datetime(2026, 9, 10, 23, 5, tzinfo=timezone.utc)  # 19:05 ET
    client, _ = _client(
        monkeypatch,
        chain=_chain(ts=after_hours),
        window=_surface_rows(),
        latest_bucket=15 * 60 + 30,
    )
    body = _get(client, symbol="SPX", option_type="P", dte_max=0)

    assert body["baseline"]["fell_back_to_last_bucket"] is True
    assert body["baseline"]["time_bucket_label"] == "15:30-16:00 ET"


def test_an_unreadable_history_degrades_to_today_rather_than_500(monkeypatch):
    """A partially-migrated deployment still renders the live surface."""
    client, _ = _client(monkeypatch, window=RuntimeError("no such table"))
    body = _get(client, symbol="SPX", option_type="P", dte_max=0)

    assert body["summary"]["current_pct"] > 0
    assert body["summary"]["percentile"] is None
    assert body["baseline"]["sessions"] == 0


def test_the_quoted_nbbo_disclosure_travels_with_the_surface(monkeypatch):
    client, _ = _client(monkeypatch, window=_surface_rows())
    body = _get(client, symbol="SPX", option_type="P", dte_max=0)
    assert body["basis"] == "quoted_nbbo"
    assert "not effective spreads" in body["disclosure"]
