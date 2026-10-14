"""Wall alerts against a real Postgres: the store, the service tick, the API.

The unit suites mock the connection, so the SQL that decides who is alerted —
the rule match, the strength floor, the per-(member, channel) dedupe, the
daily cap, the unique event key that makes a re-run harmless, the outbox
guards and every endpoint's ownership filter — is only proven here.

Run it against a SCRATCH database with ``setup/database/schema.sql`` applied::

    make wall-alerts-db-test WALL_ALERTS_TEST_DSN=postgresql://user@host:5432/scratch

The DSN is never derived from ``.env``: this harness writes and deletes rows,
and a default that resolves to production is one ``make`` away from doing so
there. Everything it writes is keyed on a sentinel symbol and sentinel member
ids and removed afterwards.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import hmac
import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

pytestmark = pytest.mark.integration

_DSN = os.getenv("WALL_ALERTS_TEST_DSN")
_SKIP = "WALL_ALERTS_TEST_DSN not set — wall-alert DB harness skipped."

SYMBOL = "ZZWA"
USERS = ("wa-test-u1", "wa-test-u2", "wa-test-u3", "wa-test-u4")
SECRET = "wall-alert-test-secret"
ADMIN = "wall-alert-test-admin"

SESSION = date(2026, 10, 13)  # a Tuesday


def _et(hour: int, minute: int, second: int = 0) -> datetime:
    from zoneinfo import ZoneInfo

    return datetime(2026, 10, 13, hour, minute, second, tzinfo=ZoneInfo("America/New_York"))


@pytest.fixture()
def pg():
    if not _DSN:
        pytest.skip(_SKIP)
    import psycopg2

    conn = psycopg2.connect(_DSN)
    _cleanup(conn)
    cur = conn.cursor()
    cur.execute("INSERT INTO symbols (symbol) VALUES (%s) ON CONFLICT DO NOTHING", (SYMBOL,))
    conn.commit()
    try:
        yield conn
    finally:
        conn.rollback()
        _cleanup(conn)
        conn.close()


def _cleanup(conn) -> None:
    cur = conn.cursor()
    cur.execute("DELETE FROM wall_alert_deliveries WHERE end_user = ANY(%s)", (list(USERS),))
    cur.execute("DELETE FROM wall_alert_rules WHERE end_user = ANY(%s)", (list(USERS),))
    cur.execute("DELETE FROM wall_alert_events WHERE underlying = %s", (SYMBOL,))
    cur.execute("DELETE FROM wall_alert_detector_state WHERE underlying = %s", (SYMBOL,))
    cur.execute("DELETE FROM gex_historical_stats WHERE underlying = %s", (SYMBOL,))
    cur.execute("DELETE FROM gex_summary WHERE underlying = %s", (SYMBOL,))
    cur.execute("DELETE FROM underlying_quotes WHERE symbol = %s", (SYMBOL,))
    cur.execute("DELETE FROM symbols WHERE symbol = %s", (SYMBOL,))
    conn.commit()


@pytest.fixture(autouse=True)
def _symbols(monkeypatch):
    from src.alerts import config as alerts_config

    monkeypatch.setattr(alerts_config, "SYMBOLS_RAW", f"{SYMBOL},SPY")


def _rule(conn, user: str, **kw: Any) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO wall_alert_rules
            (end_user, underlying, side, events, min_strength_pctile, profile, channels, enabled)
        VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s::jsonb, %s)
        RETURNING id
        """,
        (
            user,
            kw.get("symbol", SYMBOL),
            kw.get("side", "call"),
            json.dumps(kw.get("events", {"hit": True, "break": True, "hold": False})),
            kw.get("floor", 0),
            kw.get("profile", "balanced"),
            json.dumps(kw.get("channels", {"in_app": True})),
            kw.get("enabled", True),
        ),
    )
    conn.commit()
    return cur.fetchone()[0]


def _event(kind: str = "hit", *, wall: float = 100.0, ordinal: int = 1, side: str = "call"):
    from src.alerts.wall_detector import WallEvent

    bar_ts = _et(10, 15)
    return WallEvent(
        symbol=SYMBOL,
        profile="balanced",
        side=side,
        kind=kind,
        wall=wall,
        ordinal=ordinal,
        session_date=SESSION,
        bar_ts=bar_ts,
        price=99.97,
        last_price=99.95,
        hit_ts=bar_ts,
        hit_price=99.97,
        strength=2.0e9,
        walls_ts=bar_ts,
        break_buffer=0.2,
        touch_band=0.05,
        hold_distance=0.3,
        confirm_minutes=5,
        minutes_since_hit=0.0,
        peak=99.97,
        alerted_hit=True,
        approach_number=1,
    )


def _deliveries(conn) -> List[tuple]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT end_user, channel, status FROM wall_alert_deliveries
        WHERE end_user = ANY(%s) ORDER BY end_user, channel
        """,
        (list(USERS),),
    )
    return cur.fetchall()


# --------------------------------------------------------------------------
# Store
# --------------------------------------------------------------------------


def test_fan_out_matches_filters_and_dedupes(pg):
    from src.alerts.wall_store import record_and_fan_out

    u1, u2, u3, u4 = USERS
    _rule(pg, u1, channels={"in_app": True, "email": True})
    _rule(pg, u1, side="both", channels={"in_app": True})  # same member twice: one row per channel
    _rule(pg, u2, floor=80)  # event is p70: filtered
    _rule(pg, u3, side="put")  # wrong side
    _rule(pg, u4, enabled=False)
    _rule(pg, u4, profile="conservative")  # other profile

    result = record_and_fan_out(
        pg,
        _event(),
        strength_pctile=70.0,
        net_gex_at_spot=1.0e9,
        current_wall=100.0,
        odds=[],
        now=datetime.now(timezone.utc),
    )
    pg.commit()
    assert result.event_id is not None and not result.duplicate
    assert result.rules_matched == 3
    assert result.filtered_by_strength == 1
    assert _deliveries(pg) == [(u1, "email", "queued"), (u1, "in_app", "sent")]

    cur = pg.cursor()
    cur.execute("SELECT payload FROM wall_alert_events WHERE id = %s", (result.event_id,))
    payload = cur.fetchone()[0]
    assert payload["strength_tier"] == "above_typical"
    assert payload["regime"] == "positive"
    assert payload["kind"] == "hit"

    again = record_and_fan_out(
        pg,
        _event(),
        strength_pctile=70.0,
        net_gex_at_spot=1.0e9,
        current_wall=100.0,
        odds=[],
        now=datetime.now(timezone.utc),
    )
    pg.commit()
    assert again.duplicate is True
    assert len(_deliveries(pg)) == 2


def test_unknown_strength_never_satisfies_a_floor(pg):
    from src.alerts.wall_store import record_and_fan_out

    u1, u2 = USERS[:2]
    _rule(pg, u1)  # no floor: delivered
    _rule(pg, u2, floor=50)  # floor + unknown strength: not delivered
    record_and_fan_out(
        pg,
        _event(),
        strength_pctile=None,
        net_gex_at_spot=None,
        current_wall=100.0,
        odds=[],
        now=datetime.now(timezone.utc),
    )
    pg.commit()
    assert _deliveries(pg) == [(u1, "in_app", "sent")]


def test_daily_cap_stops_delivery(pg, monkeypatch):
    from src.alerts import config as alerts_config
    from src.alerts.wall_store import record_and_fan_out

    monkeypatch.setattr(alerts_config, "DAILY_CAP_PER_USER", 1)
    u1 = USERS[0]
    _rule(pg, u1)
    now = datetime.now(timezone.utc)
    first = record_and_fan_out(
        pg,
        _event(),
        strength_pctile=None,
        net_gex_at_spot=None,
        current_wall=None,
        odds=[],
        now=now,
    )
    second = record_and_fan_out(
        pg,
        _event("break"),
        strength_pctile=None,
        net_gex_at_spot=None,
        current_wall=None,
        odds=[],
        now=now,
    )
    pg.commit()
    assert first.deliveries == 1
    assert second.deliveries == 0 and second.capped_users == [u1]


def test_sms_is_never_queued_while_disabled(pg):
    from src.alerts.wall_store import record_and_fan_out

    u1 = USERS[0]
    # A rule saved with SMS on (as if the flag had been enabled) must not
    # build a backlog nobody drains.
    _rule(pg, u1, channels={"in_app": True, "sms": True})
    record_and_fan_out(
        pg,
        _event(),
        strength_pctile=None,
        net_gex_at_spot=None,
        current_wall=None,
        odds=[],
        now=datetime.now(timezone.utc),
    )
    pg.commit()
    assert _deliveries(pg) == [(u1, "in_app", "sent")]


def test_states_round_trip_and_prune(pg):
    from src.alerts.wall_store import load_states, prune, record_and_fan_out, save_states

    save_states(pg, SYMBOL, SESSION, {"balanced": {"version": 1, "x": [1, 2]}})
    pg.commit()
    assert load_states(pg, SYMBOL, SESSION) == {"balanced": {"version": 1, "x": [1, 2]}}
    assert load_states(pg, SYMBOL, SESSION + timedelta(days=1)) == {}

    _rule(pg, USERS[0])
    old = datetime.now(timezone.utc) - timedelta(days=400)
    res = record_and_fan_out(
        pg,
        _event(),
        strength_pctile=None,
        net_gex_at_spot=None,
        current_wall=None,
        odds=[],
        now=old,
    )
    pg.commit()
    assert res.deliveries == 1
    assert prune(pg, 180) >= 1
    pg.commit()
    assert _deliveries(pg) == []


# --------------------------------------------------------------------------
# Service tick, end to end
# --------------------------------------------------------------------------


def _seed_session(conn) -> None:
    """Walls 100 / 90 all morning; price sits at 99, then hits 100 at 10:15."""
    cur = conn.cursor()
    for minute in range(0, 50):
        ts = _et(9, 30) + timedelta(minutes=minute)
        cur.execute(
            """
            INSERT INTO gex_summary
                (underlying, timestamp, max_gamma_strike, max_gamma_value, put_call_ratio,
                 total_net_gex, call_wall, put_wall, call_wall_strength, put_wall_strength,
                 wall_break_buffer, net_gex_at_spot)
            VALUES (%s, %s, 100, 1, 1, 1, 100, 90, 2e9, 1e9, 0.2, 5e8)
            """,
            (SYMBOL, ts),
        )
        if minute < 45:
            cur.execute(
                """
                INSERT INTO underlying_quotes (symbol, timestamp, open, high, low, close)
                VALUES (%s, %s, 99, 99, 99, 99)
                """,
                (SYMBOL, ts),
            )
    conn.commit()


def test_service_tick_alerts_once_and_survives_a_restart(pg, monkeypatch):
    from src.alerts.wall_alert_engine import WallAlertService
    from src.database import connection as dbconn

    @contextlib.contextmanager
    def _db():
        yield pg

    monkeypatch.setattr(dbconn, "db_connection", _db)
    u1 = USERS[0]
    _rule(pg, u1, channels={"in_app": True, "email": True})
    _seed_session(pg)

    service = WallAlertService(symbols=(SYMBOL,))
    # 10:14:30 — the tape so far is 09:30..10:14, all at 99: arms the call wall.
    summary = service.tick(_et(10, 14, 30))
    assert summary.events == 0
    # The 10:15 minute prints a high of 99.98 — within the 5 bp band of 100.
    cur = pg.cursor()
    cur.execute(
        "INSERT INTO underlying_quotes (symbol, timestamp, open, high, low, close)"
        " VALUES (%s, %s, 99, 99.98, 99, 99.9)",
        (SYMBOL, _et(10, 15)),
    )
    pg.commit()
    summary = service.tick(_et(10, 15, 20))
    # Three profiles watch every symbol; the member subscribed to balanced.
    assert summary.events >= 1
    assert sorted(_deliveries(pg)) == [(u1, "email", "queued"), (u1, "in_app", "sent")]

    # The same minute again, and a brand-new service restored from the saved
    # state: neither re-alerts.
    service.tick(_et(10, 15, 40))
    restarted = WallAlertService(symbols=(SYMBOL,))
    restarted.tick(_et(10, 15, 50))
    assert len(_deliveries(pg)) == 2
    cur.execute(
        "SELECT profile, event_type FROM wall_alert_events WHERE underlying = %s ORDER BY profile",
        (SYMBOL,),
    )
    rows = cur.fetchall()
    assert ("balanced", "hit") in rows
    assert len(rows) == len(set(rows))


def test_service_skips_a_symbol_whose_walls_are_stale(pg, monkeypatch):
    from src.alerts.wall_alert_engine import WallAlertService
    from src.database import connection as dbconn

    @contextlib.contextmanager
    def _db():
        yield pg

    monkeypatch.setattr(dbconn, "db_connection", _db)
    _seed_session(pg)
    service = WallAlertService(symbols=(SYMBOL,))
    # Walls end at 10:19; at 11:30 they are 70 minutes old.
    summary = service.tick(_et(11, 30))
    assert summary.skipped.get(SYMBOL) == "walls stale"


# --------------------------------------------------------------------------
# API
# --------------------------------------------------------------------------


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _token(sub: str) -> str:
    now = int(time.time())
    h = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    p = _b64url(json.dumps({"sub": sub, "iat": now, "exp": now + 300}).encode())
    s = _b64url(hmac.new(SECRET.encode(), f"{h}.{p}".encode(), hashlib.sha256).digest())
    return f"{h}.{p}.{s}"


@pytest.fixture()
def api(pg, monkeypatch):
    import asyncpg
    from contextlib import asynccontextmanager

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.api import identity, security
    from src.api.routers import wall_alerts

    monkeypatch.setattr(identity, "_SECRET", SECRET)
    monkeypatch.setattr(security, "_KEY_ADMIN_TOKEN", ADMIN)

    class _Mgr:
        pool = None

    mgr = _Mgr()

    @asynccontextmanager
    async def lifespan(app):
        mgr.pool = await asyncpg.create_pool(_DSN, min_size=1, max_size=2)
        try:
            yield
        finally:
            await mgr.pool.close()

    app = FastAPI(lifespan=lifespan)
    app.include_router(wall_alerts.router)
    app.dependency_overrides[wall_alerts.get_db] = lambda: mgr
    with TestClient(app) as client:
        yield client


def _as(user: str) -> Dict[str, str]:
    return {"X-End-User-Token": _token(user)}


def test_api_rule_lifecycle_and_ownership(api, pg):
    u1, u2 = USERS[:2]
    body = {
        "symbol": SYMBOL.lower(),
        "side": "put",
        "events": {"hit": True, "break": True, "hold": True},
        "min_strength_pctile": 75,
        "profile": "conservative",
        "channels": {"in_app": True, "email": True},
    }
    assert api.post("/api/alerts/walls/rules", json=body).status_code == 401
    created = api.post("/api/alerts/walls/rules", json=body, headers=_as(u1))
    assert created.status_code == 201, created.text
    rule = created.json()["rule"]
    assert rule["symbol"] == SYMBOL and rule["min_strength_pctile"] == 75
    assert rule["channels"] == {"in_app": True, "email": True, "sms": False}

    listed = api.get("/api/alerts/walls/rules", headers=_as(u1)).json()
    assert [r["id"] for r in listed["rules"]] == [rule["id"]]
    assert api.get("/api/alerts/walls/rules", headers=_as(u2)).json()["rules"] == []

    # Another member can neither see, edit nor delete it.
    assert (
        api.patch(
            f"/api/alerts/walls/rules/{rule['id']}", json={"enabled": False}, headers=_as(u2)
        ).status_code
        == 404
    )
    assert api.delete(f"/api/alerts/walls/rules/{rule['id']}", headers=_as(u2)).status_code == 404

    # Turning email off cancels email already queued from this rule.
    from src.alerts.wall_store import record_and_fan_out

    record_and_fan_out(
        pg,
        _event(side="put", wall=90.0),
        strength_pctile=80.0,
        net_gex_at_spot=None,
        current_wall=90.0,
        odds=[],
        now=datetime.now(timezone.utc),
    )
    # The member's profile is conservative; the event is balanced — no rows.
    pg.commit()
    assert _deliveries(pg) == []
    cur = pg.cursor()
    cur.execute("UPDATE wall_alert_rules SET profile = 'balanced' WHERE id = %s", (rule["id"],))
    pg.commit()
    record_and_fan_out(
        pg,
        _event(side="put", wall=90.0, ordinal=2),
        strength_pctile=80.0,
        net_gex_at_spot=None,
        current_wall=90.0,
        odds=[],
        now=datetime.now(timezone.utc),
    )
    pg.commit()
    assert sorted(_deliveries(pg)) == [(u1, "email", "queued"), (u1, "in_app", "sent")]
    patched = api.patch(
        f"/api/alerts/walls/rules/{rule['id']}",
        json={"channels": {"in_app": True, "email": False}},
        headers=_as(u1),
    )
    assert patched.status_code == 200 and patched.json()["cancelled_queued"] == 1
    assert sorted(_deliveries(pg)) == [(u1, "email", "cancelled"), (u1, "in_app", "sent")]

    deleted = api.delete(f"/api/alerts/walls/rules/{rule['id']}", headers=_as(u1))
    assert deleted.status_code == 200
    assert api.get("/api/alerts/walls/rules", headers=_as(u1)).json()["rules"] == []


def test_api_validation_and_limits(api, monkeypatch):
    from src.alerts import config as alerts_config

    u1 = USERS[0]
    bad = [
        ({"symbol": "ES", "side": "call"}, "SPX / NDX"),
        ({"symbol": SYMBOL, "side": "sideways"}, "side"),
        ({"symbol": SYMBOL, "side": "call", "events": {"hit": False}}, "at least one event"),
        ({"symbol": SYMBOL, "side": "call", "channels": {"sms": True}}, "not available"),
        ({"symbol": SYMBOL, "side": "call", "channels": {}}, "at least one way"),
        ({"symbol": SYMBOL, "side": "call", "min_strength_pctile": 99}, "0 to 95"),
        ({"symbol": SYMBOL, "side": "call", "profile": "turbo"}, "profile"),
    ]
    for body, needle in bad:
        res = api.post("/api/alerts/walls/rules", json=body, headers=_as(u1))
        assert res.status_code == 422, body
        assert needle in res.json()["detail"], (body, res.json())

    monkeypatch.setattr(alerts_config, "MAX_RULES_PER_USER", 1)
    ok = {"symbol": SYMBOL, "side": "call"}
    assert api.post("/api/alerts/walls/rules", json=ok, headers=_as(u1)).status_code == 201
    over = api.post("/api/alerts/walls/rules", json=ok, headers=_as(u1))
    assert over.status_code == 409


def test_api_feed_read_and_clear(api, pg):
    from src.alerts.wall_store import record_and_fan_out

    u1, u2 = USERS[:2]
    _rule(pg, u1, channels={"in_app": True})
    record_and_fan_out(
        pg,
        _event(),
        strength_pctile=91.0,
        net_gex_at_spot=-1.0,
        current_wall=100.0,
        odds=[],
        now=datetime.now(timezone.utc),
    )
    pg.commit()
    feed = api.get("/api/alerts/walls/feed", headers=_as(u1)).json()
    assert feed["unread"] == 1 and len(feed["entries"]) == 1
    entry = feed["entries"][0]
    assert entry["kind"] == "hit" and entry["wall"] == 100.0 and entry["read"] is False
    assert entry["payload"]["strength_tier"] == "very_strong"
    assert entry["payload"]["regime"] == "negative"
    assert api.get("/api/alerts/walls/feed", headers=_as(u2)).json()["entries"] == []
    assert api.get("/api/alerts/walls/feed").json() == {
        "entries": [],
        "unread": 0,
        "signed_in": False,
    }

    # Another member cannot mark it read.
    api.post("/api/alerts/walls/feed/read", json={"ids": [entry["id"]]}, headers=_as(u2))
    assert api.get("/api/alerts/walls/feed", headers=_as(u1)).json()["unread"] == 1
    marked = api.post("/api/alerts/walls/feed/read", json={}, headers=_as(u1)).json()
    assert marked["marked"] == 1
    assert api.get("/api/alerts/walls/feed", headers=_as(u1)).json()["unread"] == 0

    cleared = api.delete("/api/alerts/walls/feed", headers=_as(u1)).json()
    assert cleared["deleted"] == 1


def test_api_internal_outbox_requires_admin_and_rechecks_opt_in(api, pg):
    from src.alerts.wall_store import record_and_fan_out

    u1, u2 = USERS[:2]
    keep = _rule(pg, u1, channels={"in_app": True, "email": True})
    drop = _rule(pg, u2, channels={"in_app": True, "email": True})
    record_and_fan_out(
        pg,
        _event(),
        strength_pctile=None,
        net_gex_at_spot=None,
        current_wall=None,
        odds=[],
        now=datetime.now(timezone.utc),
    )
    pg.commit()

    assert api.get("/api/alerts/walls/internal/queued?channel=email").status_code == 403
    assert (
        api.get(
            "/api/alerts/walls/internal/queued?channel=email",
            headers={"X-Admin-Token": "wrong"},
        ).status_code
        == 403
    )
    admin = {"X-Admin-Token": ADMIN}

    # u2 switches email off at the database level after the row queued (as a
    # PATCH from another device would): the worker must never see it.
    cur = pg.cursor()
    cur.execute(
        "UPDATE wall_alert_rules SET channels = '{\"in_app\": true}'::jsonb WHERE id = %s", (drop,)
    )
    pg.commit()
    queued = api.get("/api/alerts/walls/internal/queued?channel=email", headers=admin).json()
    assert [e["end_user"] for e in queued["entries"]] == [u1]
    assert queued["cancelled"] == 1
    row_id = queued["entries"][0]["id"]
    assert queued["entries"][0]["payload"]["kind"] == "hit"

    # retry keeps it queued until the attempts run out.
    for _ in range(2):
        api.post(
            "/api/alerts/walls/internal/mark",
            json={"ids": [row_id], "status": "retry", "error": "429"},
            headers=admin,
        )
    assert [
        e["id"]
        for e in api.get("/api/alerts/walls/internal/queued?channel=email", headers=admin).json()[
            "entries"
        ]
    ] == [row_id]
    sent = api.post(
        "/api/alerts/walls/internal/mark", json={"ids": [row_id], "status": "sent"}, headers=admin
    ).json()
    assert sent["updated"] == 1
    # A repeated mark cannot move a row that already left the queue.
    again = api.post(
        "/api/alerts/walls/internal/mark", json={"ids": [row_id], "status": "failed"}, headers=admin
    ).json()
    assert again["updated"] == 0

    # One-click unsubscribe: email off on every rule the member has.
    off = api.post(
        "/api/alerts/walls/internal/disable-channel",
        json={"end_user": u1, "channel": "email"},
        headers=admin,
    ).json()
    assert off["rules_updated"] == 1
    cur.execute("SELECT channels FROM wall_alert_rules WHERE id = %s", (keep,))
    assert cur.fetchone()[0] == {"in_app": True, "email": False}

    purged = api.post(
        "/api/alerts/walls/internal/purge-user", json={"end_user": u1}, headers=admin
    ).json()
    assert purged["rules_deleted"] == 1


def test_api_expires_stale_queue_rows(api, pg):
    from src.alerts.wall_store import record_and_fan_out

    _rule(pg, USERS[0], channels={"in_app": True, "email": True})
    record_and_fan_out(
        pg,
        _event(),
        strength_pctile=None,
        net_gex_at_spot=None,
        current_wall=None,
        odds=[],
        now=datetime.now(timezone.utc) - timedelta(hours=2),
    )
    pg.commit()
    res = api.get(
        "/api/alerts/walls/internal/queued?channel=email", headers={"X-Admin-Token": ADMIN}
    ).json()
    assert res["entries"] == [] and res["expired"] == 1


def test_api_live_reports_strength_percentile(api, pg):
    cur = pg.cursor()
    ts = datetime.now(timezone.utc).replace(microsecond=0)
    cur.execute(
        """
        INSERT INTO gex_summary
            (underlying, timestamp, max_gamma_strike, max_gamma_value, put_call_ratio,
             total_net_gex, call_wall, put_wall, call_wall_strength, put_wall_strength,
             wall_break_buffer, net_gex_at_spot)
        VALUES (%s, %s, 100, 1, 1, 1, 101, 95, 3e9, 5e8, 0.2, -2e8)
        """,
        (SYMBOL, ts),
    )
    cur.execute(
        "INSERT INTO underlying_quotes (symbol, timestamp, open, high, low, close)"
        " VALUES (%s, %s, 99, 99, 99, 99)",
        (SYMBOL, ts),
    )
    for metric in ("call_wall_strength", "put_wall_strength"):
        cur.execute(
            """
            INSERT INTO gex_historical_stats
                (underlying, metric, window_label, tod_bucket, p05, p25, p50, p75, p95, sample_size)
            VALUES (%s, %s, '30d', -1, 1e8, 5e8, 1e9, 2e9, 4e9, 500)
            """,
            (SYMBOL, metric),
        )
    pg.commit()
    live = api.get(f"/api/alerts/walls/live?symbols={SYMBOL}").json()
    (row,) = live["symbols"]
    assert row["regime"] == "negative" and row["stale"] is False
    assert row["call"]["strike"] == 101.0
    assert row["call"]["distance"] == pytest.approx(2.0)
    # 3e9 sits halfway between p75 (2e9) and p95 (4e9): the 85th percentile.
    assert row["call"]["strength_pctile"] == pytest.approx(85.0)
    assert row["call"]["strength_tier"] == "strong"
    assert row["put"]["strength_tier"] == "moderate"


def test_api_config_lists_profiles_and_channels(api):
    cfg = api.get("/api/alerts/walls/config").json()
    assert SYMBOL in cfg["symbols"]
    assert [p["name"] for p in cfg["profiles"]] == ["responsive", "balanced", "conservative"]
    channels = {c["id"]: c["available"] for c in cfg["channels"]}
    assert channels == {"in_app": True, "email": True, "sms": False}
