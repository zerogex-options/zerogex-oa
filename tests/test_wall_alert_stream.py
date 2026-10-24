"""The wall alert stream and events page, without a database.

What a filter lets through, how the tail hands rows to open streams, the
stream's wire format and resume rules, and how the routes are mounted. The
SQL behind ``fetch_page`` is proven against a real Postgres in
``tests/test_wall_alerts_db.py`` (integration).
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from src.alerts import config as alerts_config
from src.api import wall_alert_stream as stream


def _filter(**kw: Any) -> stream.EventFilter:
    base: Dict[str, Any] = dict(
        symbols=("SPY", "SPX"),
        sides=("call", "put"),
        kinds=("hit", "break", "hold"),
        profile="balanced",
        touch_bp=5,
        auto_touch=True,
        min_strength_pctile=0,
    )
    base.update(kw)
    return stream.EventFilter(**base)


def _row(id: int, **kw: Any) -> Dict[str, Any]:
    base: Dict[str, Any] = dict(
        id=id,
        underlying="SPY",
        side="put",
        event_type="hit",
        wall_strike=772.0,
        price=772.31,
        profile="balanced",
        touch_bp=5,
        strength_pctile=81.25,
        payload=json.dumps({"v": 1, "strength_tier": "strong"}),
        detected_at=datetime(2026, 10, 23, 14, 5, tzinfo=timezone.utc),
    )
    base.update(kw)
    return base


def _entry(id: int, **kw: Any) -> Dict[str, Any]:
    return stream.event_entry(_row(id, **kw))


# --------------------------------------------------------------------------
# The filter
# --------------------------------------------------------------------------


def test_an_entry_carries_the_row_and_its_decoded_payload():
    entry = _entry(7)
    assert entry == {
        "id": 7,
        "detected_at": "2026-10-23T14:05:00+00:00",
        "symbol": "SPY",
        "side": "put",
        "kind": "hit",
        "wall": 772.0,
        "price": 772.31,
        "profile": "balanced",
        "touch_bp": 5,
        "strength_pctile": 81.25,
        "payload": {"v": 1, "strength_tier": "strong"},
    }


@pytest.mark.parametrize(
    "change,expected",
    [
        ({}, True),
        ({"underlying": "QQQ"}, False),
        ({"event_type": "hold"}, True),
        ({"profile": "responsive"}, False),
        ({"touch_bp": 10}, False),
        # Rows from before touch tolerances ran at the profile's own band.
        ({"touch_bp": 0}, True),
    ],
)
def test_a_filter_hears_one_detector_stream(change, expected):
    assert _filter().matches(_entry(1, **change)) is expected


def test_a_chosen_tolerance_does_not_match_old_rows():
    flt = _filter(touch_bp=10, auto_touch=False)
    assert flt.matches(_entry(1, touch_bp=10))
    assert not flt.matches(_entry(1, touch_bp=0))


def test_side_and_kind_narrow_the_filter():
    flt = _filter(sides=("call",), kinds=("break",))
    assert not flt.matches(_entry(1))
    assert flt.matches(_entry(1, side="call", event_type="break"))


def test_unknown_strength_never_passes_a_floor():
    assert _filter(min_strength_pctile=0).matches(_entry(1, strength_pctile=None))
    assert not _filter(min_strength_pctile=50).matches(_entry(1, strength_pctile=None))
    assert _filter(min_strength_pctile=81).matches(_entry(1, strength_pctile=81.25))
    assert not _filter(min_strength_pctile=82).matches(_entry(1, strength_pctile=81.25))


def test_the_filter_is_echoed_as_the_caller_writes_it():
    assert _filter(sides=("put",), touch_bp=10, auto_touch=False).as_dict() == {
        "symbols": ["SPY", "SPX"],
        "side": "put",
        "events": ["hit", "break", "hold"],
        "profile": "balanced",
        "touch_bp": 10,
        "min_strength_pctile": 0,
    }
    assert _filter().as_dict()["side"] == "both"
    assert _filter().as_dict()["touch_bp"] is None


def _parse(**kw: Any) -> stream.EventFilter:
    from src.api.routers.wall_alerts import event_filter

    args: Dict[str, Any] = dict(
        symbols=None,
        side="both",
        events=None,
        profile="balanced",
        touch_bp=None,
        min_strength_pctile=0,
    )
    args.update(kw)
    return event_filter(**args)


def test_the_default_filter_is_every_symbol_side_and_event_on_balanced_auto():
    flt = _parse()
    assert flt.symbols == alerts_config.symbols()
    assert flt.sides == ("call", "put")
    assert flt.kinds == ("hit", "break", "hold")
    assert (flt.profile, flt.touch_bp, flt.auto_touch) == ("balanced", 5, True)


def test_the_query_string_is_normalized():
    flt = _parse(symbols="spx, spy,SPX", side="Put", events="break,HIT", profile="Conservative")
    assert flt.symbols == ("SPX", "SPY")
    assert flt.sides == ("put",)
    assert flt.kinds == ("hit", "break")
    # Auto resolves to the profile's own band: conservative runs at 3 bp.
    assert (flt.profile, flt.touch_bp, flt.auto_touch) == ("conservative", 3, True)
    assert _parse(touch_bp="10").touch_bp == 10
    assert _parse(touch_bp="auto").auto_touch is True


@pytest.mark.parametrize(
    "kw,needle",
    [
        ({"symbols": "SPY,ES"}, "use SPX / NDX"),
        ({"symbols": "TSLA"}, "symbols must be among"),
        ({"side": "up"}, "side must be"),
        ({"events": "hit,touch"}, "unknown event(s): touch"),
        ({"profile": "fast"}, "profile must be"),
        ({"touch_bp": "7"}, "touch_bp must be"),
    ],
)
def test_a_bad_filter_is_refused_with_a_reason(kw, needle):
    with pytest.raises(HTTPException) as exc:
        _parse(**kw)
    assert exc.value.status_code == 422
    assert needle in exc.value.detail


def test_last_event_id_wins_over_after_id():
    from src.api.routers.wall_alerts import _resume_cursor

    assert _resume_cursor(None, None) is None
    assert _resume_cursor(None, 40) == 40
    assert _resume_cursor(" 52 ", 40) == 52
    assert _resume_cursor("", 40) == 40
    for bad in ("abc", "-1", "1.5", "²"):
        with pytest.raises(HTTPException):
            _resume_cursor(bad, None)


def test_a_page_resumes_after_its_last_row_only_while_more_follow():
    assert stream.Page([_entry(3), _entry(9)], head=20, has_more=True).next_after_id == 9
    # Nothing more matched up to the head: skip the rows that did not match.
    assert stream.Page([_entry(3)], head=20, has_more=False).next_after_id == 20
    assert stream.Page([], head=20, has_more=False).next_after_id == 20


def test_an_sse_frame_is_one_event_with_one_data_line():
    frame = stream.sse_frame("wall_alert", {"a": "two\nlines", "b": 1}, event_id=12)
    assert frame == 'id: 12\nevent: wall_alert\ndata: {"a":"two\\nlines","b":1}\n\n'
    assert stream.sse_frame("gap", {}) == "event: gap\ndata: {}\n\n"


# --------------------------------------------------------------------------
# The tail
# --------------------------------------------------------------------------


class _Table:
    """``wall_alert_events`` as the tail reads it: a head and an id range."""

    def __init__(self, rows: Optional[List[Dict[str, Any]]] = None) -> None:
        self.rows = list(rows or [])
        self.polls = 0
        self.fail = False

    def acquire(self):
        table = self

        class _Ctx:
            async def __aenter__(self):
                if table.fail:
                    raise ConnectionError("database down")
                return _Conn(table)

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


class _Conn:
    def __init__(self, table: _Table) -> None:
        self.table = table

    async def fetchval(self, sql: str, *args: Any) -> int:
        return max((r["id"] for r in self.table.rows), default=0)

    async def fetch(self, sql: str, after: int, limit: int) -> List[Dict[str, Any]]:
        self.table.polls += 1
        return [r for r in self.table.rows if r["id"] > after][:limit]


async def _until(predicate, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


def test_a_stream_starts_after_the_head_and_hears_only_its_filter():
    async def scenario():
        table = _Table([_row(1), _row(2)])
        tail = stream.WallAlertTail(poll_seconds=0.01)
        spy = await tail.subscribe(_filter(symbols=("SPY",)), "caller:a", lambda: table)
        spx = await tail.subscribe(_filter(symbols=("SPX",)), "caller:b", lambda: table)
        assert (spy.start_after, spx.start_after) == (2, 2)
        table.rows += [_row(3), _row(4, underlying="SPX"), _row(5)]
        await _until(lambda: tail.position == 5)
        assert [spy.queue.get_nowait()["id"] for _ in range(spy.queue.qsize())] == [3, 5]
        assert [spx.queue.get_nowait()["id"] for _ in range(spx.queue.qsize())] == [4]
        # A stream that joins a running tail starts where the tail is.
        late = await tail.subscribe(_filter(), "caller:c", lambda: table)
        assert late.start_after == 5
        for sub in (spy, spx, late):
            tail.unsubscribe(sub)
        await _until(lambda: tail._task is None)

    asyncio.run(scenario())


def test_the_tail_stops_polling_when_the_last_stream_closes():
    async def scenario():
        table = _Table([_row(1)])
        tail = stream.WallAlertTail(poll_seconds=0.01)
        sub = await tail.subscribe(_filter(), "caller:a", lambda: table)
        await _until(lambda: table.polls >= 2)
        tail.unsubscribe(sub)
        await _until(lambda: tail._task is None)
        polls = table.polls
        await asyncio.sleep(0.05)
        assert table.polls == polls
        # The next stream starts a fresh tail from the current head.
        table.rows.append(_row(2))
        again = await tail.subscribe(_filter(), "caller:a", lambda: table)
        assert again.start_after == 2
        tail.unsubscribe(again)

    asyncio.run(scenario())


def test_stream_caps_are_per_caller_and_per_worker(monkeypatch):
    monkeypatch.setattr(alerts_config, "STREAM_MAX_PER_CALLER", 2)
    monkeypatch.setattr(alerts_config, "STREAM_MAX_PER_WORKER", 3)

    async def scenario():
        table = _Table()
        tail = stream.WallAlertTail(poll_seconds=0.01)
        subs = [await tail.subscribe(_filter(), "caller:a", lambda: table) for _ in range(2)]
        with pytest.raises(stream.StreamLimit, match="already have 2"):
            await tail.subscribe(_filter(), "caller:a", lambda: table)
        subs.append(await tail.subscribe(_filter(), "caller:b", lambda: table))
        with pytest.raises(stream.StreamLimit, match="capacity"):
            await tail.subscribe(_filter(), "caller:c", lambda: table)
        tail.unsubscribe(subs[0])
        subs.append(await tail.subscribe(_filter(), "caller:a", lambda: table))
        for sub in subs:
            tail.unsubscribe(sub)

    asyncio.run(scenario())


def test_a_stream_is_refused_when_the_table_cannot_be_read(monkeypatch):
    monkeypatch.setattr(stream, "READY_TIMEOUT_SECONDS", 0.05)

    async def scenario():
        table = _Table()
        table.fail = True
        tail = stream.WallAlertTail(poll_seconds=0.01)
        with pytest.raises(stream.StreamUnavailable):
            await tail.subscribe(_filter(), "caller:a", lambda: table)
        assert tail.open_streams == 0
        await _until(lambda: tail._task is None)

    asyncio.run(scenario())


def test_the_tail_rides_out_a_database_blip():
    async def scenario():
        table = _Table([_row(1)])
        tail = stream.WallAlertTail(poll_seconds=0.001)
        sub = await tail.subscribe(_filter(), "caller:a", lambda: table)
        table.fail = True
        await asyncio.sleep(0.02)
        table.fail = False
        table.rows.append(_row(2))
        await _until(lambda: sub.queue.qsize() == 1)
        assert sub.queue.get_nowait()["id"] == 2
        tail.unsubscribe(sub)

    asyncio.run(scenario())


def test_a_stream_that_falls_behind_is_closed_not_buffered_forever(monkeypatch):
    monkeypatch.setattr(stream, "QUEUE_MAX", 2)

    async def scenario():
        table = _Table()
        tail = stream.WallAlertTail(poll_seconds=0.01)
        sub = await tail.subscribe(_filter(), "caller:a", lambda: table)
        table.rows += [_row(i) for i in (1, 2, 3)]
        await _until(lambda: tail.open_streams == 0)
        assert sub.closed == "overflow"
        assert sub.queue.qsize() == 2

    asyncio.run(scenario())


def test_a_stream_that_stops_checking_in_is_dropped(monkeypatch):
    monkeypatch.setattr(stream, "STALE_SECONDS", 0.02)

    async def scenario():
        table = _Table()
        tail = stream.WallAlertTail(poll_seconds=0.005)
        sub = await tail.subscribe(_filter(), "caller:a", lambda: table)
        await _until(lambda: tail.open_streams == 0)
        assert sub.closed == "stale"

    asyncio.run(scenario())


# --------------------------------------------------------------------------
# The stream body
# --------------------------------------------------------------------------


def _parse_frames(chunks: List[str]) -> List[Dict[str, Any]]:
    frames = []
    for chunk in chunks:
        if chunk.startswith(":"):
            frames.append({"comment": chunk.strip()})
            continue
        frame: Dict[str, Any] = {}
        for line in chunk.strip().split("\n"):
            key, _, value = line.partition(": ")
            frame[key] = json.loads(value) if key == "data" else value
        frames.append(frame)
    return frames


async def _take(gen, n: int) -> List[str]:
    return [await gen.__anext__() for _ in range(n)]


def test_a_fresh_stream_goes_live_from_where_it_joined(monkeypatch):
    monkeypatch.setattr(stream, "HEARTBEAT_SECONDS", 0.2)

    async def scenario():
        table = _Table([_row(1)])
        tail = stream.WallAlertTail(poll_seconds=0.005)
        sub = await tail.subscribe(_filter(), "caller:a", lambda: table)
        gen = stream.serve(tail, sub, lambda: table, cursor=None)
        head = _parse_frames(await _take(gen, 2))
        assert head[0] == {"retry": "5000"}
        assert head[1]["event"] == "ready" and head[1]["id"] == "1"
        assert head[1]["data"]["after_id"] == 1
        assert head[1]["data"]["filter"]["profile"] == "balanced"
        table.rows += [_row(2), _row(3, underlying="QQQ"), _row(4)]
        live = _parse_frames(await _take(gen, 2))
        assert [(f["event"], f["id"], f["data"]["id"]) for f in live] == [
            ("wall_alert", "2", 2),
            ("wall_alert", "4", 4),
        ]
        assert _parse_frames(await _take(gen, 1)) == [{"comment": ": keepalive"}]
        await gen.aclose()
        assert tail.open_streams == 0

    asyncio.run(scenario())


def test_a_reconnect_replays_what_it_missed_then_goes_live(monkeypatch):
    replayed = stream.Page([_entry(5), _entry(8)], head=9, has_more=False)
    seen: Dict[str, Any] = {}

    async def fake_page(conn, flt, *, after_id, limit):
        seen.update(after_id=after_id, limit=limit)
        return replayed

    monkeypatch.setattr(stream, "fetch_page", fake_page)

    async def scenario():
        table = _Table([_row(i) for i in range(1, 8)])
        tail = stream.WallAlertTail(poll_seconds=0.005)
        sub = await tail.subscribe(_filter(), "caller:a", lambda: table)
        assert sub.start_after == 7
        # Recorded while the replay ran: 8 is in the replay, 10 is not.
        sub.offer(_entry(8))
        sub.offer(_entry(10))
        frames = _parse_frames(await _take(stream.serve(tail, sub, lambda: table, cursor=4), 5))
        assert seen == {"after_id": 4, "limit": stream.BACKFILL_MAX}
        assert [(f.get("event"), f.get("id")) for f in frames] == [
            (None, None),
            ("wall_alert", "5"),
            ("wall_alert", "8"),
            ("ready", "9"),
            ("wall_alert", "10"),
        ]
        tail.unsubscribe(sub)

    asyncio.run(scenario())


def test_a_reconnect_too_far_behind_gets_a_gap_and_the_live_stream(monkeypatch):
    async def fake_page(conn, flt, *, after_id, limit):
        return stream.Page([_entry(i) for i in range(1, limit + 1)], head=900, has_more=True)

    monkeypatch.setattr(stream, "fetch_page", fake_page)

    async def scenario():
        table = _Table([_row(880)])
        tail = stream.WallAlertTail(poll_seconds=0.005)
        sub = await tail.subscribe(_filter(), "caller:a", lambda: table)
        frames = _parse_frames(await _take(stream.serve(tail, sub, lambda: table, cursor=3), 3))
        assert frames[1]["event"] == "gap"
        assert frames[1]["data"]["after_id"] == 3
        assert frames[1]["data"]["resumed_after_id"] == 880
        assert frames[2]["event"] == "ready" and frames[2]["id"] == "880"
        tail.unsubscribe(sub)

    asyncio.run(scenario())


def test_a_failed_replay_is_reported_as_a_gap_not_a_dead_stream(monkeypatch):
    async def broken_page(conn, flt, *, after_id, limit):
        raise ConnectionError("database down")

    monkeypatch.setattr(stream, "fetch_page", broken_page)

    async def scenario():
        table = _Table([_row(4)])
        tail = stream.WallAlertTail(poll_seconds=0.005)
        sub = await tail.subscribe(_filter(), "caller:a", lambda: table)
        frames = _parse_frames(await _take(stream.serve(tail, sub, lambda: table, cursor=1), 3))
        assert [f.get("event") for f in frames] == [None, "gap", "ready"]
        tail.unsubscribe(sub)

    asyncio.run(scenario())


def test_a_stream_the_tail_let_go_of_ends_and_releases_its_slot():
    async def scenario():
        table = _Table()
        tail = stream.WallAlertTail(poll_seconds=0.005)
        sub = await tail.subscribe(_filter(), "caller:a", lambda: table)
        sub.closed = "overflow"
        chunks = [chunk async for chunk in stream.serve(tail, sub, lambda: table, cursor=None)]
        assert [f.get("event") for f in _parse_frames(chunks)] == [None, "ready"]
        assert tail.open_streams == 0

    asyncio.run(scenario())


def test_stopping_ends_waiting_streams_at_once_and_opens_no_more():
    async def scenario():
        table = _Table()
        tail = stream.WallAlertTail(poll_seconds=0.005)
        sub = await tail.subscribe(_filter(), "caller:a", lambda: table)
        gen = stream.serve(tail, sub, lambda: table, cursor=None)
        await _take(gen, 2)
        # The stream is waiting on its queue with a 15 s heartbeat; stopping
        # must end it now, not at the next heartbeat.
        pending = asyncio.ensure_future(gen.__anext__())
        await asyncio.sleep(0.01)
        tail.stop()
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(pending, 1.0)
        assert tail.open_streams == 0
        with pytest.raises(stream.StreamUnavailable, match="restarting"):
            await tail.subscribe(_filter(), "caller:a", lambda: table)

    asyncio.run(scenario())


def test_a_stop_signal_ends_the_streams_and_still_reaches_the_server():
    import signal

    received: List[int] = []
    saved = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}

    async def scenario():
        table = _Table()
        tail = stream.WallAlertTail(poll_seconds=0.005, end_on_shutdown=True)
        sub = await tail.subscribe(_filter(), "caller:a", lambda: table)
        signal.raise_signal(signal.SIGTERM)
        await _until(lambda: sub.closed == "shutdown")
        assert received == [signal.SIGTERM]
        tail.unsubscribe(sub)

    try:
        # Stand-in for uvicorn's handler, which the tail must chain to.
        for s in saved:
            signal.signal(s, lambda signum, frame: received.append(signum))
        asyncio.run(scenario())
    finally:
        for s, handler in saved.items():
            signal.signal(s, handler)


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------


@pytest.fixture()
def app_module(monkeypatch):
    for name in ("API_KEY", "ENVIRONMENT", "CORS_ALLOW_ORIGINS", "END_USER_TOKEN_SECRET"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("API_KEY", "static-key")
    monkeypatch.setenv("ENVIRONMENT", "development")
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)
    from src.api import database as dbmod

    dbmod.DatabaseManager.connect = AsyncMock(return_value=None)
    dbmod.DatabaseManager.disconnect = AsyncMock(return_value=None)
    from src.api import main as main_mod

    yield main_mod
    for mod in list(sys.modules):
        if mod.startswith("src.api"):
            sys.modules.pop(mod, None)


def test_events_and_stream_are_mounted_on_v1_and_v2(app_module):
    from tests.test_wall_alerts_units import _mounted_paths

    paths = _mounted_paths(app_module.app)
    for path in (
        "/api/alerts/walls/events",
        "/api/alerts/walls/stream",
        "/api/v2/alerts/walls/events",
        "/api/v2/alerts/walls/stream",
    ):
        assert path in paths, path


def test_the_stream_needs_an_api_key_and_a_valid_filter(app_module):
    from fastapi.testclient import TestClient

    with TestClient(app_module.app) as client:
        assert client.get("/api/alerts/walls/stream").status_code == 401
        assert client.get("/api/alerts/walls/events").status_code == 401
        headers = {"Authorization": "Bearer static-key"}
        res = client.get("/api/alerts/walls/stream?symbols=ES", headers=headers)
        assert res.status_code == 422 and "SPX / NDX" in res.json()["detail"]
        res = client.get(
            "/api/alerts/walls/stream", headers={**headers, "Last-Event-ID": "not-an-id"}
        )
        assert res.status_code == 422


def test_a_caller_past_the_stream_cap_gets_429(app_module, monkeypatch):
    from fastapi.testclient import TestClient

    monkeypatch.setattr(alerts_config, "STREAM_MAX_PER_WORKER", 0)
    with TestClient(app_module.app) as client:
        res = client.get("/api/alerts/walls/stream", headers={"Authorization": "Bearer static-key"})
    assert res.status_code == 429
    assert res.headers["retry-after"] == "60"
