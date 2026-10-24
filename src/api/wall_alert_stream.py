"""Wall alerts as a stream — every recorded alert, filtered by the caller.

The member endpoints (``/rules``, ``/feed``) answer "what did I ask to be
sent". This module answers "hand me every wall alert matching this filter",
for code that wants the alerts without a saved rule: a trading bot, a chat
relay, a spreadsheet. Two shapes over the one table every alert is written
to, ``wall_alert_events``:

* ``GET /api/alerts/walls/events`` — a cursor page, for polling and backfill
  (:func:`fetch_page`).
* ``GET /api/alerts/walls/stream`` — Server-Sent Events: one ``wall_alert``
  event per alert as it is recorded, resumable with ``Last-Event-ID``
  (:func:`serve`).

The filter is the subscription. Its fields mean what a rule's do (symbols,
side, events, noise filter, touch tolerance, strength floor), so a stream and
a rule set the same way hear the same alerts. Two rule guards do not apply:
the per-member daily cap (a stream has no member to count against) and the
email worker's 20-minute expiry.

How alerts reach a stream
-------------------------
The detector (``src/alerts/wall_alert_engine.py``) is its own process, so the
only surface it shares with the API is Postgres. Each API worker runs one
:class:`WallAlertTail`, which polls ``wall_alert_events`` for ids past its
cursor every :data:`POLL_SECONDS`, only while a stream is open on that
worker, and offers each new row to every open stream. That is one indexed
query a second per worker however many streams are open.

Why poll rather than LISTEN/NOTIFY, which the quote stream uses: a wall alert
fires a few times an hour per symbol, so a second of latency is invisible.
Polling needs no change to the detector, cannot lose an alert when a listener
connection drops, and resumes from an id.

Why an id cursor is safe: ids come from one sequence, and the detector is the
only writer (an advisory lock keeps a second copy on standby) and commits
each event before recording the next. Ids therefore become visible in order,
so no row commits behind a cursor that has already passed it. The replay tool
(``make wall-alerts-replay``) is read-only.

Nothing stale is in the table to stream: the detector drops an event found on
a bar older than ``WALL_ALERTS_MAX_EVENT_AGE_SECONDS`` (a catch-up after a
restart or a feed gap) before recording it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import threading
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Set, Tuple

from src.alerts import config as alerts_config

logger = logging.getLogger(__name__)

#: How often the tail looks for new alerts while a stream is open.
POLL_SECONDS = 1.0
#: Rows read per poll; a full page is followed at once by the next.
POLL_BATCH = 500
#: A comment line this often keeps a quiet stream open through proxies
#: (nginx's 120 s read timeout, a load balancer's idle timeout) and lets the
#: server notice a client that went away.
HEARTBEAT_SECONDS = 15.0
#: The SSE ``retry`` field: how long a client waits before reconnecting.
RETRY_MS = 5000
#: Alerts a stream may fall behind by before it is closed. The client
#: reconnects with ``Last-Event-ID`` and catches up from the table.
QUEUE_MAX = 1000
#: Alerts a reconnect replays. A cursor further behind gets a ``gap`` event
#: and the live stream; ``/events`` pages through the rest.
BACKFILL_MAX = 500
#: How long a new stream waits for the tail to find its place (else 503).
READY_TIMEOUT_SECONDS = 10.0
#: A stream that has not checked in for this long is dropped by the tail —
#: the backstop for one whose response never started, so it never ran its
#: own cleanup.
STALE_SECONDS = HEARTBEAT_SECONDS * 4
#: Failures to read the table are logged at most this often.
_ERROR_LOG_SECONDS = 60.0

_COLUMNS = (
    "id, underlying, side, event_type, wall_strike, price, profile, touch_bp, "
    "strength_pctile, payload, detected_at"
)


@dataclass(frozen=True)
class EventFilter:
    """Which alerts a stream or page carries.

    Built from query parameters by ``wall_alerts.parse_event_filter``, which
    validates every field; this class only applies it.
    """

    symbols: Tuple[str, ...]
    sides: Tuple[str, ...]
    kinds: Tuple[str, ...]
    profile: str
    #: The detector stream's touch tolerance in basis points: the one chosen,
    #: or the profile's own band under Auto.
    touch_bp: int
    #: The caller left the tolerance on Auto.
    auto_touch: bool
    min_strength_pctile: int

    def matches(self, entry: Dict[str, Any]) -> bool:
        """The same test the fan-out applies to a rule (``wall_store``)."""
        if (
            entry["symbol"] not in self.symbols
            or entry["side"] not in self.sides
            or entry["kind"] not in self.kinds
            or entry["profile"] != self.profile
        ):
            return False
        # Rows written before touch tolerances existed carry 0; each ran at
        # its profile's own band, which is what Auto means.
        if entry["touch_bp"] != self.touch_bp and not (self.auto_touch and entry["touch_bp"] == 0):
            return False
        if self.min_strength_pctile > 0:
            # "Only strong walls" is never satisfied by "we could not tell".
            pctile = entry["strength_pctile"]
            if pctile is None or pctile < self.min_strength_pctile:
                return False
        return True

    def as_dict(self) -> Dict[str, Any]:
        """The filter as the caller would write it, echoed back to them."""
        return {
            "symbols": list(self.symbols),
            "side": self.sides[0] if len(self.sides) == 1 else "both",
            "events": list(self.kinds),
            "profile": self.profile,
            "touch_bp": None if self.auto_touch else self.touch_bp,
            "min_strength_pctile": self.min_strength_pctile,
        }


def event_entry(row: Any) -> Dict[str, Any]:
    """One ``wall_alert_events`` row as the stream and ``/events`` serve it."""
    payload = row["payload"]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            payload = {}
    pctile = row["strength_pctile"]
    detected = row["detected_at"]
    return {
        "id": int(row["id"]),
        "detected_at": detected.isoformat() if detected is not None else None,
        "symbol": row["underlying"],
        "side": row["side"],
        "kind": row["event_type"],
        "wall": float(row["wall_strike"]),
        "price": float(row["price"]),
        "profile": row["profile"],
        "touch_bp": int(row["touch_bp"]),
        # Unrounded, as the fan-out compares it with a rule's floor; the
        # payload carries the rounded figure for display.
        "strength_pctile": None if pctile is None else float(pctile),
        "payload": payload if isinstance(payload, dict) else {},
    }


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------


@dataclass
class Page:
    entries: List[Dict[str, Any]]
    #: The newest alert id in the table when the page was read.
    head: int
    #: More matching alerts follow this page.
    has_more: bool

    @property
    def next_after_id(self) -> int:
        """Where the next page starts.

        After the last alert returned while more follow; otherwise after
        everything the table held, so a filter that rarely matches does not
        rescan the same rows on every poll.
        """
        if self.has_more and self.entries:
            return int(self.entries[-1]["id"])
        return self.head


async def fetch_page(conn: Any, flt: EventFilter, *, after_id: Optional[int], limit: int) -> Page:
    """Alerts matching ``flt``, oldest first.

    With ``after_id``: up to ``limit`` alerts recorded after it. Without: the
    newest ``limit``, with nothing after them by definition. Both reads share
    one REPEATABLE READ snapshot, so ``head`` and the rows agree: every
    matching alert up to ``head`` is in this page or, when ``has_more``,
    after it.
    """
    where = """
        e.underlying = ANY($1::text[])
        AND e.side = ANY($2::text[])
        AND e.event_type = ANY($3::text[])
        AND e.profile = $4::text
        AND (e.touch_bp = $5::smallint OR ($6::boolean AND e.touch_bp = 0))
        AND ($7::int = 0 OR e.strength_pctile >= $7::int)
    """
    args = [
        list(flt.symbols),
        list(flt.sides),
        list(flt.kinds),
        flt.profile,
        flt.touch_bp,
        flt.auto_touch,
        flt.min_strength_pctile,
    ]
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        head = int(await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM wall_alert_events"))
        if after_id is None:
            rows = await conn.fetch(
                f"""
                SELECT {_COLUMNS} FROM wall_alert_events e
                WHERE {where}
                ORDER BY e.id DESC
                LIMIT $8::int
                """,
                *args,
                limit,
            )
            return Page([event_entry(r) for r in reversed(rows)], head, False)
        rows = await conn.fetch(
            f"""
            SELECT {_COLUMNS} FROM wall_alert_events e
            WHERE {where} AND e.id > $8::bigint
            ORDER BY e.id
            LIMIT $9::int
            """,
            *args,
            after_id,
            limit + 1,
        )
    has_more = len(rows) > limit
    return Page([event_entry(r) for r in rows[:limit]], head, has_more)


# ---------------------------------------------------------------------------
# The tail
# ---------------------------------------------------------------------------


class StreamLimit(Exception):
    """The caller, or the worker, already holds the most streams allowed."""


class StreamUnavailable(Exception):
    """The tail could not read the table to find its place."""


class Subscriber:
    """One open stream's end of the tail."""

    def __init__(self, flt: EventFilter, subject: str) -> None:
        self.filter = flt
        self.subject = subject
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
        #: Every alert recorded after this id is offered to the stream. Set
        #: by the tail, with no await between it and the next offer.
        self.start_after: Optional[int] = None
        #: Why the tail let go of this stream ("overflow", "stale"), if it did.
        self.closed: Optional[str] = None
        self.touched = time.monotonic()

    def offer(self, entry: Dict[str, Any]) -> None:
        if self.closed is not None or not self.filter.matches(entry):
            return
        try:
            self.queue.put_nowait(entry)
        except asyncio.QueueFull:
            self.closed = "overflow"

    def close(self, reason: str) -> None:
        """End the stream now, waking it if it is waiting for an alert."""
        if self.closed is None:
            self.closed = reason
        try:
            self.queue.put_nowait(None)
        except asyncio.QueueFull:
            pass  # a full queue wakes it anyway

    def touch(self) -> None:
        self.touched = time.monotonic()


class WallAlertTail:
    """Polls ``wall_alert_events`` while streams are open; fans rows out.

    One per API worker. The polling task starts with the first stream and
    stops after the last one closes, so an idle worker issues no queries.

    ``end_on_shutdown`` (the API's own tail) ends every open stream the moment
    the worker is told to stop. uvicorn's graceful shutdown waits for open
    responses to finish before it exits, and a stream never finishes on its
    own, so without this a restart with one stream open would sit out
    systemd's ``TimeoutStopSec`` (60 s) with the API down. uvicorn tells the
    app nothing until after that wait, so the tail puts its own handler in
    front of uvicorn's for SIGTERM and SIGINT. Clients reconnect to the new
    process after ``retry`` and resume from their last id.
    """

    def __init__(
        self, *, poll_seconds: float = POLL_SECONDS, end_on_shutdown: bool = False
    ) -> None:
        self._poll_seconds = poll_seconds
        self._end_on_shutdown = end_on_shutdown
        self._hooked = False
        self._stopping = False
        self._get_pool: Optional[Callable[[], Any]] = None
        self._subs: Set[Subscriber] = set()
        self._task: Optional[asyncio.Task] = None
        self._ready: Optional[asyncio.Event] = None
        #: The id of the last alert offered (None until the task has read the
        #: table's head).
        self.position: Optional[int] = None
        self._last_error_log = 0.0

    @property
    def open_streams(self) -> int:
        return len(self._subs)

    def close_all(self, reason: str) -> None:
        """End every open stream; each releases its slot as it ends."""
        for sub in list(self._subs):
            sub.close(reason)

    def stop(self) -> None:
        """The worker is shutting down: end every stream and open no more."""
        self._stopping = True
        self.close_all("shutdown")

    def _hook_shutdown(self) -> None:
        """Chain :meth:`stop` in front of the process's stop signals.

        Installed once, from the worker's event loop. Signal handlers can only
        be set from the main thread, which is where uvicorn runs the app.
        """
        if self._hooked or threading.current_thread() is not threading.main_thread():
            return
        self._hooked = True
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous = signal.getsignal(signum)

            def handler(received: int, frame: Any, previous: Any = previous) -> None:
                try:
                    loop.call_soon_threadsafe(self.stop)
                except RuntimeError:
                    pass  # the loop is already closed
                if callable(previous):
                    previous(received, frame)
                elif previous == signal.SIG_DFL:
                    signal.signal(received, signal.SIG_DFL)
                    signal.raise_signal(received)

            signal.signal(signum, handler)

    async def subscribe(
        self, flt: EventFilter, subject: str, get_pool: Callable[[], Any]
    ) -> Subscriber:
        """Open a stream for ``subject``; returns once alerts will reach it.

        ``get_pool`` is the request's database: the tail reads through
        whatever the app is wired to rather than holding its own handle.

        :raises StreamLimit: the caller or the worker is at its stream cap.
        :raises StreamUnavailable: the table could not be read in time, or the
            worker is shutting down.
        """
        if self._stopping:
            raise StreamUnavailable("The API is restarting. Reconnect in a few seconds.")
        if len(self._subs) >= alerts_config.STREAM_MAX_PER_WORKER:
            raise StreamLimit("The alert stream is at capacity. Try again in a minute.")
        held = sum(1 for s in self._subs if s.subject == subject)
        if held >= alerts_config.STREAM_MAX_PER_CALLER:
            raise StreamLimit(
                f"You already have {held} alert streams open; close one to open another."
            )
        if self._end_on_shutdown:
            self._hook_shutdown()
        self._get_pool = get_pool
        sub = Subscriber(flt, subject)
        self._subs.add(sub)
        if self._task is None:
            self.position = None
            self._ready = asyncio.Event()
            self._task = asyncio.create_task(self._run(), name="wall-alert-tail")
        elif self.position is not None:
            sub.start_after = self.position
        ready = self._ready
        assert ready is not None
        try:
            await asyncio.wait_for(ready.wait(), READY_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            self.unsubscribe(sub)
            raise StreamUnavailable("Wall alerts are temporarily unavailable. Try again shortly.")
        except BaseException:
            self.unsubscribe(sub)
            raise
        return sub

    def unsubscribe(self, sub: Subscriber) -> None:
        self._subs.discard(sub)

    def _pool(self) -> Any:
        pool = self._get_pool() if self._get_pool is not None else None
        if pool is None:
            raise RuntimeError("database not initialized")
        return pool

    async def _run(self) -> None:
        failures = 0
        try:
            while self._subs:
                try:
                    if self.position is None:
                        await self._find_head()
                    full = await self._poll_once()
                    failures = 0
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 — the tail must outlive a DB blip
                    failures += 1
                    self._log_failure(exc)
                    await asyncio.sleep(min(self._poll_seconds * 2 ** min(failures, 5), 30.0))
                    continue
                self._prune()
                if not full:
                    await asyncio.sleep(self._poll_seconds)
        finally:
            # Reached only with no streams left (or at shutdown), and nothing
            # awaits between the loop's check and here, so a stream that
            # subscribes from now on starts a fresh task.
            self._task = None

    async def _find_head(self) -> None:
        async with self._pool().acquire() as conn:
            head = int(await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM wall_alert_events"))
        self.position = head
        for sub in self._subs:
            if sub.start_after is None:
                sub.start_after = head
        assert self._ready is not None
        self._ready.set()

    async def _poll_once(self) -> bool:
        """Offer alerts recorded since the last poll; True if the page was full."""
        async with self._pool().acquire() as conn:
            rows = await conn.fetch(
                f"SELECT {_COLUMNS} FROM wall_alert_events WHERE id > $1 ORDER BY id LIMIT $2",
                self.position,
                POLL_BATCH,
            )
        if not rows:
            return False
        # Offer and advance with no await between, so a stream that joined
        # during the fetch above is offered this batch, and one that joins
        # after it starts from the new position.
        for row in rows:
            entry = event_entry(row)
            for sub in list(self._subs):
                sub.offer(entry)
        self.position = int(rows[-1]["id"])
        return len(rows) == POLL_BATCH

    def _prune(self) -> None:
        cutoff = time.monotonic() - STALE_SECONDS
        for sub in [s for s in self._subs if s.closed is not None or s.touched < cutoff]:
            if sub.closed is None:
                sub.closed = "stale"
            self._subs.discard(sub)

    def _log_failure(self, exc: Exception) -> None:
        now = time.monotonic()
        if now - self._last_error_log >= _ERROR_LOG_SECONDS:
            self._last_error_log = now
            logger.warning("Wall alert stream: could not read new alerts (%s); retrying", exc)


# ---------------------------------------------------------------------------
# Server-Sent Events
# ---------------------------------------------------------------------------


def sse_frame(event: str, data: Dict[str, Any], *, event_id: Optional[int] = None) -> str:
    """One SSE event. ``data`` is compact JSON, so it is always one line."""
    head = f"id: {event_id}\n" if event_id is not None else ""
    body = json.dumps(data, separators=(",", ":"), default=str)
    return f"{head}event: {event}\ndata: {body}\n\n"


async def serve(
    tail: WallAlertTail,
    sub: Subscriber,
    get_pool: Callable[[], Any],
    cursor: Optional[int],
) -> AsyncIterator[str]:
    """The body of one stream. Always releases ``sub``, however it ends.

    Order on the wire: ``retry``, then (with a cursor) the alerts recorded
    after it, then ``ready``, then live alerts and keepalive comments.
    ``ready`` carries an ``id`` too, so a client that reconnects before any
    alert arrives still resumes from where it joined.
    """
    try:
        yield f"retry: {RETRY_MS}\n\n"
        assert sub.start_after is not None
        sent = sub.start_after
        if cursor is not None:
            try:
                async with get_pool().acquire() as conn:
                    page = await fetch_page(conn, sub.filter, after_id=cursor, limit=BACKFILL_MAX)
            except Exception as exc:  # noqa: BLE001 — say so, then go live
                logger.warning("Wall alert stream: replay after %s failed (%s)", cursor, exc)
                page = None
            if page is None or page.has_more:
                # Too far behind (or the replay failed): go live, and say which
                # alerts were not replayed so the client can page /events.
                yield sse_frame(
                    "gap",
                    {
                        "after_id": cursor,
                        "resumed_after_id": sent,
                        "detail": (
                            "Alerts between these ids were not replayed. "
                            "Fetch them from /api/alerts/walls/events?after_id=…"
                        ),
                    },
                )
            else:
                for entry in page.entries:
                    yield sse_frame("wall_alert", entry, event_id=entry["id"])
                # Every matching alert up to the head was just replayed, which
                # covers whatever the tail queued below it.
                sent = max(sent, page.head)
        yield sse_frame("ready", {"after_id": sent, "filter": sub.filter.as_dict()}, event_id=sent)
        while sub.closed is None:
            sub.touch()
            try:
                entry = await asyncio.wait_for(sub.queue.get(), HEARTBEAT_SECONDS)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
                continue
            if entry is None or entry["id"] <= sent:
                continue  # None: woken to end (see Subscriber.close)
            sent = entry["id"]
            yield sse_frame("wall_alert", entry, event_id=sent)
    finally:
        tail.unsubscribe(sub)
