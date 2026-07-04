"""Small HTTP wrapper around the Beehiiv Publications API.

Two operations we need:

  * :meth:`BeehiivClient.create_post` — POST /publications/{pub}/posts to
    create a scheduled post carrying our rendered HTML body. Free /
    premium audience is a per-call flag.
  * :meth:`BeehiivClient.confirm_post` — PATCH /publications/{pub}/posts/{id}
    with ``status=confirmed`` for publications that need a separate
    confirm step. The daily job's ``--confirm`` flag toggles this on.
  * :meth:`BeehiivClient.create_subscription` — POST /publications/{pub}/subscriptions
    for the frontend /api/newsletter/subscribe route to reuse if we ever
    move that call server-side (today the frontend calls Beehiiv directly).

Design constraints (matching the Move-1 cron-job patterns):

  * **No new dependencies.** Uses ``urllib.request`` like the tweet jobs
    already do — the deploy target is a single-instance EC2 box, and one
    fewer wheel to install is one fewer thing that can break a release.
  * **Retries.** 3 attempts with 2s / 4s / 8s exponential backoff on
    5xx and 429; every other 4xx is a permanent-failure fast-fail.
  * **Structured logging.** Every attempt logs at INFO the URL + status
    + Beehiiv's ``X-Request-Id`` response header so ops can grep by
    request id when Beehiiv support asks for one.
  * **Redacted secrets.** The bearer never gets logged, even on error
    paths; the ``__repr__`` masks it.
  * **Timeout 15s.** Matches the tweet jobs' timeout — long enough for
    a scheduled post to serialize on Beehiiv's side, short enough that
    a hung connection doesn't wedge the timer.

The API contract Beehiiv publishes at https://developers.beehiiv.com/
is authoritative — this module encodes the fields we send today, and
callers are expected to hand over exactly the payload Beehiiv accepts.
If Beehiiv adds a required field we don't know about, they'll return
422 and the operator sees it in journalctl on the first cron fire.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

logger = logging.getLogger("zerogex.beehiiv_client")

DEFAULT_BASE_URL = "https://api.beehiiv.com/v2"
DEFAULT_TIMEOUT_SECONDS = 15
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_SECONDS = (2.0, 4.0, 8.0)
_USER_AGENT = "zerogex-oa-beehiiv/1.0"


class BeehiivError(RuntimeError):
    """Any non-retryable Beehiiv API failure. Carries the HTTP status +
    the parsed error body when the response was JSON, otherwise the raw
    text so the caller can log the right thing."""

    def __init__(
        self,
        message: str,
        *,
        status: Optional[int] = None,
        body: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.body = body
        self.request_id = request_id


@dataclass
class BeehiivResponse:
    """Parsed successful response — the JSON payload + Beehiiv's request
    id for reconciliation. Callers pull ``data`` out for the fields they
    care about; the request id is captured separately because we log it
    on every attempt (success + failure) and the caller may want it too."""

    status: int
    data: dict[str, Any]
    request_id: Optional[str] = None


class BeehiivClient:
    """Thin wrapper around Beehiiv's REST API.

    One instance per job invocation is fine — there's no shared state
    across calls other than the bearer and publication id, and the
    stdlib's ``urllib`` opens fresh connections per request anyway.
    """

    def __init__(
        self,
        *,
        api_key: str,
        publication_id: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        backoff_seconds: tuple[float, ...] = DEFAULT_BACKOFF_SECONDS,
        sleep: Any = time.sleep,
        opener: Any = urlopen,
    ) -> None:
        if not api_key:
            raise ValueError("BeehiivClient: api_key is required")
        if not publication_id:
            raise ValueError("BeehiivClient: publication_id is required")
        self._api_key = api_key
        self._publication_id = publication_id
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout_seconds
        self._max_attempts = max(1, int(max_attempts))
        self._backoff = backoff_seconds
        self._sleep = sleep
        self._opener = opener

    def __repr__(self) -> str:
        return (
            "BeehiivClient(publication_id="
            f"{self._publication_id!r}, api_key='***')"
        )

    # ------------------------------------------------------------------
    # High-level operations
    # ------------------------------------------------------------------

    def create_post(self, payload: dict[str, Any]) -> BeehiivResponse:
        """POST /publications/{pub}/posts. Returns the parsed response.

        ``payload`` is the JSON body Beehiiv accepts — the caller is
        responsible for setting ``title``, ``subtitle``, ``body_html``,
        ``status`` (``draft`` or ``confirmed``), ``scheduled_at``,
        ``audience`` (``free`` / ``premium`` / ``all``), ``subject_line``,
        etc. We don't second-guess what Beehiiv accepts; the operator
        can adjust the job's payload if the API contract changes.
        """
        path = f"/publications/{self._publication_id}/posts"
        return self._request("POST", path, json_body=payload)

    def confirm_post(self, post_id: str) -> BeehiivResponse:
        """PATCH the post to ``status=confirmed`` — used when the
        publication isn't configured to auto-send scheduled posts."""
        if not post_id:
            raise ValueError("BeehiivClient.confirm_post: post_id is required")
        path = f"/publications/{self._publication_id}/posts/{post_id}"
        return self._request("PATCH", path, json_body={"status": "confirmed"})

    def create_subscription(self, payload: dict[str, Any]) -> BeehiivResponse:
        """POST /publications/{pub}/subscriptions — server-side newsletter
        signup path. Client-side subscriptions go through the Next.js
        /api/newsletter/subscribe route on the web side; this method
        exists so the wrapper is complete for future server-side callers
        (e.g. importing an existing list, resubscribing on account
        recovery)."""
        path = f"/publications/{self._publication_id}/subscriptions"
        return self._request("POST", path, json_body=payload)

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Optional[dict[str, Any]] = None,
    ) -> BeehiivResponse:
        url = f"{self._base_url}{path}"
        body: Optional[bytes] = None
        if json_body is not None:
            body = json.dumps(json_body).encode("utf-8")

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Accept": "application/json",
            "User-Agent": _USER_AGENT,
        }
        if body is not None:
            headers["Content-Type"] = "application/json"

        last_exc: Optional[Exception] = None
        for attempt in range(1, self._max_attempts + 1):
            req = Request(url, data=body, headers=headers, method=method)
            try:
                resp = self._opener(req, timeout=self._timeout)
            except HTTPError as exc:
                request_id = _extract_request_id(exc.headers)
                raw_body = _safe_read_body(exc)
                logger.warning(
                    "beehiiv %s %s attempt=%d status=%s request_id=%s body=%s",
                    method, path, attempt, exc.code, request_id, _truncate(raw_body),
                )
                if _is_retryable_status(exc.code) and attempt < self._max_attempts:
                    last_exc = exc
                    self._sleep(self._pick_backoff(attempt))
                    continue
                raise BeehiivError(
                    f"Beehiiv {method} {path} failed with HTTP {exc.code}",
                    status=exc.code,
                    body=raw_body,
                    request_id=request_id,
                ) from exc
            except URLError as exc:
                logger.warning(
                    "beehiiv %s %s attempt=%d transport error: %s",
                    method, path, attempt, exc,
                )
                if attempt < self._max_attempts:
                    last_exc = exc
                    self._sleep(self._pick_backoff(attempt))
                    continue
                raise BeehiivError(
                    f"Beehiiv {method} {path} failed with transport error: {exc}",
                ) from exc

            with resp:
                request_id = _extract_request_id(resp.headers)
                status = getattr(resp, "status", None) or resp.getcode() or 200
                raw = resp.read().decode("utf-8", errors="replace")
                data: dict[str, Any] = {}
                if raw:
                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        logger.warning(
                            "beehiiv %s %s returned non-JSON body: %s",
                            method, path, _truncate(raw),
                        )
                        parsed = {}
                    if isinstance(parsed, dict):
                        data = parsed
                    else:
                        # Beehiiv wraps most responses in {"data": {...}};
                        # a top-level list still counts as a valid payload.
                        data = {"data": parsed}
                logger.info(
                    "beehiiv %s %s attempt=%d status=%d request_id=%s",
                    method, path, attempt, status, request_id,
                )
                return BeehiivResponse(status=status, data=data, request_id=request_id)

        # Loop exited without a return — the last retry raised HTTPError
        # or URLError. The raise inside the loop covers this; this line is
        # a defensive backstop.
        raise BeehiivError(
            f"Beehiiv {method} {path} exhausted {self._max_attempts} attempts",
        ) from last_exc

    def _pick_backoff(self, attempt: int) -> float:
        """Pick the delay before the ``attempt``-th retry. ``attempt`` is
        1-indexed and always ≥ 1 by the caller; if the tuple is shorter
        than max_attempts we reuse the last entry (defensive — the
        default tuple lines up with the default attempt count)."""
        idx = min(attempt - 1, len(self._backoff) - 1)
        return self._backoff[idx] if self._backoff else 0.0


# ----------------------------------------------------------------------
# Helpers — kept module-level so tests can call them directly.
# ----------------------------------------------------------------------


def _is_retryable_status(status: Optional[int]) -> bool:
    """5xx and 429 retry; every other 4xx is permanent (bad payload,
    missing publication, etc.)."""
    if status is None:
        return False
    if status == 429:
        return True
    return 500 <= status < 600


def _extract_request_id(headers: Any) -> Optional[str]:
    """Read Beehiiv's request id out of the response headers.

    ``http.client.HTTPMessage`` supports case-insensitive lookup via
    ``.get``; older tests hand in a plain dict — try both."""
    if headers is None:
        return None
    getter = getattr(headers, "get", None)
    if callable(getter):
        for key in ("X-Request-Id", "X-Request-ID", "x-request-id"):
            val = getter(key)
            if val:
                return str(val)
    return None


def _safe_read_body(exc: HTTPError) -> str:
    """Read the response body off an HTTPError without letting a bad
    encoding throw. Small bodies only — we log truncated in the caller."""
    try:
        raw = exc.read() or b""
    except Exception:  # noqa: BLE001
        return ""
    return raw.decode("utf-8", errors="replace")


def _truncate(text: str, limit: int = 500) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "…"
