"""OAuth1-signed client for the X (Twitter) v1.1 media/upload endpoint.

Split out of :mod:`src.jobs.bulletin_tweet` so the tweet job stays
readable and the crypto has one place to be audited. Uses stdlib
``hmac`` + ``hashlib`` so no new third-party dependency lands.

Why v1.1: the v2 tweet endpoint accepts media by ID, but the v2 media
upload endpoint is behind a separate access tier we don't have. v1.1
``media/upload.json`` still works, is authenticated via OAuth1
user-context, and returns a ``media_id_string`` that plugs straight
into ``POST /2/tweets`` — the same shape :func:`bulletin_tweet.
post_tweet_via_x_api` already handles.

Two modes:

  * **Simple upload** — one POST, multipart/form-data, ``media``
    field. Works for images up to 5 MB (PNG/JPEG/GIF).
  * **Chunked upload** — INIT / APPEND / FINALIZE dance, required
    for video and for images > 5 MB. Selected automatically based on
    file size + media_category.

Credentials:
  X_BOT_API_KEY, X_BOT_API_SECRET,
  X_BOT_ACCESS_TOKEN, X_BOT_ACCESS_TOKEN_SECRET
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger("zerogex.x_media_client")

X_MEDIA_UPLOAD_URL = "https://upload.twitter.com/1.1/media/upload.json"

# Chunked upload threshold: X requires it for video, and per docs for
# PNG > ~5 MB. We flip at 4 MB to stay comfortably inside the simple-
# upload envelope.
CHUNKED_UPLOAD_THRESHOLD_BYTES = 4 * 1024 * 1024
CHUNK_BYTES = 4 * 1024 * 1024  # 4 MB slices — X caps at 5 MB per chunk

# Best-effort status-poll ceiling for chunked video processing. Real
# X waits are typically < 15 s; we cap at 60 s so a wedged upload
# never blocks the systemd job past the next fire.
STATUS_POLL_MAX_SECONDS = 60
STATUS_POLL_INTERVAL_SECONDS = 2


class MissingCredentialsError(RuntimeError):
    """Raised when one or more of the four OAuth1 secrets is unset."""


@dataclass(frozen=True)
class OAuth1Credentials:
    """The four secrets that identify the bot on the v1.1 endpoint.

    Kept immutable so the same instance can be reused across multiple
    media uploads within a single tweet fire without any risk of
    an intermediate mutation being picked up mid-signature."""

    consumer_key: str
    consumer_secret: str
    access_token: str
    access_token_secret: str


def load_credentials_from_env() -> OAuth1Credentials:
    """Read the four OAuth1 env vars, raising a typed error if any is missing.

    We check *all four* even after the first miss so the operator sees
    a single log line naming every gap, not one whack-a-mole failure
    per run."""
    env_names = (
        "X_BOT_API_KEY",
        "X_BOT_API_SECRET",
        "X_BOT_ACCESS_TOKEN",
        "X_BOT_ACCESS_TOKEN_SECRET",
    )
    missing = [name for name in env_names if not os.environ.get(name, "").strip()]
    if missing:
        raise MissingCredentialsError(
            f"X media upload requires the following env vars: {', '.join(missing)}"
        )
    return OAuth1Credentials(
        consumer_key=os.environ["X_BOT_API_KEY"].strip(),
        consumer_secret=os.environ["X_BOT_API_SECRET"].strip(),
        access_token=os.environ["X_BOT_ACCESS_TOKEN"].strip(),
        access_token_secret=os.environ["X_BOT_ACCESS_TOKEN_SECRET"].strip(),
    )


# ---------------------------------------------------------------------------
# OAuth1 signing (RFC 5849)
# ---------------------------------------------------------------------------


def _percent_encode(value: str) -> str:
    """OAuth1's percent-encoding: RFC 3986 unreserved chars only.

    ``urllib.parse.quote(safe='')`` is close, but by default it leaves
    ``/`` alone; OAuth1 requires it encoded. Reserved chars ``!*'()``
    also need explicit encoding under the OAuth1 spec, which
    urllib does by default (they're not in ``safe``)."""
    return quote(value, safe="~")


def _build_signature(
    method: str,
    url: str,
    params: dict[str, str],
    credentials: OAuth1Credentials,
) -> str:
    """Compute the OAuth1 HMAC-SHA1 signature over the request.

    Follows RFC 5849 §3.4 verbatim:
      1. Percent-encode every key & value.
      2. Sort by (key, value).
      3. Concatenate with '&'.
      4. Base string = method + '&' + encoded(url) + '&' + encoded(params).
      5. Signing key = encoded(consumer_secret) + '&' + encoded(token_secret).
      6. HMAC-SHA1(signing_key, base_string), then base64-encode.
    """
    encoded_pairs = sorted(
        (_percent_encode(k), _percent_encode(v)) for k, v in params.items()
    )
    param_string = "&".join(f"{k}={v}" for k, v in encoded_pairs)
    base = "&".join(
        [
            method.upper(),
            _percent_encode(url),
            _percent_encode(param_string),
        ]
    )
    signing_key = "&".join(
        [
            _percent_encode(credentials.consumer_secret),
            _percent_encode(credentials.access_token_secret),
        ]
    )
    digest = hmac.new(
        signing_key.encode("utf-8"),
        base.encode("utf-8"),
        hashlib.sha1,
    ).digest()
    return base64.b64encode(digest).decode("ascii")


def _oauth1_header(
    method: str,
    url: str,
    credentials: OAuth1Credentials,
    extra_params: dict[str, str] | None = None,
) -> str:
    """Build the ``Authorization: OAuth …`` header for one request.

    ``extra_params`` is the union of the request's query-string params
    and any x-www-form-urlencoded body params — both feed into the
    signature base string per RFC 5849 §3.4.1.3. For multipart POSTs
    (the media/upload simple + INIT/APPEND/FINALIZE calls that use
    application/x-www-form-urlencoded), we pass the form fields here."""
    oauth_params = {
        "oauth_consumer_key": credentials.consumer_key,
        "oauth_nonce": secrets.token_hex(16),
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_token": credentials.access_token,
        "oauth_version": "1.0",
    }
    sig_params = dict(oauth_params)
    if extra_params:
        sig_params.update(extra_params)
    oauth_params["oauth_signature"] = _build_signature(
        method, url, sig_params, credentials,
    )
    header = "OAuth " + ", ".join(
        f'{_percent_encode(k)}="{_percent_encode(v)}"'
        for k, v in sorted(oauth_params.items())
    )
    return header


# ---------------------------------------------------------------------------
# Multipart body builder — minimal, stdlib-only
# ---------------------------------------------------------------------------


def _build_multipart_body(
    fields: dict[str, str],
    file_field: str | None = None,
    file_bytes: bytes | None = None,
    filename: str | None = None,
    file_mime: str | None = None,
) -> tuple[bytes, str]:
    """Assemble a ``multipart/form-data`` body from plain fields + one file.

    Returns ``(body_bytes, boundary)``. We generate a random boundary
    long enough that a collision with the payload is astronomically
    unlikely. All text fields are UTF-8 encoded; file bytes are passed
    through verbatim."""
    boundary = "----zerogex" + secrets.token_hex(16)
    lines: list[bytes] = []
    for name, value in fields.items():
        lines.append(f"--{boundary}".encode("ascii"))
        lines.append(
            f'Content-Disposition: form-data; name="{name}"'.encode("utf-8")
        )
        lines.append(b"")
        lines.append(value.encode("utf-8"))
    if file_field and file_bytes is not None:
        lines.append(f"--{boundary}".encode("ascii"))
        lines.append(
            (
                f'Content-Disposition: form-data; name="{file_field}"; '
                f'filename="{filename or "upload.bin"}"'
            ).encode("utf-8")
        )
        lines.append(
            f"Content-Type: {file_mime or 'application/octet-stream'}".encode("ascii")
        )
        lines.append(b"")
        lines.append(file_bytes)
    lines.append(f"--{boundary}--".encode("ascii"))
    lines.append(b"")
    return b"\r\n".join(lines), boundary


# ---------------------------------------------------------------------------
# Upload primitives
# ---------------------------------------------------------------------------


def _mime_for(path: Path, override: str | None) -> str:
    if override:
        return override
    ext = path.suffix.lower()
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".mp4": "video/mp4",
        ".mov": "video/quicktime",
    }.get(ext, "application/octet-stream")


def _category_for(mime: str) -> str:
    if mime.startswith("video/"):
        return "tweet_video"
    if mime == "image/gif":
        return "tweet_gif"
    return "tweet_image"


def _simple_upload(
    path: Path,
    credentials: OAuth1Credentials,
    mime: str,
    timeout_seconds: int,
) -> str | None:
    body_bytes = path.read_bytes()
    fields = {
        "media_category": _category_for(mime),
    }
    body, boundary = _build_multipart_body(
        fields=fields,
        file_field="media",
        file_bytes=body_bytes,
        filename=path.name,
        file_mime=mime,
    )
    # Multipart bodies are NOT signed (their fields aren't in the
    # signature base string — RFC 5849 §3.4.1.3.1 requires
    # application/x-www-form-urlencoded to be signed; multipart is
    # explicitly excluded). Sign only the URL/method + query-string
    # bits (of which we have none here).
    auth_header = _oauth1_header("POST", X_MEDIA_UPLOAD_URL, credentials)
    req = Request(
        X_MEDIA_UPLOAD_URL,
        data=body,
        headers={
            "Authorization": auth_header,
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "User-Agent": "zerogex-x-media/1.0",
        },
        method="POST",
    )
    with urlopen(req, timeout=timeout_seconds) as resp:
        payload = json.loads(resp.read().decode("utf-8", errors="replace") or "{}")
    return payload.get("media_id_string") or (
        str(payload["media_id"]) if payload.get("media_id") else None
    )


def _chunked_upload(
    path: Path,
    credentials: OAuth1Credentials,
    mime: str,
    timeout_seconds: int,
) -> str | None:
    """INIT → APPEND (many) → FINALIZE, with STATUS polling for video.

    The APPEND step is multipart (the chunk bytes need a file field);
    INIT and FINALIZE are application/x-www-form-urlencoded which
    means their params DO go into the signature base string."""
    total_bytes = path.stat().st_size
    if total_bytes == 0:
        return None

    # ---- INIT -------------------------------------------------------
    init_params = {
        "command": "INIT",
        "media_type": mime,
        "total_bytes": str(total_bytes),
        "media_category": _category_for(mime),
    }
    media_id = _post_form(X_MEDIA_UPLOAD_URL, init_params, credentials, timeout_seconds)
    if not media_id:
        return None

    # ---- APPEND -----------------------------------------------------
    segment_index = 0
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(CHUNK_BYTES)
            if not chunk:
                break
            append_fields = {
                "command": "APPEND",
                "media_id": media_id,
                "segment_index": str(segment_index),
            }
            body, boundary = _build_multipart_body(
                fields=append_fields,
                file_field="media",
                file_bytes=chunk,
                filename=f"chunk-{segment_index}",
                file_mime="application/octet-stream",
            )
            # Multipart isn't signed; only query-string params (none here)
            # feed the signature.
            auth_header = _oauth1_header(
                "POST", X_MEDIA_UPLOAD_URL, credentials,
            )
            req = Request(
                X_MEDIA_UPLOAD_URL,
                data=body,
                headers={
                    "Authorization": auth_header,
                    "Content-Type": f"multipart/form-data; boundary={boundary}",
                    "User-Agent": "zerogex-x-media/1.0",
                },
                method="POST",
            )
            with urlopen(req, timeout=timeout_seconds) as resp:
                resp.read()  # APPEND returns 204 with empty body
            segment_index += 1

    # ---- FINALIZE ---------------------------------------------------
    finalize_params = {"command": "FINALIZE", "media_id": media_id}
    finalize_resp = _post_form_json(
        X_MEDIA_UPLOAD_URL, finalize_params, credentials, timeout_seconds,
    )
    if finalize_resp is None:
        return None

    # ---- STATUS (only for async processing, i.e. video) ------------
    processing = finalize_resp.get("processing_info")
    if processing is not None:
        _await_processing(media_id, credentials, timeout_seconds)

    return media_id


def _post_form(
    url: str,
    params: dict[str, str],
    credentials: OAuth1Credentials,
    timeout_seconds: int,
) -> str | None:
    """POST an application/x-www-form-urlencoded body, return media_id_string."""
    payload = _post_form_json(url, params, credentials, timeout_seconds)
    if payload is None:
        return None
    return payload.get("media_id_string") or (
        str(payload["media_id"]) if payload.get("media_id") else None
    )


def _post_form_json(
    url: str,
    params: dict[str, str],
    credentials: OAuth1Credentials,
    timeout_seconds: int,
) -> dict[str, Any] | None:
    body = urlencode(params).encode("utf-8")
    auth_header = _oauth1_header("POST", url, credentials, extra_params=params)
    req = Request(
        url,
        data=body,
        headers={
            "Authorization": auth_header,
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "zerogex-x-media/1.0",
        },
        method="POST",
    )
    with urlopen(req, timeout=timeout_seconds) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw) if raw else {}


def _await_processing(
    media_id: str,
    credentials: OAuth1Credentials,
    timeout_seconds: int,
) -> None:
    """Poll the STATUS command until X reports processing succeeded.

    Videos go through async transcoding; the finalize response
    includes ``processing_info.state`` = ``pending``|``in_progress``,
    and only when it reaches ``succeeded`` is the media_id usable in
    a tweet. We poll every 2s up to STATUS_POLL_MAX_SECONDS then give
    up (returning silently — the caller will discover the failure
    when the tweet POST is rejected)."""
    deadline = time.monotonic() + STATUS_POLL_MAX_SECONDS
    while time.monotonic() < deadline:
        params = {"command": "STATUS", "media_id": media_id}
        url = f"{X_MEDIA_UPLOAD_URL}?{urlencode(params)}"
        auth_header = _oauth1_header("GET", X_MEDIA_UPLOAD_URL, credentials, extra_params=params)
        req = Request(
            url,
            headers={
                "Authorization": auth_header,
                "User-Agent": "zerogex-x-media/1.0",
            },
        )
        with urlopen(req, timeout=timeout_seconds) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        payload = json.loads(raw) if raw else {}
        pi = payload.get("processing_info") or {}
        state = pi.get("state")
        if state in ("succeeded", None):
            return
        if state == "failed":
            logger.warning(
                "bulletin_tweet: X reported media processing failed (media_id=%s): %s",
                media_id, pi.get("error"),
            )
            return
        time.sleep(STATUS_POLL_INTERVAL_SECONDS)
    logger.warning(
        "bulletin_tweet: X media processing did not finish within %ds (media_id=%s)",
        STATUS_POLL_MAX_SECONDS, media_id,
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def upload_media(
    path: Path,
    credentials: OAuth1Credentials,
    mime_type: str | None = None,
    timeout_seconds: int = 60,
) -> str | None:
    """Upload one file to X and return its ``media_id_string``.

    Auto-dispatches to simple vs chunked upload based on file size +
    MIME. Returns None (and logs a warning) if the upload fails so
    the caller can continue with a text-only tweet."""
    if not path.exists():
        logger.warning("bulletin_tweet: media file %s does not exist", path)
        return None

    mime = _mime_for(path, mime_type)
    size = path.stat().st_size
    use_chunked = size >= CHUNKED_UPLOAD_THRESHOLD_BYTES or mime.startswith("video/")

    try:
        if use_chunked:
            return _chunked_upload(path, credentials, mime, timeout_seconds)
        return _simple_upload(path, credentials, mime, timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        logger.warning("bulletin_tweet: media upload of %s failed (%s)", path, exc)
        return None
