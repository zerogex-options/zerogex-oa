"""Tests for src.services.beehiiv_client.

Pin down the retry contract that matters operationally:

  * 5xx and 429 → retry with exponential backoff, up to 3 attempts.
  * every other 4xx → fast-fail without retry (bad payload / missing pub).
  * URLError transport failures → retry.
  * Every attempt logs the request path + status + X-Request-Id header
    so ops can grep by request id when Beehiiv support asks.
  * The bearer never appears in the module's ``__repr__`` or logs.
"""

from __future__ import annotations

import io
import json
import logging
from typing import Any, Optional
from unittest.mock import MagicMock
from urllib.error import HTTPError, URLError

import pytest

from src.services.beehiiv_client import (
    BeehiivClient,
    BeehiivError,
    _extract_request_id,
    _is_retryable_status,
)


class _FakeResponse:
    """Fake ``urlopen`` return value that mimics the parts of
    ``http.client.HTTPResponse`` the client actually reads."""

    def __init__(self, status: int, body: dict[str, Any] | list | str, request_id: str | None = None):
        self.status = status
        if isinstance(body, (dict, list)):
            self._body = json.dumps(body).encode("utf-8")
        else:
            self._body = str(body).encode("utf-8")
        self.headers = {"X-Request-Id": request_id} if request_id else {}

    def getcode(self) -> int:
        return self.status

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _make_http_error(status: int, body: str = "", request_id: str | None = None) -> HTTPError:
    hdrs = {}
    if request_id:
        hdrs["X-Request-Id"] = request_id
    return HTTPError(
        url="https://api.beehiiv.com/v2/x", code=status,
        msg="err", hdrs=hdrs, fp=io.BytesIO(body.encode("utf-8")),
    )


def test_repr_masks_api_key():
    client = BeehiivClient(api_key="super-secret", publication_id="pub_123")
    r = repr(client)
    assert "super-secret" not in r
    assert "***" in r
    assert "pub_123" in r


def test_missing_api_key_rejected():
    with pytest.raises(ValueError):
        BeehiivClient(api_key="", publication_id="pub_123")


def test_missing_publication_id_rejected():
    with pytest.raises(ValueError):
        BeehiivClient(api_key="k", publication_id="")


def test_is_retryable_status_5xx_and_429():
    assert _is_retryable_status(500) is True
    assert _is_retryable_status(502) is True
    assert _is_retryable_status(429) is True
    assert _is_retryable_status(400) is False
    assert _is_retryable_status(401) is False
    assert _is_retryable_status(404) is False
    assert _is_retryable_status(422) is False


def test_extract_request_id_case_insensitive():
    assert _extract_request_id({"X-Request-Id": "abc"}) == "abc"
    assert _extract_request_id({"x-request-id": "abc"}) == "abc"
    assert _extract_request_id({}) is None
    assert _extract_request_id(None) is None


def test_create_post_success_logs_request_id(caplog):
    opener = MagicMock(return_value=_FakeResponse(
        status=201,
        body={"data": {"id": "post_abc", "web_url": "https://x.beehiiv.com/p/abc"}},
        request_id="req_xyz",
    ))
    client = BeehiivClient(
        api_key="k", publication_id="pub_123",
        opener=opener, sleep=lambda _: None,
    )
    with caplog.at_level(logging.INFO):
        resp = client.create_post({"title": "hi", "body_html": "<p>hi</p>"})
    assert resp.status == 201
    assert resp.data["data"]["id"] == "post_abc"
    assert resp.request_id == "req_xyz"
    # Bearer must not appear in logs (defense in depth).
    log_dump = "\n".join(rec.message for rec in caplog.records)
    assert "k" not in log_dump.split() or "attempt=1" in log_dump
    assert "req_xyz" in log_dump
    assert "Bearer" not in log_dump

    # Only one attempt on success.
    assert opener.call_count == 1
    req = opener.call_args.args[0]
    assert req.method == "POST"
    assert req.full_url.endswith("/v2/publications/pub_123/posts")
    assert req.headers["Authorization"] == "Bearer k"
    body = json.loads(req.data.decode("utf-8"))
    assert body == {"title": "hi", "body_html": "<p>hi</p>"}


def test_retry_on_500_then_success():
    sleeps: list[float] = []
    call_count = {"n": 0}

    def _opener(req, timeout):
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise _make_http_error(503, body='{"err":"upstream"}', request_id=f"req_{call_count['n']}")
        return _FakeResponse(status=200, body={"data": {"id": "p_ok"}}, request_id="req_ok")

    client = BeehiivClient(
        api_key="k", publication_id="pub", opener=_opener,
        sleep=sleeps.append,
        backoff_seconds=(0.01, 0.02, 0.04),
    )
    resp = client.create_post({"title": "hi"})
    assert resp.status == 200
    assert call_count["n"] == 3
    # Two backoffs before the third attempt succeeded.
    assert sleeps == [0.01, 0.02]


def test_retry_on_429_then_success():
    sleeps: list[float] = []
    call_count = {"n": 0}

    def _opener(req, timeout):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise _make_http_error(429, body='{"err":"rate limited"}')
        return _FakeResponse(status=201, body={"data": {"id": "p_ok"}})

    client = BeehiivClient(
        api_key="k", publication_id="pub", opener=_opener,
        sleep=sleeps.append,
        backoff_seconds=(0.01, 0.02, 0.04),
    )
    resp = client.create_post({"title": "hi"})
    assert resp.status == 201
    assert call_count["n"] == 2
    assert sleeps == [0.01]


def test_no_retry_on_400():
    call_count = {"n": 0}

    def _opener(req, timeout):
        call_count["n"] += 1
        raise _make_http_error(400, body='{"errors":[{"msg":"bad title"}]}')

    client = BeehiivClient(
        api_key="k", publication_id="pub", opener=_opener,
        sleep=lambda _: None,
    )
    with pytest.raises(BeehiivError) as excinfo:
        client.create_post({"title": ""})
    assert excinfo.value.status == 400
    assert '"bad title"' in (excinfo.value.body or "")
    # Fast-fail: no retry on a 4xx that isn't 429.
    assert call_count["n"] == 1


def test_no_retry_on_401():
    calls = {"n": 0}

    def _opener(req, timeout):
        calls["n"] += 1
        raise _make_http_error(401, body='{"err":"bad token"}')

    client = BeehiivClient(
        api_key="k", publication_id="pub", opener=_opener,
        sleep=lambda _: None,
    )
    with pytest.raises(BeehiivError):
        client.create_post({"title": "hi"})
    assert calls["n"] == 1


def test_no_retry_on_422():
    calls = {"n": 0}

    def _opener(req, timeout):
        calls["n"] += 1
        raise _make_http_error(422, body='{"err":"unprocessable"}')

    client = BeehiivClient(
        api_key="k", publication_id="pub", opener=_opener,
        sleep=lambda _: None,
    )
    with pytest.raises(BeehiivError):
        client.create_post({"title": "hi"})
    assert calls["n"] == 1


def test_exhausted_retries_raises_with_status():
    calls = {"n": 0}

    def _opener(req, timeout):
        calls["n"] += 1
        raise _make_http_error(503, body="", request_id=f"req_{calls['n']}")

    client = BeehiivClient(
        api_key="k", publication_id="pub", opener=_opener,
        sleep=lambda _: None,
        backoff_seconds=(0.0, 0.0, 0.0),
        max_attempts=3,
    )
    with pytest.raises(BeehiivError) as excinfo:
        client.create_post({"title": "hi"})
    assert excinfo.value.status == 503
    assert excinfo.value.request_id == "req_3"
    assert calls["n"] == 3


def test_transport_error_retries():
    calls = {"n": 0}

    def _opener(req, timeout):
        calls["n"] += 1
        if calls["n"] == 1:
            raise URLError("connection reset")
        return _FakeResponse(status=200, body={"data": {"id": "ok"}})

    client = BeehiivClient(
        api_key="k", publication_id="pub", opener=_opener,
        sleep=lambda _: None,
        backoff_seconds=(0.0, 0.0, 0.0),
    )
    resp = client.create_post({"title": "hi"})
    assert resp.status == 200
    assert calls["n"] == 2


def test_confirm_post_patches_status():
    opener = MagicMock(return_value=_FakeResponse(
        status=200, body={"data": {"id": "p1", "status": "confirmed"}},
    ))
    client = BeehiivClient(
        api_key="k", publication_id="pub_123", opener=opener,
        sleep=lambda _: None,
    )
    resp = client.confirm_post("p1")
    assert resp.status == 200
    req = opener.call_args.args[0]
    assert req.method == "PATCH"
    assert req.full_url.endswith("/v2/publications/pub_123/posts/p1")
    body = json.loads(req.data.decode("utf-8"))
    assert body == {"status": "confirmed"}


def test_create_subscription_shape():
    opener = MagicMock(return_value=_FakeResponse(
        status=201, body={"data": {"id": "sub_1", "email": "a@b.com"}},
    ))
    client = BeehiivClient(
        api_key="k", publication_id="pub_z", opener=opener,
        sleep=lambda _: None,
    )
    payload = {
        "email": "a@b.com",
        "reactivate_existing": True,
        "send_welcome_email": True,
        "utm_source": "landing",
    }
    resp = client.create_subscription(payload)
    assert resp.status == 201
    req = opener.call_args.args[0]
    assert req.method == "POST"
    assert req.full_url.endswith("/v2/publications/pub_z/subscriptions")
    assert json.loads(req.data.decode("utf-8")) == payload


def test_confirm_post_requires_post_id():
    client = BeehiivClient(api_key="k", publication_id="pub")
    with pytest.raises(ValueError):
        client.confirm_post("")
