"""TradeStation _request robustness: 2xx handling and 401 auth-refresh budget.

* B9: a 2xx other than 200/201 (e.g. 204 No Content) must not fall through
  to a no-op raise_for_status() and return None.
* B10: a 401 must always trigger exactly one token refresh + retry, even
  when API_RETRY_ATTEMPTS<=1 (the data-retry budget must not gate auth).
"""

from src.ingestion.tradestation_client import TradeStationClient


class _Resp:
    def __init__(self, status_code, content=b"{}", payload=None):
        self.status_code = status_code
        self.content = content
        self.text = content.decode() if isinstance(content, bytes) else str(content)
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"HTTP {self.status_code}")


class _FakeAuth:
    def __init__(self):
        self.refreshes = 0

    def get_headers(self):
        return {"Authorization": "Bearer tok"}

    def force_refresh_access_token(self, failed_token=None):
        self.refreshes += 1


def _client():
    c = TradeStationClient.__new__(TradeStationClient)
    c.base_url = "https://api.tradestation.com/v3"
    c.auth = _FakeAuth()
    return c


def test_204_no_content_returns_empty_shape_not_none():
    c = _client()
    c._build_request_response = lambda *a, **k: _Resp(204, content=b"")
    result = c._request("GET", "marketdata/options/expirations/SPY")
    # Must be a dict (endpoint-shaped empty), not None.
    assert result == {"Expirations": []}


def test_202_with_body_returns_parsed_json():
    c = _client()
    c._build_request_response = lambda *a, **k: _Resp(202, payload={"ok": True})
    assert c._request("GET", "marketdata/quotes/SPY") == {"ok": True}


def test_401_always_refreshes_once_then_succeeds():
    c = _client()
    calls = {"n": 0}

    def _resp(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            return _Resp(401, content=b"unauthorized")
        return _Resp(200, payload={"Expirations": ["2026-06-19"]})

    c._build_request_response = _resp
    result = c._request("GET", "marketdata/options/expirations/SPY")
    assert c.auth.refreshes == 1
    assert result == {"Expirations": ["2026-06-19"]}


def test_401_twice_raises_after_single_refresh():
    c = _client()
    c._build_request_response = lambda *a, **k: _Resp(401, content=b"unauthorized")
    try:
        c._request("GET", "marketdata/options/expirations/SPY")
        raised = False
    except AssertionError:
        raised = True
    # Exactly one refresh attempted, then it gives up (raises) instead of
    # looping forever.
    assert c.auth.refreshes == 1
    assert raised
