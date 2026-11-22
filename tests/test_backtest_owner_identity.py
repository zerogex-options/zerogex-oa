"""Who owns a backtest: the member's token, else the member's own API key.

Owner used to come from the end-user token alone, so everything a member
saved through their own API key was stored ownerless, in one pool every other
key holder could list, read, publish, edit and delete.
"""

from __future__ import annotations

from types import SimpleNamespace


def _request(identity=None, headers=None):
    return SimpleNamespace(state=SimpleNamespace(identity=identity), headers=headers or {})


def test_a_members_own_key_owns_what_it_creates(monkeypatch):
    from src.api.routers import backtest

    monkeypatch.setattr(backtest, "resolve_end_user", lambda request: (None, None))
    req = _request(identity=SimpleNamespace(caller_user_id="user-7"))
    assert backtest._owner(req) == "key:user-7"


def test_two_keys_get_different_owners(monkeypatch):
    from src.api.routers import backtest

    monkeypatch.setattr(backtest, "resolve_end_user", lambda request: (None, None))
    a = backtest._owner(_request(identity=SimpleNamespace(caller_user_id="user-7")))
    b = backtest._owner(_request(identity=SimpleNamespace(caller_user_id="user-8")))
    assert a != b


def test_the_website_member_token_wins(monkeypatch):
    from src.api.routers import backtest

    monkeypatch.setattr(backtest, "resolve_end_user", lambda request: ("member-1", "web-token"))
    req = _request(identity=SimpleNamespace(caller_user_id="static"))
    assert backtest._owner(req) == "member-1"


def test_the_shared_key_with_no_member_is_ownerless(monkeypatch):
    from src.api.routers import backtest

    monkeypatch.setattr(backtest, "resolve_end_user", lambda request: (None, None))
    assert backtest._owner(_request(identity=SimpleNamespace(caller_user_id="static"))) is None
    assert backtest._owner(_request(identity=None)) is None
