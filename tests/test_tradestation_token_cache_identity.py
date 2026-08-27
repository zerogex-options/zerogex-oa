"""The shared access-token cache must not leak across TradeStation identities.

``TradeStationAuth`` caches its access token in a file under the system
tempdir so the many ingestion processes share one refresh instead of each
hammering the token endpoint. That file used to be named for the ENVIRONMENT
alone (`tradestation_token_cache.json`), which was correct only while the whole
deployment had a single identity.

It no longer does. The ES/NQ feeds can run under a second TradeStation username
— the one holding the real-time CME entitlement — while everything else keeps
the main credential. With one shared file, whichever process refreshed last
wrote its token there and every other process read it back, so the futures
ingester silently ran on the MAIN username's access token and
``TRADESTATION_FUTURES_REFRESH_TOKEN`` had no effect whatsoever.

Nothing failed. Streams connected, bars arrived, and the entitlement simply
never applied — the same shape as every other bug in this feature. It was
caught because ``make ts-whoami`` reported both credentials as the same
username with byte-identical `iat`/`exp` claims, ten minutes stale.
"""

from src.ingestion.tradestation_auth import TradeStationAuth


def _auth(refresh_token: str, sandbox: bool = False) -> TradeStationAuth:
    return TradeStationAuth("client-id", "client-secret", refresh_token, sandbox=sandbox)


def test_two_identities_do_not_share_a_cache_file():
    """The regression. Same app, same environment, different user."""
    main = _auth("main-username-token")
    futures = _auth("cme-entitled-username-token")
    assert main._token_cache_path != futures._token_cache_path
    assert main._token_cache_lock_path != futures._token_cache_lock_path


def test_the_same_identity_still_shares_one_cache():
    """The point of the cache: many processes, one refresh. Keying per identity
    must not become keying per process."""
    assert _auth("same-token")._token_cache_path == _auth("same-token")._token_cache_path


def test_the_lock_travels_with_the_cache_it_guards():
    """A per-identity cache behind a shared lock would serialise unrelated
    refreshes; a shared cache behind a per-identity lock would race."""
    auth = _auth("some-token")
    assert auth._token_cache_path.stem == auth._token_cache_lock_path.stem


def test_the_refresh_token_never_appears_in_the_filename():
    """The tempdir is world-listable even though the file itself is 0600."""
    secret = "super-secret-refresh-token"
    auth = _auth(secret)
    assert secret not in str(auth._token_cache_path)
    assert secret not in str(auth._token_cache_lock_path)


def test_sandbox_and_production_stay_separated_for_one_identity():
    """The distinction the old name existed to make must survive."""
    assert _auth("t", sandbox=True)._token_cache_path != _auth("t")._token_cache_path
    assert "sandbox" in _auth("t", sandbox=True)._token_cache_path.name
