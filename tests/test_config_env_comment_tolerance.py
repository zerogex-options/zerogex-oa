"""Numeric env vars must tolerate inline ``# comment`` tails.

python-dotenv preserves everything after ``=`` literally, so a .env line like
``DB_PORT=5432  # prod`` used to crash worker startup with a ValueError from a
bare int()/float(). All numeric config is now parsed via the comment-tolerant
_getenv_int/_getenv_float helpers. This pins that across a representative set of
module-level constants and the auth refresh fields.
"""

import importlib

import src.config as cfg


def _reload_with(env, monkeypatch):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return importlib.reload(cfg)


def test_config_constants_tolerate_inline_comments(monkeypatch):
    c = _reload_with(
        {
            "DB_PORT": "5432   # prod db",
            "DB_STATEMENT_TIMEOUT_MS": "30000  # tuned",
            "IV_MIN": "0.01  # floor",
            "AGGREGATION_BUCKET_SECONDS": "60  # 1 minute",
            "API_REQUEST_TIMEOUT": "30  # seconds",
            "RISK_FREE_RATE": "0.05  # 5%",
            "MAX_BUFFER_SIZE": "1000  # cap",
        },
        monkeypatch,
    )
    assert c.DB_PORT == 5432
    assert c.DB_STATEMENT_TIMEOUT_MS == 30000
    assert c.IV_MIN == 0.01
    assert c.AGGREGATION_BUCKET_SECONDS == 60
    assert c.API_REQUEST_TIMEOUT == 30
    assert c.RISK_FREE_RATE == 0.05
    assert c.MAX_BUFFER_SIZE == 1000


def test_auth_refresh_fields_tolerate_inline_comments(monkeypatch):
    monkeypatch.setenv("API_REQUEST_TIMEOUT", "30          # seconds")
    monkeypatch.setenv("TS_REFRESH_MAX_ATTEMPTS", "3  # retries")
    importlib.reload(cfg)
    from src.ingestion.tradestation_auth import TradeStationAuth

    a = TradeStationAuth("id", "secret", "refresh")
    assert a._refresh_timeout_seconds == 30
    assert a._refresh_max_attempts == 3


def teardown_module(module):
    importlib.reload(cfg)
