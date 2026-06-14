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


def test_boolean_flags_tolerate_inline_comments(monkeypatch):
    # A commented bool used to silently flip: "true  # on".lower() == "true"
    # is False, so the flag fell back to the wrong value with no error.
    c = _reload_with(
        {
            "GREEKS_ENABLED": "true   # keep on",
            "IV_CALCULATION_ENABLED": "yes  # also on",
            "TS_WARN_MARKET_HOURS": "false  # quiet please",
            "OPTION_REST_SEED_ON_RECALC": "on  # enable",
        },
        monkeypatch,
    )
    assert c.GREEKS_ENABLED is True
    assert c.IV_CALCULATION_ENABLED is True
    assert c.TS_WARN_MARKET_HOURS is False
    assert c.OPTION_REST_SEED_ON_RECALC is True


def test_safe_string_vars_strip_inline_comments(monkeypatch):
    # Symbols / templates / log level / environment are read via _getenv_str,
    # which strips an inline comment that would otherwise become part of the
    # string (and break enum/choices checks or symbol parsing).
    c = _reload_with(
        {
            "LOG_LEVEL": "DEBUG   # verbose",
            "SESSION_TEMPLATE": "USEQ24Hour  # 24h",
            "ENVIRONMENT": "staging  # env",
            "INGEST_UNDERLYINGS": "SPY,QQQ  # majors",
            "ANALYTICS_UNDERLYING": "SPX  # index",
        },
        monkeypatch,
    )
    assert c.LOG_LEVEL == "DEBUG"
    assert c.SESSION_TEMPLATE == "USEQ24Hour"
    assert c.ENVIRONMENT == "staging"
    assert c.INGEST_UNDERLYINGS == "SPY,QQQ"
    assert c.ANALYTICS_UNDERLYING == "SPX"


def test_api_numeric_vars_tolerate_inline_comments(monkeypatch):
    # API-layer module-level reads (ratelimit/security) now use the helpers
    # too. Reload the actual modules with commented env to exercise them.
    monkeypatch.setenv("END_USER_RATE_LIMIT_REQUESTS", "600  # rl")
    monkeypatch.setenv("END_USER_RATE_LIMIT_WINDOW_SECONDS", "60  # window")
    monkeypatch.setenv("API_KEY_CACHE_TTL_SECONDS", "60  # ttl")
    import src.api.ratelimit as rl
    import src.api.security as sec

    importlib.reload(rl)
    importlib.reload(sec)
    assert rl._LIMIT == 600
    assert rl._WINDOW == 60


def test_list_and_map_vars_tolerate_inline_comments(monkeypatch):
    # List- and JSON-map env vars previously silently fell back to the default
    # on a commented value (the trailing token / '}  # note' broke parsing).
    c = _reload_with(
        {
            "GAMMA_PROFILE_EXPANSION_RUNGS": "0.2,0.35,0.5  # ladder",
            "DIVIDEND_YIELD_BY_SYMBOL": '{"SPY": 0.013, "SPX": 0.015}  # yields',
            "SIGNALS_INDEPENDENT_PHASE_SCALP_MINUTES_BY_SYMBOL": '{"SPY": 30}  # scalp',
        },
        monkeypatch,
    )
    assert c.GAMMA_PROFILE_EXPANSION_RUNGS == [0.2, 0.35, 0.5]
    assert c.DIVIDEND_YIELD_BY_SYMBOL == {"SPY": 0.013, "SPX": 0.015}
    assert c.resolve_dividend_yield("SPX") == 0.015
    assert c.SIGNALS_INDEPENDENT_PHASE_SCALP_MINUTES_BY_SYMBOL == {"SPY": 30}


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
