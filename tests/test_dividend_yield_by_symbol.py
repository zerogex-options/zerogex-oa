"""Per-symbol dividend-yield resolution.

DIVIDEND_YIELD_BY_SYMBOL (JSON, canonical-upper keys) overrides the scalar
DIVIDEND_YIELD fallback. Each ingestion/analytics worker is single-symbol and
resolves its q once at construction.
"""

import importlib

import src.config as cfg


def _reload(env, monkeypatch):
    for k, v in env.items():
        if v is None:
            monkeypatch.delenv(k, raising=False)
        else:
            monkeypatch.setenv(k, v)
    return importlib.reload(cfg)


def test_map_overrides_scalar_fallback(monkeypatch):
    c = _reload(
        {
            "DIVIDEND_YIELD": "0.0",
            "DIVIDEND_YIELD_BY_SYMBOL": '{"SPY": 0.013, "QQQ": 0.006, "SPX": 0.015}',
        },
        monkeypatch,
    )
    assert c.resolve_dividend_yield("SPY") == 0.013
    assert c.resolve_dividend_yield("QQQ") == 0.006
    assert c.resolve_dividend_yield("SPX") == 0.015
    # Unmapped symbol -> scalar fallback.
    assert c.resolve_dividend_yield("IWM") == 0.0


def test_keys_are_case_insensitive_canonical(monkeypatch):
    c = _reload(
        {"DIVIDEND_YIELD": "0.02", "DIVIDEND_YIELD_BY_SYMBOL": '{"spy": 0.013}'}, monkeypatch
    )
    assert c.resolve_dividend_yield("SPY") == 0.013  # lowercased map key still matches
    assert c.resolve_dividend_yield("aapl") == 0.02  # fallback, case-insensitive lookup


def test_invalid_json_falls_back_safely(monkeypatch):
    c = _reload({"DIVIDEND_YIELD": "0.01", "DIVIDEND_YIELD_BY_SYMBOL": "not-json"}, monkeypatch)
    assert c.DIVIDEND_YIELD_BY_SYMBOL == {}
    assert c.resolve_dividend_yield("SPY") == 0.01


def test_values_are_clamped(monkeypatch):
    c = _reload(
        {"DIVIDEND_YIELD": "0.0", "DIVIDEND_YIELD_BY_SYMBOL": '{"SPY": 9.9, "QQQ": -1.0}'},
        monkeypatch,
    )
    assert c.resolve_dividend_yield("SPY") == 0.2  # clamped to max
    assert c.resolve_dividend_yield("QQQ") == 0.0  # clamped to min


def test_scalar_tolerates_inline_comment(monkeypatch):
    # Regression: the bare float() crash on a '# comment' tail.
    c = _reload(
        {"DIVIDEND_YIELD": "0.01          # SPY-ish", "DIVIDEND_YIELD_BY_SYMBOL": None}, monkeypatch
    )
    assert c.DIVIDEND_YIELD == 0.01


def teardown_module(module):
    # Restore the module to its unpatched env so other tests see defaults.
    importlib.reload(cfg)
