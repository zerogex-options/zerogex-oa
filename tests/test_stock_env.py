"""``make stock-env-add`` (src/tools/stock_env.py): add a stock the way NVDA was added."""

import json
import os
import stat
from datetime import datetime
from pathlib import Path

import pytest
from dotenv import dotenv_values

from src.tools import stock_env as se

ENV = """\
# ZeroGEX production
DB_HOST=zerogex.c1a2b3.us-east-1.rds.amazonaws.com
DB_PASSWORD=hunter2-very-secret
INGEST_UNDERLYINGS=SPY,$SPXW.X,QQQ,$NDXP.X,NVDA
ANALYTICS_UNDERLYINGS=SPY,SPX,QQQ,NDX,NVDA
MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS=SPY,SPX,QQQ,NDX,NVDA
INGEST_STRIKE_PCT_RANGE_BY_SYMBOL={"NVDA": 8}
WALL_ALERTS_SYMBOLS=SPY,QQQ,SPX,NDX,NVDA
REGIME_SESSION_SYMBOLS=SPY,SPX,QQQ,NDX,NVDA
DAILY_ATM_IV_SYMBOLS=SPY QQQ SPX NDX NVDA
WS_ALLOWED_SYMBOLS=SPY,QQQ,SPX,$VIX.X,$VXN.X
SIGNALS_UNDERLYINGS=SPY,SPX,QQQ,NDX
OPS_API_KEY="ops-key-also-secret"
"""


def _plan(text=ENV, symbol="MSFT", **kw):
    return se.plan_addition(text.splitlines(keepends=True), symbol, **kw)


def test_a_stock_goes_where_nvda_is_and_nowhere_else():
    plan = _plan()
    after = dotenv_values(stream=__import__("io").StringIO("".join(plan.lines)))
    before = dotenv_values(stream=__import__("io").StringIO(ENV))

    assert after["INGEST_UNDERLYINGS"] == "SPY,$SPXW.X,QQQ,$NDXP.X,NVDA,MSFT"
    assert after["ANALYTICS_UNDERLYINGS"] == "SPY,SPX,QQQ,NDX,NVDA,MSFT"
    assert after["MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS"] == "SPY,SPX,QQQ,NDX,NVDA,MSFT"
    assert after["WALL_ALERTS_SYMBOLS"].endswith(",NVDA,MSFT")
    assert after["REGIME_SESSION_SYMBOLS"].endswith(",NVDA,MSFT")
    # Space-separated stays space-separated: the Makefile reads it as a make list.
    assert after["DAILY_ATM_IV_SYMBOLS"] == "SPY QQQ SPX NDX NVDA MSFT"
    # NVDA is not on the WebSocket list, so neither is MSFT.
    assert after["WS_ALLOWED_SYMBOLS"] == before["WS_ALLOWED_SYMBOLS"]
    # NVDA's band is kept and MSFT gets the same, unquoted, as valid JSON.
    assert json.loads(after["INGEST_STRIKE_PCT_RANGE_BY_SYMBOL"]) == {"NVDA": 8, "MSFT": 8}
    assert 'INGEST_STRIKE_PCT_RANGE_BY_SYMBOL={"NVDA": 8, "MSFT": 8}\n' in plan.lines
    # Every other line is byte-for-byte what it was.
    changed = set(plan.changed)
    for old, new in zip(ENV.splitlines(keepends=True), plan.lines):
        key = old.split("=", 1)[0]
        if key not in changed:
            assert old == new
    assert changed == {
        "INGEST_UNDERLYINGS",
        "ANALYTICS_UNDERLYINGS",
        "MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS",
        "WALL_ALERTS_SYMBOLS",
        "REGIME_SESSION_SYMBOLS",
        "DAILY_ATM_IV_SYMBOLS",
        "INGEST_STRIKE_PCT_RANGE_BY_SYMBOL",
    }
    assert any("WS_ALLOWED_SYMBOLS does not list NVDA" in n for n in plan.notes)


def test_the_band_map_reads_back_through_the_services_own_parser(monkeypatch):
    plan = _plan(symbol="TSLA", band=10)
    line = next(row for row in plan.lines if row.startswith(se.BAND_KEY))
    monkeypatch.setenv(se.BAND_KEY, line.split("=", 1)[1].strip())
    from src.config import _parse_symbol_float_map

    assert _parse_symbol_float_map(se.BAND_KEY, min=0.5, max=50.0) == {"NVDA": 8.0, "TSLA": 10.0}


def test_running_it_twice_changes_nothing_the_second_time():
    once = "".join(_plan().lines)
    again = _plan(once)
    assert again.changed == {}
    assert se.describe(again).startswith("Nothing to change")


def test_what_it_prints_is_only_the_changed_lines():
    shown = se.describe(_plan())
    assert "hunter2" not in shown and "ops-key" not in shown and "DB_HOST" not in shown
    assert "+ INGEST_UNDERLYINGS=SPY,$SPXW.X,QQQ,$NDXP.X,NVDA,MSFT" in shown
    assert "- INGEST_UNDERLYINGS=SPY,$SPXW.X,QQQ,$NDXP.X,NVDA" in shown


def test_restarts_follow_the_keys_that_changed():
    assert _plan().restarts == [
        "zerogex-oa-ingestion",
        "zerogex-oa-analytics",
        "zerogex-oa-wall-alerts",
    ]
    with_ws = ENV.replace("WS_ALLOWED_SYMBOLS=SPY,QQQ,SPX,", "WS_ALLOWED_SYMBOLS=SPY,QQQ,NVDA,SPX,")
    assert "zerogex-oa-api" in _plan(with_ws).restarts


def test_quotes_are_kept_and_a_fractional_band_is_written_as_is():
    text = ENV.replace(
        "ANALYTICS_UNDERLYINGS=SPY,SPX,QQQ,NDX,NVDA", 'ANALYTICS_UNDERLYINGS="SPY,SPX,QQQ,NDX,NVDA"'
    )
    plan = _plan(text, band=7.5)
    assert 'ANALYTICS_UNDERLYINGS="SPY,SPX,QQQ,NDX,NVDA,MSFT"\n' in plan.lines
    assert 'INGEST_STRIKE_PCT_RANGE_BY_SYMBOL={"NVDA": 8, "MSFT": 7.5}\n' in plan.lines


def test_a_missing_band_line_is_added_at_the_end():
    text = ENV.replace('INGEST_STRIKE_PCT_RANGE_BY_SYMBOL={"NVDA": 8}\n', "").rstrip("\n")
    plan = _plan(text)
    assert plan.lines[-1] == 'INGEST_STRIKE_PCT_RANGE_BY_SYMBOL={"MSFT": 8}\n'
    assert plan.lines[-2].endswith("\n")


@pytest.mark.parametrize(
    "broken, reason",
    [
        (ENV.replace('{"NVDA": 8}', '{"NVDA": 8,}'), "not valid JSON"),
        (ENV.replace("NDX,NVDA\nMAX", "NDX,NVDA  # stocks\nMAX"), "comment"),
        (ENV + "ANALYTICS_UNDERLYINGS=SPY\n", "set twice"),
        (
            ENV.replace("SIGNALS_UNDERLYINGS=SPY,", "SIGNALS_UNDERLYINGS=MSFT,SPY,"),
            "no stock belongs",
        ),
        (
            ENV.replace(
                "MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS=", "#MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS="
            ),
            "not in .env",
        ),
    ],
)
def test_anything_it_cannot_change_safely_is_refused(broken, reason):
    with pytest.raises(se.Refused, match=reason):
        _plan(broken)


def test_a_comment_naming_a_stock_does_not_count_as_listing_it():
    # On a list every stock needs: the comment does not hide that MSFT is
    # missing, and the line is refused rather than edited around the comment.
    required = ENV.replace(
        "ANALYTICS_UNDERLYINGS=SPY,SPX,QQQ,NDX,NVDA",
        "ANALYTICS_UNDERLYINGS=SPY,SPX,QQQ,NDX,NVDA  # MSFT next",
    )
    with pytest.raises(se.Refused, match="ANALYTICS_UNDERLYINGS has a comment"):
        _plan(required)
    # On an optional list: NVDA named only in the comment is not NVDA listed.
    optional = ENV.replace(
        "WALL_ALERTS_SYMBOLS=SPY,QQQ,SPX,NDX,NVDA", "WALL_ALERTS_SYMBOLS=SPY,QQQ  # NVDA later"
    )
    plan = _plan(optional)
    assert "WALL_ALERTS_SYMBOLS" not in plan.changed
    assert any("WALL_ALERTS_SYMBOLS does not list NVDA" in n for n in plan.notes)


def test_nvda_in_a_model_list_is_reported_and_left_alone():
    text = ENV.replace(
        "SIGNALS_UNDERLYINGS=SPY,SPX,QQQ,NDX", "SIGNALS_UNDERLYINGS=SPY,SPX,QQQ,NDX,NVDA"
    )
    plan = _plan(text)
    assert "SIGNALS_UNDERLYINGS" not in plan.changed
    assert any("NVDA is in SIGNALS_UNDERLYINGS" in n for n in plan.notes)


def test_not_a_ticker_is_refused():
    with pytest.raises(se.Refused, match="not a stock ticker"):
        _plan(symbol="MS FT")


# ── the command ─────────────────────────────────────────────────────────────


@pytest.fixture
def env_file(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setattr(Path, "home", lambda: home)
    path = tmp_path / ".env"
    path.write_text(ENV)
    os.chmod(path, 0o600)
    return path


def test_without_apply_nothing_is_written(env_file, capsys):
    assert se.main(["--symbol", "msft", "--env", str(env_file)]) == 0
    assert env_file.read_text() == ENV
    assert "CONFIRM=yes" in capsys.readouterr().out


def test_apply_backs_up_then_writes_keeping_permissions(env_file, capsys):
    assert se.main(["--symbol", "MSFT", "--env", str(env_file), "--apply"]) == 0
    out = capsys.readouterr().out
    backups = list((env_file.parent / "home").glob(".env.pre-msft-*"))
    assert len(backups) == 1 and backups[0].read_text() == ENV
    assert "MSFT" in dotenv_values(env_file)["INGEST_UNDERLYINGS"]
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600
    assert "Restart for it to take effect: zerogex-oa-ingestion, zerogex-oa-analytics" in out
    # A second backup the same day does not overwrite the first.
    later = se.backup_path(
        env_file, "MSFT", datetime(2026, 11, 5, 18, 0, 0), env_file.parent / "home"
    )
    assert later != backups[0] or not backups[0].exists()


@pytest.mark.parametrize("symbol", ["SPY", "SPX", "ES"])
def test_an_etf_index_or_future_is_refused(env_file, symbol, capsys):
    assert se.main(["--symbol", symbol, "--env", str(env_file), "--apply"]) == 1
    assert env_file.read_text() == ENV
    assert "Refused, nothing written" in capsys.readouterr().out


def test_a_one_symbol_list_takes_its_keys_own_separator():
    text = ENV.replace(
        "DAILY_ATM_IV_SYMBOLS=SPY QQQ SPX NDX NVDA", "DAILY_ATM_IV_SYMBOLS=NVDA"
    ).replace("WALL_ALERTS_SYMBOLS=SPY,QQQ,SPX,NDX,NVDA", "WALL_ALERTS_SYMBOLS=NVDA")
    plan = _plan(text)
    assert "DAILY_ATM_IV_SYMBOLS=NVDA MSFT\n" in plan.lines
    assert "WALL_ALERTS_SYMBOLS=NVDA,MSFT\n" in plan.lines


def test_the_service_lists_that_default_to_the_ingestion_list_are_mirrored_when_set():
    text = (
        ENV
        + "SESSION_LEVELS_SYMBOLS=SPY,QQQ,NVDA\nSESSION_SURFACE_WARM_SYMBOLS=SPY,SPX,QQQ,NDX,NVDA\n"
    )
    plan = _plan(text)
    assert "SESSION_LEVELS_SYMBOLS=SPY,QQQ,NVDA,MSFT\n" in plan.lines
    assert "SESSION_SURFACE_WARM_SYMBOLS=SPY,SPX,QQQ,NDX,NVDA,MSFT\n" in plan.lines
    assert "zerogex-oa-api" in plan.restarts  # the API reads the warm list at start


def test_a_list_it_does_not_know_that_names_nvda_is_reported_not_edited():
    text = ENV + "NORMALIZER_SYMBOLS=SPY QQQ NVDA\nDB_NAME=zerogex\n"
    plan = _plan(text)
    assert "NORMALIZER_SYMBOLS" not in plan.changed
    assert "NORMALIZER_SYMBOLS=SPY QQQ NVDA\n" in plan.lines
    assert any(
        "NVDA is also in NORMALIZER_SYMBOLS; add MSFT there by hand" in n for n in plan.notes
    )
    assert not any("DB_NAME" in n for n in plan.notes)
