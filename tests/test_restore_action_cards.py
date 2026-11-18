"""Putting back the Action Card history from a restored copy: the planning
rules, without a database. tests/test_restore_action_cards_db.py runs the
whole copy against two scratch databases."""

from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from src import config
from src.tools import restore_action_cards as rac

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
PROD = "zerogex.c1kcsgeko1d4.us-east-2.rds.amazonaws.com"


# ----------------------------------------------------------------------
# Which server
# ----------------------------------------------------------------------


def test_an_instance_name_takes_production_s_domain():
    assert (
        rac.resolve_source_host("zerogex-restore", PROD)
        == "zerogex-restore.c1kcsgeko1d4.us-east-2.rds.amazonaws.com"
    )


def test_a_full_host_is_used_as_given():
    assert rac.resolve_source_host(" Other.Example.COM ", PROD) == "other.example.com"


@pytest.mark.parametrize("source", ["zerogex", PROD.upper()])
def test_production_itself_is_refused(source):
    with pytest.raises(ValueError, match="production"):
        rac.resolve_source_host(source, PROD)


def test_an_instance_name_needs_a_domain_to_join():
    with pytest.raises(ValueError, match="full host"):
        rac.resolve_source_host("zerogex-restore", "localhost")
    with pytest.raises(ValueError, match="empty"):
        rac.resolve_source_host("  ", PROD)


# ----------------------------------------------------------------------
# Password
# ----------------------------------------------------------------------


def test_pgpass_fields_unescape_and_keep_the_password_whole():
    assert rac.pgpass_fields(r"h:5432:db:u:pa\:ss") == ["h", "5432", "db", "u", "pa:ss"]
    # deploy/steps/020.database writes the password unescaped
    assert rac.pgpass_fields("h:5432:db:u:a:b\\\\c") == ["h", "5432", "db", "u", "a:b\\c"]


def test_pgpass_password_finds_production_s_line():
    lines = [
        "# comment",
        "",
        f"{PROD}:5432:zerogex_shadow:postgres:shadow",
        f"{PROD}:5432:zerogex:postgres:s3cret",
        "*:*:*:*:wildcard",
    ]
    found = rac.pgpass_password(lines, host=PROD, port="5432", dbname="zerogex", user="postgres")
    assert found == "s3cret"
    assert (
        rac.pgpass_password(lines, host="elsewhere", port="5432", dbname="x", user="y")
        == "wildcard"
    )
    assert (
        rac.pgpass_password(lines[:4], host="elsewhere", port="5432", dbname="x", user="y") is None
    )


def _password(
    environ, provider=lambda: None, lines=lambda: [f"{PROD}:5432:zerogex:postgres:pgpass"]
):
    return rac.source_password(
        environ,
        provider=provider,
        host=PROD,
        port="5432",
        dbname="zerogex",
        user="postgres",
        pgpass_lines=lines,
    )


def test_source_password_prefers_the_explicit_one_then_production_s():
    assert _password({"RESTORE_DB_PASSWORD": "r", "DB_PASSWORD": "d"}, lambda: "p") == "r"
    assert _password({"DB_PASSWORD": "d"}, lambda: "p") == "p"
    assert _password({"DB_PASSWORD": "d"}) == "d"
    assert _password({}) == "pgpass"


def test_source_password_without_a_pgpass_is_none():
    def missing():
        raise FileNotFoundError("~/.pgpass")

    assert _password({}, lines=missing) is None


# ----------------------------------------------------------------------
# Columns and ids
# ----------------------------------------------------------------------


def test_copyable_columns_follow_production():
    target = [
        rac.Column("id", False, True),
        rac.Column("underlying", False, False),
        rac.Column("note", True, False),  # new and nullable: fine
        rac.Column("created_at", False, True),  # new with a default: fine
        rac.Column("total", False, False, generated=True),  # never inserted
    ]
    copied, dropped, missing = rac.copyable_columns(target, ["underlying", "id", "old_col"])
    assert copied == ["id", "underlying"]
    assert dropped == ["old_col"]
    assert missing == []

    target.append(rac.Column("required_new", False, False))
    assert rac.copyable_columns(target, ["id", "underlying"])[2] == ["required_new"]


def test_classify_sorts_new_already_back_and_clashing_rows():
    t = datetime(2026, 9, 24, 19, 59, tzinfo=UTC)
    source = {
        1: ("SPY", "vwap_reversion", t, "time_exit"),
        2: ("SPY", "call_wall_fade", t, "pending"),
        3: ("QQQ", "max_pain_gravitation", t, "stop_hit"),
    }
    target = {
        # same row; the grader has since finished it, which is not a clash
        2: ("SPY", "call_wall_fade", t, "target_hit"),
        3: ("SPY", "gamma_flip_break", t, "stop_hit"),  # a different row
    }
    got = rac.classify(source, target, width=3)
    assert got.to_add == [1]
    assert got.already_there == [2]
    assert got.clashes == [3]


def test_next_serial():
    assert rac.next_serial(12160, True) == 12161
    assert rac.next_serial(1, False) == 1
    assert rac.next_serial(None, None) is None


def test_headroom_accepts_the_real_ranges():
    # restored copy: 1-11,649; production from 11,651, counter past 12,160
    assert rac.headroom_problem("Cards", range(1, 11650), 11651, 12161) is None
    assert rac.headroom_problem("Cards", [], 11651, 12161) is None


def test_headroom_refuses_interleaving_or_a_future_collision():
    assert "interleave" in rac.headroom_problem("Cards", [5, 11651], 11651, 12161)
    assert "collide" in rac.headroom_problem("Cards", [1, 150], None, 100)


# ----------------------------------------------------------------------
# Nothing put back may change what goes out live
# ----------------------------------------------------------------------

NOW = datetime(2026, 11, 18, 15, 0, tzinfo=UTC)


def test_entry_bar_ignores_grades_before_the_floor():
    last_restored = datetime(2026, 9, 24, 15, 59, tzinfo=ET)
    issued = [datetime(2026, 9, 10, 10, 0, tzinfo=ET), last_restored]
    assert rac.entry_bar_reach(issued, since=date(2026, 9, 25), lookback_days=90, now=NOW) == 0
    later = issued + [datetime(2026, 9, 25, 0, 0, tzinfo=ET)]
    assert rac.entry_bar_reach(later, since=date(2026, 9, 25), lookback_days=90, now=NOW) == 1


def test_entry_bar_reach_also_respects_the_lookback():
    issued = [datetime(2026, 8, 1, 10, 0, tzinfo=ET)]
    assert rac.entry_bar_reach(issued, since=date(2026, 7, 1), lookback_days=90, now=NOW) == 0
    assert rac.entry_bar_reach(issued, since=date(2026, 7, 1), lookback_days=120, now=NOW) == 1


def test_calibration_names_the_first_day_it_no_longer_reaches_the_cards():
    newest = datetime(2026, 9, 24, 19, 59, tzinfo=UTC)
    assert rac.calibration_clear_on([newest], lookback_days=60, now=NOW) == date(2026, 11, 24)
    after = datetime(2026, 11, 24, 12, 0, tzinfo=UTC)
    assert rac.calibration_clear_on([newest], lookback_days=60, now=after) is None
    assert rac.calibration_clear_on([], lookback_days=60, now=NOW) is None


def test_calibration_lookback_follows_calib_days_then_config(monkeypatch):
    monkeypatch.setattr(config, "SIGNALS_PATTERN_CALIBRATION_LOOKBACK_DAYS", 60)
    assert rac.calibration_lookback_days({}) == 60
    assert rac.calibration_lookback_days({"CALIB_DAYS": "45  # shorter"}) == 45
    assert rac.calibration_lookback_days({"CALIB_DAYS": "lots"}) == 60
