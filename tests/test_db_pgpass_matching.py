"""Unit tests for .pgpass target-matching credential resolution.

Regression guard for the bug where the async (asyncpg) credential loaders in
``api/database.py`` and ``api/admin_keys.py`` took the FIRST ``~/.pgpass`` line
regardless of host — so a multi-environment .pgpass could authenticate the API
against the wrong database. ``find_pgpass_entry`` / ``resolve_db_credentials``
now match the configured ``DB_*`` target while staying backward-compatible when
those vars are unset.
"""

import pytest

from src.database.password_providers import find_pgpass_entry, resolve_db_credentials


def _write_pgpass(tmp_path, *lines):
    p = tmp_path / ".pgpass"
    p.write_text("\n".join(lines) + "\n")
    return p


# ---------------------------------------------------------------------------
# find_pgpass_entry — low-level line matcher
# ---------------------------------------------------------------------------
def test_match_selects_target_line_not_first(tmp_path):
    pg = _write_pgpass(
        tmp_path,
        "localhost:5432:zerogex:postgres:local_pw",
        "prod.rds.amazonaws.com:5432:zerogex:zerogex_user:prod_pw",
    )
    entry = find_pgpass_entry(
        host="prod.rds.amazonaws.com",
        port="5432",
        database="zerogex",
        user="zerogex_user",
        pgpass_path=pg,
    )
    assert entry is not None
    assert entry["host"] == "prod.rds.amazonaws.com"
    assert entry["password"] == "prod_pw"  # NOT local_pw (the first line)


def test_wildcard_field_matches_any_target(tmp_path):
    pg = _write_pgpass(tmp_path, "*:*:*:*:wild_pw")
    entry = find_pgpass_entry(host="anything", port="6543", database="db", user="u", pgpass_path=pg)
    assert entry is not None
    assert entry["password"] == "wild_pw"


def test_unconstrained_returns_first_line(tmp_path):
    # No target → behaves like the legacy first-line parser (backward compat).
    pg = _write_pgpass(
        tmp_path,
        "first.example:5432:db1:u1:pw1",
        "second.example:5432:db2:u2:pw2",
    )
    entry = find_pgpass_entry(pgpass_path=pg)
    assert entry["host"] == "first.example"
    assert entry["password"] == "pw1"


def test_password_with_colon_preserved(tmp_path):
    pg = _write_pgpass(tmp_path, "h:5432:db:u:pa:ss:word")
    entry = find_pgpass_entry(host="h", pgpass_path=pg)
    assert entry["password"] == "pa:ss:word"


def test_comments_blanks_and_short_lines_skipped(tmp_path):
    pg = _write_pgpass(
        tmp_path,
        "# a comment",
        "",
        "too:few:fields",
        "h:5432:db:u:pw",
    )
    entry = find_pgpass_entry(host="h", pgpass_path=pg)
    assert entry is not None and entry["password"] == "pw"


def test_no_match_returns_none(tmp_path):
    pg = _write_pgpass(tmp_path, "h1:5432:db:u:pw")
    assert find_pgpass_entry(host="h2", pgpass_path=pg) is None


def test_missing_file_returns_none(tmp_path):
    assert find_pgpass_entry(host="h", pgpass_path=tmp_path / "nope") is None


# ---------------------------------------------------------------------------
# resolve_db_credentials — env target + matched .pgpass password
# ---------------------------------------------------------------------------
@pytest.fixture
def clean_db_env(monkeypatch):
    for var in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


def test_resolve_picks_matching_env_target_password(clean_db_env, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_pgpass(
        tmp_path,
        "localhost:5432:zerogex:postgres:local_pw",
        "prod.rds.amazonaws.com:5432:zerogex:zerogex_user:prod_pw",
    )
    monkeypatch.setenv("DB_HOST", "prod.rds.amazonaws.com")
    monkeypatch.setenv("DB_NAME", "zerogex")
    monkeypatch.setenv("DB_USER", "zerogex_user")
    creds = resolve_db_credentials()
    assert creds["host"] == "prod.rds.amazonaws.com"
    assert creds["password"] == "prod_pw"  # the bug would have returned local_pw


def test_resolve_unconstrained_uses_first_line(clean_db_env, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_pgpass(tmp_path, "rds.example:5432:zerogex:u:pw1")
    creds = resolve_db_credentials()
    assert creds["host"] == "rds.example"  # env unset → pgpass host adopted (legacy)
    assert creds["password"] == "pw1"


def test_resolve_wildcard_host_resolves_to_env(clean_db_env, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    _write_pgpass(tmp_path, "*:*:*:*:wild_pw")
    monkeypatch.setenv("DB_HOST", "real.host")
    creds = resolve_db_credentials()
    assert creds["host"] == "real.host"  # '*' resolved to the configured host
    assert creds["password"] == "wild_pw"


def test_resolve_no_pgpass_falls_back_to_db_password(clean_db_env, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))  # no .pgpass written here
    monkeypatch.setenv("DB_HOST", "h")
    monkeypatch.setenv("DB_PASSWORD", "envpw")
    creds = resolve_db_credentials()
    assert creds["host"] == "h"
    assert creds["password"] == "envpw"
