"""Invariants for the cutover-rehearsal (``shadow-*``) Makefile targets.

Both of these were real accidents during the ThetaData rehearsal, and both
are silent -- nothing in the output says the command went somewhere else:

1. ``~/.pgpass`` matches on ``host:port:database:user``. A psql connection
   string that leaves the port out matches no line in the file, falls back to
   an interactive password prompt, and in a non-interactive context just
   fails. Every psql invocation here must carry ``port=``.

2. The Makefile does ``-include .env``, and a variable assigned in an included
   makefile beats one inherited from the environment. So
   ``DB_NAME=zerogex_shadow make schema-apply`` applies the schema to
   PRODUCTION while looking like it targeted the scratch database. Only
   ``make schema-apply DB_NAME=zerogex_shadow`` -- a command-line argument --
   overrides it.

   The Python side is the exact opposite: ``src/config.py`` calls
   ``load_dotenv()`` without ``override=True``, so there the environment wins
   and the env-prefix form is the correct one. These tests pin both halves so
   a maintainer "tidying up" the inconsistency doesn't reintroduce either bug.
"""

from __future__ import annotations

import re
from pathlib import Path

MAKEFILE = Path(__file__).resolve().parents[1] / "Makefile"


def _text() -> str:
    return MAKEFILE.read_text(encoding="utf-8")


def _recipe(target: str) -> str:
    """Return the tab-indented recipe body for ``target``, joining continuations."""
    match = re.search(rf"^{re.escape(target)}:[^\n]*\n((?:\t[^\n]*\n)*)", _text(), re.M)
    assert match, f"{target} recipe not found in Makefile"
    return match.group(1).replace("\\\n", " ")


def _prerequisites(target: str) -> list[str]:
    match = re.search(rf"^{re.escape(target)}:([^\n#]*)", _text(), re.M)
    assert match, f"{target} not found in Makefile"
    return match.group(1).split()


def _shadow_targets() -> list[str]:
    """Every ``shadow-*`` target except the guard itself."""
    found = re.findall(r"^(shadow-[a-z-]+):", _text(), re.M)
    assert found, "no shadow-* targets found in Makefile"
    return [t for t in found if t != "shadow-guard"]


def test_shadow_psql_carries_the_port():
    """Without port=, ~/.pgpass matches nothing and psql prompts for a password."""
    match = re.search(r"^SHADOW_PSQL\s*=\s*([^\n]*)$", _text(), re.M)
    assert match, "SHADOW_PSQL not found in Makefile"
    assert "port=$(DB_PORT)" in match.group(1)


def test_shadow_psql_targets_the_shadow_database():
    match = re.search(r"^SHADOW_PSQL\s*=\s*([^\n]*)$", _text(), re.M)
    assert match
    assert "dbname=$(SHADOW_DB)" in match.group(1)
    assert "dbname=$(DB_NAME)" not in match.group(1)


def test_every_psql_in_a_shadow_recipe_carries_the_port():
    for target in _shadow_targets():
        body = _recipe(target)
        for call in re.findall(r'psql "([^"]*)"', body):
            assert "port=" in call, f"{target}: psql call without port= -> {call}"


def test_schema_apply_gets_db_name_as_a_make_argument():
    """`make schema-apply DB_NAME=x`, never `DB_NAME=x make schema-apply`."""
    body = _recipe("shadow-create")
    assert "$(MAKE)" in body, "shadow-create no longer calls schema-apply"
    assert re.search(r"\$\(MAKE\)[^\n]*\bschema-apply\b[^\n]*DB_NAME=", body), (
        "DB_NAME must follow the target name on the sub-make command line; "
        "set as an environment variable it is overridden by -include .env "
        "and the schema is applied to production"
    )


def test_no_shadow_recipe_sets_db_name_as_an_environment_variable_for_make():
    # No trailing \b: it sits after ")" in "$(MAKE)", and ")" followed by a
    # space is not a word boundary, so the pattern never fired.
    env_prefix = re.compile(r"DB_NAME=\S+\s+(?:\$\(MAKE\)|make(?![\w-]))")
    for target in _shadow_targets():
        assert not env_prefix.search(_recipe(target)), (
            f"{target}: DB_NAME=... before make is silently ignored "
            "(-include .env wins) and the command runs against production"
        )


def test_shadow_run_sets_db_name_in_the_environment_for_python():
    """The Python half is the opposite rule: load_dotenv() does not override."""
    body = _recipe("shadow-run")
    assert re.search(
        r"DB_NAME=\$\(SHADOW_DB\)[^\n]*\$\(VENV_PYTHON\)", body
    ), "shadow-run must export DB_NAME into the Python process's environment"
    assert "MARKET_DATA_PROVIDER=$(SHADOW_PROVIDER)" in body


def test_every_shadow_target_runs_the_guard():
    for target in _shadow_targets():
        assert "shadow-guard" in _prerequisites(
            target
        ), f"{target} can run without the SHADOW_DB != DB_NAME check"


def test_guard_refuses_an_empty_or_production_database_name():
    body = _recipe("shadow-guard")
    assert '-z "$(SHADOW_DB)"' in body
    assert '"$(SHADOW_DB)" = "$(DB_NAME)"' in body
    assert "exit 1" in body


def test_shadow_drop_requires_confirmation():
    body = _recipe("shadow-drop")
    assert '"$(CONFIRM)" != "yes"' in body
    dropped = body.index("DROP DATABASE")
    guarded = body.index('"$(CONFIRM)" != "yes"')
    assert guarded < dropped, "the CONFIRM check must precede the DROP"
