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


def _logical_lines(target: str) -> list[str]:
    """Split a recipe into the shells Make will actually spawn.

    Make runs each recipe LINE in its own shell, joining backslash
    continuations into one. That distinction is the whole point of
    ``test_a_recipe_cannot_guard_itself_with_exit_0`` below.
    """
    match = re.search(rf"^{re.escape(target)}:[^\n]*\n((?:\t[^\n]*\n)*)", _text(), re.M)
    assert match, f"{target} recipe not found in Makefile"
    joined = match.group(1).replace("\\\n", " ")
    return [ln for ln in joined.split("\n") if ln.strip()]


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


def test_shadow_psql_never_prompts_for_a_password():
    """-w is what turns a missing ~/.pgpass line into an error, not a hang.

    Without it psql falls back to an interactive prompt. In a scripted target
    that blocks forever, and it reads as "the credentials are broken" rather
    than "the .pgpass line for the production database does not cover this
    one". This is the failure this whole block exists to prevent.
    """
    match = re.search(r"^SHADOW_PSQL\s*=\s*([^\n]*)$", _text(), re.M)
    assert match, "SHADOW_PSQL not found in Makefile"
    assert re.search(
        r"\bpsql\s+-w\b", match.group(1)
    ), "SHADOW_PSQL must pass -w so a missing ~/.pgpass line fails fast"


def test_shadow_create_proves_it_can_authenticate_before_applying_schema():
    """schema-apply has no -w of its own, so it would prompt and hang.

    shadow-create must test the connection itself and say what to do, which
    is why the check sits between CREATE DATABASE and the sub-make.
    """
    lines = _logical_lines("shadow-create")
    checked = next((i for i, ln in enumerate(lines) if "$(SHADOW_PSQL)" in ln), None)
    applied = next((i for i, ln in enumerate(lines) if "schema-apply" in ln), None)
    assert checked is not None, "shadow-create no longer tests the connection"
    assert applied is not None, "shadow-create no longer calls schema-apply"
    assert checked < applied, (
        "the connection check must run BEFORE schema-apply, which has no -w "
        "and would sit on an interactive password prompt"
    )
    assert (
        "shadow-pgpass" in lines[checked]
    ), "the failure message must name the fix: make shadow-pgpass"


def test_shadow_pgpass_never_prints_the_matched_line():
    """The copied line contains the password. It must only ever reach a file.

    Anchored on what follows sed's input file, not on the presence of a ">"
    somewhere in the line: the recipe's own help text contains "<password>",
    and that stray ">" made the first version of this test pass against a
    `| cat` mutant.
    """
    # The s|...|...|p delimiters are pipes, so "no pipe in the line" is no use.
    # Look at the first character after sed's input file instead.
    sed_call = re.compile(r'sed -n\s+"[^"]*"\s*~/\.pgpass\s*(\S)')
    found = False
    for line in _logical_lines("shadow-pgpass"):
        for match in sed_call.finditer(line):
            found = True
            assert match.group(1) == ">", (
                "sed reads the ~/.pgpass line including its password; its "
                f"output must be redirected to a file, not {match.group(1)!r}"
            )
    assert found, "shadow-pgpass no longer copies the line with sed"


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
    """The CONFIRM check and the DROP must be in the SAME shell.

    Textual order is not enough, and asserting it is what let the original
    bug through. Make spawns a shell per recipe line, so a guard that ends
    with `exit 0` on its own line exits that shell with SUCCESS -- make then
    runs the next line. Split across lines, this recipe printed "Dry run" and
    dropped the database anyway.
    """
    guarding, dropping = None, None
    for i, line in enumerate(_logical_lines("shadow-drop")):
        if '"$(CONFIRM)" != "yes"' in line:
            guarding = i
        if "DROP DATABASE" in line:
            dropping = i
    assert guarding is not None, "shadow-drop no longer checks CONFIRM"
    assert dropping is not None, "shadow-drop no longer drops anything"
    assert guarding == dropping, (
        "the CONFIRM check and the DROP are on separate recipe lines, so they "
        "run in separate shells and the guard's `exit 0` does not stop the "
        "DROP -- join them with `; \\` into one line"
    )


def test_a_recipe_cannot_guard_itself_with_exit_0():
    """`exit 0` cannot stop make, so it must be on a recipe's LAST shell.

    Anywhere earlier it reads as an abort but is really a no-op: the shell
    exits 0, make sees success, and every following line runs.
    """
    for target in _shadow_targets() + ["shadow-guard"]:
        lines = _logical_lines(target)
        for i, line in enumerate(lines):
            if "exit 0" in line:
                assert i == len(lines) - 1, (
                    f"{target}: `exit 0` on shell {i + 1} of {len(lines)} does "
                    "not stop make; the lines after it still run"
                )


def test_shadow_run_reloads_the_whole_env_in_the_shell():
    """`-include .env` parses that file AS A MAKEFILE, so every `$` in it is a
    variable reference.

    `SYMBOL_ALIASES=SPX=$SPXW.X,NDX=$NDXP.X` becomes `SPX=PXW.X,NDX=DXP.X`,
    because `$S` and `$N` are empty make variables, and `export` pushes that
    into every child. The 2026-09-24 rehearsal ran engines for "PXW.X" and
    "DXP.X", which are not symbols -- and, having lost the `$`, they also
    stopped looking like indices, so they were sent to the stock endpoint.

    Fixing one variable is not enough: the first attempt read
    INGEST_UNDERLYINGS as text and the run STILL came up with PXW.X, because
    on this deployment the `$` lives in SYMBOL_ALIASES one step later. So the
    recipe reloads all of .env rather than naming variables one at a time.
    """
    body = _recipe("shadow-run")
    assert "< .env" in body, "the recipe must re-read .env itself, in the shell"
    assert 'export "$$key=$$val"' in body, "and export each value literally"
    for var in (
        "INGEST_UNDERLYINGS",
        "INGEST_UNDERLYING",
        "SYMBOL_ALIASES",
        "INGEST_MONTHLY_UNDERLYING_ALIASES",
        "OPTION_ROOT_ALIASES",
    ):
        assert f"$({var})" not in body, (
            f"{var} must not come through make -- any $ in its value is eaten "
            "as an empty make variable"
        )


def test_shadow_run_sets_the_shadow_database_after_loading_env():
    """.env sets DB_NAME to PRODUCTION, so the override must come after it.

    Loaded first, the rehearsal would write into the live tables.
    """
    body = _recipe("shadow-run")
    assert body.index("< .env") < body.index("DB_NAME=$(SHADOW_DB)")
    assert body.index("< .env") < body.index("MARKET_DATA_PROVIDER=$(SHADOW_PROVIDER)")


def test_shadow_run_refuses_to_start_with_no_underlyings():
    """An empty list silently becomes the engine's SPY default, which would
    look like a successful rehearsal covering one quarter of production."""
    body = _recipe("shadow-run")
    assert '-z "$$INGEST_UNDERLYINGS"' in body
    assert "exit 1" in body


def test_shadow_run_writes_a_log_that_outlives_the_terminal():
    """Output has to survive the tmux session going away.

    It did not, twice on 2026-09-24: the run was inspected only in the pane,
    the pane was killed, and every line of evidence went with it. The G2 gate
    also reads this log at the close -- "no circuit-breaker trips, no
    sustained reconnects" has to be read from somewhere, and tmux scrollback
    overflows during a full session.
    """
    body = _recipe("shadow-run")
    assert "tee -a" in body, "the engine's output must be appended to a file"
    assert "2>&1" in body, "stderr carries the logging -- it must be captured too"
    assert re.search(r"date \+%F", body), (
        "the log name must be computed in the SHELL; a make $(shell date) is "
        "evaluated once at parse time and would name a stale file"
    )


def test_shadow_run_announces_where_it_is_logging():
    body = _recipe("shadow-run")
    assert "Logging to:" in body


def test_shadow_status_never_kills_anything():
    """It is the target you reach for when unsure, so it must be read-only."""
    body = _recipe("shadow-status")
    for danger in ("kill", "DROP", "DELETE", "rm "):
        assert danger not in body, f"shadow-status must not {danger.strip()}"


def test_shadow_status_counts_only_the_current_run():
    """The log accumulates every run of the day, so whole-file counts mislead.

    On 2026-09-24 a healthy run reported "errors: 123" because two earlier
    failed starts were in the same file. A status line you have to
    second-guess is worse than none.

    Checked per counter, not once for the whole recipe: the first version of
    this test asserted only that "tail -n +$START" appeared somewhere, and
    passed against a mutant that reverted just the initialized count.
    """
    body = _recipe("shadow-status")
    assert "=== shadow-run " in body, "it must find the last run header"

    for pattern in ("Streaming initialized", " - ERROR - "):
        counter = f"grep -c '{pattern}'"
        assert counter in body, f"no counter for {pattern!r}"
        for match in re.finditer(re.escape(counter), body):
            # Whatever feeds this grep must be the scoped stream, not $LOG.
            before = body[: match.start()]
            piece = before[before.rfind("$$(") :]
            assert "tail -n +$$START" in piece, (
                f"the {pattern!r} count reads the whole file; earlier runs in "
                "the same log would be counted as this run's"
            )


def test_option_volume_is_summed_per_contract_not_per_row():
    """option_chains is a TIME SERIES and `volume` is session-cumulative.

    A plain SUM(volume) over the rows multiplies every contract by its
    snapshot count -- at a 5s poll that is hundreds of times its real
    volume, and it would look plausible rather than obviously wrong.
    Verified against PostgreSQL with a contract seeded at 10 -> 50 -> 120:
    the query returns 120, not 180.
    """
    body = _recipe("shadow-compare")
    assert (
        "DISTINCT ON (option_symbol)" in body
    ), "option volume must be taken from the latest row per contract"
    assert (
        "ORDER BY option_symbol, timestamp DESC" in body
    ), "DISTINCT ON without that ORDER BY picks an arbitrary row, not the latest"
    # No SUM(volume) may read option_chains directly, outside the CTE.
    for match in re.finditer(r"SUM\(volume\)", body):
        window = body[max(0, match.start() - 600) : match.start()]
        assert (
            "DISTINCT ON (option_symbol)" in window
        ), "a SUM(volume) is reading option_chains rows directly"


def test_shadow_compare_reports_both_contract_count_and_volume():
    """The percentage alone cannot answer whether a gap matters.

    NDX on 2026-09-25 showed 39.1% of contracts traded on TradeStation
    against 26.1% on ThetaData. Whether that moves the published flow
    numbers depends entirely on whether the missing contracts carried any
    size, which only the volume figure says.
    """
    body = _recipe("shadow-compare")
    assert "traded_pct" in body, "the share of contracts that traded"
    assert "option_volume" in body, "and the volume behind them"


def test_shadow_compare_checks_the_volatility_bars_land_on_the_grid():
    """``off_grid`` is the whole reason this block exists.

    ``vix_bars`` and ``vxn_bars`` hold one row per 5-minute bar. TradeStation
    delivers finished bars; the provider seam delivers a MARK every few
    seconds, which the ingester folds into its bucket. If that folding ever
    stops, the tables fill at the poll rate instead -- ~23,000 rows a session
    against 78 -- and nothing about the numbers looks wrong until the gauge's
    "last ten bars" quietly becomes the last ten seconds.

    Epoch seconds modulo 300 is the check: the epoch is aligned to midnight
    UTC and ET is a whole-hour offset, so a 5-minute boundary in either zone
    is a multiple of 300. Verified against PostgreSQL with one deliberately
    off-grid row seeded at 09:42:17 -- the query returned off_grid = 1.
    """
    body = _recipe("shadow-compare")
    block = body[body.index("=== vix_bars / vxn_bars") :]
    # Asserted on the FROM clause and the column alias, not on the table names
    # loose in the text: the block's own explanatory echo lines mention both
    # "vxn_bars" and "off_grid", so a looser check passes on the prose alone
    # after the query has stopped reading either.
    assert block.count("FROM vix_bars WHERE") == 2, "vix_bars is not read from both databases"
    assert block.count("FROM vxn_bars WHERE") == 2, "vxn_bars is not read from both databases"
    assert block.count("AS off_grid") == 2, "no off-grid column in the output"
    grid = "EXTRACT(EPOCH FROM timestamp)::bigint % 300 <> 0"
    assert grid in block, "the grid check must count rows OFF the boundary, not merely on it"
    assert "$(PSQL) -c" in block and "$(SHADOW_PSQL) -c" in block, (
        "the check must run against BOTH databases -- a rehearsal figure with "
        "nothing to compare it to answers nothing"
    )
    # Two databases, two tables each.
    assert block.count(grid) == 4, f"grid check appears {block.count(grid)} times, expected 4"


def test_shadow_compare_counts_flat_volatility_candles():
    """A candle with no body and no wicks means the marks never accumulated.

    The conflict clause takes first-seen open, running high and low, last
    close. Overwrite all four instead -- which is what the TradeStation
    upsert does, correctly, for a finished bar -- and every row becomes four
    copies of whichever mark landed last. The close stays right, so the
    gauge's level is right and only its candles are wrong.
    """
    body = _recipe("shadow-compare")
    assert (
        "COUNT(*) FILTER (WHERE open = high AND high = low AND low = close) AS flat" in body
    ), "nothing counts flat candles, so a flattened accumulation reads as healthy"
