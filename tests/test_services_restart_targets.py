"""Invariants for the ``services-restart`` Makefile targets.

``make services-restart`` restarts the four services one at a time by default,
health-checking each and halting at the first that does not come back, so a
bad deploy never takes down more than one service. ``STOP_ALL_FIRST=1`` keeps
the original stop-all → start-all sequence. Both modes must end on
``api-health-assert``, the deploy gate added after a stop-all → start-all left
the API down while the target still exited 0.

These read the Makefile as text rather than running ``make``: the recipes call
``sudo systemctl``, and a test that shells out to them is one stub away from
restarting production services on the host where the suite also runs.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

MAKEFILE = Path(__file__).resolve().parents[1] / "Makefile"


def _recipe(target: str) -> str:
    """Return the recipe body (tab-indented lines) for a Make target."""
    text = MAKEFILE.read_text(encoding="utf-8")
    match = re.search(rf"^{re.escape(target)}:.*\n((?:(?:\t.*)?\n)*)", text, re.M)
    assert match, f"{target} recipe not found in Makefile"
    return match.group(1)


def _dispatch_branch(value_pattern: str) -> str:
    """Return the sub-target services-restart runs for a STOP_ALL_FIRST case."""
    recipe = _recipe("services-restart")
    match = re.search(
        rf"^\s*{re.escape(value_pattern)}\)\s*\$\(MAKE\).*?(services-restart-[\w-]+)",
        recipe,
        re.M,
    )
    assert match, f"no STOP_ALL_FIRST branch for {value_pattern!r}"
    return match.group(1)


def test_default_restarts_one_at_a_time():
    assert _dispatch_branch('""|0|no|false') == "services-restart-one-by-one"


def test_stop_all_first_keeps_the_original_sequence():
    assert _dispatch_branch("1|yes|true") == "services-restart-stop-all-first"
    recipe = _recipe("services-restart-stop-all-first")
    assert recipe.index("SERVICES_STOP_ORDER") < recipe.index("SERVICES_START_ORDER")
    assert "services-health" in recipe


def test_unknown_stop_all_first_value_is_rejected():
    """A typo must not silently pick a mode."""
    assert re.search(r"^\s*\*\).*exit 2", _recipe("services-restart"), re.M)


def test_one_by_one_health_checks_each_service_and_halts_on_failure():
    recipe = _recipe("services-restart-one-by-one")
    loop = recipe[recipe.index("for svc in") : recipe.index("done")]
    assert "$(SERVICES_START_ORDER)" in loop
    assert loop.index("systemctl stop") < loop.index("systemctl start")
    assert "-health" in loop
    assert re.search(r"if ! systemctl is-active --quiet \$\$svc; then.*?exit 1", loop, re.S)


def test_both_modes_end_on_the_api_deploy_gate():
    for target in ("services-restart-one-by-one", "services-restart-stop-all-first"):
        last_line = _recipe(target).strip().splitlines()[-1]
        assert "api-health-assert" in last_line, target


def test_a_new_symbol_restarts_only_ingestion_then_analytics():
    """Adding a symbol to INGEST_/ANALYTICS_UNDERLYINGS needs those two units
    and no others. The API and signals keep serving, and the two still go
    through the health-checked one-by-one restart."""
    recipe = _recipe("services-restart-ingest-analytics")
    assert "services-restart-one-by-one" in recipe
    assert re.search(
        r'SERVICES_START_ORDER="\$\(INGESTION_SERVICE\) \$\(ANALYTICS_SERVICE\)"', recipe
    )
    assert "API_SERVICE" not in recipe and "SIGNALS_SERVICE" not in recipe


# ── engine split (docs/runbooks/engine_box_split.md) ────────────────────────
# Two markers in /etc/zerogex say what a box runs. The bulk targets act only on
# the services that run on this box: a habitual make services-restart on the web
# box must not try to start the engines, and the engine box has no API to gate.


ENGINES = ["zerogex-oa-ingestion", "zerogex-oa-analytics", "zerogex-oa-signals"]
API = "zerogex-oa-api"


def _lists(tmp_path, *, engines_disabled=False, engine_box=False):
    """SERVICES_START_ORDER / _STOP_ORDER as make expands them. ``make -n``
    prints the expanded echo instead of running anything."""
    disabled = tmp_path / "engines.disabled"
    box = tmp_path / "engine-box"
    for path, present in ((disabled, engines_disabled), (box, engine_box)):
        if present:
            path.write_text("x")
        elif path.exists():
            path.unlink()
    out = subprocess.run(
        [
            "make",
            "-s",
            "-n",
            "-C",
            str(MAKEFILE.parent),
            "--eval=zz-lists: ; @echo"
            " START=[$(SERVICES_START_ORDER)] STOP=[$(SERVICES_STOP_ORDER)]",
            "zz-lists",
            f"ENGINES_DISABLED_MARKER={disabled}",
            f"ENGINE_BOX_MARKER={box}",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    start = re.search(r"START=\[(.*?)\]", out).group(1).split()
    stop = re.search(r"STOP=\[(.*?)\]", out).group(1).split()
    return start, stop


def test_one_box_keeps_all_four_services(tmp_path):
    assert _lists(tmp_path) == (ENGINES + [API], [API] + ENGINES[::-1])


def test_the_web_box_leaves_the_engines_out(tmp_path):
    assert _lists(tmp_path, engines_disabled=True) == ([API], [API])


def test_the_engine_box_leaves_the_api_out(tmp_path):
    assert _lists(tmp_path, engine_box=True) == (ENGINES, ENGINES[::-1])
    assert _lists(tmp_path, engines_disabled=True, engine_box=True) == ([], [])


def test_weekly_vacuum_stops_and_restarts_only_this_boxs_services():
    recipe = _recipe("db-maintain-managed")
    assert "systemctl stop $(SERVICES_STOP_ORDER)" in recipe
    assert "systemctl start $(SERVICES_START_ORDER)" in recipe
    assert not re.search(r"systemctl (stop|start) zerogex-oa-", recipe)


def test_a_new_symbols_restart_refuses_where_the_engines_do_not_run():
    recipe = _recipe("services-restart-ingest-analytics")
    assert recipe.index("ENGINES_DISABLED_MARKER") < recipe.index("services-restart-one-by-one")
    assert "exit 1" in recipe


def test_the_api_gate_passes_on_the_engine_box_before_checking_systemd():
    recipe = _recipe("api-health-assert")
    assert recipe.index("ENGINE_BOX_MARKER") < recipe.index("systemctl is-active")
    assert "services-health" in _recipe("services-restart-stop-all-first")
    assert "$(SERVICES_START_ORDER)" in _recipe("services-health")


def test_make_pull_leaves_the_schema_to_the_web_box():
    recipe = _recipe("pull")
    assert recipe.index("git pull --ff-only") < recipe.index("ENGINE_BOX_MARKER")
    assert recipe.index("ENGINE_BOX_MARKER") < recipe.index("schema-apply")
