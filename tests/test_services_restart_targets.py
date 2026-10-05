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
