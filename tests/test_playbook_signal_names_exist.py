"""Every signal name and range label a playbook pattern looks up must exist.

Patterns asked for ``"0dte_position_imbalance"`` while the engine and both
context builders key that signal ``"zero_dte_position_imbalance"``, and they
compared range_break_imminence's label to ``"Range Fade"`` after it had been
renamed ``"Range-Bound"``. Each lookup silently missed: zero_dte_imbalance_drift
reported "signal unavailable" every cycle, pin_risk_premium_sell never applied
its "0DTE flow too directional" veto and refused the most range-bound state,
and the Range checks in squeeze_breakout and zero_dte_imbalance_drift never
fired. The pattern tests built their snapshots with the same wrong names, so
they passed. This test reads the pattern sources instead.
"""

from __future__ import annotations

import re
from pathlib import Path

from src.signals.playbook.context_builder import (  # noqa: I001 (import order matters)
    ADVANCED_SIGNAL_NAMES,
    BASIC_SIGNAL_NAMES,
    MSI_COMPONENT_SIGNAL_NAMES,
)
from src.signals.advanced.range_break_imminence import RangeBreakImminenceSignal

PATTERNS = Path(__file__).resolve().parent.parent / "src" / "signals" / "playbook" / "patterns"
KNOWN_SIGNALS = (
    set(ADVANCED_SIGNAL_NAMES) | set(BASIC_SIGNAL_NAMES) | set(MSI_COMPONENT_SIGNAL_NAMES)
)


def _sources() -> dict[str, str]:
    return {p.name: p.read_text() for p in sorted(PATTERNS.glob("*.py")) if p.name != "__init__.py"}


def test_every_signal_a_pattern_looks_up_is_one_the_engines_emit():
    unknown = []
    for name, src in _sources().items():
        looked_up = set(re.findall(r'ctx\.signal\(\s*"([a-z0-9_]+)"', src))
        for block in re.findall(r"confluence_signals_(?:for|against)\s*=\s*\(([^)]*)\)", src):
            looked_up |= set(re.findall(r'"([a-z0-9_]+)"', block))
        unknown += [f"{name}: {s}" for s in sorted(looked_up - KNOWN_SIGNALS)]
    assert not unknown, f"patterns reference signals nothing emits: {unknown}"


def _emitted_range_labels() -> set[str]:
    labels = set()
    for imminence in range(0, 101):
        for direction in (-1.0, 0.0, 1.0):
            label, _ = RangeBreakImminenceSignal._label_and_playbook(float(imminence), direction)
            labels.add(label)
    return labels


def test_every_range_label_a_pattern_compares_to_is_one_range_break_imminence_emits():
    emitted = _emitted_range_labels()
    used = set()
    for src in _sources().values():
        used |= set(
            re.findall(
                r'(?:label\b[^\n]*?|rbi_label[^\n]*?)"((?:Range|Weak|Break)[A-Za-z -]*)"', src
            )
        )
    assert used, "no label comparisons found; the pattern is stale"
    assert used <= emitted, f"labels compared but never emitted: {sorted(used - emitted)}"
