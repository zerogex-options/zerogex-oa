"""Tests for the customer-facing pattern description extraction.

The pattern modules under ``src/signals/playbook/patterns/`` use a convention
where the first docstring line is a developer-only header
(``Pattern X.Y: id — Name.``) and the natural-language explanation lives in
the next paragraph. The /backtesting form and the /backtesting/insights
tooltip both surface ``BacktestMeta.patterns[].description`` to subscribers,
so it needs to be the readable paragraph, not the dev header.
"""

from __future__ import annotations

import sys
import types

from src.backtesting.meta import _docstring_for_pattern, _extract_description


def test_strips_header_line_and_returns_first_paragraph():
    doc = (
        "Pattern 1.4: ``eod_pressure_drift`` — Last-Hour Hedging Drift.\n"
        "\n"
        "In the last hour of regular trading, dealer 0DTE hedging dominates the\n"
        "tape; the ``eod_pressure`` advanced signal aggregates that directional\n"
        "push.  We lean into it with an ATM 0DTE debit, anchored to VWAP for\n"
        "target and invalidation.\n"
        "\n"
        "Per ``docs/playbook_catalog.md`` §7.1.4.\n"
    )
    description = _extract_description(doc)
    # Lines from the natural-language paragraph, joined with single spaces.
    assert description.startswith(
        "In the last hour of regular trading, dealer 0DTE hedging dominates"
    )
    # Inline `` markers are stripped so the UI doesn't render them.
    assert "``" not in description
    assert "`" not in description
    # The developer-only "Per docs" trailer is dropped.
    assert "Per docs" not in description
    assert "playbook_catalog" not in description


def test_stops_at_dev_cruft_lines_directly_attached_to_paragraph():
    # Some patterns put the "Per docs" line as the final sentence of the same
    # paragraph (no blank-line separator). The extractor must still drop it.
    doc = (
        "Pattern 1.1: ``call_wall_fade`` — Fade Touches of the Call Wall.\n"
        "\n"
        "Long-gamma backdrop (positive net GEX) + price tagging the call wall +\n"
        "flow turning negative + a corroborating advanced signal = sell into the\n"
        "wall.  Per ``docs/playbook_catalog.md`` §7.1.1.\n"
    )
    description = _extract_description(doc)
    assert description.startswith("Long-gamma backdrop")
    assert "Per docs" not in description
    assert "§7.1.1" not in description


def test_stops_at_pr_implementation_notes():
    # Some docstrings have multiple paragraphs where the second is a PR-XX
    # implementation note — drop those too (the customer doesn't care).
    doc = (
        "Pattern 3.5: ``gex_gradient_trend`` — Asymmetric Gamma Drift.\n"
        "\n"
        "Asymmetric dealer gamma above vs below spot creates a multi-day drift\n"
        "toward the lower-gamma direction (less hedging resistance there).\n"
        "\n"
        "Per ``docs/playbook_catalog.md`` §7.3.5.\n"
        "\n"
        "PR-11 simplification: spec calls for \"1 confirming 4-hour bar in\n"
        "direction\".  Without intraday-bar resolution we approximate by checking\n"
        "that the most recent close has moved in the gradient-favored direction\n"
        "relative to the prior close.\n"
    )
    description = _extract_description(doc)
    assert description.startswith("Asymmetric dealer gamma above vs below spot")
    assert "PR-11" not in description
    assert "intraday-bar" not in description


def test_falls_back_to_header_when_no_body():
    # Defensive: if a pattern only has a one-line docstring, return that
    # rather than nothing — at least the UI shows the developer name.
    doc = "Pattern 9.9: ``placeholder`` — Placeholder."
    description = _extract_description(doc)
    assert description == "Pattern 9.9: ``placeholder`` — Placeholder."


def test_handles_empty_docstring():
    assert _extract_description("") == ""
    assert _extract_description("   \n   \n   ") == ""


def test_caps_length_at_280_chars():
    body = "x " * 300
    doc = f"Pattern X.Y: ``thing`` — Thing.\n\n{body}\n"
    description = _extract_description(doc)
    assert len(description) <= 280


def test_collapses_internal_whitespace():
    doc = (
        "Pattern 1.4: ``x`` — X.\n"
        "\n"
        "Word    one\n"
        "word two\tword three.\n"
    )
    description = _extract_description(doc)
    assert description == "Word one word two word three."


def test_drops_leading_blank_lines_in_docstring():
    # __doc__ from inspect.cleandoc or oddly formatted docstrings can have
    # leading blanks. Shouldn't break header detection.
    doc = "\n\n  Pattern 1.4: ``x`` — X.\n\n  Real explanation here.\n"
    description = _extract_description(doc)
    assert description == "Real explanation here."


# ----------------------------------------------------------------------
# _docstring_for_pattern — module-vs-class docstring lookup
# ----------------------------------------------------------------------


def _make_fake_pattern(module_name: str, module_doc: str | None, class_doc: str | None):
    """Build a fake pattern instance whose __module__ points at a temp
    module with ``module_doc`` and whose class has ``class_doc``. Used to
    cover the module-vs-class precedence in _docstring_for_pattern.
    """
    fake_module = types.ModuleType(module_name)
    fake_module.__doc__ = module_doc
    sys.modules[module_name] = fake_module
    cls = type("FakePattern", (), {"__module__": module_name, "__doc__": class_doc})
    return cls()


def test_docstring_for_pattern_prefers_module_doc():
    # Real patterns put the description on the MODULE, not the class — so
    # the lookup must read the module's __doc__ first.
    p = _make_fake_pattern(
        "tests._fake_pattern_module_only",
        "Module-level docstring.",
        None,
    )
    assert _docstring_for_pattern(p) == "Module-level docstring."


def test_docstring_for_pattern_falls_back_to_class_doc():
    # If a future pattern is class-doc-first (rare), don't lose its
    # description just because the module has none.
    p = _make_fake_pattern(
        "tests._fake_pattern_class_only",
        None,
        "Class-level docstring.",
    )
    assert _docstring_for_pattern(p) == "Class-level docstring."


def test_docstring_for_pattern_returns_empty_when_both_missing():
    p = _make_fake_pattern("tests._fake_pattern_none", None, None)
    assert _docstring_for_pattern(p) == ""


def test_docstring_for_pattern_real_pattern_has_module_doc():
    # End-to-end smoke: at least one real pattern in the catalog must have
    # a non-empty module docstring. Cheap insurance against future code that
    # accidentally drops the docstrings.
    from src.signals.playbook.engine import PlaybookEngine

    patterns = PlaybookEngine._discover_builtin_patterns()
    assert any(_docstring_for_pattern(p) for p in patterns), (
        "no playbook pattern has a discoverable docstring — meta descriptions "
        "would all be empty"
    )
