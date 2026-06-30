"""Tests for the customer-facing pattern description extraction.

The pattern modules under ``src/signals/playbook/patterns/`` use a convention
where the first docstring line is a developer-only header
(``Pattern X.Y: id — Name.``) and the natural-language explanation lives in
the next paragraph. The /backtesting form and the /backtesting/insights
tooltip both surface ``BacktestMeta.patterns[].description`` to subscribers,
so it needs to be the readable paragraph, not the dev header.
"""

from __future__ import annotations

from src.backtesting.meta import _extract_description


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
