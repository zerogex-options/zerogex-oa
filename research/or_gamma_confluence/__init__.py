"""Opening-range extension x gamma confluence — research only.

**Changes no production behaviour.** It imports from ``src``; ``src`` imports
nothing from here.  Every database statement is a ``SELECT``.  Outputs go to
files under ``research_output/``.

The question, in one line: *when price reaches an opening-range extension, does
pre-existing ZeroGEX gamma structure at that price change what happens next?*

Three claims are under test, none of them assumed:

1. **Stretch.** Further from the opening range -> higher probability the
   previous extension trades before the next one.
2. **Confluence.** A gamma level that was ALREADY THERE before price arrived
   raises that probability further.
3. **Regime.** Sign of dealer gamma / side of the flip decides whether to
   expect reversion or continuation at all.

Design notes that are load-bearing rather than stylistic:

* **The look-ahead rule is about visibility, not timestamps.**
  ``gex_summary.timestamp`` is the option-chain instant the levels were
  computed FROM, not the instant they existed.  See :mod:`.levels` and
  :class:`.config.ResearchConfig.availability_clock`.
* **Walls re-centre on spot.**  A wall or GEX rank read at the touch is partly
  CAUSED by the touch.  Confluence is therefore always ranked against the spot
  in force at the lead-time cutoff, never the touch spot.
* **Distances are stored raw, thresholds applied at analysis time.**  The
  dataset never decides that 10 points is "the" confluence distance.

Methodology and the Phase 1 repository assessment:
``docs/design/or-extension-gamma-confluence.md``.
"""
