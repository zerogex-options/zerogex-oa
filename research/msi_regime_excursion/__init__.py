"""Does the MSI regime gauge predict realized forward price excursion?

Research-only. Changes no production behavior: it imports from ``src``, ``src``
imports nothing from here, every database statement is a ``SELECT``, and all
output goes to files or stdout.

The question, stated so it can fail: the product bands the 0-100 Composite
Score / MSI into four regimes whose copy makes explicit claims about how far
price travels ("trends can run" / "range-bound, fade extremes"). If those
claims hold, forward excursion measured after a high-MSI reading must differ
from forward excursion after a low-MSI reading by more than sampling noise --
and by enough to matter next to the unconditional base rate for the same
instrument and horizon.

See ``README.md`` for how to run it and ``docs/design/msi-regime-excursion.md``
for the methodology.
"""
