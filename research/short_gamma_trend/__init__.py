"""Should the Trade Bias panel have a trend state in short gamma?

Research-only. Changes no production behavior: it imports from ``src``, ``src``
imports nothing from here, and every database statement is a ``SELECT``.

Replays the persisted Trade Bias inputs through production's rule and through
a candidate that relabels short-gamma CHOP minutes with directional flow as a
trend, then asks whether price kept going the called way. See ``README.md``
for the question, the measures and the verdict rule, which was written down
before any data was read.
"""
