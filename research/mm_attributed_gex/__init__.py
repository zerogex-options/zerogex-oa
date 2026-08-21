"""Market-Maker Attributed GEX — research-only experiment.

Tests whether SPX gamma positioning reconstructed from Cboe exchange-classified
Market Maker Open/Close activity carries materially more information than
ZeroGEX's current dealer-gamma methodology.

**Terminology.**  The metric produced here is *Market-Maker Attributed GEX*
(equivalently: Exchange-Classified MM GEX, Reconstructed Market Maker
Positioning, Participant-Attributed Gamma).  It is NOT "true dealer GEX".
Exchange participant tags improve *attribution*; the resulting inventory is
still a reconstruction resting on stated assumptions (see
``docs/design/market-maker-attributed-gex.md``).

**Isolation.**  This package imports FROM ``src`` and is imported BY nothing in
``src``.  It writes no production table, mutates no production module, and
changes no production behavior.  Every gamma number it produces comes from the
*unmodified* production kernels in :mod:`src.analytics.main_engine` so that the
only variable that differs between the two methodologies is how option
positioning is attributed to Market Makers.

Layer map (each layer is independently testable and free of the next):

    schema.py       normalized ParticipantActivity records (exchange-agnostic)
    cboe/           Cboe C1 Open-Close file parsing -> ParticipantActivity
    inventory.py    ParticipantActivity -> MM long/short/net inventory per series
    confidence.py   how complete/trustworthy is a reconstructed inventory
    reconcile.py    independent cross-checks against ZeroGEX open interest
    gex.py          MM inventory -> gamma@spot / flip / net GEX (production kernels)
    walls.py        MM inventory -> strike structure (walls, nodes, profile)
    sources.py      read-only access to the production database
    dataset.py      side-by-side existing-vs-MM research dataset
    stats.py        statistics toolkit (CI / t / bootstrap / Monte Carlo / WF)
    backtest.py     the experiment battery that answers the central question
    report.py       renders the research report from a completed run
    cli.py          `python -m research.mm_attributed_gex.cli ...`
"""

__all__ = [
    "schema",
    "inventory",
    "confidence",
    "reconcile",
    "gex",
    "walls",
    "sources",
    "dataset",
    "stats",
    "backtest",
    "report",
]
