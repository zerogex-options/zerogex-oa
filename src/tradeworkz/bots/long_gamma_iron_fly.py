"""LongGammaIronFly — sell the 0DTE ATM straddle, defined-risk, when dealers are long gamma.

The mechanism
-------------
Two forced counterparties. Dealers who are long gamma must sell every rally
and buy every dip to stay delta-neutral — a mechanical, price-insensitive flow
that compresses realized range, and the one effect ZeroGEX's own data has
confirmed at every horizon on every instrument (``docs/design/
msi-regime-excursion.md`` section 5: the bottom regime bands travel 0.81-0.97x
the base rate, short gamma 1.26x). And 0DTE buyers pay for same-day convexity
regardless of whether the day will move: the 0DTE variance risk premium
(Beckmeyer, Branger & Gayda 2023; Bandi, Fusari & Reno 2023), the most
replicated anomaly in index options. This bot sells to the second counterparty
only while the first is at full strength.

The edge-defining question is WHEN NOT TO SELL, and every gate is a stand-down:
top-quartile positive gamma by the symbol's own history (not merely positive),
the magnitude-only MSI band in chop or reversal, trailing realized vol under
the ATM implied so the premium is accruing now, neither gamma flip within a
sigma of the strike, no VIX shock in the last half hour, ATM IV not in its
top quintile, no scheduled macro release, and the morning forecast not calling
expansion. The structure is an ATM iron butterfly with wings a sigma-multiple
out, where sigma is the market's own remaining move read from the straddle.

Why this is not a dead row
--------------------------
``range_iron_condor`` (PF 0.67, n=52) sold premium AT THE WALLS — short strikes
two increments inside each wall, where ``wall_break_stats`` says a tested wall
breaks 31-50% of the time within the hour — gated on an absolute VIX < 18 and
any positive gamma, entered from 10:00 with a flat 180-minute hold and no
credit take or stop. This bot never reads a wall, centers on the money where
the credit per unit of wing is largest, gates on the symbol-relative
percentile and a measured realized-under-implied condition, and books or cuts
on the credit. ``vol_expansion_straddle`` (PF 0.44) bought vol in exactly these
sessions; its post-mortem ("compressed-vol entry selects the sessions least
likely to deliver a move") is this bot's thesis read from the other side.

The controls that decide whether the gamma gate earns its place: the same fly
with ``required_regimes: "any"`` (pure clock plus variance premium) and with
the realized-under-implied gate alone. If either matches the gated bot's
profit factor within 0.1, the gamma conditioning adds nothing and the catalog
says so.
"""

from __future__ import annotations

from src.tradeworkz.bots.short_premium import ShortPremiumBot


class LongGammaIronFly(ShortPremiumBot):
    tier = "0DTE"
    direction_mode = "neutral"
    tagline = "Top-quartile long gamma, quiet tape. Sell the 0DTE straddle, wings on."
    description = (
        "Sells a defined-risk 0DTE ATM iron butterfly between 10:30 and 13:30 ET "
        "only when dealer gamma is in the symbol's top quartile, the magnitude-"
        "only MSI reads chop or reversal, trailing realized vol is under the ATM "
        "implied, and no flip, VIX shock, elevated IV or scheduled release says "
        "stand down. Wings sit 1.25 sigma out on the straddle's own remaining "
        "move; exits book half the credit or cut at one credit."
    )
