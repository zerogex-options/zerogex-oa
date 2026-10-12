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
the trailing realized move projected over the rest of the day under what the
ATM straddle charges for it (dollars against dollars, on the cone model's
diurnal profile), neither gamma flip within a sigma of the strike, no VIX
shock in the last half hour, no scheduled macro release, and the morning
forecast not calling expansion. The structure is an ATM iron butterfly with
wings a sigma-multiple out, where sigma is the market's own remaining move
read from the straddle.

What the adversarial review (2026-10-11) changed before the first screen
------------------------------------------------------------------------
* The only clock-and-tape-adjusted gamma figures on this platform are SHORT
  gamma at 1.08x and the "neither" zone at 0.92x; long gamma was never
  reported and is implied to be about 1.0x. The MSI band ratios are against
  the unconditional base rate. So the prior that the gamma gate adds
  anything beyond a quiet tape is weaker than the designers assumed, and the
  tape-only twin is the PRIMARY comparison, not a control.
* The magnitude band gate is off by default (a sweep axis) and the ATM IV
  percentile gate is off (it ranked two different quantities).
* The realized-vs-implied gate now compares dollars with dollars; the
  annualized-vol form mixed an RTH and a calendar convention and read ~0.43
  for a fair straddle.
* The credit stop is 0.5x the credit; at a 1.25-sigma wing the wing itself is
  the loss, roughly 0.9-1.0x the credit (the wing is 1.57x the straddle mid
  out, the credit ~0.8x the mid), so a 1.0x stop could never fire.
* Roughly 35 gated trades in 80 sessions cannot distinguish PF 1.1 from 1.0
  (the PF sampling error is of order 0.5). The 80-session screen can return
  ``insufficient`` or ``no-edge`` on the gamma conditioning, never a defensible
  ``edge``; the paired per-session comparison of the gated fly against its
  twins on the SAME sessions is the statistic to read, and the pre-screen
  SQL's straddle-versus-realized table is the option-free test to run first.

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
        "only when dealer gamma is in the symbol's top quartile, the trailing "
        "realized move projected over the rest of the day is under what the ATM "
        "straddle charges for it, and no flip, VIX shock or scheduled release "
        "says stand down. Wings sit 1.25 sigma out on the straddle's own "
        "remaining move; exits book half the credit or cut at half of it."
    )
