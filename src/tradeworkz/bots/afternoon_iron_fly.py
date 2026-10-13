"""AfternoonIronFly — the 0DTE ATM iron butterfly at 13:00-14:30 ET, no gamma gate.

The tape-only twin of :class:`LongGammaIronFly` as its own catalog entry, so
it can be screened with ``make tradeworkz-backtest`` and paper-traded beside
the gated fly rather than only appearing as a sweep arm.

Why it exists
-------------
Two production runs of the pre-screen (``docs/design/tradeworkz-v6-strategies.md``
section 10) put the 0DTE ATM straddle paying out about 70% of what it charged
at 13:30 and 14:30 ET on SPY, QQQ and SPX — three to four standard errors
below fair pricing on each symbol, on ~51 sessions — while the 10:30 and
12:00 straddles sat at the published unconditional level or above fair
pricing. The long-gamma split seen at 13:30 did not carry into 14:30 on the
same sessions, so the best-supported hypothesis on this tape is the clock plus
the quiet-tape gate (realized under the straddle), and the gated fly is the
test of whether the gamma gate adds anything.

What the afternoon case actually is
-----------------------------------
A ratio effect, not more dollars: the naked seller's gross edge in bps of
spot is about the same at 10:30 and 14:30, but the straddle is half the size,
so the costs that scale with it (wings, slippage) consume less of it. Fixed
commissions cut the other way, which is why SPX is the instrument.

Exits
-----
Held to the harness cap (15:55 ET). A 15:40 flat buys back roughly half of a
14:30 straddle's value plus four legs of friction, and on a pinned tape the
50% credit take cannot fire before ~15:45, so the early flat was the fly's
most expensive exit. ``gated/flat15:40`` and ``gated/exit15:05`` in the sweep
keep the earlier exits measurable. On SPY and QQQ, pin risk at the bell is a
live-trading concern the paper harness does not model.
"""

from __future__ import annotations

from src.tradeworkz.bots.short_premium import ShortPremiumBot


class AfternoonIronFly(ShortPremiumBot):
    tier = "0DTE"
    direction_mode = "neutral"
    tagline = "The 0DTE premium at 13:00-14:30, no gamma gate."
    description = (
        "Sells a defined-risk 0DTE ATM iron butterfly between 13:00 and 14:30 ET "
        "whenever the trailing realized move projected over the rest of the day "
        "is under what the ATM straddle charges for it, with no dealer-gamma "
        "gate; holds to the harness cap. The gated long_gamma_iron_fly is the "
        "test of whether the gamma gate adds anything to this."
    )
