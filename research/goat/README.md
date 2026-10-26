# GOAT — running the backtest

Jim Edwards' GOAT midband setup, written as a NinjaTrader 8 strategy so it can
be tested in Strategy Analyzer and produce a trade list.

**Confidential.** This encodes a partner's trading method, shared in
confidence. It stays in this repository. It does not go in
`zerogex-web/frontend/public/`, which is served at the site root, and it is not
part of any published package.

## Why the backtest runs on his machine, not ours

His charts are range bars: 3 Range on YM, 5 Range on NQ. Range bars are built
from ticks, so one forms whenever price travels far enough — about a second and
a half in a fast move, up to half a minute when it is quiet. ZeroGEX stores
one-minute OHLC and nothing finer, for the cash indexes and for
`futures_quotes` alike. There are no tick tables in the schema.

So in exactly the moves where the setup fires, twenty or thirty of his bars fit
inside one of our rows. The pinch, the entry bar and a 2-point stop all happen
inside a single data point on our side. The strategy half of this question can
only be answered where the bars are.

What ZeroGEX can answer is the other half. His second target is either an
8-tick trail or a *predrawn level*, and he named ZeroGEX as one of the level
sources. Joining his exported trade list to our stored level history answers
whether the second lot does better with a level in front of it. That join is
already built: `GammaTimeline.as_of()` in `research/or_gamma_confluence/levels.py`
does the point-in-time lookup off the publish clock, and
`research/or_gamma_confluence/basis.py` carries NDX levels onto the NQ axis.

## Read this before trusting any result

**Range bars need tick data.** If Strategy Analyzer does not have ticks for the
period, NinjaTrader builds the range bars out of minute data instead. The bars
will not match the ones on the live chart — same settings, different bars,
different trades, and no error message anywhere. This is the single most
likely way to get a confident wrong answer out of this.

Check it first:

1. Tools → Historical Data Manager → Load tab.
2. Pick the instrument, set Type to **Tick**, and look at the date range that
   actually has data.
3. Only backtest inside that range.

If the tick history is short, download more (Historical Data Manager → Download)
before running anything. A backtest over a period with no ticks will still
produce a tidy equity curve. It just will not be about the GOAT.

## Running it

1. **Install.** Copy `ZeroGexGoat.cs` into
   `Documents\NinjaTrader 8\bin\Custom\Strategies`, then open New →
   NinjaScript Editor and Compile (F5). The error list should be empty.
   Errors that name some other file are another script on that machine
   blocking the compile, not this one.

   Do not use File → Utilities → Import NinjaScript for this. That importer
   takes a `.zip` exported from NinjaTrader, not a bare `.cs` (see
   `zerogex-web/assets/ninjatrader/README.md` for what an export contains).
2. **Open Strategy Analyzer.** New → Strategy Analyzer.
3. **Pick the instrument and bar type.** Instrument `YM 12-26`, Bars type
   **Range**, Value **3**. (Or `NQ 12-26` at Range 5.)
4. **Set the date range** to a period you confirmed has tick data.
5. **Select `ZeroGexGoat`** in the strategy list.
6. **Check the defaults** against the chart settings — they ship matching the
   simplified chart: midband SMA 55, EE line HMA 22, stochastic 3/3 with 60/40
   thresholds.
7. **Set slippage and commission.** Both default to zero in Strategy Analyzer,
   and on a 4-tick stop that is not a rounding error — it is most of the edge.
   Put in what the broker actually charges.
8. Run.

To export the trade list: right-click the results grid → Export → CSV. That
file is what the level join consumes.

## Running it on a live chart

It was written for Strategy Analyzer, and live order handling has failure
modes a backtest cannot show. Jim's first live session (2026-10-08, sim
account) found one. When the first bar after entry ran straight against the
trade, the tick that closed that bar was also lot 1's stop price. The stop and
lot 1's market exit both filled, so one contract too many traded, leaving
an unprotected position on the wrong side that looked like a backwards entry.
`ExitTarget1` now leaves lot 1 to its stop when price is that close to it, and
`OnPositionUpdate` closes any wrong-side position as a backstop. The compile
check cannot verify either; only a live sim session can. Jim's demo log of
2026-10-23 showed the backstop's first version failing: it sent an
`ExitLong`, NinjaTrader ignored it, and the contract stayed open (see below).

Rules that keep live runs readable:

- **One copy at a time.** Control Center → Strategies tab lists every running
  instance, including ones on other charts. A copy he thought was off kept
  trading from another chart.
- **No manual trading on the same instrument and account while it is on.**
  NinjaTrader plots the account's executions on every chart of that
  instrument, so its trades appear on his manual chart. And closing its
  position by hand leaves the strategy still believing it holds one.
- **To get out of a trade, untick Enabled first,** then flatten from the
  SuperDOM.
- **A stop the market refuses closes its lot at market, and GA keeps
  running.** A stop is placed just after its entry fills and moved when a bar
  closes, and in a fast market price can be through the level before the
  order arrives. A new stop comes back `Rejected` ("Please check the order
  price. The current price is outside the price limits set for this
  product"); a move comes back "Unable to change order" (`InvalidPrice`),
  with the stop left where it was. Under NinjaTrader's default,
  `RealtimeErrorHandling.StopCancelClose`, either one switched GA off and
  flattened it. Jim's full log of 2026-10-23 (NQ, 3 contracts per lot, on
  his NinjaTrader demo account) has four: new stops at 09:01, 09:59 and
  10:14, each sent within half a second of the entry fill, and a stop move
  at 10:24. At 10:14 the default's own close order was sized while lot
  1's stop was still filling, sold 4 when 3 were left, and left the account
  1 short; GA was re-enabled on top of it. GA now runs with
  `IgnoreAllErrors` (set again in Configure, so an older saved copy cannot
  bring the default back) and answers every error in `OnOrderUpdate`: a
  refused "Stop loss" exits that entry signal at market, once, and its stop
  is not moved again; anything else calls `CloseStrategy`, which does what
  the default did. `StopCancelCloseIgnoreRejects` is not enough, because it
  still stops the strategy on "Unable to change order".
- **A contract left against the trade closes GA's position and switches it
  off.** The same log has lot 1's profit target filling one contract after
  its stop had filled the lot, at 08:46 and 11:23: NinjaTrader shrank the
  target to the one contract it thought was left instead of cancelling it.
  `OnPositionUpdate` logged the overfill and sent `ExitLong("Overfill")`,
  NinjaTrader never submitted it (most likely because the contract belongs
  to no entry), and
  the contract sat unprotected until Jim closed it by hand: 3 minutes the
  first time, 1 hour 48 minutes the second, during which GA took no trades
  because it was not flat. The backstop now calls `CloseStrategy`, which
  closes the position itself and switches GA off.
- **GA starts live only once its replayed trades are flat.** Jim's copy ran
  with Start behavior "Immediately submit", which sends the stops and targets
  of whatever trade the chart's history ends in. At 13:34 he enabled GA while
  that history held 6 long and the account was flat: the stops filled and
  left the account 6 short, under every GA trade until he closed it about 3
  minutes later. Configure now turns either "Immediately submit" choice into
  its "Wait until flat" counterpart. GA's default was always "Wait until
  flat", so his saved copy carried the change.
- His first pasted log was manual ATM trades on another account (orders
  named `Entry` and `Stop1`), not GA.
- **A strategy label that starts with (D) means GA is off on that chart.** A
  disabled strategy draws no trades at all, past or present, so a chart in
  that state cannot answer "why didn't it trade here?".
- **Every distance setting is in ticks, and a tick is not the same size
  everywhere.** On NQ it is 0.25 point, on YM a full point, so the same
  number asks for four times the distance on YM. Jim raised the minimum slope
  to 10 for NQ (2.5 points) and the same 10 on YM (10 points) filtered out
  setups he wanted. Keep one saved template per instrument (the template link
  at the bottom of the settings window).

**Dead zones** (Jim's ask, 2026-10-13) are up to three time windows where no
new trade opens; open trades are still managed. They are HHMM on the 24-hour
clock, in whatever time zone the chart shows, and off while from equals to. A
window whose from is after its to wraps past midnight. A window runs from its
from minute up to, but not including, its to minute, so one ending at 829
leaves 8:29 to 8:30 open.

**The pumpkin line** (Jim's ask, 2026-10-15 and 10-17) is an HMA(100), 80
on his later charts, that he reads as an exhaustion line. With "Require pumpkin in sync" ticked, a trade
is skipped when the pumpkin is flat or curling against it, unless the
pumpkin sits at least "room" ticks beyond the entry in the trade's direction
(above a long, below a short): far enough to reach breakeven before price
meets it. Off by default; see open question 6 for the choices it makes.

## Why didn't it take that trade?

Tick **Label skipped setups (why GA passed)** and re-enable. On each near miss
GA writes the rules the bar failed, one per line, under the bar for a long and
over it for a short: `pinch 9 > 6` means the yellow line came no closer than 9
ticks to the midband and the setting allows 6; `slope 8 < 10` means the
midband moved 8 ticks over the lookback against a minimum of 10. The other
codes are `width`, `EE`, `mid`, `pumpkin`, `break`, `flip` and `zone` (a dead
zone).

What counts as a near miss: a bar closing the way the midband points, in the
entry bar color, that failed one to three rules. Bars in other colors, bars
failing four or more rules, and bars within two of a previous label get none,
which keeps a trending chart readable. The labels come from the same checks
the entries use, so a label can never disagree with what GA did, and turning
them on never changes a trade. Only the newest **Labels to keep** (300 by
default) stay on the chart: the tags roll over, so each new label past that
replaces the oldest. On Jim's 5-range NQ chart 300 was a few days, and he
studies 30, hence the setting. Each label is a drawn object, and thousands of
them slow a chart down.

Before this existed, each "why not?" was answered by measuring a screenshot.
Every one in Jim's 2026-10-15/17 batch came down to a setting: the pinch on
three NQ shorts, the minimum slope on two YM longs and an NQ long, and a
dead zone on a YM short at 8:30.

## Optimizing

Strategy Analyzer's Optimize mode sweeps any parameter with a range. Everything
in the strategy is a parameter, deliberately — the judgment calls are settings
to be tested rather than decisions baked in by whoever typed it up.

Two things worth knowing before building a grid:

**Bar range and lookback are not independent.** The midband lookback is counted
in bars, and on a range chart a bar is not a unit of time. Thirty bars on a
2-range chart and thirty on a 5-range chart are different lookbacks in clock
time, and both shorten as the market speeds up. Sweep them together, and do not
read a 2-range result and a 5-range result as the same setup with a different
filter.

**A big grid on a small sample finds something whatever is there.** Several
hundred combinations against a few hundred trades will produce an impressive
best cell by arithmetic alone. Hold out a date range before optimizing, and
check the winner on it afterwards.

**Optimizations run before 2026-10-19 are slightly off.** The strategy reuses
one instance across optimization passes, and until then it did not reset the
pinch tracker between them. Each pass after the first could treat the last
pass's final pinch as recent and take a few extra trades before the first
real pinch. Single backtests were not affected.

## Bar colors

The entry rule is written in terms of bar color, and the colors come from
jeStochastics' paint bars. Jim has **remapped the indicator's default brushes**,
so the colors do not mean what the stock indicator's field names suggest. From
his own settings dialog:

| Condition | Color |
|---|---|
| K > 60, close up | WHITE |
| K > 60, close down | DARK RED |
| K < 40, close up | FOREST GREEN |
| K < 40, close down | BLACK |
| 40 ≤ K ≤ 60, close up | LIME |
| 40 ≤ K ≤ 60, close down | RED |

So *green* and *white* are both up-closing bars at rising K; *red* and *black*
are both down-closing bars at falling K. "Jumping from black to white and
skipping green" is one bar carrying K from under 40 to over 60, which is why he
reads it as stronger.

The strategy reimplements this rather than referencing `jeStochastics`, so the
backtest does not depend on a third-party indicator being installed. Under
`Calculate.OnBarClose` the arithmetic is identical; the indicator's intrabar
path (a running high/low updated per tick) is only reachable on each-tick
calculation, which neither his chart nor this strategy uses.

The colors only match his chart while the strategy's stochastic settings
(%K 3, smoothing 3, 60/40) match that chart's jeStochastics. His YM chart's do.
Check any other chart before trusting a color filter on it.

`EntryBarColor` filters on the entry bar's own color. Jim asked for it on
2026-10-09 ("the red/green is a good filter"). **Bright** means lime for a
long and red for a short. **Skip** means white or black, which with
`RequireColorFlip` on is the black-straight-to-white jump. **BrightOrSkip**
accepts either.

## Open questions

These are the places where the written rules left a genuine choice, and the
strategy guesses rather than knows. Each is a parameter, so the guess is
testable rather than load-bearing — but they are worth settling with him.

1. **Short entry colors.** The chart says "red or black bar" — both are
   down-closing with K ≤ 60, which excludes dark red (K > 60). The mirror for a
   long would then exclude forest green (K < 40), which contradicts "green
   should follow a black bar". The strategy implements the looser reading: any
   bar closing in the trade direction after an opposite-extreme bar.
   `EntryBarColor` now narrows it. `BrightOrSkip` is his written short rule
   ("red or black") and its mirror for longs (lime or white); `Bright` is the
   narrower filter he asked for on 2026-10-09.
2. **Wave three and five.** Still unresolved — he asked whether the proposal
   meant waves or pulses, which it did not answer. Not implemented.
3. **Midline break.** Measured on closes, from the pinch forward.
   `MidlineBreakBypassSlopeTicks` is the flexibility he asked for when slope is
   strong; it defaults to off.
4. **Entry distance from the midband.** Jim wants entries "at or below the
   midband" (2026-10-09). Nothing else bounds it: the pinch only says the EE
   line was near the midband within `PinchLookbackBars`, so a late qualifying
   bar can print well above it. `EntryMaxTicksBeyondMid` caps how far past the
   midband the entry bar may close (0 = at or below for a long); -1 is off.
5. **Lot 2 target.** The trail is implemented. The predrawn-level branch is
   not: a backtest fetching levels per bar over HTTP would be unusable, and the
   level question is better answered by joining the exported trade list to our
   stored history afterwards. Breakeven (Jim's ask, 2026-10-10) is in:
   `BreakevenTriggerTicks` moves both stops to entry + `BreakevenPlusTicks`
   once price has gone that far the trade's way, and holds lot 2's trail
   until then. Off at 0. Stops only ever tighten, and are never placed
   through the market; one the market refuses anyway closes that lot. Lot 2 can
   also be split over three profit targets in ticks (Jim, 2026-10-19: "The
   three targets for Lot 2 will be ticks for now"); its contracts are shared
   out one at a time starting with target 1, and a target of 0 leaves that
   contract on the trail. Lot 1 can be split the same way in FixedTicks
   mode (Jim, 2026-10-21: "20, 30, 40"), over the targets that are set.
   Fib-extension targets are out of scope: he uses them on Wave 3s, not on
   GOATs.
6. **The pumpkin.** Three readings of his words, each a setting. "Flat" is a
   pumpkin that moved less than `PumpkinFlatTicks` over
   `PumpkinSlopeLookback` bars (default 2 over 5; he gave no number).
   "Room" is measured from the entry bar's close to the pumpkin in the
   trade's direction, as he measured it on his YM charts, so a pumpkin
   behind price counts as no room and the trade is skipped. He said 18 to 20
   ticks in one email and "15 minimum" on a chart; the default is 15.
   The slope is a net change over the lookback, so it lags a turn: a
   pumpkin that topped three bars ago still reads as rising over 10. Jim's
   "loser against the pumpkin" (2026-10-25) is that case: +5.6 ticks over 10
   bars against his minimum of 5, but down 0.7 over the last 3. Measured
   from his screenshots, a second check, "still moving the trade's way over
   the last 3 bars", would have skipped 3 to 5 of 11 losers and 1 or 2 of 7
   winners, with most readings within a fraction of a tick of the cutoff.
   Not built: the settings already do it. Jim, 2026-10-26: "What I look for
   is the pumpkin and midband slopes to be in sync (heading in the same
   direction)". His copy then ran a 10-bar lookback, a 2-tick minimum and
   room 18 on a 3-range NQ chart, and both longs he sent were taken as the
   pumpkin rolled over, still up 2 ticks over 10 bars. Room 0 with a 3-bar
   lookback and a 0 or 1 minimum is his rule; the guide's section 7 says
   so. The other losers had a pumpkin still heading the trade's way, which
   no slope rule can tell from his winners.
