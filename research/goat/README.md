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
check cannot verify either; only a live sim session can.

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
   stored history afterwards.
