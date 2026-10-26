# GA Settings Guide

*For the ZeroGexGoat version of October 26, 2026. Confidential: this
describes Jim Edwards' GOAT method.*

## Before you change anything

- **Every distance is in ticks, and a tick is a different size on each
  instrument.** On NQ a tick is a quarter point; on YM it is a full point. The
  same number asks for four times the distance on YM. Keep one saved set of
  settings for each instrument: the **template** link at the bottom of the
  settings window saves and loads them.
- **GA decides when a bar closes and enters at the open of the next bar.**
- **GA trades one setup at a time.** While it is in a trade, it does not look
  for another.
- **When you re-enable GA, it replays the bars already on the chart** and
  draws the trades it would have taken. That is the quickest way to see what a
  setting change does.
- **If the strategy label at the top of the chart starts with (D), GA is off**
  on that chart and shows no trades at all.
- **Switch GA on only when the account is flat on that instrument.** GA
  trades as if the account holds nothing but its own trades. It also waits
  for the trades it replays from the chart to finish before it trades live,
  even if Start behavior says "Immediately submit": on October 23 that
  setting sent a replayed trade's stops to your account and left it 6
  contracts short.
- **The stochastic settings must match the jeStochastics on the same chart.**
  GA works out bar colors with its own copy of that math. If the numbers
  differ, GA sees different colors than you do.

## 1. Midband

| Setting | What it does |
|---|---|
| Midband MA type | The kind of average used for the midband. Your charts use SMA. |
| Midband period | How many bars the midband averages. Your charts use 55. |
| Band std dev | How far out the upper and lower bands sit. Your charts use 2. |

## 2. EE line (the yellow line)

| Setting | What it does |
|---|---|
| EE line MA type | The kind of average used for the yellow line. Your charts use HMA. |
| EE line period | How many bars it averages. Your charts use 22. |

## 3. Stochastic (bar colors)

| Setting | What it does |
|---|---|
| Stochastic %K period | Must match your jeStochastics. 3 on your YM chart. |
| Stochastic %K smoothing | Must match your jeStochastics. 3 on your YM chart. |
| Overbought (white/dark red above) | Above this, up bars are white and down bars dark red. 60. |
| Oversold (dark green/black below) | Below this, up bars are dark green and down bars black. 40. |

Between the two, up bars are lime (bright green) and down bars red.

## 4. Setup filters

| Setting | What it does |
|---|---|
| Slope lookback (bars) | How many bars back GA measures the midband's slope. |
| Min slope over lookback (ticks) | The midband must have risen (for a long) or fallen (for a short) at least this much over the lookback. Raise it to trade only steeper trends. |
| Pinch: max EE-to-mid (ticks) | How close the yellow line must come to the midband for a setup to count. |
| Pinch: valid for (bars) | How many bars after that touch the entry may still come. Small numbers keep entries close to the touch. |
| Require no midline break | When ticked, no bar may have closed through the midband against the trade since the pinch. |
| Midline break tolerance (ticks) | How far past the midband a close may go before it counts as a break. |
| Bypass break rule above slope (ticks, 0=off) | When the midband is sloping at least this steeply, the break rule is skipped. |
| Require color flip (black->up / white->down) | When ticked, the bar before the entry bar must be black for a long or white for a short. |
| Entry bar color | Any: any bar closing the right way. Bright: lime for a long, red for a short. Skip: white for a long, black for a short. BrightOrSkip: either. |
| Entry close vs EE tolerance (ticks) | How far on the wrong side of the yellow line the entry bar may close: below it for a long, above it for a short. This is the snuggler-versus-reacher rule. |
| Min band width (ticks, 0=off) | The distance between the upper and lower bands must be at least this much. It keeps GA out of tight, choppy markets. |
| Entry: max ticks above midband (below for shorts, -1=off) | How far past the midband the entry bar may close. 0 means at or below the midband for a long. |

## 5. Sizing and exits

GA trades two lots: lot 1 (GOAT1) and lot 2 (GOAT2).

| Setting | What it does |
|---|---|
| Contracts per lot | Contracts in each lot. 3 means 6 contracts in all. |
| Stop mode | EntryBarRangePlusTicks: the stop is the entry bar's height plus the extra ticks in the next setting. FixedTicks: a set number of ticks. |
| Stop: extra ticks beyond entry bar (range mode only) | Added to the entry bar's height. On a 5-range NQ chart the bar is 5 ticks, so 2 extra makes a 7-tick stop. |
| Stop: fixed ticks (FixedTicks mode only) | The stop distance in FixedTicks mode. |
| Lot 1 exit mode | FirstOppositeCloseBar: lot 1 exits on the first bar that closes against the trade. FixedTicks: lot 1 exits at set profit targets. |
| Lot 1 target 1 (ticks; FixedTicks mode only) | Lot 1's profit target. With targets 2 and 3 at 0, all of lot 1 exits here. |
| Lot 1 target 2 (ticks, 0=off) | Splits lot 1 over more targets (FixedTicks mode only). Its contracts are shared over the targets that are set, so 20, 30 and 40 is one contract each with 3 per lot. |
| Lot 1 target 3 (ticks, 0=off) | The third target for lot 1. |
| Lot 2 target 1 (ticks, 0=trail) | Splits lot 2 over up to three profit targets. Each target takes one contract (with 3 per lot), and a 0 leaves that contract on the trail. All three at 0 keeps lot 2 as one trailing lot. |
| Lot 2 target 2 (ticks, 0=trail) | The second contract's target. |
| Lot 2 target 3 (ticks, 0=trail) | The third contract's target. |
| Lot 2 trail (ticks; waits for breakeven when it's on) | Lot 2's stop follows the best price since entry, this far behind, and never moves back. With breakeven on, the trail starts only once breakeven is reached. |
| Breakeven: after ticks in profit (0=off) | **When** the stops move: once price has gone this far your way. 0 turns breakeven off. |
| Breakeven: stop at entry plus (ticks) | **Where** they move to: your entry plus this many ticks. 1 or 2 is typical. |

**Three targets per lot, with breakeven.** Each lot can take profit at up to
three levels. For lot 1, set "Lot 1 exit mode" to FixedTicks and fill in its
three targets. For lot 2, fill in its three targets; a 0 there leaves that
contract on the trail. Then set breakeven with its two boxes: "after ticks in
profit" is when, "stop at entry plus" is where.

For example, with 3 contracts per lot: lot 1 at 20, 30 and 40 takes one
contract off at each. Lot 2 at 60, 80 and 0 takes one off at 60 and one at 80,
and lets the last ride the trail. With breakeven at 15 and plus 1, every
contract still open moves to entry plus 1 tick once price has gone 15 ticks
your way.

Contracts are handed out one at a time, starting with target 1: 4 per lot over
three targets gives 2, 1 and 1, and 2 per lot leaves the third target without
one.

## 6. Dead zones (no new trades)

Up to three windows where GA opens no new trades. Trades already open are
still managed. Times are HHMM on the 24-hour clock in your chart's time zone,
so 2:40 PM is 1440. A zone is off when its "from" and "to" are the same. A
zone can run past midnight: 1550 to 200 covers 3:50 PM to 2:00 AM.

A zone ends at its "to" time: it covers the "from" minute up to, but not
including, the "to" minute. So a zone ending at 829 leaves 8:29 to 8:30 open
for new trades; end it at 830 to close that minute.

## 7. Pumpkin line

Off until you tick the first setting.

| Setting | What it does |
|---|---|
| Require pumpkin in sync (off = ignore it) | When ticked, GA skips a trade when the pumpkin is flat or curling against it, unless there is room (last setting). |
| Pumpkin: HMA period | Match it to the pumpkin on your chart: 80 on your latest charts. |
| Pumpkin: slope lookback (bars) | How many bars back GA measures the pumpkin's direction: it compares the pumpkin now with this many bars ago. Short reacts quickly to a curl; long is steadier but slower to notice one. At 10, a pumpkin that topped out three bars ago can still count as rising. Default 5. |
| Pumpkin: min slope over lookback (ticks; less is flat) | How much the pumpkin must move over the lookback to count as heading somewhere. Less than this is flat. Default 2. |
| Pumpkin: room to trade against it (ticks, 0=never) | A trade against a flat or curling pumpkin is still taken when the pumpkin sits at least this far beyond the entry, above a long or below a short. Default 15. 0 means never trade against it. |

**Example on YM**, where a tick is a point. GA sees a long, the pumpkin is
flat, and it sits 19 points above the entry bar's close. With room at 15,
GA takes it, because price can reach breakeven before it meets the pumpkin.
At 10 points above, GA passes and labels the bar "pumpkin 10 < 15". If the
pumpkin is flat or falling and sits below price on a long, there is no room by
this measure, so GA skips that one too. Setting the room at or a little above your breakeven trigger
matches the idea of reaching breakeven first.

**To trade only when the pumpkin and the midband head the same way.** GA
already requires the midband to head the trade's way (group 4), so the
pumpkin settings decide whether the two are in step:

1. Set the room to 0, so GA never trades against a flat or turning pumpkin.
2. Set the slope lookback to 3, so GA reads the pumpkin's last few bars, the
   way your eye does. At 10 on a fast chart, a pumpkin that has just rolled
   over is still higher than it was 10 bars ago, so it counts as rising.
3. Set the min slope to 0 to start. Over 3 bars that asks only that the
   pumpkin isn't moving against the trade. If losers still slip through as
   it tops out, try 1, which asks that it is clearly moving with the trade.

Expect fewer trades, including some winners where the pumpkin pauses. With
"Label skipped setups" on, every trade this blocks is labeled "pumpkin," so
you can see what it gives up.

## 8. Chart labels

| Setting | What it does |
|---|---|
| Label skipped setups (why GA passed) | When GA passes on a bar it could have used as an entry bar, it writes the reason on the chart in light blue: under the bar for a long, over it for a short. |
| Labels to keep (newest; raise to go back further) | GA keeps only this many labels, the most recent ones; each new label past that replaces the oldest. 300 covers a few days on a 5-range NQ chart. To study a longer chart, raise it: try 5000 for 30 days. Each label is a drawing on the chart, so if the chart gets slow, lower it. |

A label lists each rule the bar failed, one per line. The first number is what
GA measured and the second is your setting:

| Label | Meaning | Setting to look at |
|---|---|---|
| zone | The bar was in a dead zone. | Dead zones |
| pinch 9 > 6 | The yellow line came no closer than 9 ticks to the midband; you allow 6. | Pinch: max EE-to-mid, Pinch: valid for |
| width 36 < 40 | The bands were 36 ticks apart; you require 40. | Min band width |
| slope 8 < 10 | The midband moved 8 ticks over the lookback; you require 10. | Min slope over lookback, Slope lookback |
| flip | The bar before wasn't black (long) or white (short). | Require color flip |
| EE 5 > 4 | The bar closed 5 ticks on the wrong side of the yellow line; you allow 4. | Entry close vs EE tolerance |
| mid 12 > 10 | The bar closed 12 ticks past the midband; you allow 10. | Entry: max ticks above midband |
| break | A bar closed through the midband since the pinch. | Require no midline break, tolerance, bypass |
| pumpkin 8 < 15 | The pumpkin was flat or against the trade, with only 8 ticks of room; you require 15. | Pumpkin settings |
| pumpkin | The pumpkin was flat or against the trade and behind price, or room is set to 0. | Pumpkin settings |

Labels go only on near misses: bars closing the way the midband points, in
your entry bar color, that failed one to three rules. Bars failing four or
more rules aren't really setups, so they get no label. To keep a chart
readable, the two bars after a label get none either. If two labels still
overlap, zoom in on the chart to spread the bars apart. Labels only explain;
turning them on never changes a trade.

## When GA passes on a trade you expected

1. **Is GA on?** If the strategy label starts with (D), it's off.
2. **Turn on "Label skipped setups"** and re-enable. If the bar has a label,
   the table above says which setting stopped it.
3. **No label on that bar?** Then one of these is true:
   - GA doesn't see the bar in your entry bar color. Check that the
     stochastic settings match your chart.
   - The bar closed against the way the midband is pointing.
   - It failed four or more rules.
   - A label was drawn on one of the two bars before it. Look there.
   - The bar is older than the oldest label kept. Raise "Labels to keep."
   - GA was already in a trade.
   - The bar is among the first 100 or so on the chart, before GA has enough
     history to start trading.
4. **Moving settings between NQ and YM?** Remember a YM tick is four times
   an NQ tick.

## If the market rejects a stop

GA places a stop as soon as an entry fills, and moves it when a bar closes.
In a fast market, price can get past the stop level before the order
arrives, and the market rejects it: a new stop outright, a moved one as
"Unable to change order." Either way GA closes that lot at market, which is
what the stop was there to do, and keeps trading. The Log tab (Control
Center) shows a line starting "ZeroGexGoat: the market rejected", and the
broker's own rejection message may still pop up.

## If GA switches itself off

Two things still switch GA off. Each first cancels GA's orders and closes
its position, and the Log tab says which it was:

- **"ZeroGexGoat: an exit overfilled."** A stop and a profit target both
  filled, leaving an extra contract on the wrong side of the trade. GA
  closes it at once.
- **"ZeroGexGoat: ... failed."** Any other order problem, such as an entry
  while the market is closed or on an expired contract.

Check that the account is flat on that instrument, then re-enable GA.

In the Log, GA's orders carry its names: GOAT1A, GOAT2 and so on, with exits
named "Stop loss" and "Profit target." Orders named Entry, Stop1 and Target1
come from an ATM strategy, such as one placed from the SuperDOM, not from GA.
NinjaTrader also saves each day's log as a text file in
`Documents\NinjaTrader 8\log`, with the date in its name, ready to attach to
an email.

## Installing a new version

1. Untick Enabled on every copy of GA in the Control Center's Strategies tab.
2. Save the new file over the old one in
   `Documents\NinjaTrader 8\bin\Custom\Strategies`.
3. Open the NinjaScript Editor and press F5. The error list should be empty.
4. Check your settings (each new one starts out leaving GA as it was), then
   re-enable one copy.
