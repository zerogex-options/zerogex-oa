# Do the Trade Bias panel's states say how far price will move?

Research-only replay. **Changes no production behavior**: it imports from `src`,
`src` imports nothing from here, and every database statement is a `SELECT`.

## The question

`research/short_gamma_trend` and `research/trade_bias_inputs` found that neither the
panel's states nor any of its nine inputs predicts *which way* price goes. The same
studies saw hints that the inputs say something about *how far* it goes: short-gamma
minutes with one-sided flow saw 30-minute ranges 1.29x the average, and the MSI's gamma
and volatility components track forward excursion. If the panel is to be presented as
a read on movement rather than direction, its states have to earn that.

Two of them already make movement claims to customers, in the copy the dashboard shows
(`frontend/core/tradeBias.ts`):

| State | What the panel tells the customer | Claim about movement |
|---|---|---|
| Chop | "Range-Bound", "Two-way chop", "Avoid premium breakout trades", "Favor theta / defined-risk structures" | **less** |
| Trap Squeeze / Trap Reversal | "Short-gamma expansion higher" / "lower" | **more** |
| Awaiting confluence (`UNKNOWN`) | "Range-bound / choppy" | **less** |
| Trend Up / Trend Down | "Steady grind", "Shallow pullbacks", "Call-wall pin effect" | none on size |

**Stated so it can fail:** after each state, does price travel further or less far
over the next 30 minutes than after the panel's other states, at the same time of day
and after the same recent movement?

## Why "at the same time of day, after the same recent movement"

Two things a trader can already see on a chart predict how far price moves. The session
has a strong rhythm: the first minutes move far more than lunchtime. And movement
clusters: a market that has just been busy tends to stay busy. A state that mostly
appears at the open, or right after a burst, would look like it predicts movement while
only restating the clock or the tape. The simpler construction every signal is asked to
beat (the dashboard's `frontend/content/methodology.md` §5) is those two things, so the
verdict is on what a state adds beyond them.

## What is measured

* **Population.** Every persisted Trade Bias reading (`tenor = 'swing'`), 09:30-15:59
  ET, one per minute, SPY and SPX, from the start of the archive, with the state
  recomputed from the stored inputs by production's current rule. The plumbing check
  from `research/short_gamma_trend` must show the stored inputs reproducing the stored
  state; if it is not close to 100%, nothing below means anything.
* **Outcome: range.** The forward high minus the forward low, in bps of the entry
  price: how far price travels. Same windows as the earlier studies: cash-session bars
  only, entry at the close of the reading's own minute, forward windows that never
  include the entry bar, 15, 30 and 60 minutes when the whole window fits before the
  close, and rest of session.
* **What a trader can already see**, every gauge ending at the entry bar and never
  reaching back past the session's open:
  * *the time of day*: 5-minute slots through the first half hour, where movement fades
    fastest, then half hours to the close;
  * *how busy the bars have been*: the average one-minute bar (high minus low) over the
    last 5, 15 and 60 minutes;
  * *how far price has swung*: high minus low over the last 30 minutes, and over the
    session so far.
* **The ratio.** A least-squares fit of the log of the forward range on the symbol and
  time of day, the logs of the five gauges, and one indicator for "the panel shows
  this state". The indicator's coefficient, exponentiated, is the ratio: how far price
  typically travelled after the state as a multiple of what other minutes with the same
  clock and the same tape travelled. 1.00 means the state adds nothing to them; 1.30
  means 30% further. Each state is set against the panel's other states, not against
  all minutes, because Chop is most of the day: against all minutes it would largely be
  compared with itself. A state whose minutes cannot be told apart from the time of day
  (all of some time slots and nothing else) has no ratio.
* **Statistics.** Every interval is a date-level block bootstrap (2,000 resamples,
  fixed seed): whole ET dates are resampled with both symbols inside them, and the whole
  fit is redone inside every resample. The four states are judged together with Holm's
  correction at 5%, so the chance that even one of the four verdicts is a fluke is at
  most 5%. A state too rare to judge still counts as one of the four (as p = 1).

## The verdict for each state, fixed before any data was read

Primary: 30-minute range ratio against comparable minutes, SPY and SPX pooled.

| Verdict | Rule |
|---|---|
| **TOO RARE** | fewer than 15 distinct sessions or 300 minutes in the state scored at 30 minutes, or no ratio (the reason line says which) |
| **MOVES MORE** | significant after Holm's correction, ratio at least 1.10, **and** SPY and SPX each above 1, **and** the pooled 60-minute ratio above 1 |
| **MOVES LESS** | the mirror image: significant, ratio at most 0.90, SPY and SPX each below 1, 60-minute ratio below 1 |
| **SLIGHTLY MORE** / **SLIGHTLY LESS** | significant, SPY, SPX and the 60-minute ratio all on the same side, but within 10% of comparable minutes |
| **NOTHING DETECTABLE** | anything else |

The 10% bar keeps a difference too small to matter from being reported as one that
does, as the MSI study required a material effect and not just a detectable one.
Nothing else changes a verdict. Whether a verdict matches the copy (Chop and Awaiting
claim less, the Trap states more) is read off the table above, and the report prints it.

## How the design was checked, and what that changed

`selftest.py` builds invented markets where SPY and SPX share one price path whose
movement follows a daily level, a time-of-day rhythm and slow clustering, so the clock
and the tape really do predict movement. In every world Trend is switched on and off at
random, so it knows nothing, and Awaiting confluence appears only at the open of 8
sessions, so it is too rare to judge. Each world was run with 40 seeds (1,000
resamples):

| World | How its Trap and Chop states are made | Must return | All four verdicts right |
|---|---|---|---|
| `informative` | a hidden regime multiplies movement, and the panel sees it 10 minutes before price does: Trap ahead of the busy regime, Chop otherwise | Trap MOVES MORE, Chop MOVES LESS | 39 of 40 |
| `confounded` | no hidden regime: Trap whenever the last 20 minutes were busy for the time of day, Chop otherwise. They only echo the tape | no MOVES MORE or LESS | 37 of 40 |
| `null` | drawn at random | NOTHING DETECTABLE | 38 of 40 |

* **Real effects were found every time**: Trap MOVES MORE and Chop MOVES LESS in all 40
  `informative` worlds, at 1.57 and 0.80 on average.
* **The echo was taken out.** The `confounded` Trap state travels 1.44x as far as the
  other minutes before adjustment (1.31 to 1.59 across the 40 worlds) and 1.01x after
  (0.94 to 1.13).
* **The misses.** A random state was called SLIGHTLY more or less 5 times in 200 such
  verdicts, the rate Holm's correction allows. And once in 120 worlds a difference that
  was not there was called material: the echo state in one `confounded` world, at 1.13.
  A state that purely restates recent movement is the hardest case this study faces, so
  a real ratio between 1.10 and about 1.15 should be read with that in mind.

The `confounded` world changed the design before any real data was read:

* The first draft matched minutes on the same half hour and the same fifth of the last
  30 minutes' swing. The echo state came out at 1.17 and was called MOVES MORE: the
  coarse match left most of "it has been busy" unaccounted for. The fit above replaced
  it.
* With swings alone as gauges, the echo state still averaged 1.04 and reached 1.18.
  Adding the average one-minute bar, a steadier gauge of how busy the market is, brought
  that to 1.01 and 1.13. Adding a 10-minute swing and a 30-minute average bar after that
  did not move the worst case, so the design stopped there.
* Separately, half-hour slots were too coarse a clock for the open: a state seen only in
  the opening minutes kept a 1.32 ratio from the open's own movement. Five-minute slots
  through the first half hour brought it to 1.04 on average across the 40 worlds.

## Context, outside the verdicts

These explain a verdict; none of them moves one. None is corrected for multiple
comparisons, so about one interval in twenty excludes 1.00 by chance alone.

* **How much is the clock and the tape.** The 30-minute ratio three ways: against the
  other states' minutes of the same symbol (roughly what a customer sees), at the same
  time of day, and at the same time of day after the same recent movement (the verdict).
* **Every horizon, and each symbol on its own.**
* **Each state on its own**: Trend Up and Trend Down, Squeeze and Reversal, apart.
* **The panel's gamma regime.** Short gamma, long gamma and neither, by production's own
  definition: the textbook claim is that short gamma moves further.
* **Recent movement alone**: the forward range after the first half hour against the
  rest of the day, and after each fifth of the last 30 minutes' swing against the other
  fifths at the same half hour. The size of what the adjustment removes.
* **How often each state is on the panel.**

## What this cannot tell you

* **Movement, not option P&L.** A state that correctly flags a bigger range can still
  lose money if the options already price that range. Implied volatility is not in this
  replay.
* **The adjustment is only as good as its gauges.** It holds fixed what a chart shows,
  not everything a trader knows (the economic calendar, overnight news, implied
  volatility). A state that tracks those would get credit for them here.
* **Signals have changed under the archive.** Each minute carries the signal versions
  of its time.
* **Intraday only**, 15 minutes to the close.

## Result: the 2026-09-25 run

49 sessions (2026-07-17 to 2026-09-25), SPY and SPX, 38,207 cash-session minutes. The
stored inputs reproduced the stored market state on 100.00% of minutes.

**Verdict: NOTHING DETECTABLE for all three states the panel shows. Awaiting
confluence never appeared.** No state is followed by more or less travel than the
panel's other states, and none comes close (the smallest p is 0.40).

| State | Copy says | Minutes (days) | Same symbol | + same time of day | + same recent movement: the verdict |
|---|---|---|---|---|---|
| Trend Up / Down | no claim | 5,123 (35) | 0.92 [0.80, 1.07] | 0.92 [0.81, 1.06] | **0.97 [0.92, 1.04]** NOTHING DETECTABLE |
| Trap Squeeze / Reversal | more | 1,004 (29) | 1.11 [0.91, 1.31] | 1.16 [0.97, 1.36] | **1.04 [0.95, 1.12]** NOTHING DETECTABLE |
| Chop (Range-Bound) | less | 29,140 (49) | 1.05 [0.93, 1.19] | 1.04 [0.92, 1.17] | **1.02 [0.96, 1.07]** NOTHING DETECTABLE |
| Awaiting confluence | less | 0 | | | TOO RARE: never on the panel in the cash session |

* **"Range-Bound" does not mark quieter markets.** Chop is on the panel for 84% (SPY)
  and 81% (SPX) of cash-session minutes, and those minutes travel as far as the rest,
  before any adjustment (1.05) and after (1.02). It is the panel's default state, not a
  read on movement.
* **"Short-gamma expansion" is not detectable either.** Trap minutes travel 1.04x as far
  as comparable minutes. They lean further over longer windows (1.12 at 60 minutes, 1.16
  to the close), but neither is clearly above 1 (the intervals reach down to 0.99 and
  1.00), and the Trap states are on the panel only 1.6% (SPY) and 4.3% (SPX) of the time.
* **Nothing clear at any horizon, for any single state, or for either symbol.**
* **The clock and the tape carry the movement information.** The first half hour travels
  1.76x as far as the rest of the day. After the busiest fifth of recent movement, price
  travels 1.61x as far as after the other fifths at the same half hour; after the
  quietest fifth, 0.75x.
* **One lead, outside the verdicts: the panel's gamma regime.** Short gamma, by
  production's definition, travels 1.26x [1.08, 1.45] as far as the other minutes, and
  1.08x [1.03, 1.13] after the clock and the tape; the zone where net GEX and the gradient
  disagree ("neither", 23% of minutes) 0.92x [0.88, 0.97]. This is uncorrected context
  and under the 10% bar, but it runs the way the textbook predicts and matches
  `research/short_gamma_trend` (1.29x raw in short gamma with one-sided flow). The states
  hide it: Chop, most of the day, mixes short and long gamma. Confirming it needs
  sessions this run has not seen.

## How to run it

From the repository root, with the service's environment (the `.env` the services
use). Read-only; a few seconds of light `SELECT`s.

```bash
python -m research.trade_bias_movement.cli selftest
python -m research.trade_bias_movement.cli run
```

`selftest` runs the whole pipeline on the three invented worlds above. It must print
`PASS` for all three; if it does not, no number from `run` means anything.

`run` prints the report. `--json PATH` also writes every number to a file.
`--symbols` defaults to `SPY,SPX`, which is what the verdicts are defined on.
