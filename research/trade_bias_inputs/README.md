# Which of the Trade Bias panel's nine inputs predicts direction?

Research-only replay. **Changes no production behavior**: it imports from `src`,
`src` imports nothing from here, and every database statement is a `SELECT`.

## The question

The Trade Bias panel (`compute_bias`, `src/signals/trade_bias/bias.py`) turns nine
inputs into a market state. `research/short_gamma_trend` found that neither a new
short-gamma trend state nor production's own directional states (Trend Up/Down,
Trap Squeeze/Reversal) showed a detectable edge on direction over 47 sessions. Before
anyone tunes the rule that combines the inputs, the prior question is whether any
input carries direction on its own.

**Stated so it can fail:** for each input, when its reading leans one way, does price
go that way over the next 30 minutes by more than the unconditional drift?

## The call each input makes

Every input is read as it was persisted in `trade_bias_scores.payload.inputs`, the
exact value the panel saw at that minute. Positive is bullish for all nine, which is
how every production consumer reads them. An input **calls up** when its reading is
above the bar, **calls down** when it is below the negative bar, and makes no call in
between or when it has no reading.

| Input (panel name) | `payload.inputs` key | Calls up / down when the reading is | Where the bar comes from |
|---|---|---|---|
| Tape Flow | `tape_flow` | above +25 / below -25 | production's vote bar for it (`STRONG`) |
| Vanna/Charm | `vanna_charm` | above +12 / below -12 | production's vote bar (`MODERATE`) |
| 0DTE Positioning | `odte_positioning` | above +12 / below -12 | production's vote bar (`MODERATE`) |
| Positioning Trap | `positioning_trap` | above +12 / below -12 | production's vote bar (`MODERATE`) |
| Trap Detection | `trap_detection` | above +25 / below -25 | production's vote bar (`STRONG`) |
| Gamma/VWAP | `gamma_vwap` | above +12 / below -12 | production's vote bar (`MODERATE`) |
| GEX Gradient | `gex_gradient` | above +12 / below -12 | production's veto bar on the gamma regime (`MODERATE`) |
| Net GEX | `net_gex` | long gamma (+50) / short gamma (-50) | stored as its sign only, so it always calls: the common reading "negative gamma is bearish" |
| MSI | `msi` | above 62 / below 38 | 50 is the index's neutral; 12 points either side is roughly the `MODERATE` bar applied to its signed component sum |

Production calls Net GEX and the MSI "regime strength, not direction". Reading them as
direction tests that claim: if either one carries direction, the panel is leaving it
unused (Net GEX) or reading it as something else (MSI, whose design review found 36
of its 100 points are nominally direction reads: `docs/design/msi-regime-excursion.md`
§3).

The bars are written into `calls.py` as numbers, not read from production's
environment-overridable constants, so a setting on the box cannot move them.

## What is measured

* **Population.** Every persisted Trade Bias reading (`tenor = 'swing'`; all nine
  inputs are identical on both tenors), 09:30-15:59 ET, one per minute, SPY and SPX,
  from the start of the archive. The same inputs replayed through the rule must
  reproduce the `market_state` stored at the time (the plumbing check from
  `research/short_gamma_trend`). If agreement is not close to 100%, the stored inputs
  are not what the panel saw, and nothing below means anything.
* **Outcomes.** The same windows as `research/short_gamma_trend`: entry at the close of
  the reading's own minute, forward windows that never include the entry bar, cash
  session only, 15, 30 and 60 minutes when the whole window fits before the close, and
  rest of session. The inputs themselves see no data past their own minute (the cycle
  reads the latest bar and flow stamped at or before it), so no reading is scored on
  price it had already seen.
* **Directional excess over drift** (the primary measure): the forward return in the
  called direction, minus the unconditional drift over every cash-session minute of the
  same symbol and horizon, so a falling market does not flatter every bearish call.
* **Statistics.** Every interval is a date-level block bootstrap (2,000 resamples,
  fixed seed): whole ET dates are resampled with both symbols inside them, so the
  interval reflects the number of independent days, not minutes. Nine inputs are nine
  chances to find something by luck, so significance is judged with Holm's correction
  at 5% across the nine primary tests: the chance that even one of the nine verdicts is
  a fluke is at most 5%, however the inputs are correlated with each other. An input
  too rare to judge still counts as one of the nine (as p = 1), so the bar is the same
  whatever the data turns out to hold.

## The verdict for each input, fixed before any data was read

Primary: 30-minute directional excess, SPY and SPX pooled.

| Verdict | Rule |
|---|---|
| **TOO RARE** | fewer than 15 distinct sessions or 300 minutes with a call scored at 30 minutes |
| **PREDICTS** | significant after Holm's correction, pooled excess > 0, **and** SPY and SPX each > 0, **and** the pooled 60-minute excess > 0 |
| **WRONG WAY** | the mirror image: significant, pooled excess < 0, SPY and SPX each < 0, 60-minute excess < 0. Price reliably went against the reading |
| **NOTHING DETECTABLE** | anything else |

Nothing else changes a verdict. A WRONG WAY input is information too, but it means the
panel, which reads it the right way round, is being misled by it.

**How often the rule is fooled.** The selftest worlds were run with 40 different seeds
each (80 worlds, 1,000 resamples). The two inputs built to lead and to mislead were
found in all 40 worlds that had them. A pure-noise input was flagged in 4 of the 80
worlds (5%): 1 of the 40 with nothing real in them, 3 of the 40 with two real effects.
On noise inputs the raw 95% intervals excluded zero 6% of the time, a shade over the
nominal 5%, as bootstrap intervals over about 40 days tend to. A PREDICTS or WRONG WAY
here is strong evidence, not proof. Benjamini-Hochberg, the correction first written
into this design, flagged noise in 6 of the same 80 worlds, 5 of them among the worlds
with real effects, which is why the rule uses Holm's correction instead. That change was
made on invented data only, before any real data was read.

## Context, outside the verdicts

These explain a verdict; none of them moves one. None is corrected for multiple
comparisons, so about one interval in twenty excludes zero by chance alone.

* **Every horizon, hit rate, and each symbol on its own.**
* **By reading strength.** The 30-minute return minus drift in each band of the
  reading: past 65 either way (production's two-vote `DOMINANT` level), between the bar
  and 65, and inside the bar. Net GEX by its two values; the MSI by its own regime
  bands (below 20, 20-40, 40-70, 70 and above). A real signal should get stronger
  toward the extremes.
* **By gamma regime.** Production's short- and long-gamma definitions, each scored
  against its own regime's drift, since production uses flow for trend states only in
  long gamma and for trap states only in short gamma.
* **With or against the prior 30-minute move.** An input that only "works" when it
  agrees with the move price just made may be echoing price rather than leading it. A
  call that agrees with the move inherits whatever momentum is worth, so the clean reads
  are the calls made against the move and those made when price had not moved.
* **Price momentum alone** (the prior 30-minute move, at least 5 bps), scored the same
  way: the "simpler construction" every signal is asked to beat (the dashboard's
  `frontend/content/methodology.md` §5), and each input minus momentum at 30 minutes.

## What this cannot tell you

* **Each input alone, not the combination.** Inputs that read the same data (Tape
  Flow and 0DTE Positioning both read the option tape) can show the same effect twice.
  Two PREDICTS are not necessarily two independent edges.
* **Intraday only.** The horizons run from 15 minutes to the close. A signal whose
  claim is about the next day is not tested here.
* **Signals have changed under the archive.** Each minute carries the signal versions
  of its time. That is what the panel showed then, which is the point, but a signal
  fixed last month shapes the history differently from how it behaves now.
* **Direction, not option P&L.** A reading that is right on direction can still lose
  money on premium.

## Result: the 2026-09-25 run

49 sessions (2026-07-17 to 2026-09-25), SPY and SPX, 38,207 cash-session minutes. The
stored inputs reproduced the stored market state on 100.00% of minutes, so every call
below is the reading the panel actually saw.

**Verdict: NOTHING DETECTABLE for all nine.** No input predicts 30-minute direction on
its own, and none comes close: the smallest p is 0.059 (Trap Detection, leaning the
wrong way), ten times Holm's bar of 0.0056 for the first test.

| Input | Calls (days) | 30m excess over drift, bps, 95% | Hit rate | Verdict |
|---|---|---|---|---|
| Tape Flow | 19,301 (49) | -0.2 [-0.7, +0.2] | 49.6% | NOTHING DETECTABLE |
| Vanna/Charm | 27,840 (49) | -0.4 [-1.3, +0.5] | 47.8% | NOTHING DETECTABLE |
| 0DTE Positioning | 14,711 (49) | -0.3 [-1.6, +1.0] | 49.3% | NOTHING DETECTABLE |
| Positioning Trap | 27,681 (49) | +0.3 [-0.5, +1.0] | 49.8% | NOTHING DETECTABLE |
| Trap Detection | 3,117 (36) | -1.1 [-2.2, +0.1] | 46.6% | NOTHING DETECTABLE |
| Gamma/VWAP | 17,082 (43) | -0.0 [-0.8, +0.8] | 49.0% | NOTHING DETECTABLE |
| GEX Gradient | 21,529 (49) | +0.6 [-0.3, +1.6] | 51.7% | NOTHING DETECTABLE |
| Net GEX (gamma sign) | 35,267 (49) | +0.1 [-0.9, +1.2] | 50.5% | NOTHING DETECTABLE |
| MSI | 24,056 (49) | -0.7 [-1.8, +0.4] | 47.9% | NOTHING DETECTABLE |
| *price momentum alone (reference)* | 19,977 (49) | +0.1 [-0.9, +1.2] | 49.7% | |

* **A real null, not a blind spot.** The intervals are narrow: every one lies between
  -2.2 and +1.6 bps, so an edge in the direction an input points, if one hides in
  there, is at most 1.6 bps over half an hour. The hit rates run from 46.6% to 51.7%,
  none reliably better than a coin flip. Price momentum alone does no better.
* **It explains the short-gamma result.** The panel's directional states showed no edge
  because none of their ingredients has one. Re-weighting or re-thresholding the vote
  cannot make direction out of inputs that carry none.
* **Nothing at any horizon.** None of the 40 intervals across 15 minutes, 30 minutes,
  60 minutes and the rest of the session excludes zero, for any input or for momentum.
* **No pattern anywhere else either.** No input gets more right toward its extremes;
  the MSI's own regime bands, from high-risk reversal to trend expansion, all sit within
  1 bp of zero. SPY and SPX, one index read through two option chains, disagree in sign
  on five of the nine. Of the 152 uncorrected intervals in the context tables (hit rates
  aside), 7 exclude zero, about what chance alone produces (7 or 8 at 5%), and three of
  those rest on 85 minutes or fewer. The rest (0DTE Positioning -1.3 in short gamma,
  Positioning Trap +0.8 in short gamma, Trap Detection -2.5 when it disagrees with the
  prior move) are what 152 comparisons produce by luck, and none is worth chasing
  without fresh data.

Two wiring facts the tables surfaced. Neither changes a verdict.

* **Trap Detection never fires when net GEX is negative.** That is by design: its score
  is multiplied by a long-gamma factor that is zero there
  (`src/signals/advanced/trap_detection.py`), and the data agrees, with 0 calls in short
  gamma in 49 sessions. But the panel's two Trap states exist only in short gamma, so
  Trap Detection, one of their three structure votes, can never count toward them, and
  the checklist the dashboard shows with the panel (`frontend/core/tradeBias.ts`) can
  never tick both "Short-gamma regime" and "Trap detection triggered".
* **0DTE Positioning never went past +/-65** in 49 sessions, so production's two-vote
  `DOMINANT` boost has never applied to it.

What this does not touch: **movement**. The same inputs do carry information about how
far price travels. Short-gamma minutes with one-sided flow saw 30-minute ranges 1.29x
the average (`research/short_gamma_trend`), and the MSI's gamma and volatility
components track forward excursion (`docs/design/msi-regime-excursion.md`). Direction
and distance are separate questions; this study answers only the first.

## How to run it

From the repository root, with the service's environment (the `.env` the services
use). Read-only; a few seconds of light `SELECT`s.

```bash
python -m research.trade_bias_inputs.cli selftest
python -m research.trade_bias_inputs.cli run
```

`selftest` runs the whole pipeline on two invented worlds whose answer is known by
construction: in `mixed`, Tape Flow leads price, Trap Detection misleads, Gamma/VWAP
reads on too few sessions and the other six are noise; in `null`, all nine are noise.
It must print `PASS` for both; if it does not, no number from `run` means anything.

`run` prints the report. `--json PATH` also writes every number to a file.
`--symbols` defaults to `SPY,SPX`, which is what the verdicts are defined on.
