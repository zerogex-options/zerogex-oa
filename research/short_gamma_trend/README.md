# Should the Trade Bias panel have a trend state in short gamma?

Research-only replay. **Changes no production behavior**: it imports from `src`,
`src` imports nothing from here, and every database statement is a `SELECT`.

## The question

`compute_bias` (`src/signals/trade_bias/bias.py`, mirrored in the dashboard's
`frontend/core/tradeBias.ts`) has trend states only in long gamma. In short gamma,
flow that leans one way with no opposing structure falls through to **CHOP** --
"Range-Bound: fade extremes of the session range". That is what the panel showed
through the 2026-09-23 SPY sell-off from about 10:10 ET, once total net GEX turned
negative.

Dealer-gamma theory says the opposite should happen there: short-gamma dealers hedge
*with* the move, so directional flow in short gamma is where trends run. If that holds
in our own history, "Range-Bound" is the wrong label for those minutes.

**Stated so it can fail:** among short-gamma CHOP minutes whose flow votes lean one
way, does price keep going that way over the next 30 minutes by more than the
unconditional drift, or does it snap back?

## The candidate

Only rows that production labels CHOP are ever relabeled. Every other state is
production's own, computed by calling production's `compute_bias`, so the replay can
differ from the live panel only in the rows under test.

| Variant | Relabels a short-gamma CHOP minute as a trend when… |
|---|---|
| `flow` (**primary**) | the flow votes lean one way (the same tape / vanna-charm / 0DTE vote production uses for its long-gamma trend states) |
| `aligned` | the `flow` minutes whose structure votes lean the same way too |
| `momentum` (null) | price itself moved ≥ 5 bps over the prior 30 minutes of the same session. No flow votes at all: the "simpler construction" `content/methodology.md` §5 asks every signal to beat |

"Short gamma" is production's definition: total net GEX negative and the GEX gradient
not strongly contradicting it (`inputs.net_gex` / `inputs.gex_gradient` as persisted).

## What is measured

* **Population.** Every persisted Trade Bias reading in `trade_bias_scores`
  (`tenor = 'swing'`; the structural state is identical on both tenors), 09:30-15:59 ET,
  one per minute, for SPY and SPX by default. States are recomputed from the stored
  `payload.inputs` with the current rule (after the 2026-09-23 MSI fix), so history is
  judged by the rule customers see now.
* **Plumbing check.** The same inputs replayed through the *pre-fix* rule must
  reproduce the `market_state` that was stored at the time. Agreement is printed; if it
  is not close to 100%, the inputs are not what the rule saw, and nothing below means
  anything.
* **Outcomes**, from `underlying_quotes` minute bars, cash session only. Entry is the
  close of the reading's own minute; the outcome window is the bars strictly after it,
  so no reading is scored on price it had already seen. Horizons 15, 30 and 60 minutes
  (only when the whole window fits before the close) and rest-of-session.
  * *Directional excess*: forward return in the call's direction, minus the
    unconditional drift over all cash-session minutes of the same symbol and horizon
    (so a falling market does not flatter every bearish call). **The primary measure.**
  * *Trend quality*: favorable minus adverse excursion in the call's direction, on the
    same drift-adjusted basis.
  * *Hit rate*: share of calls whose forward return went the called way.
  * *Range ratio*: forward high-low range after the call versus all minutes, since
    "trend" copy is also a claim that price travels.
* **Statistics.** Minutes a minute apart share almost their whole forward window, and
  SPY and SPX on the same day are one market. Every interval is a **date-level block
  bootstrap** (2,000 resamples, fixed seed): whole ET dates are resampled with both
  symbols inside them, so the interval reflects the number of independent days, not
  minutes. Row-level tests would be roughly 13x over-confident on this data (measured in
  `docs/design/msi-regime-excursion.md` §4).
* **Context, not verdict.** The same measures for production's existing directional
  states (long-gamma TREND_UP / TREND_DOWN, TRAP_SQUEEZE / TRAP_REVERSAL), how often the
  candidate would fire, how long its episodes last (flicker is a product cost), the
  first minute of each episode on its own, and `flow` minus `momentum`.

## The verdict, fixed before any data was read

Primary: variant `flow`, 30-minute directional excess, SPY and SPX pooled.

| Verdict | Rule |
|---|---|
| **INSUFFICIENT** | fewer than 15 distinct sessions or 300 minutes in the candidate state |
| **SHIP** | pooled excess > 0 with the 95% interval above 0, **and** SPY and SPX each > 0, **and** the pooled 60-minute excess > 0 |
| **DON'T SHIP** | pooled excess < 0 with the 95% interval below 0: price reverted, so "Range-Bound" was the right label |
| **NO EVIDENCE** | anything else |

Nothing else changes the verdict. Everything else in the report is there to explain it.

A 95% interval is wrong one time in twenty by construction: on the `null` world run with
20 different seeds, the interval excluded zero once (and that one read SHIP). A SHIP here
is strong evidence, not proof.

## What this cannot tell you

* **Signals have changed under the archive.** Each minute is judged on the inputs the
  engine computed at the time, so older minutes carry older signal versions. That is
  what the panel showed then, which is the point, but a signal fixed last month shapes
  the history differently from how it behaves now.
* **Gamma sign is production's.** "Short gamma" is total net GEX, which can disagree
  with net GEX at spot. The replay tests the rule as it would ship, not a better sign.
* **Direction and travel, not option P&L.** A state that is right on direction can
  still lose money on premium.

## How to run it

From the repository root, with the service's environment (the `.env` the services
use). Read-only; a few seconds of light `SELECT`s.

```bash
python -m research.short_gamma_trend.cli selftest
python -m research.short_gamma_trend.cli run --day 2026-09-23
```

`selftest` runs the whole pipeline on three invented worlds whose answer is known by
construction (continuation, none, reversion). It must print `PASS` three times; if it
does not, no number from `run` means anything.

`run` prints the report. `--day` adds that session's states under the current and
candidate rules, run by run, as a check against what the panel actually did. `--json
PATH` also writes every number to a file. `--symbols` defaults to `SPY,SPX`, which is
what the verdict is defined on; other symbols can be run on their own as context.
