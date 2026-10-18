# Opening-range extension × gamma confluence — the 60-minute follow-up

**Status (2026-10-18): the harness is audited, fixed and ready to run at 60
minutes. The production run has NOT happened yet.** The session that did this
work had no route to the database, so `cli coverage` and the study itself
could not be run from it. Everything below that comes from production is
marked *pending*, and §7 is where those numbers go. What is here now is real:
the audit of the harness at 60 minutes (§2), two corrections to the September
findings (§3), what the run can and cannot show, worked out before it runs
(§4), and the decision rule, fixed before the data is read (§5).

September's method and results:
[`or-extension-gamma-confluence.md`](./or-extension-gamma-confluence.md),
[`or-extension-gamma-confluence-findings.md`](./or-extension-gamma-confluence-findings.md).
Harness: [`research/or_gamma_confluence/`](../../research/or_gamma_confluence).
One command runs everything in this document: `make orgc-or60` (§6).

---

## 1. Recommendation

**Don't build bots 1, 3 and 4. Keep bot 2 (continuation) deferred, on a much
longer clock than September gave it.** The 60-minute run should still happen;
it is one command and about twenty minutes. But it can only overturn this in
one narrow way, and it cannot touch H2 at all.

| bot | thesis | now | what the 60-minute run can change |
|---|---|---|---|
| 1. OR Gamma Reversion | H1 + H3 | **don't build** | Only an effect of roughly 15 points or more at the customer's rungs, surviving the §5 rule. September's depth bands showed no rise at all, and its confluence effects were 2.4 points at most. |
| 2. OR Gamma Continuation | H2 | **defer** | Nothing. Broken ladders are about ten times rarer at 60 minutes (§4.1): ~30 touches, below the floor for a test. |
| 3. OR Gamma Confluence | H3, stricter | **don't build** | Same bar as bot 1: only a large confluence effect at 60 minutes. |
| 4. Regime Switcher | routes 1 ↔ 2 | **don't build** | Nothing until 1 and 2 have each cleared on their own. |

**What would settle H2**, the one live thread: about **300 sessions at the
5-minute setting where it was seen**, roughly 220 more than the window holds
now, so about **September 2027** at today's accrual, not "re-run in ~6 months".
A clean confirmation, on sessions the hypothesis was not named on, needs about
300 *new* sessions, or roughly 13 more months. §3.2 explains why September's
estimate was too optimistic.

**What would settle the 60-minute question for H1 and H3:** this one run. If
the customer's rungs show less than about 15 points of excess over their null
(the smallest effect the run can resolve, §4.4), the 60-minute range adds
nothing September did not already say. If they show more, the §5 rule decides
whether it is real.

---

## 2. What a 60-minute range changes in the harness

Each item the brief named, plus what else turned up.

| item | what happens at 60 minutes | action |
|---|---|---|
| **`min_or_bars`** | **Broke silently.** The derived default was `min(2, minutes)`: written for 5 minutes, it accepted **2 of 60 bars**. A session whose feed started at 10:20 would build its "opening range" from the last ten minutes of the hour, report a range a fraction of the real one, and compress every rung toward the boundary. | **Fixed.** Now 80% of the window: 4 of 5, 24 of 30, 48 of 60 bars (`MIN_OR_COVERAGE`). This moves every config fingerprint, and the 5-minute column is not bit-identical to September's: a session now drops if two or more of its first five bars are missing. |
| **Session window** | Range 09:30–10:30; touches from the 10:30 bar to the 16:00 bar, 330 minutes against 385 at 5 minutes. No early-close days fall in the window (the first configured one is 2026-11-27). | Unchanged. |
| **`rearm_minutes`** | Off by default (first touch only). If switched on, its distance half (`0.25 R`) scales with the range and its clock half does not. | Unchanged; not used. |
| **`trend_lookback_minutes`** (30) | At 5 minutes the earliest touches had too little history for a trend read. At 60, the first touches' 30-minute lookback lies entirely inside the opening range, so "trend" at 10:30 means the trend within the range. The trend filter is off by default (cohorts 8–9 empty). | Unchanged; noted. |
| **Forward horizons** | MFE/MAE at 1–30 minutes cover a smaller share of a step, because a step is about 2.5× wider in points (§4.1). No hypothesis uses them. The prev-vs-next race runs to the bell, and at 60 minutes far more touches end unresolved: about 17% in simulation, against 1–2% at 5 minutes. | That raised a real question: does the September baseline formula still hold? Checked (§4.2), and measured on real bars by a new null (below). |
| **Gamma lead clock** (120 s, `visible`) | The earliest touch is 10:30, so a qualifying frame always exists. At 5 minutes, touches at 09:35 sometimes predated the first visible frame and joined the no-confluence group, so H3's comparison group differs slightly in make-up between range lengths. | Unchanged; `events_without_gamma` is reported per length. |
| **Phantom 09:30 bar repair** | Bar-local and keyed on the 09:30 timestamp, so it applies identically. It matters less (1 bar of 60), but on a gap day an unrepaired phantom still sets the high or low of the whole hour. | Unchanged; stays on. |
| **Thresholds in units of R** | The "extreme" cohort (≥ 2R) and H2's "2+ broken rungs" (which needs a touch at ≥ 1.5R) keep their meaning, but R is ~2.5× wider in points, so both cohorts get far thinner. | **New guard** (below). |

### Three more things found while doing this

* **Thin cohorts were being tested, and could pass.** On synthetic data a
  cohort of 2 touches on 1 day scored p = 0.001 and survived
  Benjamini-Hochberg: with one session to resample, the bootstrap has nothing
  to vary. At 60 minutes that is the normal case for the deep and broken-ladder
  cohorts. A cohort now needs **30 touches over 10 sessions** before it gets an
  interval or a p-value; below that its point estimate still prints, and it
  stays out of the multiplicity family. September's headline cohorts were all
  well above this floor.
* **`coverage` counted calendar dates, not sessions** (§3.1). Fixed, and it
  now reports the sessions a build can actually use: dates with 60+ gamma
  frames inside the cash session AND a covered opening-range window, as an
  intersection.
* **The mechanical null is now measured as well as derived** (`null.py`). For
  every session, the bars after the opening range are replayed with each bar's
  direction flipped by a fair coin, 20 times, and pushed through the identical
  event rule. Same minute-by-minute size, same bar shapes, same intraday
  volatility, no direction persistence. This checks the formula on the real
  tape at each range length, and gives every price-only cohort (depth bands,
  the customer's rungs, broken ladders) a null of its own, because the touches
  that reach -200% at 14:30 are not a random draw of all touches.

---

## 3. Two corrections to the September findings

### 3.1 SPY and QQQ had about 49 usable sessions, not 59

`coverage` dated rows with `timestamp AT TIME ZONE 'UTC' AT TIME ZONE
'America/New_York'`, the idiom for a column without a time zone. These columns
are `TIMESTAMPTZ`, where it shifts the instant twice, so an ETF's after-hours
frames landed on the next date and every Friday produced a Saturday. The
arithmetic fits exactly: the window held 49 trading days and 10 Fridays, and
SPY and QQQ showed 59, while the SPX and NDX gamma frames, which stop at the
16:00 close, showed exactly their trading days (49 and 31); and the 249
symbol-sessions September used = 49 + 49 + 49 + 49 + 31 + 31, less 9 skipped. The dataset build was never affected (it bounds every read by
exact instants, and the phantom Saturdays were skipped as `no_bars`), so no
September result moves. Only the stated sample ceiling does.

### 3.2 H2's power was overstated about sevenfold

September projected H2 from the pooled sample's power curve ("59 sessions
resolve a 5-point effect"). H2's cohort held about a ninth of the touches, so
that borrowed precision it never had. Read off H2's own interval (−6.0 points
at p = 0.256, so a standard error of about 5.3 points on 49 sessions):

| | September said | H2's own interval says |
|---|---|---|
| power for a true −6.0 | — | **21%** |
| smallest effect resolvable at 80% | ~5 points | **~15 points** |
| sessions to resolve −6.0 at 80% | ~40 | **~300** |
| sessions to resolve −2.4 at 80% | ~256 | **~1,860** |

So "re-run in ~6 months roughly doubles the sample" was right about the sample
and wrong about what doubling buys. The harness now computes power from each
cohort's own interval (`power.py`), never from a pooled curve.

---

## 4. What the run can and cannot show, worked out before it runs

**Everything in this section is a projection from invented paths**, not a
result: a driftless random walk with an opening-hour volatility bump, pushed
through the identical event code, then scaled to September's real 5-minute
touch rate (6.0 per symbol-session against 8.3 in the walk). Real trend days
will move these numbers. The run replaces all of them.

### 4.1 Sample

If no feed gaps opened after September, the window 2026-06-29 .. 2026-10-16
holds **78 sessions** for SPY, QQQ, SPX and ES and **60** for NDX and NQ (29
of them new since September). Expect about 417 usable symbol-sessions.
*Pending: `cli coverage`, §7.1.*

| | 5 min | 30 min | 60 min |
|---|---:|---:|---:|
| touch events (0.5R steps) | ~2,500 | ~750 | **~460** |
| share of the 5-minute count | 100% | 30% | 18% |
| H2 cohort (2+ broken rungs) | ~270 | ~60 | **~30** |
| touches at ±100% / ±150% / ±200% | 385 / 320 / 270 | 170 / 100 / 60 | **100 / 45 / 20** |
| of which below the range (-100 / -150 / -200%) | ~190 / 160 / 135 | ~85 / 50 / 30 | **~50 / 23 / 10** |
| NQ opening range (median) | ~60 pts | ~125 pts | **~155 pts** |
| touches unresolved at the bell | 1–2% | ~10% | **~17%** |

A 60-minute range leaves about a fifth of the 5-minute touches: the range is
wider, so every rung is further away, and there is an hour less to reach it.
**The customer's -150% and -200% below a 60-minute range will see roughly 10
to 25 touches each in the whole window: too few to test on their own.** Only
the pooled rungs are testable.

### 4.2 The mechanical null

September's baseline was the event rule measuring itself:
`P = 0.5 + d / (2 · step)`, with `d` the touch bar's close-offset. Two things
change at 60 minutes.

* **`d` shrinks relative to the range.** The offset is set by bar size, not
  range size, so in units of R it falls by about the range ratio: from
  0.026R to roughly 0.010R. Projected formula null: **~51.0%** at 60 minutes,
  ~51.2% at 30, against 52.6% at 5.
* **The formula assumes every race finishes.** It does not at 60 minutes, and
  conditioning on resolution tilts a driftless walk toward the nearer rung.
  This was checked before trusting it either way: 28,800 simulated sessions
  at 60 minutes, a sixth of touches unresolved, and the resolved reversal rate
  still matched the formula to **+0.01 points** (95% interval −0.39 to +0.46).
  An early, smaller simulation suggested a gap of about a point; the large one
  showed that was noise.

**Pre-registered prediction:** on the real tape the formula and the surrogate
null agree to within about half a point at every range length. If they do not,
the real tape has structure the formula misses, and every cohort has to be
read against the surrogate.

### 4.3 The cost hurdle

A wider range spreads the same $14.50 round turn over a wider step. With NQ's
range at ~155 points, a 0.5R step is ~78 points, and the reversal rate needs
to beat its null by only **~0.5 points** to pay costs, against 1.21 at 5
minutes. The economics get easier at 60 minutes, but only if there is an
effect to pay for.

### 4.4 Power: the smallest effect each test can see

Standard errors scaled from September's measured H2 interval (design effect
1.35 over a binomial), at 80% power, two-sided α = 0.05:

| test | 5 min | 30 min | 60 min |
|---|---:|---:|---:|
| H2 cohort, smallest resolvable effect | ~12 pts | ~25 pts | **~36 pts** |
| H2, power for a true −6.0 | ~30% | ~10% | **~8%** |
| H2, sessions to resolve −6.0 | ~300 | ~1,400 | **~2,800** |
| customer's rungs pooled, smallest resolvable effect | ~6 pts | ~10 pts | **~15 pts** |

H2 at 60 minutes needs about eleven years of sessions to resolve the effect
September saw; it will most likely fall under the 30-touch floor and print as
untestable. At its own 5-minute setting it needs about 10 more months, or
about 13 on fresh data alone. *Pending: measured values, §7.4.*

---

## 5. The decision rule, fixed before the data

Each hypothesis is judged on one cohort, chosen in advance, in one claimed
direction (`report.HYPOTHESES`):

| | cohort | claim |
|---|---|---|
| H1 | 18. any of ±100 / 150 / 200% of R | the customer's marked rungs revert MORE than the average touch |
| H2 | 11. 2+ consecutive rungs broken | price CONTINUES more |
| H3 | 3. a gamma level within 10 pts, available 120 s before the touch | reverts MORE |

Per hypothesis, per range length (`compare.decide`):

* **build** — all of: beats the baseline in the claimed direction and
  survives Benjamini-Hochberg across **every cohort of every range length in
  the run** (5, 30 and 60 are one family, because acting on any of them is
  one decision); its excess over its own mechanical null clears the NQ cost
  hurdle at the near end of its session-clustered 95% interval; keeps the
  claimed sign against its own column's baseline in all three chronological
  splits, each holding 30+ touches; and the two option books do not disagree.
* **don't build** — any of: the effect net of its null points the wrong way;
  even the far end of its interval falls short of the cost hurdle; or a split
  with 30+ touches reverses the sign.
* **defer** — pointing the right way, not contradicted, not resolved.
* **untestable** — fewer than 30 touches or 10 sessions. Nothing can be built
  on it and nothing can be ruled out.

`defer` is not praise. Every verdict prints beside the smallest effect its
sample could have seen; a deferral with a 25-point MDE says the sample is
blind, not that the effect is there.

H2 is also tested on sessions from **2026-09-08** on: the hypothesis was named
after looking at the September window, so a test that re-uses those sessions
partly grades its own homework.

Unchanged from September: the mechanical null leads; every interval and
p-value resamples whole calendar sessions; Benjamini-Hochberg is applied to
the cohort family (and now across range lengths too); the split is
chronological 60 / 20 / 20 on session dates; the books are checked before
pooling; results are surfaces across range lengths, never a ranking.

---

## 6. Running it

On the EC2 box, from a checkout of this branch (read-only against the
database; writes only under `research_output/orgc_or60/`):

```bash
make orgc-or60
# = coverage at OR=60, then OR 5 / 30 / 60 at 0.5R over 2026-06-29 .. 2026-10-16
#   in ONE pass over the sessions, then comparison.md with the §5 verdicts.
# Overrides: START=... END=... FRESH_FROM=2026-09-08 FAST=yes (skips gex_by_strike)
```

About one solo build's time (~15–25 minutes): the three range lengths share
every database read. `research_output/orgc_or60/comparison.md` holds every
table §7 needs; the per-length reports sit beside it. A dataset already built
under the same fingerprint and window is reused on a re-run.

---

## 7. Results — pending the run

Filled from `research_output/orgc_or60/comparison.md`. Section numbers there
match the sub-sections here.

### 7.1 Sessions available per symbol — *pending (`comparison.md` §1)*
### 7.2 Sample by range length — *pending (§2)*
### 7.3 Mechanical null at 5 / 30 / 60, formula against surrogate — *pending (§3)*
### 7.4 H1: depth bands and the customer's rungs — *pending (§4)*
### 7.5 H3: confluence by threshold, and the cohort family — *pending (§5)*
### 7.6 H2: effect, power, fresh-data slice — *pending (§6)*
### 7.7 Out of sample — *pending (§7)*
### 7.8 Cost hurdle — *pending (§8)*
### 7.9 Verdicts — *pending (§9)*

---

## 8. Caveats

* **§4 is projection, not measurement.** It decides what the run can show; it
  says nothing about what it will show.
* **The ranked-GEX levels exist only for the later part of the window.**
  `gex_by_strike` is pruned at the retention horizon, so early sessions have
  walls, flip, max pain and pin but no GEX ranks. The confluence cohort is
  therefore denser in the later chronological splits than in discovery, which
  muddies H3's out-of-sample check. `FAST=yes` drops ranked levels everywhere
  for a uniform definition, at the cost of half of what the customer's chart
  shows.
* **Rebuilds are not bit-identical** (the `min_or_bars` change, a live
  `futures_quotes`, and new fingerprints); `.meta.json` records what each one
  used.
* Everything September listed still holds: dealer sign is modeled, not
  observed; the ±3% strike band censors the deep rungs, more so at 60 minutes
  where the rungs are further out; the customer's own indicator is not
  replicated; nothing here has been validated live.
