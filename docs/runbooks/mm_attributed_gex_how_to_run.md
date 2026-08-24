# Market-Maker Attributed GEX — how to actually run it

Operator's guide. For *why* it works the way it does, see
[`docs/design/market-maker-attributed-gex.md`](../design/market-maker-attributed-gex.md).

---

## 1. What this is, in one paragraph

A standalone research script that answers one question: *does gamma positioning
reconstructed from Cboe's Market Maker classification beat ZeroGEX's current
dealer-gamma model?* It reads two things — Cboe Open-Close CSV files you supply, and
ZeroGEX's database (read-only) — and writes three files to a folder. It is not a
service. It runs when you type a command and exits when it's done.

## 2. Does it touch production?

**No.** Concretely:

| Question | Answer | How to verify yourself |
|---|---|---|
| Is it part of the analytics engine? | No. Separate package `research/`, separate process. | `grep -r "research\." src/` → no hits. Nothing in `src/` imports it. |
| Does it run automatically? | No. No systemd unit, no timer, no cron. | `grep -rl "mm_attributed" setup/systemd/ deploy/` → empty. |
| Does it write to the database? | No. | `grep -rioE "\b(insert\|update\|delete\|create\|drop\|alter)\b" research/ --include="*.py"` returns only Python `list.insert` / `dict.update` and prose. The only SQL verb in the package is `SELECT`. |
| Could it write by accident? | The connection sets `SET TRANSACTION READ ONLY` before any query. | `research/mm_attributed_gex/sources.py::research_connection` |
| Did it change any production metric? | No. Dealer Gamma @ Spot, Gamma Flip, Call/Put Wall, Composite Score are all untouched. | `git show --stat` on the commit: the only file outside `research/`, `tests/` and `docs/` is `Makefile`, and that diff is **41 insertions, 0 deletions** — new targets only. |
| Does it affect the dashboard? | No. `zerogex-web` was not modified. | |

The one way it interacts with production at all: it opens a pooled read connection and
runs `SELECT`s against `option_chains`, `gex_summary`, `underlying_quotes`, `vix_bars`
and `signal_scores`. Those are the same reads the API serves all day. Run it off-hours
if you're cautious about query load; it is not otherwise a risk.

**It does import production code** — `AnalyticsEngine._gamma_exposure_profile`,
`_resolve_gamma_flip`, `walls.compute_call_put_walls_with_strength` — but only to *call*
them as a library, in its own process. That's deliberate: it's what makes the comparison
apples-to-apples. Importing `AnalyticsEngine` opens no connection and starts no loop.

## 3. Where to get the data

You need **Cboe Open-Close Volume Summary** for **C1**, from
[Cboe DataShop](https://datashop.cboe.com/). The relevant product pages:

* [Open-Close Cboe C1 exchange](https://datashop.cboe.com/open-close-cboe-c1-exchange)
* [Cboe Open-Close Volume Summary](https://datashop.cboe.com/cboe-options-open-close-volume-summary)

Confirmed properties of that dataset (Cboe's own description):

* Categorizes every trade by **participant type** (customer, professional customer,
  broker-dealer, market maker), **action** (buy/sell) and **position** (open/close) —
  exactly the four dimensions this experiment needs.
* Customer and professional-customer volume is further split by order size
  (<100, 100–199, >199 contracts). The loader keeps that split as provenance and sums
  it into one participant; it does not affect the arithmetic.
* Delivered as **end-of-day summaries** or **intraday snapshots at 1-minute or
  10-minute** granularity.
* Files are delivered **per exchange**. C1 files include both regular trading hours and
  the global trading session.
* There is a file spec PDF on DataShop (`Open_Close_10m_Spec_v1.6.pdf` at time of
  writing). **Get it** — it is what you check the proposed column mapping against.

### What to order

| Field | What to ask for | Why |
|---|---|---|
| Exchange | **C1** | SPX is Cboe-proprietary and predominantly C1 |
| Symbol | SPX / SPXW | the experiment's universe |
| Cadence | **10-minute intraday** (1-minute if affordable) | EOD works but every intraday snapshot then carries yesterday's inventory, which badly weakens the 5–30 minute tests |
| History | see below — **this is the decision that determines whether the study can conclude anything** | |

### History depth — the thing that actually matters

The reconstruction can only build an inventory from a known zero if the data starts at
or before each contract's **listing date**. Buying a window that starts on your study
start date produces a study that reports `INCONCLUSIVE_DATA` and nothing else.

| Universe | Buy history starting *before* your study window by |
|---|---|
| 0DTE (SPXW dailies) | ≥ 3 weeks |
| Weeklies | ≥ 2–3 months |
| Near-term (≤ 45 DTE) | ≥ 4 months |

So for a study window of, say, 2026-05-01 → 2026-06-30 covering weeklies, order files
from roughly **2026-02-01** onward. The extra months are cheap relative to the study
being uninterpretable without them.

> **Scope note:** your original brief said *do not buy or subscribe to any new dataset
> yet*. Nothing here is a purchase recommendation — it's what to ask for when you decide
> to. A single historical pull (not a subscription) is the smallest version of this.
> Cboe also publishes some free daily Open-Close samples; a few sample days are enough to
> confirm the column mapping and run `check-load` and `reconstruct`, though not enough
> to reach a verdict.

Your team's broader vendor analysis is in
[`docs/design/historical-options-data-vendors.md`](../design/historical-options-data-vendors.md).

## 4. Running it

Everything runs from the repository root.

> **Run these ONE AT A TIME, not as a pasted block.** Steps 1–5 need real Cboe files;
> anything with `/path/to/...` in it is a placeholder that will fail until you replace
> it. Step 1 also has a manual edit in the middle of it.

### Step 0 — prove the plumbing works (no data needed, 30 seconds)

```bash
make mmgex-pipeline-check
```

Runs every layer on synthetic inputs. Expect `"ok": true` and a verdict of
`INCONCLUSIVE_SAMPLE` — that's correct, the synthetic run is deliberately too small to
conclude anything. **Its numbers are properties of the generator, not the market. Never
quote them.**

### Step 0b — rehearse the whole workflow without buying anything

If you don't have Cboe files yet, generate a synthetic set shaped like an Open-Close
delivery and walk steps 1–3 against it. This is how to learn what each command prints
and what a healthy reconstruction looks like before spending money.

```bash
make mmgex-sample
make mmgex-inspect MMGEX_FILE=research_output/sample/SYNTHETIC_openclose_c1_20260601.csv
make mmgex-confirm                    # prints the mapping and REFUSES
make mmgex-confirm REVIEWED=yes       # confirms it
make mmgex-check-load  MMGEX_FILES=research_output/sample/
make mmgex-reconstruct MMGEX_FILES=research_output/sample/
```

Expected shape of the result: 42,470 records parsed from 5 files, 36 series tracked,
20 clean / 16 censored (`clean_share` 0.556), `zero_sum_residual_share` 0.0, and no
reconciliation notes. The mix of clean and censored series is deliberate — it is what a
short window looks like, and it is the diagnostic you will be comparing a real delivery
against.

**The sample's column names are invented and its numbers are random.** A real Cboe
header will differ, which is exactly why `inspect-cboe` proposes a mapping rather than
assuming one. Nothing computed from the sample is a finding. Delete
`research_output/sample/` once you have real files.

### Step 0c — rehearse steps 4 and 5 as well

The plain sample uses invented expirations and strikes, so it cannot exercise the
dataset step: nothing in it matches your chain, and you will see
`listing dates resolved for 0 series`. To shake out the database queries, the replay
and the report render *before* buying anything:

```bash
make mmgex-sample-anchored          # synthetic FLOW over REAL contracts from your DB

# The anchored files have NEW dates, so re-propose the mapping from one of them.
# make-sample prints the exact command; the filename is in its output.
make mmgex-inspect MMGEX_FILE=research_output/sample/SYNTHETIC_openclose_c1_<DATE>.csv
make mmgex-confirm REVIEWED=yes

make mmgex-dataset MMGEX_FILES=research_output/sample/ \
    START=<printed anchor start> END=<printed anchor end>
make mmgex-backtest
```

The re-inspect matters even though the columns happen to be identical: a profile
carries the name of the file it was proposed from, and reusing one across deliveries is
how a stale mapping survives a schema change. `mmgex-confirm` prints that provenance.

`mmgex-sample-anchored` reads your database (read-only, bounded: a ±3% strike band, a
few expirations, a few recent sessions) and prints exactly which contracts and sessions
it anchored to. Use the `start`/`end` it prints for the dataset step.

This exercises every remaining code path — the chain snapshot reads, the `gex_summary`
join, bar loading, VIX and composite-score lookups, the two-pass replay, the statistics
and the report render — against your real database and your real market data.

**Read the provenance block before the report.** The two numbers that decide whether a
run is usable are `data_completeness` and `session_gap_count`. A healthy run over a
contiguous delivery shows completeness near 1.0 and no gaps. Anything else means the
inventory was carried across days it never saw traded, and every level built on it is
suspect regardless of what the statistics say. `build-dataset` prints a warning when it
sees this, but the provenance block is the record — keep it with any result you share.

Expect roughly 0.5–1 snapshot/second; the progress line reports a live rate and ETA.
Raise `--step-minutes` (30 or 60) for a faster first pass.

**The Market Maker flow over those contracts is still invented, so every MM-attributed
number it produces is meaningless.** The report will say `INCONCLUSIVE_*`. What you are
checking is that it *runs*, how long it takes, and that your study window actually has
production rows to compare against — not what it says.

### Step 1 — read a real file and propose a column mapping

```bash
make mmgex-inspect MMGEX_FILE=<YOUR-CBOE-FILE>.csv
```

Prints the file's actual header, the mapping it inferred, and — the important part —
every column it could **not** map. Writes a proposed profile to
`research_output/cboe_profile.json`.

### Step 1b — confirm the mapping

```bash
make mmgex-confirm
```

Prints the structural columns, all four Market Maker volume columns, the bucket width
and the strike scale — then **refuses**. Check every line against Cboe's spec PDF. When
it matches:

```bash
make mmgex-confirm REVIEWED=yes
```

Until you do, every other command refuses with `ProfileNotConfirmed`. That is
intentional — a guessed column mapping that silently mis-attributes flow is the worst
possible failure for this experiment, so it cannot happen by accident. `REVIEWED=yes`
is the deliberate act; the two-step shape is what stops it being skipped by reflex.

### Step 2 — check the parse

```bash
make mmgex-check-load MMGEX_FILES=<YOUR-CBOE-DIRECTORY>/
```

Prints row counts, records emitted, contracts parsed, MM contracts parsed, symbols seen,
trading dates covered, and any parse errors. **If `mm_contracts_total` is 0, stop** —
the MM columns aren't mapped and nothing downstream will be meaningful.

### Step 3 — reconstruct inventory and validate it

```bash
make mmgex-reconstruct MMGEX_FILES=<YOUR-CBOE-DIRECTORY>/
```

This is your go/no-go gate. It prints two blocks:

**`reconstruction`** — how many series were tracked, how many are *cleanly*
reconstructed (built from a known zero) vs left-censored, session gaps, participants
seen. Look at `clean_share`. If it's near zero, you bought too short a history window;
go back to §3.

**`reconciliation`** — the independent check. It tests Cboe's numbers against ZeroGEX's
own stored open interest: every contract opened by one participant is opened against a
counterparty, so aggregate opening flow must move open interest by the amount the file
implies. Look at `verdict`:

| Verdict | Meaning | Do what |
|---|---|---|
| `consistent` | ≥80% of series/sessions reconcile | proceed |
| `partially_consistent` | 50–80% | investigate before trusting results |
| `inconsistent` | <50% | **stop.** The column mapping, participant coverage, or open/close semantics is wrong. Re-check step 1 against the spec PDF. |
| `no_overlapping_oi` | no overlap between the Cboe window and stored chain history | widen one of them |

### Step 4 — build the side-by-side dataset

```bash
make mmgex-dataset MMGEX_FILES=<YOUR-CBOE-DIRECTORY>/ \
    START=2026-05-01T13:30:00Z END=2026-06-30T20:00:00Z
```

This is the long one — it re-prices the whole option chain at every snapshot, twice
(once the existing way, once MM-attributed). Expect minutes to tens of minutes depending
on window length and `--step-minutes`. It prints progress every 50 snapshots.

### Step 5 — run the experiment and get the report

```bash
make mmgex-backtest
```

Prints the verdict to your terminal and writes the full report.

## 5. Where the results are

Everything lands in `research_output/` (gitignored — it's reproducible from the inputs):

| File | What's in it |
|---|---|
| `mm_report.md` | **Start here.** The research report: verdict, coverage, all four test families, controls, multiple-testing correction. |
| `mm_report.json` | Same content machine-readable, for plotting or further analysis. |
| `mm_dataset.jsonl` | One row per snapshot — every existing value next to every MM-attributed value, plus per-universe detail. This is the raw research dataset. |
| `mm_dataset.csv` | The flat scalar columns of the same thing, for a spreadsheet. |
| `mm_dataset_provenance.json` | What the run was built from: reconstruction summary, data completeness, spec used. Keep this with any result you share. |
| `mm_inventory.json` | Per-series MM positions and censoring verdicts (from step 3). |

**Nothing is written to the database. Nothing appears on the dashboard. Nothing is
exposed to customers.**

## 6. How to read the report

Read it in this order.

**1. The verdict**, at the top. It is one of seven values, decided mechanically:

| Verdict | Meaning |
|---|---|
| `INCONCLUSIVE_SAMPLE` | too few observations to detect a moderate effect |
| `INCONCLUSIVE_DATA` | reconstruction too incomplete — you measured the data, not the methodology |
| `YES` | MM attribution adds information that survives out-of-sample |
| `USEFUL_FOR_0DTE` | it works, but only in 0DTE-dominant snapshots |
| `USEFUL_IN_REGIMES` | it works, but only inside specific control splits |
| `USEFUL_AS_CONFIRMATION` | it doesn't replace the existing model, but agreement between the two is informative |
| `NO` | it does not beat the existing methodology |

The two `INCONCLUSIVE_*` gates fire **before** any effect is examined. That's the whole
point of the diagnostic layer: an incomplete Open-Close history reports as "inconclusive
— data", never as "no". If you see `INCONCLUSIVE_DATA`, the answer is buy more history,
not abandon the idea.

**2. Data and coverage**, immediately below. Two numbers decide whether the rest is worth
reading:

* `Mean gamma-universe coverage` — what share of the gamma that actually exists in the
  SPX chain was reconstructed. Below ~20% nothing else in the report means much.
* `Mean inventory confidence` — |dollar-gamma|-weighted completeness. Weighted by gamma,
  not contract count, so 2,000 cleanly reconstructed far-OTM series carrying no gamma
  don't flatter the number.

**3. Section 4, Incremental value** — the section that actually answers the question.
Everything else is descriptive; this is the test of whether MM attribution adds anything
*beyond what ZeroGEX already knows*. Look at:

* `Δ adj R²` — how much explanatory power the MM variables add over the baseline
* `Mean Δ OOS R²` and `Folds improved` — **the ones that count.** In-sample gains from
  five extra predictors are expected even when those predictors are noise. Walk-forward
  out-of-sample is where a real effect survives and a fake one doesn't.

**4. Section 7, Multiple testing.** The report runs dozens of tests. At α=0.05, roughly
one in twenty "findings" is noise by construction. Compare `Significant at p<0.05
(uncorrected)` against `Surviving Benjamini-Hochberg`. If the corrected count is 0, there
is no result regardless of how many uncorrected ones there are.

**A note on effect sizes vs p-values.** At these sample sizes a p-value clears any
threshold. Cohen's d, Δ adjusted R² and Δ AUC are what tell you whether an effect is
worth acting on.

## 7. Experiment arms

The default run is: cleanly-reconstructed series only, ZeroGEX's horizon weighting
applied, open/close-aware inventory, near-term universe headline. Alternatives, via the
CLI (`python -m research.mm_attributed_gex.cli build-dataset ...`):

| Flag | What it tests |
|---|---|
| `--include-censored` | the partial-data arm: does including incomplete series help or hurt? |
| `--raw-positioning` | raw MM positioning with no horizon weighting |
| `--net-flow-estimator` | quantity = buys − sells, ignoring open/close entirely (robustness against Cboe's position-effect flag, which is the least reliable field for market makers) |
| `--headline-universe 0dte` | headline columns from 0DTE only |

Every universe (`0dte`, `nearest`, `weekly`, `near_term`, `all`) is computed on every row
regardless, under `universes` in the JSONL — a 0DTE-only conclusion never needs a re-run.

## 8. Data flow

```
  Cboe Open-Close CSVs                 ZeroGEX production DB (READ ONLY)
  (you supply)                         option_chains  → IV, gamma, open interest
        │                              gex_summary    → the existing published values
        │                              underlying_quotes → spot + forward outcomes
        │                              vix_bars, signal_scores → controls
        ▼                                        │
  ┌───────────────┐                              │
  │ cboe/loader   │  profile-driven parse        │
  └───────┬───────┘                              │
          ▼                                      │
  ParticipantActivity  (normalized, venue-agnostic)
          ▼                                      │
  ┌───────────────┐                              │
  │ inventory     │  MM long/short/net per series│
  │ + confidence  │  censoring, expiry, scoring  │
  │ + reconcile   │  ◄───────── open interest ───┤
  └───────┬───────┘                              │
          ▼                                      │
  ┌───────────────┐        ┌──────────────────┐  │
  │ gex + walls   │◄───────┤ AnalyticsEngine  │  │
  │               │        │ (imported, NOT   │  │
  │               │        │  the live engine)│  │
  └───────┬───────┘        └──────────────────┘  │
          ▼                                      │
  ┌───────────────┐                              │
  │ dataset       │◄─────────────────────────────┘
  │ (replay)      │  existing values side-by-side with MM values
  └───────┬───────┘
          ▼
  ┌───────────────┐
  │ backtest      │  regime / flip / walls / incremental / confluence
  │ + report      │
  └───────┬───────┘
          ▼
  research_output/*.md, *.json, *.jsonl, *.csv          ← files only. No DB writes.
```

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ProfileNotConfirmed` | mapping not human-verified | set `"confirmed": true` in the profile JSON after checking it against Cboe's spec |
| `ProfileMismatch: column 'x' not found` | profile doesn't match this file's header | re-run `mmgex-inspect` on *this* file |
| `mm_contracts_total: 0` | MM columns unmapped | check the inspect output's "UNMAPPED columns" list |
| reconciliation `inconsistent` | mapping/coverage/semantics wrong | do not proceed; re-check against the spec PDF |
| `clean_share` near 0 | history starts too late | buy more lead-in (§3) |
| `INCONCLUSIVE_DATA` | coverage < 20% or confidence < 0.35 | same — more history |
| `No gex_summary timestamps for SPX in [...]` | study window predates ZeroGEX's own retention | pick a window ZeroGEX has data for, or use `option_chains_archive` depth |
| `listing dates resolved for 0 series` | the Open-Close expirations/strikes do not exist in your chain | expected for the plain sample; with real Cboe files it means the chain history does not cover those contracts, and censoring falls back to the window heuristic |
| `No SPX analytics rows found to anchor to` | no recent `gex_summary` rows for that symbol | check the analytics service is writing, or pass `SYMBOL=` |
| `DATA COMPLETENESS` warning with many session gaps | the input directory holds files from two deliveries, or the delivery is missing days | list the directory. A reconstruction across a gap carries inventory it never saw traded |
| `[listing dates]` phase is slow | it scans chain history back `--listing-lookback-days` | lower it. Shorter is faster but weaker evidence, and more series fall back to the window heuristic |
| Dataset build is slow | it re-prices the full chain twice per snapshot | raise `--step-minutes` (default 15; try 30-60 on a first pass). The progress line reports a rate and an ETA |
| `canceling statement due to statement timeout` | a setup query outgrew the pool ceiling | the timed `[phase]` lines name which one. Research reads get 5 min by default (`MMGEX_STATEMENT_TIMEOUT_MS`); if it is the listing-date scan, lower `--listing-lookback-days` |
| `No profile file at '...'` | step 1 never produced it (or you skipped step 1) | run `mmgex-inspect` first; the error prints the exact command |
| `[Errno 2] No such file or directory: '/path/to/...'` | a placeholder was run literally | substitute your real path, or use `make mmgex-sample` to rehearse |
| `none of the N file(s) found matched profile` | wrong profile for this directory | re-run `mmgex-inspect` on a file from *this* delivery |
| `files_skipped` > 0 in check-load | some directory members had a different header | check the `errors` list; readmes/manifests are filtered out automatically, so a skip means a real header mismatch |

## 10. Tests

```bash
make mmgex-test
```

161 tests. They use small synthetic examples whose correct answers can be checked by
hand. **Synthetic data is never used as evidence about the methodology** — only to prove
the code does what it says.
