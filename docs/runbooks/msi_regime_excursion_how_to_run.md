# Running the MSI excursion study

Answers: **does the MSI regime gauge predict realized forward price excursion, and does
it beat the unconditional base rate?**

Design and the current state of the answer:
[`docs/design/msi-regime-excursion.md`](../design/msi-regime-excursion.md).
Code: [`research/msi_regime_excursion/`](../../research/msi_regime_excursion/).

Everything here is **read-only**. Every database statement in the package is a
`SELECT`. Nothing writes to production, and nothing changes production behavior.

Run from the repository root. Paths assume the standard deploy root
`/home/ubuntu/zerogex-oa`. No extra packages are needed — the module is stdlib only.

---

## 0. Prove the machinery before trusting a number

Two checks, both offline. Run them first; they take seconds and they are what makes a
later result worth reading.

```bash
python -m research.msi_regime_excursion.cli selftest
```

Runs the pipeline against three synthetic worlds whose answers are known by
construction. It must print `PASS` on all three:

```
  signal    rho=+0.7077  -> supported                                     [PASS]
  null      rho=+0.0152  -> not supported — no effect beyond the base rate [PASS]
  inverted  rho=-0.7138  -> INVERTED — bands run backwards                [PASS]
```

If any line says FAIL, **stop** — the machinery is reporting something other than what
is in the data, and no output from step 3 means anything.

```bash
python -m research.msi_regime_excursion.cli structural
```

Needs no database. Drives the real `ScoringEngine`, holding the whole gamma structure
fixed and varying only the direction of options flow. This is a property of the shipped
code, so it produces the same answer every time; it is in the runbook because it is the
context for reading everything else.

## 1. Check what the archive actually holds

```bash
python -m research.msi_regime_excursion.cli describe
```

Prints, per instrument, the row count and date span of both the score archive and the
bar archive. **Read this before choosing a window.** Two things routinely bite:

- **Retention.** `DATA_RETENTION_DAYS` defaults to 90 and prunes `signal_scores`,
  `underlying_quotes` and `futures_quotes`. Asking for six months gets you three.
- **ES/NQ score rows do not exist and are not supposed to.** The `score<-` column shows
  SPX for ES and NDX for NQ. `src/jobs/futures_projection.py` projects levels and never
  scores, so an ES customer is shown the SPX score against ES prices — which is exactly
  what the study measures.

### Want more ES/NQ history than retention allows?

Order matters, and getting it wrong silently deletes the work:

```bash
# 1. Raise retention FIRST and restart ingestion, or the next prune eats the backfill.
#    (FUTURES_BARS_RETENTION_DAYS in the ingester's environment.)
# 2. Then backfill.
make futures-backfill SYMBOLS=SPX,NDX START=2026-04-01 END=2026-09-01
```

`src/tools/futures_backfill.py` warns when the requested range exceeds resolved
retention. Believe the warning. Note this extends the **bars** only — `signal_scores`
has no backfill, so the score archive still bounds the study.

## 2. Pull the dataset

```bash
python -m research.msi_regime_excursion.cli extract \
    --start 2026-06-01 --end 2026-09-01 \
    --out research_output/msi_excursion.jsonl
```

Reads `signal_scores` for each instrument's score symbol, reads the matching bars, and
joins each reading to the price action that followed it. Writes JSONL plus a
`.meta.json` beside it.

The bar window is padded past `--end` automatically so the last readings still have a
full forward window; a horizon whose window runs past the end of the archive is dropped
rather than truncated, so the tail does not enter the sample as an artificially small
move.

Useful flags:

| Flag | Default | Why change it |
|---|---|---|
| `--instruments` | `SPY,SPX,QQQ,NDX,ES,NQ` | narrow while iterating |
| `--horizons` | `5,15,30,60` | rest-of-session is always included |
| `--bias-lookback` | `30` | window for the "prevailing bias" MFE/MAE is taken against; 30 matches `frontend/core/impliedDirection.ts` |

**Keep the JSONL.** It is the evidence. It can be archived, handed to someone else, or
re-analysed under different settings without re-querying production — which matters,
because the source tables roll over and the exact window will not be reproducible in
three months.

## 3. Produce the report

```bash
python -m research.msi_regime_excursion.cli analyze \
    research_output/msi_excursion.jsonl \
    --out research_output/msi_excursion_report.md \
    --json-out research_output/msi_excursion_findings.json
```

No database. A six-instrument, five-horizon run takes a few minutes.

| Flag | Default | Why change it |
|---|---|---|
| `--iterations` | `2000` | block-bootstrap resamples; lower while iterating, not for a final run |
| `--min-bucket` | `30` | skip buckets with fewer usable rows |
| `--horizons` | all in the dataset | focus one horizon |

## 4. Reading the output

Read it in this order.

**Sample first.** The `sessions` column matters more than `rows`. Significance here is
driven by the number of independent *days*, not minutes — 40,000 rows over 12 sessions
is 12 observations wearing a disguise. If a band has fewer than roughly 20 sessions,
treat its interval as indicative.

**Then the verdict table.** `supported` requires all three of: the score orders
excursion (|ρ| ≥ 0.05, surviving Benjamini-Hochberg), at least one band beats the base
rate by a non-negligible effect (|Cliff's delta| ≥ 0.147), and the bands run in the
claimed order. Anything else is reported as what it is:

| Verdict | Meaning |
|---|---|
| `supported` | the bands' copy survives contact with the data |
| `weak — detectable but not material` | a real relationship too small to trade or to justify the copy |
| `not supported — no effect beyond the base rate` | the bands do not beat the unconditional rate |
| `INVERTED — bands run backwards` | worse than nothing: the gauge is confidently wrong |

**Then, always, compare `p (block)` against `p (naive)`.** They will differ by orders of
magnitude and that is the point. Readings land once a minute with overlapping forward
windows, so a row-level test treats correlated observations as independent: under a
*true* null with this correlation structure it rejects **65%** of the time, against
**5.8%** for the block bootstrap. Quote the block p-value. Never quote the naive one on
its own.

**Then the horizons, separately.** The MSI is a session-level statistic. If it holds at
rest-of-session and fails at 5/15 minutes, that is a **positioning** finding, not a
validation failure — it means the gauge should not be presented to scalpers as a
scalp-scale read. Say that plainly rather than reporting the horizon that worked.

**Then the alternative constructions.** `msi_direction` is the negative control: it is
built only from the components whose own docstrings call them bullish/bearish. If it
tracks excursion as well as `msi` does, the shipped score's apparent regime content is
direction wearing a regime label. If `msi_folded` or `msi_magnitude` beats `msi`, the
repair is identified, not just the fault.

**Then the point targets**, which are the customer-facing form of the question: does the
regime read change the odds of a 4 / 8 / 10 point run in ES?

### If the result is null

Report it as null. The finding would be that **the copy on the bands is a claim the
number does not support** — which is a different repair from "the gauge is worthless",
and a much smaller one. Do not go looking for a horizon or a subset where it works and
lead with that; the report prints every horizon for exactly that reason.

## 5. Cost and safety

- Read-only: every statement in the package is a `SELECT`, and `archive_span` validates
  its table names against a fixed allowlist rather than interpolating them.
- `extract` is the only step that touches the database. It runs two indexed range scans
  per instrument (`idx_signal_scores_*`, `idx_underlying_quotes_symbol_timestamp` /
  `idx_futures_quotes_index_symbol_timestamp`). A 90-day, six-instrument pull is a few
  hundred thousand rows — run it off-peak, but it is not a heavy query.
- `analyze` needs no connection at all.

## 6. Troubleshooting

| Symptom | Cause |
|---|---|
| `extract` writes 0 rows | window predates retention, or the instruments had no readings. Run `describe`. |
| ES/NQ rows but no SPY rows | `underlying_quotes` gap; `futures_quotes` is fed by a different ingester. |
| Every horizon `insufficient data` | `--min-bucket` above the per-band count; check the readings-per-band table. |
| `cannot import the database layer` | not at the repository root, or the service environment is not loaded. |
| Very wide bootstrap intervals | few sessions. Widen the window; more rows per day will not help. |
| `selftest` fails | do not run the study. Fix the machinery first. |
