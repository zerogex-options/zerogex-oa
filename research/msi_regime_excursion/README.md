# Does the MSI regime gauge predict realized price excursion?

Research-only experiment. **Changes no production behavior**: it imports from `src`,
`src` imports nothing from here, every database statement is a `SELECT`, and outputs
go to files.

**How to run it and how to read the results:**
[`docs/runbooks/msi_regime_excursion_how_to_run.md`](../../docs/runbooks/msi_regime_excursion_how_to_run.md).

Methodology, the structural finding, and the current state of the answer:
[`docs/design/msi-regime-excursion.md`](../../docs/design/msi-regime-excursion.md).

## The question

`frontend/core/regime.ts` bands the 0–100 Composite Score / MSI into four regimes,
and each band's copy is a claim about how far price travels:

| Band | Score | Copy |
|---|---|---|
| `trend_expansion` | ≥ 70 | "Strong directional regime — favor trades in the prevailing bias." |
| `controlled_trend` | 40–70 | "Moderate directional edge — trade with reduced size." |
| `chop_range` | 20–40 | "Range-bound — fade extremes, avoid trend trades." |
| `high_risk_reversal` | < 20 | "Mean-reversion only — extreme move risk elevated." |

`content/methodology.md` §5 commits the product to testing claims like these against
"the unconditional base rate, a simpler construction, or the existing production
method". That is what this does, for the central signal.

## Status

Two arms, at very different stages.

**The structural arm is complete and has a result.** It asks a question about the
shipped code, so it needs no archive: hold the entire gamma structure fixed and vary
only the *direction* of options flow. The MSI moves up to **60.7 points and across
three regime bands** on direction alone. See `structural.py`, and
`tests/test_msi_regime_excursion.py` for the pinned assertions.

**The empirical arm is complete and unrun.** The pipeline is built, tested end to end
against synthetic worlds whose answers are known by construction, and ready — but no
database was reachable from the environment it was written in, so **no market data has
been through it and no empirical result exists**. Point it at the archive and the
commands below produce the dataset and the report.

## Quick start

```bash
# 0. Machinery only — synthetic worlds, invented numbers, never evidence.
python -m research.msi_regime_excursion.cli selftest

# 0b. A property of the shipped code. Needs no database.
python -m research.msi_regime_excursion.cli structural

# 1. What does the archive actually hold?
python -m research.msi_regime_excursion.cli describe

# 2. Pull the dataset (read-only).
python -m research.msi_regime_excursion.cli extract \
    --start 2026-06-01 --end 2026-09-01 \
    --out research_output/msi_excursion.jsonl

# 3. Report. No database.
python -m research.msi_regime_excursion.cli analyze \
    research_output/msi_excursion.jsonl \
    --out research_output/msi_excursion_report.md \
    --json-out research_output/msi_excursion_findings.json
```

Run from the repository root (`pythonpath = ["."]` in `pyproject.toml` makes
`research.*` importable, same as the test suite). **Stdlib only** — no numpy, no
pandas — so it runs on a production host with nothing installed beyond what the
services already need.

`extract` is the only command that touches the database. The JSONL it writes is the
evidence: archive it, hand it to someone else, or re-analyse it under different
settings without re-querying production or hoping the archive has not rolled over.

## What gets measured

| | |
|---|---|
| **Horizons** | 5 / 15 / 30 / 60 min, plus rest-of-session. Reported separately, because a session-level statistic may work at session scale and fail at scalp scale — and that is a positioning finding, not a validation failure. |
| **Directionless magnitude** | `max_up`, `max_down`, `range`, `abs_ret`. What "trends can run" vs "range-bound" is a claim about. |
| **Bias-conditioned** | `mfe` / `mae` against the prevailing bias, read from the prior 30 minutes exactly as `frontend/core/impliedDirection.ts` defines it. This is what "favor trades in the prevailing bias" is a claim about. |
| **Point targets** | P(a 4 / 8 / 10 point run in ES), by band, against the base rate. The question in the units a scalper actually uses. |
| **Baseline** | The **unconditional base rate** for the same instrument and horizon — the bucket included in its own baseline. Not bucket-vs-complement, which is an easier and different question. |
| **Effect size** | Cliff's delta (rank-based, no distributional assumption) and Hedges' g, alongside means, medians and ratios. |

## Instruments, and the ES/NQ design

| Instrument | Score from | Bars from |
|---|---|---|
| SPY, SPX, QQQ, NDX | own `signal_scores` | `underlying_quotes` |
| **ES** | **SPX** `signal_scores` | `futures_quotes` (index_symbol `SPX`) |
| **NQ** | **NDX** `signal_scores` | `futures_quotes` (index_symbol `NDX`) |

ES and NQ have no MSI of their own and never have: `src/jobs/futures_projection.py`
projects *levels* onto the futures price axis and explicitly not scores, and
`docs/runbooks/es_nq_futures_rollout.md` says the same in table form. An ES trader
reading the regime gauge is reading the SPX score with ES prices on the chart. That is
not a gap in the archive to work around — it is the product's behavior, and testing it
means exactly what the table above does.

## The statistical trap this is built around

Readings land about once a minute and forward windows overlap, so a 30-minute measure
at 10:00 shares 29 of its 30 minutes with the one at 10:01. Under a **true null** with
that correlation structure, a row-level Welch test rejects at **65%** where it should
reject at 5%; the session-level block bootstrap used here rejects at **5.8%**
(measured — see `tests/test_msi_regime_excursion.py`). Both p-values appear in every
table so the gap stays visible.

Any earlier read of this data that used row-level significance was roughly **13×
over-confident**.

## Layers

| Module | Responsibility |
|---|---|
| `bands.py` | the four bands and the falsifiable reading of each one's copy; asserted equal to `ScoringEngine._regime_label` |
| `excursion.py` | minute bars → forward excursion (MFE/MAE/range/return/point targets) |
| `stats.py` | describe, Welch, Mann-Whitney, Cliff's delta, Spearman, session block bootstrap, Wilson, Benjamini-Hochberg |
| `decompose.py` | split the composite into its magnitude and direction halves; rebuild the alternatives |
| `sources.py` | read-only archive access; the instrument table |
| `study.py` | the experiment battery |
| `report.py` | verdict logic + markdown |
| `structural.py` | the data-free flow-direction sweep against the real engine |
| `selftest.py` | synthetic worlds with known answers; validates the machinery |
| `cli.py` | `describe` / `extract` / `analyze` / `selftest` / `structural` |

## The one idea worth knowing

The composite is a **signed** sum: `sum_offset = Σ(points_i × score_i)`, mapped through
`50 + 50·tanh(sum_offset/50)`. Every component contributes on a signed [−1, +1] axis,
and `ComponentBase` documents that axis as "+1.0 = strongly bullish / −1.0 = strongly
bearish". But the components do not all point along the same axis, and each one's own
docstring says which:

- `gamma_anchor` (30 pts) — "+1.0 — price is 'free' … expect movement" → **magnitude**
- `net_gex_sign` (16 pts) — short gamma "amplifies volatility" → **magnitude**
- `volatility_regime` (6 pts) — `(vix − 20)/10` → **magnitude**
- `order_flow_imbalance` (19 pts) — "call premium dominates (**bullish** model output)" → **direction**
- `dealer_delta_pressure` (17 pts) — dealers short delta, "**bullish** for price" → **direction**
- `put_call_ratio` (12 pts) — comment claims "larger potential move"; the formula is the standard bearish-sentiment gauge → **ambiguous**

So at least 36 of 100 points are a direction read, on a scale whose bands are labelled
as a regime — while `frontend/core/impliedDirection.ts` states of the same number: "It
is deliberately directionless."

## Tests

```bash
pytest tests/test_msi_regime_excursion.py -q
```

38 tests: the bands are asserted identical to the production labeller boundary by
boundary; the excursion arithmetic is pinned against hand-built price paths (including
that the entry bar is never part of its own forward window); the statistics are pinned
against closed forms; the block bootstrap is shown not to reject a session-clustered
null while the naive test does; and the structural finding is pinned so it cannot rot.

Synthetic data is used to verify the machinery and **never as evidence about the
market**.
