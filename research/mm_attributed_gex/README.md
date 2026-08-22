# Market-Maker Attributed GEX

Research-only experiment. **Changes no production behavior**: it imports from `src`,
`src` imports nothing from here, every database statement is a `SELECT`, and outputs
go to files.

**How to run it, where to get the data, and how to read the results:**
[`docs/runbooks/mm_attributed_gex_how_to_run.md`](../../docs/runbooks/mm_attributed_gex_how_to_run.md).

Design, methodology and full data requirements:
[`docs/design/market-maker-attributed-gex.md`](../../docs/design/market-maker-attributed-gex.md).

> The metric is **Market-Maker Attributed GEX**. Not "true dealer GEX" — exchange tags
> improve attribution, but the position is still a reconstruction.

## Status

The pipeline is complete and tested. **No Cboe Open-Close file has been supplied**, so
no historical comparison has been run and no result exists. Drop real files in and the
commands below produce the dataset and the report.

## Quick start

Run these **one at a time** — steps 1+ need real Cboe files, and step 1 has a manual
edit in the middle of it.

```bash
# 0. Nothing but plumbing — synthetic inputs, not a result.
python -m research.mm_attributed_gex.cli pipeline-check

# 0b. No Cboe files yet? Generate a synthetic set and rehearse steps 1-3 on it.
#     Invented columns, random numbers — for learning the workflow, never a result.
python -m research.mm_attributed_gex.cli make-sample

# 0c. Add --anchor-to-db to put that synthetic flow over REAL contracts from your
#     database, so steps 3-4 below can be rehearsed too. Read-only. The flow is
#     still invented, so every MM number it produces is meaningless.
python -m research.mm_attributed_gex.cli make-sample --anchor-to-db

# 1. Read a delivered Cboe file and PROPOSE a column mapping.
python -m research.mm_attributed_gex.cli inspect-cboe <file> --save profile.json

# 1b. Review the mapping against Cboe's field docs, then confirm it.
#     Without --reviewed this prints the mapping and refuses; an unconfirmed
#     profile refuses to load anywhere else, by design.
python -m research.mm_attributed_gex.cli confirm-profile profile.json --reviewed

# 2. Check the parse, then the reconstruction + its independent reconciliation.
python -m research.mm_attributed_gex.cli check-load  <files...> --profile profile.json
python -m research.mm_attributed_gex.cli reconstruct <files...> --profile profile.json

# 3. Side-by-side dataset (read-only against the production database).
python -m research.mm_attributed_gex.cli build-dataset <files...> --profile profile.json \
    --start 2026-05-01T13:30:00Z --end 2026-06-30T20:00:00Z \
    --out research_output/mm_dataset.jsonl

# 4. Experiment battery + research report.
python -m research.mm_attributed_gex.cli backtest research_output/mm_dataset.jsonl \
    --out research_output/mm_report.md
```

Run from the repository root (`pythonpath = ["."]` in `pyproject.toml` makes
`research.*` importable, same as the test suite).

## Experiment arms

| Flag | Effect |
|---|---|
| `--include-censored` | include left-censored series (partial-data arm) |
| `--raw-positioning` | drop ZeroGEX's horizon-occupancy weighting |
| `--net-flow-estimator` | quantity = buys − sells, ignoring the open/close flag |
| `--headline-universe 0dte` | headline columns from the 0DTE universe |

Every universe (`0dte`, `nearest`, `weekly`, `near_term`, `all`) is computed on every
row regardless, under `universes` in the JSONL — a 0DTE-only conclusion never needs a
re-run.

## Layers

| Module | Responsibility |
|---|---|
| `schema.py` | `ParticipantActivity` — the exchange-agnostic normalized record |
| `cboe/profiles.py` | declarative column mapping; JSON round-trip; `confirmed` flag |
| `cboe/loader.py` | streaming csv / csv.gz / zip / parquet → records |
| `cboe/inspect.py` | propose a mapping from a real header; name what it could not map |
| `inventory.py` | the long/short recursion, left-censoring, expiration retirement |
| `confidence.py` | per-series and gamma-weighted per-snapshot confidence |
| `reconcile.py` | open-interest identity + zero-sum checks against ZeroGEX data |
| `gex.py` | MM inventory → gamma@spot / flip / net GEX via production kernels |
| `walls.py` | strike structure: wall definitions A and B, nodes, concentration |
| `sources.py` | read-only production database access |
| `dataset.py` | two-pass replay → the side-by-side dataset |
| `outcomes.py` | forward market outcomes |
| `stats.py` | HAC OLS, logit, bootstrap, permutation, walk-forward, BH |
| `backtest.py` | the experiment battery |
| `report.py` | verdict logic + markdown/JSON report |

Cboe parsing is not coupled to the GEX engine. Supporting another exchange's
Open-Close feed means writing one more loader that emits `ParticipantActivity`.

## The one idea worth knowing

Production sums `sign(type) · γ · OI · 100 · S² · 0.01` with `+` for calls and `−` for
puts — a *modeled* dealer-positioning proxy. MM attribution replaces that sign with the
observed position. Since BS gamma is identical for calls and puts at the same
`(K, T, σ)`, a signed MM quantity is encoded as a synthetic row (`net > 0 → 'C'`,
`net < 0 → 'P'`, `oi = |net|`) and pushed through the **unmodified** production kernel.
Exact, not approximate — and it means the flip resolver, DTE ramp, structural gates and
interpolation are shared rather than reimplemented, so the only thing that differs
between the two methodologies is the attribution.

## Tests

```bash
pytest tests/ -k mm_attributed -q
```

172 tests across ingestion, inventory, gamma, walls, confidence, reconciliation,
outcomes, statistics, replay and verdict logic. Synthetic examples throughout, sized so
the correct answer can be checked by hand. Synthetic data is never used as evidence
about the methodology.
