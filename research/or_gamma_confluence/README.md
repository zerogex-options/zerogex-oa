# Opening-range extension × gamma confluence

Research-only. **Changes no production behaviour**: it imports from `src`, `src`
imports nothing from here, every database statement is a `SELECT`, and outputs
go to files.

Phase 1 repository assessment and the full design rationale:
[`docs/design/or-extension-gamma-confluence.md`](../../docs/design/or-extension-gamma-confluence.md).

## The question

> A trader watches price reach a fixed level projected away from the opening
> range. Does ZeroGEX gamma structure that was **already there** change what
> happens next — and can it say *which* extensions matter?

Origin: a customer's observation that "the further from the opening price, the
stronger the rubber band effect when it reverts", plus a second claim that
levels which are *not* respected signal continuation rather than reversion.

Three hypotheses, none assumed:

1. **Stretch.** Deeper extension → higher probability the previous rung trades
   before the next one.
2. **Confluence.** A pre-existing gamma level at that price raises it further.
3. **Regime.** Sign of dealer gamma / side of the flip decides whether to
   expect reversion or continuation at all.

## Status

**Pipeline complete, tested, and verified end to end on synthetic data. No
result yet** — this session had no database access, so Phases 3–6 have not run.
Every number the `selftest` prints is invented.

| Phase | State |
|---|---|
| 1 — repository discovery | done (design doc) |
| 2 — data / event harness | **done** (this package, 53 tests) |
| 3 — statistical research | not run — needs a production database |
| 4 — trade simulation | not built — see *Costs*, below |
| 5 — TradeWorkz candidates | not built |
| 6 — final report | not written |

## Quick start

```bash
# 0. Plumbing only — no database, no market data, invented numbers.
python -m research.or_gamma_confluence.cli selftest

# 1. How much history is actually there, and is the publish clock usable?
python -m research.or_gamma_confluence.cli coverage

# 2. Label touch events over a window (read-only against production).
python -m research.or_gamma_confluence.cli build-dataset NQ NDX \
    --start 2026-06-01 --end 2026-09-05 \
    --out research_output/orgc_events.jsonl

# 3. Cohorts, sensitivity grids, chronological out-of-sample.
python -m research.or_gamma_confluence.cli analyze \
    research_output/orgc_events.jsonl \
    --out research_output/orgc_report.md \
    --json-out research_output/orgc_summary.json \
    --csv-out research_output/orgc_events.csv

# 4. Parameter neighbourhoods, reported as a surface not a leaderboard.
python -m research.or_gamma_confluence.cli sweep NQ \
    --start 2026-06-01 --end 2026-09-05
```

`make orgc-selftest`, `orgc-coverage`, `orgc-dataset`, `orgc-analyze`,
`orgc-sweep` wrap these.

**Run `coverage` first.** It is the only thing that can tell you how much of
this study is possible on a given deployment, and it checks an assumption the
code cannot: whether `created_at` is a real publish clock or has been made
fiction by backfilling.

## The look-ahead discipline

This is the whole point of the package, so it is worth stating precisely.

### `timestamp < touch` is NOT sufficient

`gex_summary.timestamp` is the **option-chain instant the levels were computed
from** (`src/analytics/main_engine.py:3953`), not the instant they existed. The
engine computes, then writes (`created_at`), then the indicator polls up to 30 s
later. A frame stamped 10:13:00 was on nobody's chart at 10:13:00.

So `availability_clock` picks which instant counts:

| clock | `available_at` | means |
|---|---|---|
| `data` | `timestamp` | optimistic — the level did not exist yet |
| `published` | `created_at` | the platform knew it |
| `visible` | `created_at + poll lag` | **default** — a trader could have acted on it |

`gamma_min_lead_seconds` is measured against that, and `GammaTimeline` orders
and searches on `available_at`, not on `timestamp`.

### Walls re-centre, so a level read at the touch is partly caused by it

Production's Call Wall is the largest call-gamma strike **above spot**
(`src/analytics/walls.py:249`); when price trades through a strike it becomes
the Put Wall and the Call Wall re-points upward. Ranked GEX levels do the same,
and 0DTE gamma piles up at the money.

Two defences:

* Ranks are recomputed from `gex_by_strike` **at each frame's own timestamp,
  against that frame's own spot** — never the touch spot. For ES/NQ that spot
  is the **index** spot, because the strikes are index strikes.
* Every level carries `recenters`, and cohort 12 restricts confluence to the
  levels that *don't* chase spot (max pain, flip, pin). If the effect exists
  only on chasing levels, it is probably the re-centring artefact.

### Fail closed

A frame with no `created_at` under a publish-based clock, a negative lag, or a
lag beyond `max_publish_lag_seconds` (a backfilled row, whose `created_at` is
the backfill time) **rejects the whole session**. A touch with no qualifying
frame is recorded as *no pre-existing gamma* — it joins the comparison group,
it never reaches for the next frame.

## What is reused rather than rebuilt

| Need | Reused from |
|---|---|
| Read-only DB session | `research/mm_attributed_gex/sources.py:84` |
| Minute bars, cash **and** futures routing | `research/msi_regime_excursion/sources.py:132` |
| Instrument table (all six symbols) | `research/msi_regime_excursion/sources.py:INSTRUMENTS` |
| MFE / MAE / forward excursion | `research/msi_regime_excursion/excursion.py:246` |
| Wilson CI, session-clustered bootstrap, BH-FDR | `research/msi_regime_excursion/stats.py` |
| Chronological walk-forward idiom | `research/wall_break_odds/model.py:310` |
| Ranked wall ladder (`C1..Cn` / `P1..Pn`) | `src/analytics/walls.py:165` |
| Futures ↔ index basis, with historical anchor | `src/jobs/futures_projection.py:460` |
| Futures symbol map and tick sizes | `src/symbols.py` |
| Cash-index open-bar phantom repair | `src/tools/cash_index_open_repair.py:155` |
| Session / DST / holidays | `src/market_calendar.py` |

Stdlib only — no numpy — matching `wall_break_odds` and `msi_regime_excursion`.

## The event definition

| | |
|---|---|
| **ladder** | Mode A: `ORH + k·R` above, `ORL − k·R` below. Mode B: `open ± k·R`. `k` steps by `extension_step` to `max_extension`. |
| **anchor** | `k=0` is a PRICE, not an event: it exists so rung `±1` has a reversion target. Events start at `±50%`. |
| **frozen** | `ORH`/`ORL`/`R` are computed once at the close of the OR window and never recomputed. |
| **touched** | price came within `touch_tolerance_bp` of the rung, at or after the OR closes. |
| **reversal_first** | the PREVIOUS rung traded before the next one. |
| **continuation_first** | the NEXT rung traded first. |
| **continuation_same_bar** | the touch bar itself ran past the next rung — price never paused. |
| **ambiguous** | both rungs inside one 1-minute bar; intra-bar order is unknowable, so no winner is picked. |
| **censored** | neither before the bell. |

`ambiguous` and `censored` are **reported, never folded into the rate
denominator**.

Bars are period-START stamped (`src/ingestion/main_engine.py:530`), so a
5-minute opening range is the bars stamped 09:30–09:34.

**One grind at a level is one observation.** A rung fires at most once per
session by default; `--rearm` enables re-firing but still requires price to
have travelled `rearm_distance_r · R` away, so a slow grind cannot re-fire on
the cooldown clock alone.

## Known limitations

* **Sample size will bind on the ranked-GEX arm.** `gex_summary` and
  `underlying_quotes` are retention-exempt; `gex_by_strike` — the only source
  of ranked levels — is pruned at `DATA_RETENTION_DAYS`. `coverage` prints both
  windows. Run with `--no-gex-ranks` to use the long-history arm alone.
* **The ±3% ingest strike band censors the far extensions.** A rung at −300%
  of R on a wide day can sit outside the band, where **no gamma level can
  exist by construction** — precisely where the reversion hypothesis is most
  interesting. This must be read as coverage, not as "far extensions have no
  confluence".
* **Dealer sign is modelled, not observed.** Walls come from the
  call-positive / put-negative open-interest convention. Carried forward from
  `research/wall_break_odds`; see `research/mm_attributed_gex` for the
  attribution work.
* **Correlation is not dealer causality.** A wall beside a reverting extension
  is a co-location, not a mechanism.
* **The trader's own indicator is not replicated.** His implementation is
  proprietary; HMA / EMA / VWAP-slope / trade-bias are *candidate* filters, and
  which (if any) helps is a research question.

## Costs

`ResearchConfig` carries `slippage_ticks`, `commission_round_turn` and
`entry_delay_bars`, but **no trade simulator uses them yet**. TradeWorkz has no
futures execution model at all — no point value, tick value or round-turn
commission anywhere in `src/tradeworkz`, `src/backtesting` or
`src/signals/execution.py`; every bot it has shipped trades option legs. The
one test covering this is `@pytest.mark.skip`ped with that reason rather than
faked. See §6 of the design doc.

**A reversal rate above 50% is not an edge until it is priced.**
