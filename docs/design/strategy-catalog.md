# The Strategy Catalog — one source of truth for three surfaces

**Status:** shipped (backend + frontend) · **Repos:** `zerogex-oa`, `zerogex-web`
**Module:** `src/strategies/` · **Audit:** `make strategy-catalog-audit`

## The problem this solves

ZeroGEX showed strategies in three places, and each place had its own list.

| Surface | Route | Where its strategies came from | Count |
|---|---|---|---|
| Bot Trading | `/trading-signals` | `src/tradeworkz/registry.py` — ~900 lines of hand-written `BotSpec` literals | 27 bot classes |
| Backtesting | `/backtesting` | `PlaybookEngine._discover_builtin_patterns()` | 18 patterns |
| Pattern Insights | `/backtesting/insights` | `playbook_pattern_stats`, keyed on pattern id | 18 patterns |

Only **one** id was common to all three (`vix_regime_breakout`), and even that
one disagreed with itself: the bot gated on `min_vix = 16.0`, the pattern on
`18.0`, for no recorded reason. Ten more strategy pairs were the same thesis
under different names (bot `gamma_flip_defender` ↔ pattern
`gamma_flip_bounce`). Twelve bot-only strategies — the entire v3–v8 tier —
could not be backtested from the product at all; they were reachable only from
the offline `make tradeworkz-backtest` CLI. Seven patterns had no bot, so
nothing could trade them, including `gex_gradient_trend`, the one strategy with
a conclusive measured edge.

Customer-facing descriptions were scraped out of pattern module docstrings, so
a strategy's explanation lived in a different file from the strategy and could
silently disagree with the bot trading the same idea.

## The shape of the fix

`src/strategies` is now the only place a strategy is defined. The three
surfaces read it; none of them defines anything.

```
                    src/strategies/catalog.py
                     35 StrategyEntry records
                    (identity · thesis · params
                     · bindings · evidence · stage)
                               │
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
  tradeworkz/registry     backtesting/meta      backtesting/queries
  projects BotSpecs,      publishes the         folds measured stats
  layers bot_params       testable universe     onto catalog ids
        │                      │                      │
        ▼                      ▼                      ▼
   Bot Trading            Backtesting          Pattern Insights
```

A bot **pulls the general strategy and tunes it to its own spec**: catalog
`params` are the shared defaults, `bot_params` is the bot's explicit override
layer (`effective_bot_params()` merges them). The `min_vix` disagreement is now
visible as exactly that — catalog default `18.0`, bot override `16.0`, with a
comment saying which is better-evidenced — instead of being two unrelated
numbers in two files.

### Identity: nothing is ever renamed

`tw_trades` and `tw_positions` have foreign keys onto `tw_bots.id`;
`signal_action_cards` and `playbook_pattern_stats` key on the pattern id.
Renaming either would orphan history. So each entry carries up to three ids:

- `id` — the canonical catalog key, permanent. Where both engines exist it
  follows the **pattern** id, because that is the id already customer-visible
  in Backtesting and Pattern Insights (two of the three surfaces).
- `bot_id_legacy` — the `tw_bots.id` history already wrote, when it differs.
- `pattern_id_legacy` — likewise for the playbook pattern.

`canonical_id()` folds any of them onto the entry, so one strategy reads as one
row however it was measured, and `make tradeworkz-backtest --bots <id>` accepts
either spelling.

### Stage describes evidence, not deployment

```
RESEARCH ──► CANDIDATE ──► VALIDATED          SUPERSEDED        RETIRED
(default)    (promising)   (cleared gate)     (better impl)     (exhausted)
```

Live capital is a **separate** question: `is_provisionable` requires
`stage is VALIDATED` **and** a bot binding. Keeping them apart is what lets a
strategy be actively refined — backtested, measured, tuned — with no paper
capital riding on it. That is the state 22 of the 35 strategies are in, and
`RESEARCH` is deliberately the resting state, not a failure grade.

The practical consequence today: `DEFAULT_ROSTER` is **empty**, unchanged from
before this work. The one `VALIDATED` strategy has no bot, so nothing is
funded. The audit reports that as the highest-value gap in the catalog.

### Backtest coverage: every strategy, one pricing path

All 35 strategies are now backtestable — 18 by replaying the Action Cards they
actually emitted live, 17 by replaying the bot's own entry rule over
reconstructed as-of snapshots (`src/backtesting/bot_replay.py`).

Bot replay emits `CardRow`s rather than running a parallel simulator, so
`engine.run_backtest` prices them through the identical forward walk, fill
model, sizing and concurrency cap as every other card. That is what makes a
bot strategy and a pattern strategy in one run genuinely comparable — one
equity curve, one set of assumptions. Where both bindings exist the pattern
wins, because cards that fired live are the stronger claim; bot replay is
tagged `source='bot_replay'` in `playbook_pattern_stats` so a reconstructed
measurement is never silently averaged with a live-emitted one.

Faithfulness: snapshots are bounded to `timestamp <= t`, the clock is injected
so time-of-day gates evaluate on replay time, and the bias veto and RTH
no-new-opens cutoff are applied exactly as the live engine applies them.
Disclosed limitation: `gex_historical_stats` regime percentile bands are a
nightly 30-day aggregate with no per-instant history, so a symbol-relative
strong/weak split uses today's bands. It shifts slowly and never flips a
regime's sign, but it can nudge a boundary entry.

## The retirement policy

Retirement is near-irreversible — a retired strategy stops being refined and
the thesis stops being tested — so the bar is set where a wrong call is
affordable. `src/strategies/policy.py`:

| Requirement | Value | Why |
|---|---|---|
| History behind the deepest screen | **1825 days (5y)** | Long enough to span genuinely different regimes, not one quarter's weather |
| Conclusive tuning generations | **3** | One failure is a parameter choice; three independent re-parameterisations failing is a statement about the thesis |
| Trades across conclusive screens | **200** | A five-year window that produced 40 trades has not tested the mechanism |
| No screen ever found an edge | — | A measured edge means refine, not retire |

`UNDERPOWERED`, `INSUFFICIENT` and `INVALID` runs never count toward
retirement. A screen whose gates never opened, or that produced five trades,
has not tested anything. This is the rule that keeps `profile_shelf_breaker`
(profit factor 2.28 on five trades) out of the retirement pile.

### Nothing is retirement-eligible, and that is correct

The deepest screen anywhere in the catalog is **90 days — 4.9% of the bar**.
`option_chains` is pruned at `DATA_RETENTION_DAYS` (60 in `.env.example`, 90
code default) and `option_chains_archive` only began accumulating in spring
2026.

So the 2026-08-09 fleet shelving — which retired 12 bots on a **45-day**
screen — does not meet this policy, and those strategies are now `RESEARCH`
rather than retired. Their `tw_bots` rows stay `enabled=false` with zeroed
sleeves (no capital moved), but they are back in the catalog, backtestable,
measured in Pattern Insights, and on the refinement queue.

Reaching the five-year bar needs the deep-history backfill costed in
[`historical-options-data-vendors.md`](historical-options-data-vendors.md):
~$125–400/mo vendor plus the ~$2,000/mo OPRA non-display entitlement that
dominates the budget. Until then `RETIRED` is an unreachable state by design,
and `make strategy-catalog-audit` says so on every run.

## The research ledger

Each entry carries its `ResearchRun` history: when it was screened, over what
window, how many trades, the verdict, and the numbers. That is what makes the
catalog a working record rather than a list — the 2026-08-09 screen, the four
successive flow-direction failures, the harness bugs that invalidated two
screens, and the calibration runs are all in there with their notes.

`tuning_generation` is how "we tried tuning it" is expressed: generation 0 is
as-shipped, each deliberate re-parameterisation bumps it, and the retirement
policy counts *distinct generations that concluded NO_EDGE* — so re-running the
same parameters twenty times never accumulates toward retirement.

Two family-level findings are recorded in the catalog's own comments because
they are findings about mechanisms rather than implementations:

- **Aggressor order flow (follow-the-flow) is closed** pending deeper history.
  Four independent formulations — cumulative premium, windowed premium plus
  acceleration, and delta-weighted hedge obligation both behind and ahead of
  the tape — failed with the same signature (PF 0.31 / 0.33 / 0.32, win rates
  22–33%, wins *smaller* than losses, which is a directional-prediction failure
  rather than a stop/target tuning problem). The contrarian read
  (`climax_flow_fade`) is the open hypothesis.
- **Wall fades want a credit structure.** Three wall-fade failures share a
  structural diagnosis: a debit vertical needs price to *move* to a target, but
  "the wall holds" is a boundary, not a target.

## Using it

```bash
make strategy-catalog-audit                      # the full report
make strategy-catalog-audit ARGS="--family wall" # one thesis family
make strategy-catalog-audit ARGS="--json"        # machine-readable
make strategy-catalog-check                      # non-zero on integrity problems (CI)
```

```python
from src.strategies import all_strategies, get, canonical_id, can_retire

get("call_wall_rejector").id          # 'call_wall_fade' — legacy id folds
canonical_id("gamma_flip_defender")   # 'gamma_flip_bounce'
can_retire(get("hedge_impulse_quiet_tape")).blockers
# ('deepest screen covers 60d, needs 1825d (5y)',
#  '1 conclusive tuning generation(s), needs 3')
```

### Adding a strategy

1. Add a `StrategyEntry` to the right family tuple in `catalog.py`: identity,
   thesis prose, tier, direction, `stage=Stage.RESEARCH`, and default `params`.
2. Bind an engine — `bot_class` (and `bot_id_legacy` if the bot shipped under a
   different id), and/or `has_pattern=True`.
3. Screen it, and record the run in `research`.
4. `make strategy-catalog-check` — it verifies the binding resolves, the id is
   unique, the prose exists, and the stage is supported by the evidence.

Promotion to `VALIDATED` is not a judgement call: `can_promote` reads the
research log, so the run that justified it stays in the catalog and the
decision is auditable after the fact.

## What this does not change

- **No live behavior changed.** `DEFAULT_ROSTER` was empty before and is empty
  now; the same 14 bot ids are force-disabled on provision; no capital moved.
- **ES and NQ stay out.** They carry no option chain of their own — their
  levels are SPX/NDX-derived and projected onto the futures price axis, read-
  side only, never persisted. All three surfaces price real option round-trips
  against `option_chains`, so there is nothing to fill. See
  `docs/runbooks/es_nq_futures_rollout.md`.
- **The two engines are still two engines.** The catalog connects them; it does
  not fuse the live tick loop with the historical replay. That remains the
  right call for the same reason
  [`tradeworkz-backtest-bridge.md`](tradeworkz-backtest-bridge.md) gave.
