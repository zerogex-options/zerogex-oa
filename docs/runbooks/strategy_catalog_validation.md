# Validating the strategy catalog (and the gex_gradient_trend bot)

Four tiers, cheapest first. **Tier 1 runs anywhere. Tiers 2–4 need a database
with real history**, so nothing in them has been run yet — they are the work
that has to happen on staging before this goes near production.

Design docs: [`strategy-catalog.md`](../design/strategy-catalog.md),
[`tradeworkz-backtest-bridge.md`](../design/tradeworkz-backtest-bridge.md).

---

## Tier 1 — static checks (no DB, seconds)

These are green today. They prove the catalog is internally coherent and the
bot reproduces the pattern's arithmetic; they prove **nothing** about whether
the bot makes money.

```bash
# Catalog integrity: every binding resolves, every id is unique, every stage
# is supported by its own evidence, every playbook pattern is claimed.
make strategy-catalog-check

# Read the catalog the way an operator would.
make strategy-catalog-audit
make strategy-catalog-audit ARGS="--family trend"

# Engine-side tests.
pytest tests/test_strategy_catalog.py \
       tests/test_bot_replay_cards.py \
       tests/test_tw_gex_gradient_drift.py \
       tests/test_backtesting_meta_catalog.py \
       tests/test_backtest_insights.py \
       tests/test_playbook_gex_gradient_trend.py -q

# Formatting / lint on the changed surface (CI runs black --check src tests).
black --check src/strategies src/backtesting src/tradeworkz tests
flake8 src/strategies src/backtesting/bot_replay.py src/tradeworkz
```

```bash
# Client side.
cd ../zerogex-web/frontend
npx tsc --noEmit
npx eslint app/backtesting app/trading-signals
npm run test:strategy-catalog-view
npm run test:insights-view
npm run test:backtest-featured
```

**What Tier 1 actually establishes.** The highest-value case is the
bot-vs-pattern parity block in `tests/test_tw_gex_gradient_drift.py`: both are
handed the *same* market state (built from the pattern test's own `_ctx`, so
the fixtures cannot drift apart) and must agree on whether to fire, the
direction, the strike, and the target. The ATR estimator is asserted
bit-identical to the pattern's across five input shapes, because strike offset
and target are both multiples of it.

---

## Tier 2 — against real history (the tier that decides anything)

### 2a. Schema

**No migration.** The catalog is code; measured rows still go to
`playbook_pattern_stats`, and `bot_replay` writes under the existing
`source` column. Confirm nothing is pending:

```bash
make schema-apply     # idempotent; expect no table creations
```

### 2b. Does the bot reproduce the pattern's measured edge?

This is the question that gates funding. The pattern measured PF 2.80 on 33
QQQ trades over 60 days. Run the bot's own screen over the same window and
compare:

```bash
# The bot, through the bot harness.
make tradeworkz-backtest ARGS="--bots gex_gradient_trend --days 60 --interval-min 5"

# The pattern's number, per-trade, for the same strategy and window.
python -m src.tools.pattern_calibration_refresh \
  --explain gex_gradient_trend --underlyings QQQ --days 60 --no-touch
```

Read the comparison honestly:

| Bot screen result | What it means | Action |
|---|---|---|
| PF ≥ 1.1, positive expectancy, ≥ 20 trades | The bot inherits the edge | Record an `EDGE` run with `harness="tradeworkz-backtest"` in the catalog entry → `is_provisionable` flips true → next provision funds it |
| Positive but < 20 trades | The `max_minutes_to_close=30` window throttles entries vs the pattern's one-per-close | Record `INSUFFICIENT`. Widen the window or lengthen the screen — do **not** promote |
| Zero trades | A gate never opened. Read `miss_reasons` in the report | Diagnose the gate, record `UNDERPOWERED` |
| PF materially below the pattern's | The bot is not the pattern. Most likely suspects, in order: the conviction floor (no pattern analogue), the strike grid, the entry window | Record `NO_EDGE` for this tuning generation and fix the divergence |

> **Read the comparison with this caveat.** The pattern picks its expiry as
> `entry_date + 5 CALENDAR days`, which is a Saturday for a Monday entry and
> a Sunday for a Tuesday entry. `_fetch_leg_quote_from` matches expiry
> **exactly** (only strike is fuzzy-matched), so those cards find no quote and
> are dropped. The PF 2.80 / n=33 sample is therefore **filtered by day of
> week**, not a clean 60-day sample. The bot walks weekdays instead, so it
> fires on Mondays and Tuesdays where the pattern effectively could not, and
> some divergence is expected for that reason alone. To compare like with
> like, restrict the bot screen to Wed–Fri entries, or fix the pattern's
> arithmetic first and re-run the calibration (a separate decision: it
> changes live signal output and invalidates the existing record).

A **zero-trade or conviction-dominated** result is the specific failure the
catalog already records for `weekly_charm_grind`, and
`test_a_typical_gate_passing_setup_also_clears_conviction` exists to catch it
before it reaches a screen. If the screen still shows it, the quality
saturations (`quality_gradient_saturation`, `quality_vol_saturation`) are
mis-set for the live distribution.

### 2b-i. If the screen dies before it starts

```
psycopg2.errors.QueryCanceled: canceling statement due to statement timeout
  ... in _chain_window
  cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM option_chains_archive")
```

Fixed in code (2026-09-14) — pull and re-run. The cause is worth knowing
because it will resurface anywhere else that aggregates this table without a
predicate: `option_chains_archive` is retention-EXEMPT and grows forever, and
**no index on it has `timestamp` as its leading column** (the PK is
`(option_symbol, timestamp)`; the only other index is
`(underlying, timestamp)`). Postgres can only shortcut an unqualified min/max
to an index endpoint when the target column leads, so that query seq-scanned
the whole archive and got slower every night until it crossed the 90-second
`statement_timeout`.

The probe now walks the distinct underlyings off the leading column of the
existing index and takes each one's endpoints with an equality predicate —
O(symbols x log n), no new index — bounded by its own 10-second timeout and
falling back to the hot-table window if it still fails. Adding
`option_chains_archive(timestamp)` would also have fixed the plan, but
`option_chains_indexing.md` records 3.5-21 GB indexes on this family of tables
being dropped for exactly that cost, and this query runs once per screen.

Check the table you are up against:

```bash
make query SQL="SELECT relname, n_live_tup, pg_size_pretty(pg_total_relation_size(relid)) AS total FROM pg_stat_user_tables WHERE relname IN ('option_chains','option_chains_archive')"
```

If the run logs `archive coverage probe failed; clamping the replay window to
option_chains only`, the screen still works but cannot reach past live
retention — so a thin result there is a coverage artifact, not a verdict.

### 2c. Is the bot seeing the signals it needs?

The bot reads `signal_component_scores` and `signal_scores` as-of. If the
signals engine has not written for the symbol, every entry fails closed and
the screen is vacuous rather than negative. Check coverage **first**, so a
zero-trade screen is not misread as a verdict:

```bash
make query SQL="SELECT component_name, COUNT(*) AS rows, MAX(timestamp) AS latest
                FROM signal_component_scores
                WHERE underlying='QQQ'
                  AND component_name IN ('gex_gradient','range_break_imminence','vol_expansion')
                  AND timestamp > NOW() - INTERVAL '60 days'
                GROUP BY 1 ORDER BY 1"

make query SQL="SELECT COUNT(*) AS rows, MAX(timestamp) AS latest,
                       COUNT(*) FILTER (WHERE components ? 'volatility_regime') AS with_volreg
                FROM signal_scores
                WHERE underlying='QQQ' AND timestamp > NOW() - INTERVAL '60 days'"
```

Both must be populated across the screen window. `gex_gradient` in particular
depends on `gex_by_strike` having been available to the scoring engine at the
time.

### 2d. Does bot_replay work for the strategies that had no backtest at all?

Twelve strategies were previously unreachable from the product. Pick one and
run it through the customer path:

```bash
curl -s -X POST localhost:8000/api/backtest/runs -H 'content-type: application/json' -d '{
  "underlying": "QQQ",
  "start_date": "2026-07-15", "end_date": "2026-09-10",
  "patterns": ["settlement_flow_snap"],
  "sizing": {"capital": 100000, "risk_per_trade_pct": 1.0, "max_concurrent": 5},
  "exit": {"stop_loss_pct": 0.5, "profit_target_pct": 0.75}
}'
# then poll /api/backtest/runs/{id} and read summary.diagnostics
```

Expect `cards_total > 0` where before the run would have found nothing. A
`cards_in_scope` of 0 with a non-zero `cards_total` means the selection
routing is wrong; `cards_total == 0` means the replay produced no signals —
check `miss_reasons` by running the same strategy through
`make tradeworkz-backtest ARGS="--bots settlement_flow_snap --days 60"`.

Cross-check that a pattern-backed strategy still reads the same as before:
run `gex_gradient_trend` on QQQ over the calibration window and confirm the
result is in the same region as the Pattern Insights row. A large divergence
means the catalog relabelling changed which cards are in scope.

### 2e. Do the three surfaces agree?

The whole point. For one strategy, confirm the name, thesis, tier and stage
are identical in all three places:

```bash
curl -s localhost:8000/api/backtest/meta | jq '.strategies[] | select(.id=="gex_gradient_trend")'
curl -s 'localhost:8000/api/backtest/insights/patterns?source=option_pnl' \
  | jq '.[] | select(.pattern=="gex_gradient_trend")'
curl -s localhost:8000/api/tradeworkz/bots | jq '.bots[] | select(.strategy_id=="gex_gradient_trend")'
```

Then the same in the UI: `/backtesting` (family-grouped picker),
`/backtesting/insights` (Stage column, and the unscreened rows at the bottom),
`/trading-signals` (stage badge on the card).

Two specific things to eyeball, because they are the ones that read wrong if
the plumbing is off:

- an unscreened strategy shows as an explicit "not screened" row, not as an
  absent one;
- a bot whose catalog entry exists reads **UNFUNDED**, not PAUSED.

---

## Tier 3 — live-behavior safety (before/after deploy)

The claim is that **no capital moves**. Verify it rather than trusting it.

```bash
# Before deploy: snapshot the fleet's funded state.
make query SQL="SELECT b.id, b.enabled, c.starting_capital, c.current_capital
                FROM tw_bots b LEFT JOIN tw_bot_capital c ON c.bot_id=b.id
                ORDER BY b.id" > /tmp/fleet_before.txt
```

Deploy, restart, then re-run the same query into `/tmp/fleet_after.txt` and
diff. **Expect an empty diff.** `DEFAULT_ROSTER` is empty because
`gex_gradient_trend.bot_validated` is false, so `provision_defaults` seeds
nothing and re-disables the same 14 ids it always did.

```bash
diff /tmp/fleet_before.txt /tmp/fleet_after.txt   # expect no output
make services-health
```

Also assert the invariant directly, so a future change that would fund
something cannot pass silently:

```bash
python -c "
from src.tradeworkz.registry import DEFAULT_ROSTER, DISABLED_BOT_IDS
assert DEFAULT_ROSTER == (), DEFAULT_ROSTER
assert len(DISABLED_BOT_IDS) == 14, DISABLED_BOT_IDS
print('fleet unfunded, 14 ids disabled — unchanged')"
```

`setup/database/diagnostics/tradeworkz_invariants.sql` is the existing
fleet-consistency check; run it after the restart.

---

## Tier 4 — what cannot be validated yet

**The retirement policy is untestable end to end on our data.** It requires
five years of history behind the deepest screen; the deepest anywhere in the
catalog is 90 days. `make strategy-catalog-audit` reports this on every run,
and `test_nothing_in_the_catalog_is_retirement_eligible_today` asserts it — so
the policy's *logic* is unit-tested against synthetic runs
(`test_retirement_requires_history_generations_and_trades` and friends), but
its *application* waits on the deep-history backfill costed in
[`historical-options-data-vendors.md`](../design/historical-options-data-vendors.md).

Until then, treat any pressure to retire a strategy as a data-acquisition
decision, not a code change.

**The bot's live edge is also not validated by any of the above.** A screen is
a backtest. The receipt is a forward, out-of-sample paper record on the
leaderboard, which only starts accumulating after the bot is funded — and
funding it requires Tier 2b to pass first.
