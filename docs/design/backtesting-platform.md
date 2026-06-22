# ZeroGEX Backtesting Platform — Design

**Status:** Phases 1–5 shipped (1–5a validated on live data); Phase 6 shipped (CSV export, saved/shareable configs, parameter sweeps) · **Last updated:** 2026-06-22
**Owners:** ZeroGEX engine team
**Repos:** `zerogex-oa` (engine + API), `zerogex-web` (subscriber UI)

---

## 1. What this is

A customer-facing backtesting platform layered on the existing ZeroGEX
engine. It lets a subscriber pick an underlying, a date window, and one or
more **playbook patterns** (the same patterns that drive live Action Cards),
then simulates those trades over history with **realistic bid/ask option-leg
fills** and returns an equity curve, summary statistics, and a trade blotter.

This is **not** greenfield. It productizes and generalizes the existing
single-pattern CLI harness (`src/signals/playbook/backtest.py`) into:

- a durable, retention-exempt **historical archive** so backtests can reach
  past the 90-day `DATA_RETENTION_DAYS` prune horizon;
- a reusable **simulation engine** that prices real option legs at the
  quoted spread instead of measuring underlying-touch proxies;
- an **async REST API** (`/api/backtest/*`) consumed by `zerogex-web`;
- a **B2C UI** (`/backtesting`) gated behind the existing tier system.

### Design constraints discovered in the backbone

| Constraint | Source | Consequence |
|---|---|---|
| `DATA_RETENTION_DAYS = 90` prunes `option_chains` | `src/config.py:569` | Need a durable archive the prune job never touches. |
| `option_chains` already carries per-minute `bid/ask/mid/iv/greeks` | `setup/database/schema.sql:59` | Leg-level fills are feasible inside the retained window. |
| Action Cards persist with explicit `legs` + `entry/target/stop` | `src/signals/playbook/types.py`, `signal_action_cards` | The "what to trade" is already recorded; the engine replays it. |
| Canonical fill model already exists | `src/signals/execution.py` (`leg_fill_price`) | Reuse it verbatim — long buys ask·(1+slip), short sells bid·(1−slip), invert on close. |
| Proven outcome/timing logic exists | `src/signals/playbook/backtest.py` (`compute_outcome`) | Reuse intrabar MFE/MAE + entry-trigger fill enforcement for exit *timing*; layer leg P&L on top. |

---

## 2. Architecture

```
                       zerogex-web (Next.js BFF)
   /backtesting page ──► /api/backtest/* (proxy.ts, Bearer key) ──┐
                                                                  │
                       zerogex-oa (FastAPI)                       ▼
   ┌──────────────────────────────────────────────────────────────────┐
   │ src/api/routers/backtest.py   (REST: runs, trades, equity, meta)  │
   │        │ enqueue (BackgroundTasks v1 → queue/worker later)         │
   │        ▼                                                           │
   │ src/backtesting/runner.py     (lifecycle: queued→running→done)    │
   │        ▼                                                           │
   │ src/backtesting/engine.py     (replay cards → leg fills → P&L)    │
   │   ├── src/signals/execution.py        (leg_fill_price — reused)   │
   │   ├── src/signals/playbook/backtest.py(compute_outcome — reused)  │
   │   └── src/backtesting/archive.py      (durable price source)      │
   └──────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼  Postgres
   backtest_runs · backtest_trades · backtest_equity · option_chains_archive
```

### Data flow for one run

1. **Create** — `POST /api/backtest/runs` validates a `BacktestSpec`, inserts a
   `backtest_runs` row (`status='queued'`), and schedules the job.
2. **Replay** — the engine loads `signal_action_cards` for `(underlying, window,
   patterns)`, then applies a **per-pattern cooldown** (`cooldown_minutes`,
   default `BACKTEST_SIGNAL_COOLDOWN_MINUTES=30`) to collapse the continuous
   card stream into discrete entries before pricing — the live engine emits a
   card nearly every cycle, so without this a run would price (and the
   concurrency cap then discard) thousands of near-identical signals per day.
   For each surviving Card it resolves the option leg (from the Card's own
   `legs`, falling back to a tier-based ATM contract).
3. **Entry fill** — looks up the leg's `option_chains` row at/after the Card
   timestamp; entry premium = `leg_fill_price(action='open')`.
4. **Exit timing** — reuses `compute_outcome` on the **underlying** series to
   find the exit bar (target / stop / time / no_fill).
5. **Exit fill** — looks up the leg's `option_chains` row at the exit timestamp;
   exit premium = `leg_fill_price(action='close')`.
6. **P&L** — `(exit − entry) · 100 · contracts − commissions`; position sizing
   from `BacktestSpec.sizing`; concurrency capped by `max_concurrent`.
7. **Persist** — each trade → `backtest_trades`; a chronological equity series →
   `backtest_equity`; aggregate stats → `backtest_runs.summary` (JSONB).

### Exit model (forward walk)

The engine walks the underlying series bar by bar:

1. **Entry fills at its trigger bar** — immediately for at-market Cards, or at
   the first bar that touches `entry.ref_price` for touch/break triggers (a Card
   whose trigger never prints is `no_fill`). The entry option is priced at that
   fill bar.
2. **Exit is scanned strictly after the fill bar** — a **≥1-bar minimum hold**.
   This is deliberate: the original two-point model priced entry and exit at the
   same resolution timestamp, so a target that printed *inside the entry bar*
   booked a zero-hold same-instant round trip = pure bid/ask spread loss (a
   `target_hit` that lost money). Requiring the exit to be a later bar removes
   that artifact. The first level target/stop touch resolves the exit (same-bar
   both-touch → stop, conservatively); otherwise it times out at the last bar in
   the hold window.
3. **P&L** is priced from the resolved contract's bid/ask at the fill and exit
   bars via `leg_fill_price` (long buys at ask·(1+slip), sells at bid·(1−slip)),
   net of per-contract commission both ways.

**Phase 2** extends the same walk to **option-premium exits**: resolve
`Target.kind = premium_pct` (and a configurable premium take-profit/stop) on the
option's own premium series, which also rescues Cards currently dropped as
`unresolved` (target and stop both non-level).

---

## 3. Persistence (new tables)

Appended to `setup/database/schema.sql`. All FK to `symbols(symbol)`.

- **`option_chains_archive`** — durable, retention-exempt copy of the minute
  `option_chains` rows the backtester needs. Same columns as `option_chains`
  plus `archived_at`. The DB-maintenance prune job **must skip** this table.
  v1 reads live `option_chains` when the window is inside retention and
  `option_chains_archive` otherwise; a nightly job (`tools/backtest_archive.py`,
  Phase 1.5) copies the prior day's chains in before they age out.

- **`backtest_runs`** — one row per run: `id`, `end_user`, `underlying`,
  `start_date`, `end_date`, `patterns TEXT[]`, `spec JSONB`, `status`
  (`queued|running|completed|failed`), `progress`, `summary JSONB`, `error`,
  `created_at`, `started_at`, `completed_at`.

- **`backtest_trades`** — one row per simulated trade: `run_id`, `seq`,
  `pattern`, `direction`, `tier`, `option_symbol`, `option_type`, `strike`,
  `expiration`, `entered_at`, `exited_at`, `entry_premium`, `exit_premium`,
  `contracts`, `gross_pnl`, `commission`, `net_pnl`, `return_pct`, `outcome`,
  `mfe_pct`, `mae_pct`, `hold_minutes`.

- **`backtest_equity`** — `run_id`, `t`, `equity`, `drawdown_pct` — the curve.

See `setup/database/schema.sql` for the canonical DDL.

---

## 4. API contract (`/api/backtest`)

All routes live under the `market_raw`/signals scope and are reached by the
browser via the same-origin BFF proxy. Dates are `YYYY-MM-DD` (ET sessions).

### `GET /api/backtest/meta`
Catalog for the configuration form.
```json
{
  "underlyings": ["SPY", "SPX", "QQQ"],
  "patterns": [
    {"id": "gamma_flip_break", "name": "Gamma Flip Break", "tier": "0DTE",
     "description": "..."}
  ],
  "data_window": {"earliest": "2026-03-20", "latest": "2026-06-18",
                  "retention_days": 90},
  "defaults": {"capital": 25000, "risk_per_trade_pct": 2.0,
               "slippage_pct": 0.01, "commission_per_contract": 0.65,
               "max_concurrent": 3}
}
```

### `POST /api/backtest/runs` → `202`
Body = `BacktestSpec`:
```json
{
  "underlying": "SPY",
  "start_date": "2026-05-01",
  "end_date": "2026-06-15",
  "patterns": ["gamma_flip_break", "call_wall_fade"],
  "fill_model": {"slippage_pct": 0.01, "commission_per_contract": 0.65},
  "sizing": {"capital": 25000, "risk_per_trade_pct": 2.0, "max_concurrent": 3},
  "exit": {"max_hold_minutes": null}
}
```
`patterns: []` ⇒ all patterns. Response: `{"run_id": 123, "status": "queued"}`.

### `GET /api/backtest/runs` → list recent runs (for the end-user)
### `GET /api/backtest/runs/{id}` → status + `summary`
```json
{"run_id": 123, "status": "completed", "progress": 1.0, "spec": {...},
 "summary": {"n_trades": 84, "win_rate": 0.56, "net_pnl": 3100.0,
   "total_return_pct": 12.4, "max_drawdown_pct": -8.1, "profit_factor": 1.7,
   "avg_win_pct": 31.0, "avg_loss_pct": -22.0, "avg_hold_minutes": 47,
   "by_pattern": [{"pattern": "...", "n": 40, "win_rate": 0.6, "net_pnl": 1800}]},
 "error": null, "created_at": "...", "completed_at": "..."}
```
### `GET /api/backtest/runs/{id}/trades?limit=&offset=` → blotter rows
### `GET /api/backtest/runs/{id}/equity` → `[{"t": "...", "equity": 26000.0, "drawdown_pct": -2.1}]`

Failure modes: `404` unknown run, `409` window has no archived data, `422`
invalid spec (engine returns a structured `error` on `backtest_runs`).

---

## 5. Statistics

Computed in `engine.py`, stored in `backtest_runs.summary`:

- **win_rate** = winning trades / resolved trades.
- **net_pnl / total_return_pct** = Σ net P&L; return vs starting capital.
- **max_drawdown_pct** = max peak-to-trough on the equity curve.
- **profit_factor** = Σ wins / |Σ losses|.
- **sharpe** (per-trade, annualized by √(trades/yr)) — informational in v1.
- **avg_win_pct / avg_loss_pct / avg_hold_minutes**, plus **by_pattern** breakdown.

**Diagnostics.** `summary.diagnostics` records the funnel for the run so a
0-trade result is explainable: `cards_total`, `cards_in_scope` (after pattern
filter), `cards_after_cooldown`, `priced_candidates`, `drops` (a reason→count
map: `outcome:no_fill`, `no_entry_quote`, `no_exit_quote`, `no_leg`, …),
`concurrency_skipped`, and `sized_out`.

A round-trip **commission** (`commission_per_contract × contracts × 2`) and the
**slippage_pct** widening are both applied so the curve is net of frictions —
critical because gross 0DTE underlying-touch numbers materially overstate edge
(see `backtest.py` cost notes).

---

## 6. Phasing

- **Phase 1 — v1 (shipped, validated on live data):** schema, engine, async API,
  `/backtesting` UI, tests. Reads live `option_chains` within retention.
- **Phase 1.5 (shipped):** nightly `option_chains_archive` writer
  (`src/tools/backtest_archive.py`, `make backtest-archive`,
  `zerogex-oa-backtest-archive.timer`) + an explicit prune-job exemption
  (`option_chains_archive` is intentionally absent from `DB_MAINTAIN_TABLES`),
  so windows older than 90 days resolve. The engine tries live `option_chains`
  first and falls back to the archive.
  **Plus** the pattern-calibration feedback loop
  (`docs/design/pattern-calibration.md`): the live engine can replace each
  pattern's hand-set `pattern_base` with its measured win rate, refreshed
  nightly (`zerogex-oa-pattern-calibration.timer`). Off by default.
- **Phase 1.6 — forward-walk exit engine (shipped):** replaced the two-point
  pricing model with a bar-by-bar walk — entry filled at its trigger bar, exit
  scanned **strictly after** the fill bar (≥1-bar min hold), so a target that
  prints inside the entry bar no longer books a zero-hold spread loss. Plus the
  per-pattern signal **cooldown** (`BACKTEST_SIGNAL_COOLDOWN_MINUTES`) and the
  run **diagnostics funnel** (`summary.diagnostics`, surfaced in the UI).
- **Phase 2 — option-premium exit resolution (shipped):** take-profit /
  stop-loss resolved on the option's own premium series
  (`exit.profit_target_pct` / `stop_loss_pct`), checked alongside the underlying
  level triggers and exposed in the config form. Rescues Cards whose target/stop
  are non-level (previously dropped as `unresolved`).
- **Phase 3 — custom strategy builder (shipped):** a condition builder over
  per-minute market structure (`net_gex_sign`, `flip_distance_pct`, `msi` /
  `msi_regime`, wall distances, `put_call_ratio`, …) compiled into synthetic ATM
  Cards that flow through the same forward-walk engine. `src/backtesting/
  strategy.py` as-of merges `underlying_quotes ⋈ gex_summary ⋈ signal_scores`;
  exits combine underlying level offsets with the Phase-2 premium overlay. UI:
  a "Custom strategy" mode with a dynamic condition builder on `/backtesting`.
- **Phase 4 — multi-leg structures + dedicated worker (shipped):** the engine
  prices N legs per-leg through `leg_fill_price` (net debit/credit, defined-risk
  max-loss sizing, per-leg commission); custom strategies can trade a defined-
  risk **vertical** (long ATM + short OTM by width). And a standalone
  **worker** (`src/backtesting/worker.py`, `zerogex-oa-backtest-worker.service`)
  drains queued runs via `FOR UPDATE SKIP LOCKED` with stale-run recovery, so
  long runs survive API restarts; gated by `BACKTEST_WORKER_ENABLED` (the API
  enqueues only when the worker is the executor).
- **Phase 5a — neutral defined-risk structures (shipped):** long straddle, long
  strangle, and iron condor in the custom strategy builder, via a unified
  `_risk_profile` that bounds defined risk for any structure (debit, credit
  vertical, or condor). Neutral structures are non-directional and exit on the
  premium overlay. Naked short straddles/strangles are intentionally excluded
  (undefined risk). UI: structure picker with strike-offset + condor wing.
- **Phase 5b — sizing/exit fidelity (shipped):** Greeks-aware sizing
  (`sizing.max_net_delta` / `max_net_vega` cap net position delta/vega per trade
  using the per-leg greeks in `option_chains`; net Δ/vega surfaced in the
  blotter), and a credit-structure premium-exit fix — the profit target is now a
  fraction of the **credit** (max gain) for credit structures rather than
  max-loss (which made it unreachable).
- **Phase 6 — convenience & fidelity (in progress):**
  - *CSV export (shipped):* `GET /api/backtest/runs/{id}/trades.csv` streams the
    full blotter; an "Export CSV" link sits in the Trade Blotter header.
  - *Saved & shareable configs (shipped):* `backtest_configs` table +
    `/api/backtest/configs` CRUD. A config is a named, validated `BacktestSpec`;
    each carries a random `share_token` so `/backtesting?config=<token>` clones it
    read-only into a fresh form. Owner-scoped list/get/delete mirror the runs
    ownership model. UI: a "Saved configurations" block in the config panel
    (save / load / share-link / delete).
  - *Parameter sweeps (shipped):* `backtest_sweeps` table + `sweep_id` /
    `sweep_cell` on `backtest_runs`, and `/api/backtest/sweeps` (create / list /
    get). A sweep runs one base spec across the Cartesian product of one or two
    whitelisted parameter axes (`src/backtesting/sweeps.py::SWEEPABLE`, surfaced
    in `/meta.sweep_params`); each cell is a normal run, so it reuses the engine,
    worker, and persistence. Bounded to ≤2 axes / ≤8 values / ≤24 cells. UI: an
    optional axes editor in the config panel and a results grid (1-axis table /
    2-axis heat matrix) with a metric selector. Sweep child runs are filtered out
    of the standalone Recent Runs list.
  - *Per-leg intraday option-premium target (future).*

---

## 7. Out of scope (still open)

- Custom user-defined strategies beyond the playbook patterns (Phase 3).
- A standalone worker/queue (today uses FastAPI `BackgroundTasks`; fine for the
  single-process API host, replaced in Phase 4).
- **Same-bar exit precision:** the min-hold removes the zero-hold artifact, but
  a same-bar level target still exits at next-bar market rather than the exact
  target premium. The premium overlay (Phase 2) is the precise tool when a
  defined profit/stop is wanted.
