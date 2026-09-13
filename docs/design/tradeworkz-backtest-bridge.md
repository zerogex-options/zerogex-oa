# TradeWorkz™ ↔ Backtesting Bridge — Design

**Status:** in progress — the spec-driven bot core is shipped; the deploy
plumbing + UI are the remaining work. · **Repos:** `zerogex-oa`, `zerogex-web`

> **Update:** the *catalog* half of the unification shipped separately — all
> three surfaces (Bot Trading, Backtesting, Pattern Insights) now read one
> strategy catalog, and every catalog strategy is backtestable. See
> [`strategy-catalog.md`](strategy-catalog.md). This document covers the
> remaining piece: deploying a **user's own** custom strategy as a live bot.

## The idea

Backtesting and the TradeWorkz bot fleet are the **same product in two time
directions**: a custom strategy tested over the **past** (backtest) vs. traded
**forward, live** (a bot on the leaderboard). Unifying them under the TradeWorkz
brand lets us tell the one story competitors can't: **"Backtest it → watch it
trade live → follow the winners."** A backtest is a promise; a live, public,
out-of-sample track record is the receipt.

The merge is **brand + product + a deploy bridge**, not an engine rewrite. The
historical-replay engine (`src/backtesting/`) and the live bot engine
(`src/tradeworkz/`) each do their job well; we connect them, we don't fuse them.

## Architecture

```
  Backtest a custom strategy (BacktestSpec.strategy)
        │  "Deploy live"
        ▼
  tw_bots row (strategy_class='spec_strategy', params.strategy = the rule)
        │
        ▼
  SpecStrategyBot  ── evaluates the SAME conditions live via _passes ──►  TradeSignal
        │                                                                    │
  live fleet engine (src/tradeworkz/engine.tick) ── fills, capital, P&L ──►  leaderboard
        │
        ▼
  Leaderboard shows the bot's live equity NEXT TO its backtest result
```

## What's shipped

- **`src/tradeworkz/bots/spec_strategy.py` — `SpecStrategyBot`** (+ tests). A
  data-driven bot: its entries come from a stored `BacktestSpec.strategy`, and
  it evaluates those conditions against the live `MarketSnapshot` using the
  **exact same `_passes`** the backtest uses over history — so backtest ≡ live
  behavior by construction. Scope v1: directional single / vertical, exiting on
  the base class's spot-level target/stop/time policy.
- **`is_live_deployable(strategy)`** — a saved strategy is deployable only when
  every condition field maps to the live snapshot (`LIVE_MAPPABLE_FIELDS`) and
  the structure is directional. Fields the snapshot can't supply **fail closed**
  (the bot won't fire on a half-evaluated rule).
- **Registry wiring** (shipped with the strategy catalog, see
  [`strategy-catalog.md`](strategy-catalog.md)). `SpecStrategyBot` is now
  registered in `registry.STRATEGY_CLASSES` under `'spec_strategy'`, so
  `get_bot_class('spec_strategy')` resolves and the fleet loader can
  instantiate a user-deployed bot row. It was previously absent, which meant
  the class existed and was unit-tested but could never actually be loaded.
  It is listed in `NON_CATALOG_STRATEGY_CLASSES` and so exempt from the
  catalog-coverage check: its behavior comes from a saved `BacktestSpec`
  rather than a catalog thesis, so it is deliberately not a catalog entry.

The bot is **not** in the default roster, so nothing runs until a deploy flow
explicitly creates its `tw_bots` row — production behavior is unchanged.

## What remains (sequenced)

1. **Persistence.** Let `tw_bots` carry a user-created bot with
   `strategy_class='spec_strategy'` and `params.strategy`, owner-scoped via a
   new `end_user` column, alongside the catalog-projected roster. (The registry
   half of this step is done — see above.)
2. **Deploy API.** `POST /api/backtest/runs/{id}/deploy` (or on a saved config):
   validate `is_live_deployable`, provision a `tw_bots` + `tw_bot_capital` sleeve
   from the run's strategy + sizing, return the bot id. Guard rails: one bot per
   (user, strategy) hash, a cap on live bots per user, Pro-tier only.
3. **Snapshot MSI extension.** Attach `msi` / `msi_regime` to
   `MarketSnapshot.extra` from `signal_scores` in the live context builder, then
   add them to `LIVE_MAPPABLE_FIELDS` — so MSI-based strategies deploy too.
   (Everything else already maps.)
4. **Neutral structures.** Give the bot an option-premium exit path so
   straddles/strangles/condors can deploy (they currently return no signal).
5. **UI.** A **"Deploy live"** button on a completed backtest / shared report →
   creates the bot and links to its leaderboard card. On `/trading-signals`
   (now **Bot Trading**), show user-deployed bots and, on each, a
   **backtest-vs-live** strip (the promise next to the receipt).

## Why this order

The bot core (done) is the risky, reusable heart and is fully unit-tested in
isolation. Persistence + API + registry (steps 1–2) make it *usable* without
touching the live tick loop's math. The snapshot/neutral/UI work (3–5) widen
coverage and surface it. Each step is shippable and reversible; none flips live
behavior until a user clicks Deploy.
