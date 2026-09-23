# Cutover checklist — TradeStation → ThetaData

**Target: Friday 2026-09-26.** Written Tuesday 2026-09-23. T minus 3 days.

This is the flight plan. It supersedes scattered notes in the runbook's steps 13–15 for the
purpose of getting to launch. Every item is either **done**, **assigned**, or **a decision you
owe**. Nothing here is a suggestion.

---

## 1. Go / no-go gates

Four conditions. All four must be true at launch or we do not launch.

| # | Gate | State |
|---|---|---|
| G1 | Production ingestion can read ThetaData at all | ✅ **built** — `4fd69a6`, `a94c8e6`; full suite clean |
| G2 | ThetaData has written one full trading session through the real ingestion path | ⚠️ G1 done; needs the rehearsal in §6.1 |
| G3 | ES / NQ has a source that survives TradeStation being switched off | ✅ **done** — carry basis, `a94c8e6`; site label still to change |
| G4 | Every known behaviour change at cutover is written down and accepted | ⚠️ §5 |

**G1 is the whole job.** Everything else is small by comparison.

---

## 2. What is actually already done

Stated so nobody re-does it, and so the size of what remains is honest.

- **The numbers match.** 116 paired samples, SPY / SPX / QQQ. Spot, call wall, put wall and max
  pain identical on every sample; gamma flip and net GEX inside each feed's own self-variance.
  `market-data-feed-comparison-findings-2026-09.md`.
- **Contract signed, first invoice paid.** NDX added to the Indices – Market Value line, which is
  what cleared the Nasdaq GIDS permission error.
- **ThetaData proven to serve the chains**, at the strike and expiration counts production uses.
- **Two cutover-breaking defects fixed** (`b391764`, `2856ea2`, `dfabb19`): underlying volume would
  have gone blank, and the skew signal would have kept abstaining on SPX and NDX.

---

## 3. The gap: production cannot read ThetaData

`MARKET_DATA_PROVIDER` exists in `config.py:1869` and in `.env.example`. **Nothing in the
ingestion path reads it.** `stream_manager.py:28` and `main_engine.py:29` both construct
`TradeStationClient` directly and call it in 17 places. Setting `MARKET_DATA_PROVIDER=thetadata`
today changes nothing at all.

The provider abstraction is consumed by exactly two files, both tools: `src/tools/feed_compare.py`
and `src/tools/chain_depth_sweep.py`. Neither runs in production.

### 3.1 What the rewiring involves

The interface in `providers/base.py` already covers the substance:

| Production needs | Interface has | Status |
|---|---|---|
| `get_option_expirations` | same | maps directly |
| `get_option_strikes` | same | maps directly |
| `get_option_quotes` (polled) | `stream_option_quotes` / `snapshot_option_quotes` | maps |
| `get_stream_bars` (underlying) | `stream_underlying_bars` | maps |
| `get_stream_bars` (index) | `stream_index_bars` | maps |
| `build_option_symbol` | — | **decision** (§3.2a) |
| `close_all_streams` | `stop()` per stream | **decision** (§3.2b) |
| `flush_api_call_window` | — | **decision** (§3.2c) |
| `invalidate_strikes_cache` | — | **decision** (§3.2d) |

### 3.2 The four that do not map

- **a. `build_option_symbol`.** TradeStation symbology vs OCC. ThetaData already has
  `build_occ_symbol` / `parse_occ_symbol`. Proposal: move symbol construction behind the provider
  so the engine never spells a vendor's symbol format.
- **b. `close_all_streams`.** Lifecycle. The interface has per-stream `stop()`; the engine wants
  one call. Proposal: the engine tracks its streams and stops each. No interface change.
- **c. `flush_api_call_window`.** TradeStation rate-limit bookkeeping. ThetaData is a local
  terminal with no API quota, so this is vendor-specific housekeeping that should not be in the
  generic path at all. Proposal: drop from the generic path; TradeStation's provider keeps it
  internal.
- **d. `invalidate_strikes_cache`.** Called on strike recalculation. Proposal: same as (c) —
  the provider owns its own caching and exposes nothing.

None of these is hard. They are decisions about where a boundary sits, and they should be made
once, deliberately, rather than discovered mid-rewrite.

---

## 4. Decisions you owe, with a recommendation each

### 4.1 Futures — ES and NQ · **blocks G3**

ThetaData sells no CME product. The provider declares the capability false and raises if asked.
When TradeStation goes off, ES and NQ have no source.

**You already have the answer in the codebase and had forgotten it, correctly.**
`src/jobs/futures_projection.py` computes the index→future basis two ways:

- `source="measured"` — read from actual ES/SPX print pairs. **Needs CME data.**
- `source="carry"` — the theoretical cost-of-carry ratio `e^((r−q)·T)`, from `RISK_FREE_RATE` and
  `resolve_dividend_yield()`. **Needs no CME data whatsoever.**

The carry path already exists and already runs as the fallback when no print pair is available
(`futures_projection.py:544-551`).

**Recommendation: force `source="carry"` and drop the measured path.** ES/NQ become a published
projection of SPX/NDX at theoretical fair value, computed entirely from data you are licensed for.
This removes the CME dependency *and* removes the derived-work exposure the licensing audit
flagged (F4), because no CME data touches the calculation.

**What you give up, and must say publicly:** a carry-derived ES is *fair value*, not the traded
price. When futures trade away from fair value — overnight, on risk events — your number will
differ from a real ES chart. It must be labelled as a projection, not quoted as the future.

### 4.2 The four interface boundaries in §3.2

**Recommendation: take all four proposals as written.** They all push vendor specifics behind the
provider, which is the point of having the seam.

### 4.3 Strike and expiration range

Not a launch item, but you asked, so the facts are here to stop it being re-litigated later.

- **Widening strikes is nearly free.** `THETADATA_STRIKE_RANGE` is unset, so every call already
  requests `strike="*"` — the entire expiration — and the code discards rows down to 40 strikes
  afterward. You already pay the network cost. Widening adds **zero** API calls; the cost is
  downstream IV solving, Greeks and database writes.
- **Expirations cost linearly.** One call per (underlying, expiration, endpoint). Today
  4 × 3 × 2 = 24 calls per cycle; six expirations would be 48.
- **The TradeStation constraint is gone.** `INGEST_EXPIRATIONS = 3` was set for a TradeStation
  streaming symbol cap. ThetaData is a local terminal with no API quota.

**Recommendation: change nothing before launch.** Cut over on today's geometry so that if
something moves, the feed is the only variable. Widen afterwards, deliberately, with the flip
weighting question in §4.4 settled first.

### 4.4 Gamma flip weighting at depth — open question, not a launch item

The depth sweep found the flip resolves at 3 expirations and **stops resolving** at 6, 9 and 12.
The mechanism is now understood and it is not the chain's fault.

`_dte_profile_weight` is `min(1, DTE / 5)`. **Near-dated contracts are weighted DOWN, not up** — a
0DTE contract contributes ≈ 0, a 5+ DTE contract contributes 1.0. That is deliberate: it was built
so an OPEX-day 0DTE wall, carrying a colossal re-greeked 1/√T gamma spike, cannot pin the
multi-day regime flip to a same-day strike.

So deepening the chain adds full-weight far-dated gamma that swamps the near-term profile. Your
instinct to weight near-term more is the right diagnosis, and **the machinery already exists**:
`GAMMA_PROFILE_DTE_WEIGHT_SHAPE` accepts `linear` (current), `sqrt` (more aggressive on
near-dated) and `exp`. Volume or OI weighting does not exist and would be new work.

**Recommendation: do not touch this before Friday.** It changes a published number on every
underlying, it is unrelated to the vendor migration, and mixing it into the cutover means a moved
flip cannot be attributed. Revisit the week after.

---

## 5. What changes for customers at cutover

Every one of these is a real, visible difference. They are acceptable or they are not — that is
your call, but they must not be discovered by a subscriber.

| Change | Impact | Severity |
|---|---|---|
| **SPY/QQQ price is Nasdaq Basic, single venue** | Exhibit A: *"Derived from Real Time Nasdaq Basic Data."* Price tracks consolidated within pennies. Charts will not tick-for-tick match a consolidated chart elsewhere. | Low — price is materially the same |
| **SPY/QQQ volume is Nasdaq Basic, single venue** | A *fraction* of consolidated volume. Anything scale-invariant (VWAP, z-scores) is unaffected. **Any displayed absolute share count will drop sharply versus today.** | **High if you display volume** |
| **Uptick/downtick volume is gone** | Four views lose the split and now return "no data" rather than a fabricated 50% / Neutral. Note TradeStation's split was itself a tick-test guess, not true buy/sell attribution. | Medium |
| **Option flow classification accuracy** | Unmeasured. See §6. | **Unknown — measure first** |
| **ES / NQ become a fair-value projection** | Per §4.1. Requires a label change on the site. | Medium |
| SPX / NDX index levels | No change. Both real-time under the Indices – Market Value line. | None |
| Option chain bid/ask | Randomised by up to a penny per side. Measured effect on every published GEX metric: ≤ 0.08%. | None material |

---

## 6. The one unmeasured thing · **do this first**

After cutover, the Lee-Ready flow classifier compares inputs from two different feeds:

- trade price (`last`) and volume ← `option_snapshot_ohlc`, real-time, **exact**
- bid / ask ← `option_snapshot_market_value`, **randomised by up to a penny each side**

`_classify_volume_chunk` decides buyer- vs seller-initiated by where `last` sits relative to
bid/ask, with a mid band of 0.70 × half-spread. On a 5-cent-wide option the band is 1.75¢ and the
quote has moved up to 1¢.

**Reasoning about the direction of the error:** trades that occur *at* the bid or *at* the ask
should classify correctly regardless, because a penny of quote noise does not move the trade to
the other side of the mid. The exposure is **midpoint fills** — price-improved trades between the
quotes — which are common in options and sit exactly where the band decides.

**The feed comparison never tested this.** It compared spot, call wall, put wall, max pain, gamma
flip and net GEX. No flow metric. This is the only remaining unknown in the evidence base, and it
feeds the order-flow and tape-bias signals.

**Built** (`compare_flow_classification`, runs with every paired sample). It classifies the SAME
trade twice — `last` and `volume` come from the same non-Market-Value endpoint on both paths, so
the quote is the only variable — and reports disagreement per contract AND volume-weighted, plus
which way the shifts go.

Reasoning it through against the unit cases first, the shape is narrower than feared: a print **at
the bid or the ask** survives a penny of quote movement, because it stays on its side of the band.
The exposure is **midpoint fills**, where the band decides. Still needs a live RTH session to know
the real rate — `INCUMBENT=thetadata CANDIDATE=thetadata_mv make feed-compare MINUTES=30`.

**Read the volume-weighted number, not the contract count.** A chain is mostly untraded contracts
and a thousand one-lot far-OTM disagreements matter less to a published flow figure than one on a
heavily traded ATM strike.

---

## 6.1 How G2 is actually run · **corrected**

The earlier wording assumed flipping `MARKET_DATA_PROVIDER` could be rehearsed. It cannot. The
ingestion engine has **no shadow-write mode** — only `feed_compare` writes the shadow tables — so
setting that variable writes ThetaData straight into live `option_chains` and `underlying_quotes`.
That is the cutover, not a rehearsal, and there is no way to watch a full session first.

The fix needs no code. `DB_NAME` is a plain environment variable read at connection time, so a
**second ingestion process pointed at a scratch database** runs the real engine, the real provider
and the real write path while touching nothing a customer reads.

```bash
# once, on RDS
createdb zerogex_shadow          # or: psql -c 'CREATE DATABASE zerogex_shadow;'
DB_NAME=zerogex_shadow make schema-apply

# the rehearsal — start before 09:30 ET, leave it for the session
MARKET_DATA_PROVIDER=thetadata_mv DB_NAME=zerogex_shadow \
  .venv/bin/python -m src.ingestion.main_engine \
  --underlyings 'SPY,QQQ,$SPXW.X,$NDXP.X'
```

Rollback is `kill`. Production never sees it.

**Read at the close, against the live tables for the same session:**

| Check | Passes when |
|---|---|
| Minute coverage | `underlying_quotes` row count per symbol within a few of live |
| Gaps | no minute bucket missing between 09:30 and 16:00 |
| Chain size | `option_chains` contracts per cycle comparable to live |
| Open interest | OI present on the same share of contracts as live |
| Volume | the new `volume` column populated for SPY/QQQ, NULL for the indices |
| Errors | no circuit-breaker trips, no sustained reconnects in the log |

**G2 passes on a clean session, not on "it ran".** A process that stayed up while dropping half
its minutes is the failure this gate exists to catch.

---

## 7. Timeline

Tight with no slack. The shadow session needs a market day, which fixes the order.

| When | What | Gate |
|---|---|---|
| **Tue (today)** | Decisions §4.1 and §4.2. Build the flow-classification diff (§6). | decisions closed |
| **Wed** | Wire the ingestion path to the provider seam (§3). Deploy to the server. Run the flow diff on Wednesday's session. | G1 |
| **Thu, pre-open** | Scratch database created and schema applied; the second ingestion process started before 09:30 ET (§6.1). | — |
| **Thu, full session** | Watch it write a clean day. Compare against the live tables at the close, on the table in §6.1. | **G2** |
| **Fri** | Cut over. TradeStation stays running and warm, unused, for a week. | G3, G4 |

**The hard gate is Thursday's open.** If the wiring is not deployed and writing by then, there is
no full-session evidence, and Friday would be a cutover onto a path that has never run a complete
day. That is the abort trigger, not a thing to push through.

---

## 8. Abort and rollback

- **Rollback is `MARKET_DATA_PROVIDER=tradestation` and a service restart.** Built and verified
  (`4fd69a6`, `a94c8e6`). That is the entire reason for doing the wiring properly rather than
  swapping clients by hand.
- **Rollback for the rehearsal is `kill`** — it writes to its own database and production never
  reads it.
- TradeStation stays running and credentialed for one week after cutover. It is not switched off
  until seven consecutive days of clean ThetaData ingestion (runbook step 16).
- **Abort Friday's launch if:** Thursday's session shows any gap in shadow ingestion; the flow
  diff (§6) shows classification materially changed and the cause is not understood; or §4.1 is
  still undecided.

---

## 9. Explicitly out of scope until after launch

Listed so they stop coming up.

- Widening strike or expiration range (§4.3)
- Gamma flip weighting (§4.4)
- Recalibrating the skew baseline
- Narrowing the `gex_gradient` wing window
- The `LAG()` differencing question in `get_flow_buying_pressure`
- Anything involving ThetaData's websocket (FPSS); polling is the shipped path
- The `MARKET_REFERENCE` scope question (§10)

---

## 10. Standing item: MARKET_REFERENCE

Partially resolved, and worth knowing the migration may improve it on its own.

The emergency audit **did** shut off per-contract quoted prices: `MARKET_RAW` covers bid/ask/last
/mid on `/api/option/quote`, `/api/option/contract` and `/api/tools/option-calculator`, is
withheld from every external customer, and the boundary is enforced by
`tests/test_market_data_scope_boundary.py`. That change cost eleven paying integrations.

It **did not** shut off `MARKET_REFERENCE` — the underlying's own quote and OHLC — which remains
bundled into `TIER_ANALYTICS` and `TIER_SIGNALS` and is still sold externally. `scopes.py` is
honest about this: it records that audit finding F5 disputes the theory, that counsel has confirmed
neither reading, and that the boundary drawn is "an engineering boundary, not a legal conclusion."

**The migration may resolve it by accident.** Under ThetaData you would be redistributing Market
Value — a derived product characterised as such in Exhibit A — rather than an exchange's
real-time tape. That is a materially stronger position than the one the audit questioned. It is
not a reason to delay the cutover, and it is not a reason to assume the question is closed.
