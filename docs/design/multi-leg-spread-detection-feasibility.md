# Multi-leg spread detection on the flow pipeline — feasibility

**Question:** a customer asked whether ZeroGEX flow can be fooled the way public
flow tools are: a *sold* vertical whose long leg is reported as aggressive call
buying. We told him, in writing, that we do not reconstruct multi-leg trades
today. Before committing to build it: is real leg matching possible on the data
we have, possible only with a feed change, or not possible at a fidelity worth
shipping?

**Answer: Outcome B.** Real leg matching is **not possible on the data we
have** (Outcome A is false) and **is possible only with a per-print OPRA trade
feed that carries trade-condition codes**. On today's data the only honest
things to ship are a disclosure and, at most, a narrowly bounded "possible
spread leg" caveat whose precision we cannot measure without the feed we lack.
Do not build a chain-wide co-movement detector on the current pipeline.

**Status:** assessment only. No production code was written. Every claim below
carries a `file:line` reference; verify rather than trust. Line numbers are as
of the commit this document was added in.

---

## 0. What this document re-verified

| Claim in the brief | Verdict | Evidence |
|---|---|---|
| Side classification is Lee-Ready in `_classify_volume_chunk`, quote selection in `_select_classify_quote` | **Correct** | `src/ingestion/main_engine.py:1224` (classifier), `:1170` (prior-tick selection with staleness fallback) |
| `FLOW_CLASSIFY_MID_BAND_PCT` (0.70), `FLOW_CLASSIFY_SKIP_OPEN_AUCTION`, prior-tick staleness guard exist and are wired | **Correct** | `src/config.py:875-905`; used at `main_engine.py:1201-1222` and `:1492-1507` |
| No individual trade prints; `_FlowAccumulator` stores vendor cumulative volume and flow is the delta between snapshots, attributed by the last price | **Correct, and worse than stated** — see §2 for the coalescing detail | `main_engine.py:93-125` (accumulator), `:1488-1507` (`vol_delta = max(curr_vol - acc.last_volume_cum, 0)`, whole delta classified at `snap["last"]`) |
| Flow rows are stored on a 300-second grid | **Partly.** The flow *API* surfaces are 5-minute (`flow_by_contract`, `flow_series_5min`). The finest **persisted** per-contract classified series is **1-minute** (`option_chains`), and `/api/flow/smart-money` reads 1-minute rows. | `src/config.py:858-866` (`FLOW_BAR_SECONDS = 300`), `:857` (`AGGREGATION_BUCKET_SECONDS = 60`), `setup/database/schema.sql:112-136`, `API_Guide.md:699-700` |
| The pipeline polls vendor snapshots | **No — it is stream-driven.** A daemon thread reads `marketdata/stream/quotes/{symbols}` and merges each message into per-contract state; the main loop wakes on every message and *drains* changed contracts. The 5 s in `MARKET_HOURS_POLL_INTERVAL` is a cap on idle waiting, not a poll period. | `src/ingestion/stream_manager.py:1-16`, `:719`, `:896-966`, `:2443-2451`; `src/config.py:606-611` |
| Grepping ingestion for leg/spread/combo grouping returns nothing | **Correct.** Nothing in `src/ingestion`, `src/api`, `src/analytics` or the schema groups legs. The only mention is a caveat comment in `src/signals/components/order_flow_imbalance.py:1-8` ("does not identify … multi-leg intent"). | repo-wide grep; `git log --all` has no prior attempt |

---

## 1. Do we receive or store ANY trade-level data? (Q1)

**Stored: no.** Every persisted option row is a 1-minute bucket holding
*session-cumulative* `volume`, `ask_volume`, `mid_volume`, `bid_volume` plus the
best-available quote and Greeks for that minute (`schema.sql:112-136`, column
contract at `:159-175`). Within a minute the same `(option_symbol, timestamp)`
row is re-upserted with `GREATEST` no more often than
`OPTION_BUCKET_WRITE_MIN_SECONDS = 5` (`main_engine.py:1148-1168`,
`config.py:872`), so intra-minute detail is overwritten, not accumulated.
`flow_contract_facts` is a `LAG()` over those minute rows
(`src/api/database.py:1389-1420`); `flow_by_contract` and `flow_series_5min`
re-aggregate to 5 minutes (`schema.sql:947-962`, `:1012-1046`). There is no
table of prints, no per-print size, no exchange id, no sequence number, no
condition code. `option_chains_latest` is a latest-quote cache, not a tape.

**Received: quote-state updates, not prints.** The vendor stream delivers
partial quote objects; `_merge_single_quote` overwrites `Last`, `Bid`, `Ask`,
`Mid`, `TimeStamp`, `High`, `Low`, `Open`, `Close`, `NetChange`, `BidSize`,
`AskSize`, and `Volume` (only when > 0), and drops everything else
(`stream_manager.py:896-966`; whitelist at `:907-921`). Two prints on the same
contract between two drains collapse into one state whose `Last` is the second
print's price and whose `Volume` covers both.

**Discarded vendor fields that matter here.** TradeStation's v3 `Quote` object
carries `TradeTime`, `LastSize`, `LastVenue`, `VWAP`, `PreviousVolume` and
`MarketFlags` (vendor schema as mirrored by an independently generated Go client
of the v3 spec — see Sources; `api.tradestation.com` is egress-blocked from this
environment so the spec was not re-read directly). None of these names appears
anywhere in this repository. If the *stream* payload carries `LastSize` and
`LastVenue` (unverified — the whitelist drops them before anything logs them),
then per message we would have the size and venue of the **last** print only.
That is not a tape: a busy contract loses every print but the last per drain,
and there is no condition code anywhere in the vendor schema.

**The closest thing to prints we ever hold** is the pre-merge message stream in
the reader thread (`stream_manager.py:805-833`). If the vendor emits one message
per print (unknown; it may conflate), then ΔVolume per message is a print size
and `TimeStamp` its time to the second (`tests/test_ingestion_sticky_state_carry.py:53`
shows second resolution). That is discarded at merge time today. §6 proposes a
30-minute probe to settle whether it exists; even if it does, it has no
complex-order flag and no leg linkage.

---

## 2. Cadence, and the finest granularity at which two contracts can be correlated (Q2)

There are three layers, and only the coarsest is persisted.

| Layer | Granularity | Where | What survives |
|---|---|---|---|
| Vendor message | per quote change (per print, if not conflated by the vendor) | reader thread, `stream_manager.py:805-833` | nothing — merged into state and gone |
| Drain / snapshot | one drain per main-loop wakeup: sub-second to ~1 s while messages flow, capped at `MARKET_HOURS_POLL_INTERVAL = 5` s when quiet (`stream_manager.py:2392-2396`, `:2443-2451`; `config.py:606-611`) | `drain()` returns only dirty contracts (`stream_manager.py:613-626`) → `_yield_option_snapshot` (`:2229-2352`) → `_store_option_batch` (`main_engine.py:1005`) | in memory only: per contract, ΔVolume since last drain, the last price, the last trade time (vendor `TimeStamp`, else receive time, `:2279-2301`), NBBO |
| Persisted | 1-minute `option_chains` rows (`main_engine.py:1052`, `config.py:857`); 5-minute flow rollups | `_prepare_option_agg` writes the accumulator's cumulatives (`main_engine.py:1565-1590`) | session-cumulative volume and classified volume per minute |

**The classification unit is the drain, not the print.** `_ingest_snapshot_into_accumulator`
classifies the *entire* ΔVolume of a drain at the drain's single `last` price
(`main_engine.py:1490-1507`). If 300 contracts lifted the offer and then 200 hit the
bid inside one drain, all 500 are scored at the bid price → 500 `bid_volume`.
This is by design (the accumulator docstring explains the idempotency reason,
`:1441-1447`) and is a fidelity ceiling independent of spreads.

**So the finest time-correlation available is:**

- **Persisted:** the 1-minute bucket. Two contracts can be said to have traded
  "in the same minute", with per-minute ask/mid/bid deltas recoverable by `LAG`.
  Busy contracts carry *both* ask and bid deltas in nearly every minute, so an
  "opposite side" test on this grid has almost no discriminating power (§5).
- **In memory:** one drain, i.e. roughly a second during RTH, with a last-trade
  `TimeStamp` at second resolution. This is not persisted and not exposed.

Neither layer gives per-print size, venue, sequence or condition, which are the
four things leg matching needs.

---

## 3. Does the vendor offer a trade-level or complex-order feed we are not consuming? (Q3)

**No, not in the API we use.** The v3 market-data surface this codebase touches is:
`marketdata/quotes` (`tradestation_client.py:930-946`, `:1177-1184`),
`marketdata/stream/quotes` (`:1186-1212`, `stream_manager.py:719`),
`marketdata/barcharts` and `marketdata/stream/barcharts` (`:948-976`, `:1056`,
minute/daily units — minute is the finest unit our client requests, and to my knowledge the v3 spec offers no tick unit; see the verification limit below),
`marketdata/options/expirations` and `options/strikes` (`:1071-1150`), and
`marketdata/marketdepth/quotes` (`:1224-1230`, equities-only Level 2, unused in
production). None is a time-and-sales or trade-print endpoint, and the v3 quote
schema has no trade-condition field at all (§1). TradeStation's "Stream Option
Chain" endpoint (not used here) accepts a `spreadType` and returns *quotes* for
synthetic spreads with `Legs` — it quotes structures, it does not report
complex-order executions. TradeStation's desktop Time & Sales window and the
EasyLanguage `TimeAndSalesProvider` are platform features, not API endpoints,
and are covered by the same personal-use terms the licensing audit flags
(`docs/compliance/market-data-licensing-audit-2026-09-02.md`, F1).

**Verification limit:** `api.tradestation.com` and `tradestation.github.io` are
blocked from this environment. The statement "no v3 trade feed" rests on the
endpoint inventory in our client, the vendor schema mirrored by third-party
clients, and my knowledge of the v3 spec. A five-minute read of the live spec by
someone with access should confirm it before this is repeated to the customer.

---

## 4. Why the outcome is B, and what "real" reconstruction needs

**What a multi-leg execution looks like on the tape.** When a complex order
executes, each leg is reported to OPRA as a separate trade message on the same
exchange with (near-)identical timestamps, and each carries a trade-condition
code identifying it as part of a multi-leg execution. The OPRA Binary
specification defines, among others (Sources):

- `MLET` — Multi Leg auto-electronic trade (executed in a complex order book)
- `MLAT` — Multi Leg auction; `MLCT` — Multi Leg cross; `MLFT` — Multi Leg floor trade
- `MESL` / `MASL` / `MFSL` — multi-leg trades executed *against single-leg* orders
- `TLET` / `TLAT` / `TLCT` / `TLFT` / `TESL` / `TASL` / `TFSL` — stock-option combinations
- `CBMO`, `MCTP` — proprietary-product multi-leg variants

**What OPRA does *not* give:** a complex-order id linking the legs. Leg pairing
is always reconstruction: group flagged prints by exchange and timestamp (same
nanosecond, or within a few milliseconds), then find leg sets whose sizes are
consistent with a ratio (1:1 vertical, 1:2 ratio, 1:1:1:1 condor …). The
condition code is exchange-reported truth ("this print is a spread leg"); the
pairing is a high-precision inference; the *intent* (debit vs credit) is then a
spread-level Lee-Ready test of the net price against the composite NBBO of the
legs. Only Cboe's own complex-order feeds (exchange-direct, out of scope for our
size) carry explicit leg definitions.

**Therefore the required feed must deliver, per print:** contract, timestamp
with sub-second resolution, size, price, exchange id, sequence, and the OPRA
trade condition. Anything less — including everything TradeStation sends us —
cannot distinguish a spread leg from an outright trade, and the customer's
example is exactly the case where the two look identical print-by-print.

**Fidelity achievable *with* such a feed (honest ceiling):**

- Flagging a print as a spread leg: exchange-reported, effectively exact.
- Pairing legs: high but not perfect — ambiguous when two complex orders execute
  on one exchange in the same millisecond, when a leg fills in several partial
  prints, or for stock-tied combos whose stock leg is not on OPRA.
- Intent (sold vs bought vertical): a Lee-Ready test at the spread level, with
  the same mid-band caveats we already accept on single legs.
- Never detectable by anyone: a trader legging into a spread with separate
  single-leg orders. Those print as outright trades and *are* outright trades.

**Side benefit that is worth naming:** a per-print feed also removes the
drain-coalescing ceiling in §2 — each print gets classified at its own price
against the NBBO at its own time — which is an accuracy gain for every flow
surface, spreads or not.

---

## 5. Why not the co-movement heuristic (Outcome C's proposal) as a shipped label

The bounded heuristic in the brief (same underlying and expiry, opposite-side
classification, similar size, same bucket) was evaluated against what the
pipeline actually stores.

**On the persisted 1-minute grid it is uninformative on a busy chain.** The
tracked universe is up to `INGEST_STRIKE_COUNT_MAX = 40` strikes per expiration
(`config.py:1802`), both sides, across `INGEST_EXPIRATIONS = 3` (`:1793`). On SPY
or SPX 0DTE every near-the-money strike trades many times per minute, so nearly
every contract carries *both* an ask delta and a bid delta in every minute
(§2). The "opposite side" filter then admits almost every pair of strikes in the
expiry: on the order of C(40, 2) ≈ 780 pairs per minute per type per expiry
before any size test. A ±10 % size-similarity filter on heavy-tailed per-minute
volumes still passes a meaningful fraction of those by chance. Expect tens to
low hundreds of "possible spread" flags per minute per expiry on a busy chain,
against an unknown number of true complex executions. The precision of a flag
on that grid is roughly the base rate of spread activity, i.e. the flag adds
nothing a reader could not already assume.

**On the in-memory drain (same second, equal ΔVolume) it is better but still
uncalibrated.** Exact size equality is a strong filter for large sizes, but
small round lots (1, 2, 5, 10) coincide constantly across strikes in the same
second on an active expiry. Busy contracts also accumulate unrelated prints in
the same drain, so a spread leg on an ATM strike rarely shows the spread's size
in its ΔVolume — the size test fails exactly where the customer's concern is
largest. The heuristic only has signal on quiet contracts and large blocks.

**The decisive point: we cannot measure its false-positive rate.** Precision
requires ground truth (which prints were spread legs), and that ground truth is
the OPRA condition code — the field we do not have. Any number I put on
"false-positive rate" from our own data would be invented. Appendix A gives a
read-only query that measures the *candidate-pair rate* (an upper bound on
flags per minute); it cannot measure precision.

**The one bounded form I would consider, and only after calibration (§6 step 1):**
a caveat — never a reclassification — on `/api/flow/smart-money` block events:
"a same-size (±5 %) opposite-side print on another strike of the same expiry
occurred in the same minute; this may be a spread leg." Restricted to events at
or above the existing 100-lot tier (`database.py:5861`), same expiry only,
1:1 only. Even this must be labelled *possible*, and it must not alter
`trade_side`, `net_volume`, `net_premium` or any signal input. Shipping it
before measuring its precision against real condition codes would be doing the
thing we told the customer we do not do.

---

## 6. Recommendation

### Ship now (no feed, no guessing): disclosure, in product and in the API

1. **A one-line flow-classification note** in the style of
   `frontend/components/ModeledPositioningNote.tsx` (client, five locales,
   links to `/methodology`), placed under the surfaces where a single leg is
   read as intent, in this order of importance:
   `/smart-money` (the `Side` column, `app/smart-money/page.tsx:69-101`),
   `/option-contracts` (`Ask Vol / Mid Vol / Bid Vol`, `:277-279`, `:537-539`),
   `/flow-analysis` (the "Net Position (Buys vs. Sells)" tooltip at `:684-686`
   is the strongest classification claim in the product), `/tape-flow-bias`
   (use the existing `SignalHowItsBuilt caveat` slot, `:120`), and the
   `market-tide` flow-direction card (`:395-400`).
   Proposed copy: *"Trade side is inferred one contract at a time from price
   versus quote (Lee-Ready). Legs of multi-leg spreads are not paired, so a
   spread leg can read as outright buying or selling."*
2. **Methodology and help text:** add the same paragraph to
   `content/methodology.md:19`, `content/help/platform/flow-analysis.md:23-25`,
   `content/help/platform/smart-money.md:12`, and extend the existing caveat
   block in `content/articles/net-volume-vs-directional-flow.md:47-52`
   (which already gestures at "dark/complex executions") to name spreads
   explicitly. All five locale siblings.
3. **API:** document the limitation under "Options Flow" in `API_Guide.md:670`,
   and add a machine-readable methodology block to the flow responses/v2
   envelope, e.g. `{"side_classification": "lee_ready_prior_tick",
   "multi_leg_reconstruction": false, "bucket_seconds": 60}`, so API customers
   cannot miss it.
4. **A separate decision, surfaced here because it compounds the spread
   problem:** `flow_contract_facts` extrapolates *mid* (unclassified) volume into
   `buy_volume` / `sell_volume` in proportion to that contract's ask:bid split
   for the minute (`src/api/database.py:1438-1451`). Spread legs are commonly
   allocated *inside* the NBBO, so a leg that printed at mid is currently
   assigned a side from unrelated prints on the same contract. That is a
   manufactured value, against the "hide, don't zero" convention. It feeds
   `net_volume`, `net_premium`, Market Tide and Tape Flow Bias, so changing it
   is its own scoped change with parity tests — not to be bundled here, but it
   should be on the list.

### Step 0 — a 30-minute probe of what the stream actually carries (cheap, this week)

A read-only tool in the pattern of `src/tools/probe_option_quote_batches.py`:
open one `marketdata/stream/quotes/` connection on ~10 active SPY contracts
during RTH for N minutes, log the union of payload keys, and for every message
with `Volume > 0` record ΔVolume versus `LastSize` (if present), the `TimeStamp`
resolution, and whether `LastVenue` is present. Two answers come out:
(a) whether `LastSize`/`LastVenue` exist on the stream at all, and (b) whether
ΔVolume equals `LastSize` message-by-message (one message per print) or
routinely exceeds it (vendor conflation). If (a) and (b) are both yes, we
receive a condition-less quasi-tape we currently discard; capturing it pre-merge
would improve classification and make a same-second, same-venue, equal-size
block caveat plausible. If either is no, that door is closed and this document
is the final word on Outcome A.

### Step 1 — quantify the exposure before spending on a feed (bounded, one-off)

Buy ~5 sessions of **historical** OPRA option trades *with condition codes* for
SPY, SPX/SPXW and QQQ (ThetaData or Polygon historical; a one-off historical
pull does not carry the real-time non-display entitlement — confirm with the
vendor), and run a research module in the style of `research/mm_attributed_gex/`
(read-only, outputs to files) that answers:

- what share of our `ask_volume` / `bid_volume` on each surface is multi-leg-flagged
  (i.e. how large the customer's concern actually is, by symbol and by surface);
- how often the exact scenario — a sold vertical's long leg landing in `ask_volume`
  and surfacing as a `BUY` block — occurs per session;
- the measured precision and recall of the §5 block caveat, which decides whether
  it is shippable at all.

This converts "worth building" from an opinion into three numbers for a few
hundred dollars and about a week.

### Step 2 — fold the trade feed into the sourcing decision already on the table

The licensing audit already recommends moving to a licensed redistributor
(`docs/compliance/market-data-licensing-audit-2026-09-02.md`, "The sourcing
decision", option B + C), and the vendor brief already carries the two-layer
cost model (`docs/design/historical-options-data-vendors.md` §2a). Spread
reconstruction should be a **selection criterion for that vendor**, not a
second contract:

| Candidate | Live per-print options trades | OPRA condition codes on the feed | Notes / verification status |
|---|---|---|---|
| **ThetaData** (Options Pro) | Yes — "Full Trade Stream: every US option trade reported on the OPRA feed" | Documented trade-condition mapping | Requires running the Theta Terminal process alongside ingestion. Pro tier pricing quote-gated; brief cites ~$125/mo commercial startup tier. Docs domain blocked here; feature existence confirmed via search index only. |
| **Polygon / Massive** | Yes — options trades REST + websocket with a `conditions` array | Options condition-code reference includes the multi-leg types | Commercial/redistribution tiers quote-gated. Docs blocked here; confirm code ids. |
| **dxFeed** | Yes — `TimeAndSale` events | Exposes a spread-leg flag directly on the event | Enterprise pricing. Not re-verified from this environment. |
| **Databento (OPRA.PILLAR)** | Yes | **Not today** — Databento's public roadmap entry "Include OPRA trade conditions" states the conditions are currently lost in normalization | Disqualifying for this purpose until shipped; re-check status. |
| **Cboe DataShop Time & Sales** | Historical / delayed (15-min intraday per the brief) | Yes | Fits Step 1 calibration, not a live feed. |

**What it costs us to integrate (estimate, engineering only, after a feed is
live):**

| Piece | Scope | Rough effort |
|---|---|---|
| Trade-stream ingester | new worker under `src/ingestion/`, batched inserts, reconnect/backoff mirroring `stream_manager.py`, retention job | 1 week |
| Schema | `option_trades` hypertable (short retention — millions of rows/day across three underlyings; size from one live day) and `option_spread_trades` (matched structures) | 2 days |
| Per-print classification | classify each print against the NBBO at print time; keep the `option_chains` cumulative contract intact (parity tests exist: `tests/test_flow_series_parity.py`, `test_ingestion_volume_classification.py`) | 1 week |
| Leg matcher | pure module `src/analytics/spread_legs.py` in the `pin_strike.py` mould: prints in → `SpreadCandidate` (legs, ratio, structure, net price, net side, confidence) out; strict `None` handling | 1–2 weeks |
| Surfaces | `multi_leg_volume` per contract, spread records on smart-money, "spread leg" labels, API fields, five locales | 1 week |
| Validation | precision/recall against Step 1's labelled sessions before anything is customer-visible | 1 week |

Total ≈ 5–7 engineering weeks after the feed is live, plus vendor lead time.
The dominant cost is not engineering: it is Layer 2 of the vendor brief —
OPRA non-display ≈ $2,000/mo unless the vendor bundles it, plus the separate
Cboe licensing the brief flags for SPX/SPXW — and that cost is being incurred by
the sourcing decision anyway. Adding "trade conditions on the live feed" to the
vendor questionnaire is nearly free; building spread detection on the current
personal-use TradeStation feed would deepen the exposure the audit already
rates critical (F1, F2).

### Do not do

- Do not build a chain-wide "spread detected" label on 1-minute or 5-minute
  co-movement. Its precision is unmeasurable on our data and structurally poor
  on the chains our customers watch (§5).
- Do not reclassify or net legs on a heuristic match. A caveat is the ceiling.
- Do not promise the customer leg matching on a timeline that precedes the feed
  decision.

### What I need from you

1. Approval to ship the disclosure (Step "now"), including the API methodology
   block wording.
2. Go/no-go on Step 0 (the probe tool) and Step 1 (the historical pull and
   research module) — both are non-production and read-only.
3. Whether "OPRA trade conditions on the live trade feed" becomes a hard
   requirement in the vendor selection the licensing audit opened.

---

## 7. The customer's scenario, traced through the pipeline

A sold call vertical (sell 500 lower-strike calls, buy 500 higher-strike calls)
executes as one complex order. OPRA reports two `MLET` prints on one exchange at
the same instant, each leg priced by the exchange's allocation inside that leg's
NBBO. TradeStation's stream then sends us, per leg, a quote update with
`Volume += 500`, `Last = leg price`, `TimeStamp = trade time` — no flag, no
size, no venue.

- **Per-contract surfaces get it wrong.** If the long leg was allocated near its
  ask, `_classify_volume_chunk` credits 500 `ask_volume`
  (`main_engine.py:1497-1507`); `flow_contract_facts` turns that into
  `buy_volume`; `/api/flow/smart-money` emits a `BUY` block event
  (`database.py:5855-5860`); `/option-contracts` shows 500 `Ask Vol`;
  `/api/flow/by-contract` shows positive `net_volume` on that strike. This is
  exactly the customer's example. If the leg was allocated at mid, the
  extrapolation in `database.py:1438-1451` assigns it a side from whatever else
  traded on that contract in the minute.
- **Aggregate surfaces partly self-correct, by netting.** Across both legs,
  `net_volume` sums to zero and `net_premium` is negative (the short leg carries
  more premium), so `/flow-analysis` and Market Tide read the spread as net
  selling — directionally right, but only because both legs sit inside our
  tracked strike window and both classified as expected. Calendars whose far
  expiry falls outside `INGEST_EXPIRATIONS`, stock-tied combos, and legs that
  print at mid break the netting.

The disclosure therefore matters most on the per-contract and event surfaces,
which is the placement order in §6.

---

## Appendix A — measuring the candidate-pair rate (read-only)

Upper bound on flags per minute for the §5 heuristic on the persisted grid.
Says nothing about precision — there is no ground truth in this database.

```sql
WITH deltas AS (
  SELECT underlying, expiration, option_type, strike, option_symbol, timestamp,
         GREATEST(ask_volume - LAG(ask_volume) OVER w, 0) AS ask_d,
         GREATEST(bid_volume - LAG(bid_volume) OVER w, 0) AS bid_d
  FROM option_chains
  WHERE underlying = 'SPY'
    AND timestamp >= '2026-09-03 13:30+00' AND timestamp < '2026-09-03 20:00+00'
  WINDOW w AS (PARTITION BY option_symbol ORDER BY timestamp)
),
legs AS (SELECT * FROM deltas WHERE ask_d > 0 OR bid_d > 0)
SELECT a.timestamp, a.expiration, a.option_type,
       COUNT(DISTINCT a.option_symbol) AS contracts_traded,
       COUNT(*) FILTER (
         WHERE a.ask_d > 0 AND b.bid_d > 0
           AND ABS(a.ask_d - b.bid_d) <= 0.10 * GREATEST(a.ask_d, b.bid_d)
       ) AS candidate_pairs
FROM legs a
JOIN legs b
  ON  a.timestamp = b.timestamp AND a.underlying = b.underlying
  AND a.expiration = b.expiration AND a.option_type = b.option_type
  AND a.strike <> b.strike
GROUP BY 1, 2, 3
ORDER BY candidate_pairs DESC
LIMIT 50;
```

The 09:30 bucket is all-mid by design (`FLOW_CLASSIFY_SKIP_OPEN_AUCTION`) and
will show zero pairs; exclude it when summarising.

## Appendix B — Sources and verification status

Verified in-repo (read at the referenced lines): everything cited as `file:line`
above, plus `docs/architecture/volume-tracking-review.md` §1,
`docs/architecture/ingestion_engine_diagram.md`,
`docs/CODE_REVIEW_2026-05-15.md` §D8, `docs/design/historical-options-data-vendors.md`
§2a–§4, `docs/compliance/market-data-licensing-audit-2026-09-02.md` F1–F3 and the
sourcing table.

External, reached from this environment:
- OPRA multi-leg trade condition codes (`MLET`, `MLAT`, `MLCT`, `MLFT` and the
  single-leg / stock-option variants): OPRA Binary Participant Interface
  specification, "New Trade Type Codes" (search-indexed excerpt), and the Cboe
  note "Harmonization of Cboe and OPRA Trade Condition Field Values".
- TradeStation v3 `Quote` object fields (`TradeTime`, `LastSize`, `LastVenue`,
  `VWAP`, `PreviousVolume`, `MarketFlags`): `github.com/penny-vault/tradestation`
  Go client, `Quote` struct.
- ThetaData "Full Trade Stream" for US options (every OPRA trade; Options Pro
  required; Theta Terminal required): docs.thetadata.us / http-docs.thetadata.us
  page titles and indexed text.
- Databento roadmap entry "Include OPRA trade conditions" (conditions currently
  lost in normalization): roadmap.databento.com, indexed text.

External, **not** reachable from this environment (egress-blocked), stated from
prior knowledge and to be confirmed by someone with access before quoting to a
customer: the live TradeStation v3 specification (absence of any time-and-sales
endpoint; `unit` values for bars); Polygon/Massive options condition-code ids;
ThetaData's trade-condition mapping page; dxFeed `TimeAndSale` spread-leg flag;
current vendor pricing.
