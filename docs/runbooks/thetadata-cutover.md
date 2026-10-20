# Cutting production over to ThetaData

Decided 2026-10-13. Rehearse Wednesday 14 October, rehearse Friday 16 October
(monthly OPEX), **cut over before the open on Monday 19 October**. Paths assume
the standard deploy root `/home/ubuntu/zerogex-oa`.

The evidence behind this sits in
[the feed-comparison findings](../compliance/market-data-feed-comparison-findings-2026-09.md).
This file is only the procedure.

## Two constraints that shape the whole thing

**The switch is all-or-nothing across symbols.** `MARKET_DATA_PROVIDER` is
deployment-wide for option chains. The only per-feed override that exists is
`VOLATILITY_INDEX_PROVIDER`, for VIX and VXN. There is no per-symbol provider,
so SPY, QQQ, SPX and NDX move together or not at all. That is why NDX having
never ingested a row from ThetaData was a blocker rather than a footnote.

**The cutover must happen BEFORE the open, never during a session.**
`_FlowAccumulator` hydrates from the current cash session's persisted rows, so
switching `FLOW_CLASSIFIER` mid-session resumes cumulative totals built by the
quote test and then adds tick-test deltas to them. The day's published flow
would be a blend of two methods with no way to separate them afterwards. The
same applies to a rollback: **roll back before an open, or finish the day.**

## TradeStation does NOT go away on Monday

The ES / NQ futures ingester (`src/ingestion/futures_underlying_ingester.py`)
builds a `TradeStationClient` directly. It has **no provider seam at all** — no
`configured_provider_name`, no `get_provider`, nothing to point elsewhere. It
may also run under a second TradeStation username carrying the real-time CME
entitlement (`futures_tradestation_credentials()`).

So after cutover:

| | feed after Monday |
|---|---|
| option chains, SPY / QQQ / SPX / NDX | ThetaData |
| underlying bars for those four | ThetaData |
| VIX / VXN | ThetaData (verified on a full session, 2026-10-13) |
| **ES / NQ futures quotes** | **TradeStation, still** |

**The TradeStation subscription cannot be cancelled on Monday.** Whether it can
be downgraded to whatever tier serves @ES and @NQ is a billing question worth
asking before assuming the double-paying ends.

This does not undermine the licensing objective. The exposure this migration
exists to retire is OPRA / CTA / UTP / CGIF / GIDS — options and index data.
CME futures sit in a different licensing regime, and keeping them on
TradeStation does not reinstate the options exposure.

Porting futures off TradeStation is separate work, and there are exactly three
end states. ADDED 2026-10-16, because "cannot be cancelled" was being read as
"never".

**A — TradeStation, futures-only tier.** Zero code, zero risk. Ask what the
cheapest plan serving @ES and @NQ costs. Do this first regardless, because it
is the floor the other two have to beat.

**B — Databento `GLBX.MDP3` for ES/NQ.** Real-time CME, keeps every current
behaviour including the overnight display swap. Costs two pieces of work: wire
`futures_underlying_ingester` to the provider seam (it builds a
`TradeStationClient` directly today, and `ProviderBase.stream_futures_bars`
exists with **zero callers** — the slot was declared and never adopted), then
write the Databento provider. Note Databento cannot replace ThetaData, only sit
beside it: its catalogue carries no index feed at all (`MAIN.CGIF` 404s), so it
cannot serve SPX, VIX, VXN or NDX values at any price.

**C — delayed CME, free.** `providers/thetadata.py` records that delayed CME
"carries no exchange fee so long as nothing displays it". That qualifier decides
it, because `futures_quotes` has two consumers and they differ exactly on
display:

| consumer | displays a futures price? | delayed data OK? |
|---|---|---|
| basis engine (`src.jobs.futures_projection`) | no — it measures a carry ratio | yes |
| overnight display swap | **yes**, under the index label | **no** |

So C reaches **zero TradeStation spend**, at the price of giving up the
overnight display swap. That is a product decision, not a technical one.

None of the three is a cutover blocker and none is a reason to delay Monday.

## What actually changes

Two lines in `.env`, and nothing else:

```
MARKET_DATA_PROVIDER=thetadata_mv
FLOW_CLASSIFIER=tick
```

No schema change, no migration, no code deploy. `VOLATILITY_INDEX_PROVIDER`
stays unset, so VIX and VXN follow the deployment to ThetaData. That is not a
prediction: the 2026-10-13 rehearsal ran both on `thetadata_mv` for a full
session — `VIX feed: thetadata_mv` and `VXN feed: thetadata_mv` in its log — and
the bars agreed with production's TradeStation feed to within 1-5 cents on the
day's low and high closes, with zero off-grid bars and 77 / 74 RTH bars against
an expected ~78. Availability is settled. See the open question below before
assuming the same of the licensing.

## Go / no-go, checked the Friday before

All six must hold — gate 0 was added on 2026-10-14, after the list was first
written. Any one failing moves the date; none of them is a judgement call.

**CHECKED 2026-10-16 after the OPEX rehearsal. All six met. The cutover is a
go for Monday 2026-10-19** — with one finding that does not block it and must
not be forgotten either: on monthly expiration NDX flips 45.0% of its flow,
far outside its ordinary 22–29% band (F19). Monday is not an expiration-cluster
day, so Monday publishes NDX in its ordinary range. What to do about the dozen
monthly-OPEX sessions a year is a PRODUCT decision with five weeks on it — the
next monthly expiration is 2026-11-20 — not a reason to keep paying two
vendors.

0. **The NDX write overflow cannot silently void a symbol's session.** ADDED
   2026-10-14, REFRAMED 2026-10-15. As written this asked for the overflow to be
   "identified and fixed", and that turned out to be the wrong test: the 10-15
   rehearsal did not reproduce it, so no amount of waiting reliably names the
   value. What the 10-14 failure actually was is a **poison pill** — one
   out-of-range value fails the whole batch statement, rolls back the
   `option_chains_latest` cache with it, and is then retained and re-sent, so
   the symbol stops persisting until restart (F16, amended). That is latent in
   production on TradeStation too; the vendor change only raises the odds.
   **What clears this gate is the quarantine fix being deployed** — the write
   drops the rows Postgres cannot store, names them, and writes the rest.
   Confirm `_quarantine_numeric_overflow` is in `src/ingestion/main_engine.py`
   on the box. The triggering value is still unnamed and the diagnostic stays
   in place to catch it; that is a data-quality question now, not a blocker.
1. ~~**Wednesday's rehearsal wrote NDX rows from ThetaData.**~~ **MET
   2026-10-14** — 547,453 rows, 2,444 contracts, 8 expirations, through the full
   path. The pipeline works; see gate 0 for what it then did.
2. ~~**Friday's OPEX rehearsal completed** and its coverage is intact.~~ **MET
   2026-10-16** — 390 buckets on all four symbols, NDX persisting to 15:59 on
   both arms, no write failure and no quarantine invoked.
3. ~~**No regression on the three settled symbols.**~~ **MET 2026-10-16**, and
   this is the result that localises F19. On OPEX, SPY read 19.2% against an
   ordinary 13.7–17.0%, QQQ 16.8% inside 13.1–19.3%, SPX 24.1% against
   20.7–23.8% — all inside the ±8-point single-day band (F15). Monthly
   expiration did not degrade the tick test generally. It degraded it on NDX.
4. **NDX's own number is a decision you have made, not one you are discovering
   on Monday.** Four readings now, and they split into two regimes (F19):
   **ordinary sessions 22.2–29.1%**, mean 26.4% — 29.1% on 2026-10-09, 27.8% on
   10-14, 22.2% on 10-15 — and **monthly OPEX 45.0%**, measured once, on 10-16.
   The switch is all-or-nothing, so cutting over publishes NDX at whichever
   regime the day is in. The one lever anyone had in mind — the staleness guard
   — is refuted as NDX's fix (F17), so there is nothing left to try, on either
   regime. **This is a decision, not a measurement**, and it is now two
   decisions: publishing NDX at ~26% on ordinary days, which was always the
   question, and publishing it at ~45% on roughly twelve sessions a year, which
   is new as of 10-16. The first is settled by proceeding. The second has until
   2026-11-20 and is a product question — suppress NDX flow on monthly
   expiration, label it, or publish it — not a cutover question.
5. ~~**Production healthy through the rehearsals**~~ **MET** — 12 of 12 streams
   fresh with zero outside their window on 2026-10-14, 10-15 and 10-16, and no
   concurrent-stream cap, budget or session errors in any rehearsal log.

## Monday 19 October, before 09:30 ET

Do this with the market closed. 08:30–09:00 ET leaves room to roll back.

```bash
cd ~/zerogex-oa

# 1. nothing may be holding the single MDDS session
make shadow-status SHADOW_DB=zerogex_shadow
make shadow-status SHADOW_DB=zerogex_shadow_ts     # both must read 0

# 2. keep a copy of what you are changing from
cp .env ~/.env.pre-thetadata-$(date +%F)
grep -E "^(MARKET_DATA_PROVIDER|FLOW_CLASSIFIER|VOLATILITY_INDEX_PROVIDER)=" .env

# 3. apply the schema change FIRST
#    make schema-apply
#    There IS one as of 2026-10-15: F18 redefines the underlying_daily_volume
#    view. Nothing reads that view, so this is consistency rather than a
#    customer fix and a miss here breaks nothing — but if the gate 0 fix
#    widened a column, this step is the whole ballgame. Schema changes are
#    applied by hand and must land BEFORE the services restart; the other way
#    round, writes fail against the old column, which is the failure that fix
#    exists to stop.

# 4. make the change (edit by hand, two lines)
#    MARKET_DATA_PROVIDER=thetadata_mv
#    FLOW_CLASSIFIER=tick

# 5. read it back BEFORE restarting anything
grep -E "^(MARKET_DATA_PROVIDER|FLOW_CLASSIFIER|VOLATILITY_INDEX_PROVIDER)=" .env

# 6. restart, one service at a time, health-checked between each
make services-restart
```

`services-restart` stops at the first service that fails to come back and tells
you which, leaving the rest untouched.

### Verify, in this order

```bash
make ingestion-freshness-healthcheck          # want exit 0
make services-status
```

Then after the open, at 09:35 and 09:45:

```bash
make ingestion-freshness-healthcheck
sudo journalctl -u zerogex-oa-ingestion --since "10 min ago" | grep -ciE "error|INVALID_ARGUMENT"
```

What you are looking for on day one:

- all 12 streams fresh
- **four** symbols writing, NDX included
- `isProfessional: false, isRetail: false` in the ingestion log's auth block
- no `INVALID_ARGUMENT` and no `Invalid session ID`

A chain or underlying that is simply absent is worse than one that looks odd —
check that all four symbols appear before reading any number.

## Rolling back

Restore the two lines and restart. The copy is at
`~/.env.pre-thetadata-<date>`.

```bash
cp ~/.env.pre-thetadata-<date> .env
grep -E "^(MARKET_DATA_PROVIDER|FLOW_CLASSIFIER)=" .env
make services-restart
```

**Before an open, or at the end of the day — not mid-session**, for the
accumulator reason above. If something is badly wrong mid-session the choice is
to accept a blended day's flow or to stop ingestion until the next open; there
is no clean third option, and that is worth knowing before you need it rather
than at 11:00 on a Monday.

The rows already written stay written. Nothing in the cutover is destructive.

## After the cutover — running TradeStation as the watchdog

ADDED 2026-10-16. The same rehearsal machinery, pointed the other way: once
production is on ThetaData, run TradeStation into the rehearsal database and
compare each evening. Independent daily confirmation, nothing touching the live
site, and `shadow-stop` selects on `DB_NAME` so either arm stops alone.

**SPY only.** TradeStation caps concurrent streams at nominally 10 per account.
After Monday production's TradeStation usage drops to the futures streams
alone, which frees a lot of room — but a shadow arm also consumes TradeStation
quota for futures whatever its candidate feed is (the `shadow-run` banner says
so), so the budget is production's futures plus the arm's futures plus the
arm's option streams. SPY is comfortable, two symbols is tight, and the cap is
**not header-observable**: its only symptom is a stream that connects 200 OK
and dies silently inside 30 seconds, which is the 2026-06 incident. Measure
before widening. Do not reason about it from arithmetic alone.

First time only:

```bash
make shadow-pgpass SHADOW_DB=zerogex_shadow_ts
make shadow-create  SHADOW_DB=zerogex_shadow_ts
```

Each morning, pre-open, after confirming nothing else holds a stream:

```bash
make shadow-status SHADOW_DB=zerogex_shadow_ts          # must be 0
WID=$(tmux new-window -t shadow -P -F '#{window_id}')
tmux send-keys -t "$WID" "cd ~/zerogex-oa && \
  SHADOW_LOG=~/shadow-ts-$(date +%F).log make shadow-run \
  SHADOW_DB=zerogex_shadow_ts SHADOW_PROVIDER=tradestation \
  FLOW_CLASSIFIER=tick SHADOW_UNDERLYINGS=SPY" C-m
```

`FLOW_CLASSIFIER=tick` on purpose: production now runs tick, so holding the
method fixed makes the **feed** the only variable. That is F15's design, and
F15 measured the feed's own cost at **−0.4pp** on SPY against day-to-day
variance of about ±8 points.

After the close:

```bash
make shadow-stop SHADOW_DB=zerogex_shadow_ts
make shadow-compare      SHADOW_DB=zerogex_shadow_ts 2>&1
make shadow-compare-flow SHADOW_DB=zerogex_shadow_ts DAY=$(date +%F)
```

**What would be a real signal:** SPY's share-of-flipped-flow diverging by more
than a couple of points from production, or a coverage gap on the ThetaData
side that the TradeStation arm does not share. Anything inside ±8 on a single
day is the band, not a finding (F15).

**When to stop.** Two clean weeks is enough to retire the daily run; after
that, re-run it when something looks wrong rather than on a schedule. Keep the
rehearsal database — recreating it costs more than storing it.

## What this does NOT fix, and should not be expected to

The published flow figure **changes**, by design. Switching the quote test for
the tick test flips the sign on 16–24% of SPY's session imbalance depending on
the day, and more on the indices. That is the method change, not a defect, and
no amount of further rehearsal removes it. Whether and how that gets explained
to subscribers is a separate decision that does not depend on the cutover date.

## Open before Monday

**Nothing.** This section held one item for three weeks and it closed on
2026-10-16. Kept rather than deleted, because what was open and how it closed is
the part worth re-reading if the cutover is ever questioned.

**VIX / VXN — CLOSED on both halves.**

*Operationally*, measured 2026-10-15 rather than assumed. The rehearsal's
`vix_bars` / `vxn_bars` carried `off_grid = 0` on both indices, which is the
thing that could have broken: Market Value sends a mark every few seconds and
the ingester has to fold each one into its 5-minute bucket, and a single
off-grid row would mean it was instead filling the table one row per poll. 77
RTH bars against production's 78 and 77, closes agreeing to within three cents.
No `VOLATILITY_INDEX_PROVIDER` override is needed.

*Contractually*, answered 2026-10-16: ThetaData confirmed by email that VIX and
VXN index values are both inside the agreement's Market Value index coverage,
with nothing separate to license or pay for. That removes the one reason this
migration might have relocated redistribution exposure rather than retiring it.
Their exact wording, and why it is not interchangeable with "CGIF covers VXN",
is in F5 of the findings — the distinction is worth preserving even though the
practical answer is yes.
