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

Porting futures to ThetaData is separate work: a provider seam that does not
exist yet, plus whatever ThetaData charges for CME. Not a cutover blocker, and
not a reason to delay Monday.

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

0. **The NDX write overflow is identified and fixed.** ADDED 2026-10-14 and the
   hardest of these. NDX stopped persisting nine minutes before the close with
   "numeric field overflow", and every retry failed the same way (F16). NDX
   expires daily, so in production this recurs at the end of every expiry day —
   silently, in the part of the session its flow matters most. **Do not cut over
   with this open.**
1. ~~**Wednesday's rehearsal wrote NDX rows from ThetaData.**~~ **MET
   2026-10-14** — 547,453 rows, 2,444 contracts, 8 expirations, through the full
   path. The pipeline works; see gate 0 for what it then did.
2. **Friday's OPEX rehearsal completed** and its coverage is intact: 390
   buckets per symbol, production-only and rehearsal-only near zero.
3. **No regression on the three settled symbols.** SPY, QQQ and SPX land inside
   the band already recorded for them. A single day carries about ±8 points on
   share-of-flipped-flow (F15), so only a move well outside that is a signal.
4. **NDX's own number is a decision you have made, not one you are discovering
   on Monday.** **Now settled, and settled badly**: two independent readings,
   29.1% on 2026-10-09 and 27.8% on 2026-10-14, agreeing to 1.3 points against a
   ±8-point single-day band (F16). It is not noise. The switch is all-or-nothing,
   so cutting over publishes NDX at that figure. The one lever anyone had in mind
   — the staleness guard — is refuted as NDX's fix (F17), so there is nothing
   left to try before Monday. **This is a decision, not a measurement.**
5. **Production healthy through both rehearsals**: `make
   ingestion-freshness-healthcheck` returning 0, no concurrent-stream cap
   warnings.

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

# 3. ONLY IF the gate 0 fix widened a column: apply the schema change FIRST
#    make schema-apply
#    Schema changes are applied by hand and must land BEFORE the services
#    restart. The other way round, writes fail against the old column — which
#    is the failure this fix exists to stop.

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

## What this does NOT fix, and should not be expected to

The published flow figure **changes**, by design. Switching the quote test for
the tick test flips the sign on 16–24% of SPY's session imbalance depending on
the day, and more on the indices. That is the method change, not a defect, and
no amount of further rehearsal removes it. Whether and how that gets explained
to subscribers is a separate decision that does not depend on the cutover date.

## Open before Monday

**VIX / VXN CGIF coverage, contractually.** Operationally they are fine on
ThetaData and no override is needed. Whether they sit inside the existing CGIF
coverage in the executed agreement is still unanswered, and the whole point of
this migration is to retire redistribution exposure rather than relocate it.
Leaving them pinned to TradeStation with `VOLATILITY_INDEX_PROVIDER=tradestation`
keeps that exposure where it is today; moving them without an answer takes it
somewhere unanswered. **This needs an answer from ThetaData and has vendor lead
time** — ask now, not Monday.
