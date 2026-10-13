# Cutting production over to ThetaData

Decided 2026-10-13. Rehearse Wednesday 15 October, rehearse Friday 16 October
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

## What actually changes

Two lines in `.env`, and nothing else:

```
MARKET_DATA_PROVIDER=thetadata_mv
FLOW_CLASSIFIER=tick
```

No schema change, no migration, no code deploy. `VOLATILITY_INDEX_PROVIDER`
stays unset, so VIX and VXN follow the deployment to ThetaData — their coverage
was verified operationally on 2026-09-28 (live pre-open VIX agreeing with the
incumbent to within a penny) and no override is needed. See the open question
below before assuming that is settled contractually.

## Go / no-go, checked the Friday before

All five must hold. Any one failing moves the date; none of them is a judgement
call.

1. **Wednesday's rehearsal wrote NDX rows from ThetaData.** Not "the probe
   passed" — rows in `option_chains` for NDX in `zerogex_shadow`, through the
   full path: symbol resolution, the `$NDXP.X` alias, strike selection, flow
   accumulation, the DB write.
2. **Friday's OPEX rehearsal completed** and its coverage is intact: 390
   buckets per symbol, production-only and rehearsal-only near zero.
3. **No regression on the three settled symbols.** SPY, QQQ and SPX land inside
   the band already recorded for them. A single day carries about ±8 points on
   share-of-flipped-flow (F15), so only a move well outside that is a signal.
4. **NDX's own number is a decision you have made, not one you are discovering
   on Monday.** F12 put it at 29.1% of session imbalance flipped, worst of the
   four, and judged it not shippable on the tick test. Wednesday and Friday give
   it two more readings. If it stays that bad, cutting over ships it anyway,
   because the switch is all-or-nothing.
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

# 3. make the change (edit by hand, two lines)
#    MARKET_DATA_PROVIDER=thetadata_mv
#    FLOW_CLASSIFIER=tick

# 4. read it back BEFORE restarting anything
grep -E "^(MARKET_DATA_PROVIDER|FLOW_CLASSIFIER|VOLATILITY_INDEX_PROVIDER)=" .env

# 5. restart, one service at a time, health-checked between each
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
