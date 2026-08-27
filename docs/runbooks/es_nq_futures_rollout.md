# Rolling out ES / NQ

Step-by-step, in the order it has to happen. Paths assume the standard deploy
root `/home/ubuntu/zerogex-oa`.

## What was built

ES and NQ are first-class symbols whose dealer levels come from the **SPX and
NDX option chains**. No options on futures are ingested. ES and SPX track the
same index, so it is the same dealer book — only the price axis differs, by
cost of carry — and an ES surface is the SPX surface with its price-space
fields carried across a measured `F / S` ratio.

Three rules govern what moves:

| | Carried to the futures axis? |
|---|---|
| Strikes, walls, gamma flip, max pain, pin, forced-flow levels, price deltas | **Yes** — multiplied by the basis ratio, rounded to the contract tick |
| Net GEX, wall strength, OI, volume, IV, greeks, ratios, scores | **No** — exposure belongs to the option book, not the axis |
| Spot, OHLC, session closes | **No** — served from the future's own feed (`futures_quotes`) |

Spot is the subtlety worth internalising: overnight the cash index is frozen
at its 16:00 close, so a *projected* spot would report where ES stood at the
bell rather than where it is trading. The levels stay correct in the same
window, because the option book behind them likewise has not moved.

### Which endpoints serve ES/NQ

The axis is chosen **per route from an allowlist**, never inferred. An audit
showed a field-level allowlist cannot be made safe: price values also live in
endpoints with no response model at all (raw dicts — nothing to check), arrive
as JSON strings on models declaring `Decimal` without `json_encoders`, and on
the signal and forecast cards are embedded in free-text `rationale` prose,
which no projector can rewrite. A missed field draws a cash-index number on a
futures chart with nothing to mark it.

| Surface | ES/NQ behaviour |
|---|---|
| GEX (`/api/gex/*` bar the two surfaces below), `/api/v1/levels`, `/api/technicals*`, `/api/max-pain/*` | projected |
| `/api/signals/*`, `/api/forecast*`, `/api/forced-flow/*`, `/api/scorecard/*`, `/api/replay/*` | projected |
| `/api/flow/buying-pressure`, `/api/flow/series`, `/api/flow/market-tide` | projected |
| `/api/market/quote`, `/historical`, `/session-closes`, `/session-levels` | served natively from the future's own bars |
| **per-contract surfaces** — `/api/option/*`, `/api/tools/option-calculator`, `/api/flow/by-contract`, `/api/flow/contracts`, `/api/flow/smart-money`, `/api/market/open-interest`, `/api/gex/premium_surface`, `/api/gex/vol_surface` | **400** |

The refusals are the per-contract surfaces only, and they refuse for a reason
that does not go away: an SPX contract with its strike multiplied by the basis
is not a contract anyone can trade, and the strike no longer round-trips to the
chain it came from. There is no ES chain to substitute. The Strategy Builder,
Option Contracts and Smart Money pages render an explicit panel saying so.

**Prices quoted in prose are converted too.** Signal and forecast cards write
narratives like `target $6,650.00`, and a card reading that beside a chart
trading 6,694 is a number a trader could act on. Those narratives follow the
codebase's own convention — an index price is `$`-prefixed, a score or a
multiple is not — so `$`-amounts within ±half the index level are carried
across and everything else (an option credit, an ATR offset, a percentage) is
left alone. The response's `projection.narrative_prices_converted` flag records
whether that ran.

**To add an endpoint later:** classify every numeric field into `PRICE_FIELDS`
or `NEVER_PROJECT` in `src/jobs/futures_projection.py`, then add the path prefix
to `_PROJECTABLE_PREFIXES` in `src/api/futures_middleware.py`.
`tests/test_futures_projection_coverage.py` imports both tuples and mirrors the
middleware's dispatch, so it fails until every numeric field on the new route is
classified.

Reference: `src/jobs/futures_projection.py`, `src/api/futures_middleware.py`.

---

## Step 1 — Confirm the two things no config can fix

**1a. CME market-data entitlement on the TradeStation account —
REAL-TIME, not delayed.**

> **Status: real-time CME live and confirmed (2026-08-27).** The rollout ran
> on the delayed package; ES/NQ now print in real time.
> `FUTURES_REALTIME_PENDING` in `frontend/core/futuresDataStatus.ts`
> (zerogex-web) is `false` to match, so the site no longer discloses a standing
> delay.
>
> Everything below is now a DIAGNOSTIC path rather than a rollout step — read
> it if ES/NQ ever start lagging again, in this order:
>
> 1. `make ts-whoami` — the FUTURES credential must report the username that
>    carries the CME entitlement. Entitlements attach to a username, so one
>    added to the account's *other* username changes nothing, silently.
> 2. The freshness query below, run twice during the cash session — ES/NQ
>    should read ~1 min, not a fixed ~10.
> 3. `FUTURES_QUOTE_STALE_MINUTES` must stay at `5` in every deployed `.env`.
>    Raising it was a way to stop the flapping on the delayed feed; now that
>    real-time is live it would only hide a real outage, because `stale` has
>    gone back to meaning the feed has DIED.
>
> Note the asymmetry that makes step 1 first: a lagging feed and an unentitled
> credential look identical from the API, and only `ts-whoami` separates them.
> The delay badge still fires either way (it is measured from each quote's own
> age), but with the flag false it now attributes the lag to a stalled feed —
> correct for an outage, misleading for an entitlement that lapsed.

Futures data is a separate exchange subscription from equities/indices, and it
fails in two different ways.

*Not entitled at all:* the ingester connects and receives nothing — an empty
`futures_quotes` for that index, HTTP 200, no error in the log.

*Entitled but DELAYED:* everything looks healthy — streams connect, bars
arrive, the backfill works, the basis measures — and every ES/NQ price is
simply 10 minutes old. This is the harder one, because nothing anywhere says
so. It shipped once: on 2026-08-24 the account carried real-time equities and
**delayed** CME, so ES/NQ ran a fixed ~10 minutes behind. With
`FUTURES_QUOTE_STALE_MINUTES` at 5 the feed was permanently stale, ES/NQ
permanently reported `session: "closed"`, and the header permanently showed
Friday's cash close with Friday's day change.

The two are distinguishable in one query — same client, same process, same
code, so the only variable is the exchange:

```sql
SELECT 'futures' AS feed, index_symbol AS symbol,
       MAX(timestamp) AT TIME ZONE 'America/New_York' AS latest,
       ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/60, 1) AS min_old
FROM futures_quotes GROUP BY index_symbol
UNION ALL
SELECT 'cash', symbol,
       MAX(timestamp) AT TIME ZONE 'America/New_York',
       ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(timestamp)))/60, 1)
FROM underlying_quotes WHERE symbol IN ('SPY','QQQ') GROUP BY symbol
ORDER BY 1, 2;
```

Run it during the cash session, twice, a minute apart. SPY/QQQ are the
real-time control — don't use SPX/NDX, which don't print before 09:30.

| SPY/QQQ | ES/NQ | Reading |
|---|---|---|
| ~1 min | ~1 min | real-time CME. Correct. |
| ~1 min | ~10 min, **holding** across both runs | entitled but delayed — buy real-time CME |
| ~1 min | growing across both runs | the stream is dying, not delayed. See Step 2 |
| ~1 min | no rows | not entitled |

**Entitlements attach to a USERNAME, not to the API application.** OAuth splits
the two, and the split is easy to miss:

```
TRADESTATION_CLIENT_ID / _SECRET   identify the APP
TRADESTATION_REFRESH_TOKEN         identifies the USER who authorised it
```

An account holder with two TradeStation usernames can therefore add real-time
CME to one of them, restart everything, and keep receiving delayed quotes
indefinitely — because the refresh token in `.env` was minted by the *other*
username. Nothing in the API's behaviour says so: streams connect, bars
arrive, prices are simply old.

```bash
make ts-whoami
```

Prints the username behind each credential (decoded from the access token's
claims; the token itself is never printed). Match the futures one against the
username carrying the CME entitlement.

If they differ, restarting will not move the entitlement across — and
**repointing `TRADESTATION_REFRESH_TOKEN` at the entitled username is the wrong
fix.** That one token also drives option chains, equity and index bars,
VIX/VXN, session levels and every backfill tool, so it would trade a ten-minute
ES delay for silently unentitled option ingestion: the same invisible failure,
across everything instead of two symbols.

Give the futures feeds their own identity instead:

```bash
# 1. Sign into TradeStation IN YOUR BROWSER as the CME-entitled username
#    FIRST. The script has no idea which session you are logged into — it
#    just opens an authorise URL, and whichever username is signed in is the
#    one the token belongs to.
#
#    --var is what keeps the MAIN credential out of the blast radius. Without
#    it the script rewrites TRADESTATION_REFRESH_TOKEN, which drives option
#    chains, equity bars and every backfill.
python setup/app/get_tradestation_tokens.py --var TRADESTATION_FUTURES_REFRESH_TOKEN

# It prints the username it just authorised — check it before going further,
# and it writes a timestamped .env backup either way.

# 2. confirm both identities, then restart
make ts-whoami && make services-restart
```

Expect `ts-whoami` to report two different usernames, one per credential. If
both read the same, the browser was signed in as the wrong account — re-run
step 1 after signing in as the other one.

Unset, it falls back to the main credential and nothing changes, so this costs
single-username deployments nothing. It also splits the load across two
per-account stream caps, which buys back the two slots Step 2 spends.

If they match, the entitlement simply has not propagated yet. TradeStation's
own guidance is to sign out and back in after 10–15 minutes; the API
equivalent is a fresh ACCESS token, which the auth layer mints from the
refresh token roughly every 20 minutes anyway — so waiting is usually enough,
and `make services-restart` forces it immediately.

A delayed feed is not a code fault and there is no config that fixes it
honestly: `FUTURES_QUOTE_STALE_MINUTES` can be raised above the delay to stop
the flapping, but the terminal then presents 10-minute-old data as live, which
is worth checking against the CME data agreement. Buy the real-time
(non-professional) CME entitlement on the TradeStation account instead — then
`FUTURES_QUOTE_STALE_MINUTES=5` goes back to meaning "the feed has died",
which is what it is for.

Nothing already stored needs re-loading if you were on the delayed feed:
delayed bars carry the correct timestamp and the correct price, they just
arrived late, so `futures_quotes` history and any measured basis from that
period are accurate.

**1b. `@ES` and `@NQ` must not be back-adjusted.**
This is the one failure that would quietly corrupt every level. A
back-adjusted continuous series carries a synthetic price that is not a
tradable level, so the measured ratio would be meaningless. The code guards
against it — `FUTURES_BASIS_MAX_DEVIATION` rejects any ratio more than 3% from
parity and falls back to carry — but a *silent* fallback to carry is a
degraded state you want to know about, not discover in a month.

Check that the streamed `@ES` close sits within a few points of the front ES
contract's screen price. If your TradeStation continuous symbols are
back-adjusted, pin the front contract explicitly instead:

```bash
INDEX_FUTURES_MAP=SPX=ESZ26,NDX=NQZ26   # and update every quarterly roll
```

## Step 2 — Check the stream budget

TradeStation caps concurrent streams per account at **nominally 10**, and the
cap is *not* header-observable: exhaustion shows up as connections being
closed immediately, not as an error (see `src/ingestion/stream_manager.py`).

This change adds **two** persistent streams (one per index in
`INGEST_FUTURES_INDEXES`) and holds them through the cash session, where
previously the single futures stream slept 09:30–18:00. Before deploying,
count your steady-state streams:

```
  option chunks     = N_underlyings x ceil(symbols_per_underlying / STREAM_QUOTES_MAX_SYMBOLS_PER_CONNECTION)
  underlying bars   = N_underlyings
  VIX + VXN         = 2
  futures (new)     = len(INGEST_FUTURES_INDEXES)      # 2 with SPX,NDX
```

If that total approaches 10, do **not** deploy blind — cap exhaustion degrades
option-chain ingestion, which is far more valuable than ES/NQ. Either trim
`INGEST_FUTURES_INDEXES` to `SPX` (ES only, NQ off) or set
`FUTURES_INGEST_FULL_SESSION=false`, which reverts the ingester to
overnight-only and leaves the projection on cost-of-carry.

`make db-tail-api-calls` and the ingestion logs will show connection churn if
you are over.

## Step 3 — Environment

Edit `/home/ubuntu/zerogex-oa/.env`.

### Must set

```bash
INGEST_FUTURES_ENABLED=true          # already on if the overnight swap was live
INGEST_FUTURES_INDEXES=SPX,NDX       # was SPX — NQ needs NDX
FUTURES_INGEST_FULL_SESSION=true     # NEW, load-bearing (see below)
MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS=SPY,SPX,QQQ,NDX   # add NDX or NQ's max-pain surface ships empty
```

`FUTURES_INGEST_FULL_SESSION` is what makes the basis measurable. ES and SPX
only print at the same minute during the cash session — exactly the window the
old overnight-only gate slept through. Set it `false` and ES/NQ silently fall
back to theoretical cost-of-carry: the levels still render, they are just
modelled rather than observed, and `basis_source` will read `carry`.

### Strongly recommended

```bash
DIVIDEND_YIELD_BY_SYMBOL='{"SPX": 0.013, "NDX": 0.007}'
```

`DIVIDEND_YIELD` defaults to `0.0` — deliberately, because the Greeks engine
wants it that way. The cost-of-carry FALLBACK reuses it, so with no per-symbol
override it prices `e^(r·T)` with no dividend at all and overstates the basis
by roughly a factor of two (on SPX ≈ 83 points instead of ≈ 44). That only
bites while `basis_source` is `carry`, which is already a fault state — but it
makes the fault much worse than it needs to be, and the override costs nothing.

### Leave alone for now

```bash
INDEX_FUTURES_DISPLAY_ENABLED=true   # the old ES-under-SPX swap — step 7 turns it off
```

Turning it off before ES/NQ is verified would drop the overnight SPX header
quote before its replacement is proven.

### Defaults that are fine

| Variable | Default | Change it when |
|---|---|---|
| `FUTURES_UNDERLYINGS_MAP` | `ES=SPX,NQ=NDX,RTY=RUT,YM=DJX` | adding RTY/YM as products |
| `FUTURES_TICK_SIZES` | `ES=0.25,NQ=0.25,RTY=0.10,YM=1.0` | CME changes a tick |
| `FUTURES_BASIS_MAX_DEVIATION` | `0.03` | never, in normal operation |
| `FUTURES_BASIS_LOOKBACK_MINUTES` | `5760` (4 days) | never — sized to span a long weekend but stay inside a quarter |
| `FUTURES_BASIS_SAMPLES` | `15` | never |
| `FUTURES_BASIS_FRESH_MINUTES` | `120` | never — 120 spans the 16:00–18:00 hold exactly |
| `FUTURES_BASIS_CACHE_TTL_SECONDS` | `60` | basis reads show up hot in profiling |
| `FUTURES_BARS_RETENTION_DAYS` | *(follows `DATA_RETENTION_DAYS`)* | only to keep futures LONGER than the index bars — see below |
| `FUTURES_QUOTE_STALE_MINUTES` | `5` | a quiet feed is being reported as closed too eagerly |

### Do NOT add ES/NQ to these

```
INGEST_UNDERLYINGS       ANALYTICS_UNDERLYINGS       SIGNALS_UNDERLYINGS
```

They drive option-chain ingestion and the analytics engine. ES and NQ have no
chain of their own; adding them starts a doomed hunt for an `ES` option chain
and burns TradeStation quota for nothing.

`WS_ALLOWED_SYMBOLS` also needs **no** change. The futures ingester
deliberately never emits the `zgx_quote_updates` NOTIFY, so no ES or NQ tick
is ever published on the websocket bus — those symbols are HTTP-poll only.

## Step 4 — Database

**No migration.** `futures_quotes` already exists and is the only table
involved. The basis derives from `futures_quotes` joined against
`underlying_quotes`, both of which already retain more than the 4-day lookback.

No `symbols` row is needed for ES/NQ either — that table feeds option-chain
ingestion, which ES/NQ never enter.

Expect `futures_quotes` to grow: from ~15.5h/day for one index to ~23h/day for
two, so roughly **3x** the row count. At 1-minute bars that is ~2,760 rows/day
against a 7-day rolling window — trivial, and
`FUTURES_BARS_RETENTION_DAYS` still bounds it.

## Step 5 — Deploy

The ingester must run through one full cash session before a basis can be
measured. Deploying everything at once is not wrong — ES/NQ simply open on
cost-of-carry until the next 09:30–16:00 window.

```bash
cd /home/ubuntu/zerogex-oa
git fetch origin && git checkout claude/es-nq-futures-integration-cjp3op && git pull

# restarts in dependency order (ingestion → analytics → signals → api)
# and asserts the API came back serving
make services-restart
make services-health
```

`zerogex-oa-analytics` and `zerogex-oa-signals` are untouched by this change —
they key on the cash index and never see a futures symbol — but
`services-restart` cycles all four, which is the safe path.

Then the web app:

```bash
cd /home/ubuntu/zerogex-web/frontend
git fetch origin && git checkout claude/es-nq-futures-integration-cjp3op && git pull
npm ci && npm run build          # postbuild regenerates the sitemap
pm2 restart zerogex-web
```

Finally, submit `/es-gamma-levels` and `/nq-gamma-levels` to Search Console if
you want them indexed promptly.

## Step 6 — Verify

**Both indices are ingesting** — run during the cash session:

```sql
SELECT index_symbol, future_symbol, COUNT(*) AS bars, MAX(timestamp) AS latest
FROM futures_quotes GROUP BY index_symbol, future_symbol;
```

Two rows, `latest` within a couple of minutes. Seeing fresh rows *during*
09:30–16:00 ET is the new behaviour and the thing to confirm.

**The basis is measured, not modelled** — the single most important check:

```bash
curl -s -H "X-API-Key: $KEY" "$API/api/gex/summary?symbol=ES" | jq .projection
```

```json
{
  "symbol": "ES",
  "derived_from": "SPX",
  "basis_ratio": 1.0067,
  "basis_source": "measured",
  "basis_observed_at": "2026-08-21T19:58:00+00:00"
}
```

| `basis_source` | Meaning |
|---|---|
| `measured` | healthy — a concurrent print pair inside the last 120 min |
| `measured_stale` | expected overnight and at weekends; **not** expected mid-session |
| `carry` | degraded — no usable pair at all. Re-check steps 1a and 1b |

`basis_ratio` should sit a little above 1.0 (~1.003–1.010 depending on where
you are in the quarter), decaying toward 1.0 into quarterly expiry and
stepping back up at the roll.

**Levels moved, exposures did not:**

```bash
diff <(curl -s -H "X-API-Key: $KEY" "$API/api/gex/summary?symbol=SPX" | jq -S .) \
     <(curl -s -H "X-API-Key: $KEY" "$API/api/gex/summary?symbol=ES"  | jq -S .)
```

`call_wall` / `put_wall` / `gamma_flip` / `max_pain` differ by the basis and
land on 0.25 ticks. `net_gex`, `total_call_oi`, `put_call_ratio` must be
**byte-identical**. Any exposure figure that changed means something was added
to `PRICE_FIELDS` that does not belong there — `tests/test_futures_projection_coverage.py`
exists to catch that at CI time.

**Price is observed, not projected** — run overnight, when SPX is frozen:

```bash
curl -s -H "X-API-Key: $KEY" "$API/api/market/quote?symbol=ES" | jq '{symbol,close,session}'
curl -s -H "X-API-Key: $KEY" "$API/api/market/session-closes?symbol=ES" | jq
```

`close` must track live ES, not `SPX_close x ratio`, and the session closes
must be the future's own 16:00 marks — that pair is what the header's daily
change is computed from.

**The day change is anchored to 16:00:00, not 16:00:59.** Bars are
start-of-minute stamped, so the bar timestamped 16:00 spans the first full
minute *after* the bell. ES and NQ trade through it, so its `close` is live
tape, not a settled level. `current_session_close` must equal that bar's
**open**:

```sql
-- the mark the header divides into the live ES print
SELECT timestamp AT TIME ZONE 'America/New_York' AS et, open, close
FROM futures_quotes
WHERE index_symbol = 'SPX'
  AND (timestamp AT TIME ZONE 'America/New_York')::time = TIME '16:00'
ORDER BY timestamp DESC LIMIT 1;
```

```bash
curl -s -H "X-API-Key: $KEY" "$API/api/market/session-closes?symbol=ES" \
  | jq '{current_session_close, current_session_close_ts}'
```

`current_session_close` must match the row's `open`. If it matches `close`
instead, the headline change is off by whatever ES did in the minute after
the bell — `tests/test_futures_session_closes.py` pins this.

**The session describes the market; staleness is separate.** ES and NQ trade
nearly 23 hours, so `session` reads `open` through the whole CME session and
the header computes live-vs-the-last-16:00-mark. Feed freshness rides its own
fields:

```bash
curl -s -H "X-API-Key: $KEY" "$API/api/market/quote?symbol=ES" \
  | jq '{close, session, timestamp, stale, data_age_seconds}'
```

`session` must be `open` whenever CME is trading — even when `stale` is true.
The two were briefly folded together, so a feed that fell a few minutes behind
reported `closed`, and the frontend answers `closed` by swapping the headline
price for `current_session_close` and its change for `prior_session_close`.
The result was the last cash close published as the live price with that
session's day change: on 2026-08-24 the ES header read `$7692.00 +26.75
(+0.35%)` — Friday's numbers — while ES traded 7675.50 and this endpoint had a
live 7677.25 print. `stale` is what keeps a dead feed off the live chart tip;
it must never change which price is shown.

If `stale` is persistently true while CME is open, the feed is the problem, not
the quote. A *fixed* lag that tracks the clock is a delayed CME entitlement
(Step 1a); a *growing* lag is a dying stream — check the futures children:

```bash
journalctl -u zerogex-oa-ingestion --since '1 hour ago' | grep -i futures
```

Repeated `stream ended without an error` means the TradeStation stream-slot cap
(Step 2), not a code fault.

**Per-contract option endpoints refuse:**

```bash
curl -s -o /dev/null -w '%{http_code}\n' -H "X-API-Key: $KEY" "$API/api/option/quote?symbol=ES"
# 400
```

**Nothing regressed for SPX/SPY/QQQ/NDX:**

```bash
make api-test          # every endpoint, HTTP code + time + size
```

## Step 7 — Turn off the old ES-under-SPX swap

Only after step 6 passes and the ES/NQ pages look right:

```bash
# in /home/ubuntu/zerogex-oa/.env
INDEX_FUTURES_DISPLAY_ENABLED=false

sudo systemctl restart zerogex-oa-api
```

The header stops showing ES's price under the SPX label, and SPX reads as SPX
around the clock.

This does **not** let you switch the futures ingester off — ES/NQ need that
feed for both price and basis.

## Rollback

| Symptom | Lever | Effect |
|---|---|---|
| ES/NQ wrong or noisy | `INDEX_FUTURES_DISPLAY_ENABLED=true` + restart API | old overnight swap back immediately, no deploy |
| Stream cap exhaustion | `FUTURES_INGEST_FULL_SESSION=false` + restart ingestion | frees the cash-session slots; basis drops to carry |
| Need ES only | `INGEST_FUTURES_INDEXES=SPX` | NQ goes dark, ES unaffected |
| Full revert | redeploy the previous commit | nothing was persisted or migrated |

Nothing this feature does is written to the database, so there is no data to
unwind.

## How long ES/NQ history is kept

`futures_quotes` now keeps the **same window as the SPY/QQQ/SPX/NDX bars** by
default. You do not have to set anything.

The equity and index bars in `underlying_quotes` are pruned by `make db-prune`
at `DATA_RETENTION_DAYS` — code default 90, but **`.env.example` ships 60, so
check what your `.env` actually says**:

```bash
grep '^DATA_RETENTION_DAYS=' /home/ubuntu/zerogex-oa/.env    # blank means the 90 default
```

The futures ingester now resolves its own retention to that same number.
`FUTURES_BARS_RETENTION_DAYS` still overrides it, and the only reason to set it
is to keep futures history *longer* than the index bars — which is what a deep
backfill wants:

```bash
FUTURES_BARS_RETENTION_DAYS=365   # keep a year of ES/NQ, whatever DATA_RETENTION_DAYS says
```

Two things worth knowing about the split:

- `futures_quotes` is **not** in `DB_MAINTAIN_TABLES`, so `make db-prune` never
  deletes from it. The ingester is its only pruner. That is deliberate — it is
  what lets a long backfill outlive `DATA_RETENTION_DAYS`.
- It *is* in `DB_VACUUM_EXTRA_TABLES`, so `make db-maintain` still VACUUM FULLs
  and REINDEXes it alongside its peers. A table taking constant upsert churn
  needs that whether or not anything prunes it.

Sizing, if you raise it: one index-year of 1-minute futures bars is roughly
350k rows. Both indices for a year is well under a GB.

## Backfilling ES/NQ history

The live ingester only holds a rolling window (`FUTURES_BARS_RETENTION_DAYS`,
default 7). That is enough for the basis and the intraday chart, and not enough
for the daily/hourly candlestick timeframes, a basis history spanning a
quarterly roll, or any replay over ES/NQ price action.

**Raise retention first.** The prune cannot tell a backfilled row from a
streamed one, so anything outside the window is deleted on the next cycle. The
tool warns rather than letting that happen quietly, but it cannot stop it.

```bash
# 1. in .env — size this to the history you intend to keep
FUTURES_BARS_RETENTION_DAYS=90
sudo systemctl restart zerogex-oa-ingestion

# 2. rehearse
make futures-backfill SYMBOLS=SPX,NDX START=2026-06-01 END=2026-08-21 DRY_RUN=yes

# 3. load it
make futures-backfill SYMBOLS=SPX,NDX START=2026-06-01 END=2026-08-21
```

`SYMBOLS` takes the **cash index** (`SPX,NDX`) — rows are keyed by the index and
the mapped future is resolved through `INDEX_FUTURES_MAP`, the same way the live
ingester resolves it. Bars are stamped on their own minute, matching the live
feed, so a backfilled bar is indistinguishable from a streamed one and pairs
correctly in the basis join. Idempotent on `(index_symbol, timestamp)` — rerun a
range safely.

Two things the historical endpoint cannot give you: the Up/Down volume split
(backfilled bars land 0/0; OHLC is exact) and anything your CME entitlement does
not cover — an unentitled future returns an empty set rather than an error, so a
silent zero-bar result means check the subscription, not the dates.

Disk: one index-year of 1-minute futures bars is roughly 350k rows. Two indices
over a year is well under a GB.

## If you backfilled underlying bars before 2026-08-23

`underlying_backfill.py` used to store TradeStation's raw bar timestamp, which
is the bar's **close**, while the live ingester floors to the bar's own minute.
A row backfilled before that was fixed therefore sits **one minute later** than
the streamed row covering the same minute of market activity.

Both tools now agree, so anything backfilled from here on is correct. The
question is only whether you have older rows worth repairing.

**Do you?** Backfilled bars carry no Up/Down volume split, which is a reliable
signature for SPY/QQQ (a live bar in an active minute has volume):

```sql
SELECT symbol,
       (timestamp AT TIME ZONE 'America/New_York')::date AS et_date,
       count(*) FILTER (WHERE up_volume = 0 AND down_volume = 0) AS zero_vol,
       count(*) AS bars
FROM underlying_quotes
WHERE symbol IN ('SPY','QQQ')
GROUP BY 1, 2
HAVING count(*) FILTER (WHERE up_volume = 0 AND down_volume = 0) = count(*)
ORDER BY 2 DESC;
```

A day that comes back 100% zero-volume was backfilled. Cash indices (SPX/NDX)
have no volume either way, so for those you need to know which ranges you ran.

**Is it worth repairing?** Usually not urgently. The offset only matters where
something joins `underlying_quotes` on timestamp — the analytics
option/underlying pairing and the ES/NQ basis measurement — and both read
recent data, which is live rather than backfilled. It matters if you backfilled
a range you then measure across.

**If you do repair it:** delete the affected range and re-run the backfill with
the fixed tool. Do not try to shift timestamps in place — a `-1 minute` update
collides with rows that already occupy the target minute.

```sql
-- Check first. Only delete a range you know was backfilled, not one that
-- overlaps live streaming, or you will delete good rows.
DELETE FROM underlying_quotes
WHERE symbol = 'SPY'
  AND timestamp >= '2026-06-01' AND timestamp < '2026-07-01';
```

```bash
python -m src.tools.underlying_backfill --symbols SPY --start 2026-06-01 --end 2026-06-30
```

The upsert is idempotent on `(symbol, timestamp)`, so re-running a range is
safe — it is only the *old* mis-stamped rows that need removing first, because
they occupy different keys and would otherwise linger as duplicates.

## Known limits

- **Quarterly roll.** For a day or two around the roll the measured ratio can
  straddle two contracts. The 4-day lookback bounds how long a pre-roll
  measurement survives and the median over 15 samples absorbs the rest, but
  eyeball `basis_ratio` on roll week. The cost-of-carry *fallback* prices to
  the calendar quarter and does not know about the ~8-day roll lead, so it
  runs slightly long during that window — only relevant if you are already on
  `carry`, which is itself a problem to fix.
- **ES/NQ history is bounded by `FUTURES_BARS_RETENTION_DAYS` (7 days).**
  Daily and hourly chart timeframes look short until that is raised and a
  backfill is run — see the backfill section above.
- **A holiday-shortened week** can leave the basis `measured_stale` longer than
  usual. That is correct — nothing has measured it — and levels stay usable.
- **ES/NQ inherit SPX/NDX's analytics cadence.** They are not fresher than the
  chain behind them, even though their price ticks continuously.
- **Bar alignment.** `futures_quotes` and `underlying_quotes` can label the
  same minute up to one bar apart; the basis join tolerates 120s and never
  looks ahead, so the residual error is well under a tick.
- **No dedicated alerting** on the futures ingester children yet. A dead ES
  feed surfaces as `basis_source: carry`, not as a page.
- **Native ES/NQ options are deliberately not ingested.** If that changes, the
  projection should stay as the fallback for when the futures chain is thin.
