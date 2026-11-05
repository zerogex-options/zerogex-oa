# MAG 7 rollout: six more stocks, one at a time

MSFT, GOOGL, AMZN, META, AAPL and TSLA join NVDA. Each is ingested and modeled
exactly as NVDA is: `stock_onboarding.md` covers what a stock gets and what it
does not (no signals, forecast or Market Tide). This runbook adds what that one
did not need for a single stock: the order, a measurement before and after each
stock, and the gates that decide whether the next one goes in.

Paths assume the deploy roots `~/zerogex-oa` and `~/zerogex-web`.

## Why one at a time

A stock does not get its own server. It gets one more process inside units that
every symbol already shares:

| Shared by every symbol | Limit | What one more stock adds |
|---|---|---|
| Ingestion (`zerogex-oa-ingestion`) | 2 GB memory, 3 cores | one Python process: its chain every 5 s, its price every second, Greeks for its contracts |
| Analytics (`zerogex-oa-analytics`) | 1 GB memory, **1 core** | one Python process: a GEX cycle a minute |
| ThetaData | 8 requests at a time, per account | 6 chain calls every 5 s and 2 price calls a second. Each symbol's process has its own pool, so nothing coordinates them |
| The database (db.r6g.large) | 2 vCPUs | its option rows, written every few seconds, and its analytics read a minute |
| The box | 15.7 GB | the sum of the above |

The API, signals and Market Tide are not affected: stocks stay out of their
lists, and the API reads a symbol per request rather than holding a list.

A stock that does not fit does not fail with an error. It shows up first as
every symbol's GEX updating less often, the live price moving less than once a
second, or failed ThetaData calls, and only last as an out-of-memory kill. That
is what `make capacity-check` measures.

## The order

**MSFT, GOOGL, AMZN, META, AAPL, TSLA.** Lighter chains first, so the first stock
measures the cost of one cleanly and the risk of a surprise is lowest. TSLA is
last because it carries the busiest single-stock options book, and by then the
cost per stock is known. The probes below can reorder the middle four; TSLA stays
last either way.

All six list on Nasdaq, so each compares against QQQ and reads VXN, as NVDA does.

Strike band: 8% for each, as for NVDA. Each of these usually moves less in a
day than NVDA does, except TSLA, which usually moves more; if TSLA's probe shows
its walls sitting outside 8%, use 10% (`BAND=10` below).
`INGEST_STRIKE_COUNT_MAX` (40 strikes per expiration) caps the cost whatever the
band.

## Once, before the first stock

### 1. Deploy the tools (no restart)

Merge `claude/mag7-capacity-check` into `release`, then:

```bash
cd ~/zerogex-oa && git checkout release && git pull
```

It adds `make capacity-check`, `make stock-env-add`,
`make services-restart-ingest-analytics` and a `STRIKE_PCT_RANGE` option on
`make feed-probe`. Nothing running reads any of it, so nothing restarts.

### 2. The baseline, during a normal session

Twice, between 10:00 and 15:30 ET (late morning and mid-afternoon), so both
halves of the day have a "before":

```bash
cd ~/zerogex-oa && make capacity-check LABEL=baseline
```

It takes about a minute, reads only, and saves itself; the last line is the
file name, which every later run compares against. Check its verdict before
going further: a baseline that already says HOLD means the box is short today,
before any stock.

The line to read is the projection for the two shared units, for example:

```
zerogex-oa-analytics: peak 760 MB + one more symbol ~110 MB = 870 MB, 85% of its 1,024 MB cap
```

### 3. Probe all six, after the close

After 16:15 ET (or before 09:00), so the probes do not compete with the session
for ThetaData's 8 requests:

```bash
cd ~/zerogex-oa
for s in MSFT GOOGL AMZN META AAPL TSLA; do
  make feed-probe PROVIDER=thetadata_mv UNDERLYING=$s EXPIRATIONS=3 STRIKE_PCT_RANGE=8
done
```

Each should show a `spot`, coverage near 100%, two-sided quotes near the
contracts returned, and a chain time well under 5 s. A stock with no spot or no
contracts is an entitlement question for ThetaData; leave it out.

## For each stock

The steps for MSFT. For the others, change the ticker.

### Evening (after 16:15 ET), or before 09:00 ET

An evening change leaves hours to see the stock come up, and to undo it, before
the next open.

**1. The .env change.** First see it, then write it:

```bash
cd ~/zerogex-oa
make stock-env-add SYMBOL=MSFT
make stock-env-add SYMBOL=MSFT CONFIRM=yes
```

It adds MSFT to the lists every stock needs (`INGEST_UNDERLYINGS`,
`ANALYTICS_UNDERLYINGS`, `MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS`), to the strike
band map at NVDA's band, and to each optional list NVDA is in (wall alerts,
Gamma Shift history, the IV-rank backfill, the WebSocket list), so every stock
is set up the way NVDA was. It never touches the signal, forecast, bulletin or
TradeWorkz lists. It prints only the lines it changes, backs `.env` up to
`~/.env.pre-msft-<date>`, and refuses, writing nothing, if a line is not one it
can change safely (a comment on the line, the band map not valid JSON, a key set
twice). For TSLA at 10%: `make stock-env-add SYMBOL=TSLA BAND=10`.

**2. Restart only what reads those lists:**

```bash
make services-restart-ingest-analytics
```

Ingestion, then analytics, each health-checked before the next. The API and
signals keep serving. Step 1's last line names what to restart: if it also
names `zerogex-oa-wall-alerts`, run `sudo systemctl restart zerogex-oa-wall-alerts`;
if it names `zerogex-oa-api` (only when the WebSocket list names NVDA), run
`sudo systemctl restart zerogex-oa-api && make api-health-assert`.

**3. Check it came up:**

```bash
sudo journalctl -u zerogex-oa-ingestion --since "5 min ago" | grep -E "Initialized IngestionEngine for MSFT|strike band"
```

You want `Initialized IngestionEngine for MSFT`, an MSFT line with a `±8.0%`
band, and NVDA's line still at `±8.0%`.

```sql
SELECT symbol, asset_type, is_active FROM symbols WHERE symbol = 'MSFT';
-- MSFT | EQUITY | t
```

### The next session

**4. Max pain, the first day only,** about 20 minutes after the open, once open
interest has landed (the pre-market job will cover it from then on):

```bash
make max-pain-refresh MAX_PAIN_SYMBOLS=MSFT
```

**5. The capacity check,** at about the same times of day as the baseline,
comparing against it:

```bash
make capacity-check LABEL=after-msft BASELINE=~/monitoring/capacity/<baseline file>.json
```

Every number shows its change from the baseline in brackets. Send me the
output.

**6. The gate,** from the report's last line:

- **GO**: the stock stays. Merge its web branch (below), and the next stock can
  go in that evening.
- **LOOK FIRST**: nothing failed, but something moved. Hold the next stock until
  we have read the WARN lines together.
- **HOLD**: do not add the next stock. If customers are affected (a FAIL on
  freshness, live price or the API for a symbol other than the new one), roll the
  new one back now (below), then send me the output.

At best that is one stock a trading day, six in all. Nothing about the schedule
is urgent: a stock can wait a day at any step.

## What the gates are

| Area | Holds (FAIL) when | Look first (WARN) when |
|---|---|---|
| memory | a shared unit's peak plus one more symbol reaches 90% of its cap; any OOM kill | that projection reaches 80% |
| box | under 1 GB of memory available | under 2 GB |
| analytics | 10% or more of a symbol's cycles run past the minute, or 5 points more than the baseline | any rise in overruns; 95th-percentile work over 75% of the minute; median cycle up 50% |
| freshness | a symbol's GEX snapshots fall 25% below the baseline; its newest snapshot is 5 min old or its price bar 3 min | snapshots down 10%; snapshot 3 min old, bar 2 min; a chain's fresh share down 15 points |
| thetadata | 30 or more failed price polls in 30 min; an ingestion worker died | 5 or more failed price polls, or 10 failed chain calls, or double the baseline |
| live price | a listed symbol's price moves less often than every 1.5 s (median) | every 1.15 s, or 0.2 s slower than the baseline |
| api | `/api/gex/summary` median over 2 s, or no successful read | over 0.5 s, or twice the baseline |
| database | CPU averaging 80% over the window | averaging 60%, peaking 90%, or 15 points over the baseline |
| cpu | (never holds on its own) | a shared unit pinned at its quota, or throttled half the time |

Outside the regular session only memory, box and CPU are judged, and the
verdict says NOT JUDGED YET. The database CPU comes from CloudWatch with the
box's own AWS credentials; if they cannot read it the report says so, and the
RDS console's CPU graph for the same half hour is the substitute.

## If a gate holds

Each limit has its own remedy, and each is its own change, made and checked
before the next stock:

- **memory, on ingestion or analytics.** Raise that unit's `MemoryMax` in
  `setup/systemd/`, but only if the box has room: the report's `box` line is
  what is free during a session, and `setup/systemd/zerogex-oa-api.service`
  lists every unit's cap. Ask me for the change.
- **analytics, slower cycles or overruns.** The analytics unit has one core for
  every symbol. The remedy is a higher `CPUQuota` on it (the box has 8 cores; the
  API takes up to 4 and ingestion up to 3).
- **thetadata, failed price polls.** `THETADATA_BAR_POLL_SECONDS=2` in `.env`
  halves the price polling, but for every symbol, so SPY's live price would move
  every 2 s. A stock-only setting does not exist yet; ask me for it before
  reaching for the global one.
- **database CPU.** Stop at the current count. The next step is a larger
  database instance or narrower stock bands, and that is a decision, not a
  restart.
- **only the new stock has no data** (no chain, no price): its ThetaData
  entitlement. Roll it back and ask ThetaData.

## Rollback, one stock

```bash
cd ~/zerogex-oa
cp ~/.env.pre-msft-<date> .env
make services-restart-ingest-analytics
```

Rows it already wrote stay and are harmless. Roll back in reverse order: the
backup for the most recent stock is the state before it. If that stock's web
change is already live, roll `~/zerogex-web` back to the `release` commit before
it too, or the stock stays in the picker over pages that stop updating.

## The web side, per stock

Each stock's web change goes live only after its backend passes the gate, or it
appears in the picker over empty pages. It is one branch per stock,
`claude/mag7-<ticker>-web` in zerogex-web: the stock in the header picker and
every live page, and its free `/<ticker>-gamma-levels` page. Deploy it after the
close:

```bash
cd ~/zerogex-web && git checkout release && git pull && make deploy
```

Then in Google Search Console, URL Inspection on
`https://zerogex.io/<ticker>-gamma-levels`, Request Indexing.

When all seven are in and steady, the free levels menu becomes three pages
(S&P 500, Nasdaq, MAG 7), which is web-only work.
