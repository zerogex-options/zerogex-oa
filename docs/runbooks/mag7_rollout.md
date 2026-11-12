# MAG 7 rollout: six more stocks, one at a time

MSFT, AMZN, GOOGL, AAPL, META and TSLA join NVDA, in that order. Each is
ingested and modeled exactly as NVDA is: `stock_onboarding.md` covers what a
stock gets and what it does not (no signals, forecast or Market Tide). This
runbook adds what that one did not need for a single stock: the order, a
measurement before and after each stock, and the gates that decide whether the
next one goes in.

Paths assume the deploy roots `~/zerogex-oa` and `~/zerogex-web`.

## Why one at a time

A stock does not get its own server. It gets one more process inside units that
every symbol already shares. The per-stock costs are what the 2026-11-06
baseline measured, at 5 symbols:

| Shared by every symbol | Limit | What one more stock adds |
|---|---|---|
| Ingestion (`zerogex-oa-ingestion`) | 2 GB memory, 1.5 cores | one process: about 90-100 MB, and a share of the CPU quota. Each worker reprocesses its chain as fast as its share allows, so every symbol's option data gets a little staler with each one added |
| Analytics (`zerogex-oa-analytics`) | 1 GB memory, 1 core | one process: about 21 MB and a GEX cycle a minute |
| ThetaData | 8 requests at a time, per account | 6 chain calls every 5 s and 2 price calls a second. Each symbol's process has its own pool, so nothing coordinates them |
| The database | the RDS console says which instance; it averaged about 15 active sessions through the 2026-11-06 session | its option rows (out of ingestion's fixed CPU, so the total write rate barely moves) and its analytics read a minute |
| The box | 15.7 GB, 8 cores | the sum of the above |

Memory is not close anywhere: six stocks add about 600 MB to ingestion's
660-730 MB peak against its 2 GB, and about 130 MB to analytics' 140 MB against
its 1 GB. **The limits are ingestion's CPU, which every symbol's option data
shares, and the database, which everything shares.** The API, signals and
Market Tide are not affected: stocks stay out of their lists, and the API reads
a symbol per request rather than holding a list.

A stock that does not fit does not fail with an error. It shows up as every
symbol's option rows getting staler, its GEX updating less often, the live
price moving less than once a second, or failed ThetaData calls, and only last
as an out-of-memory kill. That is what `make capacity-check` measures.

## What 2026-11-06 found

**Ingestion runs at 1.5 cores, and stays there.** The repo said
`CPUQuota=300%` from 2026-10-28 (6adda93), but nothing installed it: a unit
file reaches `/etc/systemd/system` only through a full deploy, which stops and
starts every service, or a per-service install target, which only the API had.
The session measurements then showed it is not needed. At 14:43 ET, at 1.5
cores and throttled in 99% of its CPU periods, ingestion kept every symbol's
option rows 4-18 s old (median), GEX published every minute, prices moved
every second, and no analytics cycle overran. Doubling the quota would roughly
double how often option rows are written, into a database averaging about 15
active sessions. So the repo now says 150%, the stocks share it, and the check
watches option row age to say when ingestion needs more. With all 11 symbols
each worker gets about half today's share, so expect the ages to roughly
double (SPX, the stalest at 18 s, to about 40 s): inside the 60 s warning, and
the reason to add the stocks one at a time. `make service-limits-install`
installs one unit's limits without a restart, and the check flags any unit
whose running limits differ from the repo's.

**The database works hardest late in the day.** Both session runs averaged
about 15 active sessions (most on CPU), and the afternoon read 47 times as
many disk blocks a second as the open (5,741 against 122), likely as the day's
data outgrows memory. It also grew by about 4.3 GB between 10:04 and 14:43.
Each stock adds its own day of data to both, so compare runs at the same time
of day, and keep an eye on free storage in the RDS console.

**ThetaData's index feed failed for about half an hour.** From about 09:35 ET
its index snapshots answered "No data found" for SPX, NDX and VIX, and some
option snapshots failed too; SPX, NDX, ES and NQ levels froze from about 09:49
until it recovered, between 10:08 and 10:30. Nothing here was at fault, and
the 10:04 run that caught it is not a baseline. Stocks and ETFs, on ThetaData's
stock feed, were unaffected.

**ThetaData serves all six,** probed 2026-11-06 at 08:53 ET at an 8% band:

| Stock | Spot | Contracts | Strikes per expiration | Two-sided | Chain poll |
|---|---|---|---|---|---|
| AMZN | 261.16 | 96 | 16 | 95 | 0.17 s |
| GOOGL | 282.77 | 108 | 18 | 108 | 0.16 s |
| AAPL | 340.38 | 132 | 22 | 125 | 0.29 s |
| TSLA | 352.60 | 138 | 23 | 138 | 0.16 s |
| MSFT | 529.25 | 204 | 34 | 202 | 0.23 s |
| META | 602.19 | 234 | 39 | 234 | 0.18 s |

Coverage was 100% for each. Every one holds more strikes at 8% than NVDA's 14,
so **8% for all six**, TSLA included. META's 39 sits just under the 40-strike
cap per expiration, which bounds it.

**The database's CPU could not be read:** the box has no AWS credentials for
CloudWatch. The check now measures the database's load from Postgres itself
(active sessions, rows written per second); the RDS console's CPU graph is the
cross-check. Its `max_connections` read 3,442, more than the ~1,600 a
db.r6g.large has, so the instance may be larger than the repo's notes say.

## The order

**MSFT, AMZN, GOOGL, AAPL, META, TSLA.** A chain's cost to ingestion is
roughly its contract count, so after MSFT the order runs lightest to heaviest
by the probe. MSFT goes first anyway: its web change is ready, and with the
second-largest chain it is a conservative first measurement, so if it fits, the
three lighter ones after it will. TSLA is last: its contracts are the most
active, so the most of them change every poll.

All six list on Nasdaq, so each compares against QQQ and reads VXN, as NVDA does.

## Once, before the first stock

### 1. Deploy the tools (no restart). Done 2026-11-06

Merge `claude/mag7-capacity-check` (and later `claude/mag7-capacity-check-2`)
into `release`, then:

```bash
cd ~/zerogex-oa && git checkout release && git pull
```

Nothing running reads any of it, so nothing restarts.

### 2. A baseline during the session. Done 2026-11-06

```bash
cd ~/zerogex-oa && make capacity-check LABEL=baseline-150
```

It takes about a minute, reads only, and saves itself; the last line is the
file name. **MSFT's baseline is `~/monitoring/capacity/2026-11-06-1443-baseline-150.json`**,
the 14:43 ET run: every gate passed but one, ingestion's quota against the
repo's, which merging `claude/mag7-ingestion-cpu-150` clears. The 08:52 run was
before the open, and the 10:04 run caught ThetaData's index outage.

### 3. Ingestion's CPU stays at 1.5 cores

Nothing to install: the box already runs 150%, and since 2026-11-06 the repo's
unit file says the same (see "What 2026-11-06 found"). If option row ages ever
need more, raise it in `setup/systemd/zerogex-oa-ingestion.service` and
install it without a restart with `make service-limits-install UNIT=ingestion`,
which shows the change first and needs `CONFIRM=yes` to make it.

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
Gamma Shift history, the IV-rank backfill, the WebSocket list, and the session
levels and forced-flow warm lists when they are set), so every stock is set up
the way NVDA was. Any other line that names NVDA it reports for you to judge,
without editing it. It never touches the signal, forecast, bulletin or
TradeWorkz lists; TradeWorkz needs none, since `TRADEWORKZ_UNIVERSE=*` follows
`ANALYTICS_UNDERLYINGS` (the bots pick the stock up at the next API restart).
It prints only the lines it changes, backs `.env` up to
`~/.env.pre-msft-<date>`, and refuses, writing nothing, if a line is not one it
can change safely (a comment on the line, the band map not valid JSON, a key set
twice). The band defaults to NVDA's 8%, right for all six; `BAND=` overrides it.

**2. Restart only what reads those lists:**

```bash
make services-restart-ingest-analytics
```

Ingestion, then analytics, each health-checked before the next. The API and
signals keep serving. Step 1's last line names what to restart: if it also
names `zerogex-oa-wall-alerts`, run `sudo systemctl restart zerogex-oa-wall-alerts`;
if it names `zerogex-oa-api` (when the WebSocket or forced-flow warm list names NVDA), run
`sudo systemctl restart zerogex-oa-api && make api-health-assert`.

**3. Check it came up:**

```bash
sudo journalctl -u zerogex-oa-ingestion --since "5 min ago" | grep -E "Initialized IngestionEngine for MSFT|strike band"
```

You want `Initialized IngestionEngine for MSFT`, an MSFT line with a `±8.0%`
band, and NVDA's line still at `±8.0%`.

```bash
make symbols-list UNDERLYING=MSFT
```

One row: `MSFT`, asset type `EQUITY`, active `t`.

### The next session

**4. Max pain, the first day only,** about 20 minutes after the open, once open
interest has landed (the pre-market job will cover it from then on):

```bash
make max-pain-refresh MAX_PAIN_SYMBOLS=MSFT
```

**5. The capacity check,** at about the same times of day as the baseline,
comparing against it:

```bash
make capacity-check LABEL=after-msft BASELINE=~/monitoring/capacity/<the run before MSFT>.json
```

The baseline is the last clean run before this stock went in: for MSFT, the
2026-11-06 14:43 ET run (step 2 above); for each later stock, the run that
cleared the stock before it. Run it at about the same time of day as its
baseline, mid-afternoon, since the database's disk reads climb through the day.
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
| freshness | a symbol's GEX snapshots fall 25% below the baseline; its newest snapshot is 5 min old or its price bar 3 min; its option rows' median age is two minutes, or double the baseline and 15 s older, or none written in 10 min | snapshots down 10%; snapshot 3 min old, bar 2 min; option rows a minute old, or 1.5 times the baseline and 5 s older; a chain's fresh share down 15 points |
| thetadata | 30 or more failed price polls in 30 min; an ingestion worker died | 5 or more failed price polls, or 10 failed chain calls, or double the baseline |
| live price | a listed symbol's price moves less often than every 1.5 s (median) | every 1.15 s, or 0.2 s slower than the baseline |
| api | `/api/gex/summary` median over 2 s, or no successful read | over 0.5 s, or twice the baseline |
| database | CPU averaging 80% over the window (CloudWatch) | CPU averaging 60%, peaking 90%, or 15 points over the baseline; active sessions up 50% and by a whole session |
| limits | (never holds on its own) | a unit's running memory cap or CPU quota is not what its file in `setup/systemd` says |
| cpu | (never holds on its own) | analytics pinned at its quota or throttled half the time. Ingestion pinned or throttled is only noted (INFO): it uses all it is given, and its option rows' age is what says whether that costs anything |

Outside the regular session only memory, box, limits and CPU are judged, and
the verdict says NOT JUDGED YET. INFO lines never change the verdict. The
database CPU comes from CloudWatch with the box's own AWS credentials; the box
has none today, so the report says so, and the RDS console's CPU graph for the
same half hour is the substitute. Active sessions and rows written come from
Postgres itself and are always there.

## If a gate holds

Each limit has its own remedy, and each is its own change, made and checked
before the next stock:

- **memory, on ingestion or analytics.** Raise that unit's `MemoryMax` in
  `setup/systemd/`, but only if the box has room: the report's `box` line is
  what is free during a session, and `setup/systemd/zerogex-oa-api.service`
  lists every unit's cap. Ask me for the change.
- **analytics, slower cycles or overruns.** The analytics unit has one core for
  every symbol. The remedy is a higher `CPUQuota` on it (the box has 8 cores; the
  API takes up to 4 and ingestion up to 1.5).
- **freshness, option rows getting staler.** Ingestion's workers are sharing its
  1.5 cores too thinly. Two remedies, in this order. First, have the stocks
  reprocess their chains less often than every 5 s, which nothing downstream
  needs (the GEX cycle reads once a minute); that setting does not exist yet,
  so ask me for it. Second, raise ingestion's quota, which writes more to the
  database: only once the RDS console shows the database has room.
- **limits, a unit not running what the repo says.** `make service-limits-install
  UNIT=<unit>` installs it without a restart.
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
