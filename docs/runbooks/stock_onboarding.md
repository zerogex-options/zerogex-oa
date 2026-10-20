# Onboarding a single stock (NVDA pilot)

Step-by-step, in the order it has to happen. Paths assume the standard deploy
roots `/home/ubuntu/zerogex-oa` and `/home/ubuntu/zerogex-web`.

## What was built

Every symbol is now one of four classes:

| class | symbols | what it is |
|---|---|---|
| ETF | SPY, QQQ | listed fund: own price, volume and option chain, extended hours |
| Index | SPX, NDX | cash index: no volume, prices 09:30-16:00 only |
| Future | ES, NQ | levels projected from SPX / NDX, price from the futures feed |
| **Stock** | **NVDA** | listed single stock: ingested exactly like an ETF |

The classifier is `asset_class()` in `src/symbols.py` (backend) and
`ETF_SYMBOLS` / `INDEX_SYMBOLS` / `FUTURES_SYMBOLS` / `STOCK_SYMBOLS` in
`frontend/core/symbols.ts` (web). A stock is anything listed that is not a cash
index, a future or in `ETF_SYMBOLS`, so **the backend needs no code change per
stock**. Only the web list does.

A stock differs from an ETF in exactly these places:

| | ETF (SPY / QQQ) | Stock (NVDA) |
|---|---|---|
| `symbols.asset_type` | `ETF` | `EQUITY` |
| Option-chain session graded to | 16:15 | **16:00**. Equity options have no late session, so grading to 16:15 would read NVDA as stale for 15 minutes every day |
| Strike band | `INGEST_STRIKE_PCT_RANGE` (3%) | `INGEST_STRIKE_PCT_RANGE_BY_SYMBOL` (8% for NVDA, below) |
| Market Tide | included | **excluded**. Below seven names the Tide weights equally, so NVDA would take a fifth of the published index/ETF tide |
| Pair Comparison / Gamma Terminal second ladder | SPY↔QQQ, SPY↔SPX, QQQ↔NDX | **QQQ if Nasdaq-listed, else SPY** (NVDA → QQQ) |
| Vol gauge | VIX (S&P) / VXN (Nasdaq) | its benchmark's: VXN for NVDA |
| Expected Range band, Bulletin expected move | drawn from VIX / VXN | **hidden**. NVDA's own implied vol runs about twice VXN, so a VXN band would be half as wide as its options imply |
| Header dropdowns | under "ETFs" | under "Stocks" |

## What NVDA gets in this pilot, and what it does not

**Live for NVDA:** the header picker, Main and My Dashboard, Gamma Terminal,
Pair Comparison, Live Bulletin, every GEX surface (summary, strike profile,
heatmap, levels, walls, flip, max pain), Flow Analysis, Hedging Flow, Forced
Flow, Smart Money, Volatility, Technicals, Strategy Builder, Live Options
Quotes, Premium Surface, Replay, the API and MCP. Optionally Gamma Shift
history and wall alerts (step 3).

**Not for NVDA yet, on purpose:**

- **Signals and trade ideas** (the signal pages, Trade Bias, Composite Score,
  the Playbook cards). The playbook names a "0DTE" expiry as today's date and
  rounds strikes to $1. NVDA lists Friday weeklies only, so Monday to Thursday
  every card would name an expiration that does not exist. The signal pages say
  so for NVDA and offer a one-click switch to QQQ.
- **Forecast, intraday cone, signal scorecard.** The range model prices NVDA
  off VIX. The web pickers on those pages leave NVDA out.
- **The free levels product**: no `/nvda-gamma-levels` page, so NVDA is kept
  out of the embed widget, llms.txt and the daily levels email (each links to
  that page).
- **Market Tide** (above).

None of these block the pilot. Each is its own follow-up.

## Step 1 — Prove ThetaData serves NVDA (no DB writes)

Run this after the close or before 09:00 ET, so the probe does not compete with
production for the terminal's request budget.

```bash
cd ~/zerogex-oa
make feed-probe PROVIDER=thetadata_mv UNDERLYING=NVDA EXPIRATIONS=3
```

You want:

- `spot` is a number, not `UNAVAILABLE`. That is the stock feed, the same one
  SPY and QQQ use.
- `coverage` close to 100%, and `two-sided quotes` close to `contracts returned`.
- `chain` time well under `THETADATA_POLL_SECONDS` (5s).

If `spot` is unavailable or `contracts returned` is 0, stop here. That is an
entitlement question for ThetaData, not something config can fix.

## Step 2 — Deploy the code (backend first)

Merge `claude/bold-brahmagupta-zp25ji` into `release` in **zerogex-oa** only
for now. The web app waits until NVDA has data (step 6).

```bash
cd ~/zerogex-oa
git fetch origin && git checkout release && git pull
```

Do not restart yet. Step 4 restarts once, with the code and the `.env` change
together.

There is **no schema change** (`make schema-apply` is not needed). On its own
the code changes nothing customers see. Every new behaviour keys off a stock
being configured, and none is yet. The one exception is Market Tide's universe
filter, which does not change today's four symbols.

## Step 3 — Environment

Do steps 3 and 4 **before 09:00 ET** (or after the close). The restart in step
4 briefly interrupts every symbol, and starting before the open gives NVDA a
full first session.

```bash
cd ~/zerogex-oa
cp .env ~/.env.pre-nvda-$(date +%F)
grep -E "^(INGEST_UNDERLYINGS|ANALYTICS_UNDERLYINGS|MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS|WS_ALLOWED_SYMBOLS|WALL_ALERTS_SYMBOLS|BULLETIN_TWEET_SYMBOLS|SIGNALS_UNDERLYINGS)=" .env
```

Edit `.env` by hand.

### Must set

Append `,NVDA` to the end of each existing value:

```bash
INGEST_UNDERLYINGS=<what is there now>,NVDA        # e.g. SPY,$SPXW.X,QQQ,$NDXP.X,NVDA
ANALYTICS_UNDERLYINGS=<what is there now>,NVDA     # e.g. SPY,SPX,QQQ,NDX,NVDA
MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS=<what is there now>,NVDA
```

Add this new line, exactly as written, **with no quotes around the value**:

```bash
INGEST_STRIKE_PCT_RANGE_BY_SYMBOL={"NVDA": 8}
```

No quotes because the Makefile reads `.env` as a makefile (`-include .env`),
where quotes stay as literal characters. A quoted value would reach every
`make`-run tool as invalid JSON. The unquoted form reads the same in systemd,
python-dotenv and make.

Why 8%: the shared band (`INGEST_STRIKE_PCT_RANGE`, 3% by default) is sized so
SPY's chain spans a few of its days.
NVDA moves several times as far in a session, so ±3% covers about one NVDA day
and leaves its round-number walls outside the chain. `INGEST_STRIKE_COUNT_MAX`
(40) still caps the strikes, and SPY / QQQ / SPX / NDX are untouched.

### Optional, each independent

```bash
# Members can set call/put wall alerts on NVDA.
WALL_ALERTS_SYMBOLS=SPY,QQQ,SPX,NDX,NVDA

# Gamma Shift page history for NVDA. Copy your BULLETIN_TWEET_SYMBOLS value
# and append NVDA. Without this line Gamma Shift follows BULLETIN_TWEET_SYMBOLS.
REGIME_SESSION_SYMBOLS=<your BULLETIN_TWEET_SYMBOLS value>,NVDA

# Nightly repair of the 30-day ATM-IV history behind IV rank. Space-separated,
# no quotes (same make reason as above).
DAILY_ATM_IV_SYMBOLS=SPY QQQ SPX NDX NVDA
```

`WS_ALLOWED_SYMBOLS`: if the grep above printed it, append `,NVDA`. If it did
not, leave it alone. NVDA's header price then polls over HTTP, which is what NDX
does today.

### Do NOT add NVDA to these

```
SIGNALS_UNDERLYINGS   FORECAST_SYMBOLS   CONE_SYMBOLS   BULLETIN_TWEET_SYMBOLS
TRADEWORKZ_UNIVERSE   INGEST_FUTURES_INDEXES
```

The first five run models that are not built for a single stock yet (see
"Not for NVDA yet"). `INGEST_FUTURES_INDEXES` is for cash indexes only.

Read it back before restarting anything:

```bash
grep -E "^(INGEST_UNDERLYINGS|ANALYTICS_UNDERLYINGS|MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS|INGEST_STRIKE_PCT_RANGE_BY_SYMBOL|WALL_ALERTS_SYMBOLS|REGIME_SESSION_SYMBOLS|DAILY_ATM_IV_SYMBOLS|WS_ALLOWED_SYMBOLS)=" .env
```

## Step 4 — Restart

```bash
make services-restart
# Only if you set WALL_ALERTS_SYMBOLS:
sudo systemctl restart zerogex-oa-wall-alerts
```

`services-restart` cycles ingestion, analytics, signals and the API one at a
time and stops at the first one that fails to come back.

## Step 5 — Verify the backend

Straight after the restart:

```bash
sudo journalctl -u zerogex-oa-ingestion --since "5 min ago" | grep -E "Initialized IngestionEngine for NVDA|strike band"
```

You want `Initialized IngestionEngine for NVDA` and an NVDA config line reading
`±8.0% strike band`. Every other symbol keeps the band it had.

```sql
SELECT symbol, asset_type, is_active FROM symbols WHERE symbol = 'NVDA';
-- one row: NVDA | EQUITY | t
```

After 04:00 ET (extended-hours stock bars) and again after 09:45 ET (chain and
analytics):

```sql
SELECT 'bars' AS feed, MAX(timestamp) FROM underlying_quotes WHERE symbol = 'NVDA'
UNION ALL
SELECT 'chain', MAX(timestamp) FROM option_chains WHERE underlying = 'NVDA'
UNION ALL
SELECT 'gex', MAX(timestamp) FROM gex_summary WHERE underlying = 'NVDA';
```

All three should be within a couple of minutes of now during the session.

```bash
make ingestion-freshness-healthcheck          # want exit 0, with NVDA reading fresh
make services-health                          # ingestion memory has headroom under 2G

KEY=$(grep -E '^OPS_API_KEY=' .env | head -1 | cut -d= -f2- | tr -d '"'"'"'')
curl -s -H "X-API-Key: $KEY" "http://127.0.0.1:8000/api/gex/summary?symbol=NVDA" \
  | jq '{symbol, spot_price, gamma_flip, call_wall, put_wall, timestamp}'
```

**The request budget.** ThetaData allows 8 concurrent requests per account, and
NVDA adds one bar poll a second plus its chain polls. Compare this count against
the same window on a day before NVDA:

```bash
sudo journalctl -u zerogex-oa-ingestion --since "30 min ago" | grep -cE "thetadata (bar )?poll( for [A-Z]+)? failed"
```

A count near zero is fine. A count that climbs means the budget is tight. Raise
`THETADATA_BAR_POLL_SECONDS` from 1 to 2 before touching anything else.

**Max pain** is a once-a-day pre-market job, so the first day NVDA's max pain is
empty until you run it by hand. Do that once, about 20 minutes after the open,
when open interest has landed:

```bash
make max-pain-refresh MAX_PAIN_SYMBOLS=NVDA
```

**Market Tide** should count the same symbols as before NVDA (4: SPY, QQQ,
SPX, NDX):

```bash
curl -s -H "X-API-Key: $KEY" "http://127.0.0.1:8000/api/flow/market-tide" | jq '{configured_symbols, eligible_symbols}'
```

## Step 6 — Deploy the web app

Only once `/api/gex/summary?symbol=NVDA` returns live values. If you deploy
earlier, NVDA appears in the picker over empty pages.

Merge `claude/bold-brahmagupta-zp25ji` into `release` in **zerogex-web**, then:

```bash
cd ~/zerogex-web/frontend
git fetch origin && git checkout release && git pull
npm ci && npm run build
pm2 restart zerogex-web
```

## Step 7 — Verify the web app

1. The header symbol dropdown is grouped **ETFs / Indexes / Futures / Stocks**,
   with NVDA under Stocks.
2. Pick NVDA in the header, then open **Pair Comparison**: the second column is
   **QQQ**. Open the **Gamma Terminal**: the second ladder is **QQQ**. The
   default applies when a page opens on NVDA. If the page was already open on
   another symbol, its second ladder keeps the pick it had, as it always has.
3. On the Gamma Terminal with NVDA, the **Expected Range** overlay draws
   nothing (by design, see the table at the top).
4. Open **Composite Score** or any signal page with NVDA selected: a notice
   says signals are not available for NVDA yet, with a "Switch to QQQ" link.
5. The Forecast and Signals scorecard pickers do not offer NVDA.

## Rollback

Restore the `.env` copy and restart. Rows already written stay written and are
harmless.

```bash
cp ~/.env.pre-nvda-<date> .env
make services-restart
```

If the web app is already live, also roll zerogex-web back to the previous
`release` commit. Otherwise NVDA stays in the picker over pages that stop
updating.

## Known differences worth knowing, none blocking

- `INGEST_EXPIRATIONS=3` means the nearest three expirations. For SPY that is
  about three days. For NVDA it is the next three Fridays, about three weeks.
  NVDA's GEX is a longer-dated book than SPY's for that reason alone.
- Nothing knows NVDA's earnings dates. Expect its levels to move hard around
  them.
- The Spread Monitor needs 100 contracts within 7 DTE and ±5%, a floor sized for
  daily-expiry chains. NVDA will likely show no Spread Monitor history.
- Backtest archiving of option chains follows `SIGNALS_UNDERLYINGS`, so NVDA's
  chains are not archived and age out at `DATA_RETENTION_DAYS`.

## Adding the next stock

1. **Web:** add the ticker to `STOCK_SYMBOLS` and its listing (`NASDAQ` or
   `NYSE`) to `STOCK_LISTING` in `frontend/core/symbols.ts`. The type checker
   refuses a stock without a listing. The listing decides its comparison
   partner: QQQ for Nasdaq, SPY otherwise.
2. **Backend:** no code change. A new **ETF** goes in `ETF_SYMBOLS` instead.
3. **Environment:** steps 3 to 5 above, with the new ticker in place of NVDA.
