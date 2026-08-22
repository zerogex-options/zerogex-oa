# Rolling out ES / NQ

Everything an operator has to change to make ES and NQ live, in the order it
has to happen.

## What was built

ES and NQ are first-class symbols whose dealer levels come from the **SPX and
NDX option chains**. No options on futures are ingested. ES and SPX track the
same index, so it is the same dealer book — only the price axis differs, by
cost of carry — and an ES surface is the SPX surface with its price-space
fields carried across a measured `F / S` ratio.

Two rules govern what moves:

| | Projected onto the futures axis? |
|---|---|
| Strikes, call/put wall, gamma flip, max pain, pin | **Yes** — multiplied by the basis ratio, rounded to the contract tick |
| Net GEX, wall strength, OI, volume, IV, greeks | **No** — exposure belongs to the option book, not the axis |
| Spot, OHLC bars | **No** — served from the future's own feed (`futures_quotes`) |

Spot is the subtlety worth internalising: overnight the cash index is frozen
at its 16:00 close, so a *projected* ES price would report where ES stood at
the bell rather than where it is trading now. The levels stay correct in the
same window, because the option book behind them likewise has not moved.

Reference: `src/jobs/futures_projection.py`, `src/api/futures_middleware.py`.

## 1. Environment (zerogex-oa)

Everything below has a working default except the three marked **must set**.

### Must set

```bash
INGEST_FUTURES_ENABLED=true          # already on if the overnight swap was live
INGEST_FUTURES_INDEXES=SPX,NDX       # was SPX — NDX is new, and NQ needs it
FUTURES_INGEST_FULL_SESSION=true     # NEW — see below, this one is load-bearing
```

`FUTURES_INGEST_FULL_SESSION` is what makes the basis measurable. The futures
ingester previously slept 09:30–18:00 ET, so ES and SPX never printed at the
same minute and no ratio could be observed. It now follows the whole CME
session. Cost is two 1-minute bar streams; `FUTURES_BARS_RETENTION_DAYS` still
bounds the table. Set it to `false` and ES/NQ silently fall back to
theoretical cost-of-carry — the levels still render, they are just modelled
rather than observed.

### Turn off the old behaviour

```bash
INDEX_FUTURES_DISPLAY_ENABLED=false  # was true
```

This is the ES-price-under-the-SPX-label swap. It existed because SPX went
dark overnight and the future's price had nowhere else to live; ES now has its
own page. Flip this **after** ES/NQ is verified, not before — see §6.

Note it does **not** let you turn off the futures ingester: ES/NQ need that
feed for both price and basis.

### Defaults you can leave alone

| Variable | Default | Change it when |
|---|---|---|
| `FUTURES_UNDERLYINGS_MAP` | `ES=SPX,NQ=NDX,RTY=RUT,YM=DJX` | adding RTY/YM as products |
| `FUTURES_TICK_SIZES` | `ES=0.25,NQ=0.25,RTY=0.10,YM=1.0` | CME changes a tick |
| `INDEX_FUTURES_MAP` | `SPX=@ES,NDX=@NQ,…` | pinning a specific contract (see §3) |
| `FUTURES_BASIS_MAX_DEVIATION` | `0.03` | never, in normal operation |
| `FUTURES_BASIS_LOOKBACK_MINUTES` | `5760` (4 days) | never — sized to span a long weekend but stay inside a quarter |
| `FUTURES_BASIS_SAMPLES` | `15` | never |
| `FUTURES_BASIS_FRESH_MINUTES` | `90` | tuning the stale disclosure |
| `FUTURES_BASIS_CACHE_TTL_SECONDS` | `60` | basis reads show up hot in profiling |

### If you use the websocket surface

```bash
WS_ALLOWED_SYMBOLS=SPY,QQQ,SPX,ES,NQ,$VIX.X,$VXN.X
```

`_ALLOWED_SYMBOLS` in `src/api/routers/websockets.py` is an explicit allowlist
and does not consult the futures registry.

### Do NOT add ES/NQ to these

```
INGEST_UNDERLYINGS       ANALYTICS_UNDERLYINGS       SIGNALS_UNDERLYINGS
```

They drive option-chain ingestion and the analytics engine. ES and NQ have no
chain of their own; adding them would start a doomed hunt for an `ES` option
chain and burn TradeStation quota for nothing.

## 2. Database

**No migration.** `futures_quotes` already exists and is the only table
involved. The basis is derived from `futures_quotes` joined against
`underlying_quotes`, both of which already retain more than the 4-day lookback.

No `symbols` row is needed for ES/NQ either — that table feeds option-chain
ingestion, which ES/NQ never enter. (Worth knowing: its `asset_type` CHECK
only permits `EQUITY`/`INDEX`/`ETF`, so adding one would need a migration. It
is not needed.)

## 3. Manual steps

These are the ones no config change can do for you.

**a. Confirm CME market-data entitlement on the TradeStation account.**
Futures data is a separate exchange subscription from equities/indices. If
`@NQ` is not entitled, the ingester connects and receives nothing — the symptom
is an empty `futures_quotes` for NDX with no error in the log. Check before
blaming the code.

**b. Verify `@ES` and `@NQ` are not back-adjusted.**
This is the one that would quietly corrupt every level. A back-adjusted
continuous series carries a synthetic price that is not a tradable level, so
the measured ratio would be meaningless. The code guards against it —
`FUTURES_BASIS_MAX_DEVIATION` rejects any ratio more than 3% from parity and
falls back to carry — but a *silent* fallback to carry is a degraded state you
want to know about, not discover in a month. Check that the streamed `@ES`
close is within a few points of the front ES contract's screen price. If your
TradeStation continuous symbols are back-adjusted, pin the front contract
explicitly instead:

```bash
INDEX_FUTURES_MAP=SPX=ESZ26,NDX=NQZ26   # and update every quarterly roll
```

**c. Rebuild and redeploy the web app.** The symbol list is compile-time;
there is no web-side env var. The sitemap regenerates on `postbuild`.

**d. Submit the new URLs to Search Console** — `/es-gamma-levels` and
`/nq-gamma-levels` — if you want them indexed promptly.

## 4. Deploy order

The ingester has to run for a full cash session before the API can measure a
basis, so give it one. Deploying everything at once is not wrong, it just means
ES/NQ open on cost-of-carry until the next 09:30–16:00 window.

```bash
# 1. ingestion first — it populates futures_quotes for NDX and starts
#    measuring the basis during the cash session
sudo systemctl restart zerogex-oa-ingestion

# 2. verify futures_quotes is filling for BOTH indices (see §5)

# 3. API — starts answering symbol=ES / symbol=NQ
sudo systemctl restart zerogex-oa-api

# 4. web
npm run build && pm2 restart zerogex-web
```

`zerogex-oa-analytics` and `zerogex-oa-signals` are untouched: they key on the
cash index and never see a futures symbol.

## 5. Verification

**Both indices are ingesting:**

```sql
SELECT index_symbol, future_symbol, COUNT(*) AS bars, MAX(timestamp) AS latest
FROM futures_quotes
GROUP BY index_symbol, future_symbol;
```

Expect two rows. `latest` should be within a couple of minutes whenever CME is
open — including during the cash session, which is the new part.

**The basis is being measured, not modelled** — run during 09:30–16:00 ET:

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

`basis_source` is the health signal:

| Value | Meaning |
|---|---|
| `measured` | healthy — a concurrent print pair inside the last 90 min |
| `measured_stale` | expected overnight and at weekends; **not** expected mid-session |
| `carry` | degraded — no usable pair at all. Check §3a and §3b |

`basis_ratio` should sit a little above 1.0 (roughly `1 + (r − q) × T`, so
~1.003–1.010 depending on where you are in the quarter) and decay toward 1.0
as the quarterly expiry approaches, then step back up at the roll.

**Levels moved and exposures did not:**

```bash
diff <(curl -s -H "X-API-Key: $KEY" "$API/api/gex/summary?symbol=SPX" | jq -S .) \
     <(curl -s -H "X-API-Key: $KEY" "$API/api/gex/summary?symbol=ES"  | jq -S .)
```

`call_wall` / `put_wall` / `gamma_flip` / `max_pain` should differ by the basis
and land on 0.25 ticks. `net_gex`, `total_call_oi`, `put_call_ratio` must be
**byte-identical**. If any exposure figure changed, something got added to
`PRICE_FIELDS` that does not belong there.

**Price is observed, not projected** — run overnight, when SPX is frozen:

```bash
curl -s -H "X-API-Key: $KEY" "$API/api/market/quote?symbol=ES" | jq '{symbol, close, session}'
```

`close` must track live ES, not `SPX_close × ratio`.

## 6. Sequencing the swap-off

Leave `INDEX_FUTURES_DISPLAY_ENABLED=true` through the first session. Once
`basis_source` reads `measured` during the cash session and the ES/NQ pages
look right, flip it to `false` and restart the API. The header stops showing
ES's price under the SPX label, and SPX reads as SPX around the clock.

If ES/NQ has to be pulled, flipping it back to `true` restores the old
behaviour immediately — no deploy. That reversibility is the reason the swap's
code path and its frontend consumers were left in place rather than deleted;
removing them is a follow-up once ES/NQ has had a clean run.

## 7. Known limits

- **Quarterly roll.** For a day or two around the roll the measured ratio can
  straddle two contracts. The 4-day lookback bounds how long a pre-roll
  measurement can survive, and the median over 15 samples absorbs the rest.
  Worth eyeballing `basis_ratio` on roll week.
- **A holiday-shortened week** can leave the basis `measured_stale` longer than
  usual. That is correct — nothing has measured it — and levels stay usable.
- **ES/NQ inherit SPX/NDX's analytics cadence.** They are not fresher than the
  chain behind them, even though their price ticks continuously.
- **Native ES/NQ options are deliberately not ingested.** If that changes, the
  projection should stay as the fallback for when the futures chain is thin.
