# Design: Pine Seeds daily gamma-levels exporter (shelved — activate when Seeds reopens)

**Status:** Shelf-ready design. **Not built** — see "Why this is shelved" below.
**Owner:** growth / analytics
**Related:** `zerogex-web/docs/tradingview-indicator.md` (the shipped manual-entry v1 indicator + funnel)

---

## TL;DR

We shipped a free, **manual-entry** TradingView Pine Script that plots the daily
ZeroGEX levels (Gamma Flip, Call Wall, Put Wall, Max Gamma / Pin). The obvious
v2 is to make those levels **auto-populate** so traders don't type them in.

The only mechanism on TradingView that can push our own numeric series onto a
chart is **Pine Seeds** (a public GitHub repo TradingView ingests once daily,
read on-chart via `request.seed()`). Pine Script itself has **no HTTP client**,
so a real-time pull from our API is impossible by construction.

**Blocker:** TradingView has **suspended creation of new Pine Seeds
repositories** ("the creation of new repositories has been suspended, though
existing repositories will continue to be supported" —
<https://github.com/tradingview-pine-seeds/docs/blob/main/repo.md>). We have no
existing repo, so v2 cannot go live today. This document is the build spec to
execute in ~1 day **if/when** Seeds reopens (register interest:
`pine.seeds@tradingview.com` + TradingView's Pine Seeds interest form).

Nothing here is dead code because none of it is committed — it's a design only.

---

## Why this is shelved (decision record)

| Constraint | Consequence |
| --- | --- |
| Pine Script has no `http.*` / fetch | Cannot pull levels from zerogex.io live, ever. |
| TradingView cannot inject values into an indicator's `input.*` from outside | A "copy today's levels" button can't remove the manual typing. |
| Pine Seeds = the only custom-data path | …and **new repos are suspended**. |

So the auto-update options collapse to: (a) wait for Seeds to reopen (this doc),
or (b) a browser-extension overlay that bypasses Pine entirely (separate
project, TradingView-ToS gray area). We chose to keep the manual v1 live and
shelf this design.

---

## Data source (grounded in the current codebase)

The four levels already exist per-symbol in the **`gex_summary`** table, written
every analytics cycle (~per minute) by the analytics engine.

- Table DDL: `setup/database/schema.sql` → `gex_summary` (PK `(underlying, timestamp)`).
  Column mapping to note: DB `gamma_flip_point` → API `gamma_flip`; DB stores
  `total_net_gex` + `net_gex_at_spot` (no `spot_price` column — spot is joined
  from `underlying_quotes` at query time).
- Canonical wall definition: `src/analytics/walls.py::compute_call_put_walls`.
- **Cleanest accessor:** `DatabaseManager.get_latest_gex_summary(symbol)`
  (`src/api/database.py`) — the same cached, wall-fallback-safe read the live
  `/api/gex/summary` endpoint uses. Returns a dict with
  `timestamp, spot_price, gamma_flip, max_pain, call_wall, put_wall,
  net_gex_at_spot, put_call_ratio`.

### EOD semantics

`gex_summary` is an intraday time series. The exporter runs **after the cash
close** (systemd timer, ET), at which point analytics has stopped and
`get_latest_gex_summary` returns the **settled final row of the day** = the EOD
value. We stamp the Seeds row with the **ET date of that row's `timestamp`**, not
`now()`, so a late run still labels the correct trading day. For historical
backfill, use `get_gex_summary_at_ts(symbol, close_ts)` per day (note its column
aliases — verify against the DB, per the mapping above).

### Symbols

SPX / SPY / QQQ only (matches `MAX_PAIN_BACKGROUND_REFRESH_SYMBOLS="SPY,SPX,QQQ"`
in `src/config.py`). No ES/NQ futures — the pipeline is cash equity/index
options end-to-end.

---

## Pine Seeds output format (verified against docs)

Sources: <https://github.com/tradingview-pine-seeds/docs/blob/main/data.md>,
`.../repo.md`.

### Repo layout (a **separate public** repo, e.g. `zerogex-options/zerogex-pine-seeds`)

```
data/
  SPX_CALL_WALL.csv
  SPX_PUT_WALL.csv
  SPX_GAMMA_FLIP.csv
  SPX_MAX_GAMMA.csv
  SPY_CALL_WALL.csv   … (×3 symbols × 4 levels = 12 files)
  QQQ_MAX_GAMMA.csv
symbol_info/
  zerogex-pine-seeds.json     # filename MUST equal the repo name
```

Seeds stores **numeric series**, not labels — so **one seed symbol per level per
ticker** (12 total). Seed symbol names must match `^[A-Z0-9._]+$`, ≤42 chars.

### CSV rows (`data/<SYMBOL>.csv`)

Format: `date,open,high,low,close,volume` — **no header, no blank lines**, sorted
ascending by date, **no duplicate dates**. Date is `YYYYMMDDT` (the `T` = daily).
A level is a single value, so `open=high=low=close=level` and `volume=0.0`:

```
20260706T,5610.0,5610.0,5610.0,5610.0,0.0
20260707T,5625.0,5625.0,5625.0,5625.0,0.0
```

### `symbol_info/<repo>.json`

Required arrays (equal length; `pricescale` may be a single scalar if uniform):

```json
{
  "symbol":      ["SPX_CALL_WALL", "SPX_PUT_WALL", "SPX_GAMMA_FLIP", "SPX_MAX_GAMMA", "..."],
  "description": ["SPX Call Wall (ZeroGEX)", "SPX Put Wall (ZeroGEX)", "SPX Gamma Flip (ZeroGEX)", "SPX Max Gamma / Pin (ZeroGEX)", "..."],
  "pricescale":  100
}
```

`pricescale = 100` → 2 decimal places (fine for SPX/SPY/QQQ price levels).

### Reading it on-chart

```pinescript
callWall = request.seed("seed_zerogex_options_zerogex_pine_seeds", "SPX_CALL_WALL", close)
```

The exact data-source id (`seed_<user>_<repo>`) is **assigned/confirmed by
TradingView at onboarding** — treat the string above as a placeholder and make
it a Pine `input.string` so it's editable without republishing.

---

## Build plan (~1 day once unblocked)

### 1. Pure format module — `src/jobs/pine_seeds_format.py`

No IO, fully unit-testable:

- `seed_symbol(symbol, level) -> str` — e.g. `("SPX","call_wall") -> "SPX_CALL_WALL"`.
- `csv_row(day: date, value: float) -> str` — `YYYYMMDDT,v,v,v,v,0.0`.
- `upsert_row(existing_csv: str, day: date, value: float) -> str` — parse, replace
  same-date row or append, re-sort ascending, dedupe by date, re-serialize.
- `build_symbol_info(symbols: list[str]) -> dict` — the required-arrays JSON.

### 2. Export job — `src/jobs/pine_seeds_export.py`

Mirror `src/jobs/forecast_writer.py` exactly: `argparse` + `asyncio.run`,
`DatabaseManager()` connect/disconnect, ET tz, `_is_trading_day` guard via
`NYSE_HOLIDAYS`, **never raises → exits 0**. Flow:

1. For each of SPX/SPY/QQQ: `gex = await db.get_latest_gex_summary(sym)`.
2. `day = gex["timestamp"].astimezone(ET).date()`.
3. For each level in {call_wall, put_wall, gamma_flip, max_pain}: skip `None`;
   `upsert_row` into `data/<SEED_SYMBOL>.csv` under `PINE_SEEDS_DIR`.
4. Regenerate `symbol_info/<repo>.json`.
5. Publish (see §3). `--dry-run` writes files but skips the push.

Config (env, `src/config.py` + `.env.example`): `PINE_SEEDS_DIR`,
`PINE_SEEDS_REPO_URL`, `PINE_SEEDS_SYMBOLS="SPX,SPY,QQQ"`, `PINE_SEEDS_PRICESCALE=100`.

### 3. Publish to the Seeds repo (host-side)

The DB is only reachable from the host, so the exporter runs on the host (not a
GitHub-hosted runner) and pushes a local checkout of the public Seeds repo:
`git add data/ symbol_info/ && git commit && git push` using a deploy
key / token from env. Keep push failures inside the exits-0 contract (log WARNING,
don't crash). TradingView then pulls the public repo on its own daily cadence.

### 4. Tests — `tests/test_pine_seeds_format.py`

pytest over the pure functions: row formatting, date stamping, upsert
replace-vs-append, ascending sort, dedupe, symbol_info array lengths match.

### 5. Scheduling — systemd timer (repo convention; **no** cron/APScheduler)

Copy `setup/systemd/zerogex-oa-max-pain-refresh.{service,timer}` (the post-market
EOD template):

- `...-pine-seeds-export.service`: `Type=oneshot`, `EnvironmentFile=.env`,
  `ExecStart=venv/bin/python -m src.jobs.pine_seeds_export`, optional
  `ExecStartPost=` healthcheck.
- `...-pine-seeds-export.timer`: `OnCalendar=Mon..Fri *-*-* 16:35:00`
  (server TZ = ET), `RandomizedDelaySec`, `Persistent=true`.
- Makefile: `pine-seeds-export` (`$(PY) -m src.jobs.pine_seeds_export`) +
  `pine-seeds-export-install` (cp units, daemon-reload, enable --now) — mirror
  `max-pain-refresh` targets.

### 6. The v2 indicator (`zerogex-web/.../public/tradingview/…-auto.pine`)

`request.seed()` per level, keeping the v1 manual inputs as an **override**
(manual value ≠ 0 wins; else use the seed). One `input.string` for the data-source
id, one dropdown for which ticker's levels to plot. Publish as a **separate**
TradingView script ("ZeroGEX Gamma Levels — Auto") so the manual v1 keeps ranking.

---

## Activation checklist (do these when Seeds reopens)

1. Register interest now: `pine.seeds@tradingview.com` + the TradingView Pine
   Seeds interest form, so we're notified if it returns.
2. Create the public `zerogex-options/zerogex-pine-seeds` repo; get it approved.
3. Confirm the assigned `seed_<user>_<repo>` data-source id.
4. Implement §1–§5; backfill history via `get_gex_summary_at_ts` per trading day.
5. Enable the systemd timer; verify a day's push lands and TradingView ingests it.
6. Publish the v2 `request.seed()` indicator; link it from the three gamma pages.

## Paywall line (unchanged)

Free = **EOD** daily levels (this, once live). Paid = real-time / intraday /
flow / signals in the dashboard. Seeds' EOD-only cadence enforces that split for
us automatically.
