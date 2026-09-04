# Gamma "orb" chart with multi-ticker strike ladders and dated replay — feasibility

**Question:** a screenshot of a competitor's terminal was shared with the
question "is something like this possible to create?". The surface shows a
candlestick chart with per-strike gamma exposure drawn as horizontal
"orb" bands that persist through the session (size = magnitude, gold =
positive / purple = negative), a toolbar with GEX / VEX / GEX+VEX / Delta
toggles and 1m→1W timeframes, an expiration selector, per-strike price-axis
tags carrying an expiration date and a dollar figure, three side-by-side
strike ladders (SPXW, SPY, QQQ) with a "King" strike, and a dated replay
transport (date picker, clock, play, step sizes, "Exit Replay Mode").

**Answer: yes, and most of it is assembly rather than invention.** The
per-minute, per-strike, per-expiration history the orbs need is already
written every 60 s and already served by two endpoints; the candle chart,
zoom/pan, rewind, expiration filter, strike ladders, GEX King, and a
per-minute dated replay all exist as separate surfaces. The genuinely new
pieces are (1) the orb glyph layer itself, (2) a session-wide data path for
it that does not go through the endpoint that is currently throttled to 24
buckets in production, (3) per-strike **delta** exposure, which is not
computed or stored today, and (4) three extra bar timeframes. Nothing
requires a new data vendor or a schema redesign.

**Status:** assessment only. No production code was written. Every claim
carries a `file:line` reference into `zerogex-oa` (this repo) or
`zerogex-web/frontend` (prefixed `web:`); line numbers are as of the commit
this document was added in.

---

## 0. Feature inventory of the screenshot, mapped to what exists

| Screenshot element | Exists today? | Where |
|---|---|---|
| Candlestick chart, zoom + pan, 1m/5m/15m/1H/1D | **Yes** | `web:components/GammaTerminalChart.tsx:15-19` (hand-rolled SVG), `:59-67` (timeframes), `:143-155` (zoom/pan) |
| 3m, 4H, 1W timeframes | **No** | bar allowlist is 1min/5min/15min/1hr/1day: `src/api/queries/_sql_helpers.py:63-74` and the `Literal[...]` params e.g. `src/api/main.py:1678` |
| Per-strike GEX drawn on the price axis, per time bucket, across the session ("orbs") | **Data yes, glyph no** | data: `gex_by_strike` (`setup/database/schema.sql:577-594`), written each `ANALYTICS_INTERVAL=60` s (`src/config.py:1806`, `src/analytics/main_engine.py:2999-3011`); nearest existing renderer is the strike×time heatmap `web:components/GammaHeatmapCanvas.tsx:749-802` (candles painted over a canvas grid) |
| Gold/purple by sign, size by magnitude | **Partial** | sign+magnitude tinting exists in the heatmap (`GammaHeatmapCanvas.tsx:44-58`) and the ladders (`web:components/PairGammaHeatmap.tsx:3-36`); the ellipse glyph is new |
| GEX / VEX / GEX+VEX toggles | **Data yes, payload no** | `vanna_exposure`, `dealer_vanna_exposure` per strike/expiration/minute: `schema.sql:590`, `:624-630`; the timeseries payload only ships call/put/net gamma + OI: `web:hooks/useStrikeProfileTimeseries.ts:31-38` |
| Delta (DEX) toggle | **No** | no delta column in `gex_by_strike`; the delta-pressure signal falls back to a distance proxy: `src/signals/basic/dealer_delta_pressure.py:8-12`, `:67-73`. Per-contract `delta` IS stored: `schema.sql:129` |
| Expiration selector ("Current" / all) | **Yes** | `expirations=` on the timeseries endpoint `src/api/main.py:858-868`; shared multi-select `web:components/GammaTerminalChart.tsx:411-419`; rolling-0DTE selection `web:core/expirationPersistence.ts` |
| Price-axis tags per strike with expiration + $ ("GP $5.43B · 6/26") | **Partial** | walls/flip/pin/king tags exist on the axis; per-strike expiration mix is served by `/api/replay/range?include_expirations=true` (`src/api/routers/replay.py:253-262`) and by `/api/gex/by-strike` rows keyed per expiration |
| Three strike ladders side by side (SPXW / SPY / QQQ) | **Two exist** | `web:components/PairGammaHeatmap.tsx:3-36` (two centre-pinned ladders, king starred, levels marked); data `/api/gex/by-strike` `src/api/main.py:706-733` |
| "King" per ladder | **Yes** | `gex_summary.max_gamma_strike` (`schema.sql:445`), surfaced per frame in `src/api/routers/replay.py` (`_shape_summary`) and drawn by the chart (`GammaTerminalChart.tsx:96-104`) |
| SPXW as a ticker | **Yes, labelled SPX** | production ingests `INGEST_UNDERLYINGS=SPY,$SPXW.X,QQQ,$NDXP.X` (`docs/runbooks/ingestion_symbol_gap.md:62`); the picker exposes SPY/QQQ/SPX/NDX/ES/NQ (`web:core/symbols.ts:12`) |
| Live header quote per ticker | **Yes** | `/ws` quote stream `src/api/routers/websockets.py:1-20`; default allowlist SPY,QQQ,SPX,$VIX.X,$VXN.X (`:63-67`, env-overridable; NDX would need adding) |
| Replay: pick a date, scrub minutes, play at 1/4/16/60×, exit | **Yes (separate page)** | `web:app/replay/[symbol]/[date]/ReplayScrubber.tsx:24-37`, `:164`; sessions list `/api/replay/sessions` (`replay.py:177`); whole-session frames `/api/replay/range` (`:245`) |
| Replay inside the main chart (same surface) | **Partial** | the chart's Rewind is session-only, seeded with 78 five-minute buckets (`web:hooks/useStrikeProfileTimeseries.ts:93-107`, `GammaTerminalChart.tsx:388-401`) — no date picker |
| Growing candle during replay | **Yes** | sub-interval rebuild `GammaTerminalChart.tsx:70-79`, `:608-616`; 5-min bucketing of 1-min candles `ReplayScrubber.tsx:288-309` |
| Screenshot / PNG export (camera icon) | **Yes** | `web:core/chartImageExport.ts`, used at `GammaTerminalChart.tsx:22`, `:42` |

---

## 1. The data the orbs need already exists (Q: is the history there?)

Every 60 s the analytics engine writes one `gex_by_strike` row per
`(underlying, timestamp, strike, expiration)` with `net_gex`, `call_gamma`,
`put_gamma`, `call_oi`, `put_oi`, `vanna_exposure`, `charm_exposure` and the
dealer-signed vanna/charm columns (`setup/database/schema.sql:577-594`,
`:624-630`; writer `src/analytics/main_engine.py:2999-3011`, computed in
`_calculate_gex_by_strike` at `:1223`). That is exactly the
(time × strike) → magnitude grid the orbs are a picture of, and it already
carries the expiration dimension the screenshot's axis tags use.

Two constraints shape what the orbs can show:

* **Strike coverage per minute is a band around spot.** Ingestion streams
  strikes within `INGEST_STRIKE_PCT_RANGE` (default 3.0 %) up to
  `INGEST_STRIKE_COUNT_MAX` (40) per expiration (`src/config.py:1802-1803`;
  the wing signal needs ≥ 4 %, `:727-730`). A strike only has orb history
  for the minutes it was inside the band. The screenshot's bands sit within
  ±1 % of spot, so this matches the target picture rather than limiting it;
  far-wing orbs simply will not exist.
* **History depth is bounded by retention.** `DATA_RETENTION_DAYS` defaults
  to 90 (`src/config.py:600`; `.env.example:685` sets 60) and
  `make db-prune` deletes by `timestamp` (`Makefile:3331-3336`). The replay
  date picker can offer at most that many sessions, which is what
  `/replay` already advertises (`web:app/replay/page.tsx:95-96`).

Symbols: production ingests SPY, the SPXW weekly chain (stored as `SPX`),
QQQ and NDXP (stored as `NDX`) (`docs/runbooks/ingestion_symbol_gap.md:62`).
ES / NQ are the SPX / NDX levels projected onto the futures axis
(`web:core/symbols.ts:6-12`, `:44-47`). So the three ladders in the
screenshot (SPXW, SPY, QQQ) are all symbols we already carry per minute.

---

## 2. Which endpoint feeds it — and the one to avoid (Q: can we serve a whole session?)

There are three candidate reads. Only one of them is safe for a
session-wide, per-minute, per-strike surface.

| Endpoint | Shape | Ceiling | Verdict for orbs |
|---|---|---|---|
| `/api/gex/strike-profile-timeseries` (`src/api/main.py:847-945`) | N buckets (1/5/15 min) × every strike, call/put/net gamma + OI, flip/walls/pin/king, `expirations` filter | `window_units ≤ 480` by contract, but production is dialled to **24** after the Aug 21 2026 stampede; the 15 s guard returns `[]` with HTTP 200; measured 24 → 5.3 s, 32 → 13 s, ≥ 40 never finished (`docs/runbooks/strike_profile_timeseries_stampede.md`; knobs `.env.example:1170-1225`) | **Keep for the live tip only** (the 3-bucket 1 Hz poll it already serves, `web:hooks/useStrikeProfileTimeseries.ts:100-107`). Do not widen it to a session. |
| `/api/replay/range` (`src/api/routers/replay.py:245-393`) | every per-minute frame of one ET session: flip/walls/max-pain/pin/king + strikes within ±`strike_band_pct` (default 4 %, max 10 %) of session spot with net/call/put gex; candles included; `is_today` flag; optional per-strike expiration shares | one correlated LATERAL per minute, ~390 index probes regardless of table size; measured 94–1,158 ms in the query comments (`src/api/database.py:2898+`); raises `ReplayFramesUnavailable` → 503 instead of masquerading as empty; ~5 s end-to-end with `include_expirations=true` (`web:app/replay/[symbol]/[date]/loading.tsx:9-12`) | **Use this.** It is already the orb dataset for any date including today. `timeframe` is accepted but ignored (always 1-min, `replay.py:276-278`); 5/15-min orbs are a trivial client-side fold. |
| `/api/gex/heatmap` (`src/api/main.py:813-823`) | strike × time grid of `net_gex` (AVG across expirations), ±8 % band (`src/config.py:259`), 1min → 1day | `window_units ≤ 300`; no server-side expiration filter (the canvas passes one "best-effort", `web:components/GammaHeatmapCanvas.tsx:1087`) | Adequate for a net-only, all-expiration orb view on longer timeframes; loses the expiration dimension and call/put split. |

Recommendation: the orb layer reads `/api/replay/range` once per
`(symbol, date)` (past sessions are immutable, so the BFF can cache them
for a day like the snapshot route already does) plus the existing
3-bucket tip poll for the forming minute. Past-date loads become the
same ~1–5 s the `/replay/[date]` page already pays; today's session is
one call at mount and one every minute (`web:hooks/usePairReplay.ts:99-103`
already does exactly this for the pair page).

---

## 3. Rendering: SVG layer vs canvas (Q: will it be fast enough?)

`GammaTerminalChart` is hand-rolled SVG with a per-bar map for candles
(`web:components/GammaTerminalChart.tsx:2459-2481`) and a `layout` that
already exposes `xForIndex(i)` and `yPrice(price)` (`:1117-1126`). The orb
layer is one more `<g>`: for each visible bar `i` and each strike `s` in the
bucket aligned to that bar, `<ellipse cx=xForIndex(i) cy=yPrice(s.strike)
rx=f(|gex|) ry=g(|gex|) fill=sign>`. The rail bars at `:1173-1242` are the
template — same coordinate mapping, same coercion of the loosely-typed
payload.

Element budget: the default view is 90 bars (`:146`), ~30 in-band strikes →
~2,700 ellipses, re-rendered on the 1 Hz tip poll. That is at the upper end
of what React-managed SVG tolerates without jank on a laptop, and above it
on a phone. Two ways to keep it comfortable, both with precedent in the
repo:

1. **Canvas underlay.** `GammaHeatmapCanvas` paints the strike×time grid to
   an offscreen canvas and blits it (`web:components/GammaHeatmapCanvas.tsx:540-611`),
   then draws candles on top (`:749-802`). The orb layer can be painted the
   same way beneath the SVG chart, so the SVG keeps only candles, levels and
   hit targets. Recommended for the full-session view.
2. **SVG with memoised paths.** Group orbs per strike into one `<path>` of
   arcs (one node per strike row, ~30 nodes) and memoise on
   `(buckets, layout)`. Fine for the live default view; degrades for a
   full 390-column session at 1m.

"Orbs" vs "Orbs V2" in the screenshot are glyph parameterisations (radius
from |gex| vs from OI, opacity from the per-strike expiration share, which
`web:core/expirationGradient.ts` already computes for the rail bars). They
are settings on the same layer, not separate features.

---

## 4. The toggles: GEX now, VEX next, Delta needs new columns

* **GEX** — `net_gex`, `call_gamma`, `put_gamma` per strike/minute: served
  by both feeds today.
* **VEX** — `vanna_exposure` and `dealer_vanna_exposure` are stored per
  strike/expiration/minute (`setup/database/schema.sql:590`, `:629`) and
  already appear on `/api/gex/by-strike` rows (`src/api/main.py:724-726`),
  but neither `/api/replay/range` nor the strike-profile timeseries selects
  them (`src/api/database.py:2898+`; `web:hooks/useStrikeProfileTimeseries.ts:31-38`).
  Adding them is a SELECT-list and Pydantic-model change with full history
  from day one. **GEX+VEX** is then client-side addition, with the caveat
  that the two are different units (`schema.sql:632-637` documents "$ per
  1 % spot" vs "$ per 1 vol point"); a combined view should normalise or
  show them side by side rather than sum them silently.
* **Delta (DEX)** — not computed per strike anywhere. `option_chains` stores
  per-contract `delta` (`schema.sql:129`), so Σ delta × OI × 100 × S per
  (strike, expiration) can be added next to the gamma sum in
  `_calculate_gex_by_strike` (`src/analytics/main_engine.py:1223`), one new
  column in `gex_by_strike`, and the INSERT at `:2999-3011`. History would
  start the day it ships; a backfill is possible only inside retention,
  from `option_chains`, and would be a one-off job. The delta-pressure
  signal would also stop falling back to its distance proxy
  (`src/signals/basic/dealer_delta_pressure.py:85`).

---

## 5. Timeframes 3m, 4H, 1W

Bars come from `underlying_quotes` bucketed by the literal in
`_BUCKET_EXPRS` (`src/api/queries/_sql_helpers.py:63-74`; the three
allowlist maps sit just above it), capped at 576 buckets
(`src/api/database.py:7114`, `get_historical_quotes`). Adding 3min / 4hr /
1week means adding the three map entries, extending the `Literal[...]`
parameter lists in `src/api/main.py` (`:794`, `:817`, `:1678`, `:1806`,
`:1934`, `:1945`, `:1988`) and, because `_VIEW_SUFFIXES` implies
per-timeframe views, checking whether a matching view is expected. The
cheaper alternative is to fold on the client from 1min / 1hr / 1day, which
the replay scrubber already does for 5-min candles
(`web:app/replay/[symbol]/[date]/ReplayScrubber.tsx:288-309`).

Orbs on 1D / 1W are a different question: the strike band is relative to
each session's spot and the analytics grid is per minute, so a multi-week
orb surface would need a per-day fold with a moving band and a much larger
payload. Recommend orbs for 1m → 1H (which is also all the screenshot
shows) and levels-only on 1D / 1W.

---

## 6. The right-hand panel and the axis tags

The three ladders are `PairGammaHeatmap`'s column generalised from two to N
(`web:components/PairGammaHeatmap.tsx:31-36` notes the column already ships
standalone as `GammaLadder`). Each column is one `useGEXByStrike` subscription
(`/api/gex/by-strike`, ≤ 200 strikes, `sort_by=distance|impact`,
`src/api/main.py:706-733`) plus the summary's `max_gamma_strike` for the
"King" row. Display "SPXW" as the label for the `SPX` chain if that wording
is wanted; the data is the weekly chain already.

The per-strike axis tags ("GP $5.43B · 6/26 · 750.00") are the top-N
|net_gex| strikes of the current bucket with the dominant expiration from
`include_expirations` shares (`src/api/routers/replay.py:253-262`,
`web:core/expirationGradient.ts`). The chart already staggers axis labels
so they do not collide (`web:app/replay/[symbol]/[date]/levelStagger.ts`,
tested by `web:tests/levelStagger.test.ts`).

---

## 7. Recommended build order

1. **Orb layer, GEX, live + today's rewind** — new canvas/SVG layer in
   `GammaTerminalChart`, fed by `/api/replay/range` (today) + the existing
   tip poll; style toggle (orbs / heat); expiration filter reuses the
   shared selector. Frontend only. This alone is the screenshot's core.
2. **Dated replay inside the chart** — date picker over
   `/api/replay/sessions`, load `/api/replay/range` for the chosen date,
   drive the existing rewind clock and growing-candle logic from it, add
   the 1/2/5/10-minute step sizes to the transport. Frontend only.
3. **Multi-ladder panel + axis tags** — N-column ladder, per-strike tags with
   expiration. Frontend only.
4. **VEX / GEX+VEX** — add vanna columns to the replay range and timeseries
   selects and models; frontend toggle. Small backend change, full history.
5. **DEX** — per-strike delta exposure in analytics + schema + payloads;
   history starts at ship. Backend change with a migration.
6. **3m / 4H / 1W** — allowlist entries or client fold.

Each step ships independently and none blocks the previous one.

---

## 8. Risks and things to decide up front

* **Do not route the orbs through `/api/gex/strike-profile-timeseries`.** It
  is the endpoint that blanked every chart on Aug 21 and is still
  throttled; a session-wide orb view polling it would recreate the
  stampede. The LATERAL session read exists precisely because of this.
* **Payload size.** A 1-min SPX session at ±4 % is ~390 frames × ~50
  strikes; the `/replay/[date]` page already loads it in one round trip.
  Cache past dates at the BFF (immutable) and keep today's reload at once
  per minute.
* **Glyph density on mobile.** Prefer the canvas underlay; the SVG path
  variant is acceptable only for the default 90-bar window.
* **Units in "GEX+VEX".** Decide whether the combined toggle normalises,
  stacks, or simply overlays two glyph colours; summing them is not
  meaningful (`schema.sql:632-637`).
* **Delta history.** DEX has no past until the column ships; say so in the
  UI rather than drawing an empty band.
* **Public delayed mode.** `/chart` serves a frozen 15-minute snapshot to
  non-members (`web:app/chart/page.tsx:9-17`); the orb layer should either
  be members-only or draw from the same frozen snapshot so no real-time
  per-strike data leaks over the public path.

---

## Sources (all in-repo)

* `setup/database/schema.sql` — `gex_by_strike` (`:577-637`), `gex_summary`
  king column (`:445`), `option_chains.delta` (`:129`)
* `src/analytics/main_engine.py` — per-strike computation and write
  (`:1223`, `:2263-2265`, `:2999-3011`)
* `src/api/main.py` — by-strike (`:706`), heatmap (`:813`),
  strike-profile timeseries (`:847`), market historical (`:1668`)
* `src/api/routers/replay.py` — sessions (`:177`), frame (`:207`),
  range (`:245`), diff (`:396`)
* `src/api/database.py` — `get_gex_frames_for_session` (`:2898`),
  `get_historical_quotes` (`:7114`)
* `src/api/queries/_sql_helpers.py` — timeframe allowlists (`:63`)
* `src/config.py` — retention (`:600`), strike band (`:1802-1803`),
  analytics interval (`:1806`)
* `docs/runbooks/strike_profile_timeseries_stampede.md`,
  `docs/runbooks/ingestion_symbol_gap.md:62`
* `zerogex-web/frontend`: `components/GammaTerminalChart.tsx`,
  `components/GammaHeatmapCanvas.tsx`, `components/PairGammaHeatmap.tsx`,
  `app/replay/[symbol]/[date]/ReplayScrubber.tsx`,
  `hooks/useStrikeProfileTimeseries.ts`, `hooks/usePairReplay.ts`,
  `core/symbols.ts`, `core/expirationGradient.ts`
