# ZeroGEX API Endpoints Reference

Complete reference for all currently available API endpoints.

Base URL: `http://your-server:8000`

**Two versions are served.** New integrations should build on **v2**; v1 is
unchanged and stays supported.

| | Path | Response shape |
| --- | --- | --- |
| **v2** (recommended) | `/api/v2/...` | `{"data": <v1 body>, "freshness": {...}}` |
| **v1** (stable) | `/api/...`, `/api/v1/levels/...` | the bare body |

Every v2 response carries a **freshness envelope** that separates endpoint
health, response evaluation time, and underlying data age into distinct
machine-readable fields. See
[API versions & the freshness envelope](#api-versions--the-freshness-envelope).

Endpoints below are documented at their v1 paths. To call any of them on
v2, replace the leading `/api` (or `/api/v1`) with `/api/v2` and read the
payload from `data`.

---

## Authentication

**Every caller must send its own key.** Use the Bearer scheme on every
request:

```
Authorization: Bearer <your-key>
```

`X-API-Key: <your-key>` is also accepted (read directly from request
headers) for backward compatibility with callers that haven't migrated
to Bearer yet. New integrations should use Bearer — it is the only
scheme advertised in the OpenAPI spec, the only one shown in Swagger's
Authorize modal, and the only one not subject to reverse-proxy header
rewrites at any layer.

Requests with an invalid or missing key return `401 Unauthorized` with
`WWW-Authenticate: Bearer`.

### `?api_key=` on the levels routes only

`/api/v1/levels/*` and `/api/v2/levels/*` — and **nothing else** — additionally
accept the key as an `api_key` query parameter:

```
GET /api/v1/levels/ES?strikes=1&api_key=<your-key>
```

This exists for charting platforms that physically cannot send a request
header. The Sierra Chart study is the caller it was added for: the ACSIL HTTP
call that is portable across Sierra Chart versions, `sc.MakeHTTPRequest(URL)`,
is a bare GET with no header support, so a header-only endpoint is unreachable
from that platform.

**Use a header if you can.** A credential in a URL is materially weaker: every
proxy in the path sees it, access logs record it by default (ours redact it —
see `deploy/steps/120.nginx_api`), and it survives in `Referer`. A header
always wins when both are present, so a stale URL parameter cannot downgrade a
caller that sends one.

The allowlist is deliberately narrow and is pinned by
`tests/test_api_query_key_auth.py`: the levels endpoints return derived,
redistributable analytics only, and no endpoint serving raw per-contract
quotes, flow, or key administration will accept a credential in a URL.

Two key types are supported, validated against the same headers:

- **Per-user keys** *(primary)* — long-lived keys issued via the admin
  CLI and stored hashed (SHA-256) in the `api_keys` table. Each request
  authenticates as a specific `user_id`, and individual keys can be
  revoked without affecting others. Every human or integration that
  hits the API directly should have its own key. The website's
  Next.js server holds its own key (`user_id=zerogex-web`) and sends
  it on every API call.
- **Shared static key** *(break-glass)* — set via the `API_KEY` env
  var on the server. No per-user attribution. Every successful match
  is logged at WARNING with the caller's IP so stragglers can be
  identified. Kept only for ops emergencies and bootstrap; will be
  removed from `.env` once every caller has its own per-user key.

### Swagger UI

Open `https://api.zerogex.io/docs`, click **Authorize** in the top right,
paste your per-user key into the `HTTPBearer` field, click Authorize,
then "Try it out" any endpoint. The key is sent on every subsequent
request from that browser tab.

### Provisioning per-user keys

Run the admin CLI from the server (uses the same DB credentials as the
API). The raw key is printed exactly once — copy it then.

```bash
# Easiest: via Make
make api-keys-create USER=alice@example.com NAME=alice-laptop
make api-keys-list
make api-keys-list USER=alice@example.com
make api-keys-revoke ID=7

# Or directly
python -m src.api.admin_keys create alice@example.com --name "alice-laptop"
python -m src.api.admin_keys list [--user-id alice@example.com]
python -m src.api.admin_keys revoke 7
```

Revocations take effect within the cache TTL (default 60s, controlled by
`API_KEY_CACHE_TTL_SECONDS`). Restart the API to invalidate immediately.

When neither `API_KEY` is set nor any keys exist in the `api_keys` table,
authentication is disabled — appropriate only for local development/CI.

### End-user attribution (website-proxied requests)

The website's Next.js server calls this API with a single shared per-user
key (`user_id=zerogex-web`) on behalf of every logged-in human, so the
caller identity alone can't say *which* end-user a request is for. An
optional second factor closes that gap: the website mints a short-lived
signed token naming the end-user and sends it in an extra header
**in addition to** (never replacing) the existing `Authorization: Bearer`:

```
X-End-User-Token: <JWT>
```

**Token contract (the verifier is strict):**

- Standard JWT, **`alg=HS256` only**. Any other algorithm — including
  `none` — is rejected (algorithm-confusion guard).
- Header: `{"alg":"HS256","typ":"JWT"}`.
- Claims: `sub` (required) — the website's stable, opaque internal
  account id, non-empty after trimming, ≤ 256 chars (it lands in audit
  logs, so it must not be email/PII); `exp` (required, epoch seconds —
  a missing `exp` is rejected); `iat` (recommended, epoch seconds).
- Signature: HMAC-SHA256 over `base64url(header).base64url(payload)`
  using the raw secret string's UTF-8 bytes as the key (the secret is
  **not** base64-decoded first), base64url without padding — exactly
  what any standard JWT library produces by default.

**Secret & tunables (server env):**

- `END_USER_TOKEN_SECRET` — shared high-entropy string, byte-identical
  to the website's `ZEROGEX_END_USER_TOKEN_SECRET`. **Unset ⇒ attribution
  disabled ⇒ callers authenticate exactly as before.** Generate with
  `python3 -c 'import secrets; print(secrets.token_urlsafe(48))'`.
- `END_USER_TOKEN_LEEWAY_SECONDS` (default 60) — clock-skew tolerance.
- `END_USER_TOKEN_MAX_AGE_SECONDS` (default 900) — the honored lifetime
  is hard-capped at this many seconds from `iat`, regardless of `exp`.

**Fail-open / purely additive.** Verification is pure crypto: it never
touches the DB and never raises into the request path. No token, no
secret configured, or any invalid/expired/forged token simply means
"no end-user" — the request still authenticates as the caller and
returns its normal `200`. A bad token never turns a `200` into a `4xx`.

**Consuming the identity.** Handlers can depend on the resolved identity:

```python
from src.api.identity import RequestIdentity, current_identity
from fastapi import Depends

@app.get("/api/example")
async def example(identity: RequestIdentity = Depends(current_identity)):
    # identity.caller_kind in {"static","db","anonymous"}
    # identity.caller_user_id, identity.end_user_id, identity.end_user_source
    # identity.subject -> "end_user:<id>" | "caller:<id>" | "caller_kind:<k>"
    ...
```

**Audit trail.** Every request emits one structured line on the
`src.api.audit` logger (into the journal, under `zerogex-oa-api`):

```
api_request method=… path=… status=… client_ip=… caller_kind=…
caller_user_id=… caller_key_id=… caller_name=… end_user_id=… duration_ms=…
```

Values are always single whitespace-free tokens — internal whitespace is
collapsed to `_` and absent values render as `-` — so the line stays
parseable with `grep`/`awk` straight out of `journalctl`.

This is the **only** record that ties a key to a request: nginx's access
log is deliberately credential-free (its `zerogex_scrubbed` format logs no
key and rewrites any `?api_key=` to `REDACTED`), so `client_ip` here is
what makes "which key is this IP using" answerable. `caller_key_id` and
`caller_name` say *which* of an owner's keys was used, which is what a
rotation or revocation has to target.

To read it, use `make api-caller-report` (see
`src/tools/api_caller_report.py`), which joins these lines against the
access log's User-Agents and enriches them from the `api_keys` table:

```bash
make api-caller-report IP=23.115.8.132        # who is this IP?
make api-caller-report USER=alice@example.com HOURS=12
make api-caller-report UA=NT8 JSON=/tmp/callers.json
```

Until the API restarts, those lines carry no `client_ip` and the tool falls
back to inferring who owns each address: requests that pair unambiguously on
(second, method, path, status) vote for (caller, address), and an address is
awarded only on a decisive majority. Expect most requests to drop on a busy
window — popular paths collide constantly and cannot vote — and treat the
result as a heuristic: it never sees a request that skipped nginx, and the
website BFF talks to uvicorn at `127.0.0.1:8000` directly
(`deploy/API_BEHIND_CLOUDFLARE.md`), so its calls have no access-log row at
all. **Restart the API to get real attribution** — the fallback is a stopgap
for reading history that was already written, not a substitute.

Note that `client_ip` is the real client address only because uvicorn's
`ProxyHeadersMiddleware` rewrites it from `X-Forwarded-For` (on by default,
trusting `127.0.0.1`, which is where nginx proxies from). Serving the API
without that proxy in front would record the proxy's own address instead.

`status=0` on a line means the handler raised and no response had started
when the audit ran — the client still receives the `500` that Starlette's
outer error middleware synthesizes. Ordinary 4xx/5xx responses (including
`HTTPException`) record their real status.

**Identity-keyed rate limiting.** A global dependency (`src.api.ratelimit`)
can throttle per end-user (falling back to caller, then client IP — see
`rate_limit_key`). It is **off** by default; `END_USER_RATE_LIMIT_ENABLED=1`
turns on **log-only** mode (counts and logs `WOULD-BLOCK`, never rejects),
and additionally `END_USER_RATE_LIMIT_ENFORCE=1` returns `429` with
`Retry-After` over the limit (`END_USER_RATE_LIMIT_REQUESTS`,
`END_USER_RATE_LIMIT_WINDOW_SECONDS`). The counter is an in-memory
fixed-window map, so the limit is **per worker** — adequate for a
smoke/observability rollout; the multi-worker scale-out path is
`slowapi` + Redis with the same `rate_limit_key` derivation ported
unchanged.

---

## Scopes, tiers & what's redistributable

Every endpoint declares a capability **scope** (`src/api/scopes.py`); each
key is provisioned with a **tier** bundle that grants a set of scopes.
Enforcement is opt-in (`API_SCOPE_ENFORCEMENT`) and a wildcard `*` key
always passes, so these declarations are inert until keys are backfilled.

| Scope | Covers | Redistributable? |
| --- | --- | --- |
| `gex` | GEX summary / by-strike / profile, walls, flip term-structure & surface, vol & premium surface, replay, **`/api/market/open-interest`**, and **`/api/v1/levels`** | ✅ derived |
| `flow` | options-flow aggregates, forced flow | ✅ derived |
| `maxpain` | max-pain analytics | ✅ derived |
| `technicals` | VWAP / ORB / volume / momentum | ✅ derived |
| `signals` | signal engine, backtest, scorecard, forecast, TradeWorkz | ✅ derived (premium) |
| `market_reference` | the underlying's own tape — `/api/market/quote`, `/api/market/historical`, `/api/market/session-closes`, `/api/market/session-levels` | ✅ reference |
| `market_raw` | per-contract **quoted prices** (bid/ask/last/mid) — `/api/option/*`, `/api/tools/option-calculator` | ❌ **withheld** |

Tier bundles (the unit of commercial packaging):

- **`analytics`** — `gex` + `flow` + `maxpain` + `technicals` +
  `market_reference`: the clean derived product for external / B2B2C
  consumers. **No option chain, no signals.**
- **`signals`** — `analytics` + `signals`.
- **`full`** — everything *including* `market_raw`; the internal website
  backend only, never resold.

`market_raw` is isolated precisely so it can be granted to the internal
BFF and **withheld from every external customer**. The line is drawn at
whether a payload carries a **quoted price**: bid, ask, last or mid for an
individual contract is withheld, while a computed output and the reference
price a level is drawn against are not. Open interest sits on the derived
side — it is dealer-positioning input, carries no quote, and the same
per-strike figures already ship under `gex` via `/api/gex/by-strike`. A third-party charting integration
is issued an **`analytics`-tier key** and has everything it needs to place
a level against a price.

## API versions & the freshness envelope

### Why v2 exists

v1 answers "what is the data" but leaves "how current is it" to be
inferred from whichever `timestamp`-shaped field an endpoint happened to
carry — and those fields mean different things on different endpoints. A
consumer could not distinguish, from a v1 response alone:

* the API answered promptly but the feed behind it is stalled, from
* the API answered promptly and the market is simply closed, from
* the data is genuinely current.

**v2 makes those separate, machine-readable facts on every endpoint.** It
is not a rewrite: `data` is byte-for-byte the v1 body, including every
event timestamp v1 already carried. Migrating is "unwrap `data`".

```
GET /api/v2/levels/SPY

{
  "data": { ...exactly the /api/v1/levels/SPY body... },
  "freshness": {
    "evaluated_at": "2026-08-20T14:32:07.881Z",
    "generated_at": "2026-08-20T14:31:42.010Z",
    "source_timestamp": "2026-08-20T14:31:38.500Z",
    "latest_event_at": "2026-08-20T14:31:38.500Z",
    "age_seconds": 29.381,
    "market_session_status": "regular",
    "expected_update_cadence": "PT1M",
    "expected_update_cadence_seconds": 60.0,
    "cadence_profile": "analytics_cycle",
    "stale_after": "2026-08-20T14:34:08.500Z",
    "freshness_status": "fresh"
  }
}
```

### Envelope fields

| Field | Concept | Meaning |
| --- | --- | --- |
| `evaluated_at` | **endpoint health** | When the API evaluated *this response*. Advances on every request even when the data is frozen — so "the API is answering" is observable independently of "the data is moving". Always present. |
| `generated_at` | **compute time** | When the served data was computed. The snapshot's own stamp for cycle-backed endpoints; equal to `evaluated_at` for endpoints computed on demand. |
| `source_timestamp` | **data freshness** | The upstream market observation the payload derives from. This is what `freshness_status` is measured against. |
| `latest_event_at` | **data freshness** | Newest event timestamp in the payload (last bar, last trade, last scored row). Usually equals `source_timestamp` for a single snapshot; for a series it is the last row. |
| `age_seconds` | derived | `evaluated_at − source_timestamp`. |
| `market_session_status` | **context** | `pre-market` \| `regular` \| `after-hours` \| `closed` (US/Eastern). Same vocabulary as the `session` field on `/api/market/quote`. |
| `expected_update_cadence` | **contract** | ISO-8601 duration for how often this endpoint's data is expected to change **right now**. `null` means no update is due. |
| `expected_update_cadence_seconds` | **contract** | The same value as a number, so clients need no duration parser. |
| `cadence_profile` | **contract** | The cadence class this endpoint belongs to (table below). Stable across releases; the seconds behind it may be retuned. |
| `stale_after` | **contract** | The instant after which this payload should be treated as stale. Published so you set a timer instead of guessing a threshold. `null` when no update is due. |
| `freshness_status` | **verdict** | The rolled-up read (below). |

Every field is present on every v2 response — v2 never omits a
null-valued key, so you can index the envelope unconditionally.

### `freshness_status`

| Value | Meaning | Should you alert? |
| --- | --- | --- |
| `fresh` | `source_timestamp` is within one expected cadence. | No |
| `aging` | Past one cadence, not yet past `stale_after` — the grace band absorbing normal cycle jitter. Seen routinely when you poll faster than the cadence. | No |
| `stale` | Past `stale_after` **while an update was due**. The feed behind this endpoint is late. | **Yes** |
| `session_closed` | A feed-backed endpoint whose feed is not due to produce anything: a weekend, an NYSE holiday, an early-close afternoon, or the overnight gap (ingestion runs 04:00–20:00 ET). The payload is the last good value. | No |
| `static` | Not feed-backed at all: completed history, or a result computed from your own inputs. Age carries no health meaning. | No |
| `unknown` | No source timestamp could be resolved (an empty result set, a pure calculator). Fall back to `evaluated_at` for health; make no claim about data age. | No |

The `session_closed` / `stale` split is the point of carrying
`market_session_status`: an overnight consumer polling a closed market is
not observing a fault, and a flat "data is 14 hours old" would page every
weekend.

### Cadence profiles

Cadence is **session-dependent**. `expected_update_cadence` already
reflects the current session, so prefer reading it over hard-coding from
this table. Weekends and NYSE holidays always report `null` — nothing
upstream can change.

| Profile | Endpoints | Regular | Extended | Overnight |
| --- | --- | --- | --- | --- |
| `realtime_quote` | `/api/market/quote` and the rest of `/api/market/*` | 60 s | 60 s | — |
| `option_chain` | `/api/option/*`, `/api/market/open-interest` | 60 s | 60 s (to 16:15 only) | — |
| `volatility_bar` | `/api/market/volatility` (VIX, VXN) | 5 min | 5 min | — |
| `analytics_cycle` | `/api/gex/*`, `/api/v1/levels`, `/api/max-pain/*`, `/api/forced-flow/*`, `/api/technicals*` | 60 s | 60 s | — |
| `flow_aggregate` | `/api/flow/*` | 5 min | — | — |
| `signals_cycle` | `/api/signals/*` (incl. `trades-live`), `/api/tradeworkz/*` | 60 s | 60 s | — |
| `daily_cycle` | `/api/forecast*`, `/api/scorecard*`, `/api/news*`, session closes & levels | one per trading session | | |
| `historical` | `/api/replay/*`, `/api/backtest/*`, `/api/gex/historical`, `/api/market/historical`, `/api/signals/trades-history`, `/api/signals/{signal_name}/events` | — | — | — |
| `on_demand` | `/api/tools/*`, `/api/health*` | — | — | — |

A dash means no update is expected, which surfaces as
`freshness_status: session_closed` (feed-backed profiles) or `static`
(the rest).

**Every feed-derived profile reports no cadence overnight.** Ingestion runs
04:00–20:00 ET; between 20:00 and 04:00 the analytics engine may still tick
but it recomputes the same 20:00 observation, so an ageing payload there is
correct rather than late. The same holds on weekends, NYSE holidays, and
after the 13:00 ET close on an early-close day.

**`option_chain` is narrower still: 09:30–16:15 ET.** A chain row is written
when an option quote ticks, and options trade only during the cash session
plus the 15-minute late session — while the underlying bar feed runs
04:00–20:00. So `/api/option/*` and `/api/market/open-interest` report
`session_closed` from 16:15 to 09:30 the next morning even though
`/api/market/quote` beside them is still updating, and even though
`market_session_status` still reads `pre-market` or `after-hours`. Those two
fields answer different questions: the session label is what the *equity
market* is doing, and `freshness_status` is whether *this endpoint's* feed
owes you an update. Two narrower cases the calendar also handles: cash-index
chains (SPX, NDX) stop at 16:00 with the index they price, and every chain
stops at the early close on a half day.

Cadence describes how often a new observation can be **stored**, not how
often ingestion polls, and not how fast the producing engine loops. The quote
tape is polled every few seconds but written in 60-second buckets, so 60 s is
the fastest a new value can appear. VIX/VXN and the whole of `/api/flow/*` are
5-minute bars. The signal engine loops about once a second, but a score is
stamped with the underlying-quote timestamp it read, so it cannot be fresher
than that same 60-second bucket.

Poll faster than the cadence if you like — it is cheap against the cache — but
expect `aging` between stores. That band is normal, not a warning.

**ES and NQ are graded on the CME calendar**, not the NYSE one — they trade
Sunday 18:00 to Friday 17:00 ET. A futures symbol therefore reports
`market_session_status: regular` (and a real `stale` verdict) through the
overnight hours when the cash market is shut, so a stalled futures feed is
visible rather than hidden behind `session_closed`. Cash symbols are
unaffected.

`stale_after` is anchored to the later of `source_timestamp` and the instant
the current feed window opened, so a payload one second into a new window
gets a full grace period before it is called late — nothing can be late
before anything has had time to arrive. `age_seconds` still measures the true
age from the observation.

`daily_cycle` endpoints age in **trading sessions, not wall-clock hours**:
Friday's session close is the correct answer all through Monday morning, and
`stale_after` lands after the *next* session's artifact is due.

Source of truth: `ENDPOINT_CADENCE` and the `CadenceProfile` definitions in
`src/api/freshness.py`. Cadence numbers are read from the same config
constants the engines run on (`ANALYTICS_INTERVAL`,
`MARKET_HOURS_POLL_INTERVAL`, `AGGREGATION_BUCKET_SECONDS`), so retuning a
poll interval changes what the envelope advertises.

### Calendar configuration

`NYSE_HOLIDAYS` and `NYSE_HALF_DAYS` (both comma-separated ISO dates, both
honouring `NYSE_HOLIDAYS_STRICT`) drive the session model. An unset
`NYSE_HALF_DAYS` means early closes are graded as full sessions, which
reports `stale` for the three hours after a 13:00 ET close — keep it
populated.

### Response headers

Every v2 response also carries the envelope as headers, so a proxy, CDN or
uptime monitor can act on staleness without parsing a body. All of them are
listed in `Access-Control-Expose-Headers`, so a cross-origin browser client
can read them too (alongside `X-Request-Id` for correlation):

```
X-Freshness-Status: fresh
X-Freshness-Evaluated-At: 2026-08-20T14:32:07.881+00:00
X-Freshness-Source-Timestamp: 2026-08-20T14:31:38.500+00:00
X-Freshness-Age-Seconds: 29.381
X-Freshness-Stale-After: 2026-08-20T14:34:08.500+00:00
X-Freshness-Expected-Cadence: PT1M
X-Freshness-Cadence-Profile: analytics_cycle
X-Market-Session: regular
```

### Migrating from v1

1. Change the path: `/api/gex/summary` → `/api/v2/gex/summary`;
   `/api/v1/levels/SPY` → `/api/v2/levels/SPY`. One version segment,
   always in the same position.
2. Read your existing payload from `data` — unchanged, field for field.
3. Optionally drop your own staleness heuristics in favour of
   `freshness_status` / `stale_after`.

Auth, scopes, tiers and rate limits are **identical** on both versions: a
v2 route carries exactly the scope gate of the v1 route it mirrors, and the
no-auth health probes are public on both.

> **Before proxying v2 through the website BFF.** The consumer tier gate in
> `zerogex-web` (`core/api/apiTierGate.ts`) matches literal `/api/...`
> prefixes and is deliberately **fail-open**, and FastAPI enforces no
> per-member entitlement — that gate is the entire browser paywall. A
> `/api/v2/[...rest]` proxy added before the gate normalizes the version
> segment would leave every premium prefix unmatched, and therefore
> ungated. Normalize `^/api/v\d+/` to `/api/` there first.

`data` is serialized with **whichever encoder v1 used for that route**, so
numeric types and timestamp formats inside `data` are identical to v1 —
`Decimal` stays a JSON number, and a route that emitted `+00:00` still
emits `+00:00`. The `freshness` block always uses the same ISO-8601 `Z`
form on every route, so you parse one format for it regardless of endpoint.

Deliberate v1 → v2 differences:

* **No key is omitted.** v1's `/api/market/quote` drops null fields; v2
  always emits every declared field, on both `data` and `freshness`.
* **CSV downloads are not wrapped.** `/api/v2/backtest/runs/{id}/trades.csv`
  and the TradeWorkz audit export stream CSV unchanged; their freshness
  data arrives in the `X-Freshness-*` headers.
* **Errors are not wrapped.** A v2 error is a v1 error — same status, same
  `{"detail": ...}` body. You do not need a second error parser.
* **Control-plane surfaces are not mirrored.** `/api/admin/*`,
  `/api/tradeworkz/admin/*` (operator-only) and
  `/api/tradeworkz/internal/*` (drained by a systemd timer) stay v1 —
  they mutate state, no customer versions against them, and "how fresh is
  this data" is meaningless for them.

### Update cadence (background)

The derived analytics (GEX, walls, flip, max pain, per-strike profile) are
recomputed on the **~60-second analytics cycle** and served from a short
TTL cache (GEX summary ~1.5 s; by-strike / profile ~5 s). This is
**snapshot-poll, not a stream** — clients re-issue the GET at whatever
cadence they want (1–5 s is effectively free against the cache). The only
realtime push channel, `WS /ws`, carries **underlying quotes only**; it
does not carry GEX / wall / flip. The WebSocket is not mirrored on v2:
each pushed message carries its own stamp.

Levels and summary responses carry the snapshot time so a consumer can
reason about staleness; `/api/v1/levels` additionally returns
`age_seconds`. A future delayed-vs-live tier split (free = delayed / EOD,
paid = realtime) is a timestamp gate on these fields, not a streaming
change.

### ES / NQ and the basis a response is projected on

ES and NQ are answered from the SPX / NDX option chains — ZeroGEX never
computes gamma from options on futures. ES and SPX track the same index, so
it is the same dealer book; only the price axis differs, by cost of carry.
Every price-space field (strikes, walls, flip, max pain, pin, spot) is carried
onto the futures axis by the **basis**, and rounded to the contract tick.
Dollar exposures (net GEX, wall strength, OI, volume) are **not** rescaled —
exposure belongs to the option book, not to the axis you plot it on.

Which basis is used depends on what you asked for, and this matters if you
are backtesting:

| Request | Basis applied |
| --- | --- |
| Live (no time named) | measured off the current tape |
| Pinned to an instant (`ts=`) | the basis in force at that instant |
| Pinned to a session (`date=`, `end_date=`) | the basis in force that session |
| A timestamped series (e.g. `/api/gex/historical`) | **per row**, each on its own session's basis |

A series is projected row by row rather than under one ratio because basis
walks down through each quarterly cycle toward expiry. Over a single session
that drift is ~0.003% — below the tick a level is published at. Over a quarter
it is ~0.5%, which on ES is tens of points: enough to move every level in a
backtest without anything in the payload indicating it. Each row therefore
carries its own `projection` block naming the ratio it was projected on.

Historical responses also keep their **projected** spot rather than taking the
live futures print — today's price stamped on a past frame would state
something false about that frame.

> **Authoritative source.** This guide is the curated derived/charting
> surface. The live, complete, always-current endpoint list is the
> OpenAPI schema at `/openapi.json` (rendered at `/docs`); when the two
> disagree, the schema wins.

---

## Health & Status

### GET /api/health
Check API and database health.

---

## Consolidated Levels (v1) — recommended for charting integrations

Scope: `gex` (the `analytics` tier). Derived, redistributable.

### GET /api/v1/levels/{symbol}

The stable, **versioned** contract that bundles the headline
dealer-positioning levels and the per-strike gamma profile into one
response — the surface third-party charting integrations (a TradingView
Charting Library widget, a NinjaScript indicator, embeddable partner
widgets) should build on. The field names here are a committed contract;
the internal `/api/gex/*` models can change without breaking integrations.
It is a thin re-shape of `/api/gex/summary` + an across-expiration
aggregate of `/api/gex/by-strike`, so a consumer needs one call, not two.

**Path parameters:**
- `symbol` — underlying, e.g. `SPY`, `SPX`, `QQQ` (1–16 chars, `[A-Za-z0-9.^-]`).

**Query parameters:**
- `strikes` (optional): number of strikes nearest to spot to include in the
  gamma profile, aggregated across expirations. Min `1`, max `200`,
  default `40`.

**Returns `404`** when no snapshot exists for the symbol yet.

**Example response:**
```json
{
  "symbol": "SPY",
  "spot": 676.04,
  "as_of": "2026-07-06T19:30:00Z",
  "age_seconds": 42,
  "net_gex_at_spot": -1200000000.0,
  "levels": {
    "gamma_flip": 675.0,
    "call_wall": 680.0,
    "put_wall": 670.0,
    "max_pain": 676.0,
    "pin_strike": 676.0
  },
  "pin_score": 1504000000.0,
  "pin_confidence": 0.31,
  "pin_strike_reason": null,
  "profile": [
    {"strike": 670.0, "net_gex": -500000000.0, "call_gex": 100000000.0, "put_gex": -600000000.0},
    {"strike": 676.0, "net_gex":          0.0, "call_gex": 200000000.0, "put_gex": -200000000.0},
    {"strike": 680.0, "net_gex":  400000000.0, "call_gex": 500000000.0, "put_gex": -100000000.0}
  ]
}
```

- `levels.*` draw as horizontal lines. Any field may be `null` when the
  engine can't resolve it (e.g. an unresolved gamma flip on a thin chain)
  — hide the line, don't render a `0`.
- `levels.pin_strike` is the **Pin Strike** — the reachable 0DTE strike with
  the strongest modeled positive (restoring) dealer gamma into expiration. It
  is a *distinct* metric, not a wall / flip / max-pain / king-node: it
  simulates spot AT each candidate strike, keeps only locally-concentrated
  positive gamma, and weights by the probability price reaches that strike
  before the 0DTE close. `null` when no meaningful pin exists (hide, don't
  zero) — then `pin_strike_reason` carries a code (`NO_0DTE_EXPIRATION`,
  `NO_POSITIVE_RESTORING_GAMMA`, `INSUFFICIENT_OPTION_DATA`,
  `INSUFFICIENT_IV_DATA`, `EXPIRED`, `PIN_SCORE_TOO_WEAK`).
- `pin_score` (raw max pin score = restoring gamma × reachability) and
  `pin_confidence` (its dominance over all viable pins, `0..1`) are top-level
  scalar metadata a client can use to classify pin strength; both `null` when
  there is no active pin.
- `profile` is ascending by strike (histogram order). `net_gex` is dollar
  gamma per 1% move, calls positive / puts negative, and
  `net_gex == call_gex + put_gex` by construction.
- `as_of` / `age_seconds` describe snapshot freshness — see *Data
  freshness & update cadence* above.

---

## GEX (Gamma Exposure)

Scope: `gex` (the `analytics` tier).

### GET /api/gex/summary
Get latest GEX summary with key metrics.

**Parameters:**
- `symbol` (optional): default `SPY`

Includes the nullable **Pin Strike** fields `pin_strike`, `pin_score`,
`pin_confidence` and `pin_strike_reason` (same semantics as the
`/api/v1/levels` fields above — hide, don't zero; a `REASON_*` code populates
`pin_strike_reason` when there is no active pin).

### GET /api/gex/by-strike
Get GEX breakdown by individual strikes.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `200`, default `50`

### GET /api/gex/historical
Get historical GEX data.

**Parameters:**
- `symbol` (optional): default `SPY`
- `start_date` (optional): ISO format datetime/date
- `end_date` (optional): ISO format datetime/date
- `window_units` (optional): max `90`, default `90`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`

### GET /api/gex/heatmap
Get GEX heatmap matrix (strike × time).

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `5min`
- `window_units` (optional): max `90`, default `60`

---

## Options Flow

Scope: `flow` (the `analytics` tier).

### GET /api/flow/by-contract
Per-contract option flow in 5-minute buckets with session-cumulative values.

**Parameters:**
- `symbol` (optional): default `SPY`
- `session` (optional): `current` | `prior`, default `current`
- `intervals` (optional): trailing N 5-minute buckets, `1`–`390`; omit for the full session

### GET /api/flow/series
Server-accumulated flow series — one row per 5-minute bar (cumulative call/put premium, volume, position, net volume, put/call ratio). Rows are newest→oldest.

**Parameters:**
- `symbol` (required): `[A-Z.]{1,10}`
- `session` (optional): `current` | `prior`, default `current`
- `strikes` (optional): comma-separated strikes to include; omit for all
- `expirations` (optional): comma-separated `YYYY-MM-DD`; omit for all
- `intervals` (optional): trailing N 5-minute bars, `1`–`390`

### GET /api/flow/contracts
Distinct strikes and expirations that traded in the resolved session (powers the Flow-page filter chips).

**Parameters:**
- `symbol` (required): `[A-Z.]{1,10}`
- `session` (optional): `current` | `prior`, default `current`

### GET /api/flow/smart-money
Unusual-activity / smart-money flow — 1-minute intervals (session 07:15–16:15 ET).

**Parameters:**
- `symbol` (optional): default `SPY`
- `session` (optional): `current` | `prior`, default `current`
- `limit` (optional): max `50`, default `50`


**Ranked, not chronological.** This returns the largest-notional prints of
the session ordered by size, so the newest `timestamp` among the rows is
whichever of the biggest prints landed last — usually the opening burst on
the index names. Do **not** take `MAX(timestamp)` over the rows as a
freshness signal; it will read a healthy midday response as hours stale.
Use `session_latest_at` instead, which every row carries: the newest flow
event in the whole session, not just among the rows returned. On v2 this is
already what the envelope grades, so `freshness.source_timestamp` and
`freshness.freshness_status` are correct without any special handling.

### GET /api/flow/buying-pressure
Underlying buying/selling pressure.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): `1`–`500`, default `20`

---

## Market Data

Scope: mixed — check each endpoint below.

The underlying's own tape (`quote`, `historical`, `session-closes`,
`session-levels`) is `market_reference` and rides with the `analytics`
tier, because placing a level on a chart is meaningless without the price
it sits against. `open-interest` is `gex` — it returns open interest and a derived
exposure, no quoted price, and the same per-strike figures ship under the
same scope via `/api/gex/by-strike`. Everything under `/api/option/`
returns per-contract quoted prices and is `market_raw` — **not
redistributable**, internal `full`-tier BFF only, excluded from the
`analytics` tier issued to external customers.

### GET /api/market/quote
Get latest underlying quote (the live tick).

**Parameters:**
- `symbol` (optional): default `SPY`

### GET /api/market/session-closes
Get the two most recently completed regular session closes (4:00 PM ET bars).

- `current_session_close` — last completed 4pm ET close. During market hours on a given day (before 4pm ET), this is the previous day's close; during after-hours or the following pre-market, it is that day's close.
- `prior_session_close` — the session close immediately before `current_session_close`.

**Parameters:**
- `symbol` (optional): default `SPY`

**Example response:**
```json
{
  "symbol": "SPY",
  "current_session_close": 676.04,
  "current_session_close_ts": "2026-03-11T21:00:00Z",
  "prior_session_close": 675.73,
  "prior_session_close_ts": "2026-03-10T21:00:00Z"
}
```

### GET /api/market/session-levels
Get pre-market and previous-session high/low levels — the chart's PM High/Low and Prev High/Low overlays.

Non-index symbols only (ETFs/equities such as SPY, QQQ). Cash indexes have no pre-market print and return `is_index: true` with null levels (HTTP 200, not 404).

- `premarket_high` / `premarket_low` — high/low of `trading_date`'s 04:00–09:30 ET pre-market session. Live-updating while the pre-market is in progress; final after the 09:30 open.
- `prev_session_high` / `prev_session_low` — high/low of `prev_session_date`'s regular session (09:30–16:00 ET; 13:00 close on NYSE half-days), including the closing auction print.
- Levels roll at the start of each new pre-market session (04:00 ET), not at the close.
- `source` — provenance: `captured` (the session-levels capture job), `live` (on-the-fly 1-min-bar aggregate fallback), or `captured+live` (pre-market union of both while the session is in progress).

**Parameters:**
- `symbol` (optional): default `SPY`

**Example response:**
```json
{
  "symbol": "SPY",
  "is_index": false,
  "trading_date": "2026-07-06",
  "premarket_high": 625.4,
  "premarket_low": 622.15,
  "prev_session_date": "2026-07-03",
  "prev_session_high": 626.28,
  "prev_session_low": 621.4,
  "source": "captured",
  "updated_at": "2026-07-06T13:55:04.412331+00:00"
}
```

### GET /api/market/historical
Get historical underlying quotes.

**Parameters:**
- `symbol` (optional): default `SPY`
- `start_date` (optional): ISO format datetime/date
- `end_date` (optional): ISO format datetime/date
- `window_units` (optional): max `90`, default `90`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`

### GET /api/market/open-interest
Current open interest per option contract from the most recent chain snapshot.

**Parameters:**
- `underlying` (optional): default `SPY`

### GET /api/option/quote
Most recent quote for a single option contract.

**Parameters:**
- `underlying` (optional): default `SPY`
- `strike` (optional): strike price
- `expiration` (optional): `YYYY-MM-DD`
- `type` (optional): `C` (call) or `P` (put)

**These read `option_chains`, so they keep the options session, not the
tape's.** `/api/market/open-interest`, `/api/option/quote` and
`/api/option/contract` are graded on the `option_chain` cadence profile:
09:30–16:15 ET (16:00 for SPX/NDX, the early close on a half day). Outside
that they report `session_closed`, not `stale` — no chain row can be written
when no option is trading, so the last snapshot before the close is the
correct answer all evening. `/api/market/quote` sits beside them on the wider
04:00–20:00 tape window and will still be updating; that difference is real,
not an inconsistency.

---

## Max Pain

Scope: `maxpain` (the `analytics` tier).

### GET /api/max-pain/timeseries
Get max pain over time (aggregated by timeframe).

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `5min`
- `window_units` (optional): min `1`, max `90`, default `90`

### GET /api/max-pain/current
Get current max pain with current underlying price, difference (`max_pain - underlying_price`), and per-expiration strike payout/notional grids.

**Parameters:**
- `symbol` (optional): default `SPY`
- `strike_limit` (optional): min `10`, max `1000`, default `200`

---

## Technicals

Scope: `technicals` (the `analytics` tier).

### GET /api/technicals
Combined per 5-minute bar timeseries of VWAP deviation, opening-range
breakout, unusual volume spikes (all classifications), and momentum
divergence — plus the underlying close — for the most recent session.

Session window depends on `symbols.asset_type`:
- `INDEX` → 09:30–16:00 ET (cash session only)
- otherwise (ETF, EQUITY) → 04:00–20:00 ET (extended hours)

Each bar is a 5-minute bucket; `timestamp` is the start of the bucket
(e.g. `10:30` → `10:30:00–10:34:59`). The bar aggregates whichever
1-minute underlying bars have landed in the bucket: `close` is the
latest 1-minute close, volumes are summed, `high`/`low` use max/min.
While the 5-minute window is still active the bar updates as new
1-minute bars arrive; once the window closes the bar becomes
immutable.

Cash indices use a proxy ETF's volume for VWAP and volume-spike stats
(SPX→SPY, NDX→QQQ, RUT→IWM, DJX→DIA); the active proxy is reported in
the response's `volume_proxy` field. Bars before 09:30 ET return null
opening-range fields (the ORB hasn't been established yet).

Dealer hedging is intentionally excluded — its underlying view is a
point-in-time snapshot, not a timeseries.

**Parameters:**
- `symbol` (optional): default `SPY`
- `intervals` (optional): trailing N 5-minute bars (1–192, where 192
  bars × 5 min = 16h covers the full extended ETF session). Omit for
  the full session. Tail anchors on the most recent existing bar —
  safe for live mid-session polling.

**Response shape:**
```json
{
  "symbol": "SPY",
  "asset_type": "ETF",
  "session_date": "2026-05-08",
  "session_start_et": "2026-05-08T04:00:00-04:00",
  "session_end_et": "2026-05-08T20:00:00-04:00",
  "volume_proxy": null,
  "bars": [
    {
      "time_et": "2026-05-08T04:00:00-04:00",
      "timestamp": "2026-05-08T08:00:00+00:00",
      "close": 737.62,
      "volume": 12500,
      "vwap_deviation": { "vwap": ..., "vwap_deviation_pct": ..., "vwap_position": ... },
      "opening_range": { "orb_high": null, "orb_low": null, ... },
      "volume_spike": { "current_volume": ..., "volume_sigma": ..., "volume_class": ... },
      "momentum_divergence": { "chg_5m": ..., "opt_flow": ..., "divergence_signal": ... }
    }
  ]
}
```

### GET /api/technicals/vwap-deviation
Get VWAP deviation for mean reversion monitoring.

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`
- `window_units` (optional): max `90`, default `20`

### GET /api/technicals/opening-range
Get opening range breakout status.

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`
- `window_units` (optional): max `90`, default `20`

### GET /api/technicals/gamma-levels
Get gamma exposure levels (support/resistance zones).

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `100`, default `20`

### GET /api/technicals/dealer-hedging
Get current dealer hedging pressure (point-in-time snapshot).
Returns at most one row per symbol — this is not a timeseries.

**Parameters:**
- `symbol` (optional): default `SPY`

### GET /api/technicals/volume-spikes
Get unusual volume spike events.

**Parameters:**
- `symbol` (optional): default `SPY`
- `limit` (optional): max `100`, default `20`

### GET /api/technicals/momentum-divergence
Get momentum divergence signals.

**Parameters:**
- `symbol` (optional): default `SPY`
- `timeframe` (optional): `1min`, `5min`, `15min`, `1hr`, `1day` (also accepts `1hour`), default `1min`
- `window_units` (optional): max `90`, default `20`

---

## Signals

Scope: `signals` (the `signals` tier — premium; not in the base `analytics`
bundle).

Signal endpoints surface the Market State Index composite, Advanced Signals
(triggered events with hysteresis), and Basic Signals (continuous
directional reads). Full per-endpoint field semantics, ranges, trader
interpretation, and page-design notes live in Swagger (`/docs`) — this
section is a path quick-reference.

Endpoints below are listed in alphabetical order — matching the Swagger
UI at `/docs`, which uses `operationsSorter: "alpha"` for the same
purpose.

### Composite & trades

- `GET /api/signals/score` — latest MSI composite score, regime label, component breakdown.
- `GET /api/signals/score-history` — time series of composite scores + contributions.
- `GET /api/signals/trade-bias` — latest **directional** Trade Bias: signed
  `bias_score` (−100..+100), `direction` (long/short/neutral), regime it started
  from, `state`, `confidence`, `setup` + `playbook`, `checklist`, and the raw
  `inputs`. Distinct from MSI (a directionless 0-100 state magnitude). Query
  `tenor=swing` (multi-day / structural, default) or `tenor=intraday` (0DTE;
  populated from a later phase). Computed by the Signals Engine each cycle.
- `GET /api/signals/trade-bias-history` — time series of the Trade Bias contract
  (params `tenor`, `limit`, `lookback_days`); newest first.
- `GET /api/signals/action` — Playbook Engine Action Card: single decisive trade
  instruction (or `STAND_DOWN`) fusing MSI regime + advanced/basic signals + live
  levels. See `docs/playbook_catalog.md` for the pattern catalog and Action Card
  schema. PR-2 ships the engine plus one canonical pattern (`call_wall_fade`);
  remaining patterns land in PR-3+.
- `GET /api/signals/trades-history` — realized trade ideas with P&L / hit rate.
- `GET /api/signals/trades-live` — open trade ideas derived from current signal state.

**Grade this on `last_refreshed_at`, not on the row timestamps.** Every row's
`signal_timestamp` and `opened_at` are the instant the position was *entered*,
so a position held since the open reads hours old at midday on a perfectly
healthy engine. The response carries a top-level `last_refreshed_at`: the
newest mark-to-market write across the open book, which the reconcile loop
bumps on every open position every cycle. It is `null` when the book is empty
— an engine holding nothing and a dead engine holding nothing produce the same
payload, so no claim is made. On v2 this is already what the envelope grades,
so `freshness.source_timestamp` and `freshness.freshness_status` are correct
without any special handling.

Unlike `trades-history`, this is a live view: it is graded on the
`signals_cycle` cadence, so a stopped signal engine reports `stale` rather
than `static`.

### Advanced Signals (7, triggered + hysteresis)

- `GET /api/signals/advanced/0dte-position-imbalance`
- `GET /api/signals/advanced/confluence-matrix` — N×N pairwise agreement over rolling lookback.
- `GET /api/signals/advanced/eod-pressure`
- `GET /api/signals/advanced/gamma-vwap-confluence`
- `GET /api/signals/advanced/range-break-imminence` — regime-switch (chop vs break) detector; emits `imminence` 0–100 and `label` (Range Fade / Weak Range / Break Watch / Breakout Mode).
- `GET /api/signals/advanced/squeeze-setup`
- `GET /api/signals/advanced/trap-detection`
- `GET /api/signals/advanced/vol-expansion`

### Basic Signals (6, continuous directional reads, weight=0)

- `GET /api/signals/basic` — bundle: latest snapshot of all six in one response.
- `GET /api/signals/basic/confluence-matrix` — 6×6 pairwise agreement over rolling lookback.
- `GET /api/signals/basic/dealer-delta-pressure` — estimated dealer net-delta imbalance (DNI).
- `GET /api/signals/basic/gex-gradient` — dealer gamma asymmetry above vs below spot.
- `GET /api/signals/basic/positioning-trap` — squeeze/flush risk from one-way crowding.
- `GET /api/signals/basic/skew-delta` — short-dated OTM put-vs-call IV deviation (fear gauge).
- `GET /api/signals/basic/tape-flow-bias` — signed option-tape premium imbalance.
- `GET /api/signals/basic/vanna-charm-flow` — second-order greek dealer-hedging pressure.

### Cross-cutting

- `GET /api/signals/{signal_name}/events` — per-signal time-series with direction-flip
  detection and forward realized returns. Accepts any of the 13 advanced/basic names.

**Common response shape (per-signal):**
- `underlying`, `timestamp` (ISO-8601 UTC).
- `clamped_score` ∈ `[-1, +1]`; `score` = `clamped_score × 100` ∈ `[-100, +100]`.
- `direction` ∈ `"bullish" | "bearish" | "neutral"`.
- `context_values` — signal-specific inputs/derived fields.
- `score_history` — up to 90 recent `{score, timestamp}` points, newest→oldest.

Returns `404` when a signal has no row yet for the symbol. Weight is `0.0`
for all Advanced and Basic Signals (they do not contribute to the MSI).

---

## Interactive API Docs

### GET /docs
Swagger UI.

### GET /redoc
ReDoc UI.

### GET /openapi.json
OpenAPI schema JSON.
