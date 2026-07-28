# Route Drift Audit — T0 Baseline

Systematic cross-check of implemented routes vs. documented routes vs.
prior-known routes as of `release@bd80bdd`. Answers the question:
"beyond `/api/market/vix`, are there other undocumented removals, renames,
or drift between what's in code and what customers expect?"

**Short answer:** Yes. Three distinct classes of drift, catalogued below.

---

## Method

Compared three sources:

1. **Implementation** — every `@app.get/post`, `@router.get/post` and
   router prefix in `src/api/**` (see [endpoints.md](./endpoints.md) for
   the enumeration).
2. **Documentation** — `API_Guide.md` at the repo root, which is the
   customer-facing endpoint reference.
3. **Prior state** — email correspondence and prior audit artifacts;
   specifically the July 17 exchange with Johnnie Dunkum on `/api/market/vix`.

Any route that appears in one but not the others is drift.

---

## Class 1 — Documented but not implemented

Routes appearing in `API_Guide.md` that have no corresponding handler in
the current codebase. A consumer following the guide will get a `404`.

| Documented at | Route | Status | Blast radius |
|---|---|---|---|
| `API_Guide.md:534` | `GET /api/technicals/gamma-levels` — described as "Get gamma exposure levels (support/resistance zones)" | **Not implemented in code, and per `git log -S "gamma-levels"` never has been.** Only appears in the guide itself. | Any consumer who read the guide and wrote code against this route. |

**Interpretation.** This route was added to the API_Guide as documentation
without ever being shipped, or was intended and dropped before landing.
Same customer-facing failure mode as an unannounced removal — the docs
promise a route that returns 404.

**Remediation (Tier 1).** Two options:

- **Option A — implement the route** to satisfy the doc. Deferred:
  requires product intent about what "gamma levels" means beyond what
  `/api/gex/summary` already gives via `call_wall`/`put_wall`/`gamma_flip`.
- **Option B — remove the doc entry and redirect readers** to the
  existing GEX endpoints that provide the equivalent data. **Recommended.**
  Add a note to `API_Guide.md` that support/resistance levels come from
  `/api/gex/summary` (walls) and `/api/v1/levels/{symbol}` (consolidated
  view) instead.

Either way, gets recorded as an entry in the new machine-readable
changelog (ADR-0004 §Changelog Format).

---

## Class 2 — Implemented but not documented

Routes that exist in code but do not appear in `API_Guide.md`. A consumer
depending on the guide won't discover these; a consumer using Swagger UI
sees them but has no narrative context.

| Route | Where documented today | Gap |
|---|---|---|
| `GET /api/gex/profile` | Only inline docstring on the handler (`main.py:554`) | Not in API_Guide.md |
| `GET /api/gex/historical-context` | Inline docstring only | Not in API_Guide.md |
| `GET /api/gex/expirations` | Inline docstring only | Not in API_Guide.md |
| `GET /api/gex/strike-profile-timeseries` | Inline docstring only | Not in API_Guide.md |
| `GET /api/gex/flip-term-structure` | Inline docstring only | Not in API_Guide.md |
| `GET /api/gex/flip-surface` | Inline docstring only | Not in API_Guide.md |
| `GET /api/gex/vol_surface` | Inline docstring only | Not in API_Guide.md |
| `GET /api/gex/premium_surface` | Inline docstring only (tagged Beta) | Not in API_Guide.md |
| `GET /api/market/volatility` | Inline docstring only | Not in API_Guide.md **(the very route that replaces `/api/market/vix`)** |
| `GET /api/scorecard/daily` | Inline docstring only | Not in API_Guide.md |
| `GET /api/forecast/*` (5 routes) | Inline docstrings only | Not in API_Guide.md |
| `GET /api/replay/*` (4 routes) | Inline docstrings only | Not in API_Guide.md |
| `GET /api/backtest/*` (18 routes) | Inline docstrings only (Beta) | Not in API_Guide.md |
| `GET /api/forced-flow/*` (7 routes) | Inline docstrings only | Not in API_Guide.md |
| `GET /api/tradeworkz/*` (~20 routes) | Inline docstrings only | Not in API_Guide.md |
| `GET /api/option/contract` | Inline docstring only | Not in API_Guide.md |
| `GET /api/tools/option-calculator` | Inline docstring only | Not in API_Guide.md |
| `WS /ws` | Inline docstring only | Not in API_Guide.md |

**Interpretation.** Documentation drifted behind implementation. The
`API_Guide.md` was last comprehensively updated to cover the pre-v1
surface; every new endpoint since has been landed with a docstring but
no update to the guide.

**Especially notable:** `/api/market/volatility` is the replacement for
`/api/market/vix` that Michael pointed Johnnie to. It exists in code but
does not appear in the customer-facing guide. If Johnnie hadn't emailed,
he would have had to discover the replacement via Swagger. This is
exactly the failure mode ADR-0004's changelog and ADR-0003's
OpenAPI-as-source-of-truth close.

**Remediation.** Tier 5 rewrites `API_Guide.md` as a generated document
from the OpenAPI spec, at which point every route is documented by
construction. Interim, no action — cataloguing here is enough.

---

## Class 3 — Removed without deprecation

Routes that previously existed and were quietly removed. Known cases:

| Route | Removed in favor of | Announcement | Customer impact |
|---|---|---|---|
| `GET /api/market/vix` | `GET /api/market/volatility?ticker=VIX` (defaults to VIX) | **None until Johnnie flagged it 2026-07-17** | Silent 404. Johnnie's downstream model had VIX fields go empty. |

Michael acknowledged this in the 2026-07-17 email: *"I should have
formally deprecated it first instead of removing it outright so it
didn't cause downstream issues. I'll definitely handle API changes more
gracefully going forward."*

**Remediation.** This case becomes the pilot for ADR-0004's deprecation
policy:

1. Bring `/api/market/vix` back as a deprecated alias to
   `/api/market/volatility?ticker=VIX`.
2. Emit `Deprecation: true`, `Sunset: <2027-01-31>`, and
   `Link: </api/market/volatility?ticker=VIX>; rel="successor-version"`
   headers on every response.
3. Include an in-body warning: `meta.warnings: [{code: "ROUTE_DEPRECATED",
   detail: "...", successor: "/api/market/volatility?ticker=VIX"}]`.
4. Add the entry to the machine-readable changelog under
   `/api/version/changelog`.
5. Continue serving until the sunset date, then return `410 Gone` with a
   `Link` header pointing to the successor.

Timeline: implemented in Tier 1 as the reference case for the new
deprecation flow. See ADR-0004.

**Beyond `/api/market/vix`.** No other Class 3 cases turned up in this
audit, but the search is inherently limited by what's still visible in
git history and correspondence. Once the ADR-0003 CI gate (OpenAPI diff)
is in place, no future Class 3 case can happen silently.

---

## Class 4 — Legacy naming inconsistencies (audit for style guide)

Not drift per se, but pattern deviations worth noting for the ADR-0003
style guide.

| Pattern | Example | Convention observed | Convention preferred |
|---|---|---|---|
| Kebab vs snake in path segments | `/api/gex/vol_surface` and `/api/gex/premium_surface` use snake_case; everything else uses kebab-case | Mixed | Kebab-case throughout (`/api/gex/vol-surface`, `/api/gex/premium-surface`). Fix under v1. |
| Router-mounted `v1` prefix | `/api/v1/levels/{symbol}` mounts at `/v1/` while everything else mounts under `/api/` | One-off | Under ADR-0003 hard-cut, everything moves to `/v1/`. The current `/api/v1/levels` becomes `/v1/levels` (drop the redundant `/api/`). |
| Endpoint bundle vs breakdown | `/api/technicals` returns all bars + all signals; `/api/technicals/vwap-deviation` returns just VWAP | Bundle + individual | Keep, but document the pattern in the style guide |
| Singular vs plural | `/api/option/quote` (singular `option`) vs `/api/market/*` (singular) | Mostly singular | Keep singular; document |
| Historical singular | `/api/gex/historical`, `/api/market/historical` (adjective as noun) | Fine, established | Keep |

None of these break anything today. Under the v1 hard-cut, kebab-case is
normalized across the board (Tier 1 batch rename inside the `/v1/`
namespace; `/legacy/` keeps snake-case aliases for 90 days).

---

## Class 5 — Response shape drift within stable routes

Beyond path drift, several routes have changed their response shape over
time in ways that would break strict consumers:

| Route | Field | Drift | Detected via |
|---|---|---|---|
| `GET /api/gex/summary` | `gamma_flip_raw`, `gamma_flip_span_used`, `flip_distance`, `convexity_risk` | Added over time — a consumer against an older schema wouldn't have had these fields | Compared field list against `class GEXSummary` history via `git log -p src/api/models.py` |
| `GET /api/market/quote` | `display_source`, `data_symbol`, `futures_close`, `futures_reference_close` | Added ADDITIVELY (marked "DISPLAY-only" in docstring at `models.py:234`) | Same |
| `GET /api/market/quote` | `cumulative_daily_volume` → `volume` | Renamed at response-build time in `main.py:1231-1232` | Grep |

**Interpretation.** Additive shape changes are safe for permissive
consumers but break strict schema validators. The lack of a schema version
means consumers can't detect the change.

**Remediation.** ADR-0003's `X-ZeroGEX-Schema-Version` header + OpenAPI
diff CI gate. Any additive change bumps a build number; any breaking
change requires a major version bump and blocks CI without an ADR.

---

## Cross-cutting observations

### Root cause of every case above

Both the `/api/market/vix` removal and the `API_Guide.md` documentation
drift share a single root cause: **there is no single machine-checkable
source of truth for the API contract.**

- `main.py`, the router files, and `models.py` are the *implementation*
  truth.
- `API_Guide.md` is a *narrative* truth maintained by hand.
- FastAPI's auto-generated `/openapi.json` is a partial *schema* truth,
  but ignored by customers (Swagger UI is opt-in) and doesn't catch
  breaking changes because nothing diffs it.

They drift because there is nothing that fails when they diverge. The
end state that fixes this (ADR-0003):

1. **OpenAPI spec is source of truth** — hand-authored, versioned in the
   repo, publishable to `zerogex-docs`.
2. **Types are generated from the spec** — Pydantic models via
   `datamodel-code-generator` (or equivalent), not the other way around.
3. **CI diffs the spec** on every PR and blocks breaking changes without
   an ADR + version bump.
4. **`API_Guide.md` retires** in favor of the generated docs site.

That's the terminal state. Getting there is Tier 3–5. This audit
documents the pre-state so we can measure the improvement.

### Absent cases

Explicit not-found list (searched but nothing found):

- No routes that changed method (GET → POST or similar).
- No routes that changed authentication requirements silently (all
  auth-relevant changes visible in `security.py` history).
- No routes where the scope changed silently (scope wiring in `main.py`
  makes every guard visible at include-router time).
- No routes returning different content types than what their
  `response_model` suggests (spot-checked; consistent).

### Not audited (out of scope)

- WebSocket message shape drift — `/ws` is a separate audit.
- Query-parameter shape drift within stable paths — some routes have
  added new optional query params over time. These are additive-only
  (Pydantic query defaults tolerate absence) so they're not drift risks
  for consumers. Documented in Tier 3 as needed.

---

## Summary

| Class | Count | Fix in tier |
|---|---|---|
| 1 (documented but not implemented) | 1 | T5 (docs rewrite) or T1 if we choose to implement |
| 2 (implemented but not documented) | ~55 routes | T5 (docs regenerated from OpenAPI) |
| 3 (removed without deprecation) | 1 known | T1 (VIX shim + deprecation policy) |
| 4 (naming inconsistencies) | ~5 patterns | T1 (v1 hard-cut) |
| 5 (response shape drift) | ~3 fields | T1 (schema version) + T4 (CI diff gate) |

None of these require immediate hotfixes. All are addressed as byproducts
of Tier 1–5 execution. The one action-taking commitment: **restore
`/api/market/vix` as a deprecated alias in Tier 1** so we honor Michael's
July 17 promise to Johnnie in code, not just in email.
