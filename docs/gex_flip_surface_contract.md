# `/api/gex/flip-surface` — proposed payload contract

Status: **CONTRACT READY / NOT IMPLEMENTED**. All four originally-open
design questions are resolved below; the API shape is locked enough for
an implementation pass. The companion endpoint
`/api/gex/flip-term-structure` (implemented as a prototype) returns the
flip points only; this one returns the full per-horizon dealer-gamma
profile that the contour / 3D-surface visualizations consume.

## Purpose

Return the spot-shift dealer gamma profile (the same primitive
`AnalyticsEngine._gamma_exposure_profile` builds today) at several
multi-day horizons, on a shared price grid, plus the resolved flip per
horizon and a few overlay aids (current spot, walls).

Single canonical payload for both renderings:

- **Horizon × price contour (2D)** — `grid` on X, `horizons_days` on Y,
  color = signed dealer-GEX value picked from `profiles`. The flip
  curve is the zero contour of that surface; render it explicitly via
  `flips`.
- **3D mesh** — same arrays, rendered as `Plotly.surface` / similar.

## Endpoint

```
GET /api/gex/flip-surface
```

### Query parameters

| name           | type              | default                | constraint                  |
| -------------- | ----------------- | ---------------------- | --------------------------- |
| `symbol`       | string            | `SPX`                  | underlying alias            |
| `horizons`     | csv of float days | `1,3,5,10,20,60`       | `[0.25, 365]`, ≤ 12 entries |
| `span_pct`     | float             | server default (0.20)  | `[0.02, 1.0]`               |
| `step_pct`     | float             | server default (0.0025)| `[0.0005, 0.05]`            |
| `include_walls`| bool              | `true`                 |                             |

`span_pct` / `step_pct` mirror `GAMMA_PROFILE_SPAN_PCT` /
`GAMMA_PROFILE_STEP_PCT` and bound the price grid to
spot · (1 ± span_pct) stepped by spot · step_pct. The grid is **shared
across all horizons** so each profile slice is directly stackable —
this is the contract that makes the contour render with a single
colormap and the surface render as a non-ragged mesh.

### Response model (Pydantic-style)

```python
class FlipSurfaceWall(BaseModel):
    strike: float           # USD
    type: Literal["call", "put"]
    abs_dollar_gex: float   # peak |dollar GEX|, calls-positive/puts-negative

class FlipSurfacePoint(BaseModel):
    horizon_days: float
    flip: Optional[float]   # resolved zero crossing in USD; None when unresolved
    resolved: bool
    span_used: float        # fraction of spot; the rung that resolved (or last tried)
    net_gex_at_spot: Optional[float]  # dollar GEX at the grid point closest to spot

class FlipSurfaceResponse(BaseModel):
    symbol: str
    spot: float                         # USD
    timestamp: datetime                 # snapshot timestamp (UTC)

    # Shared price grid across all horizons. Strictly ascending,
    # uniformly spaced by spot * step_pct. ~160 entries at defaults.
    grid: List[float]                   # USD per entry

    # Multi-day reference horizons, ascending. One entry per row of
    # `profiles`. Aligned 1:1 with `flips`.
    horizons_days: List[float]

    # 2D array: profiles[h_idx][grid_idx] = signed dealer dollar GEX
    # per 1% spot move at the hypothetical price grid[grid_idx], with
    # weight min(1, DTE / horizons_days[h_idx]) applied per contract.
    # Sign convention: calls +, puts − (dealer short calls / long puts).
    # Units: $ per 1% move. Magnitude ≈ 1e8–1e10 for SPX.
    profiles: List[List[float]]

    # Resolved flip per horizon (zero crossing of profiles[h_idx]).
    # Aligned 1:1 with horizons_days.
    flips: List[FlipSurfacePoint]

    # Optional wall overlay; high-|dollar-GEX| strikes from the chain.
    # Independent of horizon (uses the production weight). Returned as
    # vertical lines on the contour.
    walls: List[FlipSurfaceWall]
```

### Example response (truncated)

```json
{
  "symbol": "SPX",
  "spot": 7250.00,
  "timestamp": "2026-05-22T19:35:00Z",
  "grid": [6525.00, 6543.13, ..., 7975.00],
  "horizons_days": [1, 3, 5, 10, 20, 60],
  "profiles": [
    [-9.2e9, -8.7e9, ..., +7.1e9],
    [-9.4e9, -8.9e9, ..., +7.3e9],
    [-9.5e9, -9.0e9, ..., +7.4e9],
    ...
  ],
  "flips": [
    {"horizon_days":  1, "flip": 7298.50, "resolved": true,
     "span_used": 0.20, "net_gex_at_spot": -1.2e9},
    {"horizon_days":  3, "flip": 7311.00, "resolved": true,
     "span_used": 0.20, "net_gex_at_spot": -0.8e9},
    {"horizon_days":  5, "flip": 7332.77, "resolved": true,
     "span_used": 0.20, "net_gex_at_spot": -0.3e9},
    {"horizon_days": 10, "flip": null,    "resolved": false,
     "span_used": 0.50, "net_gex_at_spot":  null},
    ...
  ],
  "walls": [
    {"strike": 7300.0, "type": "call", "abs_dollar_gex": 2.4e10},
    {"strike": 7200.0, "type": "put",  "abs_dollar_gex": 1.9e10}
  ]
}
```

### Units, sign, and shape contract

- All prices and walls in **USD** (the underlying's quote unit).
- Profile values are **dollar GEX per 1% move**, same convention as the
  persisted `gex_summary.net_gex_at_spot` and `gex_by_strike.net_gex`.
- Sign: **calls positive, puts negative** (dealer short calls / long
  puts). The flip is where `profiles[h]` crosses zero.
- `len(grid) ≥ 2` always; bounded `[~80, ~800]` at the parameter limits.
- `len(profiles) == len(horizons_days) == len(flips)`.
- `len(profiles[i]) == len(grid)` for every `i` — server enforces.
- Timestamps in **ISO-8601 UTC**.

## Errors

| status | condition                                                  |
| ------ | ---------------------------------------------------------- |
| `400`  | malformed `horizons`, out-of-range `span_pct`/`step_pct`   |
| `404`  | no usable snapshot for `symbol` (no Greeks-bearing rows)   |
| `500`  | profile build failed (logged with snapshot diagnostics)    |

## Caching / cost

Per-request cost: `len(horizons) × _resolve_gamma_flip` invocations,
each of which builds 1–3 profiles (ladder rungs) at
`len(grid) × len(options)` cost. For SPX at production chain size
(~5000 contracts), one full request ≈ 100–400 ms wall on a warm cache.

Server-side cache: keyed on `(symbol, horizons, span_pct, step_pct)`,
TTL **5s** matching the existing analytics endpoints. The bulk of
clients re-fetching for live updates will hit it.

Payload size: dominated by `profiles`. At defaults (`grid` ≈ 160,
6 horizons) ≈ 960 floats ≈ 8 KB JSON / 4 KB gzipped. Cap the
combinatorial: `len(grid) × len(horizons_days) ≤ 4000`.

## Computation contract

- Profiles are built by `AnalyticsEngine._gamma_exposure_profile` with
  the existing `dte_ref_days=` override (already wired by the
  term-structure work). No new analytics math.
- `flips[h].flip` is exactly the `_resolve_gamma_flip` result for that
  horizon — same adaptive ladder, interior gate, structural gate, and
  actionable-distance gate as production. The surface endpoint never
  fabricates a flip; unresolved horizons return `flip: null` and
  `resolved: false`.
- Walls come from `src.analytics.walls.compute_call_put_walls` (the
  canonical definition the rest of the codebase uses), unscaled by
  horizon — they're a chain-level overlay, not horizon-dependent.

## Resolved design decisions

The four originally-open questions, now answered as v1 defaults. Each
can be revisited if data shows it's needed; the goal is to ship the
simplest contract that doesn't paint us into a corner.

### 1. Cache key — **exact horizon list**

The cache is keyed on the tuple
``(symbol, tuple(sorted(horizons)), span_pct, step_pct)`` with the
standard 5s TTL. Reasoning: the 12-horizon cap bounds total cardinality,
the dashboard's default horizon set will dominate traffic, and a
normalized canonical set would force every client to use the same
horizons (breaking the per-consumer-horizon promise the multi-horizon
design exists to deliver). Revisit if hit-rate ever measures below
~60% in production.

### 2. Payload representation — **dense arrays, no sparse fallback**

Profiles are returned as dense `List[List[float]]` matching the shared
`grid`. At default settings (~160 grid points × 6 horizons) the JSON
payload is ~8 KB, ~4 KB gzipped — already small. A sparse
`(grid_idx, value)` representation adds schema complexity and forks
every client renderer for negligible byte savings on a chain dense
enough to actually have a flip. The combinatorial cap
``len(grid) × len(horizons_days) ≤ 4000`` still applies as a guardrail.

### 3. Walls — **horizon-independent v1, per-horizon walls deferred**

`walls[]` is computed once per request from
`src.analytics.walls.compute_call_put_walls` (the same canonical
definition the rest of the codebase uses) with the production
DTE weighting — independent of the requested horizons. The contour
visualization renders walls as a single set of vertical strike lines.

Trade-off acknowledged: the per-horizon walls (the strikes whose
*horizon-weighted* dollar GEX dominates) would be slightly different
at the 1d vs 60d horizon, and a viewer might find the single overlay
misleading on contour edges. Per-horizon walls become an additive
field (`walls_by_horizon: Dict[float, List[FlipSurfaceWall]]`) in v2
if/when a user reports this confusion — the existing field stays
populated for backwards compatibility.

### 4. Live updates — **HTTP polling, no SSE v1**

Clients refresh by re-issuing the GET on whatever cadence they want.
The 5s server cache and the existing analytics 5s recompute cadence
make polling at 1–5s effectively free. A `/flip-surface/stream` SSE
endpoint adds stateful connections, reconnection logic, and a second
code path with no current consumer; defer until a "live dashboard" use
case actually materializes. Most surface-style visualizations are
opened, browsed, then closed — polling fits that pattern.
