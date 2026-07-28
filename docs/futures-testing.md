# Futures Support — Testing

This document lists the test commands and what the ES/NQ feature's test suites
cover, for both the backend (`zerogex-oa`) and the frontend (`zerogex-web`).

See [futures-support-architecture.md](./futures-support-architecture.md) for the
big picture.

---

## 1. Backend

### Commands

| Command | What it runs |
| --- | --- |
| `make test` | `pytest -m "not integration"` (with coverage) |
| `make test-fast` | `pytest --no-cov -q -m "not integration"` |
| `make lint` | `flake8 src tests` |
| `make type-check` | `mypy src` |
| `make fmt-check` | `black --check src tests` (line length 100) |
| `make fmt` | `black src tests` (writes changes) |

`make test` runs the whole suite excluding integration-marked tests: the **2686
existing tests continue to pass**, plus **~113 new tests** across the suites
below.

In CI (`ci.yml`, mirrored by `make ci`), `black --check` and `pytest` are
**blocking**; `flake8` and `mypy` are **advisory** (`continue-on-error`).

### New backend test suites (`tests/`)

| Suite | Covers |
| --- | --- |
| `test_instruments.py` | Registry metadata (multiplier 100/50/20, analytics source, calendar), capabilities × availability, feature-flag gating, `enabled_symbols()`, `effective_capabilities()`, NQ reference-source guard |
| `test_black76.py` | Black-76 price/delta/gamma/vega/theta/rho against the ATM reference (`F=K=100, T=1, σ=0.2, r=0.05`), vanna/charm finite differences, IV solver (Newton + bisection, `None` below discounted intrinsic), degenerate/expired quality |
| `test_cme_calendar.py` | Globex session model, trade-date roll at 17:00 CT, weekend gap, maintenance window, RTH cash overlap, session labels, holidays/early closes, DST, `classify_dte` / `is_zero_dte` by exact instant |
| `test_futures_contracts_resolver.py` | Domain objects (tz-aware expiration enforced, decimal-safe notional/tick), month codes, quarterly generation, front/next resolution, roll policies (calendar/volume/OI/hybrid, hard-roll fallback) |
| `test_futures_gex.py` | GEX formula with the futures multiplier (50/20, not 100), by-strike/expiration/underlying breakdowns, gamma flip, walls, max pain, DTE buckets, provenance metadata, `aggregate_by_underlying` cross-maturity seam |
| `test_futures_providers_reference.py` | Fixture providers (marked `SYNTHETIC_FIXTURE`), `PendingRealCMEProvider` raises `RealCMEProviderNotConfigured`, ingestion metrics/health, reference-mode basis + projection, skew/quality gating, NQ guard raises |
| `test_futures_api.py` | `/api/instruments`, `/api/instruments/{symbol}` (availability, `known_limitations`), `/api/futures/{product}/contracts` (null volume/OI), `/roll-state`, `/reference-levels` (409 when disabled, unavailable quality when data missing) |

Key invariants the suites assert:

- No fabricated market data: fixtures are labeled `SYNTHETIC_FIXTURE`; the real
  adapter raises rather than emitting ticks; contract endpoints return null
  volume/OI.
- The three analytics sources stay distinct: `cme_futures_options` vs
  `opra_equity_options` vs `cash_index_projection` are never conflated, and
  reference-mode results always carry the disclaimer.
- Unavailable futures are described but never made to look operational.

---

## 2. Frontend

### Commands

| Command | What it runs |
| --- | --- |
| `node --experimental-strip-types --test tests/instruments.test.ts` | Registry mirror tests |
| `node --experimental-strip-types --test tests/symbolPersistence.test.ts` | Symbol selection/persistence tests |
| `npx tsc --noEmit` | Type-check |
| `npm run lint` | ESLint |
| `npm run build` | `next build` (production build must succeed) |

### Frontend test coverage

- **`tests/instruments.test.ts`** — the `core/instruments/registry.ts` mirror:
  instrument definitions match the backend (multiplier 100/50/20, tick 0.25 for
  ES/NQ, analytics source, calendar, `referenceSourceSymbol` SPX for ES / null
  for NQ), `getAvailability()` flag gating, `enabledSymbols()`,
  `getEffectiveCapabilities()` intersection, `setServerAvailability()` override
  precedence, and the NQ reference-mode guard reason.
- **`tests/symbolPersistence.test.ts`** — instrument selection/persistence keeps
  the equity core selectable and only exposes ES/NQ when available.

Formatting helpers in `core/instruments/formatting.ts`
(`getPricePrecision`, `getContractMultiplier`, `getPriceIncrement`,
`formatInstrumentPrice`, `roundToTick`, `formatNotional`) are registry-driven so
ES/NQ (0.25 tick, $50/$20 point value) are never rendered as a 2-decimal stock
price; they fall back to equity defaults (precision 2, multiplier 100) for
unknown symbols so legacy call-sites are unchanged.

---

## 3. Quick pre-merge checklist

```bash
# Backend (from zerogex-oa/)
make fmt-check && make test        # blocking
make lint && make type-check       # advisory

# Frontend (from zerogex-web/frontend/)
node --experimental-strip-types --test tests/instruments.test.ts
node --experimental-strip-types --test tests/symbolPersistence.test.ts
npx tsc --noEmit && npm run lint && npm run build
```
