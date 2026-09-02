#!/usr/bin/env python3
"""
ZeroGEX API Server
FastAPI backend for serving analytics data to the frontend
"""

from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
import asyncio
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone, date as date_type
from enum import IntEnum
import os
from src.config import _getenv_str
import re
from typing import List, Optional, Literal
import pytz

from .database import DatabaseManager
from .errors import handle_api_errors
from .futures_middleware import FuturesProjectionMiddleware
from .middleware import AuditLogMiddleware, RequestIdMiddleware, UsageMeterMiddleware
from .ratelimit import rate_limit
from .scopes import FLOW, GEX, MARKET_RAW, MARKET_REFERENCE, MAXPAIN, SIGNALS, TECHNICALS
from .security import api_key_auth, key_store, require_scopes
from .usage import usage_meter
from .quote_broadcaster import QuoteBroadcaster, set_broadcaster
from src.analytics.pin_stability import build_pin_stability
from .routers import websockets as ws_router
from .models import (
    GEXSummary,
    PinStabilityResponse,
    GEXByStrike,
    GEXProfile,
    GEXHistoricalContext,
    FlowPoint,
    FlowSeriesPoint,
    FlowContractsResponse,
    MarketTideResponse,
    MarketTideHistoryResponse,
    SmartMoneyFlowPoint,
    MomentumDivergencePoint,
    FlowBuyingPressurePoint,
    UnderlyingQuote,
    SessionCloses,
    SessionLevels,
    HealthStatus,
    MaxPainCurrent,
    MaxPainTimeseriesPoint,
    OptionQuote,
    OpenInterestRecord,
    OpenInterestResponse,
    StrikeProfileBucket,
)
from .routers.trade_signals import router as trade_signals_router
from .routers.trade_bias import router as trade_bias_router
from .routers.tradeworkz import router as tradeworkz_router
from .routers.volatility_gauge import router as volatility_gauge_router
from .routers.option_contract import router as option_contract_router
from .routers.option_calculator import router as option_calculator_router
from .routers.vol_surface import router as vol_surface_router
from .routers.premium_surface import router as premium_surface_router
from .routers.gex_flip_horizon import router as gex_flip_horizon_router
from .routers.gamma_shift import router as gamma_shift_router
from .routers.backtest import router as backtest_router
from .routers.scorecard import router as scorecard_router
from .routers.forecast import router as forecast_router
from .routers.replay import router as replay_router
from .routers.forced_flow import router as forced_flow_router
from .routers.levels import router as levels_router
from .routers.admin_api_keys import router as admin_api_keys_router
from .routers.admin_xpost import router as admin_xpost_router
from .routers.news import router as news_router

# Logging is configured centrally in src.utils.logging; importing
# get_logger triggers _configure_logging which honors LOG_LEVEL and
# LOG_FORMAT and installs the request-id filter. We must NOT call
# logging.basicConfig() here — it would wipe the centralized handler
# and the structured/request-id format would silently revert to plain.
from src.utils import get_logger  # noqa: E402

logger = get_logger(__name__)


class MarketTideWindow(IntEnum):
    """Supported Market Tide lookbacks, parsed from HTTP query strings."""

    FIVE_MINUTES = 5
    FIFTEEN_MINUTES = 15
    THIRTY_MINUTES = 30
    SIXTY_MINUTES = 60


# Database manager
db_manager: Optional[DatabaseManager] = None


def _db() -> DatabaseManager:
    """Return the initialized db_manager.

    Endpoint handlers run after the lifespan startup hook has set
    ``db_manager``; this helper narrows the Optional for type-checkers and
    raises a clear error if called before initialization.
    """
    if db_manager is None:
        raise RuntimeError("db_manager not initialized")
    return db_manager


def _parse_cors_origins(raw_origins: Optional[str]) -> List[str]:
    """Parse comma-separated origins from env var into a normalized list.

    When ``ENVIRONMENT=production`` the wildcard ``"*"`` is refused — any
    production deployment must explicitly list its allowed origins so an
    accidentally-empty env var can't open the API to every cross-origin
    caller on the internet.
    """
    origins: List[str] = []
    if raw_origins:
        origins = [origin.strip() for origin in raw_origins.split(",") if origin.strip()]

    environment = _getenv_str("ENVIRONMENT", "development").lower()
    if not origins:
        if environment == "production":
            raise RuntimeError(
                "CORS_ALLOW_ORIGINS is unset and ENVIRONMENT=production; "
                "refusing to start with wildcard CORS.  Set CORS_ALLOW_ORIGINS "
                "to an explicit comma-separated list of allowed origins."
            )
        return ["*"]

    if "*" in origins and environment == "production":
        raise RuntimeError(
            "CORS_ALLOW_ORIGINS contains '*' and ENVIRONMENT=production; "
            "wildcard CORS is not permitted in production."
        )
    return origins


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    global db_manager

    # Startup
    logger.info("Starting ZeroGEX API Server...")
    db_manager = DatabaseManager()
    await db_manager.connect()
    logger.info("Database connected successfully")

    # Wire the per-user API-key store to the live DB pool so api_key_auth()
    # can validate keys against the api_keys table.  Static API_KEY env-var
    # auth (if set) keeps working alongside this.
    #
    # Pass a *getter* rather than the pool itself: if DatabaseManager later
    # reconnects (replacing self.pool), the key store picks up the new pool
    # on the next lookup instead of holding a stale, closed reference.
    key_store.configure(lambda: db_manager.pool)

    # Wire the durable usage meter to the same live pool and start its
    # background flush loop. No-op unless API_USAGE_METERING_ENABLED is set,
    # so this is inert until metering is switched on.
    usage_meter.configure(lambda: db_manager.pool)
    usage_meter.start()

    # Real-time quote broadcaster — LISTEN 'zgx_quote_updates' →
    # per-symbol WebSocket fan-out. Held for the process lifetime;
    # reconnects internally if the DB blips. get_market_session is
    # defined later in this module, so passing it by callable defers
    # binding until the notify actually fires. Feature-gated: unset
    # WS_ENABLED (or set it to "0") to keep the socket wiring dormant
    # during the rollout window.
    if _getenv_str("WS_ENABLED", "1").strip().lower() not in {"0", "false", "no"}:
        # Same host/port/database/user/password the pool was built with
        # so the LISTEN connection tracks the pool's credentials (a
        # DatabaseManager reconnect that changed them is picked up on
        # the next reconnect of the LISTEN loop). We build the kwargs
        # here rather than inspecting asyncpg's private ``_params``
        # because that internal field's schema varies by version (0.31+
        # lost the ``dsn`` attribute the earlier draft relied on,
        # causing LISTEN to never come up).
        def _listen_kwargs():
            if db_manager is None:
                return None
            ssl_mode = os.getenv("DB_SSLMODE", "").strip().lower()
            ssl = True if ssl_mode in {"require", "verify-ca", "verify-full"} else None
            return {
                "host": db_manager.host,
                "port": db_manager.port,
                "database": db_manager.database,
                "user": db_manager.user,
                "password": db_manager.password,
                "ssl": ssl,
                # Short connect timeout — a slow initial connect just
                # means the outer reconnect loop retries; don't tie up
                # the LISTEN task for minutes.
                "timeout": 10.0,
            }

        # Same 1s-LRU-cached signal the HTTP handler uses, so the two
        # paths' session labels agree at 16:00 ET (previously the
        # broadcaster shortcut'd close_data_available=True and briefly
        # disagreed with the HTTP handler while the first post-close
        # bar was landing).
        async def _close_data_check(symbol, asset_type):
            if db_manager is None:
                return True
            return await db_manager.has_todays_close_landed(symbol, asset_type)

        quote_broadcaster = QuoteBroadcaster(
            connect_kwargs_getter=_listen_kwargs,
            session_computer=lambda asset_type, stable, close_avail: get_market_session(
                asset_type, stable, close_avail
            ),
            close_data_check=_close_data_check,
        )
        set_broadcaster(quote_broadcaster)
        await quote_broadcaster.start()
    else:
        logger.info("WS_ENABLED=0 — quote broadcaster not started")

    # The max-pain snapshot is refreshed off-process by the
    # zerogex-oa-max-pain-refresh.timer (daily, pre-market) — not by an
    # in-process loop and not inline on the request path.  The endpoint is
    # a pure cache read; nothing to start/stop here.

    # Seed TradeWorkz bot roster + capital sleeves on first boot. Idempotent:
    # ON CONFLICT DO NOTHING at the row level, so a subsequent restart with
    # existing rows is a no-op. Wrapped in try/except so a missing schema
    # (fresh instance where `make schema-apply` has not yet been run) does
    # not block API startup — the operator will run schema-apply and then a
    # subsequent boot will complete the seed.
    try:
        from src.database import db_connection as _db_connection
        from src.tradeworkz.engine import provision_defaults as _tw_provision

        with _db_connection() as _conn:
            _inserted = _tw_provision(_conn)
        if _inserted:
            logger.info("TradeWorkz: provisioned %d default bots", _inserted)
    except Exception:  # noqa: BLE001 — defensive during startup
        logger.warning("TradeWorkz bot provisioning skipped", exc_info=True)

    # Start the TradeWorkz in-process tick scheduler. Runs one engine.tick()
    # every TRADEWORKZ_ENGINE_INTERVAL_SECONDS on a background asyncio task,
    # dispatching the synchronous psycopg2 work through asyncio.to_thread so
    # it doesn't block the FastAPI event loop. No-op when
    # TRADEWORKZ_ENGINE_ENABLED=false. Wrapped in try/except so a startup
    # failure inside the scheduler can't prevent the API from serving.
    try:
        from src.tradeworkz.scheduler import scheduler as _tw_scheduler

        _tw_scheduler.start()
    except Exception:  # noqa: BLE001
        logger.warning("TradeWorkz scheduler failed to start", exc_info=True)

    # Keep the full-session forced-flow field warm so no user pays the cold
    # full-day rebuild. Runs PER uvicorn worker (each worker has its own
    # in-process cache), so a request landing on a cold worker no longer stalls.
    # Background asyncio task; the sync reprice inside runs via asyncio.to_thread
    # so it never blocks the event loop.
    _ff_warm_task = None
    try:
        from .routers.forced_flow import session_surface_warm_loop

        _ff_warm_task = asyncio.create_task(session_surface_warm_loop())
    except Exception:  # noqa: BLE001
        logger.warning("session-surface warmer failed to start", exc_info=True)

    yield

    # Shutdown
    logger.info("Shutting down ZeroGEX API Server...")
    # Stop the meter first (cancels the loop and flushes the final window)
    # while the DB pool is still up, then drop both pool getters.
    from .quote_broadcaster import get_broadcaster as _get_bc

    bc = _get_bc()
    if bc is not None:
        await bc.stop()
        set_broadcaster(None)
    await usage_meter.stop()
    usage_meter.configure(None)
    key_store.configure(None)
    # Stop the TradeWorkz scheduler while the pool is still up so any final
    # in-flight tick can finish its DB writes cleanly before we tear down.
    try:
        from src.tradeworkz.scheduler import scheduler as _tw_scheduler

        await _tw_scheduler.stop()
    except Exception:  # noqa: BLE001
        logger.warning("TradeWorkz scheduler shutdown failed", exc_info=True)
    # Stop the session-surface warmer.
    if _ff_warm_task is not None:
        _ff_warm_task.cancel()
        try:
            await _ff_warm_task
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001
            logger.warning("session-surface warmer shutdown error", exc_info=True)
    if db_manager:
        await db_manager.disconnect()
    logger.info("Shutdown complete")


# Create FastAPI app.
#
# The global dependency enforces API-key auth when API_KEY is set in the
# environment; when unset, the dependency is a no-op so local development
# and CI continue to work without credentials.
app = FastAPI(
    title="ZeroGEX API",
    description=(
        "Real-time options analytics API.\n\n"
        "**Two versions are served.**\n\n"
        "* **v2 (recommended)** — `/api/v2/*`. Every endpoint returns "
        '`{"data": ..., "freshness": {...}}`, where the `freshness` block '
        "reports response evaluation time, the underlying data's source "
        "timestamp, the market session, the expected update cadence, and a "
        "rolled-up `freshness_status` — so endpoint health and data age are "
        "separate, machine-readable facts on every response.\n"
        "* **v1 (stable)** — `/api/*` and `/api/v1/levels/*`. Unchanged and "
        "supported; `data` in a v2 response is byte-for-byte the v1 body.\n\n"
        'See "API versions & the freshness envelope" in API_Guide.md for '
        "the field contract and the per-endpoint cadence table."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
    # rate_limit runs after api_key_auth so request.state.identity (set by
    # the auth dependency) is available for the rate-limit key. No-op
    # unless END_USER_RATE_LIMIT_ENABLED is set.
    dependencies=[Depends(api_key_auth), Depends(rate_limit)],
    # Alphabetize endpoints within each tag in the Swagger UI. Operations
    # (HTTP methods) are also sorted so per-path groups render in a stable
    # order. `tagsSorter` keeps the tag list itself alphabetical.
    swagger_ui_parameters={
        "operationsSorter": "alpha",
        "tagsSorter": "alpha",
    },
    openapi_tags=[
        {"name": "Health", "description": "API and database health checks"},
        {"name": "GEX", "description": "Gamma Exposure (GEX) analytics"},
        {"name": "Options Flow", "description": "Options flow and buying pressure data"},
        {"name": "Market Data", "description": "Underlying and option quote data"},
        {"name": "Max Pain", "description": "Max pain analysis"},
        {
            "name": "Technicals",
            "description": (
                "Intraday technical signals: VWAP, ORB, dealer hedging, volume, momentum"
            ),
        },
        {
            "name": "Tools",
            "description": (
                "Trader-facing calculators and what-if utilities. "
                "Includes the option-calculator (intrinsic-value P&L fan across "
                "underlying-price moves)."
            ),
        },
        {
            "name": "Trade Signals",
            "description": (
                "Options-structure signal engine: composite Market State Index (MSI) gauge, "
                "advanced signal components (vol-expansion, eod-pressure, squeeze-setup, "
                "trap-detection, 0dte-position-imbalance, gamma-vwap-confluence, "
                "range-break-imminence), per-component event history with realized returns, "
                "a pairwise confluence matrix, and live/historical trade records. "
                "Default symbol is SPY; pass ?symbol= or ?underlying= to override."
            ),
        },
        {
            "name": "Beta",
            "description": (
                "Beta features under active development. The contract and behaviour "
                "of these endpoints may change without notice. Currently includes the "
                "backtesting platform and the options premium (extrinsic-value) surface."
            ),
        },
    ],
)

# CORS middleware
cors_origins = _parse_cors_origins(os.getenv("CORS_ALLOW_ORIGINS"))
allow_credentials = "*" not in cors_origins
if not allow_credentials:
    logger.info(
        "CORS_ALLOW_ORIGINS contains '*'; disabling allow_credentials for standards compliance."
    )

# ES / NQ projection. Registered FIRST, which makes it the INNERMOST
# middleware (last add_middleware is outermost — see the ordering note
# below). That placement matters twice over: it reads the raw JSON straight
# off routing before GZip compresses it, and CORS ends up wrapping it, so the
# 400/503 responses it synthesizes still carry Access-Control-* headers
# instead of surfacing to a browser as an opaque CORS failure.
#
# It is pure ASGI, not BaseHTTPMiddleware: a request that names no future
# calls straight through with no task group and no body buffering, so the
# SPX/SPY/QQQ/NDX hot path is genuinely untouched by this feature.
app.add_middleware(FuturesProjectionMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=allow_credentials,
    # The API surface is entirely read-only GETs (verified: no POST/PUT/
    # PATCH/DELETE routes), so there is no reason to advertise mutating
    # methods or a wildcard header set to cross-origin callers. Narrow to
    # exactly what the browser clients send. ``allow_headers`` enumerates
    # the auth + correlation headers the app actually reads
    # (``security.api_key_auth``, ``identity``, ``middleware``) plus the
    # standard content-negotiation pair.
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=[
        "Authorization",
        "X-API-Key",
        "X-Request-Id",
        "X-End-User-Token",
        "Content-Type",
        "Accept",
    ],
    # Headers a cross-origin browser client may READ. Without this list the
    # browser strips them from the response before JS sees them, however
    # correctly the server sets them — so the v2 freshness headers and the
    # correlation id were reaching curl and server-side consumers but were
    # invisible to any browser integration, which is precisely the audience
    # a charting widget serves.
    #
    # Kept in step with v2._freshness_headers by a test; add a header there
    # and CI fails until it is listed here.
    expose_headers=[
        "X-Freshness-Status",
        "X-Freshness-Evaluated-At",
        "X-Freshness-Source-Timestamp",
        "X-Freshness-Age-Seconds",
        "X-Freshness-Stale-After",
        "X-Freshness-Expected-Cadence",
        "X-Freshness-Cadence-Profile",
        "X-Market-Session",
        # Echoed by RequestIdMiddleware for correlation; same problem.
        "X-Request-Id",
    ],
)

# Compress responses so that large JSON payloads from endpoints like
# /api/flow/by-contract (which can return hundreds of thousands of rows for a
# full session) don't get bottlenecked on transfer.
app.add_middleware(GZipMiddleware, minimum_size=1024)

# Audit logging. Added immediately BEFORE RequestIdMiddleware so the
# resulting nesting is (outer→inner): RequestId → Audit → GZip → CORS →
# routing. Starlette's add_middleware() inserts at index 0 and
# build_middleware_stack() wraps in reverse, so the *last* add_middleware
# call is outermost. Keeping RequestId outermost means the request-id
# contextvar is still set when AuditLogMiddleware emits its line in its
# finally (RequestId's reset runs after Audit returns); having Audit wrap
# routing means it observes the identity the auth dependency set during
# dependency resolution.
app.add_middleware(AuditLogMiddleware)

# Usage metering. Same nesting level as audit (wraps routing so it sees
# the resolved identity). No-op unless API_USAGE_METERING_ENABLED is set.
app.add_middleware(UsageMeterMiddleware)

# Request-ID propagation: every log line emitted while handling a request
# carries the id, and the same id is echoed back via X-Request-Id so
# clients (and server logs) can correlate.
app.add_middleware(RequestIdMiddleware)

# Per-endpoint scope dependencies — one reusable Depends per capability
# scope (see scopes.py for the taxonomy and tier bundles). Wiring these
# onto the routes below is a no-op until keys are backfilled with scopes
# AND API_SCOPE_ENFORCEMENT=1 (see security.require_scopes); a key with
# the wildcard "*" scope always passes.
#
# The one boundary that carries a licence rather than a price is
# MARKET_REFERENCE vs MARKET_RAW: the underlying's own tape (its quote,
# bars, session levels) is reference data every levels integration needs
# and rides with the derived scopes, while the option chain enumerated
# contract by contract is withheld from external keys. See scopes.py for
# why the line sits there and not around "upstream data" generally.
_scope_gex = Depends(require_scopes(GEX))
_scope_flow = Depends(require_scopes(FLOW))
_scope_maxpain = Depends(require_scopes(MAXPAIN))
_scope_technicals = Depends(require_scopes(TECHNICALS))
_scope_signals = Depends(require_scopes(SIGNALS))
_scope_market_reference = Depends(require_scopes(MARKET_REFERENCE))
_scope_market_raw = Depends(require_scopes(MARKET_RAW))

# The /api/signals surface is the premium (basic/pro) tier.
app.include_router(
    trade_signals_router,
    dependencies=[_scope_signals],
)
# Trade Bias rides the same /api/signals premium surface.
app.include_router(trade_bias_router, dependencies=[_scope_signals])
# Derived analytics routers — broadly redistributable.
# Consolidated, versioned dealer-positioning levels (gamma flip, walls,
# max pain, per-strike gamma profile) — the stable external contract that
# third-party charting integrations (TradingView widget, NinjaScript
# indicator) build on. Derived-only, so it rides the GEX/analytics tier.
app.include_router(levels_router, dependencies=[_scope_gex])
app.include_router(volatility_gauge_router, dependencies=[_scope_gex])
app.include_router(vol_surface_router, dependencies=[_scope_gex])
# Options premium (extrinsic-value) surface — Beta. Derived analytics built
# from quoted option prices, redistributable on the same GEX scope as the
# vol surface.
app.include_router(premium_surface_router, dependencies=[_scope_gex])
app.include_router(gex_flip_horizon_router, dependencies=[_scope_gex])
# Gamma Regime Shift — the derivative of the dealer-gamma surface (what
# CHANGED between two snapshots, what expires next, and the classified read
# stored per session). Same derived-GEX scope as the surfaces it differences.
app.include_router(gamma_shift_router, dependencies=[_scope_gex])
# Option-chain routers — per-contract option history (option_contract) and
# the option-calculator (which embeds raw contract prices). Both enumerate
# the chain contract by contract, which is what MARKET_RAW exists to
# withhold, so they stay excluded from derived-only tiers.
app.include_router(option_contract_router, dependencies=[_scope_market_raw])
app.include_router(option_calculator_router, dependencies=[_scope_market_raw])
# Backtesting platform — premium (basic/pro) tier, same scope as /api/signals.
app.include_router(backtest_router, dependencies=[_scope_signals])
# Daily Scorecard — aggregates Action Cards + per-signal flip P&L into one
# trading day's recap. Powers the public /scorecard/{date} permalink and the
# 4:15 PM ET auto-tweet job. Same signals scope as /api/signals.
app.include_router(scorecard_router, dependencies=[_scope_signals])
# Daily Gamma Forecast — 7:00 AM ET commit + 4:05 PM ET receipt for the
# public /forecast/{date} page. Read-only here; the writer cron jobs live
# in src.jobs.forecast_writer and src.jobs.forecast_receipt.
app.include_router(forecast_router, dependencies=[_scope_signals])
# GEX Replay — scrubbable per-minute frames over historical gex_summary +
# gex_by_strike data. Read-only; no new ingestion. Scope matches the rest
# of the GEX surface (basic + pro tiers).
app.include_router(replay_router, dependencies=[_scope_gex])
# Forced Flow (Phase 3) — dealers' mechanically-forced hedging under scenarios of
# spot / time / vol, derived from the same OI the GEX surface uses. Options-flow
# analytics, so gated on the FLOW scope (same as /api/flow/series).
app.include_router(forced_flow_router, dependencies=[_scope_flow])

# TradeWorkz™ multi-bot signaled-trading engine — admin-tier surface. The
# frontend /trading-signals page is admin-gated in frontend/core/auth.ts so
# customers never reach the API even though the router is mounted here.
# Router-level guard is the SIGNALS scope; the /admin/* sub-endpoints add
# an additional require_scopes check inline.
app.include_router(tradeworkz_router, dependencies=[_scope_signals])

# Admin key-administration surface — mints/revokes the per-user API keys
# behind the website's self-service "Generate API Key" button. NOT scope-
# gated like the data routers: the router carries its own require_admin
# dependency (a dedicated X-Admin-Token shared secret, fail-closed) because
# it issues credentials and must not ride on the lenient data-plane scopes.
app.include_router(admin_api_keys_router)

# X-post review page (/admin/x-post): admin-token-gated, same rationale.
app.include_router(admin_xpost_router)

# CNBC market headlines — the same feed the Live-Bulletin auto-tweet is built
# from, surfaced for the website's "Top Headlines" dropdown and dashboard
# wire. Deliberately NOT scope-gated: market headlines are broadly
# redistributable, so it rides the app-wide api_key_auth only.
app.include_router(news_router)

# ============================================================================
# Health Check
# ============================================================================


# Liveness and readiness/deep health are split ON PURPOSE.
#
# /api/health/live answers only "is this worker process up and serving
# HTTP?" — with NO database dependency — and is what the systemd unit's
# ExecStartPost gate (and any process-liveness monitor) probes. The deep
# /api/health below reports DB reachability + data freshness for load
# balancers and uptime monitors, and returns 503 when the backend is degraded.
#
# Why the split matters: when the DB-touching /api/health gated the systemd
# readiness probe, a transient DB blip *during a (re)start* failed
# ExecStartPost; Restart=always then relaunched, and five such failures
# inside StartLimitIntervalSec latched the unit into start-limit-hit — i.e.
# seconds of DB trouble became a full API outage needing a human restart. A
# liveness probe must not depend on the database.
@app.get("/api/health/live", tags=["Health"])
async def liveness_probe():
    """Liveness probe: the process is up and able to serve HTTP. No DB call.

    Public (see ``security._PUBLIC_PATHS``) and dependency-free so it cannot
    401, 503, or hang on the database — the properties a systemd/ELB liveness
    gate needs. Deep DB + freshness health lives at ``/api/health``.
    """
    return {"status": "alive"}


@app.get("/api/health", response_model=HealthStatus, tags=["Health"])
async def health_check(response: Response):
    """Check API and database health (DEEP — touches the DB).

    Returns HTTP 200 only when the database is reachable. A degraded
    backend (db_manager.check_health() returns False) surfaces as HTTP
    503 with the same response body so uptime monitors, load balancers,
    and Kubernetes probes can act on the status code — the previous
    behavior returned 200 with ``status="degraded"`` and was treated as
    healthy by every standard probe.

    For a process-liveness signal that does NOT depend on the DB (e.g. the
    systemd ExecStartPost gate), use ``/api/health/live`` instead.
    """
    try:
        db = _db()
        # DB connectivity is the fitness signal. check_health() is bounded
        # (see DatabaseManager.check_health) so a saturated pool fails this
        # fast instead of hanging the probe.
        is_healthy = await db.check_health()

        if not is_healthy:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return HealthStatus(
                status="degraded",
                database_connected=False,
                last_data_update=None,
                data_age_seconds=None,
            )

        # Data freshness is BEST-EFFORT: a slow or failed freshness read must
        # not turn a reachable backend into a 503, and must not hang the
        # response, so it is bounded and its failure is swallowed.
        last_update = None
        data_age_seconds = None
        try:
            last_quote = await asyncio.wait_for(db.get_latest_quote(), timeout=3.0)
            last_update = last_quote["timestamp"] if last_quote else None
            if last_update:
                et_tz = pytz.timezone("US/Eastern")
                now = datetime.now(et_tz)
                data_age_seconds = int((now - last_update).total_seconds())
        except Exception:
            logger.warning("health: data-freshness read unavailable", exc_info=True)

        return HealthStatus(
            status="healthy",
            database_connected=True,
            last_data_update=last_update,
            data_age_seconds=data_age_seconds,
        )
    except Exception as e:
        logger.error(f"Health check failed: {e!r}")
        raise HTTPException(status_code=503, detail="Service unavailable")


# ============================================================================
# GEX Endpoints
# ============================================================================


@app.get("/api/gex/summary", response_model=GEXSummary, tags=["GEX"], dependencies=[_scope_gex])
@handle_api_errors("GET /api/gex/summary")
async def get_gex_summary(symbol: str = Query(default="SPY")):
    """Get latest GEX summary"""
    data = await _db().get_latest_gex_summary(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="No GEX data available")
    return GEXSummary(**data)


@app.get(
    "/api/gex/pin-stability",
    response_model=PinStabilityResponse,
    tags=["GEX"],
    dependencies=[_scope_gex],
)
@handle_api_errors("GET /api/gex/pin-stability")
async def get_pin_stability(symbol: str = Query(default="SPY")):
    """How stable the Pin Strike has been across the current session.

    Kept off ``/api/gex/summary`` on purpose: that endpoint is a single-row
    read on a hot cached path, and folding a whole-session scan into it would
    make every chart poll pay for a figure that changes at most once a minute.
    404 when the session carries no active pin at any point — the client then
    shows nothing rather than a zeroed record.
    """
    frames = await _db().get_pin_path_for_session(symbol)
    stability = build_pin_stability(frames)
    if stability is None:
        raise HTTPException(status_code=404, detail="No pin activity for this session")
    return PinStabilityResponse(symbol=symbol.upper(), **stability.to_dict())


@app.get(
    "/api/gex/by-strike",
    response_model=List[GEXByStrike],
    tags=["GEX"],
    dependencies=[_scope_gex],
)
@handle_api_errors("GET /api/gex/by-strike")
async def get_gex_by_strike(
    symbol: str = Query(default="SPY"),
    limit: int = Query(default=50, le=200),
    sort_by: str = Query(
        default="distance",
        pattern="^(distance|impact)$",
        description="Sort by 'distance' (closest to spot) or 'impact' (highest absolute net GEX)",
    ),
):
    """
    Get GEX breakdown by strike

    Returns detailed gamma exposure data including vanna/charm for each strike.

    - sort_by=distance: Returns strikes closest to current spot price (default)
    - sort_by=impact: Returns strikes with highest absolute net GEX (like 'make gex-strikes')
    """
    data = await _db().get_gex_by_strike(symbol, limit, sort_by)
    return [GEXByStrike(**row) for row in data]


@app.get("/api/gex/profile", response_model=GEXProfile, tags=["GEX"], dependencies=[_scope_gex])
@handle_api_errors("GET /api/gex/profile")
async def get_gex_profile(symbol: str = Query(default="SPY")):
    """Latest spot-shift dealer dollar-gamma curve for ``symbol``.

    Returns the curve persisted by the Analytics Engine on the most
    recent cycle: an ascending ``[(price, gex), ...]`` series whose
    zero crossing is ``gamma_flip`` and whose value at ``spot_price``
    is ``net_gex_at_spot``.  Designed for the GEX-Profile overlay on
    the per-strike chart; the same dataset that drives the headline
    flip / net-at-spot figures already in /api/gex/summary.
    """
    data = await _db().get_latest_gex_profile(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="No GEX profile data available")
    return GEXProfile(**data)


@app.get(
    "/api/gex/historical-context",
    response_model=GEXHistoricalContext,
    tags=["GEX"],
    dependencies=[_scope_gex],
)
@handle_api_errors("GET /api/gex/historical-context")
async def get_gex_historical_context(symbol: str = Query(default="SPY")):
    """Historical-distribution context for the live headline GEX metrics.

    For each of ``net_gex_at_spot`` and ``total_net_gex``, returns the
    current value plus the rolling-30-day and all-time distributions
    (p05/p25/p50/p75/p95, mean/std, min/max, sample size), the live value's
    interpolated percentile and z-score against each window, and a regime
    label (``record_high`` / ``extreme_high`` / ``elevated`` / ``normal`` /
    ``low`` / ``extreme_low`` / ``record_low`` / ``unknown``).

    Distribution rows are produced nightly by
    ``src.tools.gex_historical_stats_refresh`` and bucketed by 5-minute ET
    RTH bucket so the EOD-pinning seasonality doesn't dominate the
    comparison.  Falls back to a flat (no TOD bucketing) distribution when
    a specific bucket is thin.

    Returns 404 if no ``gex_summary`` row exists for the symbol yet.
    """
    data = await _db().get_gex_historical_context(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="No GEX historical context available")
    return GEXHistoricalContext(**data)


@app.get(
    "/api/gex/historical",
    response_model=List[GEXSummary],
    tags=["GEX"],
    dependencies=[_scope_gex],
)
async def get_historical_gex(
    symbol: str = Query(default="SPY"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    window_units: int = Query(default=90, ge=1, le=90),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
):
    """Get historical GEX data"""
    try:
        # Parse dates if provided
        start_dt = datetime.fromisoformat(start_date) if start_date else None
        end_dt = datetime.fromisoformat(end_date) if end_date else None

        data = await _db().get_historical_gex(symbol, start_dt, end_dt, window_units, timeframe)
        return [GEXSummary(**row) for row in data]
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error fetching historical GEX: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/gex/heatmap", tags=["GEX"], dependencies=[_scope_gex])
@handle_api_errors("GET /api/gex/heatmap")
async def get_gex_heatmap(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="5min"),
    window_units: int = Query(default=60, ge=1, le=300),
):
    """Get GEX heatmap data (strike x time)"""
    data = await _db().get_gex_heatmap(symbol, timeframe, window_units)
    return data or []


@app.get(
    "/api/gex/expirations",
    response_model=List[date_type],
    tags=["GEX"],
    dependencies=[_scope_gex],
)
@handle_api_errors("GET /api/gex/expirations")
async def get_gex_expirations(
    symbol: str = Query(default="SPY"),
    lookback_hours: int = Query(default=24, ge=1, le=168),
):
    """Distinct option expirations seen in ``gex_by_strike`` within the
    trailing ``lookback_hours``.  Powers the Strike-Profile chart's
    expiry dropdown so today's expiration stays available even after
    market close (when the latest ``/api/gex/by-strike`` snapshot has
    dropped it — the analytics engine stops writing rows for expired
    contracts).  Default 24h covers a post-close shift; cap at 168h
    (1 week) bounds the scan."""
    data = await _db().get_gex_expirations(symbol, lookback_hours)
    return data


@app.get(
    "/api/gex/strike-profile-timeseries",
    response_model=List[StrikeProfileBucket],
    tags=["GEX"],
    dependencies=[_scope_gex],
)
@handle_api_errors("GET /api/gex/strike-profile-timeseries")
async def get_strike_profile_timeseries(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min"] = Query(default="1min"),
    window_units: int = Query(default=78, ge=1, le=480),
    expirations: str = Query(
        default="all",
        description=(
            "'all' to aggregate strikes across every expiration (same basis as "
            "the live /api/gex/by-strike with Expiry All), or a comma-separated "
            "list of YYYY-MM-DD expiration dates to restrict the strikes "
            "payload to that set (summed across the set)."
        ),
    ),
):
    """Aligned per-bucket Strike-Profile timeseries used by the rewind chart.

    Returns ``window_units`` time buckets ASCENDING (most recent last) for
    ``symbol`` at the requested ``timeframe``.  Each bucket carries:

      * the underlying OHLC inside the bucket (from ``underlying_quotes``);
      * the bucket's ``gamma_flip`` from the representative
        ``gex_summary`` row;
      * ``call_wall`` / ``put_wall`` computed live for the bucket from
        the same (expiration-filtered, summed-by-strike) gamma rows the
        bucket's bars render, against the bucket's own close — via the
        canonical :func:`src.analytics.walls.compute_call_put_walls`
        helper (single source of record).  ``expirations=all`` yields
        the cross-expiration aggregate walls (matches
        ``/api/gex/summary``); a specific set yields walls scoped to
        that set's gamma alone;
      * ``pin_strike`` / ``pin_confidence`` as of the bucket's close,
        from the representative ``gex_summary`` row.  Unlike the walls
        and the flip these are NOT scoped by ``expirations``: the pin is
        0DTE-by-construction and whole-chain by definition, so it reads
        the same in every expiration scope (matching the live surfaces,
        where the pin does not move with the Expiry selector).  Both are
        ``null`` when the bucket has no active pin and on rows written
        before the pin columns shipped — draw no line, never a zero;
      * every strike's gamma exposure in the same dollar-GEX units
        ``/api/gex/by-strike`` uses (``γ × OI × 100 × S² × 0.01``),
        evaluated against the bucket's own ``close`` so the surface
        matches each bucket's candlestick.

    ``expirations=all`` sums strike gamma / OI across all expirations per
    (bucket, strike).  A comma-separated list of ``YYYY-MM-DD`` dates
    restricts the strikes payload to that set (summed across the set).
    For a specific set the bucket ``gamma_flip`` is recomputed from the
    summed-by-strike gamma (the aggregate spot-shift flip can't be rebuilt
    for a subset); ``all`` keeps the canonical ``gex_summary`` flip.  Names
    in the strikes payload follow the request shape (``call_gamma`` /
    ``put_gamma`` / ``net_gamma``) but the values are the dollar-GEX
    quantities, not raw gamma — see the per-row docstring on
    ``StrikeProfileStrike``.  Any unparseable entry is a 400.
    """
    expiration_dates: Optional[List[date_type]] = None
    raw = (expirations or "all").strip()
    if raw.lower() != "all":
        parsed: List[date_type] = []
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                parsed.append(date_type.fromisoformat(part))
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "expirations must be 'all' or a comma-separated list "
                        "of YYYY-MM-DD expiration dates"
                    ),
                )
        # An all-blank list (e.g. ``expirations=,``) collapses to All rather
        # than a chart-blanking empty filter.
        expiration_dates = parsed or None

    # Return the raw bucket dicts and let ``response_model`` do the single
    # validation + encode pass. Building StrikeProfileBucket instances here was
    # a redundant round-trip: FastAPI's serializer calls model_dump() on
    # whatever it is handed and re-validates that back into the response field,
    # so constructing them up front made it dict -> model -> dict -> model.
    # Dropping it is strictly less work on the largest response this API
    # serves, though measurement puts the request's peak heap in the
    # serialisation that follows, so expect a smaller allocation churn rather
    # than a lower high-water mark.
    #
    # Same JSON either way — verified byte-for-byte, including Decimal
    # precision and nulls. The model's json_encoders still run: they apply to
    # the field FastAPI validates into, not to the object the endpoint returns.
    return await _db().get_strike_profile_timeseries(
        symbol, timeframe, window_units, expiration_dates
    )


# ============================================================================
# Options Flow Endpoints
# ============================================================================


@app.get(
    "/api/flow/by-contract",
    response_model=List[FlowPoint],
    tags=["Options Flow"],
    dependencies=[_scope_flow],
)
@handle_api_errors("GET /api/flow/by-contract")
async def get_flow_by_contract(
    symbol: str = Query(default="SPY"),
    session: str = Query(default="current", pattern="^(current|prior)$"),
    intervals: Optional[int] = Query(
        default=None,
        ge=1,
        le=390,
        description=(
            "Number of trailing 5-minute buckets to return. Defaults to the "
            "entire session (09:30–16:15 ET, ~81 buckets). Capped at 390 "
            "(one trading day at 1-minute resolution) to bound DB load."
        ),
    ),
):
    """Per-contract option flow in 5-min buckets with session-cumulative values.

    Returns one row per (option_type, strike, expiration) per 5-min bucket.
    raw_volume, raw_premium, net_volume and net_premium are day-to-date
    cumulative for each contract as of the end of its bucket; counters reset
    at 09:30 ET (TradeStation RTH open).

    session=current returns today's open session (or most recent if closed);
    session=prior returns the previous full session. Pass intervals=N to
    limit the response to the most recent N 5-minute buckets.
    """
    data = await _db().get_flow(symbol, session, intervals=intervals)
    return [FlowPoint(**row) for row in data]


_FLOW_SYMBOL_PATTERN = re.compile(r"^[A-Z.]{1,10}$")


def _parse_flow_strikes(raw: Optional[str]) -> Optional[List[float]]:
    """Parse the ?strikes= CSV into a list of floats.

    Silently drops unparseable entries. Returns ``None`` for missing or
    empty input (meaning "no strike filter"). Raises ``HTTPException(400)``
    only when every supplied entry is unparseable — an all-bad filter is a
    client error, not an accidental no-op.
    """
    if raw is None:
        return None
    trimmed = raw.strip()
    if not trimmed:
        return None
    parts = [p.strip() for p in trimmed.split(",") if p.strip()]
    if not parts:
        return None
    parsed: List[float] = []
    for part in parts:
        try:
            value = float(part)
        except ValueError:
            continue
        if value != value or value in (float("inf"), float("-inf")):  # NaN / inf
            continue
        parsed.append(value)
    if not parsed:
        raise HTTPException(
            status_code=400, detail="strikes must contain at least one finite number"
        )
    return parsed


_FLOW_EXPIRATION_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_flow_expirations(raw: Optional[str]) -> Optional[List[date_type]]:
    """Parse the ?expirations= CSV into a list of dates.

    Silently drops entries that don't match ``YYYY-MM-DD`` or aren't real
    calendar dates. Returns ``None`` for missing/empty input. Raises 400
    only when every entry is unparseable.
    """
    if raw is None:
        return None
    trimmed = raw.strip()
    if not trimmed:
        return None
    parts = [p.strip() for p in trimmed.split(",") if p.strip()]
    if not parts:
        return None
    parsed: List[date_type] = []
    for part in parts:
        if not _FLOW_EXPIRATION_PATTERN.match(part):
            continue
        try:
            parsed.append(date_type.fromisoformat(part))
        except ValueError:
            continue
    if not parsed:
        raise HTTPException(
            status_code=400,
            detail="expirations must contain at least one valid YYYY-MM-DD date",
        )
    return parsed


def _format_flow_series_row(row: dict) -> dict:
    """Coerce a raw DB row into the JSON shape documented in the spec.

    The timestamp fields are emitted as ``...Z`` (trailing-Z UTC) — spec
    requirement. Decimal/Numeric columns are cast to float so JSON callers
    don't have to care about asyncpg's native Decimal output.
    """
    bar_start: datetime = row["bar_start"]
    if bar_start.tzinfo is None:
        bar_start = bar_start.replace(tzinfo=pytz.UTC)
    else:
        bar_start = bar_start.astimezone(pytz.UTC)
    bar_end = bar_start + timedelta(minutes=5)
    fmt = "%Y-%m-%dT%H:%M:%SZ"

    def _to_float(v):
        return float(v) if v is not None else None

    def _to_int(v):
        return int(v) if v is not None else 0

    return {
        "timestamp": bar_start.strftime(fmt),
        "bar_start": bar_start.strftime(fmt),
        "bar_end": bar_end.strftime(fmt),
        "call_premium_cum": _to_float(row.get("call_premium_cum")) or 0.0,
        "put_premium_cum": _to_float(row.get("put_premium_cum")) or 0.0,
        "call_volume_cum": _to_int(row.get("call_volume_cum")),
        "put_volume_cum": _to_int(row.get("put_volume_cum")),
        "net_volume_cum": _to_int(row.get("net_volume_cum")),
        "raw_volume_cum": _to_int(row.get("raw_volume_cum")),
        "call_position_cum": _to_int(row.get("call_position_cum")),
        "put_position_cum": _to_int(row.get("put_position_cum")),
        "net_premium_cum": _to_float(row.get("net_premium_cum")) or 0.0,
        "put_call_ratio": _to_float(row.get("put_call_ratio")),
        "underlying_price": _to_float(row.get("underlying_price")),
        "contract_count": _to_int(row.get("contract_count")),
        "is_synthetic": bool(row.get("is_synthetic")),
    }


@app.get(
    "/api/flow/market-tide",
    response_model=MarketTideResponse,
    tags=["Options Flow"],
    dependencies=[_scope_flow],
)
@handle_api_errors("GET /api/flow/market-tide")
async def get_market_tide(
    window: MarketTideWindow = Query(
        default=MarketTideWindow.FIFTEEN_MINUTES,
        description="Directional premium lookback in minutes.",
    ),
):
    """Market-wide directional options flow adjusted by dealer gamma.

    The score is withheld when fewer than 60% of active symbols have fresh
    gamma and flow inputs. Component leaders/laggards explain which names are
    driving the reading.
    """
    return await _db().get_market_tide(window_minutes=int(window))


@app.get(
    "/api/flow/market-tide/history",
    response_model=MarketTideHistoryResponse,
    tags=["Options Flow"],
    dependencies=[_scope_flow],
)
@handle_api_errors("GET /api/flow/market-tide/history")
async def get_market_tide_history(
    window: MarketTideWindow = Query(
        default=MarketTideWindow.FIFTEEN_MINUTES,
        description="Directional premium lookback in minutes.",
    ),
    mode: Literal["intraday", "daily"] = Query(
        default="intraday",
        description="'intraday' = the most recent session's 5-minute series; "
        "'daily' = one close per session.",
    ),
    days: int = Query(
        default=30,
        ge=1,
        le=90,
        description="Trailing sessions to return in daily mode (ignored for intraday).",
    ),
):
    """Persisted Market Tide series for the trend chart.

    A pure cache read over the snapshot table: ``intraday`` returns the most
    recent session's 5-minute series (frozen at the 16:00 ET close after
    hours), ``daily`` returns the last ``days`` session closes. Empty until
    the refresher/backfill has written snapshots.
    """
    return await _db().get_market_tide_history(window_minutes=int(window), mode=mode, days=days)


@app.get(
    "/api/flow/series",
    response_model=List[FlowSeriesPoint],
    tags=["Options Flow"],
    dependencies=[_scope_flow],
)
@handle_api_errors("GET /api/flow/series")
async def get_flow_series(
    symbol: str = Query(..., min_length=1, max_length=10),
    session: Literal["current", "prior"] = Query(default="current"),
    strikes: Optional[str] = Query(
        default=None,
        description="Comma-separated strikes to include. Empty/missing = all strikes.",
    ),
    expirations: Optional[str] = Query(
        default=None,
        description="Comma-separated YYYY-MM-DD expirations to include. Empty/missing = all.",
    ),
    intervals: Optional[int] = Query(
        default=None,
        ge=1,
        le=390,
        description=(
            "If provided, return only the last N 5-minute bars (tail window) "
            "for cheap incremental polling. A full regular session is 81 bars."
        ),
    ),
):
    """Server-accumulated flow series — one row per 5-minute bar.

    Returns cumulative call/put premium, volume, position, net volume, and
    put/call ratio per bar across all contracts matching the optional
    ``strikes``/``expirations`` filters. Rows are contiguous (quiet bars
    carry forward as synthetic rows flagged by ``is_synthetic``). Frontend
    renders this series directly — no client-side accumulators.

    Rows are ordered newest → oldest so ``rows[0]`` is the most recent bar.
    With ``intervals=N`` you get the leading N rows (the trailing-N most
    recent bars).

    ``session=current`` is the most recent ET trading day that has any data
    for the symbol; ``session=prior`` is the ET day immediately before that.
    Unknown symbols return 404; symbols that exist but have no data for the
    requested session return 200 with ``[]``.
    """
    normalized = symbol.strip().upper()
    if not _FLOW_SYMBOL_PATTERN.match(normalized):
        raise HTTPException(
            status_code=400,
            detail="symbol must match [A-Z.]{1,10} (letters and dots only, up to 10 chars)",
        )

    strikes_list = _parse_flow_strikes(strikes)
    expirations_list = _parse_flow_expirations(expirations)

    rows = await _db().get_flow_series(
        symbol=normalized,
        session=session,
        strikes=strikes_list,
        expirations=expirations_list,
        intervals=intervals,
    )
    if rows is None:
        raise HTTPException(status_code=404, detail="symbol not found")
    return JSONResponse(content=[_format_flow_series_row(r) for r in rows])


@app.get(
    "/api/flow/contracts",
    response_model=FlowContractsResponse,
    tags=["Options Flow"],
    dependencies=[_scope_flow],
)
@handle_api_errors("GET /api/flow/contracts")
async def get_flow_contracts(
    symbol: str = Query(..., min_length=1, max_length=10),
    session: Literal["current", "prior"] = Query(default="current"),
):
    """Distinct strikes and expirations that traded in the resolved session.

    Powers the Strike / Expiration filter chips on the Flow Analysis page.
    Companion to ``/api/flow/series``: same session resolution, same 404
    semantics for unknown symbols.
    """
    normalized = symbol.strip().upper()
    if not _FLOW_SYMBOL_PATTERN.match(normalized):
        raise HTTPException(
            status_code=400,
            detail="symbol must match [A-Z.]{1,10} (letters and dots only, up to 10 chars)",
        )

    result = await _db().get_flow_contracts(symbol=normalized, session=session)
    if result is None:
        raise HTTPException(status_code=404, detail="symbol not found")
    return FlowContractsResponse(**result)


@app.get(
    "/api/flow/smart-money",
    response_model=List[SmartMoneyFlowPoint],
    tags=["Options Flow"],
    dependencies=[_scope_flow],
)
@handle_api_errors("GET /api/flow/smart-money")
async def get_smart_money_flow(
    symbol: str = Query(default="SPY"),
    session: str = Query(default="current", pattern="^(current|prior)$"),
    limit: int = Query(default=50, ge=1, le=50),
):
    """Get legacy ``smart-money`` premium-flow fields — 1-min intervals.

    These backward-compatible fields contain aggressor-classified premium;
    premium and quote-side classification do not establish opening/closing
    status, ultimate ownership, strategy, or information advantage.
    Session runs 07:15–16:15 ET. session=current returns today's open session
    (or most recent if closed); session=prior returns the previous full session.
    """
    data = await _db().get_smart_money_flow(symbol, session, min(limit, 50))
    return [SmartMoneyFlowPoint(**row) for row in data]


@app.get(
    "/api/flow/buying-pressure",
    response_model=List[FlowBuyingPressurePoint],
    tags=["Options Flow"],
    dependencies=[_scope_flow],
)
@handle_api_errors("GET /api/flow/buying-pressure")
async def get_flow_buying_pressure(
    symbol: str = Query(default="SPY"), limit: int = Query(default=20, ge=1, le=500)
):
    """Get underlying buying/selling pressure"""
    data = await _db().get_flow_buying_pressure(symbol, limit)
    return [FlowBuyingPressurePoint(**row) for row in data] if data else []


# ============================================================================
# Market Session Helper
# ============================================================================

from src.market_calendar import (  # noqa: E402
    ET as _ET,
    NYSE_HOLIDAYS as _NYSE_HOLIDAYS,
    should_display_future,
    is_futures_session_open,
    current_cash_close_reference,
)
from src.config import _getenv_bool, _getenv_float  # noqa: E402
from src.symbols import resolve_futures_index, resolve_index_future  # noqa: E402

_SOFT_CLOSE_WINDOW = timedelta(seconds=30)


def _index_futures_display_enabled() -> bool:
    """Read-side master switch for the index→future display swap.

    Read per-call (cheap env lookup) so an operator can flip the feature on
    or off without restarting the API, once the futures ingester has
    populated ``futures_quotes``.
    """
    return _getenv_bool("INDEX_FUTURES_DISPLAY_ENABLED", False)


async def _native_futures_session_closes(futures_symbol: str, index_symbol: str) -> SessionCloses:
    """The future's own two most recent 16:00 ET closes.

    Mirrors :func:`_native_futures_quote`: the daily-change denominator has to
    be the future's own prior close, not the cash index's, or the headline
    percentage is wrong by the whole basis.
    """
    data = await _db().get_futures_session_closes(index_symbol)
    if not data or data.get("current_session_close") is None:
        raise HTTPException(
            status_code=404, detail=f"No {futures_symbol} session close data available"
        )
    return SessionCloses(**{**data, "symbol": futures_symbol})


# A futures bar older than this while CME is open means the feed is dead, not
# that the market is quiet — 1-minute bars print continuously through the
# session.
_FUTURES_QUOTE_STALE_MINUTES = _getenv_float("FUTURES_QUOTE_STALE_MINUTES", 5.0)


async def _native_futures_quote(futures_symbol: str, index_symbol: str) -> UnderlyingQuote:
    """Latest observed bar for a first-class future (ES / NQ).

    ES and NQ take their PRICE from their own feed rather than from a
    projection of the index: overnight the cash index is frozen at its 16:00
    close, so a projected price would report where ES stood at the bell
    instead of where it is trading now.  (Their dealer LEVELS are still
    SPX/NDX-derived and projected — see ``src/api/futures_middleware.py``.)
    """
    fut = await _db().get_latest_future_quote(index_symbol, current_cash_close_reference())
    if not fut or fut.get("close") is None:
        raise HTTPException(status_code=404, detail=f"No {futures_symbol} quote data available")
    payload = {
        key: value
        for key, value in fut.items()
        if key
        in ("timestamp", "open", "high", "low", "close", "up_volume", "down_volume", "volume")
    }
    payload["symbol"] = futures_symbol

    # ``session`` describes the MARKET, not the feed.
    #
    # This used to fold feed staleness into the session — a stale bar was
    # reported "closed" so the frontend would stop merging it onto the live
    # chart tip (see isSessionLive). That was the wrong lever: the frontend's
    # "closed" branch does not merely stop merging, it REPLACES the headline
    # price with ``current_session_close`` and its change with
    # ``prior_session_close``. For a 23-hour instrument that means swapping a
    # slightly-late ES print for the last 16:00 cash close and publishing that
    # session's day change as today's. On 2026-08-24 the header read
    # "ES $7692.00 +26.75 (+0.35%)" — Friday's close and Friday's change —
    # while ES traded 7675.50 and the feed had a live 7677.25 print in hand.
    #
    # A late observed print is always closer to the truth than a three-day-old
    # one. Serve it, label the session honestly, and describe the staleness
    # separately so each consumer can make its own decision.
    bar_ts = payload.get("timestamp")
    age_seconds: Optional[int] = None
    if bar_ts is not None:
        age_seconds = max(0, int((datetime.now(timezone.utc) - bar_ts).total_seconds()))
    payload["session"] = "open" if is_futures_session_open() else "closed"
    payload["data_age_seconds"] = age_seconds
    payload["stale"] = age_seconds is not None and age_seconds > _FUTURES_QUOTE_STALE_MINUTES * 60.0
    return UnderlyingQuote(**payload)


def _future_display_label(future_symbol: Optional[str]) -> Optional[str]:
    """UI ticker for a TradeStation continuous future (``@ES`` → ``ES``)."""
    if not future_symbol:
        return None
    return future_symbol.lstrip("@").upper() or None


if not _NYSE_HOLIDAYS:
    logger.warning("NYSE_HOLIDAYS env var is empty — no holiday filtering will occur")


class _SoftCloseTracker:
    """Rolling window of the last 3 close prices for a symbol.

    Used to evaluate soft-close stability: if the last 3 consecutive
    price observations are all identical the price is considered stable
    and the session can transition to 'closed'.
    """

    __slots__ = ("_prices",)

    def __init__(self) -> None:
        self._prices: deque = deque(maxlen=3)

    def record(self, price) -> None:
        if price is not None:
            self._prices.append(price)

    def is_stable(self) -> bool:
        """True when 3 consecutive identical prices have been observed."""
        return len(self._prices) >= 3 and len(set(self._prices)) == 1


# Per-symbol soft-close trackers (populated lazily on first quote request)
_soft_close_trackers: dict[str, _SoftCloseTracker] = {}
_SOFT_CLOSE_TRACKER_MAX = 100  # prevent unbounded growth


def get_market_session(
    asset_type: Optional[str],
    price_is_stable: bool = False,
    close_data_available: bool = True,
) -> str:
    """Return the current US equity market session label.

    Session boundaries (all times US/Eastern, exact to the second):

      Both types
        < 04:00:00            closed
        >= 20:00:30           closed
        weekends / holidays   closed

      non-INDEX only
        04:00:00 – 09:29:59   pre-market
        16:00:00 – 19:59:59   after-hours
        20:00:00 – 20:00:29   after-hours (soft close: closed once price_is_stable)

      INDEX only
        16:00:00 – 16:00:29   open (soft close: closed once price_is_stable)
        16:00:30 – 19:59:59   closed

      Both types
        09:30:00 – 15:59:59   open

    ``close_data_available`` gates the wall-clock-driven transition out of
    the cash session.  Wall-clock says "the market closed at 16:00 ET" but
    the frontend's after-hours rendering pulls ``current_session_close``
    from ``/api/market/session-closes`` — if today's close hasn't yet been
    ingested when this endpoint flips ``session``, the two endpoints
    disagree about reality and the header briefly renders yesterday's
    daily change.  Gating the transition on the data layer's view of the
    same observable keeps both endpoints structurally consistent: the
    session label only advances once the close has been observed.  See
    ``DatabaseClient.has_todays_close_landed`` for the signal definition.
    """
    now_et = datetime.now(_ET)
    today = now_et.date()

    if today.weekday() >= 5 or today in _NYSE_HOLIDAYS:
        return "closed"

    def _boundary(h: int, m: int, s: int = 0) -> datetime:
        return _ET.localize(  # type: ignore[no-any-return]
            datetime(today.year, today.month, today.day, h, m, s)
        )

    pre_open_dt = _boundary(4, 0)
    market_open_dt = _boundary(9, 30)
    market_close_dt = _boundary(16, 0)
    ah_close_dt = _boundary(20, 0)

    is_index = asset_type == "INDEX"

    # Before pre-market
    if now_et < pre_open_dt:
        return "closed"

    # Pre-market (non-INDEX only)
    if pre_open_dt <= now_et < market_open_dt:
        return "pre-market" if not is_index else "closed"

    # Cash session — open for both types
    if market_open_dt <= now_et < market_close_dt:
        return "open"

    # Past wall-clock close.  Hold "open" until the data layer confirms
    # today's close has been observed, so /api/market/quote and
    # /api/market/session-closes can't disagree about whether today's
    # close is available.  Once the gate opens, fall through to the
    # existing post-close branches below.
    if not close_data_available:
        return "open"

    # Soft-close window at market close
    if market_close_dt <= now_et < market_close_dt + _SOFT_CLOSE_WINDOW:
        if is_index:
            # INDEX: soft close from 16:00:00 — closed once price is stable
            return "closed" if price_is_stable else "open"
        else:
            # non-INDEX: hard transition to after-hours at exactly 16:00:00
            return "after-hours"

    # [16:00:30, 20:00:00) window
    if market_close_dt + _SOFT_CLOSE_WINDOW <= now_et < ah_close_dt:
        return "closed" if is_index else "after-hours"

    # Soft-close window at after-hours close (non-INDEX only)
    if ah_close_dt <= now_et < ah_close_dt + _SOFT_CLOSE_WINDOW:
        if is_index:
            return "closed"
        return "closed" if price_is_stable else "after-hours"

    return "closed"


# ============================================================================
# Market Data Endpoints
# ============================================================================


@app.get(
    "/api/market/quote",
    dependencies=[_scope_market_reference],
    response_model=UnderlyingQuote,
    response_model_exclude_none=True,
    tags=["Market Data"],
)
async def get_current_quote(symbol: str = Query(default="SPY")):
    """Get current underlying quote.

    Returns the live tick (the latest ``underlying_quotes`` bar of any
    session). This is what every "what is it trading at right now"
    surface needs: the header's extended-hours row, the GEX live spot,
    the chart's tip-close merge, the strike-profile spot line.

    The header's HEADLINE price during AH / pre-market / closed /
    weekend is sourced separately from ``/api/market/session-closes``
    (which applies the asset-aware 16:00 cash-close rule); the frontend
    multiplexes between the two via ``getPrimaryPriceChangeSummary``.
    Do NOT collapse this endpoint into the cash-close path — that
    freezes the extended-hours ticker at the cash close on every
    surface that reads ``quoteData.close``.
    """
    try:
        # ES / NQ are first-class symbols served from their own bars.
        futures_index = resolve_futures_index(symbol)
        if futures_index:
            return await _native_futures_quote(symbol.strip().upper(), futures_index)

        data = await _db().get_latest_quote(symbol)
        if not data:
            raise HTTPException(status_code=404, detail="No quote data available")

        data = dict(data)
        asset_type = data.pop("asset_type", None)
        if "cumulative_daily_volume" in data:
            data["volume"] = data.pop("cumulative_daily_volume")

        # Update per-symbol soft-close tracker and evaluate stability
        # Evict oldest entries if tracker dict grows too large
        if (
            symbol not in _soft_close_trackers
            and len(_soft_close_trackers) >= _SOFT_CLOSE_TRACKER_MAX
        ):
            oldest_key = next(iter(_soft_close_trackers))
            del _soft_close_trackers[oldest_key]
        tracker = _soft_close_trackers.setdefault(symbol, _SoftCloseTracker())
        tracker.record(data.get("close"))

        close_data_available = await _db().has_todays_close_landed(symbol, asset_type)
        data["session"] = get_market_session(asset_type, tracker.is_stable(), close_data_available)

        # Index→future DISPLAY swap (ADDITIVE): during the overnight futures
        # window, attach the future's price as *separate* fields. The base
        # quote (close/open/high/low/session) stays the cash index, so every
        # downstream consumer of this endpoint — GEX spot, greeks, options
        # calculator, heatmap — is untouched. Only the header quote, the quote
        # card, and the candlestick chart read the futures_* fields. Silently
        # omitted if the futures ingester has no rows yet.
        if _index_futures_display_enabled() and should_display_future(symbol):
            fut = await _db().get_latest_future_quote(symbol, current_cash_close_reference())
            if fut and fut.get("close") is not None:
                data["display_source"] = "futures"
                data["data_symbol"] = _future_display_label(fut.get("future_symbol"))
                data["futures_close"] = fut.get("close")
                data["futures_reference_close"] = fut.get("reference_close")

        return UnderlyingQuote(**data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching quote: {e!r}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get(
    "/api/market/session-closes",
    response_model=SessionCloses,
    tags=["Market Data"],
    dependencies=[_scope_market_reference],
)
@handle_api_errors("GET /api/market/session-closes")
async def get_session_closes(symbol: str = Query(default="SPY")):
    """
    Get the two most recently completed regular session closes.

    - current_session_close: the most recent cash session close (last bar <= 16:00 ET
      on the most recent completed trading day).
    - prior_session_close: the session close immediately before current.
    """
    # ES / NQ close against their OWN 16:00 prints. Projecting SPX's cash
    # closes here would put the headline daily change on the wrong basis —
    # the number a futures trader checks first.
    futures_index = resolve_futures_index(symbol)
    if futures_index:
        return await _native_futures_session_closes(symbol.strip().upper(), futures_index)

    data = await _db().get_session_closes(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="No session close data available")
    return SessionCloses(**data)


@app.get(
    "/api/market/session-levels",
    response_model=SessionLevels,
    tags=["Market Data"],
    dependencies=[_scope_market_reference],
)
@handle_api_errors("GET /api/market/session-levels")
async def get_session_levels(symbol: str = Query(default="SPY")):
    """
    Get pre-market and previous-session high/low levels for a symbol.

    Non-index symbols only (ETFs/equities such as SPY, QQQ) — cash indexes
    have no pre-market print, so they return ``is_index: true`` with null
    levels and no 404.

    - premarket_high / premarket_low: high/low of today's 04:00-09:30 ET
      pre-market session (live-updating while the pre-market is in
      progress, final after the open).
    - prev_session_high / prev_session_low: high/low of the previous
      trading day's regular session (09:30-16:00 ET), including the
      closing auction print.

    Levels roll at the start of each new pre-market session (04:00 ET),
    not at the close — the same anchoring traders use for PDH/PDL lines.
    Source of record is the ``session_levels`` capture job; the endpoint
    falls back to a live 1-min-bar aggregate when no captured row exists.
    """
    # A future trades ~23h a day, so "pre-market high/low" describes nothing
    # for ES / NQ. Return the empty shape rather than projecting SPX's levels,
    # which would draw cash-index lines on a futures chart.
    if resolve_futures_index(symbol):
        return SessionLevels(symbol=symbol.strip().upper(), is_index=False)

    data = await _db().get_session_levels(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="No session level data available")
    return SessionLevels(**data)


@app.get(
    "/api/market/historical",
    response_model=List[UnderlyingQuote],
    tags=["Market Data"],
    dependencies=[_scope_market_reference],
)
async def get_historical_quotes(
    symbol: str = Query(default="SPY"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    window_units: int = Query(default=192, ge=1, le=576),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    allow_futures: bool = Query(
        default=False,
        description=(
            "Opt-in: when true and outside the cash session, return the cash "
            "index's futures bars instead of the (frozen) index series. Only "
            "the candlestick chart sets this; index-keyed overlays (gamma "
            "heatmap, max-pain, smart-money) leave it false so their price "
            "series stays the index."
        ),
    ),
):
    """Get historical quotes"""
    try:
        # Parse dates if provided
        start_dt = datetime.fromisoformat(start_date) if start_date else None
        end_dt = datetime.fromisoformat(end_date) if end_date else None

        # ES / NQ: their own bar series, always — not gated on allow_futures
        # or the overnight display window, because for a first-class future
        # the futures series IS the price series at every hour.
        futures_index = resolve_futures_index(symbol)
        if futures_index:
            label = symbol.strip().upper()
            fut_rows = await _db().get_historical_futures(
                futures_index, start_dt, end_dt, window_units, timeframe
            )
            return [UnderlyingQuote(**{**row, "symbol": label}) for row in fut_rows]

        # Index→future DISPLAY swap for the candlestick series — ONLY when the
        # caller opts in via allow_futures (the candle chart). Read-only from
        # futures_quotes; falls through to the index series if the ingester
        # has no rows for the requested window.
        if allow_futures and _index_futures_display_enabled() and should_display_future(symbol):
            fut_rows = await _db().get_historical_futures(
                symbol, start_dt, end_dt, window_units, timeframe
            )
            if fut_rows:
                label = _future_display_label(resolve_index_future(symbol))
                return [
                    UnderlyingQuote(**{**row, "display_source": "futures", "data_symbol": label})
                    for row in fut_rows
                ]

        data = await _db().get_historical_quotes(symbol, start_dt, end_dt, window_units, timeframe)
        return [UnderlyingQuote(**row) for row in data]
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    except Exception as e:
        logger.error(f"Error fetching historical quotes: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get(
    "/api/option/quote",
    response_model=OptionQuote,
    tags=["Market Data"],
    dependencies=[_scope_market_raw],
)
async def get_option_quote(
    underlying: str = Query(default="SPY", description="Underlying symbol, e.g. SPY"),
    strike: Optional[float] = Query(default=None, description="Strike price"),
    expiration: Optional[str] = Query(default=None, description="Expiration date (YYYY-MM-DD)"),
    type: Optional[Literal["C", "P"]] = Query(
        default=None, description="Option type: C for Call, P for Put"
    ),
):
    """Get the most recent quote for a specific option contract"""
    try:
        data = await _db().get_option_quote(underlying, strike, expiration, type)
        if not data:
            raise HTTPException(status_code=404, detail="No option quote data available")
        return OptionQuote(**data)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid parameter: {e}")
    except Exception as e:
        logger.error(f"Error fetching option quote: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get(
    "/api/market/open-interest",
    response_model=OpenInterestResponse,
    tags=["Market Data"],
    dependencies=[_scope_market_raw],
)
@handle_api_errors("GET /api/market/open-interest")
async def get_open_interest(
    underlying: str = Query(default="SPY", description="Underlying symbol, e.g. SPY"),
):
    """Get current open interest for each option contract for the underlying.

    Returns one record per (strike, expiration, option_type) from the most recent
    option chain snapshot, ordered by expiration, strike, and option type.
    """
    data = await _db().get_open_interest(underlying)
    if not data or not data.get("contracts"):
        raise HTTPException(status_code=404, detail="No open interest data available")
    return OpenInterestResponse(
        underlying=data["underlying"],
        spot_price=data["spot_price"],
        contracts=[OpenInterestRecord(**row) for row in data["contracts"]],
    )


@app.get(
    "/api/max-pain/timeseries",
    response_model=List[MaxPainTimeseriesPoint],
    tags=["Max Pain"],
    dependencies=[_scope_maxpain],
)
@handle_api_errors("GET /api/max-pain/timeseries")
async def get_max_pain_timeseries(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="5min"),
    window_units: int = Query(default=90, ge=1, le=300),
):
    """Get max pain over time aggregated by timeframe."""
    data = await _db().get_max_pain_timeseries(symbol, timeframe, window_units)
    return [MaxPainTimeseriesPoint(**row) for row in data]


@app.get(
    "/api/max-pain/current",
    response_model=MaxPainCurrent,
    tags=["Max Pain"],
    dependencies=[_scope_maxpain],
)
@handle_api_errors("GET /api/max-pain/current")
async def get_max_pain_current(
    symbol: str = Query(default="SPY"), strike_limit: int = Query(default=200, ge=10, le=1000)
):
    """Get current max pain and strike-by-strike call/put payout notional.

    The top-level ``timestamp``, ``max_pain``, ``underlying_price``, and
    ``difference`` fields reflect the latest analytics-engine reading and
    agree with the most recent point in ``/api/max-pain/timeseries``.

    The ``expirations[]`` array (per-expiration max pain plus the full
    call/put payoff curve at each settlement candidate) comes from a
    daily pre-market snapshot — open interest only publishes once per
    day at settlement, so this detail is stable through the trading day.
    """
    data = await _db().get_max_pain_current(symbol, strike_limit)
    if not data:
        raise HTTPException(status_code=404, detail="No max pain data available")
    return MaxPainCurrent(**data)


# ============================================================================
# Technicals Endpoints
# ============================================================================


_TECHNICALS_SYMBOL_PATTERN = re.compile(r"^[A-Z.]{1,10}$")


@app.get("/api/technicals", tags=["Technicals"], dependencies=[_scope_technicals])
@handle_api_errors("GET /api/technicals")
async def get_technicals(
    symbol: str = Query(default="SPY", min_length=1, max_length=10),
    intervals: Optional[int] = Query(
        default=None,
        ge=1,
        le=192,
        description=(
            "If provided, return only the trailing N 5-minute bars (max "
            "192 = 16h, the full extended ETF session). Use this for "
            "cheap incremental polling."
        ),
    ),
):
    """Combined per 5-minute bar timeseries of VWAP deviation,
    opening-range breakout, unusual volume spikes (all classes), and
    momentum divergence — plus the underlying close — for the most
    recent session.

    Session window depends on ``symbols.asset_type``:
      - ``INDEX`` → 09:30–16:00 ET (cash session only)
      - otherwise (ETF, EQUITY) → 04:00–20:00 ET (extended hours)

    Each bar represents a 5-minute bucket; ``timestamp`` is the START
    of the bucket (e.g. 10:30 → 10:30:00–10:34:59). The bar aggregates
    whichever 1-minute underlying bars have landed in the bucket:
    ``close`` is the latest 1-minute close, volumes are summed,
    ``high`` / ``low`` use max / min. While the 5-minute window is
    still active the bar updates as new 1-minute bars arrive; once the
    window closes the bar becomes immutable.

    ``bars`` is returned newest-first (``bars[0]`` is the most recent
    5-minute bucket), matching the convention used by the other
    timeseries endpoints.

    Cash indices have no native volume; VWAP and volume-spike rolling
    stats are computed against a proxy ETF's per-bar volume when one
    is configured (SPX→SPY, NDX→QQQ, RUT→IWM, DJX→DIA). The active
    proxy is reported in the top-level ``volume_proxy`` field;
    ``null`` for equities/ETFs.

    The "most recent session" is the trading day of the latest bar in
    ``underlying_quotes`` for the symbol — i.e. the live session if
    it's in progress, otherwise the most recent completed session.
    Bars before 09:30 ET have ``opening_range`` fields nulled out (the
    ORB window is 09:30–09:59 ET, so it doesn't exist yet).

    ORB anchor: ``orb_high`` / ``orb_low`` come from the most recent
    ET date that has cash-session data (>= 09:30 ET), which can differ
    from ``session_date`` for ETFs in pre-market. While in pre-market
    of a new trading day, the response shows the previous session's
    ORB through every pre-market bar; once today's 09:30 ET data
    arrives, ORB switches to today's values. INDEX symbols never carry
    pre-market data, so ORB and session always agree for them.

    Pass ``intervals=N`` to get only the last N 5-minute buckets
    (trailing from the most recent existing bar). The response
    metadata (``session_start_et`` / ``session_end_et``) still reports
    the canonical session boundaries; ``bars`` is the trimmed tail.

    404 when ``symbol`` isn't in the ``symbols`` table; 200 with an
    empty ``bars`` list when the symbol exists but has no quote data.

    Dealer hedging is intentionally excluded — its underlying view is
    a point-in-time snapshot, not a timeseries. Use
    ``/api/technicals/dealer-hedging`` for the current-state read.
    """
    normalized = symbol.strip().upper()
    if not _TECHNICALS_SYMBOL_PATTERN.match(normalized):
        raise HTTPException(
            status_code=400,
            detail="symbol must match [A-Z.]{1,10} (letters and dots only, up to 10 chars)",
        )

    result = await _db().get_technicals_timeseries(normalized, intervals=intervals)
    if result is None:
        raise HTTPException(status_code=404, detail="symbol not found")
    return result


@app.get("/api/technicals/vwap-deviation", tags=["Technicals"], dependencies=[_scope_technicals])
@handle_api_errors("GET /api/technicals/vwap-deviation")
async def get_vwap_deviation(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90),
):
    """Get VWAP deviation for mean reversion signals"""
    return await _db().get_vwap_deviation(symbol, timeframe, window_units)


@app.get("/api/technicals/opening-range", tags=["Technicals"], dependencies=[_scope_technicals])
@handle_api_errors("GET /api/technicals/opening-range")
async def get_opening_range(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90),
):
    """Get opening range breakout status"""
    return await _db().get_opening_range_breakout(symbol, timeframe, window_units)


@app.get("/api/technicals/dealer-hedging", tags=["Technicals"], dependencies=[_scope_technicals])
@handle_api_errors("GET /api/technicals/dealer-hedging")
async def get_dealer_hedging(symbol: str = Query(default="SPY")):
    """Get current dealer hedging pressure (point-in-time snapshot).

    The underlying view aggregates the latest snapshot of every option
    contract (delta × open interest × 100) on the symbol to produce a
    single ``expected_hedge_shares`` figure — the net share position
    market makers would have to be long to be delta-neutral against
    current option open interest. ``hedge_pressure`` classifies that
    figure as 🟢 Heavy Buy-Hedging Risk (< -1M), 🔴 Heavy Sell-Hedging
    Risk (> +1M), or ⚪ Balanced Hedging.

    Returns at most one row per symbol — this is not a timeseries.
    """
    return await _db().get_dealer_hedging_pressure(symbol)


@app.get("/api/technicals/volume-spikes", tags=["Technicals"], dependencies=[_scope_technicals])
@handle_api_errors("GET /api/technicals/volume-spikes")
async def get_volume_spikes(
    symbol: str = Query(default="SPY"), limit: int = Query(default=20, le=100)
):
    """Get unusual volume spikes"""
    return await _db().get_unusual_volume_spikes(symbol, limit)


@app.get(
    "/api/technicals/momentum-divergence",
    response_model=List[MomentumDivergencePoint],
    tags=["Technicals"],
    dependencies=[_scope_technicals],
)
@handle_api_errors("GET /api/technicals/momentum-divergence")
async def get_momentum_divergence(
    symbol: str = Query(default="SPY"),
    timeframe: Literal["1min", "5min", "15min", "1hr", "1day", "1hour"] = Query(default="1min"),
    window_units: int = Query(default=20, ge=1, le=90),
):
    """Get momentum divergence signals"""
    data = await _db().get_momentum_divergence(symbol, timeframe, window_units)
    return [MomentumDivergencePoint(**row) for row in data]


# ============================================================================
# WebSocket streaming endpoints
# ============================================================================

# /ws — real-time underlying quote stream. See routers/websockets.py for
# the wire protocol. Registered via a plain function (not APIRouter) so
# the route captures ``get_broadcaster`` lazily and picks up the lifespan-
# owned broadcaster singleton without a global reference at import time.
from .quote_broadcaster import get_broadcaster as _get_ws_broadcaster  # noqa: E402

ws_router.register(app, get_broadcaster=_get_ws_broadcaster)


# ============================================================================
# API v2 — the freshness-envelope surface
# ============================================================================

# Mirrors every /api route above onto /api/v2 with a consistent freshness
# envelope ({"data": ..., "freshness": {...}}). MUST stay below every route
# registration in this module and every include_router() call: the mirror
# reads the route table, so anything registered after it is not mirrored.
# v1 is left untouched. See src/api/v2.py and src/api/freshness.py.
from .v2 import mount_v2  # noqa: E402

try:
    mount_v2(app)
except Exception:  # noqa: BLE001
    # v2 is additive; v1 is the product. A mirroring failure must degrade the
    # new surface, never stop the process from booting and take the whole API
    # down with it. mount_v2 reads FastAPI internals to enumerate included
    # routers, so the thing most likely to break here is a framework upgrade —
    # exactly when you want v1 still serving. The parity test in
    # tests/test_api_v2_freshness_envelope.py turns a partial mirror into a
    # red CI run so this never degrades silently for long.
    logger.exception("v2 mirror failed to mount; /api/v2 will be unavailable")


# ============================================================================
# Error Handlers
# ============================================================================


@app.exception_handler(404)
async def not_found_handler(request, exc):
    detail = getattr(exc, "detail", None)
    if not detail or detail == "Not Found":
        detail = "Endpoint not found"
    return JSONResponse(status_code=404, content={"detail": detail})


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    logger.error(f"Internal server error: {exc}")
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, log_level="info")
