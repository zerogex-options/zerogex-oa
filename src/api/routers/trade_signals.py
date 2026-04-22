"""Signal/trade APIs backed by unified signal tables."""

from __future__ import annotations

import math
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from src.config import SIGNALS_PORTFOLIO_SIZE

from ..database import DatabaseManager

router = APIRouter(prefix="/api/signals", tags=["Trade Signals"])


def _normalize_msi_components(value: Any) -> Any:
    """Normalize MSI component payload while preserving point contributions."""
    if not isinstance(value, dict):
        return value

    expected = {
        "net_gex_sign",
        "flip_distance",
        "local_gamma",
        "put_call_ratio",
        "price_vs_max_gamma",
        "volatility_regime",
    }
    out: dict[str, Any] = {}
    for name, payload in value.items():
        if name not in expected or not isinstance(payload, dict):
            continue
        points = payload.get("max_points", payload.get("points"))
        contribution = payload.get("contribution")
        score = payload.get("score")
        if isinstance(points, (int, float)) and isinstance(contribution, (int, float)):
            out[name] = {
                "max_points": float(points),
                "contribution": round(float(contribution), 4),
                "score": round(float(score), 6) if isinstance(score, (int, float)) else score,
            }
    return out


def _normalize_signal_score_row(row: dict[str, Any]) -> dict[str, Any]:
    """Return compact MSI payload for /api/signals/score endpoints."""
    out = dict(row)
    composite_score = out.get("composite_score")
    if not isinstance(composite_score, (int, float)) or isinstance(composite_score, bool):
        composite_score = 50.0
    composite_score = float(composite_score)
    if math.isnan(composite_score) or math.isinf(composite_score):
        composite_score = 50.0

    components = _normalize_msi_components(out.get("components") or {})
    return {
        "composite_score": round(max(0.0, min(100.0, composite_score)), 2),
        "components": components,
    }



def get_db() -> DatabaseManager:
    from ..main import db_manager
    return db_manager


@router.get("/trades-history")
async def get_signal_history(
    limit: int = Query(default=500, ge=1, le=5000),
    db: DatabaseManager = Depends(get_db),
):
    """Closed-trade log with aggregate win/loss statistics.

    **Params:** `limit` (1–5000, default 500).

    **Returns:**
    ```json
    {
      "portfolio_size": 1000000,
      "trades": [
        {
          "id": 421, "underlying": "SPY",
          "signal_timestamp": "...", "opened_at": "...", "closed_at": "...", "updated_at": "...",
          "status": "closed", "direction": "bullish",
          "score_at_entry": 72.1, "score_latest": 64.8,
          "option_symbol": "SPY 250425C00680000", "option_type": "call",
          "expiration": "2025-04-25", "strike": 680.0,
          "entry_price": 1.22, "current_price": 1.55,
          "quantity_initial": 10, "quantity_open": 0,
          "realized_pnl": 330.0, "unrealized_pnl": 0.0, "total_pnl": 330.0,
          "pnl_percent": 27.05,
          "outcome": "win"
        }
      ],
      "summary": {
        "total_trades": 120, "wins": 72, "losses": 44,
        "win_rate": 0.6, "total_pnl": 12450.32
      }
    }
    ```

    - `portfolio_size` — from `SIGNALS_PORTFOLIO_SIZE` env var.
    - `outcome` — `"win"` (pnl > 0), `"loss"` (pnl < 0), or `"flat"`.
    - `win_rate` — 4-decimal fraction; `null` when no trades exist.
    - `score_at_entry` / `score_latest` — MSI composite values (0–100).

    **Page design.** Stats header strip (win rate, total P&L, counts);
    sortable/filterable trades table; equity-curve sparkline from cumulative `total_pnl`.
    """
    rows = await db.get_closed_signal_trades(limit=limit)
    total_pnl = round(sum(float(r.get("total_pnl") or 0) for r in rows), 4)
    wins = sum(1 for r in rows if r.get("outcome") == "win")
    return {
        "portfolio_size": SIGNALS_PORTFOLIO_SIZE,
        "trades": rows,
        "summary": {
            "total_trades": len(rows),
            "wins": wins,
            "losses": sum(1 for r in rows if r.get("outcome") == "loss"),
            "win_rate": round(wins / len(rows), 4) if rows else None,
            "total_pnl": total_pnl,
        },
    }


@router.get("/trades-live")
async def get_live_signals(db: DatabaseManager = Depends(get_db)):
    """Open paper/live positions initiated by the signal engine.

    **Params:** None.

    **Returns:**
    ```json
    {
      "trades": [
        {
          "id": 421, "underlying": "SPY",
          "signal_timestamp": "...", "opened_at": "...", "updated_at": "...",
          "status": "open", "direction": "bullish",
          "score_at_entry": 72.1, "score_latest": 64.8,
          "option_symbol": "SPY 250425C00680000", "option_type": "call",
          "expiration": "2025-04-25", "strike": 680.0,
          "entry_price": 1.22, "current_price": 1.55,
          "quantity_initial": 10, "quantity_open": 10,
          "realized_pnl": 0.0, "unrealized_pnl": 330.0, "total_pnl": 330.0,
          "pnl_percent": 27.05
        }
      ],
      "count": 1
    }
    ```

    - `direction` — `"bullish"` | `"bearish"`.
    - `option_type` — `"call"` | `"put"`.
    - `score_at_entry` / `score_latest` — MSI composite values (0–100).
    - `pnl_percent` — percentage of premium paid.

    **Page design.** Card grid: option symbol, direction chip, big P&L number
    (unrealized), MSI-at-entry vs latest delta with a color arrow.
    """
    rows = await db.get_live_signal_trades()
    return {
        "trades": rows,
        "count": len(rows),
    }


@router.get("/score")
async def get_latest_score(
    underlying: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Composite Market State Index (MSI) — single 0–100 regime gauge.

    Aggregates 6 option-structure components into one reading of "what kind
    of market are we in right now?" Each component returns a raw score in
    [-1, +1], multiplied by its weight (points) to form a signed contribution.
    All contributions are summed onto a 50-point baseline then clamped to [0, 100].

    **Params:** `underlying` (default `SPY`). Returns 404 when no rows exist.

    **Components and weights:**

    | Component | Max pts | What it measures |
    |---|---|---|
    | `net_gex_sign` | 20 | Sign of dealer net gamma |
    | `flip_distance` | 25 | Proximity to gamma-flip strike |
    | `local_gamma` | 20 | Gamma density near spot |
    | `put_call_ratio` | 15 | OI-weighted P/C tilt |
    | `price_vs_max_gamma` | 10 | Distance from max-gamma strike |
    | `volatility_regime` | 10 | Realized/VIX regime |

    **Returns:**
    ```json
    {
      "composite_score": 63.42,
      "components": {
        "net_gex_sign":       {"max_points": 20, "contribution":  12.00, "score":  0.6},
        "flip_distance":      {"max_points": 25, "contribution":   5.25, "score":  0.21},
        "local_gamma":        {"max_points": 20, "contribution":  -8.40, "score": -0.42},
        "put_call_ratio":     {"max_points": 15, "contribution":   3.00, "score":  0.2},
        "price_vs_max_gamma": {"max_points": 10, "contribution":   1.70, "score":  0.17},
        "volatility_regime":  {"max_points": 10, "contribution":  -0.13, "score": -0.013}
      }
    }
    ```

    - `composite_score` — float [0, 100]; `50` is the neutral/fallback value.
    - `components[*].max_points` — the component's weight ceiling.
    - `components[*].contribution` — signed points added to the baseline, rounded to 4 decimals.
    - `components[*].score` — raw component score [-1, +1], 6-decimal precision.

    **Regime interpretation:**
    - **≥ 70** — trend/expansion; favor directional trades in the prevailing bias.
    - **40–70** — controlled trend; moderate directional edge, size down.
    - **20–40** — chop/range; fade extremes, avoid trend trades.
    - **< 20** — high-risk reversal; mean-reversion only.

    **Page design.** Big radial gauge (0–100). Horizontal bar stack below showing
    each component's signed contribution. Hover for `score` and `max_points`.
    """
    row = await db.get_latest_signal_score_enriched(underlying.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No score rows found for {underlying.upper()}")
    return _normalize_signal_score_row(row)


@router.get("/score-history")
async def get_score_history(
    underlying: str = Query(default="SPY"),
    limit: int = Query(default=90, ge=1, le=5000),
    db: DatabaseManager = Depends(get_db),
):
    """Time series of the composite MSI, newest-first.

    **Params:** `underlying` (default `SPY`), `limit` (default 90, max 5000).

    **Returns.** An array of objects identical to `/score` — each with
    `composite_score` and `components`. Rows are ordered by `timestamp DESC`
    so index 0 is the most recent. No timestamp is included in the normalized
    payload; use `/score` for current and `/{signal_name}/events` for historical
    component series.

    ```json
    [
      {
        "composite_score": 63.42,
        "components": {
          "net_gex_sign":       {"max_points": 20, "contribution":  12.00, "score":  0.6},
          "flip_distance":      {"max_points": 25, "contribution":   5.25, "score":  0.21},
          "local_gamma":        {"max_points": 20, "contribution":  -8.40, "score": -0.42},
          "put_call_ratio":     {"max_points": 15, "contribution":   3.00, "score":  0.2},
          "price_vs_max_gamma": {"max_points": 10, "contribution":   1.70, "score":  0.17},
          "volatility_regime":  {"max_points": 10, "contribution":  -0.13, "score": -0.013}
        }
      }
    ]
    ```

    **Page design.** Line chart of `composite_score` with shaded regime bands at
    20/40/70. Stacked-area chart of component `contribution` values underneath
    shows which component flipped the regime.
    """
    rows = await db.get_signal_score_history(underlying.upper(), limit)
    normalized_rows = [_normalize_signal_score_row(row) for row in rows]
    return normalized_rows


@router.get("/advanced/vol-expansion")
async def get_vol_expansion_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Volatility expansion forecast — expansion readiness + directional bias.

    Answers two questions at once: *Will vol expand?* (GEX-driven) and
    *If it does, which way?* (momentum-driven).

    **Logic highlights** (`src/signals/advanced/vol_expansion.py`):
    - `expansion = gex_readiness × 100` (0–100); short-gamma regimes raise readiness.
    - `direction_score = tanh(momentum_z / 1.0) × 100` based on a 5-bar z-score.
    - `score = (expansion × direction_score) / 100` — sign matches direction,
      magnitude gated by expansion readiness.

    **Params:** `symbol` (default `SPY`). Returns 404 when no data exists.

    **Returns:**
    ```json
    {
      "underlying": "SPY", "timestamp": "...",
      "clamped_score": 0.42, "weighted_score": 0.084, "weight": 0.20,
      "direction": "bullish",
      "score": 42.0,
      "expansion": 78.4,
      "direction_score": 53.5,
      "magnitude": 41.9,
      "expected_5min_move_bps": 14.2,
      "context_values": { "...full engine context..." },
      "score_history": [{"score": 42.0, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` — [-100, +100]; product of expansion × direction, scaled.
    - `expansion` — [0, 100]; GEX-driven vol-expansion readiness.
    - `direction_score` — [-100, +100]; momentum-driven directional bias.
    - `magnitude` — [0, 100]; absolute size of the combined signal.
    - `expected_5min_move_bps` — forecasted 5-minute move in basis points.
    - `direction` — `"bullish"` | `"bearish"` | `"neutral"`.
    - `score_history` — up to 90 recent scores; sort client-side by `timestamp`.

    **Trader interpretation:**
    - `expansion > 60` + `|direction_score| > 50` → high-conviction expansion;
      long gamma or directional debit spread in the direction sign.
    - `expansion < 30` → wait; dealers long gamma, likely pinning.
    - Direction-score sign-flip while expansion stays high → whipsaw warning.

    **Page design.** Two half-circle gauges (expansion + direction_score), a
    "expected 5-min move" number (bps), and a sparkline of `score_history`.
    Color the card green/red by direction when `|score| ≥ 25`.
    """
    row = await db.get_vol_expansion_signal(symbol.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No vol-expansion score found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["expansion"] = ctx.get("expansion")
    row["direction_score"] = ctx.get("direction")
    row["magnitude"] = ctx.get("magnitude")
    row["expected_5min_move_bps"] = ctx.get("expected_5min_move_bps")
    return row


@router.get("/advanced/eod-pressure")
async def get_eod_pressure_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """End-of-day close pin/drift forecast for the last ~75 minutes of the session.

    Combines (a) dealer charm exposure near spot, (b) gamma-gated pin gravity,
    and (c) an OpEx/quad-witching calendar amplifier into a directional close forecast.

    **Active window: 14:30–16:00 ET only.** Outside this window the endpoint returns
    `score = 0` and `time_ramp = 0` — treat as an "inactive" state, not neutral.

    **Logic highlights** (`src/signals/advanced/eod_pressure.py`):
    - `time_ramp` — 0 before T-90min, 1.0 by T-15min, linear in between.
    - `charm_score = tanh(charm_at_spot / 20M)` over strikes within the ATM band.
    - `pin_gravity = sign(net_gex) × min(1, pin_distance_pct / 0.3%)`.
    - `score = (0.6·charm + 0.4·pin) × calendar_amp × time_ramp`;
      `calendar_amp` is 1.5× OpEx, 2.0× quad-witching.

    **Params:** `symbol` (default `SPY`). Returns 404 when no data exists.

    **Returns:**
    ```json
    {
      "score": 48.1,
      "direction": "bullish",
      "clamped_score": 0.48, "weighted_score": 0.096, "weight": 0.20,
      "charm_at_spot": 12850000.0,
      "pin_target": 676.0,
      "pin_distance_pct": 0.00042,
      "gamma_regime": "positive",
      "time_ramp": 0.76,
      "calendar_flags": {"opex": false, "quad_witching": false},
      "context_values": {"...atm_band_pct, pin_source, calendar_amp, ..."},
      "score_history": [{"score": 48.1, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` — [-100, +100]; positive = bullish close bias, negative = bearish.
    - `direction` — `"bullish"` | `"bearish"` | `"neutral"`.
    - `charm_at_spot` — signed dollar-delta of dealer charm in the ±ATM band.
    - `pin_target` — heavy-GEX strike, or `max_pain` as fallback.
    - `pin_distance_pct` — (pin − spot) / spot; signed; typically ±2%.
    - `gamma_regime` — `"positive"` | `"negative"` (flips pin-gravity direction).
    - `time_ramp` — [0, 1]; scaling ramp toward close.
    - `calendar_flags` — booleans for `opex` and `quad_witching`.

    **Trader interpretation:**
    - `time_ramp > 0.5` + `score > 50` → strong upside pin; trade drift toward `pin_target`.
    - `quad_witching = true` doubles the amplifier; treat `|score| > 60` as high-conviction.
    - Render an "Inactive — EOD window opens at 14:30 ET" state when `time_ramp == 0`.

    **Page design.** Horizontal close-bias bar (-100..+100). Show `pin_target` vs spot
    on a small price axis with distance labeled. Badge for OpEx/quad-witching.
    Greyed-out card when `time_ramp == 0`.
    """
    row = await db.get_eod_pressure_signal(symbol.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No eod-pressure score found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["charm_at_spot"] = ctx.get("charm_at_spot")
    row["pin_target"] = ctx.get("pin_target")
    row["pin_distance_pct"] = ctx.get("pin_distance_pct")
    row["gamma_regime"] = ctx.get("gamma_regime")
    row["time_ramp"] = ctx.get("time_ramp")
    row["calendar_flags"] = ctx.get("calendar_flags")
    return row


@router.get("/advanced/squeeze-setup")
async def get_squeeze_setup_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Squeeze-setup detector — directional flow z-scores correlated with momentum.

    Detects bullish/bearish squeeze setups by correlating directional flow z-scores
    with momentum acceleration and dealer-gamma posture. Standalone event detector,
    not part of the MSI composite.

    **Logic highlights** (`src/signals/advanced/squeeze_setup.py`):
    - Bullish: `call_flow_z × tanh_scaled(momentum) × momentum_strength × 1.2 (if accelerating)
      × (1.0 if above flip else 0.6) × (1.0 if net_gex < 0 else 0.5)`.
    - Bearish: symmetric using `put_flow_z`.
    - **Triggered when `|score| ≥ 25` (clamped 0.25).**
    - Dead-VIX regime (`vix_level < 15`) attenuates strength ~50%.

    **Params:** `symbol` (default `SPY`). Returns 404 when no data exists.

    **Returns:**
    ```json
    {
      "score": 38.0, "clamped_score": 0.38, "direction": "bullish",
      "triggered": true,
      "signal": "bullish_squeeze",
      "call_flow_delta": 125000.0, "put_flow_delta": -45000.0,
      "call_flow_z": 2.1, "put_flow_z": -0.8,
      "momentum_z": 1.4,
      "vix_regime": "normal",
      "context_values": {"...momentum_5bar, momentum_10bar, accel_up, accel_dn, net_gex, gamma_flip, flow_norm_used..."},
      "score_history": [{"score": 38.0, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` — [-100, +100].
    - `signal` — `"bullish_squeeze"` | `"bearish_squeeze"` | `"none"`.
    - `triggered` — `true` when `|score| ≥ 25`.
    - `call_flow_z` / `put_flow_z` — z-scores; typically [-5, +5].
    - `vix_regime` — `"dead"` | `"normal"` | `"elevated"` | `"panic"` | `"unknown"`.

    **Trader interpretation:**
    - `signal == "bullish_squeeze"` + `accel_up == true` + price above gamma flip
      + `net_gex < 0` → classic short-gamma squeeze; long call spreads.
    - `vix_regime == "dead"` → cut conviction; dead-vol regimes lack squeeze fuel.
    - `momentum_z` vs flow z-scores divergence: flow without price = early signal;
      price without flow = exhausted move.

    **Page design.** Signal pill (bullish/bearish/none) with `triggered` as a bright dot.
    Paired bar showing `call_flow_z` and `put_flow_z`. VIX regime chip (color-coded).
    """
    row = await db.get_advanced_signal(symbol.upper(), "squeeze_setup")
    if not row:
        raise HTTPException(status_code=404, detail=f"No squeeze-setup signal found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["triggered"] = ctx.get("triggered", False)
    row["signal"] = ctx.get("signal", "none")
    row["call_flow_delta"] = ctx.get("call_flow_delta")
    row["put_flow_delta"] = ctx.get("put_flow_delta")
    row["call_flow_z"] = ctx.get("call_flow_z")
    row["put_flow_z"] = ctx.get("put_flow_z")
    row["momentum_z"] = ctx.get("momentum_z")
    row["vix_regime"] = ctx.get("vix_regime")
    return row


@router.get("/advanced/trap-detection")
async def get_trap_detection_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Trap detector — failed-breakout fade opportunities at gamma walls.

    Flags failed breakouts (bull trap / bear trap) as fade opportunities when
    dealer long gamma reinforces a reversal at a resistance/support wall.
    Standalone detector, not part of the MSI composite.

    **Logic highlights** (`src/signals/advanced/trap_detection.py`):
    - `breakout_buffer_pct = min(0.1%, 0.15 × realized_sigma × √5)` — vol-scaled noise floor.
    - Upside-fail (bear fade): `close > resistance + buffer` AND `net_gex > 0`
      AND gamma strengthening AND wall NOT migrating up.
    - Score = `0.4 + 0.4 × distance_strength + 0.2 × gex_boost`, then flow multiplier.
    - **Triggered when `|score| ≥ 25` (clamped 0.25).**

    **Params:** `symbol` (default `SPY`). Returns 404 when no data exists.

    **Returns:**
    ```json
    {
      "score": -35.0, "clamped_score": -0.35, "direction": "bearish",
      "triggered": true,
      "signal": "bearish_fade",
      "breakout_up": true, "breakout_down": false,
      "net_gex_delta": 120000000.0,
      "net_gex_delta_pct": 0.018,
      "resistance_level": 680.0,
      "support_level": 678.0,
      "breakout_buffer_pct": 0.0008,
      "wall_migrated_up": false, "wall_migrated_down": false,
      "context_values": {"...close, realized_sigma, long_gamma, gamma_strengthening, call_wall, prior_call_wall, call_flow_decelerating, put_flow_decelerating..."},
      "score_history": [{"score": -35.0, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` — [-100, +100].
    - `signal` — `"bullish_fade"` | `"bearish_fade"` | `"none"`.
    - `triggered` — `true` when `|score| ≥ 25`.
    - `breakout_up` / `breakout_down` — whether price has crossed the buffer.
    - `wall_migrated_up` / `wall_migrated_down` — invalidates the setup when `true`.

    **Trader interpretation:**
    - `signal == "bearish_fade"` + `breakout_up == true` → price poked above resistance
      but dealers are long gamma and call wall hasn't migrated; short-call-spread / put-debit.
    - `signal == "bullish_fade"` → mirror play at support.
    - `wall_migrated_up == true` → setup invalidated; dealers repositioning with price.

    **Page design.** Price ladder showing support / close / resistance with breakout-buffer bands.
    Red/green "TRAP" badge when triggered. Two chips: `gamma_strengthening` and `wall_migrated_*`.
    """
    row = await db.get_advanced_signal(symbol.upper(), "trap_detection")
    if not row:
        raise HTTPException(status_code=404, detail=f"No trap-detection signal found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["triggered"] = ctx.get("triggered", False)
    row["signal"] = ctx.get("signal", "none")
    row["breakout_up"] = ctx.get("breakout_up", False)
    row["breakout_down"] = ctx.get("breakout_down", False)
    row["net_gex_delta"] = ctx.get("net_gex_delta")
    row["net_gex_delta_pct"] = ctx.get("net_gex_delta_pct")
    row["resistance_level"] = ctx.get("resistance_level")
    row["support_level"] = ctx.get("support_level")
    row["breakout_buffer_pct"] = ctx.get("breakout_buffer_pct")
    row["wall_migrated_up"] = ctx.get("wall_migrated_up")
    row["wall_migrated_down"] = ctx.get("wall_migrated_down")
    return row


@router.get("/advanced/0dte-position-imbalance")
async def get_zero_dte_position_imbalance_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Same-day-expiry flow tilt — 0DTE call vs put premium imbalance.

    Measures when 0DTE call vs put net premium becomes lopsided enough to
    forecast short-term drift from dealer hedging. Falls back to all-expiry
    flow when 0DTE data is absent, reflected in `flow_source`.

    **Logic highlights** (`src/signals/advanced/zero_dte_position_imbalance.py`):
    - Buckets 0DTE flow by moneyness (ATM ±0.5%), weighting OTM most:
      `0.6·OTM_call − 0.6·OTM_put + 0.3·ATM_call − 0.3·ATM_put + 0.1·ITM − ...`
    - `flow_imbalance = weighted / total_abs` (gated to 0 below $50k gross).
    - Combined = `0.55·flow + 0.30·smart_imbalance + 0.15·pcr_tilt`.
    - Multiplied by a time-of-day ramp (stronger near close, zero after hours).
    - **Triggered when `|score| ≥ 25` (clamped 0.25).**

    **Note on time-gating:** `tod_multiplier` zeros after hours — treat as
    "inactive," not "neutral."

    **Params:** `symbol` (default `SPY`). Returns 404 when no data exists.

    **Returns:**
    ```json
    {
      "score": 31.0, "clamped_score": 0.31, "direction": "bullish",
      "triggered": true,
      "signal": "call_heavy",
      "flow_imbalance": 0.42,
      "smart_imbalance": 0.18,
      "context_values": {"...call_net_premium, put_net_premium, otm_call_net, atm_call_net, otm_put_net, atm_put_net, pcr_tilt, put_call_ratio, tod_multiplier, flow_source..."},
      "score_history": [{"score": 31.0, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` — [-100, +100].
    - `signal` — `"call_heavy"` | `"put_heavy"` | `"balanced"`.
    - `triggered` — `true` when `|score| ≥ 25`.
    - `flow_imbalance` — [-1, +1]; bucket-weighted net imbalance.
    - `smart_imbalance` — [-1, +1]; smart-money subset.
    - `flow_source` (in `context_values`) — `"zero_dte"` | `"all_expiry_fallback"`.

    **Trader interpretation:**
    - `call_heavy` near close with rising momentum → lean long (dealers short 0DTE calls must chase).
    - `put_heavy` near close with falling momentum → lean short.
    - Warn when `flow_source == "all_expiry_fallback"`: 0DTE picture is inferred, not measured.

    **Page design.** Diverging bar chart with four moneyness buckets (OTM/ATM calls vs OTM/ATM puts).
    `flow_source` chip + `tod_multiplier` as a clock/progress indicator.
    """
    row = await db.get_advanced_signal(symbol.upper(), "zero_dte_position_imbalance")
    if not row:
        raise HTTPException(status_code=404, detail=f"No 0DTE position-imbalance signal found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["triggered"] = ctx.get("triggered", False)
    row["signal"] = ctx.get("signal", "balanced")
    row["flow_imbalance"] = ctx.get("flow_imbalance")
    row["smart_imbalance"] = ctx.get("smart_imbalance")
    return row


@router.get("/advanced/gamma-vwap-confluence")
async def get_gamma_vwap_confluence_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Gamma+VWAP confluence detector — multi-level price cluster magnet.

    Detects when multiple reference levels (gamma flip, VWAP, max pain, max gamma,
    call wall) cluster near the same price, creating a high-conviction magnet or
    bounce level.

    **Logic highlights** (`src/signals/advanced/gamma_vwap_confluence.py`):
    - Requires flip + VWAP within 0.15% of midpoint; adds max_pain / max_gamma /
      call_wall if also within 0.15%.
    - `cluster_quality = max(0, 1 − core_gap_pct / 0.5%)`;
      multi-member bonus `1.0 + 0.15 × extra_members`.
    - `net_gex < 0` → continuation (bullish if price above, bearish below);
      long gamma → mean reversion (`−0.7 × directional`).
    - **Triggered when `|score| ≥ 20` (clamped 0.20).**

    **Params:** `symbol` (default `SPY`). Returns 404 when no data exists.

    **Returns:**
    ```json
    {
      "score": 22.0, "clamped_score": 0.22, "direction": "bullish",
      "triggered": true,
      "signal": "bullish_confluence",
      "confluence_level": 678.25,
      "cluster_gap_pct": 0.0009,
      "expected_target": 680.5,
      "context_values": {"...gamma_flip, vwap, cluster_members, cluster_quality, distance_from_level_pct, regime_direction, net_gex..."},
      "score_history": [{"score": 22.0, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` — [-100, +100].
    - `signal` — `"bullish_confluence"` | `"bearish_confluence"` | `"neutral"`.
    - `triggered` — `true` when `|score| ≥ 20`.
    - `confluence_level` — price of the cluster midpoint.
    - `cluster_gap_pct` — |flip − vwap| / close; [0, ~0.005].
    - `expected_target` — reversion target (mean-reversion) or extrapolated (continuation).
    - `regime_direction` (in `context_values`) — `"mean_reversion"` | `"continuation"`.

    **Trader interpretation:**
    - `signal == "bullish_confluence"` + `cluster_quality > 0.8` + `regime_direction == "mean_reversion"`:
      `confluence_level` is a buy zone; `expected_target` is the reversion target.
    - `regime_direction == "continuation"` (short gamma): breaks tend to run;
      use `expected_target` as the first profit taker.

    **Page design.** Vertical price axis with cluster members as colored tick marks and
    `confluence_level` as a bold band. Arrow from current price to `expected_target`.
    Regime chip ("reversion" vs "continuation").
    """
    row = await db.get_advanced_signal(symbol.upper(), "gamma_vwap_confluence")
    if not row:
        raise HTTPException(status_code=404, detail=f"No gamma+VWAP confluence signal found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["triggered"] = ctx.get("triggered", False)
    row["signal"] = ctx.get("signal", "none")
    row["confluence_level"] = ctx.get("confluence_level")
    row["cluster_gap_pct"] = ctx.get("cluster_gap_pct")
    row["expected_target"] = ctx.get("expected_target")
    return row


_BASIC_SIGNAL_NAMES: tuple[str, ...] = (
    "tape_flow_bias",
    "skew_delta",
    "vanna_charm_flow",
    "dealer_delta_pressure",
    "gex_gradient",
    "positioning_trap",
)


@router.get("/basic")
async def get_basic_signals_bundle(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Latest snapshot of all six Basic Signals in a single response.

    Basic Signals are continuous directional reads ([-1, +1], scaled to
    [-100, +100]) that complement the 6 MSI components and 6 Advanced
    Signals. They do not contribute to the composite MSI (weight=0). Each
    entry is the most recent row persisted to `signal_component_scores`.

    **Signals (6):** `tape_flow_bias`, `skew_delta`, `vanna_charm_flow`,
    `dealer_delta_pressure`, `gex_gradient`, `positioning_trap`.

    **Params:** `symbol` (default `SPY`).

    **Returns:**
    ```json
    {
      "underlying": "SPY",
      "signals": {
        "tape_flow_bias":        {"score": 28.4,  "direction": "bullish", "timestamp": "...", "context_values": {...}},
        "skew_delta":            {"score": -12.7, "direction": "bearish", "timestamp": "...", "context_values": {...}},
        "vanna_charm_flow":      {"score": 0.0,   "direction": "neutral", "timestamp": "...", "context_values": {...}},
        "dealer_delta_pressure": {"score": 45.1,  "direction": "bullish", "timestamp": "...", "context_values": {...}},
        "gex_gradient":          {"score": -8.3,  "direction": "bearish", "timestamp": "...", "context_values": {...}},
        "positioning_trap":      {"score": 0.0,   "direction": "neutral", "timestamp": "...", "context_values": {...}}
      }
    }
    ```

    - `score` — clamped_score × 100; [-100, +100]. `null` if the signal has never
      persisted a row for this symbol.
    - `direction` — `"bullish"` | `"bearish"` | `"neutral"`.
    """
    bundle = await db.get_latest_basic_signals_bundle(symbol.upper())
    signals: dict[str, Any] = {}
    for name in _BASIC_SIGNAL_NAMES:
        row = bundle.get(name)
        if not row:
            signals[name] = None
            continue
        raw = float(row.get("clamped_score") or 0.0)
        direction = "bullish" if raw > 0 else ("bearish" if raw < 0 else "neutral")
        signals[name] = {
            "score": row.get("score"),
            "clamped_score": raw,
            "direction": direction,
            "timestamp": row.get("timestamp"),
            "context_values": row.get("context_values") or {},
        }
    return {"underlying": symbol.upper(), "signals": signals}


@router.get("/basic/tape-flow-bias")
async def get_tape_flow_bias_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Signed option-tape premium imbalance — continuous order-flow bias.

    Reads the Lee-Ready-classified `flow_by_type` aggregates to measure
    aggressive call buying vs put buying (minus their sell sides) over
    the short window. Unlike `smart_money` (discrete "smart" events), this
    watches the continuous tape for directional conviction.

    **Logic** (`src/signals/basic/tape_flow_bias.py`):
    - `call_net = buy_premium − sell_premium` for option_type=C; put_net analogously.
    - `directional = call_net − put_net`; `ratio = directional / (|call_net|+|put_net|)`.
    - `score = clip(ratio / SATURATION, [-1, 1])`.
    - Abstains (score=0) if total premium below `SIGNAL_TAPE_FLOW_MIN_PREMIUM`.

    **Params:** `symbol` (default `SPY`). Returns 404 if no data exists.

    **Returns:** `score`, `direction`, `context_values` with per-side premium
    breakdown (`call_net_premium`, `put_net_premium`, `call_buy_premium`, etc.),
    and `score_history`.
    """
    row = await db.get_basic_signal(symbol.upper(), "tape_flow_bias")
    if not row:
        raise HTTPException(status_code=404, detail=f"No tape-flow-bias score found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["call_net_premium"] = ctx.get("call_net_premium")
    row["put_net_premium"] = ctx.get("put_net_premium")
    row["source"] = ctx.get("source")
    return row


@router.get("/basic/skew-delta")
async def get_skew_delta_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Short-dated OTM skew deviation — real-time fear gauge.

    OTM-put vs OTM-call implied-vol spread measured against a configurable
    baseline (index skew is structurally positive, so we score the
    *deviation from normal*, not the raw spread).

    **Logic** (`src/signals/basic/skew_delta.py`):
    - `spread = otm_put_iv − otm_call_iv` from `ctx.extra['skew']`.
    - `deviation = spread − SIGNAL_SKEW_BASELINE`.
    - `score = −clip(deviation / SIGNAL_SKEW_SATURATION, [-1, 1])` (elevated put skew → bearish).

    **Params:** `symbol` (default `SPY`). Returns 404 if no data exists.

    **Returns:** `score`, `direction`, `context_values` with `otm_put_iv`,
    `otm_call_iv`, `spread`, `baseline`, `deviation`, and `score_history`.
    """
    row = await db.get_basic_signal(symbol.upper(), "skew_delta")
    if not row:
        raise HTTPException(status_code=404, detail=f"No skew-delta score found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["otm_put_iv"] = ctx.get("otm_put_iv")
    row["otm_call_iv"] = ctx.get("otm_call_iv")
    row["spread"] = ctx.get("spread")
    row["deviation"] = ctx.get("deviation")
    return row


@router.get("/basic/vanna-charm-flow")
async def get_vanna_charm_flow_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Second-order greek pressure (vanna + charm) — dealer re-hedging bias.

    Aggregates dealer vanna (dVega/dSpot) and charm (dDelta/dTime) exposure
    across strikes. Positive = dealer buying pressure, negative = dealer
    selling pressure. Charm contribution is amplified in the afternoon
    session as expiry-day hedging accelerates.

    **Logic** (`src/signals/basic/vanna_charm_flow.py`):
    - Sum `dealer_vanna_exposure` + `dealer_charm_exposure × charm_amplification`.
    - `score = clip(combined / SIGNAL_VANNA_CHARM_NORM, [-1, 1])`.
    - Legacy rows (no dealer columns) fall back to negated market-aggregate values.

    **Params:** `symbol` (default `SPY`). Returns 404 if no data exists.

    **Returns:** `score`, `direction`, `context_values` with `vanna_total`,
    `charm_total`, `charm_amplification`, `vc_norm`, `source`, and `score_history`.
    """
    row = await db.get_basic_signal(symbol.upper(), "vanna_charm_flow")
    if not row:
        raise HTTPException(status_code=404, detail=f"No vanna-charm-flow score found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["vanna_total"] = ctx.get("vanna_total")
    row["charm_total"] = ctx.get("charm_total")
    row["charm_amplification"] = ctx.get("charm_amplification")
    row["source"] = ctx.get("source")
    return row


@router.get("/basic/dealer-delta-pressure")
async def get_dealer_delta_pressure_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Dealer net-delta imbalance (DNI) — intraday leading indicator.

    Estimates dealer net delta from per-strike OI × delta. Dealer short
    delta → forced buying into rallies → bullish for price. Flow leads
    gamma exposure intraday; this is the closest thing to a leading
    indicator for 0DTE regimes.

    **Logic** (`src/signals/basic/dealer_delta_pressure.py`):
    - Prefer `ctx.dealer_net_delta` → `gex_by_strike.call_delta_oi/put_delta_oi`
      → distance-from-spot proxy (ordered by precision).
    - `score = −clip(dni / SIGNAL_DNI_NORM, [-1, 1])` (inverted: dealer short
      delta is bullish for price).

    **Params:** `symbol` (default `SPY`). Returns 404 if no data exists.

    **Returns:** `score`, `direction`, `context_values` with
    `dealer_net_delta_estimated`, `dni_normalized`, `source`, and `score_history`.
    """
    row = await db.get_basic_signal(symbol.upper(), "dealer_delta_pressure")
    if not row:
        raise HTTPException(status_code=404, detail=f"No dealer-delta-pressure score found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["dealer_net_delta_estimated"] = ctx.get("dealer_net_delta_estimated")
    row["dni_normalized"] = ctx.get("dni_normalized")
    row["source"] = ctx.get("source")
    return row


@router.get("/basic/gex-gradient")
async def get_gex_gradient_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Gamma asymmetry around spot — above- vs below-spot dealer gamma skew.

    Decomposes per-strike gamma exposure into above-spot / below-spot /
    ATM / wing buckets. A heavy "above spot" skew signals dealer short
    gamma into a rally — any up-move unwinds (bullish). Below-spot
    dominance is the bearish mirror.

    **Logic** (`src/signals/basic/gex_gradient.py`):
    - Score = asymmetry of |gamma above| vs |gamma below|, weighted by total notional.
    - Abstains below `SIGNAL_GEX_GRADIENT_MIN_GAMMA` (thin OI protection).
    - Long-gamma regimes dampened by `SIGNAL_GEX_GRADIENT_LONG_GAMMA_DAMPING`.

    **Params:** `symbol` (default `SPY`). Returns 404 if no data exists.

    **Returns:** `score`, `direction`, `context_values` with per-bucket gamma
    breakdown (`above_spot_gamma_abs`, `below_spot_gamma_abs`, `atm_gamma_abs`,
    `wing_gamma_abs`, `asymmetry`, `strike_count`), and `score_history`.
    """
    row = await db.get_basic_signal(symbol.upper(), "gex_gradient")
    if not row:
        raise HTTPException(status_code=404, detail=f"No gex-gradient score found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["above_spot_gamma_abs"] = ctx.get("above_spot_gamma_abs")
    row["below_spot_gamma_abs"] = ctx.get("below_spot_gamma_abs")
    row["asymmetry"] = ctx.get("asymmetry")
    row["wing_fraction"] = ctx.get("wing_fraction")
    return row


@router.get("/basic/positioning-trap")
async def get_positioning_trap_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Crowd-positioning trap — squeeze/flush risk from one-way crowding.

    Flags setups where tape behavior starts invalidating crowd direction:
    heavy put-buying into a rally (short-squeeze risk), or heavy call-buying
    into a drop (long-flush risk). Uses signed net premium from
    `flow_contract_facts` when available — more informative than raw total
    premium because opposite-side buying nets out.

    **Logic** (`src/signals/basic/positioning_trap.py`):
    - `squeeze = short_crowding + put_skew + above_flip + neg_gex` (bullish side).
    - `flush  = long_crowding + call_skew + below_flip + neg_gex` (bearish side).
    - `score = clip(squeeze − flush, [-1, 1])`.

    **Params:** `symbol` (default `SPY`). Returns 404 if no data exists.

    **Returns:** `score`, `direction`, `context_values` with `put_call_ratio`,
    `smart_imbalance`, `smart_imbalance_source`, `momentum_5bar`, `close`,
    `gamma_flip`, `net_gex`, and `score_history`.
    """
    row = await db.get_basic_signal(symbol.upper(), "positioning_trap")
    if not row:
        raise HTTPException(status_code=404, detail=f"No positioning-trap score found for {symbol.upper()}")
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["smart_imbalance"] = ctx.get("smart_imbalance")
    row["smart_imbalance_source"] = ctx.get("smart_imbalance_source")
    row["momentum_5bar"] = ctx.get("momentum_5bar")
    return row


_VALID_SIGNAL_EVENT_NAMES = {
    # Advanced Signals
    "vol_expansion",
    "eod_pressure",
    "squeeze_setup",
    "trap_detection",
    "zero_dte_position_imbalance",
    "gamma_vwap_confluence",
    # Basic Signals
    *_BASIC_SIGNAL_NAMES,
}


@router.get("/{signal_name}/events")
async def get_signal_events(
    signal_name: str,
    symbol: str = Query(default="SPY"),
    limit: int = Query(default=100, ge=1, le=1000),
    horizon: str = Query(default="60m", pattern="^(30m|60m|120m)$"),
    db: DatabaseManager = Depends(get_db),
):
    """Time-stamped history of a single signal's scores with direction-flip detection and realized returns.

    Provides per-bar snapshots of a component's score, direction, and input context,
    plus *forward* realized returns for backtesting and diagnostic overlays.

    **Params:**
    - `signal_name` — one of: `vol_expansion`, `eod_pressure`, `squeeze_setup`,
      `trap_detection`, `zero_dte_position_imbalance`, `gamma_vwap_confluence`,
      `positioning_trap`, `vanna_charm_flow`. Returns 400 for unknown names.
    - `symbol` (default `SPY`).
    - `limit` — 1–1000, default 100.
    - `horizon` — `"30m"` | `"60m"` | `"120m"` (default `"60m"`); forward window for realized return.

    **Returns:**
    ```json
    {
      "underlying": "SPY",
      "signal_name": "vol_expansion",
      "horizon": "60m",
      "rows": [
        {
          "underlying": "SPY",
          "timestamp": "2026-04-22T14:55:00Z",
          "component_name": "vol_expansion",
          "score": 42.31,
          "weighted_score": 0.0846,
          "weight": 0.20,
          "direction": "bullish",
          "direction_flip": true,
          "inputs": { "...context_values snapshot..." },
          "close": 677.12,
          "horizon_close": 678.40,
          "realized_return": 0.00189
        }
      ],
      "count": 100,
      "summary": {
        "flips": 7,
        "bullish": 43, "bearish": 38, "neutral": 19,
        "latest_timestamp": "...",
        "latest_direction": "bullish"
      }
    }
    ```

    - `score` — clamped_score × 100; [-100, +100].
    - `direction_flip` — `true` when sign changed since previous non-zero row.
    - `realized_return` — (horizon_close − close) / close; 6-decimal fractional;
      `null` if no forward quote exists yet.
    - `horizon_close` — underlying close at `timestamp + horizon`.

    **Trader interpretation:**
    - Use `realized_return` to validate direction (bullish rows should average positive returns).
    - Low `flips` count = cleaner trend-following signal.
    - `direction_flip` markers make great overlay pins on a price chart.

    **Page design.** Time-series panel: top line = `score` colored by direction;
    bottom histogram of `realized_return`. Triangle glyphs at each `direction_flip`.
    KPI row from `summary`.
    """
    if signal_name not in _VALID_SIGNAL_EVENT_NAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown signal_name '{signal_name}'.",
        )
    sym = symbol.upper()
    rows = await db.get_signal_component_events(
        sym, signal_name, limit=limit, horizon=horizon
    )

    flips = [r for r in rows if r.get("direction_flip")]
    bullish = sum(1 for r in rows if r.get("direction") == "bullish")
    bearish = sum(1 for r in rows if r.get("direction") == "bearish")
    neutral = sum(1 for r in rows if r.get("direction") == "neutral")

    return {
        "underlying": sym,
        "signal_name": signal_name,
        "horizon": horizon,
        "rows": rows,
        "count": len(rows),
        "summary": {
            "flips": len(flips),
            "bullish": bullish,
            "bearish": bearish,
            "neutral": neutral,
            "latest_timestamp": rows[0]["timestamp"] if rows else None,
            "latest_direction": rows[0]["direction"] if rows else "neutral",
        },
    }


_ADVANCED_SIGNAL_NAMES: tuple[str, ...] = (
    "vol_expansion",
    "eod_pressure",
    "squeeze_setup",
    "trap_detection",
    "zero_dte_position_imbalance",
    "gamma_vwap_confluence",
)


async def _confluence_matrix_response(
    db: DatabaseManager,
    symbol: str,
    lookback: int,
    component_names: list[str],
) -> dict[str, Any]:
    matrix = await db.get_signal_confluence_matrix(
        symbol.upper(),
        component_names=component_names,
        lookback=lookback,
    )
    return {
        "underlying": symbol.upper(),
        "lookback": lookback,
        "components": matrix.get("components", []),
        "matrix": matrix.get("matrix", []),
        "row_order": matrix.get("components", []),
        "sample_count": matrix.get("sample_count", 0),
        "latest_timestamp": matrix.get("latest_timestamp"),
    }


@router.get("/advanced/confluence-matrix")
async def get_advanced_confluence_matrix(
    symbol: str = Query(default="SPY"),
    lookback: int = Query(default=120, ge=10, le=2000),
    db: DatabaseManager = Depends(get_db),
):
    """6×6 advanced-signal agreement matrix — pairwise directional confluence over a rolling window.

    Shows how often each pair of Advanced Signals points the same direction over the
    last N snapshots. Useful for spotting persistent divergences and unusual
    breakdowns in normally-correlated signals.

    **Logic** (`src/api/database.py`): Joins `signal_scores` and
    `signal_component_scores` for the last `lookback` timestamps, filtering to
    the six Advanced Signals persisted by `AdvancedSignalEngine`. Signs are
    bucketed with `neutral_epsilon = 0.02` (±0.02 counts as neutral).
    Agreement = same non-zero sign; disagreement = opposite non-zero signs.

    **Params:**
    - `symbol` (default `SPY`).
    - `lookback` — 10–2000, default 120.

    **Signals (6, fixed order):** `vol_expansion`, `eod_pressure`, `squeeze_setup`,
    `trap_detection`, `zero_dte_position_imbalance`, `gamma_vwap_confluence`.

    **Returns:**
    ```json
    {
      "underlying": "SPY",
      "lookback": 120,
      "components": ["vol_expansion", "eod_pressure", "...4 more..."],
      "row_order": ["vol_expansion", "eod_pressure", "...4 more..."],
      "matrix": {
        "vol_expansion": {
          "eod_pressure": {
            "observations": 118,
            "active_observations": 92,
            "agreement_count": 74,
            "disagreement_count": 18,
            "neutral_count": 26,
            "agreement_ratio": 0.8043,
            "disagreement_ratio": 0.1957,
            "net_confluence": 0.6087
          }
        }
      },
      "sample_count": 118,
      "latest_timestamp": "2026-04-22T14:55:00Z"
    }
    ```

    - `agreement_ratio` — agree / active_observations; 4 decimals; `null` when active == 0.
    - `disagreement_ratio` — disagree / active_observations; `null` when active == 0.
    - `net_confluence` — (agree − disagree) / active_observations; [-1, +1].

    **Trader interpretation:**
    - `net_confluence > 0.5` — signals that routinely agree; unexpected divergence is a flag.
    - `net_confluence < -0.3` — persistent disagreement pairs; useful early-warning divergences.

    **Page design.** 6×6 heatmap. Color = `net_confluence` (green +1 → red -1, white neutral).
    Cell tooltip: agreement_ratio / disagreement_ratio / observations. Sort rows by average
    agreement to surface consensus signals at top, outliers at bottom.
    """
    return await _confluence_matrix_response(
        db, symbol, lookback, list(_ADVANCED_SIGNAL_NAMES)
    )


@router.get("/basic/confluence-matrix")
async def get_basic_confluence_matrix(
    symbol: str = Query(default="SPY"),
    lookback: int = Query(default=120, ge=10, le=2000),
    db: DatabaseManager = Depends(get_db),
):
    """6×6 basic-signal agreement matrix — pairwise directional confluence over a rolling window.

    Parallel to `/api/signals/advanced/confluence-matrix`, but scoped to the six
    Basic Signals persisted by `BasicSignalEngine`. These are continuous
    directional reads (no triggered events) so every non-zero snapshot
    contributes to agreement/disagreement counts.

    **Params:**
    - `symbol` (default `SPY`).
    - `lookback` — 10–2000, default 120.

    **Signals (6, fixed order):** `tape_flow_bias`, `skew_delta`, `vanna_charm_flow`,
    `dealer_delta_pressure`, `gex_gradient`, `positioning_trap`.

    **Returns:** identical schema to the advanced variant — see
    `/api/signals/advanced/confluence-matrix` for field docs.
    """
    return await _confluence_matrix_response(
        db, symbol, lookback, list(_BASIC_SIGNAL_NAMES)
    )
