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

    # Active MSI components after Phase 2.1: gamma_anchor replaces the three
    # old gamma-cluster components (flip_distance / local_gamma /
    # price_vs_max_gamma); their per-cycle scores remain visible nested
    # inside gamma_anchor's `context` field.
    expected = {
        "net_gex_sign",
        "gamma_anchor",
        "put_call_ratio",
        "volatility_regime",
        "order_flow_imbalance",
        "dealer_delta_pressure",
    }
    out: dict[str, Any] = {}
    for name, payload in value.items():
        if name not in expected or not isinstance(payload, dict):
            continue
        points = payload.get("max_points", payload.get("points"))
        contribution = payload.get("contribution")
        score = payload.get("score")
        if isinstance(points, (int, float)) and isinstance(contribution, (int, float)):
            entry: dict[str, Any] = {
                "max_points": float(points),
                "contribution": round(float(contribution), 4),
                "score": round(float(score), 6) if isinstance(score, (int, float)) else score,
            }
            # Pass the per-component context dict through when the engine
            # populates one — this is where gamma_anchor exposes its three
            # subscores (flip_distance_subscore, local_gamma_subscore,
            # price_vs_max_gamma_subscore) plus the blend weights.
            context = payload.get("context")
            if isinstance(context, dict) and context:
                entry["context"] = context
            out[name] = entry
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
    | `net_gex_sign` | 16 | Sign of dealer net gamma |
    | `gamma_anchor` | 30 | Blended proximity to gamma flip / local gamma density / max-gamma strike |
    | `put_call_ratio` | 12 | OI-weighted P/C tilt |
    | `volatility_regime` | 6 | Realized/VIX regime |
    | `order_flow_imbalance` | 19 | Smart-money premium-weighted call vs put flow |
    | `dealer_delta_pressure` | 17 | Dealer net delta forced-hedge direction |

    `gamma_anchor` exposes its three sub-signals nested under `context`:
    `flip_distance_subscore`, `local_gamma_subscore`, `price_vs_max_gamma_subscore`,
    plus the active `blend_weights`. Use those for the per-sub-signal breakdown
    that previously appeared as standalone components.

    **Returns:**
    ```json
    {
      "composite_score": 63.42,
      "components": {
        "net_gex_sign":          {"max_points": 16, "contribution":  9.60, "score":  0.6},
        "gamma_anchor":          {
          "max_points": 30,
          "contribution":  -2.10,
          "score": -0.07,
          "context": {
            "score": -0.07,
            "flip_distance_subscore":  0.21,
            "local_gamma_subscore":   -0.42,
            "price_vs_max_gamma_subscore": 0.17,
            "blend_weights": {"flip_distance": 0.45, "local_gamma": 0.35, "price_vs_max_gamma": 0.20}
          }
        },
        "put_call_ratio":        {"max_points": 12, "contribution":  2.40, "score":  0.2},
        "volatility_regime":     {"max_points":  6, "contribution": -0.08, "score": -0.013},
        "order_flow_imbalance":  {"max_points": 19, "contribution":  6.65, "score":  0.35},
        "dealer_delta_pressure": {"max_points": 17, "contribution":  3.40, "score":  0.2}
      }
    }
    ```

    - `composite_score` — float [0, 100]; `50` is the neutral/fallback value.
    - `components[*].max_points` — the component's weight ceiling.
    - `components[*].contribution` — signed points added to the baseline, rounded to 4 decimals.
    - `components[*].score` — raw component score [-1, +1], 6-decimal precision.
    - `components[*].context` — optional, present when the component emits diagnostic
      sub-fields (e.g. `gamma_anchor` exposes its three subscores here).

    **Regime interpretation:**
    - **≥ 70** — trend/expansion; favor directional trades in the prevailing bias.
    - **40–70** — controlled trend; moderate directional edge, size down.
    - **20–40** — chop/range; fade extremes, avoid trend trades.
    - **< 20** — high-risk reversal; mean-reversion only.

    **Page design.** Big radial gauge (0–100). Horizontal bar stack below showing
    each component's signed contribution. Hover for `score` and `max_points`.
    For `gamma_anchor` specifically, render a smaller secondary breakdown of the
    three subscores from `context` so operators retain visibility into which
    sub-signal is driving the blended reading.
    """
    row = await db.get_latest_signal_score_enriched(underlying.upper())
    if not row:
        raise HTTPException(status_code=404, detail=f"No score rows found for {underlying.upper()}")
    return _normalize_signal_score_row(row)


_PLAYBOOK_ENGINE: "PlaybookEngine | None" = None


def _get_playbook_engine() -> "PlaybookEngine":
    """Lazily instantiate the engine so pattern discovery only runs once."""
    global _PLAYBOOK_ENGINE
    if _PLAYBOOK_ENGINE is None:
        from src.signals.playbook import PlaybookEngine

        _PLAYBOOK_ENGINE = PlaybookEngine()
    return _PLAYBOOK_ENGINE


@router.get("/action")
async def get_action_card(
    underlying: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Latest **Action Card** — single, decisive trade instruction.

    The Playbook Engine fuses Market Posture (the MSI regime), advanced
    + basic signals, and live structural levels into one Card per cycle.
    A Card is either a specific trade (`BUY_PUT_DEBIT`, `SELL_CALL_SPREAD`,
    `BUY_IRON_CONDOR`, etc.), a position-management directive, or
    `STAND_DOWN` when no pattern matches current structure.

    `STAND_DOWN` is an **earned** outcome, not equivocation — it ships
    with a `near_misses[]` list naming the closest patterns and the
    specific trigger conditions they failed.

    **Params:** `underlying` (default `SPY`).
    Returns 404 when no signal_score row exists for the symbol yet.

    **Returns:** see `docs/playbook_catalog.md` §2 for the full schema.
    Trade Card example:
    ```json
    {
      "underlying": "SPY",
      "timestamp": "2026-05-01T18:42:13Z",
      "action": "SELL_CALL_SPREAD",
      "pattern": "call_wall_fade",
      "tier": "0DTE",
      "direction": "bearish",
      "confidence": 0.68,
      "size_multiplier": 0.6,
      "max_hold_minutes": 90,
      "legs": [
        {"expiry": "2026-05-01", "strike": 678.0, "right": "C", "side": "SELL", "qty": 1},
        {"expiry": "2026-05-01", "strike": 683.0, "right": "C", "side": "BUY",  "qty": 1}
      ],
      "entry":  {"ref_price": 678.40, "trigger": "at_touch"},
      "target": {"ref_price": 675.00, "kind": "level", "level_name": "max_pain"},
      "stop":   {"ref_price": 680.03, "kind": "premium_pct", "level_name": "call_wall_break"},
      "rationale": "Price $678.40 pinned at call wall $678.00, net GEX $7.1B, confirmed by trap_detection → call credit spread at the wall.",
      "context": {"...regime, msi, levels, signals_aligned..."},
      "alternatives_considered": []
    }
    ```

    STAND_DOWN example:
    ```json
    {
      "underlying": "SPY",
      "timestamp": "...",
      "action": "STAND_DOWN",
      "pattern": "stand_down",
      "tier": "n/a",
      "direction": "non_directional",
      "confidence": 0.0,
      "rationale": "No tradable structure. Closest patterns: call_wall_fade.",
      "near_misses": [
        {"pattern": "call_wall_fade",
         "missing": ["price 0.45% from call_wall (needs <= 0.20%)"]}
      ],
      "context": {"msi": 32.5, "regime": "chop_range"}
    }
    ```

    **Notes for PR-2.** The engine is computed on-demand each request from
    the latest persisted signal state — no Action Card persistence yet.
    Hysteresis (re-trigger suppression across cycles) is a no-op until
    PR-3 wires Card persistence and the cycle loop. Open-position
    awareness is also disabled until PR-3, so management Cards
    (`TAKE_PROFIT`, etc.) won't fire in PR-2.
    """
    from src.signals.playbook.context_builder import build_playbook_context

    sym = underlying.upper()
    ctx = await build_playbook_context(db=db, underlying=sym)
    if ctx is None:
        raise HTTPException(
            status_code=404,
            detail=f"No signal_score rows found for {sym}; cannot build playbook context",
        )
    engine = _get_playbook_engine()
    card = engine.evaluate(ctx)
    payload = card.to_dict()
    # Persist trade Cards (not STAND_DOWNs) so the next cycle can apply
    # hysteresis.  Best-effort — DB failure must not break the response.
    await db.insert_action_card(payload)
    return payload


@router.get("/score-history")
async def get_score_history(
    underlying: str = Query(default="SPY"),
    limit: int = Query(default=600, ge=1, le=5000),
    db: DatabaseManager = Depends(get_db),
):
    """Time series of the composite MSI, newest-first.

    **Params:** `underlying` (default `SPY`), `limit` (default 600, max 5000).
    The DB read also caps results to roughly two trading sessions back (the
    four most recent calendar days) so the default response covers the
    current and previous sessions without truncation across weekends.

    **Returns.** An array of objects with `timestamp`, `composite_score`, and
    `components` (same shape as `/score`). Rows are ordered by `timestamp DESC`
    so index 0 is the most recent. `timestamp` is ISO-8601 UTC of the engine
    cycle that produced the row.

    ```json
    [
      {
        "timestamp": "2026-04-22T18:55:00Z",
        "composite_score": 63.42,
        "components": {
          "net_gex_sign":          {"max_points": 16, "contribution":  9.60, "score":  0.6},
          "gamma_anchor":          {"max_points": 30, "contribution": -2.10, "score": -0.07,
                                    "context": {"flip_distance_subscore": 0.21,
                                                "local_gamma_subscore": -0.42,
                                                "price_vs_max_gamma_subscore": 0.17}},
          "put_call_ratio":        {"max_points": 12, "contribution":  2.40, "score":  0.2},
          "volatility_regime":     {"max_points":  6, "contribution": -0.08, "score": -0.013},
          "order_flow_imbalance":  {"max_points": 19, "contribution":  6.65, "score":  0.35},
          "dealer_delta_pressure": {"max_points": 17, "contribution":  3.40, "score":  0.2}
        }
      }
    ]
    ```

    **Page design.** Line chart of `composite_score` over `timestamp` with shaded
    regime bands at 20/40/70. Stacked-area chart of component `contribution`
    values underneath shows which component flipped the regime.
    """
    rows = await db.get_signal_score_history(underlying.upper(), limit)
    normalized_rows = []
    for row in rows:
        normalized = _normalize_signal_score_row(row)
        normalized["timestamp"] = row.get("timestamp")
        normalized_rows.append(normalized)
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
    - `score_history` — up to 90 recent `{score, timestamp}` points, newest→oldest.

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
        raise HTTPException(
            status_code=404, detail=f"No vol-expansion score found for {symbol.upper()}"
        )
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
        raise HTTPException(
            status_code=404, detail=f"No eod-pressure score found for {symbol.upper()}"
        )
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
        raise HTTPException(
            status_code=404, detail=f"No squeeze-setup signal found for {symbol.upper()}"
        )
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
      AND gamma strengthening AND call wall NOT migrating up.
    - Downside-fail (bull fade): mirror, with put wall NOT migrating down.
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
      "net_gex_delta": 840000000.0,
      "net_gex_delta_pct": 0.018,
      "broken_resistance_level": 680.0,
      "broken_support_level": null,
      "breakout_buffer_pct": 0.0008,
      "call_wall": 680.0, "prior_call_wall": 680.0,
      "put_wall": 670.0, "prior_put_wall": 670.0,
      "call_wall_migrated_up": false, "put_wall_migrated_down": false,
      "context_values": {"...close, realized_sigma, long_gamma, gamma_strengthening, call_flow_decelerating, put_flow_decelerating..."},
      "score_history": [{"score": -35.0, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` — [-100, +100].
    - `signal` — `"bullish_fade"` | `"bearish_fade"` | `"none"`.
    - `triggered` — `true` when `|score| ≥ 25`.
    - `breakout_up` / `breakout_down` — whether price has crossed the buffer.
    - `call_wall` / `prior_call_wall` — current and ~30min-ago resistance level.
    - `put_wall` / `prior_put_wall` — current and ~30min-ago support level.
    - `call_wall_migrated_up` — invalidates a bear fade when `true`.
    - `put_wall_migrated_down` — invalidates a bull fade when `true`.

    **Trader interpretation:**
    - `signal == "bearish_fade"` + `breakout_up == true` → price poked above the
      `broken_resistance_level` (now sitting *below* close) but dealers are long
      gamma and the call wall hasn't migrated; short-call-spread / put-debit.
    - `signal == "bullish_fade"` → mirror play; price slipped beneath
      `broken_support_level` (now sitting *above* close), put wall hasn't
      migrated, and is set up for a reclaim.
    - `call_wall_migrated_up == true` (bear fade) or `put_wall_migrated_down == true`
      (bull fade) → setup invalidated; dealers repositioning with price.

    **Note on field naming.** `broken_resistance_level` and `broken_support_level`
    refer to the level *price has just breached* — so on an upside breakout the
    "broken resistance" sits below close, and on a downside breakdown the
    "broken support" sits above close. This is intentional: the trap setup keys
    off the recently-breached level, not the next unbroken one above/below.

    **Page design.** Price ladder showing broken_support / close / broken_resistance
    with breakout-buffer bands. Red/green "TRAP" badge when triggered. Chips for
    `gamma_strengthening`, `call_wall_migrated_up`, `put_wall_migrated_down`.
    """
    row = await db.get_advanced_signal(symbol.upper(), "trap_detection")
    if not row:
        raise HTTPException(
            status_code=404, detail=f"No trap-detection signal found for {symbol.upper()}"
        )
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["triggered"] = ctx.get("triggered", False)
    row["signal"] = ctx.get("signal", "none")
    row["breakout_up"] = ctx.get("breakout_up", False)
    row["breakout_down"] = ctx.get("breakout_down", False)
    row["net_gex_delta"] = ctx.get("net_gex_delta")
    row["net_gex_delta_pct"] = ctx.get("net_gex_delta_pct")
    row["broken_resistance_level"] = ctx.get("broken_resistance_level")
    row["broken_support_level"] = ctx.get("broken_support_level")
    row["breakout_buffer_pct"] = ctx.get("breakout_buffer_pct")
    row["call_wall"] = ctx.get("call_wall")
    row["prior_call_wall"] = ctx.get("prior_call_wall")
    row["put_wall"] = ctx.get("put_wall")
    row["prior_put_wall"] = ctx.get("prior_put_wall")
    row["call_wall_migrated_up"] = ctx.get("call_wall_migrated_up")
    row["put_wall_migrated_down"] = ctx.get("put_wall_migrated_down")
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
        raise HTTPException(
            status_code=404, detail=f"No 0DTE position-imbalance signal found for {symbol.upper()}"
        )
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
    - `cluster_quality = max(0, 1 − cluster_gap_pct / 0.5%)`;
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
      "gamma_flip": 677.82, "vwap": 678.10,
      "max_pain": 678.0, "max_gamma": 678.5, "call_wall": 681.0,
      "expected_target": 680.5,
      "context_values": {"...gamma_flip, vwap, max_pain, max_gamma, call_wall, cluster_gap_pct, cluster_members, cluster_quality, distance_from_level_pct, regime_direction, net_gex..."},
      "score_history": [{"score": 22.0, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` — [-100, +100].
    - `signal` — `"bullish_confluence"` | `"bearish_confluence"` | `"neutral"`.
    - `triggered` — `true` when `|score| ≥ 20`.
    - `confluence_level` — price of the cluster midpoint.
    - `cluster_gap_pct` — |flip − vwap| / close; [0, ~0.005].
    - `gamma_flip`, `vwap`, `max_pain`, `max_gamma`, `call_wall` — raw input
      levels used in the computation; `null` when unavailable. Always present
      regardless of whether the level ended up in `cluster_members`.
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
        raise HTTPException(
            status_code=404, detail=f"No gamma+VWAP confluence signal found for {symbol.upper()}"
        )
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["triggered"] = ctx.get("triggered", False)
    row["signal"] = ctx.get("signal", "none")
    row["confluence_level"] = ctx.get("confluence_level")
    row["cluster_gap_pct"] = ctx.get("cluster_gap_pct")
    row["gamma_flip"] = ctx.get("gamma_flip")
    row["vwap"] = ctx.get("vwap")
    row["max_pain"] = ctx.get("max_pain")
    row["max_gamma"] = ctx.get("max_gamma")
    row["call_wall"] = ctx.get("call_wall")
    row["expected_target"] = ctx.get("expected_target")
    return row


@router.get("/advanced/range-break-imminence")
async def get_range_break_imminence_signal(
    symbol: str = Query(default="SPY"),
    db: DatabaseManager = Depends(get_db),
):
    """Range-break imminence — regime-switch detector between chop and breakout.

    Fuses four orthogonal inputs into a 0–100 imminence score so the
    dashboard can flip between *fade the range* and *follow the break*
    without the operator reading four panels at once. Standalone detector,
    not part of the MSI composite.

    **Logic highlights** (`src/signals/advanced/range_break_imminence.py`):
    - Skew extreme (30%): OTM-put vs OTM-call IV deviation vs baseline.
    - Dealer delta pressure (25%): signed dealer net delta (explicit or
      rolled up from `gex_by_strike` delta-OI columns).
    - Trap detection (25%): price pinned within ¼ of a 20-bar range
      extreme while flow accelerates *against* that extreme's fade.
    - Volatility compression (20%): 10-bar / 60-bar realized sigma ratio.
    - Directional bias = weighted avg of the three directional inputs;
      compression is directionless (adds magnitude only).
    - Imminence = weighted sum of absolute sub-scores; `score` = signed
      direction × (imminence / 100).
    - **Triggered when `imminence ≥ 65` (entering the Break Watch band).**

    **Params:** `symbol` (default `SPY`). Returns 404 when no data exists.

    **Returns:**
    ```json
    {
      "score": -62.0, "clamped_score": -0.62, "direction": "bearish",
      "triggered": true,
      "signal": "bearish_break_imminent",
      "imminence": 72.4,
      "label": "Break Watch",
      "playbook": "Stop blindly fading lows. Only fade after failed breakouts/reclaims; start preparing continuation entries.",
      "bias": -0.58,
      "context_values": {"...skew, dealer, trap, compression, weights..."},
      "score_history": [{"score": -62.0, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` — [-100, +100]; sign = break direction, magnitude = imminence.
    - `signal` — `"range_fade"` | `"bearish_break_imminent"` |
      `"bullish_break_imminent"` | `"break_watch_neutral"`.
    - `imminence` — [0, 100]; composite break risk magnitude.
    - `label` — `"Range Fade"` (0–39) | `"Weak Range"` (40–64) |
      `"Break Watch"` (65–79) | `"Breakout Mode"` (80–100).
    - `playbook` — trader-facing guidance string matching the label.
    - `triggered` — `true` when `imminence ≥ 65`.

    **Trader interpretation:**
    - `label == "Range Fade"` → fade extremes normally.
    - `label == "Weak Range"` → still fade, but smaller size / faster targets.
    - `label == "Break Watch"` → stop blindly fading; prepare retest trades.
    - `label == "Breakout Mode"` + direction set → trade the retest of the
      broken level rather than fading back into the range.

    **Page design.** Half-circle gauge (0–100) colored by direction with the
    label badge below. Four-bar stack chart of sub-score contributions
    (skew / dealer / trap / compression). Playbook text under the gauge.
    Flip the card's accent color between "fade" (neutral) and "follow"
    (break) colors at the 65-imminence threshold.
    """
    row = await db.get_advanced_signal(symbol.upper(), "range_break_imminence")
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No range-break-imminence signal found for {symbol.upper()}",
        )
    ctx = row.get("context_values") or {}
    row["score_history"] = row.get("score_history") or []
    row["triggered"] = ctx.get("triggered", False)
    row["signal"] = ctx.get("signal", "range_fade")
    row["imminence"] = ctx.get("imminence")
    row["label"] = ctx.get("label")
    row["playbook"] = ctx.get("playbook")
    row["bias"] = ctx.get("bias")
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
    """Latest snapshot of all six Basic Signals in a single round-trip.

    Basic Signals are continuous directional reads (clamped to `[-1, +1]`,
    scaled to `[-100, +100]`) that complement the 6 MSI components and 6
    Advanced Signals. They do **not** contribute to the composite MSI
    (weight=0). Each entry is the most recent row persisted to
    `signal_component_scores` by `BasicSignalEngine` each cycle.

    Use this to populate a dashboard overview without firing six separate
    requests. Click-through to the individual `/api/signals/basic/{name}`
    endpoint for signal-specific decomposition and history.

    **Signals (6, fixed order):** `tape_flow_bias`, `skew_delta`,
    `vanna_charm_flow`, `dealer_delta_pressure`, `gex_gradient`,
    `positioning_trap`.

    **Params:**
    - `symbol` (default `SPY`).

    **Returns:**
    ```json
    {
      "underlying": "SPY",
      "signals": {
        "tape_flow_bias":        {"score": 28.4,  "clamped_score": 0.284,  "direction": "bullish", "timestamp": "2026-04-22T18:55:00Z", "context_values": {...}},
        "skew_delta":            {"score": -12.7, "clamped_score": -0.127, "direction": "bearish", "timestamp": "...", "context_values": {...}},
        "vanna_charm_flow":      {"score": 0.0,   "clamped_score": 0.0,    "direction": "neutral", "timestamp": "...", "context_values": {...}},
        "dealer_delta_pressure": {"score": 45.1,  "clamped_score": 0.451,  "direction": "bullish", "timestamp": "...", "context_values": {...}},
        "gex_gradient":          {"score": -8.3,  "clamped_score": -0.083, "direction": "bearish", "timestamp": "...", "context_values": {...}},
        "positioning_trap":      null
      }
    }
    ```

    - `signals[name]` — `null` when the signal has never persisted a row
      for this symbol (first-deployment case). Render `"—"` in the UI, not `0`.
    - `signals[name].score` — `clamped_score × 100`; `[-100, +100]`; 2 decimals.
    - `signals[name].clamped_score` — raw `[-1, +1]`.
    - `signals[name].direction` — `"bullish"` | `"bearish"` | `"neutral"` (sign of score).
    - `signals[name].timestamp` — ISO-8601 UTC of the engine cycle.
    - `signals[name].context_values` — signal-specific inputs/derived fields;
      same keys as the per-signal endpoint.

    **Trader interpretation.**
    - **All six aligned** (same sign) → high-conviction regime; follow the direction.
    - **Flow side** (`tape_flow_bias`, `positioning_trap`) **diverging from structure**
      (`gex_gradient`, `vanna_charm_flow`) → potential reversal; flow leads structure intraday.
    - **`0.0` during market hours** → signal *abstained* (below its data threshold),
      not "neutral conviction." Don't treat as a bearish read.

    **Page design.** Six KPI tiles in a 3×2 grid. Each tile: direction chip
    (green/red/gray), large `score` number, signal label. Click-through to
    the `/api/signals/basic/{name}` detail view. Null tiles render as `"—"`.
    Poll this endpoint together with `/basic/confluence-matrix` at cycle cadence.
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

    Answers: *right now, is the tape aggressively buying calls and selling
    puts (bullish) or the reverse (bearish)?* The ingestion layer
    Lee-Ready-classifies every print into `buy_premium` / `sell_premium`;
    this signal nets them and scores the per-side imbalance. Unlike
    `smart_money` (discrete "large premium" events), this watches the
    continuous tape — gives an earlier read on directional conviction.

    **Logic highlights** (`src/signals/basic/tape_flow_bias.py`):
    - `call_net = call_buy_premium − call_sell_premium` (aggressor side).
    - `put_net  = put_buy_premium  − put_sell_premium`.
    - `directional = call_net − put_net` (call buying = bullish; put buying = bearish).
    - `ratio = directional / (|call_net| + |put_net|)` in `[-1, +1]`.
    - `score = clip(ratio / SIGNAL_TAPE_FLOW_SATURATION, [-1, 1])` (default saturation 0.6).
    - **Abstains (score=0)** if `|call_net| + |put_net| < SIGNAL_TAPE_FLOW_MIN_PREMIUM`
      (default $250K). Treat abstention as "no data," not neutral conviction.

    **Params:**
    - `symbol` (default `SPY`). Returns 404 if no row exists yet.

    **Returns:**
    ```json
    {
      "underlying": "SPY",
      "timestamp": "2026-04-22T18:55:00Z",
      "clamped_score": 0.28, "score": 28.0, "direction": "bullish",
      "weighted_score": 0.0, "weight": 0.0,
      "call_net_premium": 312000.0,
      "put_net_premium": -85000.0,
      "source": "flow_by_type",
      "context_values": {
        "call_net_premium": 312000.0, "put_net_premium": -85000.0,
        "call_buy_premium": 450000.0, "call_sell_premium": 138000.0,
        "put_buy_premium": 120000.0, "put_sell_premium": 205000.0,
        "source": "flow_by_type"
      },
      "score_history": [{"score": 12.4, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` / `clamped_score` — `[-100, +100]` / `[-1, +1]`. `+` = calls bought / puts sold.
    - `direction` — `"bullish"` | `"bearish"` | `"neutral"` (sign of score).
    - `call_net_premium` / `put_net_premium` — USD, signed. `+` = net aggressor buying.
    - `source` — `"flow_by_type"` (data present) | `"unavailable"` (abstained).
    - `context_values` — the four per-side premium totals plus `source`.
    - `score_history` — up to 90 recent `{score, timestamp}` points, newest→oldest.

    **Trader interpretation.**
    - `score > +50` with flat price → early bullish accumulation; potential breakout.
    - `score < −50` with flat price → early distribution; watch for breakdown.
    - Score **sign-flips** while price keeps trending → exhaustion warning.
    - Score ≈ 0 during market hours with `source="unavailable"` → thin tape, not neutral.

    **Page design.** Horizontal bidirectional bar (−100 → +100) with center
    needle, green right / red left. Below: stacked bar of the four premium
    components so the trader sees *which side* drives the imbalance.
    Sparkline of `score_history` (90 pts) beneath. Footer chip shows `source`.
    """
    row = await db.get_basic_signal(symbol.upper(), "tape_flow_bias")
    if not row:
        raise HTTPException(
            status_code=404, detail=f"No tape-flow-bias score found for {symbol.upper()}"
        )
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
    """Short-dated OTM put-vs-call IV deviation — real-time fear gauge.

    Answers: *are traders paying an unusual premium for downside protection
    right now?* Equity-index skew is structurally positive (OTM puts always
    trade richer than OTM calls), so this signal scores the **deviation
    from normal**, not the raw spread. Bid-up put skew typically moves
    *before* the tape confirms bearishness — useful leading-indicator.

    **Logic highlights** (`src/signals/basic/skew_delta.py`):
    - `spread = otm_put_iv − otm_call_iv` from `ctx.extra['skew']`
      (populated by the unified engine from `option_chains` near ATM).
    - `deviation = spread − SIGNAL_SKEW_BASELINE` (default baseline 0.02).
    - `score = −clip(deviation / SIGNAL_SKEW_SATURATION, [-1, 1])`
      (default saturation 0.04). Negative sign: elevated put skew → bearish.
    - **Abstains (score=0)** if `otm_put_iv` or `otm_call_iv` is missing.

    **Params:**
    - `symbol` (default `SPY`). Returns 404 if no row exists yet.

    **Returns:**
    ```json
    {
      "underlying": "SPY", "timestamp": "...",
      "clamped_score": -0.35, "score": -35.0, "direction": "bearish",
      "weighted_score": 0.0, "weight": 0.0,
      "otm_put_iv": 0.192,
      "otm_call_iv": 0.147,
      "spread": 0.045,
      "deviation": 0.025,
      "context_values": {
        "otm_put_iv": 0.192, "otm_call_iv": 0.147,
        "spread": 0.045, "baseline": 0.02, "deviation": 0.025
      },
      "score_history": [{"score": -14.1, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` / `clamped_score` — `[-100, +100]` / `[-1, +1]`; negative = fear bid.
    - `otm_put_iv` / `otm_call_iv` — IV as a fraction (e.g. `0.18` = 18%). `null` if missing.
    - `spread` — `otm_put_iv − otm_call_iv`; typically `[0, 0.10]` for SPY. `null` if missing.
    - `deviation` — `spread − baseline`; sign drives the score (positive → bearish score).
    - `context_values` — the above plus `baseline` (the neutral-skew reference).
    - `score_history` — up to 90 recent points.

    **Trader interpretation.**
    - `score < −40` → meaningful fear bid; tighten longs, consider downside hedges.
    - `score > +30` (rare, call skew) → potential upside squeeze / short-covering setup.
    - Steady negative drift while price rallies → distribution / bull-trap warning.
    - `otm_put_iv == null` → no short-dated data this cycle; not the same as neutral.

    **Page design.** Bidirectional ±100 gauge labeled "fear" (left) / "euphoria"
    (right). Below, a two-line mini-chart of `otm_put_iv` and `otm_call_iv` as
    % IV over `score_history` length, with a horizontal reference line at
    `baseline` (the "normal skew" line). Color the gauge card red when
    `score ≤ -40`, green when `score ≥ +30`.
    """
    row = await db.get_basic_signal(symbol.upper(), "skew_delta")
    if not row:
        raise HTTPException(
            status_code=404, detail=f"No skew-delta score found for {symbol.upper()}"
        )
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
    """Second-order greek dealer-hedging pressure (vanna + charm).

    Answers: *which way are dealers being forced to trade right now from IV
    moves and time decay, independently of spot moves?*

    - **Vanna** (dVega/dSpot) — dealer delta changes when IV moves. Morning
      IV crush forces vanna-short dealers to buy underlying (the classic
      "vol-crush rally" that kills naked put sellers).
    - **Charm** (dDelta/dTime) — decay of short-dated deltas toward expiry.
      In the final ~2h, charm-short dealers short calls above spot are
      forced to sell into weakness — accelerates afternoon drift.

    **Logic highlights** (`src/signals/basic/vanna_charm_flow.py`):
    - Sum `dealer_vanna_exposure` + `dealer_charm_exposure × charm_amplification`
      across all strikes in `gex_by_strike`.
    - `charm_amplification` — 1.0 most of the day, ramps to 1.5 in the final
      ~40% of the session (charm flow dominates into the close).
    - `score = clip(combined / vc_norm, [-1, 1])`; `vc_norm` defaults to 5e7
      (may be scaled per symbol via `normalizers`).
    - **Abstains (score=0)** if `gex_by_strike` is empty.
    - Legacy rows (no dealer columns) fall back to negated market-aggregate
      exposures — signal still valid, less precise.

    **Sign convention:** `+` score = dealer buying pressure (bullish tailwind);
    `−` score = dealer selling pressure (bearish headwind).

    **Params:**
    - `symbol` (default `SPY`). Returns 404 if no row exists yet.

    **Returns:**
    ```json
    {
      "underlying": "SPY", "timestamp": "...",
      "clamped_score": 0.42, "score": 42.0, "direction": "bullish",
      "weighted_score": 0.0, "weight": 0.0,
      "vanna_total": 18500000.0,
      "charm_total": 9200000.0,
      "charm_amplification": 1.18,
      "source": "dealer_exposure",
      "context_values": {
        "vanna_total": 18500000.0, "charm_total": 9200000.0,
        "charm_amplification": 1.18, "vc_norm": 50000000.0,
        "source": "dealer_exposure"
      },
      "score_history": [{"score": 31.2, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` / `clamped_score` — `[-100, +100]` / `[-1, +1]`; `+` = bullish.
    - `vanna_total` — sum of dealer vanna exposure; `+` = dealer delta grows with spot ↑.
    - `charm_total` — sum of dealer charm exposure; `+` = dealer delta grows with time.
    - `charm_amplification` — `[1.0, 1.5]`; session-time multiplier. `1.5` = final 40%.
    - `source` — `"dealer_exposure"` | `"market_exposure_negated"` (legacy fallback) | `"unavailable"`.
    - `context_values` — the above plus `vc_norm` (saturation denominator).
    - `score_history` — up to 90 recent points.

    **Trader interpretation.**
    - `score > +40` in the morning → vanna-driven melt-up; trend-follow upside.
    - `score < −40` in the final 90 minutes → charm-driven afternoon fade; momentum shorts.
    - Crosses zero into the afternoon → pressure reversal; trim directional size.
    - `charm_amplification = 1.5` means we're in the EOD acceleration window.
    - `source = "market_exposure_negated"` → lower-precision fallback; widen thresholds.

    **Page design.** Two stacked horizontal bars: `vanna_total` and
    `charm_total × charm_amplification`, colored by sign. A small clock gauge
    shows `charm_amplification` on a 1.0→1.5 scale as "time pressure."
    Composite `score` as a big number + direction chip on top. Source chip in footer.
    """
    row = await db.get_basic_signal(symbol.upper(), "vanna_charm_flow")
    if not row:
        raise HTTPException(
            status_code=404, detail=f"No vanna-charm-flow score found for {symbol.upper()}"
        )
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

    Answers: *are dealers net-short delta (forced to buy rallies, bullish)
    or net-long (forced to sell rallies, bearish)?* Delta flow leads gamma
    exposure by minutes intraday — closest thing to a leading indicator
    for 0DTE regimes. Gamma tells you *where* dealers will hedge; delta
    tells you *how much they already are*.

    **Logic highlights** (`src/signals/basic/dealer_delta_pressure.py`).
    Three data paths in priority order:
    1. `ctx.dealer_net_delta` — if populated upstream, used directly.
    2. `gex_by_strike.{call_delta_oi, put_delta_oi}` — sum
       `−(call_delta_oi + put_delta_oi)` across strikes (dealer sign is
       flipped from customer OI; customers are typically long calls & puts).
    3. Distance-proxy fallback — use `call_oi` / `put_oi` with a linear
       delta proxy (0.5 at ATM, decaying to 0 at ±5% OTM), ×100 shares/contract.

    - `score = −clip(dni / SIGNAL_DNI_NORM, [-1, 1])` (default norm ~$3e8
      shares-equivalent). **Inverted**: dealer short delta (negative DNI)
      scores bullish, because they must buy into strength.
    - **Abstains (score=0)** if no data path yields an estimate.

    **Params:**
    - `symbol` (default `SPY`). Returns 404 if no row exists yet.

    **Returns:**
    ```json
    {
      "underlying": "SPY", "timestamp": "...",
      "clamped_score": 0.45, "score": 45.1, "direction": "bullish",
      "weighted_score": 0.0, "weight": 0.0,
      "dealer_net_delta_estimated": -135000000.0,
      "dni_normalized": -0.45,
      "source": "gex_by_strike.delta_oi",
      "context_values": {
        "dealer_net_delta_estimated": -135000000.0,
        "dni_normalized": -0.45,
        "source": "gex_by_strike.delta_oi"
      },
      "score_history": [{"score": 28.1, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` / `clamped_score` — `[-100, +100]` / `[-1, +1]`; `+` = bullish.
    - `dealer_net_delta_estimated` — shares-equivalent, signed.
      Negative = dealer net **short** delta (bullish for price after inversion).
    - `dni_normalized` — `dni / DNI_NORM` clipped to `[-1, +1]`.
      **Sign is opposite the score** (signal is inverted).
    - `source` — data-path quality:
      `"dealer_net_delta_field"` (best) | `"gex_by_strike.delta_oi"` |
      `"gex_by_strike.distance_proxy"` (weakest) | `"unavailable"`.
    - `score_history` — up to 90 recent points.

    **Trader interpretation.**
    - `score > +60` → dealers deeply short delta; any up-move likely accelerated
      (chase risk). Favor long-delta plays (calls, long futures).
    - `score < −60` → dealers net long; rallies will be sold into. Favor
      mean-reversion or short-delta debit spreads.
    - `source = "gex_by_strike.distance_proxy"` → fallback-quality estimate;
      widen conviction threshold before acting.
    - **Cross-signal:** when this disagrees with `gex_gradient` (structural
      view), trust `dealer_delta_pressure` on a ≤ 30m horizon.

    **Page design.** Single horizontal ±100 bar labeled "Dealer Delta Pressure."
    Subtext: `"Dealers short delta → bullish"` on `+` side;
    `"Dealers long delta → bearish"` on `−` side. Data-quality chip shows
    the `source` value. Sparkline of `score_history` beneath.
    """
    row = await db.get_basic_signal(symbol.upper(), "dealer_delta_pressure")
    if not row:
        raise HTTPException(
            status_code=404, detail=f"No dealer-delta-pressure score found for {symbol.upper()}"
        )
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

    Answers: *is dealer gamma stacked above or below current price, and
    how does that bias the next move?* Decomposes per-strike gamma
    exposure into four zones (above / below spot, ATM, wings) and scores
    the asymmetry, flipping sign and damping under long-gamma regimes.

    - Heavy **above-spot** concentration + short gamma → any up-move
      unwinds → **bullish**. Below-spot mirror is **bearish**.
    - Under **long-gamma** the same concentration acts as resistance
      instead of fuel, so the signal is inverted and damped.

    **Logic highlights** (`src/signals/basic/gex_gradient.py`):
    - Classify each strike in `gex_by_strike` by `(strike − spot) / spot`:
      above/below, plus ATM (`≤ ±1.5%`) and wing (`≥ ±4%`) tags.
    - `asymmetry = (above_abs − below_abs) / (above_abs + below_abs)` ∈ `[-1, +1]`.
    - `raw = asymmetry` if `net_gex < 0`, else `−asymmetry × 0.40`
      (`SIGNAL_GEX_GRADIENT_LONG_GAMMA_DAMPING`).
    - `confidence = max(0.25, 1 − wing_fraction)` — wings pin, kill directional edge.
    - `score = clip(raw × confidence, [-1, 1])`.
    - **Abstains (score=0)** if total gamma `< SIGNAL_GEX_GRADIENT_MIN_GAMMA`
      (default 5e7 — thin-OI guard).

    **Params:**
    - `symbol` (default `SPY`). Returns 404 if no row exists yet.

    **Returns:**
    ```json
    {
      "underlying": "SPY", "timestamp": "...",
      "clamped_score": 0.38, "score": 38.0, "direction": "bullish",
      "weighted_score": 0.0, "weight": 0.0,
      "above_spot_gamma_abs": 72500000.0,
      "below_spot_gamma_abs": 31200000.0,
      "asymmetry": 0.3986,
      "wing_fraction": 0.21,
      "context_values": {
        "source": "gex_by_strike",
        "above_spot_gamma_abs": 72500000.0, "below_spot_gamma_abs": 31200000.0,
        "atm_gamma_abs": 45800000.0, "wing_gamma_abs": 21900000.0,
        "above_spot_gamma_signed": 54300000.0, "below_spot_gamma_signed": -18700000.0,
        "wing_fraction": 0.21, "asymmetry": 0.3986, "strike_count": 42
      },
      "score_history": [{"score": 22.0, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` / `clamped_score` — `[-100, +100]` / `[-1, +1]`.
    - `above_spot_gamma_abs` / `below_spot_gamma_abs` — `|Σ gamma|` for each side; USD-scaled. `null` if `source="unavailable"`.
    - `asymmetry` — `[-1, +1]`; pre-regime adjustment. Matches score sign under `net_gex < 0`, flipped under `net_gex > 0`.
    - `wing_fraction` — `[0, 1]`; share of total `|gamma|` at wing strikes (`> ±4%` OTM).
    - `context_values.atm_gamma_abs` / `wing_gamma_abs` — absolute gamma in each bucket.
    - `context_values.above_spot_gamma_signed` / `below_spot_gamma_signed` — signed sums.
    - `context_values.strike_count` — int; strikes surveyed. Thin data → widen thresholds.
    - `context_values.source` — `"gex_by_strike"` | `"unavailable"`.

    **Trader interpretation.**
    - `score > +50` with `net_gex < 0` → classic short-gamma upside setup;
      dealers will chase. Favor calls.
    - `score < −50` with `net_gex < 0` → short-gamma downside setup; dealers
      accelerate flush.
    - **Strong score with `net_gex > 0`** → structural resistance in that
      direction; fade instead of follow.
    - `wing_fraction > 0.5` → confidence already reduced by code; treat score
      as weak even if magnitude is high.
    - `strike_count < 10` → sparse data; widen threshold.

    **Page design.** Four-zone horizontal strike map centered on spot: above-
    and below-spot buckets as mirrored bars, intensity by `|signed gamma|`.
    Separate ATM / wing donut slices showing concentration share. `asymmetry`
    gauge (−1 → +1). Regime chip (`net_gex > 0` vs `< 0`) since the same
    asymmetry has opposite implications in each regime.
    """
    row = await db.get_basic_signal(symbol.upper(), "gex_gradient")
    if not row:
        raise HTTPException(
            status_code=404, detail=f"No gex-gradient score found for {symbol.upper()}"
        )
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

    Flags setups where tape behavior is starting to invalidate crowd
    direction — the classic squeeze/flush patterns.

    - **`+score` (squeeze risk):** puts crowded + aggressive put-buying +
      up-momentum + price above gamma flip + short-gamma regime →
      upside squeeze fuel.
    - **`−score` (flush risk):** calls crowded + aggressive call-buying +
      down-momentum + price below gamma flip + short-gamma → downside
      air-pocket.

    Uses **signed** net smart-money premium (`buy − sell`) from
    `flow_contract_facts` when available — more informative than legacy
    total_premium because opposite-side buying nets out (a big put-buy
    + big put-sell should *not* count as crowd skew).

    **Logic highlights** (`src/signals/basic/positioning_trap.py`):
    ```
    short_crowding = clip((put_call_ratio − 1.05) / 0.35, [0, 1])
    long_crowding  = clip((0.95 − put_call_ratio) / 0.35, [0, 1])
    put_skew       = max(0, −smart_imbalance)
    call_skew      = max(0, +smart_imbalance)

    squeeze = 0.45·short_crowding + 0.25·put_skew
            + 0.15·clip(momentum_5bar / 0.004, [0,1])
            + 0.10·above_flip + 0.05·neg_gex
    flush   = 0.45·long_crowding  + 0.25·call_skew
            + 0.15·clip(−momentum_5bar / 0.004, [0,1])
            + 0.10·below_flip + 0.05·neg_gex
    score   = clip(squeeze − flush, [-1, 1])
    ```
    - `smart_imbalance = (smart_call_net − smart_put_net) / (|call_net| + |put_net|)`,
      abstaining when denominator `< $100K`.

    **Params:**
    - `symbol` (default `SPY`). Returns 404 if no row exists yet.

    **Returns:**
    ```json
    {
      "underlying": "SPY", "timestamp": "...",
      "clamped_score": 0.52, "score": 52.0, "direction": "bullish",
      "weighted_score": 0.0, "weight": 0.0,
      "smart_imbalance": -0.41,
      "smart_imbalance_source": "signed_net_premium",
      "momentum_5bar": 0.0025,
      "context_values": {
        "put_call_ratio": 1.28, "smart_imbalance": -0.41,
        "smart_imbalance_source": "signed_net_premium",
        "momentum_5bar": 0.0025, "close": 577.24,
        "gamma_flip": 574.5, "net_gex": -1.4e9
      },
      "score_history": [{"score": 31.0, "timestamp": "..."}, "...up to 90"]
    }
    ```

    - `score` / `clamped_score` — `[-100, +100]` / `[-1, +1]`. `+` = squeeze risk, `−` = flush risk.
    - `smart_imbalance` — `[-1, +1]`; `+` = call-buy heavy, `−` = put-buy heavy.
      Counter-intuitively, *put-heavy imbalance* drives `+score` (squeeze fuel).
    - `smart_imbalance_source` — `"signed_net_premium"` (best) | `"signed_top_level"` (older fields).
    - `momentum_5bar` — 5-bar price %, e.g. `0.0035` = +0.35%.
    - `context_values.put_call_ratio` — float, typically `[0.5, 2.0]`.
    - `context_values.close` / `gamma_flip` — used to derive `above_flip`/`below_flip`.
    - `context_values.net_gex` — signed; `< 0` amplifies both sides.
    - `score_history` — up to 90 recent points.

    **Trader interpretation.**
    - `score > +50` → upside squeeze setup. Long-delta; size up if
      `close > gamma_flip` AND `net_gex < 0`.
    - `score < −50` → downside flush setup. Put debit spreads; trim longs.
    - Moderate magnitude (±25–50) **plus contradictory `tape_flow_bias`**
      → the trap may already be unwinding; wait.
    - `smart_imbalance_source = "signed_top_level"` → lower-precision fields;
      signal still valid.

    **Page design.** Horizontal ±100 "trap meter" with "Flush Risk" (left)
    and "Squeeze Risk" (right). Decomposition panel showing the five input
    factors as mini-bars (short/long_crowding, put/call_skew, momentum,
    above/below_flip, neg_gex) so the trader sees *why*. Contextual footer
    with `put_call_ratio`, `momentum_5bar %`, `close vs gamma_flip`, `net_gex` sign.
    """
    row = await db.get_basic_signal(symbol.upper(), "positioning_trap")
    if not row:
        raise HTTPException(
            status_code=404, detail=f"No positioning-trap score found for {symbol.upper()}"
        )
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
    "range_break_imminence",
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
      `range_break_imminence`, `positioning_trap`, `vanna_charm_flow`.
      Returns 400 for unknown names.
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
    rows = await db.get_signal_component_events(sym, signal_name, limit=limit, horizon=horizon)

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
    "range_break_imminence",
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
    """N×N advanced-signal agreement matrix — pairwise directional confluence over a rolling window.

    Shows how often each pair of Advanced Signals points the same direction over the
    last N snapshots. Useful for spotting persistent divergences and unusual
    breakdowns in normally-correlated signals. Matrix size follows
    `_ADVANCED_SIGNAL_NAMES`.

    **Logic** (`src/api/database.py`): Joins `signal_scores` and
    `signal_component_scores` for the last `lookback` timestamps, filtering to
    the Advanced Signals persisted by `AdvancedSignalEngine`. Signs are
    bucketed with `neutral_epsilon = 0.02` (±0.02 counts as neutral).
    Agreement = same non-zero sign; disagreement = opposite non-zero signs.

    **Params:**
    - `symbol` (default `SPY`).
    - `lookback` — 10–2000, default 120.

    **Signals (fixed order):** `vol_expansion`, `eod_pressure`, `squeeze_setup`,
    `trap_detection`, `zero_dte_position_imbalance`, `gamma_vwap_confluence`,
    `range_break_imminence`.

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

    **Page design.** N×N heatmap sized to `_ADVANCED_SIGNAL_NAMES`. Color =
    `net_confluence` (green +1 → red -1, white neutral). Cell tooltip:
    agreement_ratio / disagreement_ratio / observations. Sort rows by average
    agreement to surface consensus signals at top, outliers at bottom.
    """
    return await _confluence_matrix_response(db, symbol, lookback, list(_ADVANCED_SIGNAL_NAMES))


@router.get("/basic/confluence-matrix")
async def get_basic_confluence_matrix(
    symbol: str = Query(default="SPY"),
    lookback: int = Query(default=120, ge=10, le=2000),
    db: DatabaseManager = Depends(get_db),
):
    """6×6 basic-signal agreement matrix — pairwise directional confluence.

    Parallel to `/api/signals/advanced/confluence-matrix`, scoped to the six
    Basic Signals persisted by `BasicSignalEngine`. Answers: *which of my
    basic signals normally agree, and where is an unusual divergence right now?*
    Continuous directional reads (no triggered events) — every non-zero
    snapshot contributes to agreement/disagreement counts.

    **Logic** (`src/api/database.py`): Joins `signal_scores` ×
    `signal_component_scores` for the last `lookback` timestamps, filtered
    to the six basic signal names. Each score is bucketed `+1 / 0 / −1`
    with `neutral_epsilon = 0.02`. For each ordered pair `(c1, c2)`:
    - `observations` = cycles where both signals have data.
    - `active_observations` = cycles where both signs are non-zero.
    - `agreement` = same non-zero sign; `disagreement` = opposite non-zero signs.

    **Params:**
    - `symbol` (default `SPY`).
    - `lookback` — 10–2000, default 120. Number of recent timestamps.

    **Signals (6, fixed order):** `tape_flow_bias`, `skew_delta`,
    `vanna_charm_flow`, `dealer_delta_pressure`, `gex_gradient`, `positioning_trap`.

    **Returns:**
    ```json
    {
      "underlying": "SPY",
      "lookback": 120,
      "components": ["tape_flow_bias", "skew_delta", "vanna_charm_flow",
                     "dealer_delta_pressure", "gex_gradient", "positioning_trap"],
      "row_order":  ["tape_flow_bias", "skew_delta", "...4 more..."],
      "matrix": {
        "tape_flow_bias": {
          "skew_delta": {
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
      "latest_timestamp": "2026-04-22T18:55:00Z"
    }
    ```

    - `components` / `row_order` — length-6 string array; axis labels.
    - `matrix[c1][c2].observations` — int, `[0, lookback]`; cycles where both had data.
    - `matrix[c1][c2].active_observations` — int, `[0, observations]`; both non-zero.
    - `matrix[c1][c2].agreement_count` — int, `[0, active_observations]`.
    - `matrix[c1][c2].disagreement_count` — int, `[0, active_observations]`.
    - `matrix[c1][c2].neutral_count` — int, `[0, observations]`; at least one side was 0.
    - `matrix[c1][c2].agreement_ratio` — `[0, 1]` | `null` if `active == 0`; 4 decimals.
    - `matrix[c1][c2].disagreement_ratio` — `[0, 1]` | `null`.
    - `matrix[c1][c2].net_confluence` — `[-1, +1]`; `0.0` if `active == 0`. Heatmap value.
    - `sample_count` — int; distinct timestamps aggregated.
    - `latest_timestamp` — ISO-8601 UTC | `null` if empty.

    **Trader interpretation.**
    - `net_confluence > +0.5` — pair agrees > 75% of active cycles. Live
      **disagreement** from such a pair is a flag.
    - `net_confluence < −0.3` — pair structurally disagrees. Live
      **agreement** = unusual consensus.
    - `active_observations < ~20` — weak sample; discount.
    - Diagonal cells always `net_confluence = 1.0`; sanity check only.

    **Page design.** 6×6 heatmap with diverging color scale (green = +1
    agree, white = 0, red = −1 disagree). Cell tooltip shows
    `agreement_ratio / disagreement_ratio / observations`. Sort rows by
    average `agreement_ratio` to surface consensus signals at top, outliers
    at bottom. Symmetric — render only the upper triangle if space-constrained.
    """
    return await _confluence_matrix_response(db, symbol, lookback, list(_BASIC_SIGNAL_NAMES))
