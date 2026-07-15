"""Online ML self-calibration for TradeWorkz bots.

Two things are learned per bot:

1. **A logistic-regression classifier** over a small feature vector
   ``[net_gex_norm, vix_norm, minutes_since_open_norm, spot_stretch_norm,
   conviction]`` predicting P(win). Weights live in ``tw_ml_state.weights``
   as a ``{feature: weight}`` JSONB dict, updated after each closed trade
   with a single SGD step.

2. **Bot-level scalars** — ``confidence_base`` (calibrated hit rate),
   ``size_multiplier`` (mapped from profit factor), ``confidence_threshold``
   (adaptive gate). These are recomputed from the rolling trade log by
   :func:`recalibrate_bot`, typically nightly.

Both surfaces read/write through :class:`MLState`.
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

from src.tradeworkz import config as tw_config

FEATURE_KEYS = (
    "net_gex_norm",
    "vix_norm",
    "minutes_since_open_norm",
    "spot_stretch_norm",
    "conviction",
    "bias",
)


@dataclass
class MLState:
    bot_id: str
    n_samples: int = 0
    n_wins: int = 0
    hit_rate: Optional[float] = None
    confidence_base: float = 0.55
    confidence_threshold: float = 0.55
    size_multiplier: float = 1.0
    weights: Dict[str, float] = field(default_factory=dict)
    feature_mean: Dict[str, float] = field(default_factory=dict)
    feature_var: Dict[str, float] = field(default_factory=dict)
    last_win_rate_7d: Optional[float] = None
    last_win_rate_30d: Optional[float] = None
    last_profit_factor: Optional[float] = None

    def to_row(self) -> Dict[str, Any]:
        row = asdict(self)
        row["weights"] = json.dumps(self.weights)
        row["feature_mean"] = json.dumps(self.feature_mean)
        row["feature_var"] = json.dumps(self.feature_var)
        return row


def load_state(conn: Any, bot_id: str) -> MLState:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT bot_id, n_samples, n_wins, hit_rate, confidence_base,
               confidence_threshold, size_multiplier, weights, feature_mean,
               feature_var, last_win_rate_7d, last_win_rate_30d,
               last_profit_factor
        FROM tw_ml_state WHERE bot_id = %s
        """,
        (bot_id,),
    )
    row = cur.fetchone()
    if row is None:
        return MLState(bot_id=bot_id)
    weights = row[7] or {}
    if isinstance(weights, str):
        weights = json.loads(weights)
    fmean = row[8] or {}
    if isinstance(fmean, str):
        fmean = json.loads(fmean)
    fvar = row[9] or {}
    if isinstance(fvar, str):
        fvar = json.loads(fvar)
    return MLState(
        bot_id=row[0],
        n_samples=row[1] or 0,
        n_wins=row[2] or 0,
        hit_rate=row[3],
        confidence_base=float(row[4] or 0.55),
        confidence_threshold=float(row[5] or 0.55),
        size_multiplier=float(row[6] or 1.0),
        weights=dict(weights),
        feature_mean=dict(fmean),
        feature_var=dict(fvar),
        last_win_rate_7d=row[10],
        last_win_rate_30d=row[11],
        last_profit_factor=row[12],
    )


def save_state(conn: Any, state: MLState) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tw_ml_state (
            bot_id, n_samples, n_wins, hit_rate, confidence_base,
            confidence_threshold, size_multiplier, weights, feature_mean,
            feature_var, last_win_rate_7d, last_win_rate_30d,
            last_profit_factor, computed_at
        ) VALUES (
            %(bot_id)s, %(n_samples)s, %(n_wins)s, %(hit_rate)s,
            %(confidence_base)s, %(confidence_threshold)s,
            %(size_multiplier)s, %(weights)s::jsonb, %(feature_mean)s::jsonb,
            %(feature_var)s::jsonb, %(last_win_rate_7d)s,
            %(last_win_rate_30d)s, %(last_profit_factor)s, NOW()
        )
        ON CONFLICT (bot_id) DO UPDATE SET
            n_samples = EXCLUDED.n_samples,
            n_wins = EXCLUDED.n_wins,
            hit_rate = EXCLUDED.hit_rate,
            confidence_base = EXCLUDED.confidence_base,
            confidence_threshold = EXCLUDED.confidence_threshold,
            size_multiplier = EXCLUDED.size_multiplier,
            weights = EXCLUDED.weights,
            feature_mean = EXCLUDED.feature_mean,
            feature_var = EXCLUDED.feature_var,
            last_win_rate_7d = EXCLUDED.last_win_rate_7d,
            last_win_rate_30d = EXCLUDED.last_win_rate_30d,
            last_profit_factor = EXCLUDED.last_profit_factor,
            computed_at = NOW()
        """,
        state.to_row(),
    )


def _extract_features(components: Dict[str, Any]) -> Dict[str, float]:
    """Turn the raw ``components_at_entry`` blob into the fixed feature dict.

    Every unknown feature defaults to 0.0 so bots that don't populate one
    still play well with the classifier — the online-update path clamps
    magnitudes to a sane range too.
    """

    def norm(v: Any, denom: float) -> float:
        try:
            f = float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0
        if denom == 0:
            return 0.0
        return max(-3.0, min(3.0, f / denom))

    return {
        "net_gex_norm": norm(components.get("net_gex"), 2.0e9),
        "vix_norm": norm(components.get("vix"), 20.0),
        "minutes_since_open_norm": norm(components.get("minutes_since_open"), 60.0),
        "spot_stretch_norm": norm(
            components.get("distance_pct")
            or components.get("stretch_pct")
            or components.get("drift_pct"),
            0.005,
        ),
        "conviction": max(0.0, min(1.0, float(components.get("conviction") or 0.0))),
        "bias": 1.0,
    }


def _sigmoid(z: float) -> float:
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    ez = math.exp(z)
    return ez / (1.0 + ez)


def online_update(state: MLState, components: Dict[str, Any], won: bool) -> MLState:
    """One SGD step over the logistic-regression weights.

    Learning rate is :data:`tw_config.CALIBRATION_LEARNING_RATE`. The bias
    term is included in ``FEATURE_KEYS`` so the classifier can learn a
    baseline win-rate offset.
    """
    lr = tw_config.CALIBRATION_LEARNING_RATE
    feats = _extract_features(components)
    weights = {k: float(state.weights.get(k, 0.0)) for k in FEATURE_KEYS}
    z = sum(weights[k] * feats[k] for k in FEATURE_KEYS)
    p = _sigmoid(z)
    y = 1.0 if won else 0.0
    err = p - y
    for k in FEATURE_KEYS:
        weights[k] -= lr * err * feats[k]
        # Cap weights at |5| so a rare outlier can't runaway the model.
        weights[k] = max(-5.0, min(5.0, weights[k]))
    state.weights = weights
    state.n_samples += 1
    if won:
        state.n_wins += 1
    state.hit_rate = state.n_wins / max(1, state.n_samples)
    return state


def predict_win_prob(weights: Any, components: Dict[str, Any]) -> Optional[float]:
    """Inference counterpart of :func:`online_update`: the classifier's P(win).

    Scores ``components`` under the learned ``weights`` using the *same*
    :func:`_extract_features` vector the SGD step fits, so training and
    inference never diverge. ``weights`` may be the ``{feature: weight}``
    dict or its JSON string (as stored on ``tw_ml_state``).

    Returns ``None`` — a signal to the caller to skip the ML overlay
    entirely — when the model is effectively untrained: no weights, or an
    all-zero vector (the cold-start state, where a sigmoid would return a
    meaningless 0.5). This is what keeps a fresh bot's behavior identical to
    the pre-ML blend until it has actually learned something.
    """
    if isinstance(weights, str):
        try:
            weights = json.loads(weights)
        except (ValueError, TypeError):
            return None
    if not weights:
        return None
    w = {k: float(weights.get(k, 0.0)) for k in FEATURE_KEYS}
    if not any(w[k] != 0.0 for k in FEATURE_KEYS):
        return None
    feats = _extract_features(components)
    z = sum(w[k] * feats[k] for k in FEATURE_KEYS)
    return _sigmoid(z)


def recalibrate_bot(conn: Any, bot_id: str) -> MLState:
    """Recompute rolling win-rate / profit-factor / size multiplier for ``bot_id``.

    Reads the last :data:`tw_config.CALIBRATION_LOOKBACK_DAYS` days of
    ``tw_trades``. Called by the calibration worker and, defensively, from
    :func:`src.tradeworkz.engine.on_trade_close` right after each close so
    the ML surface stays fresh even without the nightly job running.
    """
    state = load_state(conn, bot_id)
    lookback = tw_config.CALIBRATION_LOOKBACK_DAYS
    cur = conn.cursor()
    cur.execute(
        """
        SELECT outcome, realized_pnl, closed_at
        FROM tw_trades
        WHERE bot_id = %s AND closed_at >= NOW() - (%s || ' days')::interval
        ORDER BY closed_at DESC
        """,
        (bot_id, str(lookback)),
    )
    rows = cur.fetchall()
    if not rows:
        return state

    wins = [r for r in rows if r[0] == "win"]
    losses = [r for r in rows if r[0] == "loss"]
    total = len(rows)
    hit_rate = len(wins) / total if total else None

    profit_factor: Optional[float] = None
    gross_win = sum(float(r[1]) for r in wins if r[1] is not None)
    gross_loss = -sum(float(r[1]) for r in losses if r[1] is not None)
    if gross_loss > 0:
        profit_factor = gross_win / gross_loss

    now = datetime.now(timezone.utc)
    win_7d = _win_rate_since(rows, now - timedelta(days=7))
    win_30d = _win_rate_since(rows, now - timedelta(days=30))

    if hit_rate is not None and total >= tw_config.CALIBRATION_MIN_SAMPLES:
        state.confidence_base = max(
            tw_config.CALIBRATION_FLOOR,
            min(tw_config.CALIBRATION_CEIL, hit_rate),
        )

    if profit_factor is not None:
        if profit_factor < 1.0:
            state.size_multiplier = 0.5
        elif profit_factor < 1.4:
            state.size_multiplier = 1.0
        else:
            state.size_multiplier = 1.25

    if win_30d is not None:
        # Adaptive threshold: never gate above 0.75 or below 0.45.
        state.confidence_threshold = max(0.45, min(0.75, 1.0 - win_30d + 0.30))

    state.hit_rate = hit_rate
    state.last_win_rate_7d = win_7d
    state.last_win_rate_30d = win_30d
    state.last_profit_factor = profit_factor
    save_state(conn, state)
    return state


def _win_rate_since(rows: Iterable[tuple], cutoff: datetime) -> Optional[float]:
    hits = 0
    total = 0
    for outcome, _pnl, closed_at in rows:
        if closed_at is None:
            continue
        if closed_at.tzinfo is None:
            closed_at = closed_at.replace(tzinfo=timezone.utc)
        if closed_at < cutoff:
            continue
        total += 1
        if outcome == "win":
            hits += 1
    if total == 0:
        return None
    return hits / total
