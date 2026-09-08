"""Forward excursion at each touch, oriented to the reversion trade.

The bar arithmetic is not reimplemented here.
:func:`research.msi_regime_excursion.excursion.compute_excursion` already
measures max-up / max-down / return over a set of horizons plus rest-of-session,
strictly forward of the entry bar and with the entry bar itself excluded.  This
module supplies the one thing it cannot know: **which direction is favourable.**

For a touch of an extension ABOVE the range, the reversion trade is short — so
favourable excursion is downward and adverse excursion is upward.  Below the
range it is the mirror.  Getting this backwards would not crash anything; it
would silently report every MAE as an MFE, and the study would look like it had
found a large, stable, wrong effect.  Hence one function, one place, and a test
that pins both signs.

``compute_excursion``'s own ``mfe``/``mae`` are NOT reused: those are
conditioned on the prior-30-minute trend (the product's "prevailing bias"),
which is a different question and is carried alongside as a feature rather than
as the outcome.

Two measurement choices worth being explicit about:

* **Entry is the touch bar's CLOSE, never its extreme.**  Nothing fills at a
  wick, and scoring the excursion from the extreme would credit every event
  with the slippage a real trade pays.
* **A horizon cut short by the archive is dropped; one cut short by the bell is
  kept.**  That is ``require_full_window=True``, and the distinction matters:
  the first is a property of the extract, the second is a real property of a
  15:50 touch.
"""

from __future__ import annotations

from typing import Any, Optional

from research.msi_regime_excursion.excursion import (
    REST_OF_SESSION,
    BarSeries,
    ForwardExcursion,
    compute_excursion,
)
from research.or_gamma_confluence.config import ResearchConfig
from research.or_gamma_confluence.events import TouchEvent
from research.or_gamma_confluence.ranges import SIDE_UP

__all__ = ["EOD_KEY", "reversion_sign", "measure_outcome"]

#: Column suffix for the rest-of-session window — the brief's "EOD".
EOD_KEY = "eod"


def reversion_sign(side: str) -> int:
    """``-1`` above the range, ``+1`` below it.

    The sign of a price move that is FAVOURABLE to the reversion trade at a
    touch on ``side``.  Above the opening range the reversion is a short, so a
    downward move is favourable.
    """
    return -1 if side == SIDE_UP else 1


def _key(horizon: Any) -> str:
    return EOD_KEY if horizon == REST_OF_SESSION else str(horizon)


def measure_outcome(
    event: TouchEvent,
    series: BarSeries,
    cfg: ResearchConfig,
) -> dict[str, Any]:
    """Excursion columns for one touch, oriented to reversion.

    Returns a flat dict ready to merge into the event row.  Every value is in
    the instrument's own price units (``_pts``) and in basis points of the
    entry (``_bp``), because a points threshold is what a futures trader
    actually works with and a bp figure is what makes SPY and NDX comparable.
    """
    exc: Optional[ForwardExcursion] = compute_excursion(
        series,
        event.touched_at,
        horizons=tuple(cfg.outcome_horizons),
        include_rest_of_session=True,
        require_full_window=True,
    )
    out: dict[str, Any] = {"outcome_measured": exc is not None}
    if exc is None:
        return out

    sign = reversion_sign(event.side)
    out["entry_reference"] = exc.entry
    out["prevailing_bias"] = exc.bias

    for horizon in exc.horizons():
        k = _key(horizon)
        up_pts = exc.max_up_pts.get(horizon)
        down_pts = exc.max_down_pts.get(horizon)
        up_bp = exc.max_up_bps.get(horizon)
        down_bp = exc.max_down_bps.get(horizon)
        ret_bp = exc.ret_bps.get(horizon)

        # Favourable = toward the previous extension; adverse = toward the next.
        fav_pts, adv_pts = (down_pts, up_pts) if sign < 0 else (up_pts, down_pts)
        fav_bp, adv_bp = (down_bp, up_bp) if sign < 0 else (up_bp, down_bp)

        out[f"mfe_pts_{k}"] = fav_pts
        out[f"mae_pts_{k}"] = adv_pts
        out[f"mfe_bp_{k}"] = fav_bp
        out[f"mae_bp_{k}"] = adv_bp
        # Signed so positive always means "moved toward reversion".
        out[f"ret_reversion_bp_{k}"] = None if ret_bp is None else sign * ret_bp
        out[f"bars_in_window_{k}"] = exc.bars_in_window.get(horizon)

    return out
