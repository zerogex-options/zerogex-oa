"""Fit a bot's legs onto contracts the option chain actually lists.

Bots name their legs by arithmetic: an expiration ``dte_target`` weekdays out
and a strike rounded to the symbol's grid. That is exact for SPY, QQQ and SPX,
which list an expiration every weekday on a $1 / 5-point grid, and wrong for a
single stock: NVDA does not list an expiration every weekday, and its strikes
near the money sit $2.50 apart (the 2026-10-20 probe; see
``docs/runbooks/stock_onboarding.md``). A leg that names a contract nobody
lists never fills, so a stock could sit in the fleet universe and never trade.

:func:`fit_legs` maps each leg onto the chain as the ingest layer captured it:

* **Expiration** — the earliest listed expiration on or after the one the bot
  asked for, at most :data:`MAX_EXPIRATION_ROLL_DAYS` later. Never earlier:
  that would give the trade less time than the bot priced. A bot asking for
  today on a stock that lists Fridays trades Friday's contract, and the
  reconciler still caps its time stop at the close of the day it asked for.
* **Strike** — the nearest listed strike of that expiration, at most one
  listed step away. A strike off the edge of the ingested band is refused, not
  dragged inward, or a condor's wings would collapse onto its body.
* **Shape** — legs that shared a strike still share one; legs that did not
  still do not, in the same order. A fit that would merge two strikes (a $1
  vertical on a $2.50 grid) or two expirations is refused rather than turned
  into a different trade.

Legs that already name listed contracts come back unchanged, so SPY / QQQ /
SPX behave exactly as before. With no chain to read (the cache empty, a query
failure) the legs also pass through unchanged — today's behavior — and the
pricing step decides, as it always has.

:func:`listed_strike_increment` reads the same chain for the strike step near
the money, so a bot's widths (``spread_width`` × increment) are built on the
real grid in the first place and the fit rarely has to move anything.
"""

from __future__ import annotations

import bisect
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from src.tradeworkz.flow_context import _release, _rollback, _savepoint

logger = logging.getLogger(__name__)

#: How far past the requested expiration a leg may roll. Seven calendar days
#: covers a weekly-only stock asked for a Monday (Friday is four days out)
#: and a holiday-shifted week, without ever turning a short-dated thesis into
#: a different tenor.
MAX_EXPIRATION_ROLL_DAYS = 7

#: Band around spot read for the near-the-money strike step.
_INCREMENT_BAND_PCT = 0.02

#: Window a backtest reads for the chain "as of" an instant: the harness's own
#: quote tolerance, so a contract the fill would find is one the fit can see.
_AS_OF_TOLERANCE_MIN = 10

_STRIKE_EPS = 1e-6

#: ``expiration -> right ('C'|'P') -> strike -> option_symbol``
ListedChain = Dict[date, Dict[str, Dict[float, str]]]


@dataclass(frozen=True)
class FitResult:
    """Outcome of :func:`fit_legs`.

    ``legs`` is None when the fit refused; ``reason`` is then a short code for
    logs and the backtest's rejection tally. ``detail`` says what moved, for
    the position's audit payload.
    """

    legs: Optional[List[Dict[str, Any]]]
    reason: str
    detail: str = ""

    @property
    def changed(self) -> bool:
        return self.reason == "fitted"


def _option_root(underlying: str) -> str:
    """The option root the bots build symbols with (see base._synthetic_option_symbol)."""
    from src.symbols import resolve_option_root, resolve_symbol

    return resolve_option_root(resolve_symbol(underlying))


def _key(strike: Any) -> Optional[float]:
    try:
        return round(float(strike), 4)
    except (TypeError, ValueError):
        return None


def _parse_date(raw: Any) -> Optional[date]:
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    if isinstance(raw, str) and len(raw) >= 10:
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None
    return None


def _right(option_type: Any) -> Optional[str]:
    t = str(option_type or "").strip().upper()[:1]
    return t if t in ("C", "P") else None


def _chain_from_rows(rows: List[Any], root: str) -> ListedChain:
    chain: ListedChain = {}
    for row in rows:
        try:
            sym, exp, strike, otype = row[0], row[1], row[2], row[3]
        except (TypeError, IndexError):
            continue
        if not isinstance(sym, str) or sym.split(" ", 1)[0] != root:
            continue
        e, k, r = _parse_date(exp), _key(strike), _right(otype)
        if e is None or k is None or r is None:
            continue
        chain.setdefault(e, {}).setdefault(r, {})[k] = sym
    return chain


def fetch_listed_chain(
    conn: Any,
    underlying: str,
    *,
    from_date: date,
    as_of: Optional[datetime] = None,
    max_age_seconds: Optional[int] = None,
) -> ListedChain:
    """Every contract on ``underlying``'s chain expiring on/after ``from_date``.

    Live (``as_of`` None): ``option_chains_latest`` — one row per contract —
    limited to quotes younger than ``max_age_seconds`` (default: the engine's
    OPTION_QUOTE_MAX_AGE_SECONDS, the same age pricing accepts as a fill).

    Replay (``as_of`` set): contracts quoted in ``option_chains`` during the
    ten minutes up to ``as_of`` — never after it — falling back to
    ``option_chains_archive`` past the hot table's retention.

    Only the root the bots trade (``SPXW`` for SPX, not the AM-settled monthly
    ``SPX`` root). Each read runs under a savepoint so a failure leaves the
    caller's transaction usable; it returns an empty chain, which the fit
    treats as "nothing to fit against".
    """
    root = _option_root(underlying)
    if as_of is None:
        from src.tradeworkz import config as tw_config

        age = int(max_age_seconds or tw_config.OPTION_QUOTE_MAX_AGE_SECONDS)
        queries = [
            (
                "tw_chain_live",
                """
                SELECT option_symbol, expiration, strike, option_type
                FROM option_chains_latest
                WHERE underlying = %s AND expiration >= %s
                  AND timestamp >= NOW() - (%s * INTERVAL '1 second')
                """,
                (underlying, from_date, age),
            )
        ]
    else:
        lo = as_of - timedelta(minutes=_AS_OF_TOLERANCE_MIN)
        queries = [
            (
                f"tw_chain_{table}",
                f"""
                SELECT DISTINCT option_symbol, expiration, strike, option_type
                FROM {table}
                WHERE underlying = %s AND expiration >= %s
                  AND timestamp <= %s AND timestamp >= %s
                """,
                (underlying, from_date, as_of, lo),
            )
            for table in ("option_chains", "option_chains_archive")
        ]
    for name, sql, params in queries:
        cur = conn.cursor()
        armed = _savepoint(cur, name)
        try:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
            _release(cur, name, armed)
        except Exception:
            _rollback(cur, name, armed)
            logger.debug("fetch_listed_chain(%s) failed on %s", underlying, name, exc_info=True)
            rows = []
        chain = _chain_from_rows(rows, root)
        if chain:
            return chain
    return {}


def _nearest(listed: List[float], k: float) -> float:
    """Nearest value in sorted ``listed`` to ``k``; a tie goes to the lower,
    so two legs a whole step apart stay a whole step apart."""
    i = bisect.bisect_left(listed, k)
    if i <= 0:
        return listed[0]
    if i >= len(listed):
        return listed[-1]
    lo, hi = listed[i - 1], listed[i]
    return lo if (k - lo) <= (hi - k) + _STRIKE_EPS else hi


def _local_step(listed: List[float], s: float) -> Optional[float]:
    i = bisect.bisect_left(listed, s - _STRIKE_EPS)
    gaps = []
    if i > 0:
        gaps.append(s - listed[i - 1])
    if i + 1 < len(listed):
        gaps.append(listed[i + 1] - s)
    gaps = [g for g in gaps if g > _STRIKE_EPS]
    return min(gaps) if gaps else None


def fit_legs(
    legs: List[Dict[str, Any]],
    chain: ListedChain,
    *,
    max_roll_days: int = MAX_EXPIRATION_ROLL_DAYS,
) -> FitResult:
    """Map ``legs`` onto listed contracts. See the module docstring."""
    legs = [dict(leg) for leg in legs]
    if not chain:
        return FitResult(legs, "no_chain")
    listed_exps = sorted(chain)

    # 1. Each requested expiration -> the earliest listed one on/after it.
    exp_map: Dict[date, date] = {}
    for leg in legs:
        req = _parse_date(leg.get("expiration"))
        if req is None:
            return FitResult(None, "bad_expiration")
        if req in exp_map:
            continue
        target = next((e for e in listed_exps if e >= req), None)
        if target is None or (target - req).days > max_roll_days:
            return FitResult(
                None,
                "no_listed_expiration",
                f"nothing listed {req.isoformat()}..+{max_roll_days}d",
            )
        exp_map[req] = target
    if len(set(exp_map.values())) != len(exp_map):
        return FitResult(None, "expirations_merged")

    # 2. Each distinct requested strike (per expiration) -> nearest listed.
    strike_map: Dict[Tuple[date, float], float] = {}
    for req_exp, exp in exp_map.items():
        sides = chain.get(exp, {})
        listed = sorted(set(sides.get("C", {})) | set(sides.get("P", {})))
        if not listed:
            return FitResult(None, "no_listed_strikes")
        wanted = sorted(
            {
                k
                for leg in legs
                if _parse_date(leg.get("expiration")) == req_exp
                and (k := _key(leg.get("strike"))) is not None
            }
        )
        fitted: Dict[float, float] = {}
        for k in wanted:
            s = _nearest(listed, k)
            if abs(s - k) > _STRIKE_EPS:
                step = _local_step(listed, s)
                if step is None or abs(s - k) > step + _STRIKE_EPS:
                    return FitResult(None, "strike_off_band", f"{k:g} vs listed {s:g}")
            fitted[k] = s
        if len(set(fitted.values())) != len(fitted):
            return FitResult(None, "strikes_merged")
        for k, s in fitted.items():
            strike_map[(req_exp, k)] = s

    # 3. Rebuild every leg on its listed contract.
    out: List[Dict[str, Any]] = []
    moves: List[str] = []
    for leg in legs:
        req_exp = _parse_date(leg.get("expiration"))
        k = _key(leg.get("strike"))
        r = _right(leg.get("option_type"))
        if req_exp is None or k is None or r is None:
            return FitResult(None, "bad_leg")
        exp = exp_map[req_exp]
        s = strike_map[(req_exp, k)]
        sym = chain.get(exp, {}).get(r, {}).get(s)
        if sym is None:
            return FitResult(None, "not_listed_for_right", f"{s:g}{r} {exp.isoformat()}")
        if exp != req_exp:
            moves.append(f"expiration {req_exp.isoformat()}->{exp.isoformat()}")
        if abs(s - k) > _STRIKE_EPS:
            moves.append(f"strike {k:g}->{s:g}")
        new = dict(leg)
        new.update(expiration=exp.isoformat(), strike=s, option_symbol=sym)
        out.append(new)
    changed = bool(moves) or any(
        a.get("option_symbol") != b.get("option_symbol") for a, b in zip(legs, out)
    )
    detail = "; ".join(dict.fromkeys(moves))
    return FitResult(out, "fitted" if changed else "listed", detail)


def fit_legs_to_chain(
    conn: Any,
    underlying: str,
    legs: List[Dict[str, Any]],
    *,
    as_of: Optional[datetime] = None,
) -> FitResult:
    """Read the chain for ``underlying`` and fit ``legs`` onto it."""
    dates = [d for d in (_parse_date(leg.get("expiration")) for leg in legs) if d]
    if not dates:
        return FitResult([dict(leg) for leg in legs], "no_chain")
    try:
        chain = fetch_listed_chain(conn, underlying, from_date=min(dates), as_of=as_of)
    except Exception:  # no connection, or anything else: fit nothing, as before
        logger.debug("fit_legs_to_chain(%s): chain unavailable", underlying, exc_info=True)
        chain = {}
    return fit_legs(legs, chain)


def strike_step_from_strikes(strikes: List[Any]) -> Optional[float]:
    """The most common gap between adjacent distinct strikes (the smaller on
    a tie), or None with fewer than two strikes."""
    ks = sorted({k for k in (_key(s) for s in strikes) if k is not None})
    gaps = [round(b - a, 4) for a, b in zip(ks, ks[1:]) if b - a > _STRIKE_EPS]
    if not gaps:
        return None
    counts = Counter(gaps)
    best = max(counts.values())
    return min(g for g, n in counts.items() if n == best)


def listed_strike_increment(
    conn: Any,
    underlying: str,
    spot: float,
    *,
    as_of: Optional[datetime] = None,
    from_date: Optional[date] = None,
) -> Optional[float]:
    """The listed strike step near the money on the front expiration.

    Reads the same chain :func:`fetch_listed_chain` does, takes the front
    expiration on/after ``from_date`` (today by default), and returns the most
    common gap between adjacent strikes within ±2% of ``spot``. None when the
    chain is unavailable — the caller then falls back to the symbol-type
    default (cash index 5, else 1).
    """
    if not spot or spot <= 0:
        return None
    if from_date is None:
        from src.market_calendar import ET

        from_date = (as_of or datetime.now(ET)).astimezone(ET).date()
    lo, hi = spot * (1.0 - _INCREMENT_BAND_PCT), spot * (1.0 + _INCREMENT_BAND_PCT)
    if as_of is not None:
        chain = fetch_listed_chain(conn, underlying, from_date=from_date, as_of=as_of)
        if not chain:
            return None
        front = chain[min(chain)]
        return strike_step_from_strikes(
            [k for side in front.values() for k in side if lo <= k <= hi]
        )

    # Live: this runs on every snapshot, so read only the front expiration's
    # near-the-money strikes rather than the whole chain.
    from src.tradeworkz import config as tw_config

    root = _option_root(underlying)
    age = int(tw_config.OPTION_QUOTE_MAX_AGE_SECONDS)
    cur = conn.cursor()
    armed = _savepoint(cur, "tw_strike_step")
    try:
        cur.execute(
            """
            SELECT strike FROM option_chains_latest
            WHERE underlying = %s
              AND split_part(option_symbol, ' ', 1) = %s
              AND strike BETWEEN %s AND %s
              AND timestamp >= NOW() - (%s * INTERVAL '1 second')
              AND expiration = (
                  SELECT MIN(expiration) FROM option_chains_latest
                  WHERE underlying = %s
                    AND split_part(option_symbol, ' ', 1) = %s
                    AND expiration >= %s
                    AND timestamp >= NOW() - (%s * INTERVAL '1 second'))
            """,
            (underlying, root, lo, hi, age, underlying, root, from_date, age),
        )
        rows = cur.fetchall() or []
        _release(cur, "tw_strike_step", armed)
    except Exception:
        _rollback(cur, "tw_strike_step", armed)
        logger.debug("listed_strike_increment(%s) failed", underlying, exc_info=True)
        return None
    strikes = []
    for row in rows:
        try:
            strikes.append(row[0])
        except (TypeError, IndexError):
            continue
    return strike_step_from_strikes(strikes)
