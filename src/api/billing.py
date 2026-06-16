"""B2B Stripe metered billing — daily aggregator + idempotent meter events.

Two paths into this module:

1. **Lifecycle webhook** (``routers/billing.py``) — Stripe POSTs subscription
   events for the B2B API SKUs. We mirror the active subscription into
   ``api_billing_customers`` so the daily reporter knows which subscription
   item to attach each user's usage to.

2. **Daily reporter** (``BillingReporter`` here) — once per day, walks the
   previous day's ``api_usage_daily`` rows, groups by ``caller_user_id``,
   and posts one Stripe ``billing.meter_events.create`` per
   ``(user, day)``. The event payload carries an idempotency key derived
   from ``(user_id, day, meter_name)`` so a retry never double-bills, and
   the same key is persisted to ``api_billing_usage_reports`` so a crashed
   reporter resumes without re-posting what it already shipped.

The module is **inert by default**. Without ``STRIPE_API_KEY`` it never
imports the Stripe SDK (which is an optional dep); without
``API_BILLING_ENABLED=1`` the lifespan hook never schedules the reporter,
and the webhook handler returns 503 so a misconfigured Stripe install can't
mutate state.

The B2C billing layer lives in the Next.js frontend (SQLite-backed users
table). This module is *only* for the B2B API tier — separate Stripe Price
IDs, separate webhook secret, separate customer table — so a problem with
one billing stream cannot interfere with the other.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _env_flag(name: str, default: str = "0") -> bool:
    return (os.getenv(name, default) or default).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


# --- Config -----------------------------------------------------------------

# The Stripe Billing "Meter" event name (configured in the Stripe dashboard)
# whose payload column receives the per-day request totals.
_METER_EVENT_NAME = os.getenv("STRIPE_DEV_METER_EVENT_NAME", "api_requests")

# The Meter's customer-mapping key (set when the meter was created in the
# dashboard). Almost always ``stripe_customer_id`` — exposed so an operator
# who chose differently doesn't have to patch source.
_METER_CUSTOMER_KEY = os.getenv("STRIPE_DEV_METER_CUSTOMER_KEY", "stripe_customer_id")

# The Meter's value-payload key — same constraint as above.
_METER_VALUE_KEY = os.getenv("STRIPE_DEV_METER_VALUE_KEY", "value")

# Stripe API + flag plumbing. ``API_BILLING_ENABLED`` is the master switch;
# without it the reporter never runs and the webhook is shut.
_STRIPE_API_KEY: Optional[str] = (os.getenv("STRIPE_API_KEY") or "").strip() or None
_WEBHOOK_SECRET: Optional[str] = (os.getenv("STRIPE_DEV_WEBHOOK_SECRET") or "").strip() or None
_ENABLED = _env_flag("API_BILLING_ENABLED")

# Daily reporter cadence. The reporter runs continuously, sleeps until just
# after midnight UTC, then processes the day that just closed.
_REPORTER_CHECK_SECONDS = int(os.getenv("API_BILLING_REPORTER_CHECK_SECONDS", "3600") or "3600")
# Hard cap on rows shipped per pass so a backfill of weeks of skipped days
# doesn't issue tens of thousands of API calls in a tight loop.
_REPORTER_BATCH_LIMIT = int(os.getenv("API_BILLING_REPORTER_BATCH_LIMIT", "5000") or "5000")


def is_enabled() -> bool:
    return _ENABLED and _STRIPE_API_KEY is not None


def webhook_secret() -> Optional[str]:
    return _WEBHOOK_SECRET


# --- Stripe SDK adapter -----------------------------------------------------


class _StripeUnavailable(RuntimeError):
    """Raised when the Stripe SDK isn't installed but billing is configured."""


def _stripe_module() -> Any:
    """Import and configure the Stripe SDK lazily.

    Lazy so the module imports cleanly in environments without the SDK
    installed (tests, minimal deployments), and so the API key is applied
    once on first use rather than at import.
    """
    try:
        import stripe  # type: ignore[import-not-found]
    except Exception as e:
        raise _StripeUnavailable("stripe SDK not installed") from e
    if _STRIPE_API_KEY:
        stripe.api_key = _STRIPE_API_KEY
    return stripe


def _idempotency_key(user_id: str, day: date) -> str:
    """Stable per-(user, day, meter) key so retries don't double-bill.

    ``user_id`` may carry email-shaped characters or arbitrary bytes; we
    hash to give Stripe a fixed-format identifier.
    """
    raw = f"{_METER_EVENT_NAME}|{user_id}|{day.isoformat()}"
    return "zgx-mev-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _stripe_timestamp(day: date) -> int:
    """End-of-day UTC epoch — meter events are timestamped to the end of
    the billing window so a day's usage falls inside the right period."""
    return int(datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())


async def _post_meter_event(
    stripe_customer_id: str,
    quantity: int,
    day: date,
) -> str:
    """Post one ``billing.meter_events.create`` and return the event id.

    Synchronous Stripe SDK call wrapped in ``run_in_executor`` so it
    doesn't block the event loop. Idempotency key prevents Stripe from
    double-counting a retry of the same (user, day).
    """
    stripe = _stripe_module()
    loop = asyncio.get_running_loop()

    def _call() -> Any:
        return stripe.billing.MeterEvent.create(
            event_name=_METER_EVENT_NAME,
            timestamp=_stripe_timestamp(day),
            identifier=_idempotency_key(stripe_customer_id, day),
            payload={
                _METER_CUSTOMER_KEY: stripe_customer_id,
                _METER_VALUE_KEY: str(quantity),
            },
        )

    result = await loop.run_in_executor(None, _call)
    # The SDK returns a Stripe object with ``id`` (or ``identifier`` if the
    # response shape changes — handle both gracefully).
    return getattr(result, "id", None) or getattr(result, "identifier", "") or ""


# --- Persistence ------------------------------------------------------------


@dataclass(frozen=True)
class _UsageSlice:
    user_id: str
    day: date
    request_count: int
    error_count: int


_REPORT_INSERT = """
    INSERT INTO api_billing_usage_reports
        (day, user_id, request_count, error_count, posted_at, stripe_event_id, status)
    VALUES ($1, $2, $3, $4, NOW(), $5, $6)
    ON CONFLICT (day, user_id) DO UPDATE SET
        request_count   = EXCLUDED.request_count,
        error_count     = EXCLUDED.error_count,
        posted_at       = EXCLUDED.posted_at,
        stripe_event_id = EXCLUDED.stripe_event_id,
        status          = EXCLUDED.status
"""


_CUSTOMER_UPSERT = """
    INSERT INTO api_billing_customers
        (user_id, stripe_customer_id, stripe_subscription_id,
         stripe_subscription_item_id, tier, status, created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
    ON CONFLICT (user_id) DO UPDATE SET
        stripe_customer_id          = EXCLUDED.stripe_customer_id,
        stripe_subscription_id      = EXCLUDED.stripe_subscription_id,
        stripe_subscription_item_id = EXCLUDED.stripe_subscription_item_id,
        tier                        = EXCLUDED.tier,
        status                      = EXCLUDED.status,
        updated_at                  = NOW()
"""


_PENDING_DAYS_SQL = """
    SELECT u.caller_user_id AS user_id,
           u.day::date AS day,
           SUM(u.request_count)::BIGINT AS request_count,
           SUM(u.error_count)::BIGINT AS error_count
    FROM api_usage_daily u
    JOIN api_billing_customers c
      ON c.user_id = u.caller_user_id
     AND c.status IN ('active', 'trialing', 'past_due')
    LEFT JOIN api_billing_usage_reports r
      ON r.user_id = u.caller_user_id
     AND r.day = u.day::date
     AND r.status = 'posted'
    WHERE u.day::date < $1
      AND r.user_id IS NULL
      AND c.stripe_customer_id IS NOT NULL
    GROUP BY u.caller_user_id, u.day
    HAVING SUM(u.request_count) > 0
    ORDER BY u.day, u.caller_user_id
    LIMIT $2
"""


async def upsert_customer(
    pool: Any,
    *,
    user_id: str,
    stripe_customer_id: str,
    stripe_subscription_id: Optional[str],
    stripe_subscription_item_id: Optional[str],
    tier: Optional[str],
    status: str,
) -> None:
    """Mirror a Stripe subscription change into ``api_billing_customers``."""
    async with pool.acquire() as conn:
        await conn.execute(
            _CUSTOMER_UPSERT,
            user_id,
            stripe_customer_id,
            stripe_subscription_id,
            stripe_subscription_item_id,
            tier,
            status,
        )


async def _fetch_pending(pool: Any, today: date, limit: int) -> List[_UsageSlice]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(_PENDING_DAYS_SQL, today, limit)
    return [
        _UsageSlice(
            user_id=r["user_id"],
            day=r["day"],
            request_count=int(r["request_count"]),
            error_count=int(r["error_count"]),
        )
        for r in rows
    ]


async def _customer_id_for(pool: Any, user_id: str) -> Optional[str]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT stripe_customer_id FROM api_billing_customers WHERE user_id = $1",
            user_id,
        )
    return None if row is None else row["stripe_customer_id"]


async def _record_report(
    pool: Any,
    slice_: _UsageSlice,
    stripe_event_id: str,
    status: str,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            _REPORT_INSERT,
            slice_.day,
            slice_.user_id,
            slice_.request_count,
            slice_.error_count,
            stripe_event_id,
            status,
        )


# --- Reporter loop ----------------------------------------------------------


class BillingReporter:
    """Background loop that ships ``api_usage_daily`` to Stripe meters.

    The reporter ticks every ``API_BILLING_REPORTER_CHECK_SECONDS`` (default
    1h). On each tick:

    * computes "yesterday and earlier" in UTC;
    * fetches unreported ``(user_id, day)`` pairs from ``api_usage_daily``
      whose customer is active in ``api_billing_customers`` and whose
      ``(day, user_id)`` does not yet have a successfully-posted report;
    * for each, posts one Stripe meter event and writes a row to
      ``api_billing_usage_reports`` with the resulting event id;
    * on Stripe failure, writes a ``status='failed'`` row so the next pass
      sees the work as pending and retries.

    Cron-style alternatives were considered and rejected: an in-process
    loop deals with reconnection / DB-pool swaps cleanly through the same
    ``get_pool`` callable the rest of the API uses, and a 1h check is
    cheap (one indexed SELECT) when there's nothing to do.
    """

    def __init__(self) -> None:
        self._get_pool: Optional[Callable[[], Any]] = None
        self._task: Optional[asyncio.Task] = None

    def configure(self, get_pool: Optional[Callable[[], Any]]) -> None:
        self._get_pool = get_pool

    def start(self) -> None:
        if not is_enabled():
            return
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.warning("billing reporter exited with error", exc_info=True)

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(_REPORTER_CHECK_SECONDS)
            except asyncio.CancelledError:
                raise
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("billing reporter tick failed", exc_info=True)

    async def run_once(self) -> Dict[str, int]:
        """Process one batch of pending (user, day) slices.

        Returns a dict with the run's counts so the caller (tests, an admin
        CLI in the future) can observe what was shipped. The reporter task
        ignores the return — the persistence path is the audit trail.
        """
        pool = self._get_pool() if self._get_pool is not None else None
        if pool is None:
            return {"posted": 0, "failed": 0, "skipped": 0}

        today = datetime.now(timezone.utc).date()
        pending = await _fetch_pending(pool, today, _REPORTER_BATCH_LIMIT)
        posted = 0
        failed = 0
        skipped = 0

        for slice_ in pending:
            customer_id = await _customer_id_for(pool, slice_.user_id)
            if not customer_id:
                # Customer row was deleted between the SELECT and now.
                skipped += 1
                continue
            try:
                event_id = await _post_meter_event(
                    stripe_customer_id=customer_id,
                    quantity=slice_.request_count,
                    day=slice_.day,
                )
                await _record_report(pool, slice_, event_id, "posted")
                posted += 1
            except _StripeUnavailable:
                logger.error(
                    "billing.reporter: stripe SDK missing while "
                    "API_BILLING_ENABLED=1; aborting tick"
                )
                return {"posted": posted, "failed": failed, "skipped": skipped}
            except Exception:
                logger.warning(
                    "billing.reporter: stripe post failed for user=%s day=%s",
                    slice_.user_id,
                    slice_.day,
                    exc_info=True,
                )
                try:
                    await _record_report(pool, slice_, "", "failed")
                except Exception:
                    logger.warning("billing.reporter: also failed to record failure", exc_info=True)
                failed += 1

        if posted or failed:
            logger.info(
                "billing.reporter: posted=%d failed=%d skipped=%d",
                posted,
                failed,
                skipped,
            )
        return {"posted": posted, "failed": failed, "skipped": skipped}


#: Process-wide singleton wired in the FastAPI lifespan startup hook.
reporter = BillingReporter()


# --- Webhook helpers --------------------------------------------------------


def verify_webhook_signature(payload: bytes, signature_header: str) -> Any:
    """Construct a verified Stripe event from a raw request body.

    Returns the parsed Stripe Event object. Raises if the signature
    doesn't validate; the router maps the exception into a 400. Held in
    this module rather than in the router so all Stripe-SDK imports stay
    behind the optional-dep guard.
    """
    if _WEBHOOK_SECRET is None:
        raise RuntimeError("STRIPE_DEV_WEBHOOK_SECRET not configured")
    stripe = _stripe_module()
    return stripe.Webhook.construct_event(
        payload=payload,
        sig_header=signature_header,
        secret=_WEBHOOK_SECRET,
    )


def parse_subscription_event(event: Any) -> Optional[Tuple[str, Dict[str, Any]]]:
    """Pull the bits we care about out of a Stripe Event.

    Returns ``(event_type, dict)`` where ``dict`` has the keys
    ``upsert_customer`` will accept, or ``None`` for events we don't
    handle. Defensive against missing fields — Stripe's webhook shape
    has historically grown new optional fields without notice.
    """
    event_type = getattr(event, "type", None) or (
        event.get("type") if isinstance(event, dict) else None
    )
    if not event_type:
        return None
    data_object = (
        getattr(getattr(event, "data", None), "object", None)
        if not isinstance(event, dict)
        else (event.get("data") or {}).get("object")
    )
    if data_object is None:
        return None

    if event_type.startswith("customer.subscription."):
        sub = _as_dict(data_object)
        items = (sub.get("items") or {}).get("data") or []
        item = items[0] if items else {}
        # ``metadata.zgx_user_id`` is the developer's SaaS user id; the
        # checkout flow in the frontend sets it so we don't have to round-
        # trip the Stripe Customer here to find the linkage.
        metadata = sub.get("metadata") or {}
        zgx_user_id = metadata.get("zgx_user_id")
        if not zgx_user_id:
            return None
        return (
            event_type,
            {
                "user_id": zgx_user_id,
                "stripe_customer_id": sub.get("customer"),
                "stripe_subscription_id": sub.get("id"),
                "stripe_subscription_item_id": item.get("id"),
                "tier": (sub.get("metadata") or {}).get("zgx_tier"),
                "status": sub.get("status") or "unknown",
            },
        )
    return None


def _as_dict(stripe_obj: Any) -> Dict[str, Any]:
    """Best-effort coerce a Stripe object to a plain dict."""
    if isinstance(stripe_obj, dict):
        return stripe_obj
    try:
        # Stripe objects implement ``.to_dict_recursive()`` / JSON encode.
        if hasattr(stripe_obj, "to_dict_recursive"):
            return dict(stripe_obj.to_dict_recursive())
    except Exception:
        pass
    try:
        return json.loads(json.dumps(stripe_obj, default=str))
    except Exception:
        return {}
