"""B2B Stripe webhook receiver.

Mounted at ``/api/billing/dev-webhook``. Stripe POSTs subscription
lifecycle events for the B2B API SKUs here. We verify the signature with
``STRIPE_DEV_WEBHOOK_SECRET`` and mirror the subscription state into
``api_billing_customers`` so the daily reporter knows which Stripe
subscription item to attach each developer's usage to.

This is intentionally *separate* from the B2C webhook handler in the
Next.js frontend: a different signing secret, a different table, a
different code path. A bug or outage in one billing stream cannot
disturb the other.

Webhook auth is the signature header, not the API-key dependency — so
this endpoint sits outside the global ``Depends(api_key_auth)`` by way of
``_PUBLIC_PATHS`` in ``security.py``. The endpoint returns ``200`` for
every event we recognise (so Stripe stops retrying) and for every event
we do *not* recognise (so we don't end up DLQ-ing the entire webhook
because Stripe added an event type we don't care about).
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Header, HTTPException, Request, status

from .. import billing

logger = logging.getLogger(__name__)

# Tag explicitly outside the customer-facing API surface so the OpenAPI
# spec keeps the developer-facing categories clean.
router = APIRouter(prefix="/api/billing", tags=["Billing (internal)"])


@router.post("/dev-webhook", include_in_schema=False)
async def dev_webhook(
    request: Request, stripe_signature: str = Header(None, alias="Stripe-Signature")
):
    """Receive B2B Stripe events. 200 = acknowledged."""
    if not billing.is_enabled():
        # API_BILLING_ENABLED not set or no STRIPE_API_KEY — refuse to do
        # anything stateful with a webhook payload we can't act on.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="B2B billing is not enabled on this deployment.",
        )
    if billing.webhook_secret() is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="STRIPE_DEV_WEBHOOK_SECRET not configured.",
        )
    if not stripe_signature:
        # Real Stripe webhooks always carry this header; if it's missing
        # we're almost certainly being probed and a 400 is more honest
        # than pretending to verify.
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing Stripe-Signature header",
        )

    body = await request.body()
    try:
        event = billing.verify_webhook_signature(body, stripe_signature)
    except Exception:
        logger.warning("dev-webhook signature verification failed", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid Stripe signature",
        )

    parsed = billing.parse_subscription_event(event)
    if parsed is None:
        # Subscription event types we don't handle, plus everything else
        # (invoices, charges, …). Acknowledge so Stripe doesn't retry; the
        # B2C handler in the frontend deals with invoice/payment events.
        return {"received": True, "handled": False}

    event_type, payload = parsed

    # Resolve the DB pool here rather than in billing.py so the cross-
    # module wiring stays explicit and the billing module has no FastAPI
    # imports of its own.
    from .. import main as api_main

    pool = api_main._db().pool

    if event_type == "customer.subscription.deleted":
        payload["status"] = "canceled"

    try:
        await billing.upsert_customer(
            pool,
            user_id=payload["user_id"],
            stripe_customer_id=payload["stripe_customer_id"],
            stripe_subscription_id=payload["stripe_subscription_id"],
            stripe_subscription_item_id=payload["stripe_subscription_item_id"],
            tier=payload.get("tier"),
            status=payload["status"],
        )
    except Exception:
        logger.warning(
            "dev-webhook upsert failed for event=%s user=%s",
            event_type,
            payload.get("user_id"),
            exc_info=True,
        )
        # 500 so Stripe retries — the customer mirror must converge for
        # the daily reporter to bill against the right subscription item.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal error processing webhook",
        )

    logger.info(
        "dev-webhook handled event=%s user=%s status=%s",
        event_type,
        payload["user_id"],
        payload["status"],
    )
    return {"received": True, "handled": True}
