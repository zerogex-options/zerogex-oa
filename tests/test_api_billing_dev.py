"""B2B Stripe metered billing: idempotency key, customer mirror upsert,
daily reporter post-or-record semantics, webhook event parsing.

The Stripe SDK is patched at the seam (``billing._stripe_module``) so the
suite runs offline. Everything that touches the DB uses the same fake
pool pattern as ``test_api_usage_metering``.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import pytest

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


def _reload_billing(
    monkeypatch: pytest.MonkeyPatch,
    *,
    enabled: bool = True,
    stripe_key: str = "sk_test_dummy",
    webhook_secret: str = "whsec_dummy",
):
    for var in [
        "API_BILLING_ENABLED",
        "STRIPE_API_KEY",
        "STRIPE_DEV_WEBHOOK_SECRET",
        "STRIPE_DEV_METER_EVENT_NAME",
    ]:
        monkeypatch.delenv(var, raising=False)
    if enabled:
        monkeypatch.setenv("API_BILLING_ENABLED", "1")
    if stripe_key:
        monkeypatch.setenv("STRIPE_API_KEY", stripe_key)
    if webhook_secret:
        monkeypatch.setenv("STRIPE_DEV_WEBHOOK_SECRET", webhook_secret)
    sys.modules.pop("src.api.billing", None)
    return importlib.import_module("src.api.billing")


class _FakeConn:
    def __init__(self) -> None:
        self.calls: List[Tuple[str, str, Tuple[Any, ...]]] = []
        self.fetchrow_handler = lambda sql, args: None
        self.fetch_handler = lambda sql, args: []
        self.execute_handler = lambda sql, args: "INSERT 0 1"

    async def fetchrow(self, sql: str, *args: Any) -> Optional[Dict[str, Any]]:
        self.calls.append(("fetchrow", sql, args))
        return self.fetchrow_handler(sql, args)

    async def fetch(self, sql: str, *args: Any) -> List[Dict[str, Any]]:
        self.calls.append(("fetch", sql, args))
        return self.fetch_handler(sql, args)

    async def execute(self, sql: str, *args: Any) -> str:
        self.calls.append(("execute", sql, args))
        return self.execute_handler(sql, args)


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


# --------------------------------------------------------------------------
# is_enabled / config
# --------------------------------------------------------------------------


def test_inert_without_stripe_key(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("API_BILLING_ENABLED", raising=False)
    monkeypatch.delenv("STRIPE_API_KEY", raising=False)
    sys.modules.pop("src.api.billing", None)
    b = importlib.import_module("src.api.billing")
    assert not b.is_enabled()


def test_inert_without_flag(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("API_BILLING_ENABLED", raising=False)
    monkeypatch.setenv("STRIPE_API_KEY", "sk_test_x")
    sys.modules.pop("src.api.billing", None)
    b = importlib.import_module("src.api.billing")
    assert not b.is_enabled()


def test_enabled_when_both_set(monkeypatch: pytest.MonkeyPatch):
    b = _reload_billing(monkeypatch)
    assert b.is_enabled()


# --------------------------------------------------------------------------
# Idempotency key — stable across retries, unique per (user, day, meter)
# --------------------------------------------------------------------------


def test_idempotency_key_is_stable_for_same_inputs(monkeypatch: pytest.MonkeyPatch):
    b = _reload_billing(monkeypatch)
    day = date(2026, 6, 1)
    a = b._idempotency_key("alice", day)
    b_ = b._idempotency_key("alice", day)
    assert a == b_


def test_idempotency_key_differs_per_day(monkeypatch: pytest.MonkeyPatch):
    b = _reload_billing(monkeypatch)
    assert b._idempotency_key("alice", date(2026, 6, 1)) != b._idempotency_key(
        "alice", date(2026, 6, 2)
    )


def test_idempotency_key_differs_per_user(monkeypatch: pytest.MonkeyPatch):
    b = _reload_billing(monkeypatch)
    day = date(2026, 6, 1)
    assert b._idempotency_key("alice", day) != b._idempotency_key("bob", day)


# --------------------------------------------------------------------------
# Customer mirror upsert
# --------------------------------------------------------------------------


def test_upsert_customer_runs_upsert_sql(monkeypatch: pytest.MonkeyPatch):
    b = _reload_billing(monkeypatch)
    conn = _FakeConn()
    pool = _FakePool(conn)
    asyncio.run(
        b.upsert_customer(
            pool,
            user_id="alice",
            stripe_customer_id="cus_1",
            stripe_subscription_id="sub_1",
            stripe_subscription_item_id="si_1",
            tier="analytics",
            status="active",
        )
    )
    assert conn.calls
    op, sql, args = conn.calls[-1]
    assert op == "execute"
    assert "api_billing_customers" in sql
    assert "ON CONFLICT (user_id) DO UPDATE" in sql
    assert args[:6] == ("alice", "cus_1", "sub_1", "si_1", "analytics", "active")


# --------------------------------------------------------------------------
# Reporter run_once: posts each pending row and writes the report row.
# --------------------------------------------------------------------------


def test_reporter_posts_one_meter_event_per_pending_day(monkeypatch: pytest.MonkeyPatch):
    b = _reload_billing(monkeypatch)
    conn = _FakeConn()
    pending_rows = [
        {"user_id": "alice", "day": date(2026, 6, 14), "request_count": 1234, "error_count": 0},
        {"user_id": "bob", "day": date(2026, 6, 14), "request_count": 7, "error_count": 1},
    ]
    customer_map = {"alice": "cus_alice", "bob": "cus_bob"}

    def _fetch(sql, args):
        # First fetch is the pending-days SELECT.
        return pending_rows

    def _fetchrow(sql, args):
        # Subsequent fetchrows hit _customer_id_for(user_id).
        user = args[0]
        return {"stripe_customer_id": customer_map.get(user)}

    conn.fetch_handler = _fetch
    conn.fetchrow_handler = _fetchrow

    pool = _FakePool(conn)
    b.reporter.configure(lambda: pool)

    # Stub the Stripe SDK seam — record each call instead of going to the wire.
    posted = []

    class _MeterEventStub:
        @staticmethod
        def create(**kwargs):
            posted.append(kwargs)
            return type("R", (), {"id": f"mev_{len(posted)}"})()

    class _BillingStub:
        MeterEvent = _MeterEventStub

    class _StripeStub:
        billing = _BillingStub

    monkeypatch.setattr(b, "_stripe_module", lambda: _StripeStub)

    result = asyncio.run(b.reporter.run_once())
    assert result["posted"] == 2
    assert result["failed"] == 0
    assert len(posted) == 2
    # Each event addressed the right Stripe customer.
    addressed = sorted(p["payload"]["stripe_customer_id"] for p in posted)
    assert addressed == ["cus_alice", "cus_bob"]
    # And the idempotency key tracks (customer, day).
    assert all(p["identifier"].startswith("zgx-mev-") for p in posted)


def test_reporter_records_failure_row_on_stripe_error(monkeypatch: pytest.MonkeyPatch):
    b = _reload_billing(monkeypatch)
    conn = _FakeConn()
    pending_rows = [
        {"user_id": "alice", "day": date(2026, 6, 14), "request_count": 1234, "error_count": 0},
    ]
    conn.fetch_handler = lambda sql, args: pending_rows
    conn.fetchrow_handler = lambda sql, args: {"stripe_customer_id": "cus_alice"}
    pool = _FakePool(conn)
    b.reporter.configure(lambda: pool)

    class _MeterEventStub:
        @staticmethod
        def create(**kwargs):
            raise RuntimeError("simulated stripe outage")

    class _BillingStub:
        MeterEvent = _MeterEventStub

    monkeypatch.setattr(b, "_stripe_module", lambda: type("S", (), {"billing": _BillingStub}))

    result = asyncio.run(b.reporter.run_once())
    assert result["posted"] == 0
    assert result["failed"] == 1
    # A status='failed' row was written so the next pass retries.
    inserts = [c for c in conn.calls if c[0] == "execute" and "api_billing_usage_reports" in c[1]]
    assert inserts and inserts[-1][2][-1] == "failed"


def test_reporter_returns_zero_when_pool_unavailable(monkeypatch: pytest.MonkeyPatch):
    b = _reload_billing(monkeypatch)
    b.reporter.configure(lambda: None)
    out = asyncio.run(b.reporter.run_once())
    assert out == {"posted": 0, "failed": 0, "skipped": 0}


# --------------------------------------------------------------------------
# Webhook parsing
# --------------------------------------------------------------------------


def test_parse_subscription_event_extracts_metadata_user_id(monkeypatch: pytest.MonkeyPatch):
    b = _reload_billing(monkeypatch)
    event = {
        "type": "customer.subscription.created",
        "data": {
            "object": {
                "id": "sub_1",
                "customer": "cus_1",
                "status": "active",
                "items": {"data": [{"id": "si_1"}]},
                "metadata": {"zgx_user_id": "alice", "zgx_tier": "analytics"},
            }
        },
    }
    parsed = b.parse_subscription_event(event)
    assert parsed is not None
    event_type, payload = parsed
    assert event_type == "customer.subscription.created"
    assert payload["user_id"] == "alice"
    assert payload["stripe_subscription_id"] == "sub_1"
    assert payload["stripe_subscription_item_id"] == "si_1"
    assert payload["tier"] == "analytics"
    assert payload["status"] == "active"


def test_parse_subscription_event_returns_none_without_zgx_user_id(
    monkeypatch: pytest.MonkeyPatch,
):
    b = _reload_billing(monkeypatch)
    event = {
        "type": "customer.subscription.updated",
        "data": {
            "object": {
                "id": "sub_1",
                "customer": "cus_1",
                "status": "active",
                "items": {"data": []},
                "metadata": {},
            }
        },
    }
    assert b.parse_subscription_event(event) is None


def test_parse_subscription_event_skips_unrelated_events(monkeypatch: pytest.MonkeyPatch):
    b = _reload_billing(monkeypatch)
    event = {
        "type": "invoice.paid",
        "data": {"object": {"id": "in_1"}},
    }
    assert b.parse_subscription_event(event) is None
