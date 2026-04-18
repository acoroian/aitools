"""Stripe billing integration.

Handles webhook events from Stripe to sync subscription state.
Provides endpoints for creating checkout sessions and managing subscriptions.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.auth import Tenant, get_current_tenant
from api.config import settings
from api.db import get_db

log = logging.getLogger(__name__)
router = APIRouter()

# Plan → limits mapping
PLAN_LIMITS: dict[str, dict[str, int]] = {
    "free": {"monthly_api_limit": 1000, "monthly_tile_limit": 10000},
    "starter": {"monthly_api_limit": 10000, "monthly_tile_limit": 100000},
    "pro": {"monthly_api_limit": 100000, "monthly_tile_limit": 1000000},
    "enterprise": {"monthly_api_limit": 999999, "monthly_tile_limit": 9999999},
}


class CreateCheckoutRequest(BaseModel):
    plan: str
    success_url: str
    cancel_url: str


@router.post("/checkout")
def create_checkout(
    req: CreateCheckoutRequest,
    tenant: Tenant = Depends(get_current_tenant),
    db: Session = Depends(get_db),
) -> dict:
    """Create a Stripe Checkout session for the tenant to subscribe."""
    try:
        import stripe
    except ImportError:
        raise HTTPException(status_code=501, detail="Stripe not configured")

    if not settings.stripe_secret_key:
        raise HTTPException(status_code=501, detail="Stripe not configured")

    stripe.api_key = settings.stripe_secret_key

    price_id = settings.stripe_price_ids.get(req.plan)
    if not price_id:
        raise HTTPException(status_code=400, detail=f"Unknown plan: {req.plan}")

    # Create or reuse Stripe customer
    customer_id = None
    row = db.execute(
        text("SELECT stripe_customer_id FROM tenants WHERE id = :id"),
        {"id": tenant.id},
    ).fetchone()

    if row and row[0]:
        customer_id = row[0]
    else:
        customer = stripe.Customer.create(
            email=tenant.email,
            name=tenant.name,
            metadata={"tenant_id": tenant.id},
        )
        customer_id = customer.id
        db.execute(
            text("UPDATE tenants SET stripe_customer_id = :cid WHERE id = :id"),
            {"cid": customer_id, "id": tenant.id},
        )
        db.commit()

    session = stripe.checkout.Session.create(
        customer=customer_id,
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=req.success_url,
        cancel_url=req.cancel_url,
        metadata={"tenant_id": tenant.id},
    )

    return {"checkout_url": session.url}


@router.post("/webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)) -> dict:
    """Handle Stripe webhook events to sync subscription state."""
    try:
        import stripe
    except ImportError:
        raise HTTPException(status_code=501, detail="Stripe not configured")

    if not settings.stripe_secret_key or not settings.stripe_webhook_secret:
        raise HTTPException(status_code=501, detail="Stripe not configured")

    stripe.api_key = settings.stripe_secret_key

    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig, settings.stripe_webhook_secret)
    except (stripe.error.SignatureVerificationError, ValueError):
        raise HTTPException(status_code=400, detail="Invalid webhook signature")

    event_type = event["type"]
    data = event["data"]["object"]

    if event_type == "checkout.session.completed":
        _handle_checkout_completed(db, data)
    elif event_type == "customer.subscription.updated":
        _handle_subscription_updated(db, data)
    elif event_type == "customer.subscription.deleted":
        _handle_subscription_deleted(db, data)
    else:
        log.info("Unhandled Stripe event: %s", event_type)

    return {"received": True}


def _handle_checkout_completed(db: Session, session: dict) -> None:
    tenant_id = session.get("metadata", {}).get("tenant_id")
    subscription_id = session.get("subscription")
    if not tenant_id or not subscription_id:
        return

    db.execute(
        text("""
            UPDATE tenants
            SET stripe_subscription_id = :sub_id, updated_at = NOW()
            WHERE id = :tenant_id
        """),
        {"sub_id": subscription_id, "tenant_id": tenant_id},
    )
    db.commit()
    log.info("Checkout completed for tenant %s, subscription %s", tenant_id, subscription_id)


def _handle_subscription_updated(db: Session, subscription: dict) -> None:
    sub_id = subscription.get("id")
    status = subscription.get("status")

    # Look up tenant by subscription ID
    row = db.execute(
        text("SELECT id::text FROM tenants WHERE stripe_subscription_id = :sub_id"),
        {"sub_id": sub_id},
    ).fetchone()
    if not row:
        return

    tenant_id = row[0]
    is_active = status in ("active", "trialing")

    # Determine plan from price
    items = subscription.get("items", {}).get("data", [])
    plan = "free"
    if items:
        price_id = items[0].get("price", {}).get("id", "")
        for plan_name, pid in settings.stripe_price_ids.items():
            if pid == price_id:
                plan = plan_name
                break

    limits = PLAN_LIMITS.get(plan, PLAN_LIMITS["free"])

    db.execute(
        text("""
            UPDATE tenants
            SET plan = :plan, is_active = :active,
                monthly_api_limit = :api_limit, monthly_tile_limit = :tile_limit,
                updated_at = NOW()
            WHERE id = :tenant_id
        """),
        {
            "plan": plan,
            "active": is_active,
            "api_limit": limits["monthly_api_limit"],
            "tile_limit": limits["monthly_tile_limit"],
            "tenant_id": tenant_id,
        },
    )
    db.commit()
    log.info("Subscription updated for tenant %s: plan=%s active=%s", tenant_id, plan, is_active)


def _handle_subscription_deleted(db: Session, subscription: dict) -> None:
    sub_id = subscription.get("id")
    row = db.execute(
        text("SELECT id::text FROM tenants WHERE stripe_subscription_id = :sub_id"),
        {"sub_id": sub_id},
    ).fetchone()
    if not row:
        return

    tenant_id = row[0]
    limits = PLAN_LIMITS["free"]

    db.execute(
        text("""
            UPDATE tenants
            SET plan = 'free', stripe_subscription_id = NULL,
                monthly_api_limit = :api_limit, monthly_tile_limit = :tile_limit,
                updated_at = NOW()
            WHERE id = :tenant_id
        """),
        {
            "api_limit": limits["monthly_api_limit"],
            "tile_limit": limits["monthly_tile_limit"],
            "tenant_id": tenant_id,
        },
    )
    db.commit()
    log.info("Subscription cancelled for tenant %s, downgraded to free", tenant_id)
