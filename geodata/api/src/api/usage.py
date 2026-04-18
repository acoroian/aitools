"""Usage metering — tracks API and tile requests per tenant.

Events are written to the usage_events table. Monthly counts are checked
against the tenant's plan limits before allowing requests.
"""

from __future__ import annotations

import logging
import time

from fastapi import Depends, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.auth import Tenant, get_current_tenant
from api.db import get_db

log = logging.getLogger(__name__)


def record_usage(
    db: Session,
    tenant_id: str,
    event_type: str,
    endpoint: str | None = None,
    layer_slug: str | None = None,
    response_ms: int | None = None,
) -> None:
    """Record a usage event."""
    db.execute(
        text("""
            INSERT INTO usage_events (tenant_id, event_type, endpoint, layer_slug, response_ms)
            VALUES (:tenant_id, :event_type, :endpoint, :layer_slug, :response_ms)
        """),
        {
            "tenant_id": tenant_id,
            "event_type": event_type,
            "endpoint": endpoint,
            "layer_slug": layer_slug,
            "response_ms": response_ms,
        },
    )
    db.commit()


def get_monthly_usage(db: Session, tenant_id: str, event_type: str) -> int:
    """Get the current month's usage count for a tenant and event type."""
    result = db.execute(
        text("""
            SELECT COUNT(*)
            FROM usage_events
            WHERE tenant_id = :tenant_id
              AND event_type = :event_type
              AND created_at >= date_trunc('month', CURRENT_TIMESTAMP)
        """),
        {"tenant_id": tenant_id, "event_type": event_type},
    ).scalar_one()
    return int(result)


def check_api_limit(
    request: Request,
    tenant: Tenant = Depends(get_current_tenant),
    db: Session = Depends(get_db),
) -> Tenant:
    """Dependency that checks API request limits and records usage."""
    if tenant.plan == "unlimited":
        return tenant

    current_usage = get_monthly_usage(db, tenant.id, "api_request")
    if current_usage >= tenant.monthly_api_limit:
        raise HTTPException(
            status_code=429,
            detail=f"Monthly API limit reached ({tenant.monthly_api_limit}). Upgrade your plan.",
        )

    start = time.monotonic()
    request.state.usage_start = start
    request.state.tenant = tenant
    return tenant


def check_tile_limit(
    tenant: Tenant = Depends(get_current_tenant),
    db: Session = Depends(get_db),
) -> Tenant:
    """Dependency that checks tile request limits."""
    if tenant.plan == "unlimited":
        return tenant

    current_usage = get_monthly_usage(db, tenant.id, "tile_request")
    if current_usage >= tenant.monthly_tile_limit:
        raise HTTPException(
            status_code=429,
            detail=f"Monthly tile limit reached ({tenant.monthly_tile_limit}). Upgrade your plan.",
        )

    return tenant
