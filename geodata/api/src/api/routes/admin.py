"""Admin dashboard API routes.

Provides tenant management, API key provisioning, usage stats,
and Stripe billing webhooks.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.auth import Tenant, generate_api_key, get_current_tenant
from api.db import get_db

log = logging.getLogger(__name__)
router = APIRouter()


# ── Schemas ──────────────────────────────────────────────────────────────────


class CreateTenantRequest(BaseModel):
    name: str
    email: str
    plan: str = "free"


class CreateApiKeyRequest(BaseModel):
    name: str


class UpdateTenantRequest(BaseModel):
    plan: str | None = None
    monthly_api_limit: int | None = None
    monthly_tile_limit: int | None = None
    is_active: bool | None = None


# ── Tenant management ───────────────────────────────────────────────────────


@router.post("/tenants")
def create_tenant(
    req: CreateTenantRequest,
    db: Session = Depends(get_db),
    _tenant: Tenant = Depends(get_current_tenant),
) -> dict:
    """Create a new tenant account."""
    row = db.execute(
        text("""
            INSERT INTO tenants (name, email, plan)
            VALUES (:name, :email, :plan)
            RETURNING id::text, name, email, plan, created_at::text
        """),
        {"name": req.name, "email": req.email, "plan": req.plan},
    ).fetchone()
    db.commit()

    if not row:
        raise HTTPException(status_code=500, detail="Failed to create tenant")

    return {
        "id": row[0],
        "name": row[1],
        "email": row[2],
        "plan": row[3],
        "created_at": row[4],
    }


@router.get("/tenants")
def list_tenants(
    db: Session = Depends(get_db),
    _tenant: Tenant = Depends(get_current_tenant),
) -> list[dict]:
    """List all tenants."""
    rows = db.execute(
        text("""
            SELECT t.id::text, t.name, t.email, t.plan, t.is_active,
                   t.monthly_api_limit, t.monthly_tile_limit,
                   t.stripe_customer_id, t.created_at::text,
                   (SELECT COUNT(*) FROM api_keys WHERE tenant_id = t.id) AS key_count,
                   (SELECT COUNT(*) FROM usage_events
                    WHERE tenant_id = t.id
                      AND created_at >= date_trunc('month', CURRENT_TIMESTAMP)) AS usage_this_month
            FROM tenants t
            ORDER BY t.created_at DESC
        """)
    ).fetchall()

    return [
        {
            "id": r[0],
            "name": r[1],
            "email": r[2],
            "plan": r[3],
            "is_active": r[4],
            "monthly_api_limit": r[5],
            "monthly_tile_limit": r[6],
            "stripe_customer_id": r[7],
            "created_at": r[8],
            "key_count": r[9],
            "usage_this_month": r[10],
        }
        for r in rows
    ]


@router.patch("/tenants/{tenant_id}")
def update_tenant(
    tenant_id: str,
    req: UpdateTenantRequest,
    db: Session = Depends(get_db),
    _tenant: Tenant = Depends(get_current_tenant),
) -> dict:
    """Update tenant plan, limits, or active status."""
    updates = []
    params: dict[str, object] = {"id": tenant_id}

    if req.plan is not None:
        updates.append("plan = :plan")
        params["plan"] = req.plan
    if req.monthly_api_limit is not None:
        updates.append("monthly_api_limit = :api_limit")
        params["api_limit"] = req.monthly_api_limit
    if req.monthly_tile_limit is not None:
        updates.append("monthly_tile_limit = :tile_limit")
        params["tile_limit"] = req.monthly_tile_limit
    if req.is_active is not None:
        updates.append("is_active = :is_active")
        params["is_active"] = req.is_active

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    updates.append("updated_at = NOW()")
    set_clause = ", ".join(updates)

    row = db.execute(
        text(f"UPDATE tenants SET {set_clause} WHERE id = :id RETURNING id::text"),  # noqa: S608
        params,
    ).fetchone()
    db.commit()

    if not row:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return {"id": row[0], "updated": True}


# ── API Key management ───────────────────────────────────────────────────────


@router.post("/tenants/{tenant_id}/keys")
def create_api_key(
    tenant_id: str,
    req: CreateApiKeyRequest,
    db: Session = Depends(get_db),
    _tenant: Tenant = Depends(get_current_tenant),
) -> dict:
    """Create a new API key for a tenant. Returns the raw key once — store it."""
    raw_key, prefix, key_hash = generate_api_key()

    row = db.execute(
        text("""
            INSERT INTO api_keys (tenant_id, key_hash, key_prefix, name)
            VALUES (:tenant_id, :key_hash, :prefix, :name)
            RETURNING id::text, created_at::text
        """),
        {
            "tenant_id": tenant_id,
            "key_hash": key_hash,
            "prefix": prefix,
            "name": req.name,
        },
    ).fetchone()
    db.commit()

    if not row:
        raise HTTPException(status_code=500, detail="Failed to create API key")

    return {
        "id": row[0],
        "key": raw_key,  # Only shown once
        "prefix": prefix,
        "name": req.name,
        "created_at": row[1],
    }


@router.get("/tenants/{tenant_id}/keys")
def list_api_keys(
    tenant_id: str,
    db: Session = Depends(get_db),
    _tenant: Tenant = Depends(get_current_tenant),
) -> list[dict]:
    """List API keys for a tenant (without the actual key)."""
    rows = db.execute(
        text("""
            SELECT id::text, key_prefix, name, is_active,
                   last_used_at::text, expires_at::text, created_at::text
            FROM api_keys
            WHERE tenant_id = :tenant_id
            ORDER BY created_at DESC
        """),
        {"tenant_id": tenant_id},
    ).fetchall()

    return [
        {
            "id": r[0],
            "prefix": r[1],
            "name": r[2],
            "is_active": r[3],
            "last_used_at": r[4],
            "expires_at": r[5],
            "created_at": r[6],
        }
        for r in rows
    ]


@router.delete("/tenants/{tenant_id}/keys/{key_id}")
def revoke_api_key(
    tenant_id: str,
    key_id: str,
    db: Session = Depends(get_db),
    _tenant: Tenant = Depends(get_current_tenant),
) -> dict:
    """Deactivate an API key."""
    result = db.execute(
        text("""
            UPDATE api_keys SET is_active = false
            WHERE id = :key_id AND tenant_id = :tenant_id
        """),
        {"key_id": key_id, "tenant_id": tenant_id},
    )
    db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="API key not found")
    return {"revoked": True}


# ── Usage stats ──────────────────────────────────────────────────────────────


@router.get("/tenants/{tenant_id}/usage")
def get_usage(
    tenant_id: str,
    db: Session = Depends(get_db),
    _tenant: Tenant = Depends(get_current_tenant),
) -> dict:
    """Get usage stats for a tenant (current month)."""
    row = db.execute(
        text("""
            SELECT
                COUNT(*) FILTER (WHERE event_type = 'api_request') AS api_requests,
                COUNT(*) FILTER (WHERE event_type = 'tile_request') AS tile_requests,
                AVG(response_ms) FILTER (WHERE response_ms IS NOT NULL) AS avg_response_ms
            FROM usage_events
            WHERE tenant_id = :tenant_id
              AND created_at >= date_trunc('month', CURRENT_TIMESTAMP)
        """),
        {"tenant_id": tenant_id},
    ).fetchone()

    # Daily breakdown for the current month
    daily = db.execute(
        text("""
            SELECT
                created_at::date::text AS day,
                COUNT(*) FILTER (WHERE event_type = 'api_request') AS api_requests,
                COUNT(*) FILTER (WHERE event_type = 'tile_request') AS tile_requests
            FROM usage_events
            WHERE tenant_id = :tenant_id
              AND created_at >= date_trunc('month', CURRENT_TIMESTAMP)
            GROUP BY created_at::date
            ORDER BY created_at::date
        """),
        {"tenant_id": tenant_id},
    ).fetchall()

    return {
        "current_month": {
            "api_requests": row[0] if row else 0,
            "tile_requests": row[1] if row else 0,
            "avg_response_ms": round(float(row[2]), 1) if row and row[2] else None,
        },
        "daily": [{"day": d[0], "api_requests": d[1], "tile_requests": d[2]} for d in daily],
    }
