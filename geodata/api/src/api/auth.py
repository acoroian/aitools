"""API key authentication and tenant resolution.

API keys are passed via the X-API-Key header. The key is hashed and looked
up in the api_keys table. The associated tenant is loaded and attached
to the request state.

For development, if no API key is provided and JWT_SECRET is the default
dev value, requests are allowed without auth.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
from datetime import UTC, datetime

from fastapi import Depends, HTTPException, Request
from sqlalchemy import text
from sqlalchemy.orm import Session

from api.config import settings
from api.db import get_db

log = logging.getLogger(__name__)


def hash_api_key(key: str) -> str:
    """SHA-256 hash of a raw API key."""
    return hashlib.sha256(key.encode()).hexdigest()


def generate_api_key() -> tuple[str, str, str]:
    """Generate a new API key. Returns (raw_key, key_prefix, key_hash)."""
    raw = f"gd_{secrets.token_urlsafe(32)}"
    prefix = raw[:12]
    key_hash = hash_api_key(raw)
    return raw, prefix, key_hash


class Tenant:
    """Lightweight tenant object attached to request state."""

    def __init__(
        self,
        id: str,
        name: str,
        email: str,
        plan: str,
        monthly_api_limit: int,
        monthly_tile_limit: int,
        allowed_layers: list[str] | None,
        is_active: bool,
    ):
        self.id = id
        self.name = name
        self.email = email
        self.plan = plan
        self.monthly_api_limit = monthly_api_limit
        self.monthly_tile_limit = monthly_tile_limit
        self.allowed_layers = allowed_layers
        self.is_active = is_active


_DEV_TENANT = Tenant(
    id="00000000-0000-0000-0000-000000000000",
    name="Development",
    email="dev@localhost",
    plan="unlimited",
    monthly_api_limit=999999,
    monthly_tile_limit=999999,
    allowed_layers=None,
    is_active=True,
)


def _is_dev_mode() -> bool:
    return settings.jwt_secret == "dev-secret-change-in-prod"


def get_current_tenant(
    request: Request,
    db: Session = Depends(get_db),
) -> Tenant:
    """Resolve the tenant from the API key in the request header."""
    api_key = request.headers.get("X-API-Key")

    if not api_key:
        if _is_dev_mode():
            return _DEV_TENANT
        raise HTTPException(status_code=401, detail="Missing X-API-Key header")

    key_hash = hash_api_key(api_key)
    row = db.execute(
        text("""
            SELECT ak.id, ak.tenant_id, ak.is_active, ak.expires_at,
                   t.id::text, t.name, t.email, t.plan,
                   t.monthly_api_limit, t.monthly_tile_limit,
                   t.allowed_layers, t.is_active
            FROM api_keys ak
            JOIN tenants t ON t.id = ak.tenant_id
            WHERE ak.key_hash = :key_hash
        """),
        {"key_hash": key_hash},
    ).fetchone()

    if not row:
        raise HTTPException(status_code=401, detail="Invalid API key")

    ak_is_active = row[2]
    ak_expires = row[3]
    if not ak_is_active:
        raise HTTPException(status_code=401, detail="API key is deactivated")
    if ak_expires and ak_expires < datetime.now(UTC):
        raise HTTPException(status_code=401, detail="API key has expired")

    tenant_active = row[11]
    if not tenant_active:
        raise HTTPException(status_code=403, detail="Tenant account is disabled")

    # Update last_used_at
    db.execute(
        text("UPDATE api_keys SET last_used_at = NOW() WHERE id = :id"),
        {"id": str(row[0])},
    )
    db.commit()

    return Tenant(
        id=str(row[4]),
        name=row[5],
        email=row[6],
        plan=row[7],
        monthly_api_limit=row[8],
        monthly_tile_limit=row[9],
        allowed_layers=row[10],
        is_active=row[11],
    )


def require_scope(scope: str):
    """Dependency that checks the tenant has a specific scope (for future use)."""

    def checker(tenant: Tenant = Depends(get_current_tenant)) -> Tenant:
        # For now, all authenticated tenants have all scopes
        return tenant

    return checker
