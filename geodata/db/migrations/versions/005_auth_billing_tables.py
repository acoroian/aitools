"""Add tenants, api_keys, and usage_events tables for multi-tenant auth and billing.

Revision ID: 005
Revises: 004
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Tenants — each tenant is a billable account
    op.create_table(
        "tenants",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("email", sa.Text, nullable=False, unique=True),
        sa.Column("plan", sa.Text, nullable=False, server_default="free"),
        sa.Column("stripe_customer_id", sa.Text, unique=True),
        sa.Column("stripe_subscription_id", sa.Text, unique=True),
        sa.Column("monthly_api_limit", sa.Integer, server_default="1000"),
        sa.Column("monthly_tile_limit", sa.Integer, server_default="10000"),
        sa.Column("allowed_layers", sa.ARRAY(sa.Text)),
        sa.Column("is_active", sa.Boolean, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )

    # API keys — each tenant can have multiple keys
    op.create_table(
        "api_keys",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("key_hash", sa.Text, nullable=False, unique=True),
        sa.Column("key_prefix", sa.String(12), nullable=False),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("scopes", sa.ARRAY(sa.Text), server_default="{}"),
        sa.Column("is_active", sa.Boolean, server_default="true"),
        sa.Column("last_used_at", sa.DateTime(timezone=True)),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )
    op.create_index("idx_api_keys_tenant", "api_keys", ["tenant_id"])
    op.create_index("idx_api_keys_prefix", "api_keys", ["key_prefix"])

    # Usage events — append-only log of API and tile requests
    op.create_table(
        "usage_events",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("tenant_id", UUID(as_uuid=True), sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("event_type", sa.Text, nullable=False),
        sa.Column("endpoint", sa.Text),
        sa.Column("layer_slug", sa.Text),
        sa.Column("response_ms", sa.Integer),
        sa.Column("metadata", JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("NOW()")),
    )
    op.create_index("idx_usage_tenant_created", "usage_events", ["tenant_id", "created_at"])
    op.create_index("idx_usage_type", "usage_events", ["event_type"])


def downgrade() -> None:
    op.drop_table("usage_events")
    op.drop_table("api_keys")
    op.drop_table("tenants")
