"""Add ccn (CMS Certification Number) column to facilities.

CCN is needed to join HCRIS cost report data which keys on this 6-digit
Medicare provider number, distinct from NPI.

Revision ID: 003
Revises: 002
"""

from alembic import op
import sqlalchemy as sa

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("facilities", sa.Column("ccn", sa.Text(), nullable=True))
    op.create_index("idx_facilities_ccn", "facilities", ["ccn"])


def downgrade() -> None:
    op.drop_index("idx_facilities_ccn", table_name="facilities")
    op.drop_column("facilities", "ccn")
