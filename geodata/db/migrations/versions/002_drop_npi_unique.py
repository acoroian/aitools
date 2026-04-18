"""Drop unique constraints on cms_npi, oshpd_id, cdss_id.

Multiple CDPH facility records can share the same NPI (e.g. satellite
locations under one provider number). Only cdph_id is a reliable 1:1 key
from the CDPH source.

Revision ID: 002
Revises: 001
"""

from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("facilities_cms_npi_key", "facilities", type_="unique")
    op.drop_constraint("facilities_oshpd_id_key", "facilities", type_="unique")
    op.drop_constraint("facilities_cdss_id_key", "facilities", type_="unique")


def downgrade() -> None:
    op.create_unique_constraint("facilities_cms_npi_key", "facilities", ["cms_npi"])
    op.create_unique_constraint("facilities_oshpd_id_key", "facilities", ["oshpd_id"])
    op.create_unique_constraint("facilities_cdss_id_key", "facilities", ["cdss_id"])
