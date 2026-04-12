"""Add facility_violation_rollup table, citation unique constraint, severity helpers.

Revision ID: 004
Revises: 003
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # (a) Stable-key unique constraint on facility_violations
    op.create_unique_constraint(
        "uq_facility_violations_source_citation",
        "facility_violations",
        ["source", "citation_id"],
    )

    # (b) Supporting indexes
    op.create_index(
        "idx_facility_violations_facility_survey",
        "facility_violations",
        ["facility_id", sa.text("survey_date DESC")],
    )
    op.create_index(
        "idx_facility_violations_source_severity",
        "facility_violations",
        ["source", "severity"],
    )

    # (c) Rollup table
    op.create_table(
        "facility_violation_rollup",
        sa.Column(
            "facility_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("facilities.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("violation_count_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("violation_count_12mo", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cms_count_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cms_count_12mo", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cdph_count_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("cdph_count_12mo", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_severity_12mo", sa.Text(), nullable=True),
        sa.Column("max_severity_level_12mo", sa.SmallInteger(), nullable=True),
        sa.Column("has_ij_12mo", sa.Boolean(), nullable=False, server_default=sa.text("FALSE")),
        sa.Column("last_survey_date", sa.Date(), nullable=True),
        sa.Column(
            "last_refreshed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )

    op.execute(
        "CREATE INDEX idx_rollup_ij_12mo ON facility_violation_rollup(has_ij_12mo) "
        "WHERE has_ij_12mo = TRUE"
    )
    op.create_index(
        "idx_rollup_severity_level",
        "facility_violation_rollup",
        [sa.text("max_severity_level_12mo DESC")],
    )
    op.create_index(
        "idx_rollup_count_total",
        "facility_violation_rollup",
        [sa.text("violation_count_total DESC")],
    )
    op.create_index(
        "idx_rollup_last_survey",
        "facility_violation_rollup",
        [sa.text("last_survey_date DESC")],
    )

    # (d) SQL helper functions for cross-source severity comparison
    op.execute("""
        CREATE OR REPLACE FUNCTION severity_level_ord(src TEXT, sev TEXT)
        RETURNS SMALLINT AS $$
          SELECT CASE
            WHEN src = 'cms_nh_compare' THEN CASE sev
              WHEN 'A' THEN 1::smallint WHEN 'B' THEN 2::smallint WHEN 'C' THEN 3::smallint
              WHEN 'D' THEN 4::smallint WHEN 'E' THEN 5::smallint WHEN 'F' THEN 6::smallint
              WHEN 'G' THEN 6::smallint WHEN 'H' THEN 7::smallint WHEN 'I' THEN 7::smallint
              WHEN 'J' THEN 8::smallint WHEN 'K' THEN 9::smallint WHEN 'L' THEN 10::smallint
              ELSE NULL
            END
            WHEN src = 'cdph_sea' THEN CASE sev
              WHEN 'AA' THEN 10::smallint WHEN 'A' THEN 8::smallint WHEN 'B' THEN 4::smallint
              ELSE NULL
            END
            ELSE NULL
          END;
        $$ LANGUAGE SQL IMMUTABLE;
    """)

    op.execute("""
        CREATE OR REPLACE FUNCTION is_immediate_jeopardy_sql(src TEXT, sev TEXT)
        RETURNS BOOLEAN AS $$
          SELECT COALESCE(severity_level_ord(src, sev) >= 8, FALSE);
        $$ LANGUAGE SQL IMMUTABLE;
    """)


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS is_immediate_jeopardy_sql(TEXT, TEXT)")
    op.execute("DROP FUNCTION IF EXISTS severity_level_ord(TEXT, TEXT)")
    op.drop_index("idx_rollup_last_survey", table_name="facility_violation_rollup")
    op.drop_index("idx_rollup_count_total", table_name="facility_violation_rollup")
    op.drop_index("idx_rollup_severity_level", table_name="facility_violation_rollup")
    op.drop_index("idx_rollup_ij_12mo", table_name="facility_violation_rollup")
    op.drop_table("facility_violation_rollup")
    op.drop_index("idx_facility_violations_source_severity", table_name="facility_violations")
    op.drop_index("idx_facility_violations_facility_survey", table_name="facility_violations")
    op.drop_constraint(
        "uq_facility_violations_source_citation",
        "facility_violations",
        type_="unique",
    )
