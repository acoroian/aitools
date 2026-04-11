"""Initial schema: facilities, financials, violations, layers

Revision ID: 001
Revises:
Create Date: 2026-04-10
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Enable PostGIS extension
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')

    # ── facilities ────────────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE facilities (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name            TEXT NOT NULL,
            type            TEXT NOT NULL,
            subtype         TEXT,
            address         TEXT,
            city            TEXT,
            county          TEXT,
            state           TEXT DEFAULT 'CA',
            zip             TEXT,
            phone           TEXT,
            lat             DOUBLE PRECISION,
            lon             DOUBLE PRECISION,
            geom            GEOMETRY(Point, 4326),
            cdph_id         TEXT UNIQUE,
            cms_npi         TEXT UNIQUE,
            oshpd_id        TEXT UNIQUE,
            cdss_id         TEXT UNIQUE,
            license_status  TEXT,
            license_number  TEXT,
            license_expiry  DATE,
            certified_medicare  BOOLEAN DEFAULT FALSE,
            certified_medicaid  BOOLEAN DEFAULT FALSE,
            primary_source  TEXT NOT NULL,
            geocode_source  TEXT,
            geocode_confidence DOUBLE PRECISION,
            last_verified   DATE,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # Auto-populate geom from lat/lon on insert or update
    op.execute("""
        CREATE OR REPLACE FUNCTION sync_geom_from_latlon()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.lat IS NOT NULL AND NEW.lon IS NOT NULL THEN
                NEW.geom := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
            END IF;
            NEW.updated_at := NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)

    op.execute("""
        CREATE TRIGGER trg_sync_geom
        BEFORE INSERT OR UPDATE OF lat, lon
        ON facilities
        FOR EACH ROW EXECUTE FUNCTION sync_geom_from_latlon()
    """)

    op.execute("CREATE INDEX idx_facilities_geom   ON facilities USING GIST(geom)")
    op.execute("CREATE INDEX idx_facilities_type   ON facilities(type)")
    op.execute("CREATE INDEX idx_facilities_county ON facilities(county)")
    op.execute("CREATE INDEX idx_facilities_zip    ON facilities(zip)")
    op.execute("CREATE INDEX idx_facilities_status ON facilities(license_status)")

    # ── facility_financials ───────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE facility_financials (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            facility_id         UUID NOT NULL REFERENCES facilities(id) ON DELETE CASCADE,
            year                SMALLINT NOT NULL,
            source              TEXT NOT NULL,
            gross_revenue       BIGINT,
            net_revenue         BIGINT,
            total_expenses      BIGINT,
            medicare_revenue    BIGINT,
            medicaid_revenue    BIGINT,
            private_revenue     BIGINT,
            total_visits        INTEGER,
            total_patients      INTEGER,
            raw_report_id       TEXT,
            created_at          TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(facility_id, year, source)
        )
    """)
    op.execute("CREATE INDEX idx_financials_facility ON facility_financials(facility_id)")
    op.execute("CREATE INDEX idx_financials_year     ON facility_financials(year)")

    # ── facility_violations ───────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE facility_violations (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            facility_id         UUID NOT NULL REFERENCES facilities(id) ON DELETE CASCADE,
            source              TEXT NOT NULL,
            survey_date         DATE,
            deficiency_tag      TEXT,
            category            TEXT,
            severity            TEXT,
            scope               TEXT,
            description         TEXT,
            corrective_action   TEXT,
            citation_id         TEXT,
            resolved            BOOLEAN DEFAULT FALSE,
            resolved_date       DATE,
            created_at          TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    op.execute("CREATE INDEX idx_violations_facility ON facility_violations(facility_id)")
    op.execute("CREATE INDEX idx_violations_date     ON facility_violations(survey_date)")
    op.execute("CREATE INDEX idx_violations_severity ON facility_violations(severity)")

    # ── layers ────────────────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE layers (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            slug                TEXT UNIQUE NOT NULL,
            name                TEXT NOT NULL,
            description         TEXT,
            facility_types      TEXT[],
            pmtiles_path        TEXT,
            min_zoom            SMALLINT DEFAULT 4,
            max_zoom            SMALLINT DEFAULT 14,
            last_generated      TIMESTAMPTZ,
            record_count        INTEGER,
            bbox                JSONB,
            attribute_schema    JSONB,
            access_policy       TEXT DEFAULT 'public',
            created_at          TIMESTAMPTZ DEFAULT NOW(),
            updated_at          TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # Seed default layers
    op.execute("""
        INSERT INTO layers (slug, name, description, facility_types, access_policy) VALUES
        ('home-health', 'Home Health Agencies', 'CA licensed home health agencies', ARRAY['home_health'], 'public'),
        ('hospice', 'Hospice Facilities', 'CA licensed hospice providers', ARRAY['hospice'], 'public'),
        ('daycare', 'Daycare / Child Care', 'CA licensed childcare and daycare facilities', ARRAY['daycare', 'child_day_care'], 'public'),
        ('skilled-nursing', 'Skilled Nursing Facilities', 'CA skilled nursing facilities', ARRAY['snf', 'skilled_nursing'], 'public'),
        ('all-care', 'All Care Facilities', 'All care facility types combined', NULL, 'public')
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS layers CASCADE")
    op.execute("DROP TABLE IF EXISTS facility_violations CASCADE")
    op.execute("DROP TABLE IF EXISTS facility_financials CASCADE")
    op.execute("DROP TABLE IF EXISTS facilities CASCADE")
    op.execute("DROP FUNCTION IF EXISTS sync_geom_from_latlon CASCADE")
    op.execute('DROP EXTENSION IF EXISTS "uuid-ossp"')
    op.execute("DROP EXTENSION IF EXISTS postgis")
