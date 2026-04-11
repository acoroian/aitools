---
title: Care Facility Intelligence Platform — Phase 1 Foundation
type: feat
status: active
date: 2026-04-10
origin: docs/brainstorms/2026-04-10-vector-tile-platform-brainstorm.md
---

# Care Facility Intelligence Platform — Phase 1 Foundation

## Overview

Build the foundational infrastructure for a geospatial intelligence platform that maps daycares, home health agencies, hospices, and similar care businesses across California — enriched with financial data (revenue) and inspection/violation records. Phase 1 establishes the monorepo scaffold, data pipeline for the first source (CA CDPH Healthcare Facility Locations), PostGIS schema, vector tile generation, and Cloudflare R2 serving.

The platform powers Remix (web) and Expo (mobile) apps where users filter and explore care facilities by type, location, revenue, violations, and more.

**See brainstorm:** `docs/brainstorms/2026-04-10-vector-tile-platform-brainstorm.md`

---

## Problem Statement

No single interface maps all California care facilities with their financial performance and regulatory history in one place. The data exists across CDPH, CDSS, CMS, and HCAI — but it's fragmented, download-only, and untied to geography. This platform aggregates, geocodes, enriches, and serves it as interactive filterable maps.

---

## Proposed Solution

A Python ingest pipeline downloads public datasets, normalizes them, stores canonical facility records in PostGIS, generates PMTiles via Tippecanoe, uploads to Cloudflare R2, and serves via Cloudflare Workers with JWT auth. A FastAPI service answers attribute/spatial filter queries. Remix and Expo apps render MapLibre maps on top.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  PUBLIC DATA SOURCES                │
│  CDPH CSV · CDSS CSV · CMS POS · HCRIS · CASPER    │
└────────────────────┬────────────────────────────────┘
                     │ download + normalize
┌────────────────────▼────────────────────────────────┐
│              INGEST PIPELINE (Python/Celery)        │
│  GDAL/geopandas → normalize → geocode → entity-     │
│  resolve (crosswalk) → upsert PostGIS               │
└──────────┬──────────────────────┬───────────────────┘
           │                      │
┌──────────▼──────────┐  ┌────────▼───────────────────┐
│  PostGIS            │  │  Tile Generation            │
│  facilities         │  │  (Tippecanoe → PMTiles)     │
│  facility_financials│  │  triggered after ingest     │
│  facility_violations│  └────────┬───────────────────┘
│  layers             │           │
└──────────┬──────────┘  ┌────────▼───────────────────┐
           │              │  Cloudflare R2              │
┌──────────▼──────────┐  │  pmtiles/{layer}/{z}/{x}/{y}│
│  FastAPI Filter API │  └────────┬───────────────────┘
│  POST /filter       │           │
│  spatial + attrib   │  ┌────────▼───────────────────┐
└──────────┬──────────┘  │  Cloudflare Workers         │
           │              │  JWT auth + tile routing    │
           └──────┬───────┘
                  │
┌─────────────────▼───────────────────────────────────┐
│         CLIENTS                                     │
│  Remix (web) · Expo (mobile)                        │
│  MapLibre GL JS · @maplibre/maplibre-react-native   │
│  Layer toggles · Attribute filters · Draw polygon   │
└─────────────────────────────────────────────────────┘
```

---

## Monorepo Structure

```
geodata/
├── pipeline/                    # Python ingest pipeline + Celery
│   ├── pyproject.toml
│   ├── uv.lock
│   └── src/pipeline/
│       ├── config.py            # env/settings via pydantic-settings
│       ├── db.py                # SQLAlchemy + GeoAlchemy2 session
│       ├── models.py            # ORM models (Facility, Financials, Violations)
│       ├── celery_app.py        # Celery app + beat schedule
│       ├── ingest/
│       │   ├── cdph.py          # CA CDPH Healthcare Facility Locations
│       │   ├── cms_pos.py       # CMS Provider of Services
│       │   ├── hcris.py         # CMS HCRIS cost reports
│       │   ├── care_compare.py  # CMS Care Compare deficiency data
│       │   └── cdss.py          # CA CDSS Community Care Licensing
│       ├── geocoding/
│       │   └── geocoder.py      # Census Geocoder batch + Geocodio fallback
│       ├── crosswalk/
│       │   └── resolver.py      # Entity resolution: CDPH ↔ CMS ↔ OSHPD IDs
│       └── tiles/
│           ├── generate.py      # Tippecanoe subprocess wrapper
│           └── r2_upload.py     # Cloudflare R2 upload via boto3 S3-compat API
├── api/                         # FastAPI filter + metadata API
│   ├── pyproject.toml
│   └── src/api/
│       ├── main.py
│       ├── routes/
│       │   ├── facilities.py    # GET /facilities, POST /facilities/filter
│       │   ├── layers.py        # GET /layers (catalog)
│       │   └── health.py
│       └── schemas.py           # Pydantic request/response models
├── workers/                     # Cloudflare Workers
│   ├── tiles/
│   │   ├── src/index.ts         # Tile proxy: JWT validation + R2 range requests
│   │   └── wrangler.toml
│   └── package.json
├── apps/
│   ├── web/                     # Remix app
│   │   ├── app/
│   │   │   ├── routes/
│   │   │   │   └── _index.tsx   # Main map view
│   │   │   ├── components/
│   │   │   │   ├── Map.tsx      # MapLibre GL JS wrapper
│   │   │   │   ├── LayerPanel.tsx
│   │   │   │   └── FilterPanel.tsx
│   │   │   └── lib/
│   │   │       └── maplibre.ts
│   │   └── package.json
│   └── mobile/                  # Expo app
│       ├── app/
│       │   └── (tabs)/
│       │       └── map.tsx
│       ├── components/
│       │   └── MapView.tsx      # @maplibre/maplibre-react-native wrapper
│       └── package.json
├── db/
│   └── migrations/              # Alembic migrations
│       └── versions/
├── docker-compose.yml           # PostGIS + Redis + Celery worker + API
├── CLAUDE.md                    # Project conventions
└── README.md
```

---

## Database Schema

```sql
-- Core facility record (canonical, deduplicated across sources)
CREATE TABLE facilities (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    type        TEXT NOT NULL,           -- 'home_health', 'hospice', 'daycare', 'snf', etc.
    subtype     TEXT,                    -- e.g. 'adult_day_program', 'pediatric_hospice'
    address     TEXT,
    city        TEXT,
    county      TEXT,
    state       TEXT DEFAULT 'CA',
    zip         TEXT,
    phone       TEXT,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION,
    geom        GEOMETRY(Point, 4326),   -- postgis point, auto-set from lat/lon
    -- Source IDs for joining
    cdph_id     TEXT UNIQUE,
    cms_npi     TEXT UNIQUE,
    oshpd_id    TEXT UNIQUE,
    cdss_id     TEXT UNIQUE,
    -- Status
    license_status   TEXT,              -- 'active', 'expired', 'revoked', 'pending'
    license_number   TEXT,
    license_expiry   DATE,
    certified_medicare  BOOLEAN DEFAULT FALSE,
    certified_medicaid  BOOLEAN DEFAULT FALSE,
    -- Metadata
    primary_source  TEXT NOT NULL,      -- which source record was canonical
    last_verified   DATE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_facilities_geom   ON facilities USING GIST(geom);
CREATE INDEX idx_facilities_type   ON facilities(type);
CREATE INDEX idx_facilities_county ON facilities(county);
CREATE INDEX idx_facilities_zip    ON facilities(zip);
CREATE INDEX idx_facilities_npi    ON facilities(cms_npi);
CREATE INDEX idx_facilities_status ON facilities(license_status);

-- Annual financial snapshots (one row per facility per year per source)
CREATE TABLE facility_financials (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    facility_id         UUID NOT NULL REFERENCES facilities(id) ON DELETE CASCADE,
    year                SMALLINT NOT NULL,
    source              TEXT NOT NULL,      -- 'hcris', 'hcai'
    gross_revenue       BIGINT,             -- total gross patient revenue (cents)
    net_revenue         BIGINT,
    total_expenses      BIGINT,
    medicare_revenue    BIGINT,
    medicaid_revenue    BIGINT,
    private_revenue     BIGINT,
    total_visits        INTEGER,            -- patient visits / admissions
    total_patients      INTEGER,
    raw_report_id       TEXT,               -- HCRIS report ID or HCAI filing ID
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(facility_id, year, source)
);

CREATE INDEX idx_financials_facility ON facility_financials(facility_id);
CREATE INDEX idx_financials_year     ON facility_financials(year);

-- Violation / inspection events
CREATE TABLE facility_violations (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    facility_id         UUID NOT NULL REFERENCES facilities(id) ON DELETE CASCADE,
    source              TEXT NOT NULL,      -- 'cms_care_compare', 'cdph_lc', 'cdss_ccl'
    survey_date         DATE,
    deficiency_tag      TEXT,               -- e.g. 'F686' (CMS tag)
    category            TEXT,              -- 'infection_control', 'resident_rights', etc.
    severity            TEXT,              -- 'A'-'L' (CMS scale) or 'minor'/'moderate'/'serious'
    scope               TEXT,              -- 'isolated', 'pattern', 'widespread'
    description         TEXT,
    corrective_action   TEXT,
    citation_id         TEXT,
    resolved            BOOLEAN DEFAULT FALSE,
    resolved_date       DATE,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_violations_facility ON facility_violations(facility_id);
CREATE INDEX idx_violations_date     ON facility_violations(survey_date);
CREATE INDEX idx_violations_severity ON facility_violations(severity);
CREATE INDEX idx_violations_source   ON facility_violations(source);

-- Layer catalog (what tiles are available and their metadata)
CREATE TABLE layers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug            TEXT UNIQUE NOT NULL,   -- 'facilities-home-health', 'facilities-hospice'
    name            TEXT NOT NULL,
    description     TEXT,
    facility_types  TEXT[],                 -- which facility types are in this layer
    pmtiles_key     TEXT,                   -- R2 object key
    min_zoom        SMALLINT DEFAULT 4,
    max_zoom        SMALLINT DEFAULT 14,
    last_generated  TIMESTAMPTZ,
    record_count    INTEGER,
    bbox            JSONB,                  -- [west, south, east, north]
    attribute_schema JSONB,                 -- filterable attribute definitions
    access_policy   TEXT DEFAULT 'public',  -- 'public', 'authenticated', 'role:<name>'
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
```

---

## Data Sources & Ingest Details

### Phase 1 Sources

#### 1. CA CDPH Healthcare Facility Locations
- **URL:** `https://data.chhs.ca.gov/dataset/healthcare-facility-locations`
- **Download:** Direct CSV (monthly, ~7.6MB uncompressed)
- **Key fields:** FACID (CDPH ID), FACNAME, FACTYPE, ADDRESS, CITY, COUNTY, ZIP, LATITUDE, LONGITUDE, LICENSE_STATUS, LICENSE_EXPIRY
- **Facility types covered:** Home health agencies, hospices, SNFs, clinics, hospitals, 30+ types
- **Geo:** Lat/lon already present — no geocoding needed for this source
- **Ingest task:** `ingest_cdph` — runs monthly, upserts on FACID

#### 2. CA Licensed Facility Crosswalk
- **URL:** `https://data.chhs.ca.gov/dataset/licensed-and-certified-healthcare-facility-crosswalk`
- **Download:** XLSX — ELMS-ASPEN-OSHPD Crosswalk
- **Purpose:** Maps CDPH FACID → CMS NPI → OSHPD ID — critical for joining financial and violation data
- **Ingest task:** `ingest_crosswalk` — runs after CDPH ingest, updates ID fields on facility records

### Phase 2 Sources (financial enrichment)

#### 3. CMS HCRIS — Home Health Agency Cost Reports
- **URL:** `https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports/Home-Health-Agency`
- **Download:** ZIP with CSV/DAT files per fiscal year
- **Key fields:** Provider NPI, Fiscal Year End, Gross Patient Revenue, Net Revenue, Total Expenses, Medicare Revenue, Total Visits
- **Join key:** CMS NPI → `facilities.cms_npi`
- **Ingest task:** `ingest_hcris_hha` — annual

#### 4. CMS HCRIS — Hospice Cost Reports
- **URL:** `https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports/Hospice`
- **Same structure as HHA, different worksheet mappings**
- **Ingest task:** `ingest_hcris_hospice` — annual

#### 5. CA HCAI Annual Financial Disclosure
- **URL:** `https://hcai.ca.gov/data-and-reports/research-data/annual-financial-data/`
- **Download:** XLSX by facility type and year
- **Key fields:** OSHPD ID, Facility Name, Gross Patient Revenue, Payer Mix, Net Income
- **Join key:** OSHPD ID → `facilities.oshpd_id`
- **Ingest task:** `ingest_hcai` — annual

### Phase 3 Sources (violations)

#### 6. CMS Care Compare — Home Health Deficiencies
- **URL:** `https://data.cms.gov/provider-data/dataset/bnkm-str9` (Home Health Compare)
- **Download:** CSV
- **Key fields:** CMS Certification Number (CCN), Survey Date, Deficiency Tag, Severity, Scope, Description
- **Join key:** CCN (maps to NPI via crosswalk)
- **Ingest task:** `ingest_care_compare_hha`

#### 7. CMS Care Compare — Hospice Deficiencies
- **URL:** `https://data.cms.gov/provider-data/dataset/` (Hospice Compare)
- **Same structure as home health**
- **Ingest task:** `ingest_care_compare_hospice`

#### 8. CA CDPH L&C Inspection Data
- **URL:** `https://www.cdph.ca.gov/Programs/CHCQ/LCP/Pages/DatabaseDownloads.aspx`
- **Download:** CSV of citation/violation records
- **Join key:** CDPH FACID → `facilities.cdph_id`
- **Ingest task:** `ingest_cdph_violations`

### Phase 4 Sources (daycares)

#### 9. CA CDSS Community Care Licensing
- **URL:** `https://www.ccld.dss.ca.gov/carefacilitysearch/` (bulk download available)
- **Key fields:** Facility Name, Type (Child Day Care, etc.), Address, City, Zip, License Number, Status
- **Geo:** Addresses only — requires geocoding
- **Entity resolution:** No CMS NPI; match to CDPH records via name+address fuzzy match
- **Ingest task:** `ingest_cdss`

#### 10. CA CDSS CCL Inspection Reports
- **URL:** Bundled with CDSS facility data
- **Join key:** CDSS License Number → `facilities.cdss_id`
- **Ingest task:** `ingest_cdss_violations`

---

## Geocoding Strategy

```
For each facility record without lat/lon:
  1. Build address string: "{address}, {city}, {state} {zip}"
  2. Batch submit to Census Geocoder API (free, 10k/batch)
     POST https://geocoding.geo.census.gov/geocoder/locations/addressbatch
  3. For failed/unmatched addresses (~10-20%):
     Submit to Geocodio API ($0.50/1k addresses)
  4. Store result: lat, lon, geom, geocode_source, geocode_confidence
  5. Flag low-confidence results for manual review
```

**Census Geocoder batch format:**
```csv
Unique ID,Street address,City,State,ZIP
1,123 Main St,Los Angeles,CA,90001
```

---

## Tile Generation

```bash
# Export facilities from PostGIS to GeoJSON
psql $DATABASE_URL -c "\COPY (
  SELECT
    f.id, f.name, f.type, f.subtype, f.city, f.county, f.zip,
    f.license_status, f.certified_medicare, f.certified_medicaid,
    COALESCE(fin.gross_revenue, 0) AS gross_revenue_latest,
    COALESCE(viol.violation_count, 0) AS violation_count,
    ST_AsGeoJSON(f.geom)::json AS geometry
  FROM facilities f
  LEFT JOIN (
    SELECT DISTINCT ON (facility_id) facility_id, gross_revenue
    FROM facility_financials ORDER BY facility_id, year DESC
  ) fin ON fin.facility_id = f.id
  LEFT JOIN (
    SELECT facility_id, COUNT(*) AS violation_count
    FROM facility_violations GROUP BY facility_id
  ) viol ON viol.facility_id = f.id
  WHERE f.geom IS NOT NULL
) TO STDOUT" > /tmp/facilities.geojson

# Generate PMTiles
tippecanoe \
  --output=/tmp/facilities.pmtiles \
  --force \
  --minimum-zoom=4 \
  --maximum-zoom=14 \
  --drop-densest-as-needed \
  --extend-zooms-if-still-dropping \
  --layer=facilities \
  --attribute-type=gross_revenue_latest:int \
  --attribute-type=violation_count:int \
  /tmp/facilities.geojson

# Upload to R2
aws s3 cp /tmp/facilities.pmtiles \
  s3://geodata-tiles/layers/facilities/facilities.pmtiles \
  --endpoint-url https://<account_id>.r2.cloudflarestorage.com \
  --content-type application/octet-stream
```

**Tippecanoe flags explained:**
- `-Z4 -z14` — zoom 4 (state view) to zoom 14 (street level)
- `--drop-densest-as-needed` — auto-thin points at low zoom to prevent tile bloat
- `--extend-zooms-if-still-dropping` — adds zoom levels if data still crowded
- `--attribute-type` — ensures numeric fields stay numeric in MVT (not strings)

---

## Cloudflare Worker — Tile Auth

```typescript
// workers/tiles/src/index.ts
import { PMTiles, FetchSource } from 'pmtiles';

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Validate JWT from Authorization header
    const token = request.headers.get('Authorization')?.replace('Bearer ', '');
    if (!token) return new Response('Unauthorized', { status: 401 });

    const payload = await verifyJWT(token, env.JWT_SECRET);
    if (!payload) return new Response('Forbidden', { status: 403 });

    // Parse: /tiles/{layer}/{z}/{x}/{y}.mvt
    const match = url.pathname.match(/^\/tiles\/([^/]+)\/(\d+)\/(\d+)\/(\d+)\.mvt$/);
    if (!match) return new Response('Not Found', { status: 404 });

    const [, layer, z, x, y] = match;

    // Check layer access policy (fetch from KV or inline config)
    const policy = await env.LAYER_POLICIES.get(layer);
    if (!canAccess(payload, policy)) {
      return new Response('Forbidden', { status: 403 });
    }

    // Serve tile range request from R2
    const pmtilesKey = `layers/${layer}/${layer}.pmtiles`;
    const source = new R2Source(env.TILES_BUCKET, pmtilesKey);
    const tiles = new PMTiles(source);

    const tileData = await tiles.getZxy(parseInt(z), parseInt(x), parseInt(y));
    if (!tileData) return new Response('Not Found', { status: 404 });

    return new Response(tileData.data, {
      headers: {
        'Content-Type': 'application/x-protobuf',
        'Content-Encoding': 'gzip',
        'Cache-Control': 'public, max-age=86400',
        'Access-Control-Allow-Origin': '*',
      },
    });
  },
};
```

---

## FastAPI Filter API

```python
# POST /facilities/filter
# Request:
{
  "facility_types": ["home_health", "hospice"],
  "filters": {
    "license_status": "active",
    "gross_revenue_min": 1000000,
    "violation_count_max": 5,
    "year": 2023
  },
  "spatial": {
    "type": "polygon",
    "coordinates": [[[...]]],    # GeoJSON polygon drawn by user
  },
  "limit": 500
}

# Response:
{
  "type": "FeatureCollection",
  "features": [...],   # GeoJSON features with all attributes
  "total": 1234
}
```

The filter endpoint builds a dynamic PostGIS query:
```sql
SELECT f.id, f.name, f.type, f.lat, f.lon, ... 
FROM facilities f
LEFT JOIN facility_financials fin ON fin.facility_id = f.id AND fin.year = :year
LEFT JOIN (
  SELECT facility_id, COUNT(*) AS violation_count FROM facility_violations GROUP BY 1
) v ON v.facility_id = f.id
WHERE f.type = ANY(:facility_types)
  AND f.license_status = :license_status
  AND fin.gross_revenue >= :gross_revenue_min
  AND v.violation_count <= :violation_count_max
  AND ST_Intersects(f.geom, ST_GeomFromGeoJSON(:polygon))  -- spatial filter
LIMIT :limit
```

The MapLibre client receives the GeoJSON and applies `map.setFilter()` to highlight matching features within already-loaded tiles.

---

## Local Dev Runtime

**Container runtime: [Colima](https://github.com/abiosoft/colima)** (not Docker Desktop — free, lightweight, no license required on Mac).

```bash
# One-time setup
brew install colima docker docker-compose
colima start --cpu 4 --memory 8 --disk 60

# Daily use — Colima must be running before docker commands
colima start
docker compose up
```

## docker-compose.yml (local dev)

```yaml
version: '3.9'
services:
  db:
    image: postgis/postgis:16-3.4
    environment:
      POSTGRES_DB: geodata
      POSTGRES_USER: geodata
      POSTGRES_PASSWORD: geodata
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  api:
    build: ./api
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql://geodata:geodata@db:5432/geodata
      REDIS_URL: redis://redis:6379/0
    depends_on: [db, redis]

  worker:
    build: ./pipeline
    command: celery -A pipeline.celery_app worker --loglevel=info
    environment:
      DATABASE_URL: postgresql://geodata:geodata@db:5432/geodata
      REDIS_URL: redis://redis:6379/0
      CENSUS_GEOCODER_URL: https://geocoding.geo.census.gov/geocoder
    depends_on: [db, redis]
    volumes:
      - /tmp/geodata:/tmp/geodata    # tile generation scratch space

  beat:
    build: ./pipeline
    command: celery -A pipeline.celery_app beat --loglevel=info
    environment:
      DATABASE_URL: postgresql://geodata:geodata@db:5432/geodata
      REDIS_URL: redis://redis:6379/0
    depends_on: [db, redis]

volumes:
  pgdata:
```

---

## Implementation Phases

### Phase 1 — Foundation & First Layer (this plan)

#### 1.1 Repo Scaffold
- [ ] Replace `README.md` content (currently reads `# aitools`)
- [ ] Create `CLAUDE.md` with stack decisions and conventions
- [ ] Create `pipeline/pyproject.toml` with `uv` (deps: sqlalchemy, geoalchemy2, alembic, celery, redis, pandas, geopandas, httpx, pydantic-settings, boto3)
- [ ] Create `api/pyproject.toml` (deps: fastapi, uvicorn, sqlalchemy, geoalchemy2, pydantic)
- [ ] Create `docker-compose.yml` with PostGIS 16, Redis 7
- [ ] Create `db/migrations/` with Alembic init
- [ ] `apps/web/` — `npx create-remix@latest` scaffold
- [ ] `apps/mobile/` — `npx create-expo-app@latest` scaffold

#### 1.2 Database Schema
- [ ] Alembic migration: `facilities` table with GIST index
- [ ] Alembic migration: `facility_financials` table
- [ ] Alembic migration: `facility_violations` table
- [ ] Alembic migration: `layers` catalog table
- [ ] Trigger: auto-set `geom` from `lat`/`lon` on insert/update

#### 1.3 CDPH Ingest Pipeline
- [ ] `pipeline/src/pipeline/ingest/cdph.py` — download CSV, normalize column names, map FACTYPE to canonical type enum
- [ ] `pipeline/src/pipeline/crosswalk/resolver.py` — download and parse facility crosswalk XLSX, populate `cdph_id`/`cms_npi`/`oshpd_id`
- [ ] Upsert logic: match on `cdph_id`, update name/address/status/geom
- [ ] Celery task: `ingest_cdph` — monthly schedule
- [ ] Celery task: `ingest_crosswalk` — runs after `ingest_cdph`

#### 1.4 Tile Generation & R2 Upload
- [ ] `pipeline/src/pipeline/tiles/generate.py` — PostGIS → GeoJSON export → Tippecanoe subprocess
- [ ] `pipeline/src/pipeline/tiles/r2_upload.py` — boto3 S3-compat upload to R2
- [ ] Celery task: `generate_facilities_tiles` — triggered after ingest completes
- [ ] Update `layers` table record after successful upload
- [ ] Celery beat schedule: full regeneration nightly

#### 1.5 Cloudflare Worker
- [ ] `workers/tiles/` scaffold with Wrangler
- [ ] JWT validation using `jose` or `@tsndr/cloudflare-worker-jwt`
- [ ] PMTiles range request handler using `pmtiles` npm package
- [ ] R2 binding configuration in `wrangler.toml`
- [ ] Per-layer access policy stored in Workers KV

#### 1.6 FastAPI Filter API
- [ ] `api/src/api/routes/facilities.py` — `POST /facilities/filter` endpoint
- [ ] `api/src/api/routes/layers.py` — `GET /layers` catalog endpoint
- [ ] Dynamic PostGIS query builder (safe parameterized SQL — no string interpolation)
- [ ] GeoJSON FeatureCollection response

#### 1.7 Remix Web App — Basic Map
- [ ] MapLibre GL JS integration in Remix (`apps/web/app/components/Map.tsx`)
- [ ] Load PMTiles source from Worker URL
- [ ] Layer toggle panel (facility types on/off)
- [ ] Basic attribute filter panel (type, status)
- [ ] Click facility → popup with name, type, address, status

#### 1.8 Expo Mobile App — Basic Map
- [ ] `@maplibre/maplibre-react-native` integration
- [ ] Same PMTiles source
- [ ] Layer toggle bottom sheet
- [ ] Facility tap → bottom sheet with details

---

### Phase 2 — Financial Enrichment
- CMS HCRIS HHA + Hospice cost report ingest
- CA HCAI annual financial disclosure ingest
- Join financials to facility records via NPI/OSHPD ID
- Add revenue filter to filter API and UI

### Phase 3 — Violation Enrichment
- CMS Care Compare deficiency ingest (home health + hospice)
- CA CDPH L&C inspection data ingest
- Violation count, severity, most recent violation date on facility records
- Add violation filter to filter API and UI

### Phase 4 — Daycares
- CA CDSS Community Care Licensing ingest
- Geocode addresses via Census Geocoder → Geocodio fallback
- Fuzzy name+address entity resolution against CDPH records
- CDSS CCL inspection/violation ingest

### Phase 5 — Web + Mobile App Polish
- Spatial polygon draw filter (MapLibre draw plugin)
- Compound AND/OR filter logic
- Facility detail page: full financials table + violations timeline
- Mobile: full filter UI, facility detail sheet

### Phase 6 — Productize
- Multi-tenant auth (per-user API keys, role-based layer access)
- Usage metering (tile requests, API calls)
- Stripe billing integration
- Admin dashboard

---

## System-Wide Impact

### Interaction Graph
Ingest task completion triggers `generate_facilities_tiles` (Celery signal), which runs Tippecanoe subprocess → writes PMTiles file → `r2_upload` task pushes to R2 → updates `layers.last_generated` and `layers.record_count` in PostGIS.

Filter API calls PostGIS directly (no tile server involvement). Client receives GeoJSON feature IDs → calls `map.setFilter()` to highlight matching features in already-rendered tiles.

### Error & Failure Propagation
- Failed CDPH download: Celery retry (3x with exponential backoff). Alert after 3 failures. Last-known-good data stays in PostGIS.
- Failed geocode: Mark facility `geocode_status = 'failed'`, exclude from tile export until resolved.
- Failed tile generation: R2 is not updated; old tiles continue serving. Alert on failure.
- Failed R2 upload: Retry 3x; tiles remain on disk until next successful upload.
- PostGIS connection failure: Celery task fails, queued for retry. FastAPI returns 503.

### State Lifecycle Risks
- Upsert on `cdph_id` is safe — PostgreSQL `ON CONFLICT DO UPDATE` is atomic.
- Tile generation writes to a temp file first; atomic rename prevents serving partial tiles.
- R2 upload uses a versioned key pattern (`facilities-{timestamp}.pmtiles`) with Workers KV pointer update — zero-downtime tile refresh.

### Integration Test Scenarios
1. Ingest a small CDPH CSV fixture → verify `facilities` rows created with correct `type`, `geom` populated from lat/lon
2. Run crosswalk ingest → verify `cms_npi` and `oshpd_id` fields populated on matching records
3. Call `POST /facilities/filter` with polygon covering known test coordinates → verify correct facilities returned
4. Generate tiles from test dataset → verify PMTiles file is valid (tile-join or pmtiles verify)
5. Worker JWT validation: expired token returns 401; valid token for restricted layer with wrong role returns 403

---

## Acceptance Criteria

### Functional
- [ ] `docker compose up` starts PostGIS, Redis, API, Celery worker locally
- [ ] `ingest_cdph` task downloads CDPH CSV, normalizes, upserts to PostGIS without errors
- [ ] All CDPH facilities with lat/lon have `geom` populated and are queryable via `ST_Intersects`
- [ ] `ingest_crosswalk` populates `cms_npi` and `oshpd_id` on matching facility records
- [ ] `generate_facilities_tiles` produces a valid PMTiles file covering zoom 4–14
- [ ] PMTiles file uploads to Cloudflare R2 successfully
- [ ] Cloudflare Worker serves tiles with valid JWT; rejects expired/missing tokens
- [ ] `POST /facilities/filter` returns correct GeoJSON for type + status + polygon filters
- [ ] Remix app renders CDPH facilities as a point layer on MapLibre map
- [ ] Layer toggles show/hide facility type layers
- [ ] Expo app renders same tiles on iOS and Android simulators

### Non-Functional
- [ ] Tile serving latency p95 < 100ms (Cloudflare edge cache)
- [ ] Filter API p95 < 500ms for up to 100k facility records
- [ ] Ingest pipeline handles CDPH CSV (~50k rows) in under 5 minutes
- [ ] No SQL string interpolation in filter API (SQLAlchemy parameterized only)

### Quality Gates
- [ ] All Python code passes `ruff` lint + `mypy` type check
- [ ] Integration tests cover the 5 scenarios listed above
- [ ] `CLAUDE.md` documents stack decisions, env vars, and common commands
- [ ] `docker compose up` works on a clean machine with no pre-existing data

---

## Dependencies & Prerequisites

- **Cloudflare account** — R2 bucket + Workers with KV namespace
- **PostgreSQL 16 + PostGIS 3.4** — local via Docker; prod via Supabase or managed PG
- **Tippecanoe** — installed in pipeline Docker image (`apt install tippecanoe` or build from source)
- **uv** — Python package manager (faster than pip)
- **Node 20+** — for Remix, Expo, and Workers
- **Wrangler CLI** — for Cloudflare Workers deploy

---

## Risk Analysis

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| CDPH CSV schema changes | Medium | High | Schema validation on ingest; alert on unexpected columns |
| CMS HCRIS format changes | Low | High | Version-pin data download by year; test against fixture |
| Geocoding budget overrun | Medium | Low | Quota CDSS batch to Census Geocoder first (free); cap Geocodio spend |
| PMTiles R2 egress cost | Low | Medium | Cloudflare R2 has zero egress to Workers; cost is only storage + operations |
| Entity resolution false matches | Medium | Medium | Conservative threshold; flag uncertain matches for review |
| Tippecanoe drops attributes | Low | High | Test tile output with `tile-join --decode` before shipping |

---

## Sources & References

### Origin
- **Brainstorm document:** [docs/brainstorms/2026-04-10-vector-tile-platform-brainstorm.md](../brainstorms/2026-04-10-vector-tile-platform-brainstorm.md)
  - Key decisions carried forward: PMTiles hybrid architecture, FastAPI + Python pipeline (geopandas/GDAL), Cloudflare R2 + Workers for tile serving, MapLibre GL JS + Expo mobile

### Data Sources
- CA CDPH Healthcare Facility Locations: https://data.chhs.ca.gov/dataset/healthcare-facility-locations
- CA Licensed Facility Crosswalk: https://data.chhs.ca.gov/dataset/licensed-and-certified-healthcare-facility-crosswalk
- CMS HCRIS Home Health: https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports/Home-Health-Agency
- CMS HCRIS Hospice: https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/Cost-Reports/Hospice
- CMS Care Compare Data: https://data.cms.gov/provider-data/
- CA CDSS Community Care Licensing: https://www.ccld.dss.ca.gov/carefacilitysearch/

### External Documentation
- PMTiles spec + tooling: https://github.com/protomaps/PMTiles
- Tippecanoe docs: https://github.com/felt/tippecanoe
- Martin tile server: https://github.com/maplibre/martin
- MapLibre GL JS: https://maplibre.org/maplibre-gl-js/docs/
- `@maplibre/maplibre-react-native`: https://github.com/maplibre/maplibre-react-native
- Census Geocoder Batch API: https://geocoding.geo.census.gov/geocoder/Geocoding_Services_API.pdf
- Cloudflare R2 + Workers PMTiles pattern: https://developers.cloudflare.com/r2/
- PostGIS spatial indexing: https://postgis.net/workshops/postgis-intro/indexing.html
