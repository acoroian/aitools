---
date: 2026-04-10
topic: vector-tile-platform
---

# Care Facility Intelligence Platform — Vector Tile + Data Aggregation

## What We're Building

A geospatial intelligence platform that aggregates public data on care facilities (daycares, home health agencies, hospices, and similar businesses), enriches each facility record with financial and violation data, and renders everything as interactive filterable vector tile maps.

The platform powers internal web (Remix) and mobile (Expo) apps where users can:
- Toggle facility type layers on/off (daycares, home health, hospice, etc.)
- Filter within layers by attributes: revenue range, violation count, payer type, diagnosis category, year
- Draw a polygon on the map to spatially filter to a specific area
- Combine spatial + attribute filters with AND/OR logic
- Click a facility to see its full profile: financials, violations, utilization history

Built internal-first, designed to productize and sell.

## Data Sources

### Facility Locations (the base layer)

| Source | Facility Types | Geo | Update Cadence |
|--------|---------------|-----|----------------|
| CA CDPH Healthcare Facility Locations | Home health, hospice, SNF, 30+ types | **Lat/lon included** | Monthly |
| CA CDSS Community Care Licensing (CCL) | Daycares, adult day programs, residential care | Address → geocode | Periodic |
| CMS Provider of Services (POS) | All Medicare/Medicaid certified home health + hospice | Address → geocode | Quarterly |

### Financial / Revenue

| Source | What It Contains | Cadence |
|--------|-----------------|---------|
| CMS HCRIS (Healthcare Cost Report Info System) | Medicare cost reports: gross revenue, expenses, patient visits per provider | Annual |
| CA HCAI Annual Financial Disclosure | CA facility financials: gross revenue by payer mix | Annual |

### Violations / Inspections

| Source | What It Contains | Cadence |
|--------|-----------------|---------|
| CMS Care Compare (CASPER) | Federal survey deficiencies for home health + hospice: date, type, severity | Continuous |
| CA CDPH L&C Division | CA facility inspections, complaints, citations | Periodic |
| CA CDSS CCL Reports | Daycare inspection results and violations | Periodic |

### Entity Resolution (the glue)

| Source | Purpose |
|--------|---------|
| CA Licensed Facility Crosswalk | Links CDPH ID ↔ CMS NPI ↔ OSHPD ID — joins all sources to one facility record |

## Unified Facility Record Model

Every facility in PostGIS has one canonical record that aggregates across sources:

```
facility {
  id, name, type, address, lat, lon
  cdph_id, cms_npi, oshpd_id          ← crosswalk IDs
  license_status, license_expiry

  financials[] {
    year, gross_revenue, medicare_pct,
    medicaid_pct, private_pct, total_expenses
  }

  violations[] {
    date, source, severity, category, description, resolved
  }

  utilization[] {
    year, patient_count, diagnosis_category,
    payer_type, demographics
  }
}
```

## Data Pipeline Architecture

```
[ Public Data Sources ]
  CDPH CSV (monthly)    CDSS CSV      CMS POS/HCRIS     CASPER violations
       ↓                    ↓               ↓                  ↓
[ Ingest Workers ]
  Download → Normalize → Geocode (addresses) → Entity resolve (crosswalk + fuzzy match)
       ↓
[ PostGIS ]
  Canonical facility records + financials + violations + utilization
       ↓                              ↓
[ Tile Generation ]           [ Attribute Filter API ]
  Tippecanoe → PMTiles          POST /facilities/filter
  (regenerate on schedule)      spatial + attribute queries
       ↓                              ↓
[ Cloudflare R2 ]            [ Client filter expressions ]
  pmtiles/facilities/...        MapLibre layer.setFilter()
       ↓
[ Cloudflare Workers ]
  JWT auth, per-layer access
       ↓
[ Remix (web) + Expo (mobile) ]
  MapLibre GL JS / MapLibre RN
  Layer toggles + filter UI
```

## Key Hard Problems

1. **Entity resolution** — Daycares in CDSS have no CMS NPI. Home health agencies appear in CDPH, CMS POS, and HCAI with different IDs. Strategy: use the crosswalk first, then fuzzy name + address matching for the remainder.

2. **Geocoding** — CMS and CDSS provide addresses, not lat/lon. Use a geocoding service (Geocodio for bulk, Google Maps API for precision) or the Census Geocoder (free, slower).

3. **Temporal data model** — Financials are annual snapshots; violations are events; facility status changes monthly. The DB model stores all vintages so users can filter "as of year X."

4. **PMTile regeneration schedule** — Facility points change monthly (CDPH refresh). Trigger a Tippecanoe re-run on source data updates; push new PMTiles to R2 atomically.

## Why This Approach

Three approaches considered:

- **Static PMTiles only**: Cheap tile serving, but attribute filtering requires a separate API path and tiles must be regenerated when data changes.
- **Dynamic tile server (Martin/pg_tileserv)**: All data in PostGIS, tiles generated on-demand. Flexible but tile server becomes a bottleneck at scale.
- **Hybrid (chosen)**: PMTiles for high-volume tile delivery via CDN; PostGIS + filter API for attribute/spatial queries. Each scales independently.

## Technology Stack

| Layer | Tool | Why |
|-------|------|-----|
| Tile format | PMTiles | HTTP range requests, no tile server process needed |
| Tile generation | Tippecanoe | Best-in-class MVT generation from GeoJSON/CSV |
| Format normalization | GDAL / Python (geopandas) | Handles XLSX, CSV, Shapefile, GeoJSON |
| Geocoding | Census Geocoder (free) + Geocodio (bulk fallback) | Batch geocode facility addresses |
| Tile storage | Cloudflare R2 | Zero egress fees, global |
| Edge routing + auth | Cloudflare Workers | JWT validation, per-layer access policy |
| Dynamic tiles | Martin (Rust) | PostGIS-backed, for real-time/user-uploaded layers |
| Attribute + spatial DB | PostGIS / PostgreSQL | Facility records, financials, violations, ST_Intersects |
| Web app | Remix (React) | Full-stack SSR, great DX |
| Mobile app | Expo (React Native) | Managed RN, OTA updates |
| Map rendering | MapLibre GL JS (web) + @maplibre/maplibre-react-native | Open source, no Mapbox licensing fees |
| Backend API | FastAPI (Python) | Ingest orchestration, filter API, easy geo libs |
| Async pipeline | Celery + Redis | Non-blocking ingest jobs, tile regeneration triggers |

> FastAPI preferred over Go here because the ingest pipeline is Python-heavy (pandas, geopandas, GDAL bindings).

## Resolved Questions

- **Filter complexity**: Attribute (equality/range) + spatial (drawn polygon) + compound AND/OR logic. PostGIS `ST_Intersects` + dynamic `WHERE` clauses.
- **Dataset scale**: Under 10M features per dataset. PostGIS is sufficient — no ClickHouse needed at launch.
- **Update frequency**: Batch ingest on source schedules (monthly CDPH, quarterly CMS). No streaming needed.
- **Mobile**: Expo + `@maplibre/maplibre-react-native`.
- **Web**: Remix + MapLibre GL JS.
- **Auth**: Per-layer permissions via Cloudflare Workers JWT validation.
- **Geography**: California-first using state sources (CDPH, CDSS, HCAI) + federal CMS for home health/hospice. Best data quality from combining both.

## Phased Build Order

1. **Phase 1 — Ingest pipeline**: Download CDPH facility locations CSV, normalize, load into PostGIS with lat/lon. Generate PMTiles. Serve on R2.
2. **Phase 2 — Financial enrichment**: Ingest CMS HCRIS cost reports + CA HCAI disclosures. Join to facility records via NPI/OSHPD ID.
3. **Phase 3 — Violation enrichment**: Ingest CMS Care Compare deficiency data + CDSS CCL inspection reports. Link to facility records.
4. **Phase 4 — Daycares**: Ingest CDSS Community Care Licensing. Geocode addresses. Entity-resolve against CDPH crosswalk where possible.
5. **Phase 5 — Web + mobile app**: Remix map UI with layer toggles, attribute filters, facility detail panel. Expo mobile app.
6. **Phase 6 — Productize**: Multi-tenancy, API keys, usage billing.

## Next Steps

→ `/ce:plan` starting with Phase 1 — CDPH ingest pipeline, PostGIS schema, Tippecanoe tile generation, R2 upload.
