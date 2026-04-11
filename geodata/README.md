# geodata

Care facility intelligence platform — maps daycares, home health agencies, and hospices across California enriched with financial and violation data.

## Stack

- **Pipeline:** Python 3.12, FastAPI, Celery + Redis, PostGIS
- **Tiles:** Tippecanoe → PMTiles → Cloudflare R2
- **Web:** Remix (React), MapLibre GL JS
- **Mobile:** Expo (React Native), @maplibre/maplibre-react-native
- **Infra:** Docker / Colima locally, Cloudflare Workers + R2 in prod

## Quick Start

```bash
# Install Colima (Mac container runtime — no Docker Desktop required)
brew install colima docker docker-compose
colima start --cpu 4 --memory 8 --disk 60

# Start services
docker compose up

# Run migrations
cd pipeline && uv run alembic upgrade head

# Trigger CDPH ingest
cd pipeline && uv run celery -A pipeline.celery_app call pipeline.tasks.ingest_cdph

# Start API
cd api && uv run uvicorn api.main:app --reload --port 8000

# Start web app
cd apps/web && npm run dev

# Start mobile app
cd apps/mobile && npx expo start
```

## Structure

```
pipeline/    Python ingest pipeline + Celery tasks
api/         FastAPI filter + tile API
apps/web/    Remix web app
apps/mobile/ Expo mobile app
workers/     Cloudflare Workers (tile auth proxy)
db/          Alembic migrations
```

## Data Sources

| Source | What | Cadence |
|--------|------|---------|
| CA CDPH Healthcare Facility Locations | 30+ facility types with lat/lon | Monthly |
| CA Licensed Facility Crosswalk | ID linking (CDPH ↔ CMS NPI ↔ OSHPD) | Quarterly |
| CMS HCRIS | Medicare cost reports (revenue/expenses) | Annual |
| CA HCAI Annual Disclosure | CA facility financials | Annual |
| CMS Care Compare | Federal violation/deficiency data | Continuous |
| CA CDSS CCL | Daycare facility list + inspections | Periodic |
