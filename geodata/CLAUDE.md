# CLAUDE.md — geodata project conventions

## Stack

- **Python:** 3.12, managed with `uv` (NOT pip/poetry)
- **Backend API:** FastAPI + uvicorn
- **Pipeline:** Celery 5 + Redis broker
- **Database:** PostgreSQL 16 + PostGIS 3.4, SQLAlchemy 2.0 + GeoAlchemy2, Alembic migrations
- **Web app:** Remix (React), TypeScript
- **Mobile app:** Expo (React Native), TypeScript
- **Map rendering:** MapLibre GL JS (web), @maplibre/maplibre-react-native (mobile)
- **Tiles:** Tippecanoe → PMTiles format, served from Cloudflare R2
- **Edge:** Cloudflare Workers (tile auth proxy)
- **Container runtime:** Colima (NOT Docker Desktop)

## Python Conventions

- All Python packages use `src/` layout
- `ruff` for linting and formatting (`uv run ruff check .`, `uv run ruff format .`)
- Type hints required on all public functions
- Pydantic models for all API request/response schemas
- SQLAlchemy 2.0 style (not legacy 1.x patterns)
- Never use `SELECT *` — always name columns explicitly
- All SQL in FastAPI routes must use parameterized queries (SQLAlchemy text() or ORM) — no f-string SQL

## Database Conventions

- UUIDs as primary keys (`gen_random_uuid()`)
- `created_at TIMESTAMPTZ DEFAULT NOW()` on every table
- GIST spatial index on all geometry columns
- All geometry stored as `GEOMETRY(Point, 4326)` (WGS84)
- Migrations in `db/migrations/versions/` — never edit applied migrations

## Environment Variables

Copy `.env.example` to `.env`. Required:

```
DATABASE_URL=postgresql://geodata:geodata@localhost:5432/geodata
REDIS_URL=redis://localhost:6379/0
CF_ACCOUNT_ID=           # Cloudflare account ID (prod only)
CF_R2_ACCESS_KEY_ID=     # R2 access key (prod only)
CF_R2_SECRET_ACCESS_KEY= # R2 secret key (prod only)
R2_BUCKET_NAME=geodata-tiles
TILES_DIR=/tmp/geodata/tiles  # local tile storage
GEOCODIO_API_KEY=        # optional, for address geocoding fallback
```

## Common Commands

```bash
# Start local services (Colima must be running first)
colima start
docker compose up -d

# Run migrations
cd pipeline && uv run alembic upgrade head

# Start API in dev
cd api && uv run uvicorn api.main:app --reload --port 8000

# Start Celery worker
cd pipeline && uv run celery -A pipeline.celery_app worker --loglevel=info

# Start Celery beat (scheduler)
cd pipeline && uv run celery -A pipeline.celery_app beat --loglevel=info

# Run ingest manually
cd pipeline && uv run celery -A pipeline.celery_app call pipeline.tasks.ingest_cdph

# Lint
cd pipeline && uv run ruff check .
cd api && uv run ruff check .

# Remix dev
cd apps/web && npm run dev

# Expo dev
cd apps/mobile && npx expo start
```

## File Naming

- Python modules: `snake_case.py`
- React components: `PascalCase.tsx`
- Route files (Remix): `_index.tsx`, `map.tsx` per Remix v2 conventions
