---
title: Phase 3 — Violation Enrichment
type: feat
status: design
date: 2026-04-11
origin: docs/plans/2026-04-10-feat-care-facility-intelligence-platform-plan.md (Phase 3 section)
---

# Phase 3 — Violation Enrichment

## Overview

Populate the existing `facility_violations` table with real regulatory
history from two public datasets, compute per-facility rollup attributes
for fast filtering and tile rendering, and expose new violation-based
filters through the FastAPI filter API.

The original phase plan assumed CMS Care Compare publishes deficiency
CSVs for home health and hospice. It does not — CMS only publishes
bulk deficiency data for **nursing homes (SNFs)**. We therefore use two
complementary sources:

- **CMS Nursing Home Health Deficiencies** (dataset `r5ix-sfxw`): rich
  per-tag data for SNFs, rolling 3-year window, monthly refresh.
- **CA CDPH Health Facilities State Enforcement Actions** (dataset
  `1e1e2904-1bfb-448c-97e1-cf3e228c9159`): enforcement-level citations
  for 30+ CA facility types including HH, hospice, hospitals, and
  clinics, with penalty amounts and history back to 1997, annual
  refresh.

Home-health and hospice tag-level data remains a known gap; CDPH State
Enforcement Actions is the best bulk-downloadable proxy for those
facility types today.

---

## Problem Statement

The `facility_violations` table was created in Phase 1 but has never
been populated. The filter API has no violation filters. The tile
generator does not emit violation attributes, so the map cannot show
regulatory history. Phase 3 closes that gap.

---

## Architecture

```
CMS Provider Data Catalog (r5ix-sfxw)         data.chhs.ca.gov (SEA dataset)
         │                                              │
         │ monthly refresh                              │ annual refresh
         ▼                                              ▼
┌──────────────────────────┐           ┌──────────────────────────┐
│ ingest/cms_nh_compare.py │           │ ingest/cdph_sea.py       │
│  - discover current CSV  │           │  - discover current XLSX │
│  - download + parse      │           │  - download + parse      │
│  - filter to CA rows     │           │  - normalize columns     │
│  - normalize columns     │           │  - join LTC narratives   │
│  - upsert by stable key  │           │  - upsert by stable key  │
└────────────┬─────────────┘           └────────────┬─────────────┘
             │                                      │
             │        write rows with source tag    │
             ▼                                      ▼
         ┌────────────────────────────────────────────┐
         │         facility_violations (existing)     │
         │  source IN ('cms_nh_compare','cdph_sea')   │
         └────────────────────┬───────────────────────┘
                              │  chained via Celery signal
                              ▼
         ┌────────────────────────────────────────────┐
         │  refresh_violation_rollup (Celery task)    │
         │  TRUNCATE + INSERT ... SELECT              │
         └────────────────────┬───────────────────────┘
                              │
                              ▼
         ┌────────────────────────────────────────────┐
         │  facility_violation_rollup (new table)     │
         │  feeds tile generator + filter API         │
         └────────────────────────────────────────────┘
```

**Celery schedule (beat):**

- `ingest_cms_nh_compare` — monthly, 1st of month
- `ingest_cdph_sea` — annually, July 15 (after CDPH's typical mid-year
  publication)
- `refresh_violation_rollup` — chained after either ingest via Celery
  signal; also exposed as a standalone task for manual refresh
- `generate_facilities_tiles` — already exists; will now LEFT JOIN the
  rollup table

**Failure isolation:** a failure in one source does not block the
other or the rollup. Whichever data is present at rollup time is
included; missing sources contribute zero.

---

## Components & File Layout

```
pipeline/src/pipeline/
├── ingest/
│   ├── cms_nh_compare.py      NEW — CMS Health Deficiencies (SNF, nationwide, monthly)
│   └── cdph_sea.py            NEW — CDPH State Enforcement Actions (CA all types, annual)
├── violations/                 NEW package
│   ├── __init__.py
│   ├── normalize.py           severity/scope grids, source constants, IJ predicate
│   ├── rollup.py              refresh_violation_rollup SQL runner
│   └── snapshots.py           raw-file archive helper (download-to-disk before parse)
├── tasks.py                   + register 3 new Celery tasks
└── celery_app.py              + 2 new beat entries

db/migrations/versions/
└── 004_violation_rollup.py    NEW migration

pipeline/tests/
├── fixtures/violations/
│   ├── cms_nh_compare_sample.csv
│   ├── cdph_sea_sample.xlsx
│   └── ltc_narratives_sample.csv
├── test_violations_ingest.py   NEW integration tests
├── test_violations_rollup.py   NEW rollup correctness tests
└── test_violations_live.py     NEW live-endpoint smoke (pytest.mark.live)

api/src/api/
├── routes/facilities.py       + violation filter fields
└── schemas.py                 + violation filter Pydantic fields
```

### `ingest/cms_nh_compare.py`

Mirrors `hcris.py`'s shape:

- `discover_latest_csv_url()` — hits
  `https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/r5ix-sfxw`
  to find the current `NH_HealthCitations_YYYYMMM.csv` filename. No
  hardcoded date. On failure, falls back to the sidecar
  `/tmp/geodata/snapshots/cms_nh_compare/.last_url`.
- `download_csv(url) -> pd.DataFrame` — streams to a `*.tmp` file,
  atomic rename on success. Archives a copy via `snapshots.py`.
- `normalize(df) -> list[ViolationRow]` — renames columns to our
  schema, parses dates, filters to `state == 'CA'`, derives
  `deficiency_tag` as `{deficiency_prefix}{deficiency_tag_number}`
  (e.g., `"F0880"`), stores raw letter in `severity`, derives `scope`
  from the letter via the A–L grid.
- `upsert(rows)` — bulk upsert keyed on `(source, citation_id)` via
  `INSERT ... ON CONFLICT DO UPDATE`.
- `citation_id` derivation: `{ccn}_{survey_date}_{tag}_{scope_severity}`
  — deterministic composite since CMS does not publish a native
  citation ID.
- Celery task `ingest_cms_nh_compare` — downloads, normalizes,
  upserts, returns `IngestResult`, then triggers
  `refresh_violation_rollup` via Celery signal.

CMS CSV columns consumed (confirmed via live API query 2026-04-11):
`cms_certification_number_ccn`, `provider_name`, `survey_date`,
`survey_type`, `deficiency_prefix`, `deficiency_category`,
`deficiency_tag_number`, `deficiency_description`,
`scope_severity_code`, `deficiency_corrected`, `correction_date`,
`inspection_cycle`, `standard_deficiency`, `complaint_deficiency`,
`infection_control_inspection_deficiency`, `citation_under_idr`,
`citation_under_iidr`, `state`, `processing_date`.

### `ingest/cdph_sea.py`

Follows `hcai.py`'s XLSX-loading pattern:

- `discover_latest_url()` — queries CKAN
  `data.chhs.ca.gov/api/3/action/package_show?id=<dataset_id>` to find
  the current `sea_final_YYYYMMDD.xlsx` resource URL. Same sidecar
  fallback.
- `download_xlsx(url) -> pd.DataFrame` — `pandas.read_excel` via
  openpyxl. Archives raw file to snapshots.
- `normalize(df) -> list[ViolationRow]` — maps CDPH columns to our
  schema, joins to `facilities` via `cdph_id` (FACID), logs unmatched
  FACIDs, normalizes CDPH class codes (AA, A, B, …) via the
  `CDPH_CLASS_TO_SEVERITY` grid.
- `enrich_with_narratives()` — reads the LTC citation narratives CSV
  (`ltccitationnarratives19982017.csv`), joins by citation ID, and
  populates `description` for LTC rows. Kept behind a config flag
  (default on).
- `upsert(rows)` — idempotent upsert on `(source, citation_id)` where
  `citation_id` is CDPH's native enforcement action ID (confirm exact
  column name at implementation time).
- Celery task `ingest_cdph_sea` — same shape as CMS task.

Confirm at implementation time (not decoded here because the XLSX
needs to be downloaded to inspect): the exact column name for the
facility ID (FACID or CDPH number), citation/action ID, class/severity
code, violation date, penalty amount, and narrative text.

### `violations/normalize.py`

A small shared module:

- `SOURCE_CMS_NH = "cms_nh_compare"`, `SOURCE_CDPH_SEA = "cdph_sea"`.
- `CMS_SEVERITY_SCOPE_GRID` — the A–L letter grid mapping each letter
  to `(scope, severity_level_ord)`. E.g., `"J"` →
  `("isolated", 8)`, `"L"` → `("widespread", 10)`. We store the raw
  letter in `severity` and set `scope` from the grid lookup.
- `CDPH_CLASS_TO_SEVERITY` — CDPH citation classes (AA, A, B, …)
  mapping to the same normalized 0–10 scale so SQL comparisons work
  across sources. Example: AA → 10 (equivalent to IJ), A → 8,
  B → 4.
- `is_immediate_jeopardy(source, severity) -> bool` — shared
  predicate used by rollup tests and fixtures.

### `violations/rollup.py`

Single function `refresh_violation_rollup(session)` that runs the SQL
in the Schema section below inside a transaction. No ORM — raw
parameterized `text()`. Idempotent and fast (sub-second at CA scale).

### `violations/snapshots.py`

One helper:

```python
def archive_raw(source: str, content: bytes, processed_at: date) -> Path:
    """Write raw downloaded file to /tmp/geodata/snapshots/{source}/{date}.{ext}."""
```

Handles atomic write (`*.tmp` + rename), directory creation, and
content-addressed filename collisions.

---

## Database Schema Changes

**Migration `004_violation_rollup.py`** does four things:

```sql
-- (a) Stable-key unique constraint on facility_violations for idempotent upserts
ALTER TABLE facility_violations
  ADD CONSTRAINT uq_facility_violations_source_citation
  UNIQUE (source, citation_id);

-- (b) Supporting indexes for the rollup query and filter API
CREATE INDEX idx_facility_violations_facility_survey
  ON facility_violations(facility_id, survey_date DESC);
CREATE INDEX idx_facility_violations_source_severity
  ON facility_violations(source, severity);

-- (c) Rollup table — one row per facility, rebuilt after each ingest
CREATE TABLE facility_violation_rollup (
    facility_id              UUID PRIMARY KEY REFERENCES facilities(id) ON DELETE CASCADE,

    -- Counts
    violation_count_total    INTEGER NOT NULL DEFAULT 0,
    violation_count_12mo     INTEGER NOT NULL DEFAULT 0,

    -- Per-source counts (lets UI distinguish CMS deficiencies vs CDPH state actions)
    cms_count_total          INTEGER NOT NULL DEFAULT 0,
    cms_count_12mo           INTEGER NOT NULL DEFAULT 0,
    cdph_count_total         INTEGER NOT NULL DEFAULT 0,
    cdph_count_12mo          INTEGER NOT NULL DEFAULT 0,

    -- Severity indicators (12-month window)
    max_severity_12mo        TEXT,       -- source-qualified: "cms:J" or "cdph:AA"
    max_severity_level_12mo  SMALLINT,   -- normalized 0-10 for cross-source comparison
    has_ij_12mo              BOOLEAN NOT NULL DEFAULT FALSE,

    -- Freshness
    last_survey_date         DATE,
    last_refreshed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_rollup_ij_12mo
  ON facility_violation_rollup(has_ij_12mo) WHERE has_ij_12mo = TRUE;
CREATE INDEX idx_rollup_severity_level
  ON facility_violation_rollup(max_severity_level_12mo DESC);
CREATE INDEX idx_rollup_count_total
  ON facility_violation_rollup(violation_count_total DESC);
CREATE INDEX idx_rollup_last_survey
  ON facility_violation_rollup(last_survey_date DESC);

-- (d) SQL helper functions for cross-source normalization
CREATE OR REPLACE FUNCTION severity_level_ord(source TEXT, severity TEXT)
RETURNS SMALLINT AS $$
  SELECT CASE
    WHEN source = 'cms_nh_compare' THEN CASE severity
      WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3
      WHEN 'D' THEN 4 WHEN 'E' THEN 5 WHEN 'F' THEN 6
      WHEN 'G' THEN 6 WHEN 'H' THEN 7 WHEN 'I' THEN 7
      WHEN 'J' THEN 8 WHEN 'K' THEN 9 WHEN 'L' THEN 10
      ELSE NULL
    END
    WHEN source = 'cdph_sea' THEN CASE severity
      WHEN 'AA' THEN 10 WHEN 'A' THEN 8 WHEN 'B' THEN 4
      ELSE NULL
    END
    ELSE NULL
  END;
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION is_immediate_jeopardy_sql(source TEXT, severity TEXT)
RETURNS BOOLEAN AS $$
  SELECT severity_level_ord(source, severity) >= 8;
$$ LANGUAGE SQL IMMUTABLE;
```

**Rollup refresh query** (runs inside `violations/rollup.py`):

```sql
BEGIN;

TRUNCATE facility_violation_rollup;

INSERT INTO facility_violation_rollup (
    facility_id,
    violation_count_total, violation_count_12mo,
    cms_count_total, cms_count_12mo,
    cdph_count_total, cdph_count_12mo,
    max_severity_12mo, max_severity_level_12mo,
    has_ij_12mo,
    last_survey_date,
    last_refreshed_at
)
SELECT
    f.id AS facility_id,
    COUNT(v.id) AS violation_count_total,
    COUNT(v.id) FILTER (WHERE v.survey_date >= CURRENT_DATE - INTERVAL '12 months')
        AS violation_count_12mo,
    COUNT(v.id) FILTER (WHERE v.source = 'cms_nh_compare') AS cms_count_total,
    COUNT(v.id) FILTER (WHERE v.source = 'cms_nh_compare'
                          AND v.survey_date >= CURRENT_DATE - INTERVAL '12 months')
        AS cms_count_12mo,
    COUNT(v.id) FILTER (WHERE v.source = 'cdph_sea') AS cdph_count_total,
    COUNT(v.id) FILTER (WHERE v.source = 'cdph_sea'
                          AND v.survey_date >= CURRENT_DATE - INTERVAL '12 months')
        AS cdph_count_12mo,
    (SELECT v2.source || ':' || v2.severity
       FROM facility_violations v2
      WHERE v2.facility_id = f.id
        AND v2.survey_date >= CURRENT_DATE - INTERVAL '12 months'
      ORDER BY severity_level_ord(v2.source, v2.severity) DESC NULLS LAST
      LIMIT 1) AS max_severity_12mo,
    MAX(severity_level_ord(v.source, v.severity))
      FILTER (WHERE v.survey_date >= CURRENT_DATE - INTERVAL '12 months')
        AS max_severity_level_12mo,
    BOOL_OR(is_immediate_jeopardy_sql(v.source, v.severity))
      FILTER (WHERE v.survey_date >= CURRENT_DATE - INTERVAL '12 months')
        AS has_ij_12mo,
    MAX(v.survey_date) AS last_survey_date,
    NOW() AS last_refreshed_at
FROM facilities f
LEFT JOIN facility_violations v ON v.facility_id = f.id
GROUP BY f.id;

COMMIT;
```

**Why TRUNCATE + INSERT, not incremental UPDATE:** simpler to reason
about, no stale-row risk, single transaction means readers never see a
partial view. At CA scale (~50k facilities) this is sub-second.

---

## Ingest Result Shape

All three Celery tasks return:

```python
@dataclass
class IngestResult:
    source: str                 # "cms_nh_compare" | "cdph_sea" | "rollup"
    source_url: str | None
    rows_downloaded: int
    rows_ingested: int          # successfully upserted
    rows_unmatched: int         # no matching facility
    rows_invalid: int           # schema/parse errors, skipped
    runtime_seconds: float
    snapshot_path: str | None   # where the raw file was archived
```

Logged at INFO in a single structured line at completion. Matches the
existing HCRIS/HCAI return shape so future monitoring stays uniform.

---

## Error Handling

| Failure | Response |
|---|---|
| CMS/CDPH metadata endpoint down (filename discovery fails) | Celery retry 3× exponential backoff. Fall back to sidecar `.last_url`. Alert on final failure. |
| CSV/XLSX download fails mid-stream | httpx retries 3×. Partial files written to `*.tmp`, atomic rename on success. |
| Schema drift (expected column missing) | `SchemaDriftError` raised before any upsert. Alert. Last-known-good DB state remains. |
| Row references a CCN/FACID we don't have | Counted as `rows_unmatched`, logged at DEBUG, skipped. |
| Row has malformed date or non-numeric severity | Counted as `rows_invalid`, WARNING log with row index, skipped. |
| Unique constraint collision on re-ingest | Expected — `ON CONFLICT DO UPDATE` handles it. |
| Rollup refresh fails mid-transaction | Rollback leaves previous rollup intact. Task marked failed. Downstream tile gen sees old but consistent data. |
| Tile gen runs before rollup finishes | LEFT JOIN means missing rollup rows just yield zero counts — non-fatal. |

**Idempotency guarantees:**

- Re-running any ingest task back-to-back produces identical DB state.
- Re-running `refresh_violation_rollup` produces identical rollup
  table.
- Raw snapshots use content-addressed filenames; re-downloads
  overwrite themselves safely.

---

## Filter API & Tile Changes

**`api/src/api/routes/facilities.py`** — extend the filter request
schema:

```python
class FilterRequest(BaseModel):
    # ... existing fields ...
    violation_count_min:     int | None = None
    violation_count_max:     int | None = None
    max_severity_level_min:  int | None = None  # 0-10 scale
    has_ij_12mo:             bool | None = None
    survey_date_after:       date | None = None
```

The endpoint LEFT JOINs `facility_violation_rollup` and adds the
corresponding WHERE clauses. All new filters are parameterized — no
string interpolation.

**`pipeline/src/pipeline/tiles/generate.py`** — add the five rollup
fields to the GeoJSON export SELECT:

```sql
SELECT
  f.id, f.name, f.type, ...,
  COALESCE(r.violation_count_total, 0) AS violation_count_total,
  COALESCE(r.violation_count_12mo, 0)  AS violation_count_12mo,
  COALESCE(r.max_severity_level_12mo, 0) AS max_severity_level_12mo,
  COALESCE(r.has_ij_12mo, FALSE) AS has_ij_12mo,
  r.last_survey_date
FROM facilities f
LEFT JOIN facility_violation_rollup r ON r.facility_id = f.id
LEFT JOIN ...
```

Add `--attribute-type` flags in the Tippecanoe call for the integer
fields. Update the `layers.attribute_schema` JSONB for the facilities
layer to advertise the new filterable attributes.

---

## Testing

Fixtures in `pipeline/tests/fixtures/violations/`:

- `cms_nh_compare_sample.csv` — ~20 rows covering scope/severity grid
  edge cases (A, F, J, L), standard + complaint surveys, one IJ row,
  one out-of-CA row that should be filtered, one row with a CCN we
  don't have.
- `cdph_sea_sample.xlsx` — ~15 rows covering Class AA, A, B citations,
  one pre-2020 row for history testing, one with an unmatched FACID.
- `ltc_narratives_sample.csv` — a handful of rows with matching
  citation IDs so the narrative-join branch is exercised.

Integration tests:

1. **CMS happy path** — load fixture, run ingest, assert exact row
   counts in `facility_violations`, severity letters preserved, CA
   state filter applied.
2. **CMS idempotent** — run ingest twice, assert same row count.
3. **CMS schema drift** — fixture missing `scope_severity_code` →
   assert `SchemaDriftError` before any write.
4. **CDPH SEA happy path + narrative join** — loads SEA fixture plus
   LTC narratives, asserts narratives populate `description` for
   matching LTC rows.
5. **CDPH SEA unmatched** — unmatched FACID counted in
   `rows_unmatched`, no error raised.
6. **Rollup correctness** — fabricate 3 facilities with known
   violation mixes, assert `violation_count_12mo`,
   `max_severity_12mo`, per-source counts, `has_ij_12mo` match
   hand-computed expected values.
7. **Rollup IJ detection** — CMS "J" row last month →
   `has_ij_12mo = TRUE`. Same row dated 14 months ago →
   `has_ij_12mo = FALSE`.
8. **Filter API end-to-end** — POST `/facilities/filter` with
   `{"violation_count_min": 5, "has_ij_12mo": true}`, assert correct
   facility subset returned.
9. **Tile export shape** — run GeoJSON export step, assert rollup
   columns appear in feature properties.

`pipeline/tests/test_violations_live.py` is marked
`@pytest.mark.live` and runs against real CMS + CDPH endpoints
on-demand only — not in CI.

---

## Acceptance Criteria

### Functional

- [ ] Migration `004` applies cleanly: unique constraint, rollup
      table, indexes, SQL helper functions all created
- [ ] `ingest_cms_nh_compare` discovers the current CSV via the
      metastore endpoint, filters to CA rows, upserts into
      `facility_violations` with `source = 'cms_nh_compare'`
- [ ] CMS rows preserve the raw `{prefix}{tag}` in `deficiency_tag`,
      raw letter in `severity`, and derived `citation_id` as
      `{ccn}_{survey_date}_{tag}_{scope_severity}`
- [ ] `ingest_cdph_sea` discovers the current XLSX via the CKAN API,
      upserts into `facility_violations` with `source = 'cdph_sea'`
- [ ] When the LTC narratives CSV is available for a citation,
      `description` is populated for LTC rows from CDPH
- [ ] `refresh_violation_rollup` runs to completion in <5s at CA
      scale, rebuilds `facility_violation_rollup` in a single
      transaction, and is chained after either ingest task
- [ ] `facility_violation_rollup` has exactly one row per facility
      after refresh (LEFT JOIN correctness)
- [ ] Rollup counts, `max_severity_12mo`, `max_severity_level_12mo`,
      `has_ij_12mo`, and `last_survey_date` are computed correctly
      for all 9 integration-test scenarios
- [ ] Raw CSV/XLSX snapshots archived to
      `/tmp/geodata/snapshots/{source}/` on each ingest
- [ ] Re-running any ingest task back-to-back produces zero net DB
      changes
- [ ] `POST /facilities/filter` accepts the new filter fields and
      LEFT JOINs `facility_violation_rollup`
- [ ] Tile generator emits the five rollup columns in PMTiles feature
      properties with correct `--attribute-type` flags
- [ ] `layers.attribute_schema` updated to advertise the new
      filterable attributes

### Non-Functional

- [ ] All new Python code passes `ruff check` and `ruff format`
- [ ] No `SELECT *` in any new SQL; all queries parameterized
- [ ] Failure in one source does not block the other or the rollup
- [ ] Celery beat schedule registered for both ingests at their
      documented cadences

### Quality Gates

- [ ] All 9 integration tests pass in CI against the PostGIS test
      container
- [ ] `test_violations_live.py` passes on-demand against real
      endpoints at least once before the phase is considered done
- [ ] `docker compose up` + `alembic upgrade head` + manual
      `celery call pipeline.tasks.ingest_cms_nh_compare` produces
      real data in `facility_violations` on a clean machine

---

## Out of Scope

- Web and mobile UI changes beyond the attribute schema being
  filterable — UI polish happens in Phase 5
- Daycare / CDSS violation data — Phase 4
- Non-CA facilities — CMS rows filtered to `state = 'CA'` before
  upsert; national expansion is a future config flag

---

## Follow-ups (Separate Tickets)

1. **Phase 3.5 — CMS archive backfill.** One-time ingest of CMS's
   archived historical deficiency snapshots (separate dataset IDs,
   pre-2023 data) to deepen history beyond the rolling 3-year window.
2. **Violation detail view in UI.** Phase 5: a drawer listing all
   `facility_violations` for a clicked facility with timeline and
   narrative text.
3. **Ingest status dashboard.** Admin page surfacing structured
   `IngestResult` logs across sources. Ties into Phase 6 productization.
4. **CDPH full per-tag deficiency data.** If CDPH ever makes Cal
   Health Find's tag-level data bulk-downloadable, add a third source
   module. Until then, CDPH coverage is enforcement-level only.

---

## Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| CMS metastore endpoint changes URL format | Low | Medium | Sidecar `.last_url` fallback; alert on discovery failure |
| CDPH SEA XLSX column names drift year-over-year | Medium | Medium | Schema validation; `SchemaDriftError` names the missing column; annual cadence gives time to adapt |
| CMS K-prefix (life safety) rows mix with F-prefix (health) | Low | Low | `deficiency_tag` preserves prefix; rollup SQL treats K-prefix J/K/L as IJ too (same severity meaning) |
| Rollup refresh races with filter API read during long ingests | Very low | Low | Rollup refresh is a single transaction; readers see old-consistent or new-consistent, never partial |
| Unique constraint collision if `citation_id` derivation changes | Medium | High | Derivation isolated in `violations/normalize.py`; changing it requires an explicit migration that wipes+reinserts from raw snapshots |
| Non-CA rows leak into DB via a config flip | Low | Low | State filter runs on the DataFrame before upsert; test case covers this |
| CDPH SEA XLSX column names unknown until first download | High | Low | Implementation-time inspection; fall back to manual mapping if needed; annual cadence gives slack |

---

## Sources & References

### Data Sources

- CMS Nursing Home Health Deficiencies dataset:
  https://data.cms.gov/provider-data/dataset/r5ix-sfxw
- CMS Provider Data Catalog API (dataset metadata):
  https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/r5ix-sfxw
- CMS Nursing Home Data Dictionary:
  https://data.cms.gov/provider-data/sites/default/files/data_dictionaries/nursing_home/NH_Data_Dictionary.pdf
- CDPH Health Facilities State Enforcement Actions:
  https://data.chhs.ca.gov/dataset/healthcare-facility-state-enforcement-actions
- CDPH LTC Citation Narratives (1998-2017):
  https://data.chhs.ca.gov/dataset/1e1e2904-1bfb-448c-97e1-cf3e228c9159/resource/cf865dab-bbf6-4cd7-b6bf-952669ace9fb/download/ltccitationnarratives19982017.csv
- CDPH SEA Data Dictionary:
  https://data.chhs.ca.gov/dataset/1e1e2904-1bfb-448c-97e1-cf3e228c9159/resource/e38dd6e0-31a6-46f8-a9a0-8f48a410ee74/download/2022_12_05_data-dictionary-healthcare-facility-state-enforcement-actions.xlsx
- CKAN API endpoint (for SEA discovery):
  https://data.chhs.ca.gov/api/3/action/package_show?id=1e1e2904-1bfb-448c-97e1-cf3e228c9159
- Cal Health Find (reference only, not bulk-downloadable):
  https://www.cdph.ca.gov/Programs/CHCQ/LCP/CalHealthFind/pages/home.aspx

### Origin

- Phase 1 foundation plan:
  [docs/plans/2026-04-10-feat-care-facility-intelligence-platform-plan.md](../../plans/2026-04-10-feat-care-facility-intelligence-platform-plan.md)
  — Phase 3 section (originally assumed CMS Care Compare HH/hospice
  CSVs; this design corrects the source assumptions)
