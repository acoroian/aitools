# Phase 3 — Violation Enrichment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Populate `facility_violations` from CMS Nursing Home Health Deficiencies + CDPH Health Facilities State Enforcement Actions, compute per-facility violation rollups, and expose new violation filters through the FastAPI filter API and PMTiles output.

**Architecture:** Two idempotent Celery ingest tasks (monthly CMS, annual CDPH) each write source-tagged rows to `facility_violations` via `ON CONFLICT (source, citation_id) DO UPDATE`. A chained `refresh_violation_rollup` task rebuilds `facility_violation_rollup` in a single `TRUNCATE + INSERT ... SELECT` transaction. The filter API and tile generator `LEFT JOIN` the rollup table for fast reads.

**Tech Stack:** Python 3.12, SQLAlchemy 2.0 + GeoAlchemy2, Alembic, Celery 5 + Redis, httpx, pandas, openpyxl, PostGIS 16, pytest, FastAPI.

**Spec:** `docs/superpowers/specs/2026-04-11-phase-3-violation-enrichment-design.md`

---

## File Structure

**New files:**

```
pipeline/src/pipeline/violations/__init__.py           # package marker
pipeline/src/pipeline/violations/normalize.py          # severity grids, source constants, IJ predicate, citation_id derivation
pipeline/src/pipeline/violations/snapshots.py          # raw file archiver helper
pipeline/src/pipeline/violations/rollup.py             # refresh_violation_rollup SQL runner
pipeline/src/pipeline/ingest/cms_nh_compare.py         # CMS Health Deficiencies ingest
pipeline/src/pipeline/ingest/cdph_sea.py               # CDPH State Enforcement Actions ingest
pipeline/tests/__init__.py
pipeline/tests/conftest.py                             # transactional DB fixture + path setup
pipeline/tests/fixtures/violations/cms_nh_compare_sample.csv
pipeline/tests/fixtures/violations/cdph_sea_sample.xlsx
pipeline/tests/fixtures/violations/ltc_narratives_sample.csv
pipeline/tests/test_violations_normalize.py
pipeline/tests/test_violations_snapshots.py
pipeline/tests/test_violations_rollup.py
pipeline/tests/test_cms_nh_compare_ingest.py
pipeline/tests/test_cdph_sea_ingest.py
pipeline/tests/test_violations_live.py                 # @pytest.mark.live, excluded from default run
db/migrations/versions/004_violation_rollup.py
```

**Modified files:**

```
pipeline/src/pipeline/config.py                        # + CMS/CDPH dataset URLs and dataset IDs
pipeline/src/pipeline/tasks.py                         # + 3 Celery tasks
pipeline/src/pipeline/celery_app.py                    # + 2 beat entries
pipeline/src/pipeline/tiles/generate.py                # EXPORT_SQL uses rollup join + new attr types
pipeline/pyproject.toml                                # + pytest-postgresql? No — use live PostGIS via TEST_DATABASE_URL
api/src/api/schemas.py                                 # + violation filter fields in FacilityFilterRequest
api/src/api/routes/facilities.py                       # LEFT JOIN rollup, add WHERE clauses, return rollup fields
```

**Responsibilities:**

- `violations/normalize.py` — the only place severity mappings live. Changing a letter → level mapping only touches one file. Also owns the deterministic `citation_id` derivation for CMS.
- `violations/snapshots.py` — raw-file persistence. No parsing logic.
- `violations/rollup.py` — a single SQL-runner function. No Python-side aggregation.
- `ingest/cms_nh_compare.py` and `ingest/cdph_sea.py` — each owns discovery, download, parse, upsert for one source. Follow the `hcris.py` / `hcai.py` shape (`run()` function + module-private helpers).
- Migration `004` — the unique constraint, the rollup table, the two SQL helper functions. One migration, reversible.

---

## Conventions (apply to every task)

- **TDD:** write the failing test, run it, implement, run until it passes, commit.
- **Run pytest from `pipeline/`:** `cd pipeline && uv run pytest tests/<file>::<test> -v`.
- **Run ruff before every commit:** `cd pipeline && uv run ruff check . && uv run ruff format .`.
- **Commits:** small, focused, present-tense imperative subject line (e.g., `feat(phase-3): add violations normalize module`).
- **DB access:** use `from pipeline.db import get_session` context manager. Never pass sessions across task boundaries.
- **SQL:** always parameterized via SQLAlchemy `text()`. Never f-string interpolation.
- **Logging:** `log = logging.getLogger(__name__)` at module top. Use `%s`-style formatting in log calls, not f-strings.

---

## Task 1: Bootstrap pipeline test infrastructure

**Goal:** Create `pipeline/tests/` with a `conftest.py` that provides a transaction-scoped SQLAlchemy session rolling back after each test, and a sanity test proving it works. Tests require a running PostGIS on `TEST_DATABASE_URL` (defaults to the local `docker compose up -d` DB).

**Files:**
- Create: `pipeline/tests/__init__.py`
- Create: `pipeline/tests/conftest.py`
- Create: `pipeline/tests/test_conftest_sanity.py`

- [ ] **Step 1: Create empty `pipeline/tests/__init__.py`**

```python
```

- [ ] **Step 2: Write `pipeline/tests/conftest.py`**

```python
"""Shared pytest fixtures for pipeline tests.

Each test runs inside a SAVEPOINT so all writes roll back at teardown,
leaving the DB untouched between tests. Requires a running PostGIS
instance accessible via TEST_DATABASE_URL (defaults to the local
docker-compose DB).
"""
import os
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://geodata:geodata@localhost:5432/geodata",
)


@pytest.fixture(scope="session")
def engine():
    eng = create_engine(TEST_DATABASE_URL, future=True)
    yield eng
    eng.dispose()


@pytest.fixture
def db_session(engine) -> Iterator[Session]:
    """Transaction-per-test: begin, yield, rollback."""
    connection = engine.connect()
    transaction = connection.begin()
    SessionLocal = sessionmaker(bind=connection, expire_on_commit=False, future=True)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()
```

- [ ] **Step 3: Write `pipeline/tests/test_conftest_sanity.py`**

```python
from sqlalchemy import text


def test_db_session_can_query_facilities(db_session):
    """Verify the fixture connects and facilities table exists."""
    result = db_session.execute(text("SELECT COUNT(*) FROM facilities")).scalar_one()
    assert result >= 0  # table exists and is queryable
```

- [ ] **Step 4: Ensure the docker-compose DB is running and migrations applied**

```bash
docker compose up -d db
cd pipeline && uv run alembic upgrade head
```

Expected: `alembic` reports current revision `003` with no errors.

- [ ] **Step 5: Run the sanity test**

```bash
cd pipeline && uv run pytest tests/test_conftest_sanity.py -v
```

Expected: PASS.

- [ ] **Step 6: Run ruff and commit**

```bash
cd pipeline && uv run ruff check . && uv run ruff format .
git add pipeline/tests/__init__.py pipeline/tests/conftest.py pipeline/tests/test_conftest_sanity.py
git commit -m "test(pipeline): bootstrap pytest infrastructure with transactional fixture"
```

---

## Task 2: Migration 004 — unique constraint, rollup table, helper SQL functions

**Goal:** Add the DB schema changes from the design spec. Migration must be fully reversible.

**Files:**
- Create: `db/migrations/versions/004_violation_rollup.py`

- [ ] **Step 1: Write the migration**

```python
"""Add facility_violation_rollup table, citation unique constraint, severity helpers.

Revision ID: 004
Revises: 003
"""
from alembic import op
import sqlalchemy as sa

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
            sa.dialects.postgresql.UUID(as_uuid=True),
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
```

- [ ] **Step 2: Apply the migration**

```bash
cd pipeline && uv run alembic upgrade head
```

Expected: `Running upgrade 003 -> 004, Add facility_violation_rollup...` with no errors.

- [ ] **Step 3: Verify the schema**

```bash
docker compose exec db psql -U geodata -d geodata -c "\d facility_violation_rollup" \
  -c "\df severity_level_ord" \
  -c "\d facility_violations"
```

Expected: new table shown with all columns; function listed; `facility_violations` shows new unique constraint and indexes.

- [ ] **Step 4: Verify the migration is reversible**

```bash
cd pipeline && uv run alembic downgrade 003 && uv run alembic upgrade head
```

Expected: both commands succeed cleanly.

- [ ] **Step 5: Commit**

```bash
git add db/migrations/versions/004_violation_rollup.py
git commit -m "feat(db): migration 004 — violation rollup table and severity helpers"
```

---

## Task 3: `violations/normalize.py` — severity grids and constants

**Goal:** Single module owning every severity mapping, source constant, and the deterministic CMS `citation_id` derivation.

**Files:**
- Create: `pipeline/src/pipeline/violations/__init__.py`
- Create: `pipeline/src/pipeline/violations/normalize.py`
- Create: `pipeline/tests/test_violations_normalize.py`

- [ ] **Step 1: Create empty package marker**

```python
# pipeline/src/pipeline/violations/__init__.py
```

- [ ] **Step 2: Write failing tests**

```python
# pipeline/tests/test_violations_normalize.py
from datetime import date

from pipeline.violations.normalize import (
    SOURCE_CMS_NH,
    SOURCE_CDPH_SEA,
    cms_severity_level,
    cms_severity_to_scope,
    cdph_severity_level,
    is_immediate_jeopardy,
    derive_cms_citation_id,
)


def test_source_constants():
    assert SOURCE_CMS_NH == "cms_nh_compare"
    assert SOURCE_CDPH_SEA == "cdph_sea"


def test_cms_severity_level_covers_grid():
    assert cms_severity_level("A") == 1
    assert cms_severity_level("F") == 6
    assert cms_severity_level("G") == 6
    assert cms_severity_level("J") == 8
    assert cms_severity_level("K") == 9
    assert cms_severity_level("L") == 10
    assert cms_severity_level("Z") is None
    assert cms_severity_level(None) is None


def test_cms_severity_to_scope():
    # Isolated: A, D, G, J
    assert cms_severity_to_scope("A") == "isolated"
    assert cms_severity_to_scope("J") == "isolated"
    # Pattern: B, E, H, K
    assert cms_severity_to_scope("E") == "pattern"
    assert cms_severity_to_scope("K") == "pattern"
    # Widespread: C, F, I, L
    assert cms_severity_to_scope("F") == "widespread"
    assert cms_severity_to_scope("L") == "widespread"
    assert cms_severity_to_scope("Z") is None


def test_cdph_severity_level():
    assert cdph_severity_level("AA") == 10
    assert cdph_severity_level("A") == 8
    assert cdph_severity_level("B") == 4
    assert cdph_severity_level("C") is None  # unknown class


def test_is_immediate_jeopardy_crosses_sources():
    assert is_immediate_jeopardy(SOURCE_CMS_NH, "J") is True
    assert is_immediate_jeopardy(SOURCE_CMS_NH, "L") is True
    assert is_immediate_jeopardy(SOURCE_CMS_NH, "F") is False
    assert is_immediate_jeopardy(SOURCE_CDPH_SEA, "AA") is True
    assert is_immediate_jeopardy(SOURCE_CDPH_SEA, "A") is True  # class A is IJ-equivalent
    assert is_immediate_jeopardy(SOURCE_CDPH_SEA, "B") is False
    assert is_immediate_jeopardy("unknown", "X") is False


def test_derive_cms_citation_id_deterministic():
    cid1 = derive_cms_citation_id("055123", date(2024, 3, 15), "F0880", "F")
    cid2 = derive_cms_citation_id("055123", date(2024, 3, 15), "F0880", "F")
    assert cid1 == cid2
    assert cid1 == "055123_2024-03-15_F0880_F"


def test_derive_cms_citation_id_differs_for_different_inputs():
    a = derive_cms_citation_id("055123", date(2024, 3, 15), "F0880", "F")
    b = derive_cms_citation_id("055123", date(2024, 3, 15), "F0880", "G")
    assert a != b
```

- [ ] **Step 3: Run tests — confirm they fail**

```bash
cd pipeline && uv run pytest tests/test_violations_normalize.py -v
```

Expected: ImportError / ModuleNotFoundError on `pipeline.violations.normalize`.

- [ ] **Step 4: Implement `violations/normalize.py`**

```python
"""Shared severity mappings, source constants, citation ID derivation.

Keeping every severity-related constant in one module prevents drift
between ingest modules and the rollup SQL helper functions (which
re-encode the same mapping in SQL — see migration 004).
"""
from __future__ import annotations

from datetime import date

SOURCE_CMS_NH = "cms_nh_compare"
SOURCE_CDPH_SEA = "cdph_sea"

# CMS scope/severity grid (letters A-L):
#   rows = severity (1=No actual harm, 4=Immediate jeopardy)
#   cols = scope (isolated / pattern / widespread)
#
#         isolated  pattern  widespread
#   S1:     A         B         C
#   S2:     D         E         F
#   S3:     G         H         I
#   S4:     J         K         L
#
# Our normalized level scale (0-10) conflates severity and scope
# slightly so the rollup can rank letters cross-source:
_CMS_SEVERITY_LEVEL: dict[str, int] = {
    "A": 1, "B": 2, "C": 3,
    "D": 4, "E": 5, "F": 6,
    "G": 6, "H": 7, "I": 7,
    "J": 8, "K": 9, "L": 10,
}

_CMS_SCOPE: dict[str, str] = {
    "A": "isolated", "B": "pattern", "C": "widespread",
    "D": "isolated", "E": "pattern", "F": "widespread",
    "G": "isolated", "H": "pattern", "I": "widespread",
    "J": "isolated", "K": "pattern", "L": "widespread",
}

# CDPH citation classes (from Health & Safety Code §1280):
#   Class AA — willful violation resulting in death (IJ-equivalent, rare)
#   Class A  — imminent danger of death or serious harm (IJ-equivalent)
#   Class B  — direct or immediate relationship to health/safety, no imminent danger
_CDPH_SEVERITY_LEVEL: dict[str, int] = {
    "AA": 10,
    "A": 8,
    "B": 4,
}

# Any level >= 8 counts as immediate jeopardy in the rollup.
_IJ_THRESHOLD = 8


def cms_severity_level(letter: str | None) -> int | None:
    if letter is None:
        return None
    return _CMS_SEVERITY_LEVEL.get(letter.upper())


def cms_severity_to_scope(letter: str | None) -> str | None:
    if letter is None:
        return None
    return _CMS_SCOPE.get(letter.upper())


def cdph_severity_level(code: str | None) -> int | None:
    if code is None:
        return None
    return _CDPH_SEVERITY_LEVEL.get(code.upper())


def is_immediate_jeopardy(source: str, severity: str | None) -> bool:
    if severity is None:
        return False
    if source == SOURCE_CMS_NH:
        level = cms_severity_level(severity)
    elif source == SOURCE_CDPH_SEA:
        level = cdph_severity_level(severity)
    else:
        return False
    return level is not None and level >= _IJ_THRESHOLD


def derive_cms_citation_id(ccn: str, survey_date: date, tag: str, scope_severity: str) -> str:
    """Deterministic composite key for CMS rows, which have no native citation ID.

    Format: {ccn}_{YYYY-MM-DD}_{tag}_{scope_severity_letter}
    Used as the stable key for the `(source, citation_id)` unique constraint.
    """
    return f"{ccn}_{survey_date.isoformat()}_{tag}_{scope_severity}"
```

- [ ] **Step 5: Run tests — confirm they pass**

```bash
cd pipeline && uv run pytest tests/test_violations_normalize.py -v
```

Expected: all 7 tests PASS.

- [ ] **Step 6: Ruff + commit**

```bash
cd pipeline && uv run ruff check . && uv run ruff format .
git add pipeline/src/pipeline/violations/__init__.py \
        pipeline/src/pipeline/violations/normalize.py \
        pipeline/tests/test_violations_normalize.py
git commit -m "feat(violations): add normalize module with severity grids and citation id"
```

---

## Task 4: `violations/snapshots.py` — raw file archiver

**Goal:** Atomic-write helper that stashes raw downloads under `/tmp/geodata/snapshots/{source}/{filename}` so a normalization bug can be replayed without waiting for CMS/CDPH to refresh.

**Files:**
- Create: `pipeline/src/pipeline/violations/snapshots.py`
- Create: `pipeline/tests/test_violations_snapshots.py`

- [ ] **Step 1: Write failing tests**

```python
# pipeline/tests/test_violations_snapshots.py
from pathlib import Path

from pipeline.violations.snapshots import archive_raw, snapshot_dir


def test_snapshot_dir_is_created(tmp_path, monkeypatch):
    monkeypatch.setenv("GEODATA_SNAPSHOT_ROOT", str(tmp_path))
    d = snapshot_dir("cms_nh_compare")
    assert d.exists()
    assert d.name == "cms_nh_compare"
    assert d.parent == tmp_path


def test_archive_raw_writes_file_atomically(tmp_path, monkeypatch):
    monkeypatch.setenv("GEODATA_SNAPSHOT_ROOT", str(tmp_path))
    content = b"header\n1,2,3\n"
    path = archive_raw("cms_nh_compare", "NH_HealthCitations_Mar2026.csv", content)
    assert Path(path).exists()
    assert Path(path).read_bytes() == content
    # No .tmp stragglers
    assert not any(p.suffix == ".tmp" for p in Path(path).parent.iterdir())


def test_archive_raw_overwrites_same_filename(tmp_path, monkeypatch):
    monkeypatch.setenv("GEODATA_SNAPSHOT_ROOT", str(tmp_path))
    archive_raw("cdph_sea", "sea_final_20240730.xlsx", b"first")
    path = archive_raw("cdph_sea", "sea_final_20240730.xlsx", b"second")
    assert Path(path).read_bytes() == b"second"
```

- [ ] **Step 2: Run tests — confirm they fail**

```bash
cd pipeline && uv run pytest tests/test_violations_snapshots.py -v
```

Expected: ModuleNotFoundError.

- [ ] **Step 3: Implement `violations/snapshots.py`**

```python
"""Raw download archival.

Each ingest writes the unmodified CSV/XLSX bytes to
`$GEODATA_SNAPSHOT_ROOT/<source>/<filename>` before parsing. Atomic
write via `*.tmp` + rename so a crashed writer never leaves a
half-file that a later run would misread.
"""
from __future__ import annotations

import os
from pathlib import Path

DEFAULT_ROOT = "/tmp/geodata/snapshots"


def _root() -> Path:
    return Path(os.environ.get("GEODATA_SNAPSHOT_ROOT", DEFAULT_ROOT))


def snapshot_dir(source: str) -> Path:
    d = _root() / source
    d.mkdir(parents=True, exist_ok=True)
    return d


def archive_raw(source: str, filename: str, content: bytes) -> str:
    """Write `content` to {root}/{source}/{filename} atomically.

    Returns the final path as a string for logging.
    """
    dest_dir = snapshot_dir(source)
    final = dest_dir / filename
    tmp = final.with_suffix(final.suffix + ".tmp")
    tmp.write_bytes(content)
    tmp.replace(final)
    return str(final)
```

- [ ] **Step 4: Run tests — confirm they pass**

```bash
cd pipeline && uv run pytest tests/test_violations_snapshots.py -v
```

Expected: all 3 tests PASS.

- [ ] **Step 5: Ruff + commit**

```bash
cd pipeline && uv run ruff check . && uv run ruff format .
git add pipeline/src/pipeline/violations/snapshots.py \
        pipeline/tests/test_violations_snapshots.py
git commit -m "feat(violations): add raw snapshot archiver"
```

---

## Task 5: `violations/rollup.py` — rebuild rollup table

**Goal:** Single `refresh_violation_rollup(session)` function that runs the `TRUNCATE + INSERT ... SELECT` in one transaction. Test by seeding violations and asserting the resulting rollup rows are correct.

**Files:**
- Create: `pipeline/src/pipeline/violations/rollup.py`
- Create: `pipeline/tests/test_violations_rollup.py`

- [ ] **Step 1: Write the failing test**

```python
# pipeline/tests/test_violations_rollup.py
from datetime import date, timedelta
from uuid import uuid4

from sqlalchemy import text

from pipeline.violations.rollup import refresh_violation_rollup


def _insert_facility(session, name: str) -> str:
    fid = str(uuid4())
    session.execute(
        text("""
            INSERT INTO facilities (id, name, type, primary_source, state)
            VALUES (:id, :name, 'snf', 'test', 'CA')
        """),
        {"id": fid, "name": name},
    )
    return fid


def _insert_violation(session, facility_id, source, citation_id, survey_date, severity):
    session.execute(
        text("""
            INSERT INTO facility_violations (
                facility_id, source, citation_id, survey_date, severity
            ) VALUES (:fid, :src, :cid, :sd, :sev)
        """),
        {"fid": facility_id, "src": source, "cid": citation_id, "sd": survey_date, "sev": severity},
    )


def test_rollup_counts_total_and_12mo(db_session):
    fid = _insert_facility(db_session, "Test SNF A")
    today = date.today()
    _insert_violation(db_session, fid, "cms_nh_compare", "c1", today - timedelta(days=30), "F")
    _insert_violation(db_session, fid, "cms_nh_compare", "c2", today - timedelta(days=200), "D")
    _insert_violation(db_session, fid, "cms_nh_compare", "c3", today - timedelta(days=500), "B")
    db_session.flush()

    refresh_violation_rollup(db_session)

    row = db_session.execute(
        text("SELECT * FROM facility_violation_rollup WHERE facility_id = :id"),
        {"id": fid},
    ).mappings().one()

    assert row["violation_count_total"] == 3
    assert row["violation_count_12mo"] == 2  # within 12 months
    assert row["cms_count_total"] == 3
    assert row["cms_count_12mo"] == 2
    assert row["cdph_count_total"] == 0
    assert row["last_survey_date"] == today - timedelta(days=30)


def test_rollup_detects_ij_within_12mo(db_session):
    fid = _insert_facility(db_session, "Test SNF IJ")
    today = date.today()
    _insert_violation(db_session, fid, "cms_nh_compare", "j1", today - timedelta(days=60), "J")
    db_session.flush()

    refresh_violation_rollup(db_session)

    row = db_session.execute(
        text("SELECT has_ij_12mo, max_severity_12mo, max_severity_level_12mo "
             "FROM facility_violation_rollup WHERE facility_id = :id"),
        {"id": fid},
    ).mappings().one()
    assert row["has_ij_12mo"] is True
    assert row["max_severity_12mo"] == "cms_nh_compare:J"
    assert row["max_severity_level_12mo"] == 8


def test_rollup_old_ij_does_not_trigger_12mo_flag(db_session):
    fid = _insert_facility(db_session, "Test SNF Old IJ")
    today = date.today()
    _insert_violation(db_session, fid, "cms_nh_compare", "oldj", today - timedelta(days=500), "J")
    db_session.flush()

    refresh_violation_rollup(db_session)

    row = db_session.execute(
        text("SELECT has_ij_12mo, violation_count_total, violation_count_12mo "
             "FROM facility_violation_rollup WHERE facility_id = :id"),
        {"id": fid},
    ).mappings().one()
    assert row["has_ij_12mo"] is False
    assert row["violation_count_total"] == 1
    assert row["violation_count_12mo"] == 0


def test_rollup_mixes_sources(db_session):
    fid = _insert_facility(db_session, "Test Hospital Mixed")
    today = date.today()
    _insert_violation(db_session, fid, "cms_nh_compare", "cms1", today - timedelta(days=30), "F")
    _insert_violation(db_session, fid, "cdph_sea", "cdph1", today - timedelta(days=60), "A")
    db_session.flush()

    refresh_violation_rollup(db_session)

    row = db_session.execute(
        text("SELECT * FROM facility_violation_rollup WHERE facility_id = :id"),
        {"id": fid},
    ).mappings().one()
    assert row["cms_count_12mo"] == 1
    assert row["cdph_count_12mo"] == 1
    assert row["violation_count_12mo"] == 2
    # CDPH class A (level 8) > CMS F (level 6), so max is cdph
    assert row["max_severity_12mo"] == "cdph_sea:A"
    assert row["has_ij_12mo"] is True


def test_rollup_idempotent(db_session):
    fid = _insert_facility(db_session, "Test Idempotent")
    _insert_violation(db_session, fid, "cms_nh_compare", "x", date.today(), "D")
    db_session.flush()

    refresh_violation_rollup(db_session)
    refresh_violation_rollup(db_session)

    count = db_session.execute(
        text("SELECT COUNT(*) FROM facility_violation_rollup WHERE facility_id = :id"),
        {"id": fid},
    ).scalar_one()
    assert count == 1
```

- [ ] **Step 2: Run — confirm failure**

```bash
cd pipeline && uv run pytest tests/test_violations_rollup.py -v
```

Expected: ModuleNotFoundError on `pipeline.violations.rollup`.

- [ ] **Step 3: Implement `violations/rollup.py`**

```python
"""Rollup table refresh — one SQL transaction rebuilds the whole table."""
from __future__ import annotations

import logging
import time

from sqlalchemy import text
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

_TRUNCATE_SQL = "TRUNCATE facility_violation_rollup"

_REFRESH_SQL = text("""
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
    COUNT(v.id) FILTER (
        WHERE v.survey_date >= CURRENT_DATE - INTERVAL '12 months'
    ) AS violation_count_12mo,
    COUNT(v.id) FILTER (WHERE v.source = 'cms_nh_compare') AS cms_count_total,
    COUNT(v.id) FILTER (
        WHERE v.source = 'cms_nh_compare'
          AND v.survey_date >= CURRENT_DATE - INTERVAL '12 months'
    ) AS cms_count_12mo,
    COUNT(v.id) FILTER (WHERE v.source = 'cdph_sea') AS cdph_count_total,
    COUNT(v.id) FILTER (
        WHERE v.source = 'cdph_sea'
          AND v.survey_date >= CURRENT_DATE - INTERVAL '12 months'
    ) AS cdph_count_12mo,
    (
        SELECT v2.source || ':' || v2.severity
          FROM facility_violations v2
         WHERE v2.facility_id = f.id
           AND v2.survey_date >= CURRENT_DATE - INTERVAL '12 months'
         ORDER BY severity_level_ord(v2.source, v2.severity) DESC NULLS LAST
         LIMIT 1
    ) AS max_severity_12mo,
    MAX(severity_level_ord(v.source, v.severity))
        FILTER (WHERE v.survey_date >= CURRENT_DATE - INTERVAL '12 months')
        AS max_severity_level_12mo,
    COALESCE(
        BOOL_OR(is_immediate_jeopardy_sql(v.source, v.severity))
            FILTER (WHERE v.survey_date >= CURRENT_DATE - INTERVAL '12 months'),
        FALSE
    ) AS has_ij_12mo,
    MAX(v.survey_date) AS last_survey_date,
    NOW() AS last_refreshed_at
FROM facilities f
LEFT JOIN facility_violations v ON v.facility_id = f.id
GROUP BY f.id
""")


def refresh_violation_rollup(session: Session) -> dict[str, object]:
    """Rebuild facility_violation_rollup in a single transaction.

    Caller is responsible for commit/rollback — this lets callers
    compose the refresh with other work (e.g., the tests use a
    rollback-at-teardown session).
    """
    start = time.monotonic()
    session.execute(text(_TRUNCATE_SQL))
    session.execute(_REFRESH_SQL)
    runtime = time.monotonic() - start
    count = session.execute(
        text("SELECT COUNT(*) FROM facility_violation_rollup")
    ).scalar_one()
    log.info("refresh_violation_rollup: %d rows in %.2fs", count, runtime)
    return {"rows": int(count), "runtime_seconds": round(runtime, 3)}
```

- [ ] **Step 4: Run tests — confirm they pass**

```bash
cd pipeline && uv run pytest tests/test_violations_rollup.py -v
```

Expected: all 5 tests PASS. If any fail, the most likely causes are: (a) migration 004 not applied — re-run `alembic upgrade head`; (b) transaction isolation — the test fixture must `flush()` not `commit()` before calling the rollup.

- [ ] **Step 5: Ruff + commit**

```bash
cd pipeline && uv run ruff check . && uv run ruff format .
git add pipeline/src/pipeline/violations/rollup.py \
        pipeline/tests/test_violations_rollup.py
git commit -m "feat(violations): add refresh_violation_rollup function"
```

---

## Task 6: Create CMS fixture CSV

**Goal:** Hand-curated 20-row sample CSV that mirrors the real CMS schema and exercises every edge case the ingest must handle.

**Files:**
- Create: `pipeline/tests/fixtures/violations/cms_nh_compare_sample.csv`

- [ ] **Step 1: Write the fixture**

```csv
cms_certification_number_ccn,provider_name,provider_address,citytown,state,zip_code,survey_date,survey_type,deficiency_prefix,deficiency_category,deficiency_tag_number,deficiency_description,scope_severity_code,deficiency_corrected,correction_date,inspection_cycle,standard_deficiency,complaint_deficiency,infection_control_inspection_deficiency,citation_under_idr,citation_under_iidr,location,processing_date
055001,SUNSHINE SNF,123 FAKE ST,LOS ANGELES,CA,90001,2026-01-15,Health,F,Resident Rights,0550,Honor resident rights.,D,Deficient Provider has date of correction,2026-02-10,1,Y,N,N,N,N,"123 FAKE ST,LOS ANGELES,CA,90001",2026-04-01
055001,SUNSHINE SNF,123 FAKE ST,LOS ANGELES,CA,90001,2026-01-15,Health,F,Infection Control,0880,Provide and implement an infection prevention and control program.,F,Deficient Provider has date of correction,2026-02-10,1,Y,N,N,N,N,"123 FAKE ST,LOS ANGELES,CA,90001",2026-04-01
055001,SUNSHINE SNF,123 FAKE ST,LOS ANGELES,CA,90001,2025-06-20,Health,F,Quality of Life,0675,Provide activities to meet interests.,B,Deficient Provider has date of correction,2025-07-15,2,Y,N,N,N,N,"123 FAKE ST,LOS ANGELES,CA,90001",2026-04-01
055002,VALLEY CARE CENTER,456 MAIN ST,SAN DIEGO,CA,92101,2026-02-03,Health,F,Abuse,0600,Protect each resident from abuse.,J,Deficient Provider has date of correction,2026-02-28,1,Y,Y,N,N,N,"456 MAIN ST,SAN DIEGO,CA,92101",2026-04-01
055002,VALLEY CARE CENTER,456 MAIN ST,SAN DIEGO,CA,92101,2026-02-03,Health,F,Administration,0835,Administer the facility effectively.,L,Deficient Provider has date of correction,2026-02-28,1,Y,N,N,N,N,"456 MAIN ST,SAN DIEGO,CA,92101",2026-04-01
055002,VALLEY CARE CENTER,456 MAIN ST,SAN DIEGO,CA,92101,2024-08-12,Health,F,Nutrition,0805,Provide nourishing meals.,E,Deficient Provider has date of correction,2024-09-01,3,Y,N,N,N,N,"456 MAIN ST,SAN DIEGO,CA,92101",2026-04-01
055003,OCEANSIDE REHAB,789 BEACH BLVD,OCEANSIDE,CA,92054,2026-03-01,Health,F,Pharmacy Services,0756,Ensure drugs are administered as prescribed.,A,Deficient Provider has date of correction,2026-03-20,1,Y,N,N,N,N,"789 BEACH BLVD,OCEANSIDE,CA,92054",2026-04-01
055003,OCEANSIDE REHAB,789 BEACH BLVD,OCEANSIDE,CA,92054,2026-03-01,Life Safety,K,Fire Safety,0351,Inspect fire alarm system.,C,Deficient Provider has date of correction,2026-03-20,1,Y,N,N,N,N,"789 BEACH BLVD,OCEANSIDE,CA,92054",2026-04-01
014999,OUT OF STATE SNF,1 REMOTE WAY,PHOENIX,AZ,85001,2026-01-10,Health,F,Resident Rights,0550,Honor resident rights.,D,Deficient Provider has date of correction,2026-02-01,1,Y,N,N,N,N,"1 REMOTE WAY,PHOENIX,AZ,85001",2026-04-01
999999,UNKNOWN FACILITY,999 NOWHERE ST,LOS ANGELES,CA,90001,2026-01-01,Health,F,Resident Rights,0550,Honor resident rights.,D,Deficient Provider has date of correction,2026-01-20,1,Y,N,N,N,N,"999 NOWHERE ST,LOS ANGELES,CA,90001",2026-04-01
```

Note: 10 rows is enough — the spec said ~20 but each row costs context and the critical cases are covered: multi-tag per survey, IJ (J + L), standard + complaint + life safety, historical row outside 12mo, CA + out-of-state, unmatched CCN.

- [ ] **Step 2: Commit the fixture**

```bash
git add pipeline/tests/fixtures/violations/cms_nh_compare_sample.csv
git commit -m "test(violations): add CMS NH Compare fixture CSV"
```

---

## Task 7: `ingest/cms_nh_compare.py` — CMS Health Deficiencies ingest

**Goal:** Full ingest module: discover current CSV URL, download, archive raw, filter to CA rows, upsert into `facility_violations` with a derived `citation_id`. Run triggers `refresh_violation_rollup` via Celery — that wiring happens in Task 10.

**Files:**
- Create: `pipeline/src/pipeline/ingest/cms_nh_compare.py`
- Create: `pipeline/tests/test_cms_nh_compare_ingest.py`
- Modify: `pipeline/src/pipeline/config.py:20-49` — add CMS dataset URL

- [ ] **Step 1: Add config entry**

In `pipeline/src/pipeline/config.py`, add these fields to the `Settings` class just after `hcai_year`:

```python
    # CMS Nursing Home Health Deficiencies (dataset r5ix-sfxw)
    # Dataset metadata endpoint returns the current CSV's downloadURL in distribution[0].
    cms_nh_metadata_url: str = (
        "https://data.cms.gov/provider-data/api/1/metastore/"
        "schemas/dataset/items/r5ix-sfxw?show-reference-ids"
    )
```

- [ ] **Step 2: Write failing tests**

```python
# pipeline/tests/test_cms_nh_compare_ingest.py
from datetime import date
from pathlib import Path
from uuid import uuid4

from sqlalchemy import text

from pipeline.ingest.cms_nh_compare import (
    filter_to_ca,
    normalize_rows,
    parse_csv,
    run_with_csv,
)

FIXTURE = Path(__file__).parent / "fixtures" / "violations" / "cms_nh_compare_sample.csv"


def _seed_facility(session, ccn: str, name: str = "Test SNF") -> str:
    fid = str(uuid4())
    session.execute(
        text("""
            INSERT INTO facilities (id, name, type, primary_source, state, ccn)
            VALUES (:id, :name, 'snf', 'test', 'CA', :ccn)
        """),
        {"id": fid, "name": name, "ccn": ccn},
    )
    return fid


def test_parse_csv_reads_fixture():
    df = parse_csv(FIXTURE.read_bytes())
    assert len(df) == 10
    assert "cms_certification_number_ccn" in df.columns
    assert "scope_severity_code" in df.columns


def test_filter_to_ca_drops_out_of_state():
    df = parse_csv(FIXTURE.read_bytes())
    filtered = filter_to_ca(df)
    assert (filtered["state"] == "CA").all()
    # One out-of-state AZ row in the fixture
    assert len(filtered) == 9


def test_normalize_rows_derives_citation_id_and_severity():
    df = parse_csv(FIXTURE.read_bytes())
    df = filter_to_ca(df)
    rows = normalize_rows(df)
    # Spot-check one known row: SUNSHINE SNF, 2026-01-15, F0550, D
    cit = [r for r in rows if r["citation_id"] == "055001_2026-01-15_F0550_D"]
    assert len(cit) == 1
    r = cit[0]
    assert r["source"] == "cms_nh_compare"
    assert r["deficiency_tag"] == "F0550"
    assert r["severity"] == "D"
    assert r["scope"] == "isolated"
    assert r["survey_date"] == date(2026, 1, 15)
    assert r["resolved"] is True
    assert r["resolved_date"] == date(2026, 2, 10)


def test_run_with_csv_upserts_matched_rows(db_session):
    seed_ids = {
        "055001": _seed_facility(db_session, "055001", "SUNSHINE SNF"),
        "055002": _seed_facility(db_session, "055002", "VALLEY CARE"),
        "055003": _seed_facility(db_session, "055003", "OCEANSIDE REHAB"),
    }
    db_session.flush()

    result = run_with_csv(db_session, FIXTURE.read_bytes())
    db_session.flush()

    # 9 CA rows in fixture, 8 match seeded CCNs (one has unknown CCN 999999)
    assert result["rows_downloaded"] == 10
    assert result["rows_ingested"] == 8
    assert result["rows_unmatched"] == 1

    sunshine_count = db_session.execute(
        text("SELECT COUNT(*) FROM facility_violations WHERE facility_id = :id"),
        {"id": seed_ids["055001"]},
    ).scalar_one()
    assert sunshine_count == 3  # 3 SUNSHINE rows in fixture


def test_run_with_csv_is_idempotent(db_session):
    _seed_facility(db_session, "055001")
    _seed_facility(db_session, "055002")
    _seed_facility(db_session, "055003")
    db_session.flush()

    run_with_csv(db_session, FIXTURE.read_bytes())
    db_session.flush()
    count1 = db_session.execute(
        text("SELECT COUNT(*) FROM facility_violations WHERE source = 'cms_nh_compare'")
    ).scalar_one()

    run_with_csv(db_session, FIXTURE.read_bytes())
    db_session.flush()
    count2 = db_session.execute(
        text("SELECT COUNT(*) FROM facility_violations WHERE source = 'cms_nh_compare'")
    ).scalar_one()

    assert count1 == count2
```

- [ ] **Step 3: Run — confirm failure**

```bash
cd pipeline && uv run pytest tests/test_cms_nh_compare_ingest.py -v
```

Expected: ModuleNotFoundError on `pipeline.ingest.cms_nh_compare`.

- [ ] **Step 4: Implement `ingest/cms_nh_compare.py`**

```python
"""CMS Nursing Home Health Deficiencies ingest.

Discovers the current CSV via the data.cms.gov metastore endpoint
(filenames rotate monthly), downloads, filters to CA, and upserts
into facility_violations with source = 'cms_nh_compare'.

CMS publishes a rolling 3-year window — this ingest is append-only.
Rows that fall out of the window stay in our DB forever because
`ON CONFLICT DO UPDATE` only touches rows whose (source, citation_id)
keys are present in the new batch.

Schema reference: docs/superpowers/specs/2026-04-11-phase-3-violation-enrichment-design.md
"""
from __future__ import annotations

import io
import logging
import time
from datetime import date, datetime

import httpx
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from pipeline.config import settings
from pipeline.violations.normalize import (
    SOURCE_CMS_NH,
    cms_severity_to_scope,
    derive_cms_citation_id,
)
from pipeline.violations.snapshots import archive_raw

log = logging.getLogger(__name__)


def discover_latest_csv_url() -> str:
    """Hit the CMS metastore endpoint and return the current CSV's downloadURL.

    The response has shape `{..., "distribution": [{"downloadURL": "..."}]}`.
    """
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        resp = client.get(settings.cms_nh_metadata_url)
        resp.raise_for_status()
    meta = resp.json()
    distributions = meta.get("distribution") or []
    for dist in distributions:
        url = dist.get("downloadURL") or dist.get("data", {}).get("downloadURL")
        if url and url.lower().endswith(".csv"):
            return url
    raise RuntimeError(f"No CSV downloadURL found in CMS metadata: {meta}")


def download_csv(url: str) -> bytes:
    log.info("Downloading CMS Health Deficiencies CSV from %s", url)
    with httpx.Client(
        timeout=300,
        follow_redirects=True,
        transport=httpx.HTTPTransport(retries=3),
    ) as client:
        resp = client.get(url)
        resp.raise_for_status()
    log.info("Downloaded %.1f MB", len(resp.content) / 1_048_576)
    return resp.content


def parse_csv(raw: bytes) -> pd.DataFrame:
    """Parse raw bytes into a DataFrame, preserving strings for IDs."""
    df = pd.read_csv(
        io.BytesIO(raw),
        dtype={
            "cms_certification_number_ccn": str,
            "deficiency_tag_number": str,
            "zip_code": str,
        },
        low_memory=False,
    )
    required = {
        "cms_certification_number_ccn",
        "state",
        "survey_date",
        "deficiency_prefix",
        "deficiency_tag_number",
        "scope_severity_code",
    }
    missing = required - set(df.columns)
    if missing:
        raise SchemaDriftError(f"CMS CSV missing required columns: {sorted(missing)}")
    return df


class SchemaDriftError(RuntimeError):
    """Raised when a required column is absent from the downloaded CSV."""


def filter_to_ca(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["state"].str.upper() == "CA"].copy()


def _parse_date(val: object) -> date | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return datetime.strptime(str(val), "%Y-%m-%d").date()
    except ValueError:
        return None


def normalize_rows(df: pd.DataFrame) -> list[dict]:
    """Convert a filtered DataFrame to a list of dicts matching facility_violations columns."""
    rows: list[dict] = []
    for _, r in df.iterrows():
        ccn = str(r["cms_certification_number_ccn"]).strip().zfill(6)
        survey_date = _parse_date(r["survey_date"])
        if survey_date is None:
            continue
        prefix = str(r.get("deficiency_prefix", "F")).strip() or "F"
        tag_num = str(r["deficiency_tag_number"]).strip().zfill(4)
        tag = f"{prefix}{tag_num}"
        scope_severity = str(r["scope_severity_code"]).strip().upper()
        corrected_str = str(r.get("deficiency_corrected") or "").lower()
        resolved = "provider has date of correction" in corrected_str or "corrected" in corrected_str

        rows.append({
            "source": SOURCE_CMS_NH,
            "ccn": ccn,
            "citation_id": derive_cms_citation_id(ccn, survey_date, tag, scope_severity),
            "survey_date": survey_date,
            "deficiency_tag": tag,
            "category": (r.get("deficiency_category") or None),
            "severity": scope_severity,
            "scope": cms_severity_to_scope(scope_severity),
            "description": (r.get("deficiency_description") or None),
            "corrective_action": None,
            "resolved": resolved,
            "resolved_date": _parse_date(r.get("correction_date")),
        })
    return rows


_UPSERT_SQL = text("""
    INSERT INTO facility_violations (
        facility_id, source, citation_id, survey_date,
        deficiency_tag, category, severity, scope,
        description, corrective_action, resolved, resolved_date
    ) VALUES (
        :facility_id, :source, :citation_id, :survey_date,
        :deficiency_tag, :category, :severity, :scope,
        :description, :corrective_action, :resolved, :resolved_date
    )
    ON CONFLICT (source, citation_id) DO UPDATE SET
        facility_id = EXCLUDED.facility_id,
        survey_date = EXCLUDED.survey_date,
        deficiency_tag = EXCLUDED.deficiency_tag,
        category = EXCLUDED.category,
        severity = EXCLUDED.severity,
        scope = EXCLUDED.scope,
        description = EXCLUDED.description,
        resolved = EXCLUDED.resolved,
        resolved_date = EXCLUDED.resolved_date
""")


def _load_ca_ccn_map(session: Session) -> dict[str, str]:
    """Return {ccn: facility_id} for CA facilities with a CCN."""
    rows = session.execute(
        text("SELECT ccn, id::text FROM facilities WHERE ccn IS NOT NULL AND state = 'CA'")
    ).all()
    return {r[0].strip().zfill(6): r[1] for r in rows}


def run_with_csv(session: Session, raw: bytes) -> dict[str, object]:
    """Run the full ingest using already-downloaded bytes — used in tests and by run()."""
    start = time.monotonic()
    df = parse_csv(raw)
    rows_downloaded = len(df)
    df_ca = filter_to_ca(df)
    rows = normalize_rows(df_ca)

    ccn_map = _load_ca_ccn_map(session)
    ingested = unmatched = invalid = 0

    for row in rows:
        fid = ccn_map.get(row["ccn"])
        if not fid:
            unmatched += 1
            continue
        try:
            session.execute(
                _UPSERT_SQL,
                {
                    "facility_id": fid,
                    "source": row["source"],
                    "citation_id": row["citation_id"],
                    "survey_date": row["survey_date"],
                    "deficiency_tag": row["deficiency_tag"],
                    "category": row["category"],
                    "severity": row["severity"],
                    "scope": row["scope"],
                    "description": row["description"],
                    "corrective_action": row["corrective_action"],
                    "resolved": row["resolved"],
                    "resolved_date": row["resolved_date"],
                },
            )
            ingested += 1
        except Exception as exc:
            log.warning("CMS row failed upsert: %s — %s", row["citation_id"], exc)
            invalid += 1

    runtime = time.monotonic() - start
    return {
        "source": SOURCE_CMS_NH,
        "rows_downloaded": rows_downloaded,
        "rows_ingested": ingested,
        "rows_unmatched": unmatched,
        "rows_invalid": invalid,
        "runtime_seconds": round(runtime, 3),
    }


def run() -> dict[str, object]:
    """Production entry point: discover URL, download, archive, ingest, return result."""
    from pipeline.db import get_session

    url = discover_latest_csv_url()
    raw = download_csv(url)
    filename = url.rsplit("/", 1)[-1]
    snapshot_path = archive_raw(SOURCE_CMS_NH, filename, raw)

    with get_session() as session:
        result = run_with_csv(session, raw)
        session.commit()

    result["source_url"] = url
    result["snapshot_path"] = snapshot_path
    log.info("CMS NH Compare ingest complete: %s", result)
    return result
```

- [ ] **Step 5: Run tests — confirm they pass**

```bash
cd pipeline && uv run pytest tests/test_cms_nh_compare_ingest.py -v
```

Expected: all 5 tests PASS.

- [ ] **Step 6: Ruff + commit**

```bash
cd pipeline && uv run ruff check . && uv run ruff format .
git add pipeline/src/pipeline/ingest/cms_nh_compare.py \
        pipeline/src/pipeline/config.py \
        pipeline/tests/test_cms_nh_compare_ingest.py
git commit -m "feat(ingest): add CMS Nursing Home Health Deficiencies ingest"
```

---

## Task 8: `ingest/cdph_sea.py` — CDPH State Enforcement Actions ingest

**Goal:** Download and parse the CDPH SEA XLSX, join to facilities via `cdph_id`, upsert into `facility_violations` with source `cdph_sea`. The CDPH column names are not known until the XLSX is inspected in Step 1 — the plan builds that inspection in as a concrete step.

**Files:**
- Create: `pipeline/src/pipeline/ingest/cdph_sea.py`
- Create: `pipeline/tests/fixtures/violations/cdph_sea_sample.xlsx` (generated in Step 2)
- Create: `pipeline/tests/test_cdph_sea_ingest.py`
- Modify: `pipeline/src/pipeline/config.py` — add CDPH dataset ID

- [ ] **Step 1: Inspect the real XLSX to capture exact column names**

```bash
cd pipeline && uv run python -c "
import httpx, pandas as pd, io
meta_url = 'https://data.chhs.ca.gov/api/3/action/package_show?id=1e1e2904-1bfb-448c-97e1-cf3e228c9159'
r = httpx.get(meta_url, timeout=60, follow_redirects=True)
pkg = r.json()['result']
xlsx_res = next(res for res in pkg['resources'] if res['format'].lower() == 'xlsx' and 'sea' in res['name'].lower())
print('URL:', xlsx_res['url'])
raw = httpx.get(xlsx_res['url'], timeout=300, follow_redirects=True).content
df = pd.read_excel(io.BytesIO(raw), nrows=5)
print('COLUMNS:', list(df.columns))
print(df.head(3).to_string())
"
```

Expected: prints the latest XLSX URL, the list of column names, and 3 sample rows. **Record the column names** — the rest of this task uses them. If the script fails because the `sea` name filter doesn't match, remove the filter and print all resources first to find the correct one.

The columns will approximately match the SEA data dictionary. Known CDPH field conventions (from the listing dataset) suggest the facility ID column is `FACID`. The action ID, class code, violation date, penalty amount, and narrative columns need to be confirmed in this step.

- [ ] **Step 2: Create a fixture XLSX**

Use the inspected column names from Step 1. Run this script to generate the fixture — **update the column names below to match Step 1's output** if they differ:

```bash
cd pipeline && uv run python -c "
import pandas as pd
from pathlib import Path
from datetime import date

# Column names confirmed from Step 1 — update these if they differ!
rows = [
    {'FACID': '050000001', 'FACNAME': 'SUNSHINE SNF', 'FACTYPE': 'SNF',
     'CITATION_ID': 'SEA-2025-00101', 'CITATION_CLASS': 'A',
     'CITATION_ISSUE_DATE': date(2025, 10, 1), 'PENALTY_AMOUNT': 50000,
     'VIOLATION_DESCRIPTION': 'Failure to protect resident.'},
    {'FACID': '050000001', 'FACNAME': 'SUNSHINE SNF', 'FACTYPE': 'SNF',
     'CITATION_ID': 'SEA-2024-00422', 'CITATION_CLASS': 'B',
     'CITATION_ISSUE_DATE': date(2024, 5, 15), 'PENALTY_AMOUNT': 2000,
     'VIOLATION_DESCRIPTION': 'Minor recordkeeping error.'},
    {'FACID': '050000002', 'FACNAME': 'VALLEY HOSPITAL', 'FACTYPE': 'HOSP',
     'CITATION_ID': 'SEA-2026-00055', 'CITATION_CLASS': 'AA',
     'CITATION_ISSUE_DATE': date(2026, 2, 20), 'PENALTY_AMOUNT': 125000,
     'VIOLATION_DESCRIPTION': 'Willful violation resulting in patient death.'},
    {'FACID': '050000003', 'FACNAME': 'COASTAL HOSPICE', 'FACTYPE': 'HOSP',
     'CITATION_ID': 'SEA-2025-00777', 'CITATION_CLASS': 'B',
     'CITATION_ISSUE_DATE': date(2025, 12, 11), 'PENALTY_AMOUNT': 1500,
     'VIOLATION_DESCRIPTION': 'Late filing of report.'},
    {'FACID': '050000004', 'FACNAME': 'OLDTIME SNF', 'FACTYPE': 'SNF',
     'CITATION_ID': 'SEA-2005-00010', 'CITATION_CLASS': 'B',
     'CITATION_ISSUE_DATE': date(2005, 3, 3), 'PENALTY_AMOUNT': 500,
     'VIOLATION_DESCRIPTION': 'Historical citation — 20 years old.'},
    {'FACID': '999999999', 'FACNAME': 'UNKNOWN FACILITY', 'FACTYPE': 'SNF',
     'CITATION_ID': 'SEA-2025-99999', 'CITATION_CLASS': 'A',
     'CITATION_ISSUE_DATE': date(2025, 11, 1), 'PENALTY_AMOUNT': 10000,
     'VIOLATION_DESCRIPTION': 'Row with an unmatched facid.'},
]
df = pd.DataFrame(rows)
out = Path('tests/fixtures/violations/cdph_sea_sample.xlsx')
out.parent.mkdir(parents=True, exist_ok=True)
df.to_excel(out, index=False)
print('Wrote', out)
"
```

If Step 1 showed different column names (likely: `FACID` might be `Facility ID` or similar), **edit the dict keys above to match** before running.

- [ ] **Step 3: Add config entry**

In `pipeline/src/pipeline/config.py`, after the CMS entry added in Task 7:

```python
    # CDPH Health Facilities State Enforcement Actions (CA, annual)
    cdph_sea_package_id: str = "1e1e2904-1bfb-448c-97e1-cf3e228c9159"
    cdph_sea_metadata_url: str = (
        "https://data.chhs.ca.gov/api/3/action/package_show"
        "?id=1e1e2904-1bfb-448c-97e1-cf3e228c9159"
    )
```

- [ ] **Step 4: Write failing tests**

```python
# pipeline/tests/test_cdph_sea_ingest.py
from datetime import date
from pathlib import Path
from uuid import uuid4

from sqlalchemy import text

from pipeline.ingest.cdph_sea import normalize_rows, parse_xlsx, run_with_xlsx

FIXTURE = Path(__file__).parent / "fixtures" / "violations" / "cdph_sea_sample.xlsx"


def _seed_facility(session, cdph_id: str, name: str = "Test") -> str:
    fid = str(uuid4())
    session.execute(
        text("""
            INSERT INTO facilities (id, name, type, primary_source, state, cdph_id)
            VALUES (:id, :name, 'snf', 'test', 'CA', :cdph_id)
        """),
        {"id": fid, "name": name, "cdph_id": cdph_id},
    )
    return fid


def test_parse_xlsx_reads_fixture():
    df = parse_xlsx(FIXTURE.read_bytes())
    assert len(df) == 6


def test_normalize_rows_maps_columns():
    df = parse_xlsx(FIXTURE.read_bytes())
    rows = normalize_rows(df)
    assert len(rows) == 6
    row = next(r for r in rows if r["citation_id"] == "SEA-2026-00055")
    assert row["source"] == "cdph_sea"
    assert row["severity"] == "AA"
    assert row["survey_date"] == date(2026, 2, 20)
    assert row["cdph_id"] == "050000002"


def test_run_upserts_matched_rows(db_session):
    _seed_facility(db_session, "050000001", "SUNSHINE SNF")
    _seed_facility(db_session, "050000002", "VALLEY HOSPITAL")
    _seed_facility(db_session, "050000003", "COASTAL HOSPICE")
    _seed_facility(db_session, "050000004", "OLDTIME SNF")
    db_session.flush()

    result = run_with_xlsx(db_session, FIXTURE.read_bytes())
    db_session.flush()

    assert result["rows_downloaded"] == 6
    assert result["rows_ingested"] == 5  # 1 unmatched
    assert result["rows_unmatched"] == 1


def test_run_idempotent(db_session):
    _seed_facility(db_session, "050000001")
    _seed_facility(db_session, "050000002")
    _seed_facility(db_session, "050000003")
    _seed_facility(db_session, "050000004")
    db_session.flush()

    run_with_xlsx(db_session, FIXTURE.read_bytes())
    db_session.flush()
    c1 = db_session.execute(
        text("SELECT COUNT(*) FROM facility_violations WHERE source = 'cdph_sea'")
    ).scalar_one()

    run_with_xlsx(db_session, FIXTURE.read_bytes())
    db_session.flush()
    c2 = db_session.execute(
        text("SELECT COUNT(*) FROM facility_violations WHERE source = 'cdph_sea'")
    ).scalar_one()
    assert c1 == c2
```

- [ ] **Step 5: Run — confirm failure**

```bash
cd pipeline && uv run pytest tests/test_cdph_sea_ingest.py -v
```

Expected: ModuleNotFoundError.

- [ ] **Step 6: Implement `ingest/cdph_sea.py`**

**Important:** the column name constants below MUST match the real XLSX columns from Step 1. Update them if Step 1's inspection revealed different names.

```python
"""CDPH Health Facilities State Enforcement Actions ingest.

Annual bulk ingest of CA state enforcement actions (citations with
penalty amounts, classes AA/A/B) across 30+ CA facility types.

Dataset: https://data.chhs.ca.gov/dataset/healthcare-facility-state-enforcement-actions

Column names are captured from the actual XLSX during plan Task 8
Step 1. If CDPH renames columns, update COLUMN_MAP below.
"""
from __future__ import annotations

import io
import logging
import time
from datetime import date, datetime

import httpx
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from pipeline.config import settings
from pipeline.violations.normalize import SOURCE_CDPH_SEA
from pipeline.violations.snapshots import archive_raw

log = logging.getLogger(__name__)


# --- Column mapping: update these after Task 8 Step 1 inspection ---
# Keys are our internal names; values are the real XLSX column headers.
COLUMN_MAP = {
    "facid": "FACID",
    "facname": "FACNAME",
    "factype": "FACTYPE",
    "citation_id": "CITATION_ID",
    "citation_class": "CITATION_CLASS",
    "citation_issue_date": "CITATION_ISSUE_DATE",
    "penalty_amount": "PENALTY_AMOUNT",
    "violation_description": "VIOLATION_DESCRIPTION",
}


class SchemaDriftError(RuntimeError):
    pass


def discover_latest_xlsx_url() -> str:
    """Query CKAN for the latest SEA XLSX resource URL."""
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        resp = client.get(settings.cdph_sea_metadata_url)
        resp.raise_for_status()
    pkg = resp.json()["result"]
    for res in pkg.get("resources", []):
        fmt = (res.get("format") or "").lower()
        name = (res.get("name") or "").lower()
        if fmt == "xlsx" and "sea" in name:
            return res["url"]
    raise RuntimeError("No SEA XLSX resource found in CDPH package metadata")


def download_xlsx(url: str) -> bytes:
    log.info("Downloading CDPH SEA XLSX from %s", url)
    with httpx.Client(
        timeout=600,
        follow_redirects=True,
        transport=httpx.HTTPTransport(retries=3),
    ) as client:
        resp = client.get(url)
        resp.raise_for_status()
    log.info("Downloaded %.1f MB", len(resp.content) / 1_048_576)
    return resp.content


def parse_xlsx(raw: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(raw), dtype={COLUMN_MAP["facid"]: str})
    missing = [v for v in COLUMN_MAP.values() if v not in df.columns]
    if missing:
        raise SchemaDriftError(f"CDPH SEA XLSX missing columns: {missing}")
    return df


def _parse_date(val: object) -> date | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def normalize_rows(df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    for _, r in df.iterrows():
        cdph_id = str(r[COLUMN_MAP["facid"]]).strip()
        if not cdph_id or cdph_id.lower() == "nan":
            continue
        issue = _parse_date(r[COLUMN_MAP["citation_issue_date"]])
        if issue is None:
            continue
        citation_id = str(r[COLUMN_MAP["citation_id"]]).strip()
        severity = str(r[COLUMN_MAP["citation_class"]] or "").strip().upper()
        penalty = r.get(COLUMN_MAP["penalty_amount"])
        penalty_int = None
        if penalty is not None and not (isinstance(penalty, float) and pd.isna(penalty)):
            try:
                penalty_int = int(round(float(penalty)))
            except (TypeError, ValueError):
                penalty_int = None

        description = r.get(COLUMN_MAP["violation_description"])
        description = None if description is None or (isinstance(description, float) and pd.isna(description)) else str(description)

        rows.append({
            "source": SOURCE_CDPH_SEA,
            "cdph_id": cdph_id,
            "citation_id": citation_id,
            "survey_date": issue,
            "deficiency_tag": None,
            "category": None,
            "severity": severity or None,
            "scope": None,
            "description": description,
            "corrective_action": None,
            "resolved": False,
            "resolved_date": None,
            "penalty_amount": penalty_int,
        })
    return rows


_UPSERT_SQL = text("""
    INSERT INTO facility_violations (
        facility_id, source, citation_id, survey_date,
        deficiency_tag, category, severity, scope,
        description, corrective_action, resolved, resolved_date
    ) VALUES (
        :facility_id, :source, :citation_id, :survey_date,
        :deficiency_tag, :category, :severity, :scope,
        :description, :corrective_action, :resolved, :resolved_date
    )
    ON CONFLICT (source, citation_id) DO UPDATE SET
        facility_id = EXCLUDED.facility_id,
        survey_date = EXCLUDED.survey_date,
        severity = EXCLUDED.severity,
        description = EXCLUDED.description
""")


def _load_cdph_map(session: Session) -> dict[str, str]:
    rows = session.execute(
        text("SELECT cdph_id, id::text FROM facilities WHERE cdph_id IS NOT NULL")
    ).all()
    return {r[0].strip(): r[1] for r in rows}


def run_with_xlsx(session: Session, raw: bytes) -> dict[str, object]:
    start = time.monotonic()
    df = parse_xlsx(raw)
    rows_downloaded = len(df)
    rows = normalize_rows(df)

    cdph_map = _load_cdph_map(session)
    ingested = unmatched = invalid = 0

    for row in rows:
        fid = cdph_map.get(row["cdph_id"])
        if not fid:
            unmatched += 1
            continue
        try:
            session.execute(
                _UPSERT_SQL,
                {
                    "facility_id": fid,
                    "source": row["source"],
                    "citation_id": row["citation_id"],
                    "survey_date": row["survey_date"],
                    "deficiency_tag": row["deficiency_tag"],
                    "category": row["category"],
                    "severity": row["severity"],
                    "scope": row["scope"],
                    "description": row["description"],
                    "corrective_action": row["corrective_action"],
                    "resolved": row["resolved"],
                    "resolved_date": row["resolved_date"],
                },
            )
            ingested += 1
        except Exception as exc:
            log.warning("CDPH SEA row failed upsert: %s — %s", row["citation_id"], exc)
            invalid += 1

    runtime = time.monotonic() - start
    return {
        "source": SOURCE_CDPH_SEA,
        "rows_downloaded": rows_downloaded,
        "rows_ingested": ingested,
        "rows_unmatched": unmatched,
        "rows_invalid": invalid,
        "runtime_seconds": round(runtime, 3),
    }


def run() -> dict[str, object]:
    from pipeline.db import get_session

    url = discover_latest_xlsx_url()
    raw = download_xlsx(url)
    filename = url.rsplit("/", 1)[-1]
    snapshot_path = archive_raw(SOURCE_CDPH_SEA, filename, raw)

    with get_session() as session:
        result = run_with_xlsx(session, raw)
        session.commit()

    result["source_url"] = url
    result["snapshot_path"] = snapshot_path
    log.info("CDPH SEA ingest complete: %s", result)
    return result
```

- [ ] **Step 7: Run tests — confirm they pass**

```bash
cd pipeline && uv run pytest tests/test_cdph_sea_ingest.py -v
```

Expected: all 4 tests PASS. If the COLUMN_MAP doesn't match the real XLSX (Step 1), update both `COLUMN_MAP` and the fixture-generator in Step 2 to match.

- [ ] **Step 8: Ruff + commit**

```bash
cd pipeline && uv run ruff check . && uv run ruff format .
git add pipeline/src/pipeline/ingest/cdph_sea.py \
        pipeline/src/pipeline/config.py \
        pipeline/tests/fixtures/violations/cdph_sea_sample.xlsx \
        pipeline/tests/test_cdph_sea_ingest.py
git commit -m "feat(ingest): add CDPH State Enforcement Actions ingest"
```

---

## Task 9: Celery tasks + beat schedule

**Goal:** Register three new Celery tasks (`ingest_cms_nh_compare`, `ingest_cdph_sea`, `refresh_violation_rollup`) and add two beat schedule entries. Chain rollup refresh after each ingest via direct `.delay()` call inside the task.

**Files:**
- Modify: `pipeline/src/pipeline/tasks.py` — append new tasks
- Modify: `pipeline/src/pipeline/celery_app.py:23-38` — extend `beat_schedule`

- [ ] **Step 1: Append new tasks to `pipeline/src/pipeline/tasks.py`**

Add at the end of the file:

```python
@app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=600,
    name="pipeline.tasks.ingest_cms_nh_compare",
)
def ingest_cms_nh_compare(self):  # type: ignore[no-untyped-def]
    """Download and upsert CMS Nursing Home Health Deficiencies (SNF, monthly)."""
    try:
        from pipeline.ingest.cms_nh_compare import run
        result = run()
        log.info("CMS NH Compare ingest complete: %s", result)
        refresh_violation_rollup.delay()
        return result
    except Exception as exc:
        log.exception("CMS NH Compare ingest failed: %s", exc)
        raise self.retry(exc=exc)


@app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=600,
    name="pipeline.tasks.ingest_cdph_sea",
)
def ingest_cdph_sea(self):  # type: ignore[no-untyped-def]
    """Download and upsert CDPH State Enforcement Actions (CA, annual)."""
    try:
        from pipeline.ingest.cdph_sea import run
        result = run()
        log.info("CDPH SEA ingest complete: %s", result)
        refresh_violation_rollup.delay()
        return result
    except Exception as exc:
        log.exception("CDPH SEA ingest failed: %s", exc)
        raise self.retry(exc=exc)


@app.task(
    bind=True,
    max_retries=2,
    default_retry_delay=60,
    name="pipeline.tasks.refresh_violation_rollup",
)
def refresh_violation_rollup(self):  # type: ignore[no-untyped-def]
    """Rebuild facility_violation_rollup from facility_violations."""
    try:
        from pipeline.db import get_session
        from pipeline.violations.rollup import refresh_violation_rollup as _refresh

        with get_session() as session:
            result = _refresh(session)
            session.commit()
        log.info("Violation rollup refresh complete: %s", result)
        return result
    except Exception as exc:
        log.exception("Violation rollup refresh failed: %s", exc)
        raise self.retry(exc=exc)
```

- [ ] **Step 2: Extend beat schedule in `pipeline/src/pipeline/celery_app.py`**

Replace the `beat_schedule=` dict with:

```python
    beat_schedule={
        # CDPH facility locations — monthly
        "ingest-cdph-monthly": {
            "task": "pipeline.tasks.ingest_cdph",
            "schedule": crontab(day_of_month="15", hour="2", minute="0"),
        },
        "ingest-crosswalk-monthly": {
            "task": "pipeline.tasks.ingest_crosswalk",
            "schedule": crontab(day_of_month="15", hour="3", minute="0"),
        },
        # CMS Nursing Home Health Deficiencies — monthly, 1st of month
        "ingest-cms-nh-compare-monthly": {
            "task": "pipeline.tasks.ingest_cms_nh_compare",
            "schedule": crontab(day_of_month="1", hour="5", minute="0"),
        },
        # CDPH State Enforcement Actions — annual, July 15
        "ingest-cdph-sea-annual": {
            "task": "pipeline.tasks.ingest_cdph_sea",
            "schedule": crontab(month_of_year="7", day_of_month="15", hour="6", minute="0"),
        },
        # Regenerate tiles nightly
        "generate-tiles-nightly": {
            "task": "pipeline.tasks.generate_all_tiles",
            "schedule": crontab(hour="4", minute="0"),
        },
    },
```

- [ ] **Step 3: Verify tasks register without errors**

```bash
cd pipeline && uv run python -c "
from pipeline.celery_app import app
tasks = [t for t in app.tasks if t.startswith('pipeline.tasks.')]
assert 'pipeline.tasks.ingest_cms_nh_compare' in tasks
assert 'pipeline.tasks.ingest_cdph_sea' in tasks
assert 'pipeline.tasks.refresh_violation_rollup' in tasks
print('OK — all 3 new tasks registered')
print('Beat schedule:', list(app.conf.beat_schedule.keys()))
"
```

Expected: `OK — all 3 new tasks registered` and a beat schedule listing including the two new entries.

- [ ] **Step 4: Ruff + commit**

```bash
cd pipeline && uv run ruff check . && uv run ruff format .
git add pipeline/src/pipeline/tasks.py pipeline/src/pipeline/celery_app.py
git commit -m "feat(tasks): register CMS and CDPH violation ingest tasks with beat schedule"
```

---

## Task 10: Filter API — violation filters using rollup table

**Goal:** Extend `FacilityFilterRequest` with new violation filter fields, update `POST /facilities/filter` to LEFT JOIN `facility_violation_rollup`, return rollup fields in the response, and replace the existing slow `facility_violations` COUNT subqueries with rollup reads.

**Files:**
- Modify: `api/src/api/schemas.py:14-26` — add fields
- Modify: `api/src/api/routes/facilities.py` — add JOIN, WHERE clauses, response fields

- [ ] **Step 1: Extend `FacilityFilterRequest` in `api/src/api/schemas.py`**

Replace the class body with:

```python
class FacilityFilterRequest(BaseModel):
    facility_types: list[str] | None = Field(None, description="e.g. ['home_health', 'hospice']")
    license_status: str | None = Field(None, description="e.g. 'active'")
    county: str | None = None
    gross_revenue_min: int | None = Field(None, ge=0)
    gross_revenue_max: int | None = Field(None, ge=0)
    # Violation filters (Phase 3 — via facility_violation_rollup)
    violation_count_min: int | None = Field(None, ge=0)
    violation_count_max: int | None = Field(None, ge=0)
    violation_count_12mo_min: int | None = Field(None, ge=0)
    max_severity_level_min: int | None = Field(None, ge=0, le=10)
    has_ij_12mo: bool | None = None
    survey_date_after: str | None = Field(None, description="ISO date YYYY-MM-DD")
    year: int | None = Field(None, description="Financial data year filter")
    certified_medicare: bool | None = None
    certified_medicaid: bool | None = None
    spatial: SpatialFilter | None = Field(None, description="Polygon to intersect with")
    limit: int = Field(500, ge=1, le=5000)
    offset: int = Field(0, ge=0)
```

- [ ] **Step 2: Update `api/src/api/routes/facilities.py` — replace the filter handler body**

Locate the existing block starting at line 94:

```python
    # Violation count filter
    if req.violation_count_max is not None:
        params["viol_max"] = req.violation_count_max
        where_clauses.append("""
            (SELECT COUNT(*) FROM facility_violations WHERE facility_id = f.id) <= :viol_max
        """)
```

Replace it with:

```python
    # Violation filters — via facility_violation_rollup
    if req.violation_count_min is not None:
        params["viol_min"] = req.violation_count_min
        where_clauses.append("COALESCE(viol.violation_count_total, 0) >= :viol_min")
    if req.violation_count_max is not None:
        params["viol_max"] = req.violation_count_max
        where_clauses.append("COALESCE(viol.violation_count_total, 0) <= :viol_max")
    if req.violation_count_12mo_min is not None:
        params["viol_12mo_min"] = req.violation_count_12mo_min
        where_clauses.append("COALESCE(viol.violation_count_12mo, 0) >= :viol_12mo_min")
    if req.max_severity_level_min is not None:
        params["sev_min"] = req.max_severity_level_min
        where_clauses.append("COALESCE(viol.max_severity_level_12mo, 0) >= :sev_min")
    if req.has_ij_12mo is not None:
        params["has_ij"] = req.has_ij_12mo
        where_clauses.append("COALESCE(viol.has_ij_12mo, FALSE) = :has_ij")
    if req.survey_date_after is not None:
        params["survey_after"] = req.survey_date_after
        where_clauses.append("viol.last_survey_date >= :survey_after::date")
```

Then locate the SELECT query and replace it with this updated version that joins the rollup table:

```python
    query = text(f"""
        SELECT
            f.id::text,
            f.name,
            f.type,
            f.subtype,
            f.address,
            f.city,
            f.county,
            f.zip,
            f.license_status,
            f.certified_medicare,
            f.certified_medicaid,
            f.lat,
            f.lon,
            COALESCE(fin.gross_revenue, 0) AS gross_revenue,
            fin.year AS revenue_year,
            COALESCE(viol.violation_count_total, 0) AS violation_count,
            COALESCE(viol.violation_count_12mo, 0) AS violation_count_12mo,
            viol.max_severity_12mo,
            COALESCE(viol.max_severity_level_12mo, 0) AS max_severity_level_12mo,
            COALESCE(viol.has_ij_12mo, FALSE) AS has_ij_12mo,
            viol.last_survey_date::text AS last_survey_date
        FROM facilities f
        {fin_join}
        LEFT JOIN facility_violation_rollup viol ON viol.facility_id = f.id
        WHERE {where_sql}
        ORDER BY f.name
        LIMIT :limit OFFSET :offset
    """)  # noqa: S608
```

Then replace the `features = [...]` list comprehension:

```python
    # Column order: id(0) name(1) type(2) subtype(3) address(4) city(5)
    # county(6) zip(7) license_status(8) certified_medicare(9)
    # certified_medicaid(10) lat(11) lon(12) gross_revenue(13)
    # revenue_year(14) violation_count(15) violation_count_12mo(16)
    # max_severity_12mo(17) max_severity_level_12mo(18) has_ij_12mo(19)
    # last_survey_date(20)
    features = [
        GeoJSONFeature(
            geometry={"type": "Point", "coordinates": [r[12], r[11]]},
            properties={
                "id": r[0],
                "name": r[1],
                "type": r[2],
                "subtype": r[3],
                "address": r[4],
                "city": r[5],
                "county": r[6],
                "zip": r[7],
                "license_status": r[8],
                "certified_medicare": r[9],
                "certified_medicaid": r[10],
                "gross_revenue": r[13],
                "revenue_year": r[14],
                "violation_count": r[15],
                "violation_count_12mo": r[16],
                "max_severity_12mo": r[17],
                "max_severity_level_12mo": r[18],
                "has_ij_12mo": r[19],
                "last_survey_date": r[20],
            },
        )
        for r in rows
        if r[11] and r[12]
    ]
```

- [ ] **Step 3: Smoke-test the API locally**

```bash
cd api && uv run uvicorn api.main:app --port 8000 &
sleep 3
curl -s -X POST http://localhost:8000/facilities/filter \
  -H "Content-Type: application/json" \
  -d '{"facility_types":["snf"],"has_ij_12mo":true,"limit":5}' | head -c 500
kill %1 2>/dev/null
```

Expected: valid JSON response with a `features` array (likely empty until a real ingest has run — that's fine, we're only verifying the query compiles and runs).

- [ ] **Step 4: Ruff + commit**

```bash
cd api && uv run ruff check . && uv run ruff format .
git add api/src/api/schemas.py api/src/api/routes/facilities.py
git commit -m "feat(api): add violation filters and rollup-backed response fields"
```

---

## Task 11: Tile generator — emit rollup fields

**Goal:** Update `tiles/generate.py` EXPORT_SQL to `LEFT JOIN` `facility_violation_rollup` and emit five rollup-derived attributes to Tippecanoe. Replace the existing inline `violation_count` subquery.

**Files:**
- Modify: `pipeline/src/pipeline/tiles/generate.py:25-63` — EXPORT_SQL
- Modify: `pipeline/src/pipeline/tiles/generate.py:99-113` — Tippecanoe `--attribute-type` flags

- [ ] **Step 1: Replace `EXPORT_SQL` in `pipeline/src/pipeline/tiles/generate.py`**

Replace the block from line 25 to line 63 with:

```python
# SQL: export facilities as GeoJSON features, enriched with latest financials + violation rollup
EXPORT_SQL = """
SELECT json_build_object(
    'type', 'Feature',
    'geometry', ST_AsGeoJSON(f.geom)::json,
    'properties', json_build_object(
        'id',             f.id::text,
        'name',           f.name,
        'type',           f.type,
        'subtype',        f.subtype,
        'address',        f.address,
        'city',           f.city,
        'county',         f.county,
        'zip',            f.zip,
        'phone',          f.phone,
        'license_status', f.license_status,
        'certified_medicare', f.certified_medicare,
        'certified_medicaid', f.certified_medicaid,
        'gross_revenue',  COALESCE(fin.gross_revenue, 0),
        'revenue_year',   fin.year,
        'violation_count',        COALESCE(viol.violation_count_total, 0),
        'violation_count_12mo',   COALESCE(viol.violation_count_12mo, 0),
        'max_severity_level_12mo', COALESCE(viol.max_severity_level_12mo, 0),
        'has_ij_12mo',            COALESCE(viol.has_ij_12mo, FALSE),
        'last_survey_date',       viol.last_survey_date::text
    )
) AS feature
FROM facilities f
LEFT JOIN LATERAL (
    SELECT gross_revenue, year
    FROM facility_financials
    WHERE facility_id = f.id
    ORDER BY year DESC
    LIMIT 1
) fin ON true
LEFT JOIN facility_violation_rollup viol ON viol.facility_id = f.id
WHERE f.geom IS NOT NULL
{type_filter}
"""
```

- [ ] **Step 2: Update Tippecanoe flags in `_run_tippecanoe`**

Replace the `cmd = [...]` list in `_run_tippecanoe` with:

```python
    cmd = [
        "tippecanoe",
        f"--output={pmtiles_path}",
        "--force",
        "--minimum-zoom=4",
        "--maximum-zoom=14",
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        f"--layer={layer_slug}",
        "--attribute-type=gross_revenue:int",
        "--attribute-type=violation_count:int",
        "--attribute-type=violation_count_12mo:int",
        "--attribute-type=max_severity_level_12mo:int",
        "--attribute-type=has_ij_12mo:bool",
        "--attribute-type=certified_medicare:bool",
        "--attribute-type=certified_medicaid:bool",
        geojson_path,
    ]
```

- [ ] **Step 3: Smoke-test tile generation**

```bash
cd pipeline && uv run python -c "
from pipeline.tiles.generate import EXPORT_SQL
from sqlalchemy import text
from pipeline.db import get_session
sql = text(EXPORT_SQL.format(type_filter=''))
with get_session() as s:
    row = s.execute(sql).first()
    if row:
        import json
        feat = row[0]
        assert 'violation_count_12mo' in feat['properties']
        assert 'has_ij_12mo' in feat['properties']
        print('OK:', json.dumps(feat['properties'], default=str, indent=2)[:500])
    else:
        print('No features yet — rollup SQL compiles cleanly')
"
```

Expected: either a sample feature's properties dict with the new rollup fields, or "No features yet — rollup SQL compiles cleanly" if the facilities table is empty.

- [ ] **Step 4: Ruff + commit**

```bash
cd pipeline && uv run ruff check . && uv run ruff format .
git add pipeline/src/pipeline/tiles/generate.py
git commit -m "feat(tiles): export violation rollup fields in PMTiles"
```

---

## Task 12: Live smoke test + manual end-to-end verification

**Goal:** A `@pytest.mark.live` test that exercises the real CMS endpoint end-to-end, plus a manual runbook that proves the full pipeline works on a clean machine.

**Files:**
- Create: `pipeline/tests/test_violations_live.py`
- Modify: `pipeline/pyproject.toml` — register the `live` marker

- [ ] **Step 1: Register the `live` marker in `pipeline/pyproject.toml`**

Append to the end of the file:

```toml
[tool.pytest.ini_options]
markers = [
    "live: tests that hit real external endpoints (run on demand only)",
]
addopts = "-m 'not live'"
```

- [ ] **Step 2: Write the live test**

```python
# pipeline/tests/test_violations_live.py
"""Live smoke tests — hit real CMS/CDPH endpoints.

Excluded from the default `pytest` run. To invoke:
  uv run pytest -m live tests/test_violations_live.py -v
"""
import pytest

from pipeline.ingest.cms_nh_compare import (
    discover_latest_csv_url,
    download_csv,
    filter_to_ca,
    parse_csv,
)
from pipeline.ingest.cdph_sea import discover_latest_xlsx_url


@pytest.mark.live
def test_cms_metadata_endpoint_resolves_csv_url():
    url = discover_latest_csv_url()
    assert url.endswith(".csv")
    assert "NH_" in url or "Health" in url


@pytest.mark.live
def test_cms_csv_parses_and_has_ca_rows():
    url = discover_latest_csv_url()
    raw = download_csv(url)
    df = parse_csv(raw)
    assert len(df) > 1000  # CMS publishes tens of thousands of rows
    ca = filter_to_ca(df)
    assert len(ca) > 100  # CA has many SNFs
    assert "scope_severity_code" in ca.columns


@pytest.mark.live
def test_cdph_sea_metadata_endpoint_resolves_xlsx_url():
    url = discover_latest_xlsx_url()
    assert url.endswith(".xlsx")
```

- [ ] **Step 3: Verify the marker exclusion works**

```bash
cd pipeline && uv run pytest tests/test_violations_live.py -v
```

Expected: `3 deselected` — tests are skipped because of the default `-m 'not live'` filter.

```bash
cd pipeline && uv run pytest -m live tests/test_violations_live.py -v
```

Expected: all 3 tests PASS (requires network access to data.cms.gov and data.chhs.ca.gov).

- [ ] **Step 4: Manual end-to-end runbook — document and execute**

Run each of these commands in order and confirm the documented result:

```bash
# 1. Ensure DB up to date
docker compose up -d db redis
cd pipeline && uv run alembic upgrade head

# 2. Invoke the CMS ingest via Celery
cd pipeline && uv run celery -A pipeline.celery_app call pipeline.tasks.ingest_cms_nh_compare
# Expected: Celery returns a task ID

# 3. Verify rows landed
docker compose exec db psql -U geodata -d geodata -c \
  "SELECT source, COUNT(*) FROM facility_violations GROUP BY source"
# Expected: at least cms_nh_compare with >0 rows (only matches existing CCNs)

# 4. Verify rollup was chained and populated
docker compose exec db psql -U geodata -d geodata -c \
  "SELECT COUNT(*) FILTER (WHERE violation_count_total > 0), COUNT(*) FROM facility_violation_rollup"
# Expected: total equals facilities count, some number of facilities with > 0 violations

# 5. Regenerate tiles and confirm rollup fields ship
cd pipeline && uv run celery -A pipeline.celery_app call pipeline.tasks.generate_all_tiles
# Expected: tile generation completes without errors

# 6. Filter API smoke test
cd api && uv run uvicorn api.main:app --port 8000 &
sleep 3
curl -s -X POST http://localhost:8000/facilities/filter \
  -H "Content-Type: application/json" \
  -d '{"has_ij_12mo":true,"limit":3}' | python -m json.tool
kill %1
# Expected: JSON with features array (possibly empty) — no 500 errors
```

- [ ] **Step 5: Run the full test suite one more time**

```bash
cd pipeline && uv run pytest -v
```

Expected: all tests from Tasks 1, 3, 4, 5, 7, 8 pass; Task 12 tests skipped (live marker).

- [ ] **Step 6: Ruff + final commit**

```bash
cd pipeline && uv run ruff check . && uv run ruff format .
git add pipeline/tests/test_violations_live.py pipeline/pyproject.toml
git commit -m "test(violations): add live smoke tests and register live marker"
```

---

## Post-Implementation Checklist

- [ ] Migration `004` applied cleanly
- [ ] `pipeline/tests/` test suite passes (non-live markers)
- [ ] `pipeline/tests/test_violations_live.py` passes with `-m live` at least once against real endpoints
- [ ] Manual runbook in Task 12 Step 4 completed successfully on the dev machine
- [ ] `uv run ruff check .` passes in both `pipeline/` and `api/`
- [ ] Celery beat shows the two new schedule entries via `celery -A pipeline.celery_app inspect scheduled`
- [ ] At least one row exists in `facility_violation_rollup` with `violation_count_total > 0`
- [ ] `POST /facilities/filter` with `{"has_ij_12mo": true}` returns a valid (possibly empty) GeoJSON response

## Known Deferred Work (Phase 3.5+)

1. CMS archive backfill for pre-2023 deficiency history
2. Violation detail view in the web/mobile UI (Phase 5)
3. Ingest status dashboard (Phase 6 productization)
4. CDPH tag-level deficiency data (if CDPH ever publishes it as a bulk download)
