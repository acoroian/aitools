from datetime import date
from pathlib import Path
from uuid import uuid4

from sqlalchemy import text

from pipeline.ingest.cdph_sea import normalize_rows, parse_xlsx, run_with_xlsx

FIXTURE = Path(__file__).parent / "fixtures" / "violations" / "cdph_sea_sample.xlsx"


def _seed_facility(session, cdph_id: str, name: str = "Test") -> str:
    # Clean up dev-DB contamination inside SAVEPOINT.
    session.execute(
        text(
            "DELETE FROM facility_violations WHERE facility_id IN "
            "(SELECT id FROM facilities WHERE cdph_id = :cdph_id)"
        ),
        {"cdph_id": cdph_id},
    )
    session.execute(
        text("DELETE FROM facilities WHERE cdph_id = :cdph_id"),
        {"cdph_id": cdph_id},
    )
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
    assert result["rows_ingested"] == 5
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
