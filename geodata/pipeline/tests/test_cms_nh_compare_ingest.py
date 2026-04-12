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
    # The dev DB already contains real facilities for common CCNs.
    # Inside the SAVEPOINT fixture these deletes roll back at teardown,
    # so production data is not affected — this just gives each test a
    # clean slate for the CCN it wants to own.
    session.execute(
        text(
            "DELETE FROM facility_violations WHERE facility_id IN "
            "(SELECT id FROM facilities WHERE ccn = :ccn)"
        ),
        {"ccn": ccn},
    )
    session.execute(text("DELETE FROM facilities WHERE ccn = :ccn"), {"ccn": ccn})
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
