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

    row = (
        db_session.execute(
            text("SELECT * FROM facility_violation_rollup WHERE facility_id = :id"),
            {"id": fid},
        )
        .mappings()
        .one()
    )

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

    row = (
        db_session.execute(
            text(
                "SELECT has_ij_12mo, max_severity_12mo, max_severity_level_12mo "
                "FROM facility_violation_rollup WHERE facility_id = :id"
            ),
            {"id": fid},
        )
        .mappings()
        .one()
    )
    assert row["has_ij_12mo"] is True
    assert row["max_severity_12mo"] == "cms_nh_compare:J"
    assert row["max_severity_level_12mo"] == 8


def test_rollup_old_ij_does_not_trigger_12mo_flag(db_session):
    fid = _insert_facility(db_session, "Test SNF Old IJ")
    today = date.today()
    _insert_violation(db_session, fid, "cms_nh_compare", "oldj", today - timedelta(days=500), "J")
    db_session.flush()

    refresh_violation_rollup(db_session)

    row = (
        db_session.execute(
            text(
                "SELECT has_ij_12mo, violation_count_total, violation_count_12mo "
                "FROM facility_violation_rollup WHERE facility_id = :id"
            ),
            {"id": fid},
        )
        .mappings()
        .one()
    )
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

    row = (
        db_session.execute(
            text("SELECT * FROM facility_violation_rollup WHERE facility_id = :id"),
            {"id": fid},
        )
        .mappings()
        .one()
    )
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


def test_rollup_emits_row_for_facility_with_zero_violations(db_session):
    fid = _insert_facility(db_session, "Empty SNF")
    db_session.flush()

    refresh_violation_rollup(db_session)

    row = (
        db_session.execute(
            text("SELECT * FROM facility_violation_rollup WHERE facility_id = :id"),
            {"id": fid},
        )
        .mappings()
        .one()
    )
    assert row["violation_count_total"] == 0
    assert row["violation_count_12mo"] == 0
    assert row["has_ij_12mo"] is False
    assert row["max_severity_12mo"] is None
    assert row["max_severity_level_12mo"] is None
    assert row["last_survey_date"] is None
