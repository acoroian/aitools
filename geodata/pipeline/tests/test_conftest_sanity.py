from sqlalchemy import text


def test_db_session_can_query_facilities(db_session):
    """Verify the fixture connects and facilities table exists."""
    result = db_session.execute(text("SELECT COUNT(*) FROM facilities")).scalar_one()
    assert result >= 0  # table exists and is queryable
