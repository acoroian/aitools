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
    session_factory = sessionmaker(bind=connection, expire_on_commit=False, future=True)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()
