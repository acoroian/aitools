"""Shared pytest fixtures for pipeline tests.

Each test runs inside a nested SAVEPOINT so all writes — including
those committed by the code under test — roll back at teardown,
leaving the DB untouched between tests. Requires a running PostGIS
instance accessible via TEST_DATABASE_URL (defaults to the local
docker-compose DB).
"""

import os
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    "postgresql://geodata:geodata@localhost:5432/geodata",
)


@pytest.fixture(scope="session")
def engine() -> Iterator[Engine]:
    eng = create_engine(TEST_DATABASE_URL, future=True)
    yield eng
    eng.dispose()


@pytest.fixture
def db_session(engine: Engine) -> Iterator[Session]:
    """Transaction-per-test with nested SAVEPOINT.

    Any ``session.commit()`` inside the code under test commits to the
    SAVEPOINT, not the outer transaction, so the teardown rollback
    still discards everything.
    """
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection, expire_on_commit=False)
    nested = connection.begin_nested()

    @event.listens_for(session, "after_transaction_end")
    def restart_savepoint(sess: Session, trans) -> None:  # noqa: ARG001
        nonlocal nested
        if trans.nested and not trans._parent.nested:
            nested = connection.begin_nested()

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()
