"""
This module sets up fixtures for testing with Docker-based PostgreSQL databases
It includes fixtures for creating and tearing down a PostgreSQL container and
for establishing a database connection that is used throughout the test session
"""

import time
import docker
import pytest
import sqlalchemy as sqla

from test_config import DB, PORT, POSTGRES_PASSWORD, USER
from dynamic_pricing.core.db_init import create_tables


@pytest.fixture(scope="session")
def psql_docker():
    """
    Creates and runs a Docker container with PostgreSQL for testing purposes.
    Yields the container to be used in tests and cleans up by stopping and
    pruning the container after tests are complete.
    """
    client = docker.from_env()
    container_name = "test_postgres"

    container = client.containers.run(
        image="postgres:12-alpine",
        auto_remove=True,
        environment={"POSTGRES_PASSWORD": POSTGRES_PASSWORD},
        name=container_name,
        ports={"5432/tcp": ("127.0.0.1", PORT)},
        detach=True,
    )

    # Wait for the container to start
    time.sleep(5)

    yield
    container.stop()
    client.containers.prune()


@pytest.fixture(scope="session")
def connection(psql_docker):  # pylint: disable=W0613
    """
    Establishes a SQLAlchemy connection to the PostgreSQL database running in
    a Docker container. It ensures that the database tables are created before
    running the tests and provides a connection to be used for testing.
    """
    engine = sqla.create_engine(
        f"postgresql://{USER}:{POSTGRES_PASSWORD}@localhost:{PORT}/{DB}"
    )
    with engine.connect() as conn:
        create_tables(conn)
        yield conn
