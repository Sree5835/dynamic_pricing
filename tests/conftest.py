import time

import docker
import pytest
import sqlalchemy as sqla

from test_config import DB, PORT, POSTGRES_PASSWORD, USER
from dynamic_pricing.db.db_init import create_tables


@pytest.fixture(scope="session")
def psql_docker():
    client = docker.from_env()
    container_name = "test_postgres"

    container = client.containers.run(
        image="postgres:12-alpine",
        auto_remove=True,
        environment=dict(
            POSTGRES_PASSWORD=POSTGRES_PASSWORD,
        ),
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
def connection(psql_docker):
    engine = sqla.create_engine(
        f"postgresql://{USER}:{POSTGRES_PASSWORD}@localhost:{PORT}/{DB}"
    )
    with engine.connect() as conn:
        create_tables(conn)
        yield conn
