from sqlalchemy.engine.base import Connection
from sqlalchemy.sql import text

from dynamic_pricing.db.db_init import create_tables


def test_tables(connection: Connection):
    create_tables(connection)
    ans = connection.execute(
        text(
            """SELECT COUNT(*)
        FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';
        """
        )
    ).fetchone()[0]
    assert ans == 7
