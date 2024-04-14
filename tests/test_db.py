import json

from sqlalchemy.engine.base import Connection
from sqlalchemy.sql import text

from dynamic_pricing.db.db_init import create_tables
from dynamic_pricing.db.db_utils import insert_order_data


def test_tables(connection: Connection):
    ans = connection.execute(
        text(
            """SELECT COUNT(*)
        FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';
        """
        )
    ).fetchone()[0]
    assert ans == 7


def test_insert_order_data(connection: Connection):
    with open("tests/test_data/test_order.json", "r") as file:
        order_data = json.load(file)
    insert_order_data(
        connection, "nostimo", order_data=order_data, is_webhook=False
    )
    ans = connection.execute(
        text(
            """SELECT COUNT(*)
            FROM orders;
            """
        )
    ).fetchone()[0]
    assert ans == 1
