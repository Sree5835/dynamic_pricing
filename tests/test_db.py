"""
Module to conduct database tests for the dynamic pricing application. It
checks the integrity of the tables, the insertion of order data, and the
retrieval of loaded data to ensure proper functionality and integration.
"""

import json
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql import text
from dynamic_pricing.core.order_manager import insert_order_data
from dynamic_pricing.core.db_utils import load_order_data

load_dotenv()


def test_tables(connection: Connection):
    """Test to verify that the correct number of tables exists in the
    non-system schema of the database."""
    ans = connection.execute(
        text(
            """SELECT COUNT(*)
        FROM pg_catalog.pg_tables
        WHERE schemaname != 'pg_catalog'
        AND schemaname != 'information_schema';
        """
        )
    ).fetchone()[0]
    assert ans == 7, "The number of tables does not match the expected count."


def test_insert_order_data(connection: Connection):
    """Test the insertion of order data into the database from a JSON file to
    ensure data integrity and functionality."""
    with open(
        "tests/test_data/test_order.json", "r", encoding="utf-8"
    ) as file:
        order_data = json.load(file)
    insert_order_data(
        connection,
        os.getenv("PARTNER1"),
        order_data=order_data,
        is_webhook=False,
    )
    ans = connection.execute(
        text(
            """SELECT COUNT(*)
            FROM orders;
            """
        )
    ).fetchone()[0]
    assert (
        ans == 1
    ), "The number of inserted orders does not match the expected count."


def test_load_data(connection: Connection):
    """Test the loading of order data from the database, verifying the data
    structure and content against expected values."""
    df: pd.DataFrame = load_order_data(connection, os.getenv("PARTNER1"))
    assert df.shape == (
        3,
        27,
    ), "DataFrame shape does not match expected dimensions."
    assert set(df["platform_order_id"]) == {
        "gb:6606c495-e33a-4bde-b152-e3ddd4efe0ee"
    }, "Platform order ID does not match expected."
    assert set(df["item_operational_name"]) == {
        "Cheese Filled Bifteki Wrap (Handmade Greek Pitta Wraps)",
        "Cheese Filled Bifteki (Handmade Single Grills)",
    }, "Item operational names do not match expected."
    assert set(df["modifier_operational_name"]) == {
        "Mustard",
        "Mayonnaise",
        None,
    }, "Modifier operational names do not match expected."
