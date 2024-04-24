"""
This module handles database operations necessary for managing dynamic pricing data.
It includes functions to establish database connections, create temporary tables,
perform upsert operations, and load data, among other utility functions.
"""

import json
import os
from typing import List

import pandas as pd
import sqlalchemy as sqla
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


def get_db_connection():
    """
    Establishes and returns a database connection using the URL from environment variables.
    """
    db_url = os.environ.get("DB_URL")
    assert db_url, "DB_URL environment variable is not set."
    engine = create_engine(db_url, echo=True)
    return engine.connect()


def create_tmp_table(
    conn: sqla.engine.base.Connection, table_name: str, data_dict: dict
) -> str:
    """
    Creates a temporary table based on an existing table's schema and inserts initial data.
    """
    try:
        conn.execute(text(f"DROP TABLE IF EXISTS tmp_{table_name} CASCADE;"))
        conn.execute(
            text(
                f"CREATE TABLE tmp_{table_name} (LIKE {table_name} INCLUDING ALL);"
            )
        )
        insert_query = f"INSERT INTO tmp_{table_name} ({', '.join(data_dict.keys())}) VALUES ({', '.join([':' + col for col in data_dict.keys()])})"  # pylint: disable=line-too-long
        conn.execute(text(insert_query), data_dict)
    except sqla.exc.SQLAlchemyError as e:
        raise ConnectionError(
            f"Error creating temporary table for {table_name}: {e}"
        ) from e
    return f"tmp_{table_name}"


def drop_tmp_table(conn: sqla.engine.base.Connection, tmp_table_name: str):
    """
    Drops a specified temporary table.
    """
    try:
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp_table_name} CASCADE;"))
    except sqla.exc.SQLAlchemyError as e:
        raise ConnectionError(
            f"Error dropping temporary table {tmp_table_name}: {e}"
        ) from e


def add_constraints(
    conn: sqla.engine.base.Connection, table_name: str, pk_cols: List[str]
):
    """
    Adds unique constraints to a table based on specified primary key columns.
    """
    try:
        conn.execute(
            text(
                f"ALTER TABLE {table_name} ADD CONSTRAINT temp_constraint UNIQUE ({', '.join(pk_cols)});"  # pylint: disable=line-too-long
            )
        )
    except sqla.exc.SQLAlchemyError as e:
        raise ConnectionError(
            f"Error adding constraints to table {table_name}: {e}"
        ) from e


def remove_constraints(conn: sqla.engine.base.Connection, table_name: str):
    """
    Removes constraints from a specified table.
    """
    try:
        conn.execute(
            text(f"ALTER TABLE {table_name} DROP CONSTRAINT temp_constraint;")
        )
    except sqla.exc.SQLAlchemyError as e:
        raise ConnectionError(
            f"Error removing constraints from table {table_name}: {e}"
        ) from e


def upsert(
    conn: sqla.engine.base.Connection,
    table_name: str,
    data_dict,
    pk_cols: List[str],
    returning_col=None,
) -> int:
    """
    Performs an upsert operation which inserts or updates data based on conflict resolution.
    """
    tmp_table_name = create_tmp_table(conn, table_name, data_dict)
    try:
        conflict_cols = [
            f"{col}=EXCLUDED.{col}"
            for col in data_dict.keys()
            if col not in pk_cols
        ]
        add_constraints(conn, table_name, pk_cols)
        query = (
            f"INSERT INTO {table_name} ({', '.join(data_dict.keys())}) SELECT {', '.join(data_dict.keys())} FROM {tmp_table_name} "  # pylint: disable=line-too-long
            f"ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE SET {', '.join(conflict_cols)}"  # pylint: disable=line-too-long
        )
        query += f" RETURNING {returning_col};" if returning_col else ";"
        result = (
            conn.execute(text(query), data_dict).scalar()
            if returning_col
            else conn.execute(text(query), data_dict).rowcount
        )
        remove_constraints(conn, table_name)
        drop_tmp_table(conn, tmp_table_name)
    except sqla.exc.SQLAlchemyError as e:
        raise ConnectionError(
            f"Error upserting data into table {table_name}: {e}"
        ) from e
    return result


def load_order_data(
    conn: sqla.engine.base.Connection, partner_name: str
) -> pd.DataFrame:
    """
    Loads order data for a given partner from the database.
    """
    query = (
        f"SELECT orders.order_id, orders.platform_order_id, orders.platform_order_number, orders.order_status, "  # pylint: disable=line-too-long
        f"orders.order_placed_timestamp, orders.order_updated_timestamp, orders.order_prepare_for_timestamp, "  # pylint: disable=line-too-long
        f"orders.order_start_prepping_at_timestamp, customers.customer_id, customers.first_name, "  # pylint: disable=line-too-long
        f"customers.contact_number, customers.contact_access_code, partners.partner_id, partners.partner_name, "  # pylint: disable=line-too-long
        f"items.item_id, items.platform_item_id, items.item_name, items.item_operational_name, items.item_fractional_cost, "  # pylint: disable=line-too-long
        f"order_items.quantity AS item_quantity, order_items.fractional_price AS item_fractional_price, "  # pylint: disable=line-too-long
        f"modifiers.modifier_id, modifiers.platform_modifier_id, modifiers.modifier_name, "  # pylint: disable=line-too-long
        f"modifiers.modifier_operational_name, order_item_modifiers.quantity AS modifier_quantity, "  # pylint: disable=line-too-long
        f"order_item_modifiers.fractional_price AS modifier_fractional_price "  # pylint: disable=line-too-long
        f"FROM orders FULL JOIN customers ON orders.customer_id = customers.customer_id "  # pylint: disable=line-too-long
        f"FULL JOIN partners ON orders.partner_id = partners.partner_id FULL JOIN order_items ON orders.order_id = order_items.order_id "  # pylint: disable=line-too-long
        f"FULL JOIN items ON order_items.item_id = items.item_id FULL JOIN order_item_modifiers ON order_items.order_id = order_item_modifiers.order_id "  # pylint: disable=line-too-long
        f"AND order_items.item_id = order_item_modifiers.item_id FULL JOIN modifiers ON order_item_modifiers.modifier_id = modifiers.modifier_id "  # pylint: disable=line-too-long
        f"WHERE partners.partner_name = '{partner_name}';"
    )
    return pd.read_sql(query, conn)
