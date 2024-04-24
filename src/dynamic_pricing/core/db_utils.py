"""
This module manages database operations for dynamic pricing, including
creating temporary tables, and upserting data.
"""

import os
from typing import List

import pandas as pd
import sqlalchemy as sqla
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


def get_db_connection():
    """Establishes a database connection using the URL from environment variables."""
    db_url = os.environ.get("DB_URL")
    assert db_url, "DB_URL environment variable is not set."
    engine = create_engine(db_url, echo=True)
    return engine.connect()


def create_tmp_table(conn: sqla.engine.base.Connection, table_name, data_dict):
    """Creates a temporary table based on the structure of an existing table and inserts data."""
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
    """Drops the specified temporary table."""
    try:
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp_table_name} CASCADE;"))
    except sqla.exc.SQLAlchemyError as e:
        raise ConnectionError(
            f"Error dropping temporary table {tmp_table_name}: {e}"
        ) from e


def add_constraints(
    conn: sqla.engine.base.Connection, table_name: str, pk_cols: List[str]
):
    """Adds a unique constraint to the specified table based on provided columns."""
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
    """Removes a unique constraint from the specified table."""
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
):
    """Performs an upsert operation into the specified table using the provided data."""
    tmp_table_name = create_tmp_table(conn, table_name, data_dict=data_dict)
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
            else None
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
    """Loads order data for a given partner name from the database into a pandas DataFrame."""
    query = f"""
            SELECT
                orders.order_id,
                orders.platform_order_id,
                orders.platform_order_number,
                orders.order_status,
                orders.order_placed_timestamp,
                orders.order_updated_timestamp,
                orders.order_prepare_for_timestamp,
                orders.order_start_prepping_at_timestamp,
                customers.customer_id,
                customers.first_name,
                customers.contact_number,
                customers.contact_access_code,
                partners.partner_id,
                partners.partner_name,
                items.item_id,
                items.platform_item_id,
                items.item_name,
                items.item_operational_name,
                items.item_fractional_cost,
                order_items.quantity AS item_quantity,
                order_items.fractional_price AS item_fractional_price,
                modifiers.modifier_id,
                modifiers.platform_modifier_id,
                modifiers.modifier_name,
                modifiers.modifier_operational_name,
                order_item_modifiers.quantity AS modifier_quantity,
                order_item_modifiers.fractional_price AS modifier_fractional_price
            FROM
                orders
            FULL JOIN
                customers ON orders.customer_id = customers.customer_id
            FULL JOIN
                partners ON orders.partner_id = partners.partner_id
            FULL JOIN
                order_items ON orders.order_id = order_items.order_id
            FULL JOIN
                items ON order_items.item_id = items.item_id
            FULL JOIN
                order_item_modifiers ON order_items.order_id = order_item_modifiers.order_id AND order_items.item_id = order_item_modifiers.item_id
            FULL JOIN
                modifiers ON order_item_modifiers.modifier_id = modifiers.modifier_id
            WHERE
                partners.partner_name = '{partner_name}';
            """
    return pd.read_sql(query, conn)
