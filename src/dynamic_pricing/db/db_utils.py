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



def insert_customer(
    conn: sqla.engine.base.Connection, customer_data: dict
) -> int:
    """Insert or update customer data."""
    filtered_customer_data = {
        "first_name": customer_data["first_name"],
        "contact_number": customer_data["contact_number"],
        "contact_access_code": customer_data["contact_access_code"],
    }
    return upsert(
        conn,
        "customers",
        filtered_customer_data,
        ["contact_number"],
        "customer_id",
    )


def insert_order(
    conn: sqla.engine.base.Connection,
    order_data: dict,
    partner_id: int,
    customer_id: int,
) -> int:
    """Insert or update order data."""
    filtered_order_data = {
        "platform_order_id": order_data["id"],
        "platform_order_number": order_data["order_number"],
        "order_status": order_data["status"],
        "order_placed_timestamp": datetime.strptime(
            order_data["status_log"][0]["at"], "%Y-%m-%dT%H:%M:%SZ"
        ),
        "order_updated_timestamp": datetime.strptime(
            order_data["status_log"][1]["at"].split(".")[0] + "Z",
            "%Y-%m-%dT%H:%M:%SZ",
        ),
        "order_prepare_for_timestamp": datetime.strptime(
            order_data["prepare_for"],
            "%Y-%m-%dT%H:%M:%SZ",
        ),
        "order_start_prepping_at_timestamp": datetime.strptime(
            order_data["start_preparing_at"],
            "%Y-%m-%dT%H:%M:%SZ",
        ),
        "customer_id": customer_id if customer_id != -1 else None,
        "partner_id": partner_id,
    }
    return upsert(
        conn, "orders", filtered_order_data, ["platform_order_id"], "order_id"
    )


def insert_item(conn: sqla.engine.base.Connection, item_data: dict) -> int:
    """Insert or update item data."""
    filtered_item_data = {
        "platform_item_id": item_data["pos_item_id"],
        "item_name": item_data["name"],
        "item_operational_name": item_data["operational_name"],
    }
    return upsert(
        conn,
        "items",
        filtered_item_data,
        [
            "item_name"
        ],  #!TODO: will need to change this to actual pk or platform_item_id
        "item_id",
    )


def insert_modifier(
    conn: sqla.engine.base.Connection, modifier_data: dict
) -> int:
    """Insert or update modifier data."""
    filtered_modifier_data = {
        "platform_modifier_id": modifier_data["pos_item_id"],
        "modifier_name": modifier_data["name"],
        "modifier_operational_name": modifier_data["operational_name"],
    }
    return upsert(
        conn,
        "modifiers",
        filtered_modifier_data,
        [
            "modifier_name"
        ],  #!TODO: will need to change this to actual pk or platform_modifier_id
        "modifier_id",
    )


def insert_order_item(
    conn, order_id: int, item_id: int, item_data: dict
) -> None:
    """Insert order item data."""
    filtered_order_item_data = {
        "order_id": order_id,
        "item_id": item_id,
        "quantity": item_data["quantity"],
        "fractional_price": item_data["total_price"]["fractional"],
    }
    upsert(
        conn,
        table_name="order_items",
        data_dict=filtered_order_item_data,
        pk_cols=["order_id", "item_id"],
    )


def insert_order_item_modifier(
    conn, order_id: int, item_id: int, modifier_id: int, modifier_data: dict
) -> None:
    """Insert order item modifier data."""
    filtered_order_item_modifier_data = {
        "order_id": order_id,
        "item_id": item_id,
        "modifier_id": modifier_id,
        "quantity": modifier_data["quantity"],
        "fractional_price": modifier_data["total_price"]["fractional"],
    }
    upsert(
        conn,
        table_name="order_item_modifiers",
        data_dict=filtered_order_item_modifier_data,
        pk_cols=["order_id", "item_id", "modifier_id"],
    )


def get_partner_id(conn, partner_name: str) -> int:
    """Get the partner ID from the database."""

    query = text(
        "SELECT partner_id FROM partners WHERE partner_name = :partner_name"
    )
    result = conn.execute(query, {"partner_name": partner_name}).scalar()
    return result or -1


def insert_order_data(
    conn, partner_name: str, order_data: dict, is_webhook=True
) -> None:
    """Insert webhook order into the database."""

    # this statement is leveraging the understanding that the webhook order data tends to have a customer id, but the stored data doesn't
    if is_webhook:
        customer_id = insert_customer(conn, order_data["customer"])
        order_id = insert_order(
            conn, order_data, order_data["location_id"], customer_id
        )
    else:
        partner_id = get_partner_id(conn, partner_name)
        if partner_id == -1:
            raise ValueError(
                f"Partner {partner_name} does not exist in the database."
            )
        order_id = insert_order(conn, order_data, partner_id, -1)

    for item_data in order_data["items"]:
        item_id = insert_item(conn, item_data)
        insert_order_item(conn, order_id, item_id, item_data)

        for modifier_data in item_data["modifiers"]:

            modifier_id = insert_modifier(conn, modifier_data)
            insert_order_item_modifier(
                conn, order_id, item_id, modifier_id, modifier_data
            )
