import json
import os
from datetime import datetime
from typing import List

import pandas as pd
import sqlalchemy as sqla
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()


def get_db_connection():
    # Define the database connection URL
    db_url = os.environ.get("DB_URL")
    assert db_url, "DB_URL environment variable is not set."
    engine = create_engine(db_url, echo=True)
    return engine.connect()


def create_tmp_table(conn: sqla.engine.base.Connection, table_name, data_dict):
    """Create a temporary table to hold the data."""
    try:
        conn.execute(text(f"DROP TABLE IF EXISTS tmp_{table_name} CASCADE;"))
        conn.execute(
            text(
                f"""CREATE TABLE tmp_{table_name} (LIKE {table_name} INCLUDING ALL);"""
            )
        )
        conn.execute(
            text(
                f"INSERT INTO tmp_{table_name} ({', '.join(data_dict.keys())}) VALUES ({', '.join([':' + col for col in data_dict.keys()])})"
            ),
            data_dict,
        )
    except Exception as e:
        raise ConnectionError(
            f"Error creating temporary table for {table_name}: {e}"
        ) from e
    return f"tmp_{table_name}"


def drop_tmp_table(conn: sqla.engine.base.Connection, tmp_table_name: str):
    """Drop the temporary table."""

    try:
        conn.execute(text(f"DROP TABLE IF EXISTS {tmp_table_name} CASCADE;"))
    except Exception as e:
        raise ConnectionError(
            f"Error dropping temporary table {tmp_table_name}: {e}"
        ) from e


def add_constraints(
    conn: sqla.engine.base.Connection, table_name: str, pk_cols: List[str]
):
    try:
        conn.execute(
            text(
                (
                    f"ALTER TABLE {table_name} ADD CONSTRAINT "
                    f"temp_constraint UNIQUE ({', '.join(pk_cols)});"
                )
            )
        )
    except Exception as e:
        raise ConnectionError(
            f"Error adding constraints to table {table_name}: {e}"
        ) from e


def remove_constraints(
    conn: sqla.engine.base.Connection,
    table_name: str,
):
    try:
        conn.execute(
            text(
                (
                    f"ALTER TABLE {table_name} "
                    f"DROP CONSTRAINT temp_constraint;"
                )
            )
        )
    except Exception as e:
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
    """Upsert data into the main table."""
    tmp_table_name = create_tmp_table(conn, table_name, data_dict=data_dict)
    try:
        conflict_cols = [
            f"{col}=EXCLUDED.{col}"
            for col in data_dict.keys()
            if col not in pk_cols
        ]

        add_constraints(conn, table_name, pk_cols)

        query = f"""
                INSERT INTO {table_name} ({', '.join(data_dict.keys())})
                SELECT {', '.join(data_dict.keys())} FROM {tmp_table_name}
                ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE
                SET {', '.join(conflict_cols)}"""
        query += f" RETURNING {returning_col};" if returning_col else ";"

        upsert_query = text(query)
        data_dict["table_name"] = table_name

        result = conn.execute(upsert_query, data_dict)
        if returning_col:
            result = result.scalar()

        remove_constraints(conn, table_name)
        drop_tmp_table(conn, tmp_table_name)

    except Exception as e:
        raise ConnectionError(
            f"Error upserting data into table {table_name}: {e}"
        ) from e

    return result


def load_order_data(
    conn: sqla.engine.base.Connection, partner_name: str
) -> pd.DataFrame:
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


if __name__ == "__main__":
    # Load the JSON data from your provided input
    # with open("./src/data/webhook/sample.json", "r") as file:
    #     input_json = file.read()

    # # Parse the JSON data
    # order_data = json.loads(input_json)

    # print(order_data["body"]["order"].keys())

    # Insert the data into the PostgreSQL database
    # try:
    #     insert_order_data(os.getenv("PARTNER1"), order_data["body"]["order"])
    # except Exception as e:
    #     print(f"Error inserting order data: {e}")

    # with open("./src/data/fetched/sample.json", "r") as file:
    #     order_data = json.load(file)

    # try:
    #     insert_order_data(os.getenv("PARTNER1"), order_data, is_webhook=False)
    # except Exception as e:
    #     print(f"Error inserting order data: {e}")

    with open(
        "./src/dynamic_pricing/data/fetched/nostimo/raw_orders.json", "r"
    ) as file:
        orders_data = json.load(file)
    with get_db_connection() as conn:
        print(len(orders_data))
        # order number 238 ends 7/8/2023
        # already processed: [298:1500]
        for order_data in orders_data[1500:]:
            try:
                insert_order_data(
                    conn, os.getenv("PARTNER1"), order_data, is_webhook=False
                )
                conn.commit()
            except Exception as e:
                print(f"Error inserting order data: {e}")
