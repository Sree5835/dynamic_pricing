"""
This module manages the insertion and updating of customer, order, item, and modifier data
in the database for the dynamic pricing platform. It uses SQL upserts to ensure data integrity.
"""

from datetime import datetime
import sqlalchemy as sqla
from sqlalchemy.sql import text
from dynamic_pricing.db.db_utils import upsert


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
