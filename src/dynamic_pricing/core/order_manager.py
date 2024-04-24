"""
This module manages the insertion and updating of customer, order, item, and
modifier data in the database for the dynamic pricing platform.
It uses SQL upserts to ensure data integrity.
"""

from datetime import datetime
import sqlalchemy as sqla
from sqlalchemy.sql import text
from dynamic_pricing.core.db_utils import upsert


def parse_datetime(date_str):
    """Helper function to parse datetime strings."""
    return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")


def insert_customer(
    conn: sqla.engine.base.Connection, customer_data: dict
) -> int:
    """
    Insert or update customer data in the 'customers' table using upsert
    functionality.
    """
    fields = ["first_name", "contact_number", "contact_access_code"]
    return upsert(
        conn,
        "customers",
        {field: customer_data[field] for field in fields},
        ["contact_number"],
        "customer_id",
    )


def insert_order(
    conn: sqla.engine.base.Connection,
    order_data: dict,
    partner_id: int,
    customer_id: int,
) -> int:
    """
    Insert or update order data in the 'orders' table using upsert functionality.
    """
    order_fields = {
        "platform_order_id": order_data["id"],
        "platform_order_number": order_data["order_number"],
        "order_status": order_data["status"],
        "order_placed_timestamp": parse_datetime(
            order_data["status_log"][0]["at"]
        ),
        "order_updated_timestamp": parse_datetime(
            order_data["status_log"][1]["at"].split(".")[0] + "Z"
        ),
        "order_prepare_for_timestamp": parse_datetime(
            order_data["prepare_for"]
        ),
        "order_start_prepping_at_timestamp": parse_datetime(
            order_data["start_preparing_at"]
        ),
        "customer_id": customer_id if customer_id != -1 else None,
        "partner_id": partner_id,
    }
    return upsert(
        conn, "orders", order_fields, ["platform_order_id"], "order_id"
    )


def insert_item(conn: sqla.engine.base.Connection, item_data: dict) -> int:
    """
    Insert or update item data in the 'items' table using upsert functionality.
    """
    item_fields = {
        "platform_item_id": item_data["pos_item_id"],
        "item_name": item_data["name"],
        "item_operational_name": item_data["operational_name"],
    }
    return upsert(conn, "items", item_fields, ["item_name"], "item_id")


def insert_modifier(
    conn: sqla.engine.base.Connection, modifier_data: dict
) -> int:
    """
    Insert or update modifier data in the 'modifiers' table using upsert
    functionality.
    """
    modifier_fields = {
        "platform_modifier_id": modifier_data["pos_item_id"],
        "modifier_name": modifier_data["name"],
        "modifier_operational_name": modifier_data["operational_name"],
    }
    return upsert(
        conn, "modifiers", modifier_fields, ["modifier_name"], "modifier_id"
    )


def insert_order_item(conn, order_id: int, item_id: int, item_data: dict):
    """
    Insert or update order item data in the 'order_items' table using upsert
    functionality.
    """
    order_item_fields = {
        "order_id": order_id,
        "item_id": item_id,
        "quantity": item_data["quantity"],
        "fractional_price": item_data["total_price"]["fractional"],
    }
    upsert(conn, "order_items", order_item_fields, ["order_id", "item_id"])


def insert_order_item_modifier(
    conn, order_id: int, item_id: int, modifier_id: int, modifier_data: dict
):
    """
    Insert or update order item modifier data in the 'order_item_modifiers'
    table using upsert functionality.
    """
    order_item_modifier_fields = {
        "order_id": order_id,
        "item_id": item_id,
        "modifier_id": modifier_id,
        "quantity": modifier_data["quantity"],
        "fractional_price": modifier_data["total_price"]["fractional"],
    }
    upsert(
        conn,
        "order_item_modifiers",
        order_item_modifier_fields,
        ["order_id", "item_id", "modifier_id"],
    )


def get_partner_id(conn, partner_name: str) -> int:
    """
    Retrieve the partner ID from the 'partners' table.
    """
    query = text(
        "SELECT partner_id FROM partners WHERE partner_name = :partner_name"
    )
    result = conn.execute(query, {"partner_name": partner_name}).scalar()
    return result or -1


def insert_order_data(
    conn, partner_name: str, order_data: dict, is_webhook=True
):
    """
    Handles the logic to insert all order related data including customer,
    order items, and modifiers.
    """
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
