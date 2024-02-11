from datetime import datetime
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, session, sessionmaker
import os

# Define the database connection URL
db_url = os.environ.get("DB_URL")
engine = create_engine(db_url, echo=True)

Session = sessionmaker(bind=engine)


def create_tmp_table(table_name, data_dict):
    """Create a temporary table to hold the data."""
    tmp_table_name = f"tmp_{table_name}"
    with Session() as session:
        drop_table = text(f"DROP TABLE IF EXISTS {tmp_table_name} CASCADE;")
        create_table = text(
            f"""CREATE TABLE {tmp_table_name} (LIKE {table_name} INCLUDING ALL);"""
        )
        insert_data = text(
            f"INSERT INTO {tmp_table_name} ({', '.join(data_dict.keys())}) VALUES ({', '.join([':' + col for col in data_dict.keys()])})"
        )
        session.execute(drop_table)
        session.execute(create_table)
        session.execute(insert_data, data_dict)

        session.commit()
    return tmp_table_name


def drop_tmp_table(tmp_table_name):
    """Drop the temporary table."""
    with Session() as session:
        session.execute(text(f"DROP TABLE IF EXISTS {tmp_table_name} CASCADE;"))
        session.commit()


def upsert(table_name, data_dict, pk_cols, returning_col=None):
    """Upsert data into the main table."""
    tmp_table_name = create_tmp_table(table_name, data_dict=data_dict)
    try:
        with Session() as session:
            conflict_cols = [
                f"{col}=EXCLUDED.{col}"
                for col in data_dict.keys()
                if col not in pk_cols
            ]

            query = f"""
                INSERT INTO {table_name} ({', '.join(data_dict.keys())})
                SELECT {', '.join(data_dict.keys())} FROM {tmp_table_name}
                ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE
                SET {', '.join(conflict_cols)}"""
            query += f" RETURNING {returning_col};" if returning_col else ";"

            upsert_query = text(query)
            data_dict["table_name"] = table_name

            result = session.execute(upsert_query, data_dict)
            if returning_col:
                result = result.scalar()
            session.commit()
    except Exception as e:
        raise Exception(f"Error upserting data into table {table_name}: {e}") from e
    finally:
        drop_tmp_table(tmp_table_name)
    return result


def insert_customer(customer_data: dict) -> int:
    """Insert or update customer data."""
    filtered_customer_data = {
        "first_name": customer_data["first_name"],
        "contact_number": customer_data["contact_number"],
        "contact_access_code": customer_data["contact_access_code"],
    }
    return upsert(
        "customers", filtered_customer_data, ["contact_number"], "customer_id"
    )


def insert_order(order_data: dict, customer_id: int) -> int:
    """Insert or update order data."""
    filtered_order_data = {
        "deliveroo_order_id": order_data["id"],
        "deliveroo_order_number": order_data["order_number"],
        "order_status": order_data["status"],
        "order_placed_timestamp": datetime.strptime(
            order_data["status_log"][0]["at"], "%Y-%m-%dT%H:%M:%SZ"
        ),
        "order_updated_timestamp": datetime.strptime(
            order_data["status_log"][1]["at"].split(".")[0] + "Z",
            "%Y-%m-%dT%H:%M:%SZ",
        ),
        "customer_id": customer_id,
        "partner_id": order_data["location_id"],
    }
    return upsert("orders", filtered_order_data, ["deliveroo_order_id"], "order_id")


def insert_item(item_data: dict) -> int:
    """Insert or update item data."""
    filtered_item_data = {
        "deliveroo_item_id": item_data["pos_item_id"],
        "item_name": item_data["name"],
        "item_operational_name": item_data["operational_name"],
    }
    return upsert(
        "items",
        filtered_item_data,
        ["deliveroo_item_id"],
        "item_id",
    )


def insert_modifier(modifier_data: dict) -> int:
    """Insert or update modifier data."""
    filtered_modifier_data = {
        "deliveroo_modifier_id": modifier_data["id"],
        "modifier_name": modifier_data["name"],
        "modifier_operational_name": modifier_data["operational_name"],
    }
    return upsert(
        "modifiers",
        modifier_data,
        ["deliveroo_modifier_id"],
        "modifier_id",
    )


def insert_order_item(order_id: int, item_id: int, item_data: dict) -> None:
    """Insert order item data."""
    order_item_data = {
        "order_id": order_id,
        "item_id": item_id,
        "quantity": item_data["quantity"],
        "fractional_price": item_data["total_price"]["fractional"],
    }
    upsert(
        table_name="order_items",
        data_dict=order_item_data,
        pk_cols=["order_id", "item_id"],
    )


def insert_order_item_modifier(
    order_id: int, item_id: int, modifier_data: dict
) -> None:
    """Insert order item modifier data."""
    order_item_modifier_data = {
        "order_id": order_id,
        "item_id": item_id,
        "modifier_id": modifier_data["modifier_id"],
        "quantity": modifier_data["quantity"],
        "fractional_price": modifier_data["total_price"]["fractional"],
    }
    upsert(
        table_name="order_item_modifiers",
        data_dict=order_item_modifier_data,
        pk_cols=["order_id", "item_id", "modifier_id"],
    )


def insert_webhook_order(partner_name: str, order_data: dict) -> None:
    """Insert webhook order into the database."""
    customer_id = insert_customer(order_data["customer"])
    order_id = insert_order(order_data, customer_id)

    for item_data in order_data["items"]:
        item_id = insert_item(item_data)
        insert_order_item(order_id, item_id, item_data)

        for modifier_data in item_data["modifiers"]:
            modifier_id = insert_modifier(modifier_data)
            insert_order_item_modifier(order_id, item_id, modifier_data)


if __name__ == "__main__":
    # Load the JSON data from your provided input
    with open("./src/data/webhook/sample.json", "r") as file:
        input_json = file.read()

    # Parse the JSON data
    order_data = json.loads(input_json)

    # print(order_data["body"]["order"].keys())

    # Insert the data into the PostgreSQL database
    insert_webhook_order("Nostimo", order_data["body"]["order"])
