from datetime import datetime
import json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
import os

# Define the database connection URL
db_url = os.environ.get("DB_URL")
engine = create_engine(db_url, echo=True)


def upsert(table_name, data_dict, pk_cols):
    with Session(engine) as session:
        try:

            # Create a temporary table to hold the data
            tmp_table_name = f"tmp_{table_name}"
            values = ", ".join([f":{col}" for col in data_dict.keys()])

            temp_query = text(
                f"""DROP TABLE IF EXISTS {tmp_table_name} CASCADE;
                CREATE TABLE {tmp_table_name} (LIKE {table_name} INCLUDING ALL);
                INSERT INTO {tmp_table_name} ({', '.join(data_dict.keys())}) VALUES ({values})"""
            )
            session.execute(temp_query, data_dict)
            session.commit()

            # Create a list of the columns to be updated
            conflict_cols = [
                f"{col}=EXCLUDED.{col}"
                for col in data_dict.keys()
                if col not in pk_cols
            ]

            # Create the upsert query
            upsert_query = text(
                f"""
                INSERT INTO {table_name} ({', '.join(data_dict.keys())})
                SELECT * FROM {tmp_table_name}
                ON CONFLICT ({', '.join(pk_cols)}) DO UPDATE
                SET {', '.join(conflict_cols)}
                RETURNING ({', '.join(pk_cols)});
                """
            )
            data_dict["table_name"] = table_name

            # Execute the query
            result = session.execute(upsert_query, data_dict)
            result = result.scalar()
            session.commit()

        except Exception as e:
            # sourcery skip: raise-specific-error
            raise Exception(f"Error upserting data into table {table_name}: {e}") from e

        finally:
            # Drop the temporary table
            session.execute(text(f"DROP TABLE IF EXISTS {tmp_table_name} CASCADE;"))
            session.commit()

    return result


def insert_webhook_order(partner_name, order_data):
    with Session(engine) as session:
        # Customer
        customer_data = order_data["customer"]
        customer_query = text(
            "INSERT INTO customers (first_name, contact_number, contact_access_code) "
            "VALUES (:first_name, :contact_number, :contact_access_code) RETURNING customer_id"
        )
        result = session.execute(
            customer_query,
            {
                "first_name": customer_data["first_name"],
                "contact_number": customer_data["contact_number"],
                "contact_access_code": customer_data["contact_access_code"],
            },
        )
        customer_id = result.scalar()

        # Order
        order_query = text(
            "INSERT INTO orders (deliveroo_order_id, deliveroo_order_number, order_status, "
            "order_placed_timestamp, order_updated_timestamp, customer_id, partner_id) VALUES "
            "(:deliveroo_order_id, :deliveroo_order_number, :order_status, :order_placed_timestamp, :order_updated_timestamp,"
            ":customer_id, :partner_id) RETURNING order_id"
        )
        result = session.execute(
            order_query,
            {
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
            },
        )
        order_id = result.scalar()

        # Order Items
        for item_data in order_data["items"]:
            item_query = text(
                "INSERT INTO items (deliveroo_item_id, item_name, item_operational_name) "
                "VALUES (:deliveroo_item_id, :item_name, :item_operational_name) RETURNING item_id"
            )
            result = session.execute(
                item_query,
                {
                    "deliveroo_item_id": item_data["pos_item_id"],
                    "item_name": item_data["name"],
                    "item_operational_name": item_data["operational_name"],
                },
            )
            item_id = result.scalar()

            order_item_query = text(
                "INSERT INTO order_items (order_id, item_id, quantity, fractional_price) "
                "VALUES (:order_id, :item_id, :quantity, :fractional_price)"
            )
            session.execute(
                order_item_query,
                {
                    "order_id": order_id,
                    "item_id": item_id,
                    "quantity": item_data["quantity"],
                    "fractional_price": item_data["total_price"]["fractional"],
                },
            )

            # Order Item Modifiers
            for modifier_data in item_data["modifiers"]:
                modifier_query = text(
                    "INSERT INTO modifiers (deliveroo_modifier_id, modifier_name, modifier_operational_name) "
                    "VALUES (:deliveroo_modifier_id, :modifier_name, :modifier_operational_name) "
                    "RETURNING modifier_id"
                )
                result = session.execute(
                    modifier_query,
                    {
                        "deliveroo_modifier_id": modifier_data["id"],
                        "modifier_name": modifier_data["name"],
                        "modifier_operational_name": modifier_data["operational_name"],
                    },
                )
                modifier_id = result.scalar()

                order_item_modifier_query = text(
                    "INSERT INTO order_item_modifiers (order_id, item_id, modifier_id, quantity, fractional_price) "
                    "VALUES (:order_id, :item_id, :modifier_id, :quantity, :fractional_price)"
                )
                session.execute(
                    order_item_modifier_query,
                    {
                        "order_id": order_id,
                        "item_id": item_id,
                        "modifier_id": modifier_id,
                        "quantity": modifier_data["quantity"],
                        "fractional_price": modifier_data["total_price"]["fractional"],
                    },
                )

        session.commit()


if __name__ == "__main__":
    # Load the JSON data from your provided input
    with open("./src/dynamic_pricing/db/sample.json", "r") as file:
        input_json = file.read()

    # Parse the JSON data
    order_data = json.loads(input_json)

    print(order_data["body"]["order"].keys())

    # Insert the data into the PostgreSQL database
    insert_webhook_order("Nostimo", order_data["body"]["order"])
