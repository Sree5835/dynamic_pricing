from datetime import datetime
import json
import psycopg2
import os
from dotenv import find_dotenv, load_dotenv


class DatabaseManager:
    def __init__(self):
        # Load environment variables from a .env file
        load_dotenv(find_dotenv())
        self.conn = None

    def connect(self):
        try:
            # Connect to the PostgreSQL database
            self.conn = psycopg2.connect(os.environ.get("DB_URL"), sslmode="require")
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL:", error)

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()

    def close_connection(self):
        if self.conn:
            self.conn.close()

    def insert_customer(self, customer_data):
        if self.conn:
            cur = self.conn.cursor()
            if existing_customer_id := self.get_customer_id_by_contact_number(
                customer_data["contact_number"]
            ):
                return existing_customer_id

            cur.execute(
                "INSERT INTO customers (first_name, contact_number, contact_access_code) VALUES (%s, %s, %s) RETURNING customer_id",
                (
                    customer_data["first_name"],
                    customer_data["contact_number"],
                    customer_data["contact_access_code"],
                ),
            )
            return cur.fetchone()[0]

    def get_customer_id_by_contact_number(self, contact_number):
        if self.conn:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT customer_id FROM customers WHERE contact_number = %s",
                (contact_number,),
            )
            return result[0] if (result := cur.fetchone()) else None

    def insert_address(self, address_data):
        if self.conn:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO addresses (line_1, line_2, postcode) VALUES (%s, %s, %s) RETURNING address_id",
                (
                    address_data["line_1"],
                    address_data["line_2"],
                    address_data["postcode"],
                ),
            )
            return cur.fetchone()[0]

    def insert_delivery_address(self, customer_id, address_id):
        if self.conn:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO delivery_addresses (customer_id, address_id) VALUES (%s, %s)",
                (customer_id, address_id),
            )

    def insert_order(self, order_data, customer_id, partner_id):
        if self.conn:
            cur = self.conn.cursor()
            timestamp_str = order_data["status_log"][0]["at"]
            cur.execute(
                "INSERT INTO orders (deliveroo_order_id, deliveroo_order_number, order_status, order_timestamp, customer_id, partner_id) VALUES (%s, %s, %s, %s, %s, %s) RETURNING order_id",
                (
                    order_data["id"],
                    order_data["order_number"],
                    order_data["status"],
                    datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")),
                    customer_id,
                    partner_id,
                ),
            )
            return cur.fetchone()[0]

    def item_exists(self, item_id):
        if self.conn:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT item_id FROM items WHERE deliveroo_item_id = %s",
                (item_id,),
            )
            return cur.fetchone()[0]

    def insert_item(self, item_id, name, operational_name):
        if self.conn:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO items (deliveroo_item_id, item_name, item_operational_name) VALUES (%s, %s, %s) RETURNING item_id",
                (item_id, name, operational_name),
            )
            return cur.fetchone()[0]

    def insert_order_items(self, order_id, items):
        with self.conn:
            cur = self.conn.cursor()
            for item in items:
                deliveroo_item_id = item["pos_item_id"]
                quantity = item["quantity"]
                price = item["total_price"]["fractional"]
                item_id = self.item_exists(deliveroo_item_id)

                if item_id is None:
                    item_id = self.insert_item(
                        deliveroo_item_id, item["name"], item["operational_name"]
                    )
                assert item_id is not None
                cur.execute(
                    "INSERT INTO order_items (order_id, item_id, quantity,fractional_price) VALUES (%s, %s, %s, %s)",
                    (order_id, item_id, quantity, price),
                )

    def modifier_exists(self, modifier_id):
        if self.conn:
            cur = self.conn.cursor()
            cur.execute(
                "SELECT modifier_id FROM modifiers WHERE deliveroo_modifier_id = %s",
                (modifier_id,),
            )
            return cur.fetchone()[0]

    def insert_modifier(self, modifier_id, name, operational_name):
        if self.conn:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO modifiers (deliveroo_modifier_id, modifier_name, modifier_operational_name) VALUES (%s, %s, %s) RETURNING modifier_id",
                (modifier_id, name, operational_name),
            )
            return cur.fetchone()[0]

    def insert_order_item_modifiers(self, order_id, item_id, modifiers):
        with self.conn:
            cur = self.conn.cursor()
            for modifier in modifiers:
                deliveroo_modifier_id = modifier["id"]
                modifier_quantity = modifier["quantity"]
                modifier_price = modifier["total_price"]["fractional"]
                modifier_id = self.modifier_exists(deliveroo_modifier_id)
                if modifier_id is None:
                    modifier_id = self.insert_modifier(
                        deliveroo_modifier_id,
                        modifier["name"],
                        modifier["operational_name"],
                    )
                assert modifier_id is not None
                cur.execute(
                    "INSERT INTO order_item_modifiers (order_id, item_id, modifier_id, quantity,fractional_price) VALUES (%s, %s, %s, %s,%s)",
                    (order_id, item_id, modifier_id, modifier_quantity, modifier_price),
                )


def insert_order_data(order_data):
    try:
        # Create a DatabaseManager instance
        db_manager = DatabaseManager()
        db_manager.connect()

        # Insert customer data

        customer_id = db_manager.insert_customer(
            order_data["body"]["order"]["customer"]
        )
        print("customer_id:", customer_id)

        # Insert address data
        if "delivery_address" in order_data["body"]["order"].keys():
            address_id = db_manager.insert_address(
                order_data["body"]["order"]["delivery_address"]
            )
            print("address_id:", address_id)

            # Insert delivery address data
            db_manager.insert_delivery_address(customer_id, address_id)

        # Insert order data
        partner_id = order_data["body"]["order"]["location_id"]

        order_id = db_manager.insert_order(
            order_data["body"]["order"], customer_id, partner_id
        )
        print("order_id:", order_id)

        # Insert order items and modifiers
        db_manager.insert_order_items(order_id, order_data["body"]["order"]["items"])

        print("order_items added")
        for item in order_data["body"]["order"]["items"]:
            db_manager.insert_order_item_modifiers(
                order_id, item["pos_item_id"], item["modifiers"]
            )
        print("order_item_modifiers added")
        db_manager.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while inserting data:", error)
    finally:
        db_manager.close_connection()


if __name__ == "__main__":
    # Load the JSON data from your provided input
    input_json = """(The JSON data you provided)"""

    # Parse the JSON data
    order_data = json.loads(input_json)

    # Insert the data into the PostgreSQL database
    insert_order_data(order_data)
