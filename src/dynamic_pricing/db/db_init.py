"""
This module is responsible for initializing the database schema for the dynamic pricing system.
It creates necessary tables and sets up relationships between them.
"""

import os
import sqlalchemy as sqla
from dotenv import load_dotenv
from sqlalchemy import text
from dynamic_pricing.db.db_utils import get_db_connection

load_dotenv()


def create_tables(connection: sqla.engine.base.Connection) -> None:
    """Create tables in the PostgreSQL database."""
    # Drop existing tables
    connection.execute(
        text(
            """
            DROP TABLE IF EXISTS order_item_modifiers CASCADE;
            DROP TABLE IF EXISTS order_items CASCADE;
            DROP TABLE IF EXISTS orders CASCADE;
            DROP TABLE IF EXISTS partners CASCADE;
            DROP TABLE IF EXISTS items CASCADE;
            DROP TABLE IF EXISTS modifiers CASCADE;
            DROP TABLE IF EXISTS customers CASCADE;
            """
        )
    )

    # Create customers table
    connection.execute(
        text(
            """CREATE TABLE customers (
                customer_id SERIAL PRIMARY KEY,
                first_name VARCHAR(255),
                contact_number VARCHAR(20) NOT NULL,
                contact_access_code VARCHAR(20)
               );"""
        )
    )

    # Create partners table and insert initial data
    partners_table_sql = text(
        """CREATE TABLE partners (
            partner_id SERIAL PRIMARY KEY,
            partner_name VARCHAR(255) NOT NULL
           );"""
    )
    connection.execute(partners_table_sql)

    insert_partners_sql = text(
        """INSERT INTO partners (partner_name) VALUES (:partner1), (:partner2)"""
    )
    connection.execute(
        insert_partners_sql,
        {"partner1": os.getenv("PARTNER1"), "partner2": os.getenv("PARTNER2")},
    )

    # Create modifiers table
    connection.execute(
        text(
            """CREATE TABLE modifiers (
                modifier_id SERIAL PRIMARY KEY,
                platform_modifier_id VARCHAR(255) NOT NULL,
                modifier_name VARCHAR(255) NOT NULL,
                modifier_operational_name VARCHAR(255) NOT NULL,
                modifier_fractional_cost INT
               );"""
        )
    )

    # Create items table
    connection.execute(
        text(
            """CREATE TABLE items (
                item_id SERIAL PRIMARY KEY,
                platform_item_id VARCHAR(255) NOT NULL,
                item_name VARCHAR(255) NOT NULL,
                item_operational_name VARCHAR(255) NOT NULL,
                item_fractional_cost INT
               );"""
        )
    )

    # Create orders table
    connection.execute(
        text(
            """CREATE TABLE orders (
                order_id SERIAL PRIMARY KEY,
                platform_order_id VARCHAR(255) UNIQUE NOT NULL,
                platform_order_number BIGINT NOT NULL,
                order_status VARCHAR(255) NOT NULL,
                order_placed_timestamp TIMESTAMP NOT NULL,
                order_updated_timestamp TIMESTAMP,
                order_prepare_for_timestamp TIMESTAMP,
                order_start_prepping_at_timestamp TIMESTAMP,
                customer_id INT,
                CONSTRAINT fk_orders_customer_id FOREIGN KEY (customer_id)
                    REFERENCES customers(customer_id) ON DELETE SET NULL,
                partner_id INT NOT NULL,
                CONSTRAINT fk_orders_partner_id FOREIGN KEY (partner_id)
                    REFERENCES partners(partner_id) ON DELETE SET NULL
               );"""
        )
    )

    # Create order_items and order_item_modifiers tables
    connection.execute(
        text(
            """CREATE TABLE order_items (
                order_id INT NOT NULL,
                item_id INT NOT NULL,
                quantity INT NOT NULL,
                fractional_price INT NOT NULL,
                PRIMARY KEY (order_id, item_id),
                CONSTRAINT fk_order_items_order_id FOREIGN KEY (order_id)
                    REFERENCES orders(order_id) ON DELETE SET NULL,
                CONSTRAINT fk_order_items_item_id FOREIGN KEY (item_id)
                    REFERENCES items(item_id) ON DELETE SET NULL
               );"""
        )
    )

    connection.execute(
        text(
            """CREATE TABLE order_item_modifiers (
                order_id INT NOT NULL,
                item_id INT NOT NULL,
                modifier_id INT NOT NULL,
                quantity INT NOT NULL,
                fractional_price INT NOT NULL,
                PRIMARY KEY (order_id, item_id, modifier_id),
                CONSTRAINT fk_order_item_modifiers_order_id FOREIGN KEY (order_id)
                    REFERENCES orders(order_id) ON DELETE SET NULL,
                CONSTRAINT fk_order_item_modifiers_item_id FOREIGN KEY (item_id)
                    REFERENCES items(item_id) ON DELETE SET NULL,
                CONSTRAINT fk_order_item_modifiers_modifier_id FOREIGN KEY (modifier_id)
                    REFERENCES modifiers(modifier_id) ON DELETE SET NULL
               );"""
        )
    )

    connection.commit()


if __name__ == "__main__":
    try:
        with get_db_connection() as connection:
            create_tables(connection)
    except sqla.exc.SQLAlchemyError as e:
        print(f"Error: {e}")
