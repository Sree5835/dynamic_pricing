from dotenv import load_dotenv
import psycopg2
import os


def connect_to_database() -> psycopg2.extensions.connection:
    """Connect to the PostgreSQL database."""
    load_dotenv()
    db_url = os.getenv("DB_URL")
    if db_url is None:
        raise ValueError("DB_URL environment variable is not set")

    return psycopg2.connect(db_url, sslmode="require")


def create_tables(connection: psycopg2.extensions.connection) -> None:
    """Create tables in the PostgreSQL database."""
    with connection.cursor() as cursor:
        cursor.execute(
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

        cursor.execute(
            """CREATE TABLE customers (
            customer_id SERIAL PRIMARY KEY,
            first_name VARCHAR(255),
            contact_number VARCHAR(20) UNIQUE NOT NULL,
            contact_access_code VARCHAR(20)
            );"""
        )

        cursor.execute(
            """CREATE TABLE partners (
            partner_id SERIAL PRIMARY KEY,
            partner_name VARCHAR(255) UNIQUE NOT NULL
            );"""
        )

        cursor.execute(
            """INSERT INTO partners (partner_name) VALUES (%s)""",
            (os.getenv("PARTNER1"),),
        )
        cursor.execute(
            """INSERT INTO partners (partner_name) VALUES (%s)""",
            (os.getenv("PARTNER2"),),
        )

        cursor.execute(
            """CREATE TABLE modifiers (
            modifier_id SERIAL PRIMARY KEY,
            deliveroo_modifier_id VARCHAR(255) UNIQUE NOT NULL,
            modifier_name VARCHAR(255) NOT NULL,
            modifier_operational_name VARCHAR(255) NOT NULL
            );"""
        )

        cursor.execute(
            """CREATE TABLE items (
            item_id SERIAL PRIMARY KEY,
            deliveroo_item_id VARCHAR(255) UNIQUE NOT NULL,
            item_name VARCHAR(255) NOT NULL,
            item_operational_name VARCHAR(255) NOT NULL
            );"""
        )

        cursor.execute(
            """CREATE TABLE orders (
            order_id SERIAL PRIMARY KEY,
            deliveroo_order_id VARCHAR(255) UNIQUE NOT NULL,
            deliveroo_order_number BIGINT NOT NULL,
            order_status VARCHAR(255) NOT NULL,
            order_placed_timestamp TIMESTAMP NOT NULL,
            order_updated_timestamp TIMESTAMP,
            customer_id INT NOT NULL,
            CONSTRAINT fk_orders_customer_id FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE SET NULL,
            partner_id INT NOT NULL,
            CONSTRAINT fk_orders_partner_id FOREIGN KEY (partner_id) REFERENCES partners(partner_id) ON DELETE SET NULL
            );"""
        )

        cursor.execute(
            """CREATE TABLE order_items (
            order_id INT NOT NULL,
            CONSTRAINT fk_order_items_order_id FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE SET NULL,
            item_id INT NOT NULL,
            CONSTRAINT fk_order_items_item_id FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE SET NULL,
            quantity INT NOT NULL,
            fractional_price INT NOT NULL,
            PRIMARY KEY (order_id, item_id)
            );"""
        )

        cursor.execute(
            """CREATE TABLE order_item_modifiers (
            order_id INT NOT NULL,
            CONSTRAINT fk_order_item_modifiers_order_id FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE SET NULL,
            item_id INT NOT NULL,
            CONSTRAINT fk_order_item_modifiers_item_id FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE SET NULL,
            modifier_id INT NOT NULL,
            CONSTRAINT fk_order_item_modifiers_modifier_id FOREIGN KEY (modifier_id) REFERENCES modifiers(modifier_id) ON DELETE SET NULL,
            quantity INT NOT NULL,
            fractional_price INT NOT NULL,
            PRIMARY KEY (order_id, item_id, modifier_id)
            );"""
        )

        connection.commit()


def main() -> None:
    """Main function."""
    try:
        connection = connect_to_database()
        create_tables(connection)
        if "connection" in locals():
            connection.close()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
