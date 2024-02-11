from dotenv import find_dotenv, load_dotenv
import psycopg2
import os


load_dotenv(find_dotenv())

url = os.environ.get("DB_URL")
# Connect to an existing database
conn = psycopg2.connect(url, sslmode="require")

# Open a cursor to perform database operations
cur = conn.cursor()

cur.execute(
    """
    DROP TABLE IF EXISTS customers CASCADE;
    DROP TABLE IF EXISTS modifiers CASCADE;
    DROP TABLE IF EXISTS products CASCADE;
    DROP TABLE IF EXISTS items CASCADE;
    DROP TABLE IF EXISTS partners CASCADE;
    DROP TABLE IF EXISTS orders CASCADE;
    DROP TABLE IF EXISTS order_items CASCADE;
    DROP TABLE IF EXISTS order_item_modifiers CASCADE;
    """
)

cur.execute(
    """CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    contact_number VARCHAR(20),
    contact_access_code VARCHAR(20)
);"""
)

cur.execute(
    """CREATE TABLE partners (
    partner_id SERIAL PRIMARY KEY,
    partner_name VARCHAR(255) UNIQUE NOT NULL
);"""
)


cur.execute(
    """INSERT INTO partners (partner_name) VALUES (%s)""", (os.environ.get("partner1"),)
)
cur.execute(
    """INSERT INTO partners (partner_name) VALUES (%s)""", (os.environ.get("partner2"),)
)

cur.execute(
    """CREATE TABLE modifiers (
    modifier_id SERIAL PRIMARY KEY,
    deliveroo_modifier_id VARCHAR(255) NOT NULL,
    modifier_name VARCHAR(255) NOT NULL,
    modifier_operational_name VARCHAR(255) NOT NULL);
    """
)

cur.execute(
    """CREATE TABLE items (
    item_id SERIAL PRIMARY KEY,
    deliveroo_item_id VARCHAR(255) NOT NULL,
    item_name VARCHAR(255) NOT NULL,
    item_operational_name VARCHAR(255) NOT NULL);
"""
)


cur.execute(
    """CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    deliveroo_order_id VARCHAR(255) NOT NULL,
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
cur.execute(
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

cur.execute(
    """CREATE TABLE order_item_modifiers (
    order_id INT NOT NULL,
        CONSTRAINT fk_order_item_modifiers_order_id FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE SET NULL,
    item_id INT NOT NULL,
        CONSTRAINT fk_order_item_modifiers_item_id FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE SET NULL,
    modifier_id INT NOT NULL,
        CONSTRAINT fk_order_item_modifiers_modifier_id FOREIGN KEY (modifier_id) REFERENCES modifiers(modifier_id) ON DELETE SET NULL,
    quantity INT NOT NULL,
    fractional_price INT NOT NULL,
    PRIMARY KEY (order_id, item_id,modifier_id)
);"""
)

conn.commit()

cur.close()
conn.close()
