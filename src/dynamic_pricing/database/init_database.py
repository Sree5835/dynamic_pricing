from dotenv import find_dotenv, load_dotenv
import psycopg2
import os


load_dotenv(find_dotenv())

# Connect to an existing database
conn = psycopg2.connect(os.environ.get("DB_URL"), sslmode="require")

# Open a cursor to perform database operations
cur = conn.cursor()

cur.execute(
    """
    DROP TABLE IF EXISTS customers CASCADE;
    DROP TABLE IF EXISTS modifiers CASCADE;
    DROP TABLE IF EXISTS products CASCADE;
    DROP TABLE IF EXISTS items CASCADE;
    DROP TABLE IF EXISTS addresses CASCADE;
    DROP TABLE IF EXISTS delivery_addresses CASCADE;
    DROP TABLE IF EXISTS partners CASCADE;
    DROP TABLE IF EXISTS orders CASCADE;
    DROP TABLE IF EXISTS order_items CASCADE;
    DROP TABLE IF EXISTS order_item_modifiers CASCADE;
    """
)

cur.execute(
    """CREATE TABLE customers (
    customer_id SERIAL UNIQUE PRIMARY KEY,
    first_name VARCHAR(255),
    contact_number VARCHAR(20),
    contact_access_code VARCHAR(20)
);"""
)

cur.execute(
    """CREATE TABLE modifiers (
    modifier_id SERIAL UNIQUE PRIMARY KEY,
    deliveroo_modifier_id VARCHAR(255) NOT NULL,
    modifier_name VARCHAR(255) NOT NULL,
    modifier_operational_name VARCHAR(255) NOT NULL
);"""
)

cur.execute(
    """CREATE TABLE items (
    item_id SERIAL UNIQUE PRIMARY KEY,
    deliveroo_item_id VARCHAR(255) NOT NULL,
    item_name VARCHAR(255) NOT NULL,
    item_operational_name VARCHAR(255) NOT NULL);"""
)

cur.execute(
    """CREATE TABLE addresses (
    address_id SERIAL UNIQUE PRIMARY KEY,
    line_1 VARCHAR(255),
    line_2 VARCHAR(255),
    postcode VARCHAR(255)
);"""
)

cur.execute(
    """CREATE TABLE delivery_addresses (
    customer_id SERIAL UNIQUE PRIMARY KEY,
        CONSTRAINT fk_delivery_addresses_customer_id FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE SET NULL,
    address_id SERIAL NOT NULL,
        CONSTRAINT fk_delivery_addresses_address_id FOREIGN KEY (address_id) REFERENCES addresses(address_id) ON DELETE SET NULL
);"""
)

cur.execute(
    """CREATE TABLE partners (
    partner_id SERIAL UNIQUE PRIMARY KEY,
    partner_name VARCHAR(255) NOT NULL
);"""
)

cur.execute("""INSERT INTO partners (partner_name) VALUES (%s)""", ("Nostimo",))
cur.execute("""INSERT INTO partners (partner_name) VALUES (%s)""", ("Drummond Villa",))


cur.execute(
    """CREATE TABLE orders (
    order_id SERIAL UNIQUE PRIMARY KEY,
    deliveroo_order_id VARCHAR(255) NOT NULL,
    deliveroo_order_number BIGINT NOT NULL,
    order_status VARCHAR(255) NOT NULL,
    order_timestamp TIMESTAMP NOT NULL,
    customer_id SERIAL NOT NULL,
        CONSTRAINT fk_orders_customer_id FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE SET NULL,
    partner_id SERIAL NOT NULL,
        CONSTRAINT fk_orders_partner_id FOREIGN KEY (partner_id) REFERENCES partners(partner_id) ON DELETE SET NULL
);"""
)
cur.execute(
    """CREATE TABLE order_items (
    order_id SERIAL UNIQUE PRIMARY KEY,
        CONSTRAINT fk_order_items_order_id FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE SET NULL,
    item_id SERIAL NOT NULL,
        CONSTRAINT fk_order_items_item_id FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE SET NULL,
    quantity INT NOT NULL,
    fractional_price INT NOT NULL
);"""
)

cur.execute(
    """CREATE TABLE order_item_modifiers (
    order_id SERIAL UNIQUE PRIMARY KEY,
        CONSTRAINT fk_order_item_modifiers_order_id FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE SET NULL,
    item_id SERIAL NOT NULL,
        CONSTRAINT fk_order_item_modifiers_item_id FOREIGN KEY (item_id) REFERENCES items(item_id) ON DELETE SET NULL,
    modifier_id SERIAL NOT NULL,
        CONSTRAINT fk_order_item_modifiers_modifier_id FOREIGN KEY (modifier_id) REFERENCES modifiers(modifier_id) ON DELETE SET NULL,
    quantity INT NOT NULL,
    fractional_price INT NOT NULL
);"""
)

conn.commit()

cur.close()
conn.close()
