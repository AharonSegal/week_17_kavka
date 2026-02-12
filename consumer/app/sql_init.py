# creates the tables 

from mysql_connection import db

create_customers_table_sql = """
CREATE TABLE IF NOT EXISTS customers (
  customerNumber INT NOT NULL,
  customerName VARCHAR(255) NULL,
  contactLastName VARCHAR(255) NULL,
  contactFirstName VARCHAR(255) NULL,
  phone VARCHAR(50) NULL,
  addressLine1 VARCHAR(255) NULL,
  addressLine2 VARCHAR(255) NULL,
  city VARCHAR(100) NULL,
  state VARCHAR(100) NULL,
  postalCode VARCHAR(20) NULL,
  country VARCHAR(100) NULL,
  salesRepEmployeeNumber INT NULL,
  creditLimit DECIMAL(12,2) NULL,
  PRIMARY KEY (customerNumber)
);
"""

# TODO TEST FOR FK
#   CONSTRAINT orders_customerNumber_fk FOREIGN KEY (customerNumber) REFERENCES customers(customerNumber)

create_orders_table_sql = """
CREATE TABLE IF NOT EXISTS orders (
  orderNumber INT NOT NULL,
  orderDate DATE NULL,
  requiredDate DATE NULL,
  shippedDate DATE NULL,
  status VARCHAR(50) NULL,
  comments TEXT NULL,
  customerNumber INT NOT NULL,
  PRIMARY KEY (orderNumber),
);
"""

def ensure_schema():
    # create tables in mysql if not exist
    db.execute(create_customers_table_sql)
    db.execute(create_orders_table_sql)

