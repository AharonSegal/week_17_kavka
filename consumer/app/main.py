# TODO: THIS SECTION NEEDS TO BE REGULAR PY NOT FASTAPI
# 1- read from kafka
# 2 sort by type
# 3 enters to sql -> add tables and stuff needed 

import json
import os
import time

from sql_init import ensure_schema
from mysql_connection import db
from kafka_consumer import consumer
from model import CustomerModel,OrderModel

# ----------------------------------
# 1 - ensures the tables exist it not it creates them
# ----------------------------------

ensure_schema()

# ----------------------------------
# 2 - insert to db
# ----------------------------------

kafka_topic = os.getenv("KAFKA_TOPIC", "raw-records")


def upsert_customer(record):
    # insert a customer row 
    sql = """
INSERT INTO customers (
  customerNumber,
  customerName,
  contactLastName,
  contactFirstName,
  phone,
  addressLine1,
  addressLine2,
  city,
  state,
  postalCode,
  country,
  salesRepEmployeeNumber,
  creditLimit
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""".strip()

    params = (
        record.get("customerNumber"),
        record.get("customerName"),
        record.get("contactLastName"),
        record.get("contactFirstName"),
        record.get("phone"),
        record.get("addressLine1"),
        record.get("addressLine2"),
        record.get("city"),
        record.get("state"),
        record.get("postalCode"),
        record.get("country"),
        record.get("salesRepEmployeeNumber"),
        record.get("creditLimit")
    )
    db.execute(sql, params)


def upsert_order(record):
    # insert order row
    sql = """
INSERT INTO orders (
  orderNumber,
  orderDate,
  requiredDate,
  shippedDate,
  status,
  comments,
  customerNumber
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  orderDate = VALUES(orderDate),
  requiredDate = VALUES(requiredDate),
  shippedDate = VALUES(shippedDate),
  status = VALUES(status),
  comments = VALUES(comments),
  customerNumber = VALUES(customerNumber);
""".strip()

    params = (
        record.get("orderNumber"),
        record.get("orderDate"),
        record.get("requiredDate"),
        record.get("shippedDate"),
        record.get("status"),
        record.get("comments"),
        record.get("customerNumber"),
    )
    db.execute(sql, params)


# TODO: IMPLEMENT -> CustomerModel,OrderModel
def consume(limit=50, poll_seconds=5):
    # this pulls kafka messages and loads them into mysql
    # it stops when it reaches the limit or when there are no messages for a short time
    consumer.subscribe([kafka_topic])

    loaded_customers = 0
    loaded_orders = 0
    read_messages = 0

    while read_messages < int(limit):
        # for exiting the loop if no message in 5 seconds then break
        msg = consumer.poll(poll_seconds)
        # end when no message received
        if msg is None:
            break

        payload = msg.value().decode("utf-8")
        record = json.loads(payload)

        record_type = record.get("type")
        if record_type == "customer":
            upsert_customer(record)
            loaded_customers = loaded_customers + 1

        if record_type == "order":
            upsert_order(record)
            loaded_orders = loaded_orders + 1

        read_messages = read_messages + 1
        time.sleep(0.01)

    return {
        "read_messages": read_messages,
        "loaded_customers": loaded_customers,
        "loaded_orders": loaded_orders,
    }

consume()

