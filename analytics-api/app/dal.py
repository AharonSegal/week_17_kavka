from connection import db

# TODO : TEST QUERIES

# analytics/top-customers
# the 10 customers with most orders 
def top_customers():
    # join customers to orders, count orders, order by count desc, return top 10
    # i want all orders and customers matching those orders so i will use LEFT JOIN
    sql = """
    SELECT customers.customerNumber, customers.customerName, COUNT(orders.orderNumber)
    FROM customers
    LEFT JOIN orders ON orders.customerNumber = customers.customerNumber
    GROUP BY customers.customerNumber, customers.customerName
    ORDER BY COUNT(orders.orderNumber) DESC
    LIMIT 10;
""".strip()

    rows = db.query(sql)
    return rows

# analytics/customers-without-orders
# customers were order is null
def customers_without_orders():
# TODO: JOIN ALL ORDERS TO THEIR CUSTOMERS 
# TODO: THEN ON THAT SUB COMPARE TO THE CONTACTS TABLE 
# TODO: RETURN THE ONES WHO ARE IN CONTACTS BUT NOT IN THE SUB
    sql = """
..... TODO CONTINUE .....

WITH customers-with-orders (
SELECT customers.customerNumber, customers.customerName
FROM customers
JOIN orders ON orders.customerNumber = customers.customerNumber
GROUP BY customers.customerNumber, customers.customerName
ORDER BY COUNT(orders.orderNumber) DESC;
) as sub




""".strip()

    rows = db.query(sql)
    return rows

# analytics/zero-credit-active-customers
# customers with 0 credit and have orders 
def zero_credit_active_customers():
    # join orders, filter credit limit 0, group by customer, count orders
    sql = """
SELECT customers.customerNumber, customers.customerName, customers.creditLimit, COUNT(orders.orderNumber)
FROM customers
JOIN orders ON orders.customerNumber = customers.customerNumber
WHERE customers.creditLimit = 0
GROUP BY customers.customerNumber, customers.customerName, customers.creditLimit
ORDER BY COUNT(orders.orderNumber) DESC;
""".strip()

    rows = db.query(sql)
    return rows
