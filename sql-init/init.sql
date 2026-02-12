note -- q1 -- create customers table
```sql
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
```

note -- q2 -- create orders table and connect orders to customers
```sql
CREATE TABLE IF NOT EXISTS orders (
  orderNumber INT NOT NULL,
  orderDate DATE NULL,
  requiredDate DATE NULL,
  shippedDate DATE NULL,
  status VARCHAR(50) NULL,
  comments TEXT NULL,
  customerNumber INT NOT NULL,
  PRIMARY KEY (orderNumber),
  CONSTRAINT orders_customerNumber_fk FOREIGN KEY (customerNumber) REFERENCES customers(customerNumber)
);
```

note -- q3 -- ensure a customer row exists before inserting an order
```sql
INSERT INTO customers (customerNumber)
VALUES (%s)
ON DUPLICATE KEY UPDATE customerNumber = customerNumber;
```

note -- q4 -- upsert a full customer record
```sql
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
ON DUPLICATE KEY UPDATE
  customerName = VALUES(customerName),
  contactLastName = VALUES(contactLastName),
  contactFirstName = VALUES(contactFirstName),
  phone = VALUES(phone),
  addressLine1 = VALUES(addressLine1),
  addressLine2 = VALUES(addressLine2),
  city = VALUES(city),
  state = VALUES(state),
  postalCode = VALUES(postalCode),
  country = VALUES(country),
  salesRepEmployeeNumber = VALUES(salesRepEmployeeNumber),
  creditLimit = VALUES(creditLimit);
```

note -- q5 -- upsert an order record
```sql
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
```

note -- q6 -- top 10 customers by number of orders
```sql
SELECT customers.customerNumber, customers.customerName, COUNT(orders.orderNumber)
FROM customers
LEFT JOIN orders ON orders.customerNumber = customers.customerNumber
GROUP BY customers.customerNumber, customers.customerName
ORDER BY COUNT(orders.orderNumber) DESC
LIMIT 10;
```

note -- q7 -- customers with no orders
```sql
SELECT customers.customerNumber, customers.customerName
FROM customers
LEFT JOIN orders ON orders.customerNumber = customers.customerNumber
WHERE orders.orderNumber IS NULL
ORDER BY customers.customerNumber;
```

note -- q8 -- customers with credit limit 0 who still made orders
```sql
SELECT customers.customerNumber, customers.customerName, customers.creditLimit, COUNT(orders.orderNumber)
FROM customers
JOIN orders ON orders.customerNumber = customers.customerNumber
WHERE customers.creditLimit = 0
GROUP BY customers.customerNumber, customers.customerName, customers.creditLimit
ORDER BY COUNT(orders.orderNumber) DESC;