```markdown
README.md
--------------------------------------------------------------------------------
```markdown
## local run

### 1) start mysql with docker run
```bash
docker run --name education-mysql -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=classicmodels -p 3306:3306 -d mysql:8.0
```

### 2) start mongodb with docker run
```bash
docker run --name education-mongo -p 27017:27017 -d mongo:7
```

### 3) start kafka with docker run (simple local option)
```bash
docker run --name education-kafka -p 9092:9092 -p 29092:29092 -d \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,PLAINTEXT_INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://localhost:29092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT_INTERNAL \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  confluentinc/cp-kafka:7.6.1
```

### 4) create and activate a virtual environment
repeat this inside each service folder

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 5) run each service with uvicorn
open 3 terminals

terminal 1
```bash
cd service-a
source .venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8000
```

terminal 2
```bash
cd service-b
source .venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8001
```

terminal 3
```bash
cd service-c
source .venv/bin/activate
uvicorn main:app --host 0.0.0.0 --port 8002
```

### 6) quick test flow
seed sample records into mongodb via service a
```bash
curl -s http://localhost:8000/seed | python -m json.tool
```

publish mongodb records to kafka via service b
```bash
curl -s -X POST "http://localhost:8001/publish?batch_size=30&sleep_seconds=0.5" | python -m json.tool
```

consume kafka records into mysql via service c
```bash
curl -s -X POST "http://localhost:8002/consume?limit=50" | python -m json.tool
```

run analytics
```bash
curl -s http://localhost:8002/analytics/top-customers | python -m json.tool
curl -s http://localhost:8002/analytics/customers-without-orders | python -m json.tool
curl -s http://localhost:8002/analytics/zero-credit-active-customers | python -m json.tool
```

## docker compose

### build
```bash
docker compose build
```

### run
```bash
docker compose up
```

### quick test flow
```bash
curl -s http://localhost:8000/seed | python -m json.tool
curl -s -X POST "http://localhost:8001/publish?batch_size=30&sleep_seconds=0.5" | python -m json.tool
curl -s -X POST "http://localhost:8002/consume?limit=50" | python -m json.tool
curl -s http://localhost:8002/analytics/top-customers | python -m json.tool
```

## minikube
```bash
# clean
kubectl delete all --all
kubectl delete configmap --all
kubectl delete secret --all
kubectl delete pvc --all
# verify
kubectl get deploy
kubectl get pods
kubectl get svc
kubectl get statefulset
# create
kubectl apply -f k8s/
# delete
kubectl delete -f k8s/
# check
kubectl get pods
kubectl get svc
# describe
kubectl describe deploy <deployment-name>
kubectl describe pod <pod-name>
kubectl describe svc <service-name>
# access
kubectl port-forward svc/service-a 8000:8000
minikube service service-a
```

## openshift sandbox
```bash
# clean
oc delete all --all
oc delete configmap --all
oc delete secret --all
oc delete pvc --all
# verify
oc get deploy
oc get pods
oc get svc
oc get statefulset
# create
oc apply -f k8s/
# check
oc get pods
oc get svc
# describe
oc describe svc <service-name>
oc describe deploy service-a
oc describe deploy <deployment-name>
oc describe pod <pod-name>
# expose
oc expose svc/<service-name>
# get route
oc get route
```
```

================================================================================

docker-compose.yaml
--------------------------------------------------------------------------------
```yaml
services:
  mongo:
    image: mongo:7
    ports:
      - "27017:27017"

  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: classicmodels
    ports:
      - "3306:3306"

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_NODE_ID: "1"
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,PLAINTEXT_INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT_INTERNAL"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: "0"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  service-a:
    build:
      context: ./service-a
    environment:
      SERVICE_B_RECORDS_URL: "http://service-b:8001/records"
      DATA_FILE_PATH: "data/sample.json"
    ports:
      - "8000:8000"
    depends_on:
      - service-b

  service-b:
    build:
      context: ./service-b
    environment:
      MONGO_URI: "mongodb://mongo:27017"
      MONGO_DB: "raw"
      MONGO_COLLECTION: "records"
      KAFKA_BOOTSTRAP_SERVERS: "kafka:29092"
      KAFKA_TOPIC: "raw-records"
    ports:
      - "8001:8001"
    depends_on:
      - mongo
      - kafka

  service-c:
    build:
      context: ./service-c
    environment:
      KAFKA_BOOTSTRAP_SERVERS: "kafka:29092"
      KAFKA_TOPIC: "raw-records"
      KAFKA_GROUP_ID: "mysql-loader"
      DB_HOST: "mysql"
      DB_PORT: "3306"
      DB_USER: "root"
      DB_PASSWORD: "root"
      DB_NAME: "classicmodels"
    ports:
      - "8002:8002"
    depends_on:
      - mysql
      - kafka
```

================================================================================

SQL.md
--------------------------------------------------------------------------------
```markdown
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
```
```

================================================================================

k8s/00-mongo.yaml
--------------------------------------------------------------------------------
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mongo
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mongo
  template:
    metadata:
      labels:
        app: mongo
    spec:
      containers:
        - name: mongo
          image: mongo:7
          ports:
            - containerPort: 27017
---
apiVersion: v1
kind: Service
metadata:
  name: mongo
spec:
  selector:
    app: mongo
  ports:
    - port: 27017
      targetPort: 27017
```

--------------------------------------------------------------------------------

k8s/01-mysql.yaml
--------------------------------------------------------------------------------
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mysql
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mysql
  template:
    metadata:
      labels:
        app: mysql
    spec:
      containers:
        - name: mysql
          image: mysql:8.0
          ports:
            - containerPort: 3306
          env:
            - name: MYSQL_ROOT_PASSWORD
              value: root
            - name: MYSQL_DATABASE
              value: classicmodels
---
apiVersion: v1
kind: Service
metadata:
  name: mysql
spec:
  selector:
    app: mysql
  ports:
    - port: 3306
      targetPort: 3306
```

--------------------------------------------------------------------------------

k8s/02-kafka.yaml
--------------------------------------------------------------------------------
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
        - name: kafka
          image: confluentinc/cp-kafka:7.6.1
          ports:
            - containerPort: 9092
            - containerPort: 29092
          env:
            - name: KAFKA_NODE_ID
              value: "1"
            - name: KAFKA_PROCESS_ROLES
              value: "broker,controller"
            - name: KAFKA_CONTROLLER_QUORUM_VOTERS
              value: "1@kafka:9093"
            - name: KAFKA_LISTENERS
              value: "PLAINTEXT://0.0.0.0:9092,PLAINTEXT_INTERNAL://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093"
            - name: KAFKA_ADVERTISED_LISTENERS
              value: "PLAINTEXT://kafka:9092,PLAINTEXT_INTERNAL://kafka:29092"
            - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
              value: "PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT"
            - name: KAFKA_CONTROLLER_LISTENER_NAMES
              value: "CONTROLLER"
            - name: KAFKA_INTER_BROKER_LISTENER_NAME
              value: "PLAINTEXT_INTERNAL"
            - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
              value: "1"
            - name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
              value: "1"
            - name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS
              value: "0"
            - name: KAFKA_AUTO_CREATE_TOPICS_ENABLE
              value: "true"
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
spec:
  selector:
    app: kafka
  ports:
    - name: plaintext
      port: 9092
      targetPort: 9092
    - name: internal
      port: 29092
      targetPort: 29092
```

--------------------------------------------------------------------------------

k8s/10-service-a.yaml
--------------------------------------------------------------------------------
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: service-a
spec:
  replicas: 1
  selector:
    matchLabels:
      app: service-a
  template:
    metadata:
      labels:
        app: service-a
    spec:
      containers:
        - name: service-a
          image: service-a:latest
          ports:
            - containerPort: 8000
          env:
            - name: SERVICE_B_RECORDS_URL
              value: "http://service-b:8001/records"
            - name: DATA_FILE_PATH
              value: "data/sample.json"
---
apiVersion: v1
kind: Service
metadata:
  name: service-a
spec:
  selector:
    app: service-a
  ports:
    - port: 8000
      targetPort: 8000
```

--------------------------------------------------------------------------------

k8s/11-service-b.yaml
--------------------------------------------------------------------------------
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: service-b
spec:
  replicas: 1
  selector:
    matchLabels:
      app: service-b
  template:
    metadata:
      labels:
        app: service-b
    spec:
      containers:
        - name: service-b
          image: service-b:latest
          ports:
            - containerPort: 8001
          env:
            - name: MONGO_URI
              value: "mongodb://mongo:27017"
            - name: MONGO_DB
              value: "raw"
            - name: MONGO_COLLECTION
              value: "records"
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "kafka:9092"
            - name: KAFKA_TOPIC
              value: "raw-records"
---
apiVersion: v1
kind: Service
metadata:
  name: service-b
spec:
  selector:
    app: service-b
  ports:
    - port: 8001
      targetPort: 8001
```

--------------------------------------------------------------------------------

k8s/12-service-c.yaml
--------------------------------------------------------------------------------
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: service-c
spec:
  replicas: 1
  selector:
    matchLabels:
      app: service-c
  template:
    metadata:
      labels:
        app: service-c
    spec:
      containers:
        - name: service-c
          image: service-c:latest
          ports:
            - containerPort: 8002
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "kafka:9092"
            - name: KAFKA_TOPIC
              value: "raw-records"
            - name: KAFKA_GROUP_ID
              value: "mysql-loader"
            - name: DB_HOST
              value: "mysql"
            - name: DB_PORT
              value: "3306"
            - name: DB_USER
              value: "root"
            - name: DB_PASSWORD
              value: "root"
            - name: DB_NAME
              value: "classicmodels"
---
apiVersion: v1
kind: Service
metadata:
  name: service-c
spec:
  selector:
    app: service-c
  ports:
    - port: 8002
      targetPort: 8002
```

================================================================================

service-a/README.md
--------------------------------------------------------------------------------
```markdown
this service is a very small http entry point that forwards records to service b

endpoints
- GET /health
- POST /submit
- GET /seed

curl examples

GET /health
```bash
curl -s http://localhost:8000/health | python -m json.tool
```

POST /submit
```bash
curl -s -X POST http://localhost:8000/submit \
  -H "content-type: application/json" \
  -d '[{"type":"customer","customerNumber":1,"customerName":"example"}]' | python -m json.tool
```

GET /seed
```bash
curl -s http://localhost:8000/seed | python -m json.tool
```
```

--------------------------------------------------------------------------------

service-a/requirements.txt
--------------------------------------------------------------------------------
```text
fastapi==0.115.6
uvicorn==0.30.6
requests==2.32.3
python-dotenv==1.0.1
```

--------------------------------------------------------------------------------

service-a/Dockerfile
--------------------------------------------------------------------------------
```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

--------------------------------------------------------------------------------

service-a/main.py
--------------------------------------------------------------------------------
```python
from fastapi import FastAPI
from router import router

app = FastAPI()
app.include_router(router)
```

--------------------------------------------------------------------------------

service-a/router.py
--------------------------------------------------------------------------------
```python
import json
import os

import requests
from fastapi import APIRouter, Body

router = APIRouter()

service_b_records_url = os.getenv("SERVICE_B_RECORDS_URL", "http://localhost:8001/records")
data_file_path = os.getenv("DATA_FILE_PATH", "data/sample.json")


@router.get("/health")
def health():
    return {"status": "ok"}


@router.post("/submit")
def submit(payload=Body(...)):
    # payload is expected to be a json list of records
    resp = requests.post(service_b_records_url, json=payload, timeout=10)
    return resp.json()


@router.get("/seed")
def seed():
    # this reads a local json file and forwards the list to service b
    with open(data_file_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    resp = requests.post(service_b_records_url, json=payload, timeout=10)
    return resp.json()
```

--------------------------------------------------------------------------------

service-a/data/sample.json
--------------------------------------------------------------------------------
```json
[
  {
    "type": "customer",
    "customerNumber": 151,
    "customerName": "Muscle Machine Inc",
    "contactLastName": "Young",
    "contactFirstName": "Jeff",
    "phone": "2125557413",
    "addressLine1": "4092 Furth Circle",
    "addressLine2": "Suite 400",
    "city": "NYC",
    "state": "NY",
    "postalCode": "10022",
    "country": "USA",
    "salesRepEmployeeNumber": 1286,
    "creditLimit": "138500.00"
  },
  {
    "type": "order",
    "orderNumber": 10122,
    "orderDate": "2003-05-08",
    "requiredDate": "2003-05-16",
    "shippedDate": "2003-05-13",
    "status": "Shipped",
    "comments": null,
    "customerNumber": 350
  },
  {
    "type": "order",
    "orderNumber": 10129,
    "orderDate": "2003-06-12",
    "requiredDate": "2003-06-18",
    "shippedDate": "2003-06-14",
    "status": "Shipped",
    "comments": null,
    "customerNumber": 324
  },
  {
    "type": "order",
    "orderNumber": 10105,
    "orderDate": "2003-02-11",
    "requiredDate": "2003-02-21",
    "shippedDate": "2003-02-12",
    "status": "Shipped",
    "comments": null,
    "customerNumber": 145
  },
  {
    "type": "customer",
    "customerNumber": 299,
    "customerName": "Norway Gifts By Mail, Co.",
    "contactLastName": "Klaeboe",
    "contactFirstName": "Jan",
    "phone": "+47 2212 1555",
    "addressLine1": "Drammensveien 126A",
    "addressLine2": "PB 211 Sentrum",
    "city": "Oslo",
    "state": null,
    "postalCode": "N 0106",
    "country": "Norway  ",
    "salesRepEmployeeNumber": 1504,
    "creditLimit": "95100.00"
  },
  {
    "type": "customer",
    "customerNumber": 209,
    "customerName": "Mini Caravy",
    "contactLastName": "Citeaux",
    "contactFirstName": "FrÃ©dÃ©rique ",
    "phone": "88.60.1555",
    "addressLine1": "24, place KlÃ©ber",
    "addressLine2": null,
    "city": "Strasbourg",
    "state": null,
    "postalCode": "67000",
    "country": "France",
    "salesRepEmployeeNumber": 1370,
    "creditLimit": "53800.00"
  }
]
```

================================================================================

service-b/README.md
--------------------------------------------------------------------------------
```markdown
this service stores raw records in mongodb and can publish them to kafka

endpoints
- GET /health
- POST /records
- POST /publish

curl examples

GET /health
```bash
curl -s http://localhost:8001/health | python -m json.tool
```

POST /records
```bash
curl -s -X POST http://localhost:8001/records \
  -H "content-type: application/json" \
  -d '[{"type":"customer","customerNumber":1,"customerName":"example"}]' | python -m json.tool
```

POST /publish
```bash
curl -s -X POST "http://localhost:8001/publish?batch_size=30&sleep_seconds=0.5" | python -m json.tool
```
```

--------------------------------------------------------------------------------

service-b/requirements.txt
--------------------------------------------------------------------------------
```text
fastapi==0.115.6
uvicorn==0.30.6
pymongo==4.8.0
confluent-kafka==2.5.3
python-dotenv==1.0.1
```

--------------------------------------------------------------------------------

service-b/Dockerfile
--------------------------------------------------------------------------------
```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8001

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
```

--------------------------------------------------------------------------------

service-b/main.py
--------------------------------------------------------------------------------
```python
from fastapi import FastAPI
from router import router

app = FastAPI()
app.include_router(router)
```

--------------------------------------------------------------------------------

service-b/mongo.py
--------------------------------------------------------------------------------
```python
import os

from pymongo import MongoClient

mongo_uri = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
mongo_db = os.getenv("MONGO_DB", "raw")
mongo_collection = os.getenv("MONGO_COLLECTION", "records")

client = MongoClient(mongo_uri)
db = client[mongo_db]
collection = db[mongo_collection]
```

--------------------------------------------------------------------------------

service-b/kafka_producer.py
--------------------------------------------------------------------------------
```python
import os

from confluent_kafka import Producer

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
```

--------------------------------------------------------------------------------

service-b/router.py
--------------------------------------------------------------------------------
```python
import json
import os
import time

from fastapi import APIRouter, Body

from kafka_producer import producer
from mongo import collection

router = APIRouter()

kafka_topic = os.getenv("KAFKA_TOPIC", "raw-records")


@router.get("/health")
def health():
    return {"status": "ok"}


@router.post("/records")
def records(payload=Body(...)):
    # payload is expected to be a json list of records
    result = collection.insert_many(payload)
    return {"inserted_count": len(result.inserted_ids)}


@router.post("/publish")
def publish(batch_size=30, sleep_seconds=0.5, max_docs=0):
    # this publishes mongo documents to kafka one by one with a small delay
    # batch_size is how many docs we read per mongo query
    # max_docs lets you stop early for testing if you set it to a number above zero
    published = 0
    last_id = None

    while True:
        query = {}
        if last_id is not None:
            query = {"_id": {"$gt": last_id}}

        docs = list(collection.find(query).sort("_id", 1).limit(int(batch_size)))
        if len(docs) == 0:
            break

        for doc in docs:
            last_id = doc.get("_id")
            if "_id" in doc:
                doc.pop("_id")

            payload_bytes = json.dumps(doc).encode("utf-8")
            producer.produce(kafka_topic, value=payload_bytes)
            producer.poll(0)

            published = published + 1
            time.sleep(float(sleep_seconds))

            if int(max_docs) > 0 and published >= int(max_docs):
                producer.flush(10)
                return {"published": published, "stopped_early": True}

    producer.flush(10)
    return {"published": published, "stopped_early": False}
```

================================================================================

service-c/README.md
--------------------------------------------------------------------------------
```markdown
this service consumes kafka events into mysql and exposes analytics endpoints

endpoints
- GET /health
- POST /consume
- GET /analytics/top-customers
- GET /analytics/customers-without-orders
- GET /analytics/zero-credit-active-customers

curl examples

GET /health
```bash
curl -s http://localhost:8002/health | python -m json.tool
```

POST /consume
```bash
curl -s -X POST "http://localhost:8002/consume?limit=50" | python -m json.tool
```

GET /analytics/top-customers
```bash
curl -s http://localhost:8002/analytics/top-customers | python -m json.tool
```

GET /analytics/customers-without-orders
```bash
curl -s http://localhost:8002/analytics/customers-without-orders | python -m json.tool
```

GET /analytics/zero-credit-active-customers
```bash
curl -s http://localhost:8002/analytics/zero-credit-active-customers | python -m json.tool
```
```

--------------------------------------------------------------------------------

service-c/requirements.txt
--------------------------------------------------------------------------------
```text
fastapi==0.115.6
uvicorn==0.30.6
confluent-kafka==2.5.3
mysql-connector-python==9.0.0
python-dotenv==1.0.1
```

--------------------------------------------------------------------------------

service-c/Dockerfile
--------------------------------------------------------------------------------
```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8002

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8002"]
```

--------------------------------------------------------------------------------

service-c/db.py
--------------------------------------------------------------------------------
```python
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
load_dotenv()
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "world")
class Database:
    def __init__(self):
        self.host = DB_HOST
        self.port = DB_PORT
        self.user = DB_USER
        self.password = DB_PASSWORD
        self.database = DB_NAME
    def _get_connection(self):
        return mysql.connector.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )
    def query(self, sql, params=None):
        if params is None:
            params = ()
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            return rows
        except Error as e:
            print("Database query error:", e)
            raise
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    def execute(self, sql, params=None):
        if params is None:
            params = ()
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
        except Error as e:
            print("Database execute error:", e)
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
db = Database()
```

--------------------------------------------------------------------------------

service-c/schema.py
--------------------------------------------------------------------------------
```python
from db import db

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
  CONSTRAINT orders_customerNumber_fk FOREIGN KEY (customerNumber) REFERENCES customers(customerNumber)
);
"""


def ensure_schema():
    # create tables in mysql if they do not exist
    db.execute(create_customers_table_sql)
    db.execute(create_orders_table_sql)
```

--------------------------------------------------------------------------------

service-c/kafka_consumer.py
--------------------------------------------------------------------------------
```python
import os

from confluent_kafka import Consumer

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
kafka_group_id = os.getenv("KAFKA_GROUP_ID", "mysql-loader")

consumer = Consumer(
    {
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": kafka_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
)
```

--------------------------------------------------------------------------------

service-c/main.py
--------------------------------------------------------------------------------
```python
from fastapi import FastAPI
from router import router
from schema import ensure_schema

app = FastAPI()
app.include_router(router)


@app.on_event("startup")
def startup():
    # ensure mysql tables exist before we consume or query
    ensure_schema()
```

--------------------------------------------------------------------------------

service-c/router.py
--------------------------------------------------------------------------------
```python
import json
import os
import time

from fastapi import APIRouter

from db import db
from kafka_consumer import consumer

router = APIRouter()

kafka_topic = os.getenv("KAFKA_TOPIC", "raw-records")


@router.get("/health")
def health():
    return {"status": "ok"}


def ensure_customer_exists(customer_number):
    # create a placeholder customer row to satisfy the foreign key
    sql = """
INSERT INTO customers (customerNumber)
VALUES (%s)
ON DUPLICATE KEY UPDATE customerNumber = customerNumber;
""".strip()
    db.execute(sql, (customer_number,))


def upsert_customer(record):
    # insert or update a customer row using the raw record fields
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
""".strip()

    customer_number = record.get("customerNumber")
    credit_limit_raw = record.get("creditLimit")
    credit_limit = None
    if credit_limit_raw is not None:
        credit_limit = float(str(credit_limit_raw).strip())

    params = (
        customer_number,
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
        credit_limit,
    )
    db.execute(sql, params)


def upsert_order(record):
    # insert or update an order row using the raw record fields
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


@router.post("/consume")
def consume(limit=50, poll_seconds=0.5):
    # this pulls kafka messages and loads them into mysql
    # it stops when it reaches the limit or when there are no messages for a short time
    consumer.subscribe([kafka_topic])

    loaded_customers = 0
    loaded_orders = 0
    read_messages = 0

    while read_messages < int(limit):
        msg = consumer.poll(float(poll_seconds))
        if msg is None:
            break
        if msg.error():
            # we do not add error handling in this project
            break

        payload = msg.value().decode("utf-8")
        record = json.loads(payload)

        record_type = record.get("type")
        if record_type == "customer":
            upsert_customer(record)
            loaded_customers = loaded_customers + 1

        if record_type == "order":
            customer_number = record.get("customerNumber")
            ensure_customer_exists(customer_number)
            upsert_order(record)
            loaded_orders = loaded_orders + 1

        read_messages = read_messages + 1
        time.sleep(0.01)

    return {
        "read_messages": read_messages,
        "loaded_customers": loaded_customers,
        "loaded_orders": loaded_orders,
    }


@router.get("/analytics/top-customers")
def top_customers():
    # join customers to orders, count orders, order by count desc, return top 10
    sql = """
SELECT customers.customerNumber, customers.customerName, COUNT(orders.orderNumber)
FROM customers
LEFT JOIN orders ON orders.customerNumber = customers.customerNumber
GROUP BY customers.customerNumber, customers.customerName
ORDER BY COUNT(orders.orderNumber) DESC
LIMIT 10;
""".strip()

    rows = db.query(sql)
    result = []
    for row in rows:
        result.append(
            {
                "customerNumber": row[0],
                "customerName": row[1],
                "ordersCount": int(row[2]),
            }
        )
    return result


@router.get("/analytics/customers-without-orders")
def customers_without_orders():
    # left join orders and filter where order is null
    sql = """
SELECT customers.customerNumber, customers.customerName
FROM customers
LEFT JOIN orders ON orders.customerNumber = customers.customerNumber
WHERE orders.orderNumber IS NULL
ORDER BY customers.customerNumber;
""".strip()

    rows = db.query(sql)
    result = []
    for row in rows:
        result.append({"customerNumber": row[0], "customerName": row[1]})
    return result


@router.get("/analytics/zero-credit-active-customers")
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
    result = []
    for row in rows:
        result.append(
            {
                "customerNumber": row[0],
                "customerName": row[1],
                "creditLimit": float(row[2]),
                "ordersCount": int(row[3]),
            }
        )
    return result
```

================================================================================

```bash
bash -c 'mkdir -p k8s service-a/data service-b service-c && touch README.md docker-compose.yaml SQL.md k8s/00-mongo.yaml k8s/01-mysql.yaml k8s/02-kafka.yaml k8s/10-service-a.yaml k8s/11-service-b.yaml k8s/12-service-c.yaml service-a/README.md service-a/main.py service-a/router.py service-a/requirements.txt service-a/Dockerfile service-a/data/sample.json service-b/README.md service-b/main.py service-b/router.py service-b/mongo.py service-b/kafka_producer.py service-b/requirements.txt service-b/Dockerfile service-c/README.md service-c/main.py service-c/router.py service-c/db.py service-c/schema.py service-c/kafka_consumer.py service-c/requirements.txt service-c/Dockerfile'
```