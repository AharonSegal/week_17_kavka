# TODO: TEST THIS 
# go over the whole collection and send the whole collection do kafka 
# in batches of 30
# and during each batch send each document individually in 0.5 second intervals

import json
import os
import time

from fastapi import APIRouter, Body

from kafka_publisher import producer
from mongo_connection import collection

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

# TODO: IMPLEMENT -> CustomerModel,OrderModel
@router.post("/publish")
def publish(batch_size=30, sleep_seconds=0.5):
    # batch_size -> how many docs read per query
    # wait between each doc in a batch
    published = 0

    while True:
        docs = list(collection.find().limit(int(batch_size)))
        if len(docs) == 0:
            break

        for doc in docs:
            payload_bytes = json.dumps(doc).encode("utf-8")
            producer.produce(kafka_topic, value=payload_bytes)

            published = published + 1
            time.sleep(float(sleep_seconds))

    producer.flush(10)
    return {"published_count": published}

