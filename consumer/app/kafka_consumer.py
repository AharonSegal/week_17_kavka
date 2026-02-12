import os

from confluent_kafka import Consumer

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
kafka_group_id = os.getenv("KAFKA_GROUP_ID", "test-group")

consumer = Consumer(
    {
        "bootstrap.servers": kafka_bootstrap_servers,
        "group.id": kafka_group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
)