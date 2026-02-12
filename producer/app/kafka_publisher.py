import os

from confluent_kafka import Producer

kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")

producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})