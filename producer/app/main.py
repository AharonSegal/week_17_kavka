# fast api read from mongo and pass to kafka 
# seed db done in the ui 
import os, json, asyncio
from fastapi import FastAPI, status
from confluent_kafka import Producer

from app.models import UserRegisterModel

app = FastAPI()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "users.registered")
SEED_FILE_PATH = os.getenv("SEED_FILE_PATH", "data/users_with_posts.json")


producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                     'client.id': "fastAPI producer"})


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/register", status_code=status.HTTP_201_CREATED)
async def register(user: UserRegisterModel):
    user_data = user.model_dump(mode="json")
 

    payload = json.dumps(user_data, ensure_ascii=False).encode("utf-8")
    producer.produce(KAFKA_TOPIC, payload)
    producer.flush(2)  # simplest: ensure it was sent before responding

    return {
        "status": "ok",
        "validated_user_id": user.user_id,
        "posts_count": len(user.posts),
        "topic": KAFKA_TOPIC,
    }

