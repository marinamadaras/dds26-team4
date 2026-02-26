import os
import json
from confluent_kafka import Producer, Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

producer = Producer({"bootstrap.servers": BOOTSTRAP})

def publish(topic: str, key: str, value: dict):
    producer.produce(
        topic,
        key=key.encode(),
        value=json.dumps(value).encode()
    )
    producer.flush(2)

def create_consumer(group_id: str, topics: list[str]):
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(topics)
    return consumer