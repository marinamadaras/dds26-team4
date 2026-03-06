import os
import logging

import msgspec
from confluent_kafka import Producer, Consumer, TopicPartition
from messages import BaseMessage, MESSAGE_TYPES

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

producer = Producer({"bootstrap.servers": BOOTSTRAP})
logger = logging.getLogger(__name__)


def publish(topic: str, key: str, value: BaseMessage):
    producer.produce(
        topic,
        key=key.encode(),
        value=msgspec.json.encode(value)
    )
    producer.flush(2)
    logger.info("published %s key=%s", topic, key)


def decode_message(raw_value: bytes) -> BaseMessage:
    payload = msgspec.json.decode(raw_value, type=dict)
    message_type = payload.get("type")
    if not message_type:
        raise ValueError("Message is missing required 'type' field")
    message_cls = MESSAGE_TYPES.get(message_type)
    if message_cls is None:
        raise ValueError(f"Unknown message type: {message_type}")
    return msgspec.json.decode(raw_value, type=message_cls)

def create_consumer(
    group_id: str,
    topics: list[str],
    *,
    auto_offset_reset: str = "latest",
    enable_auto_commit: bool = False,
    partition: int | None = None,
    group_instance_id: str | None = None,
):
    config = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": enable_auto_commit,
    }
    if group_instance_id:
        config["group.instance.id"] = group_instance_id

    consumer = Consumer(config)
    if partition is None:
        consumer.subscribe(topics)
    else:
        consumer.assign([TopicPartition(topic, partition) for topic in topics])
    return consumer
