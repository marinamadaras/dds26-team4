import os

import msgspec
from confluent_kafka import Producer, Consumer, TopicPartition, OFFSET_END

from messages import BaseMessage, MESSAGE_TYPES

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

producer = Producer({"bootstrap.servers": BOOTSTRAP})


def publish(topic: str, key: str, value: BaseMessage):
    producer.produce(
        topic,
        key=key.encode(),
        value=msgspec.json.encode(value),
    )
    producer.flush(2)


def publish_raw(topic: str, key: str, payload: dict, partition: int | None = None):
    # Used for gateway command/reply envelopes that are plain JSON dicts.
    kwargs = {}
    if partition is not None:
        kwargs["partition"] = partition
    producer.produce(
        topic,
        key=key.encode(),
        value=msgspec.json.encode(payload),
        **kwargs,
    )
    producer.flush(2)


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
        topic_partitions = [TopicPartition(topic, partition) for topic in topics]
        committed = consumer.committed(topic_partitions, timeout=5.0)
        # If there is no committed offset yet, start from end so only new commands are consumed.
        assigned = [
            TopicPartition(tp.topic, tp.partition, tp.offset if tp.offset >= 0 else OFFSET_END)
            for tp in committed
        ]
        consumer.assign(assigned)
    return consumer
