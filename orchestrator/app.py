import atexit
import json
import os
import threading
import time
import logging
from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING
from kafka_client import publish, create_consumer
import redis
import requests
from flask import Flask, Response, jsonify, request
import msgspec
from messages import (
    Ack,
    Failure
)


app = Flask("orchestrator-service")
TASK_KEY_PREFIX = "task:"
RETRY_INTERVAL = 5
RETRY_TIMEOUT = 10
MAX_RETRIES = 5

_consumer_thread: threading.Thread | None = None

_consumer_retry_counts: dict[str, int] = {}

db: redis.Redis = redis.Redis(
    host=os.getenv("REDIS_HOST", "orchestrator-db"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    password=os.getenv("REDIS_PASSWORD", "redis"),
    db=int(os.getenv("REDIS_DB", "0")),
)

logging.basicConfig(level=logging.INFO)

def close_db_connection():
    # Close the Redis client cleanly on shutdown.
    db.close()



atexit.register(close_db_connection)


def start_kafka_consumer():
    global _consumer_thread
    if _consumer_thread is not None and _consumer_thread.is_alive():
        return
    thread = threading.Thread(target=consumer_loop, daemon=True)
    thread.start()
    _consumer_thread = thread

atexit.register(close_db_connection)


def consumer_loop():
    app.logger.info("Start consumer loop")
    consumer = create_consumer(
        group_id="orchestrator-service",
        topics=["orchestrator.requests", "orchestrator.replies"],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # partition=KAFKA_CONSUMER_PARTITION,
        # group_instance_id=KAFKA_CONSUMER_INSTANCE_ID,
    )


    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            continue
        app.logger.info("Received Kafka message | topic=%s", msg.topic())
        key = msg.key().decode() if msg.key() else ""

        try:
            task = msgspec.json.decode(msg.value(), type=dict)
            threading.Thread(
                target= handle_task,
                args=(task,),
                daemon=True,
            ).start()
            consumer.commit(message=msg)
            _consumer_retry_counts.pop(key, None)

        except Exception as e:
            app.logger.exception("Processing error in order consumer: %s", e)

            _consumer_retry_counts[key] = _consumer_retry_counts.get(key, 0) + 1
            attempt = _consumer_retry_counts[key]
            if attempt >= MAX_RETRIES:
                consumer.commit(message=msg)
                _consumer_retry_counts.pop(key, None)
            else:
                time.sleep(min(2 ** attempt, 30))  # exponential back-off




def _persist_incoming_task(task: dict) -> dict:
    """
    Persist a task that was already constructed by another service.
    Only enrich it with orchestrator metadata.
    """
    idempotency_key = task["idempotency_key"]
    task["status"] = "waiting_reply"
    task["result"] = None
    task["error"] = None
    task["retry_count"] = 1
    task["last_attempt"] = time.time()

    db.set(f"{TASK_KEY_PREFIX}{idempotency_key}", json.dumps(task))
    return task



def _update_task(idempotency_key: str, **fields: object) -> dict[str, object] | None:
    # Update a stored task in Redis and return the latest snapshot.
    task = _get_task(idempotency_key)
    if task is None:
        return None
    task.update(fields)
    task["updated_at"] = time.time()
    db.set(f"{TASK_KEY_PREFIX}{idempotency_key}", json.dumps(task))
    return task


def _get_task(idempotency_key: str) -> dict[str, object] | None:
    # Load one task record from Redis by its id.
    raw = db.get(f"{TASK_KEY_PREFIX}{idempotency_key}")
    return json.loads(raw) if raw else None

def handle_task(task:dict):
    message = task["request"].get("message")
    message_type = message.get("type")

    if message_type in (
            "FindStockReply",
            "StockSubtractedReply",
            "PaymentReply",
            "RollbackStockReply",
            "RollbackPaymentReply",
    ):
        _handle_reply(task)
        return
    else:
        _handle_request(task)
        return


def _handle_reply(task:dict):
    app.logger.info(" Received reply message | topic=%s | key=%s", task["request"].get("message"), task["idempotency_key"])
    idempotency_key = task["idempotency_key"]
    task_in_db = _get_task(idempotency_key)
    if task_in_db is None:
        app.logger.exception("Reply for nonexistent task")
    else:
        _update_task(idempotency_key, status="reply_received")
    _send_task(task)

def _handle_request(task:dict):
    app.logger.info(" Received request message |  key=%s", task["idempotency_key"])

    idempotency_key = task["idempotency_key"]
    task_in_db = _get_task(idempotency_key)
    if task_in_db is None:
        task = _persist_incoming_task(task)
    _send_ack(task)
    _send_task(task)


def _send_ack(task: dict):
    request = task["request"]
    message = Ack(
        idempotency_key=task["idempotency_key"]
    )
    app.logger.info(" Sending ack | key=%s",  task["idempotency_key"])

    publish("orchestrator.feedback", request.get("message").get("order_id"), message)


def _send_task(task: dict):
    request = task["request"]
    app.logger.info(" Send message | key=%s", task["idempotency_key"])
    publish(request.get("topic"), request.get("key"), request.get("message"), request.get("partition"))


def start_retry_checker() -> None:
    """Periodic background thread that retries timed-out outgoing messages."""
    def retry_loop():
        while True:
            try:
                check_and_retry_pending_messages()
            except Exception as e:
                pass
            time.sleep(RETRY_INTERVAL)

    thread = threading.Thread(target=retry_loop, daemon=True)
    thread.start()

def check_and_retry_pending_messages() -> None:
    now = time.time()

    try:
        keys = db.keys(f"{TASK_KEY_PREFIX}*")
    except redis.exceptions.RedisError as e:
        return

    for key in keys:
        try:
            raw = db.get(key)
            if not raw:
                continue

            task = json.loads(raw)
            status = task.get("status")
            if status != "waiting_reply":
                continue

            retry_count = task.get("retry_count")
            last_attempt = task.get("last_attempt")

            if now - last_attempt < RETRY_TIMEOUT:
                continue

            if retry_count >= MAX_RETRIES:
                _handle_failure(task)
                continue

            retry_count += 1
            _send_task(task)
            _update_task(
                task["idempotency_key"],
                retry_count=retry_count,
                last_attempt=time.time()
            )

        except Exception as e:
            app.logger.exception("Retry checker failed: %s", e)

def _handle_failure(task: dict):
    order_id = task["request"].get("message").get("order_id")
    message = Failure(
        idempotency_key=task["idempotency_key"],
        order_id=order_id
    )
    publish("orchestrator.feedback", order_id, message)

def recover_on_startup() -> None:
    """
    Called once at startup. Scans all pending outgoing messages and retries
    anything that never got a reply. This handles the case where the service
    crashed mid-saga and needs to resume or compensate.
    """
    try:
        check_and_retry_pending_messages()
    except Exception as e:
        app.logger.exception("Recover on startup failed: %s", e)


if __name__ == '__main__':
    if os.getenv("DISABLE_KAFKA_CONSUMER") != "1":
        recover_on_startup()
        start_retry_checker()
        start_kafka_consumer()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    if os.getenv("DISABLE_KAFKA_CONSUMER") != "1":
        recover_on_startup()
        start_retry_checker()
        start_kafka_consumer()
        gunicorn_logger = logging.getLogger('gunicorn.error')
        app.logger.handlers = gunicorn_logger.handlers
        app.logger.setLevel(gunicorn_logger.level)