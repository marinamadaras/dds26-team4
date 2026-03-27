import logging
import os
import atexit
import threading
import re
import uuid
import urllib.error
import urllib.request

import msgspec
import redis
import time

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from kafka_client import publish, publish_raw, create_consumer, decode_message
from messages import (
    BaseMessage,
    PaymentRequest,
    PaymentReply,
    RollbackPaymentRequest,
    RollbackPaymentReply,
)

DB_ERROR_STR = "DB error"
KAFKA_CONSUMER_PARTITION = int(os.getenv("KAFKA_CONSUMER_PARTITION", "0"))
KAFKA_CONSUMER_INSTANCE_ID = os.getenv("KAFKA_CONSUMER_INSTANCE_ID", "payment-service-0")


app = Flask("payment-service")
_consumer_thread: threading.Thread | None = None

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

MAX_RETRIES    = 5

_consumer_retry_counts: dict[str, int] = {}


def _partition_from_order_id(order_id: str) -> int:
    match = re.match(r"^s(\d+)_", order_id)
    if match:
        return int(match.group(1))
    return KAFKA_CONSUMER_PARTITION


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


class IdempotentReplyRecord(Struct):
    success: bool
    error: str | None = None


def operation_record_key(idempotency_key: str) -> str:
    return f"_idem:{idempotency_key}"


def get_operation_record(idempotency_key: str) -> IdempotentReplyRecord | None:
    if not idempotency_key:
        return None
    try:
        raw = db.get(operation_record_key(idempotency_key))
    except redis.exceptions.RedisError as e:
        raise RuntimeError(DB_ERROR_STR) from e
    return msgpack.decode(raw, type=IdempotentReplyRecord) if raw else None


def store_operation_record(idempotency_key: str, success: bool, error: str | None):
    if not idempotency_key:
        return
    record = IdempotentReplyRecord(success=success, error=error)
    try:
        db.set(operation_record_key(idempotency_key), msgpack.encode(record))
    except redis.exceptions.RedisError as e:
        raise RuntimeError(DB_ERROR_STR) from e


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


def remove_credit_core(user_id: str, amount: int) -> UserValue:
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        raise ValueError(f"User: {user_id} credit cannot get reduced below zero!")
    return user_entry


def _submit_kafka_task(topic: str, idempotency_key: str, key: str, message: BaseMessage, partition: int) -> None:
    app.logger.info(" Send message | key=%s", "idempotency_key")
    task = {
        "idempotency_key": idempotency_key, # used as id for the task

        # actual request
        "request": {
            "topic": topic, # to which service we route the request
            "key": key, # the key for that service
            "message": message, # the actual message we send
            "partition" : partition
        }
    }

    # TODO: partitions for orchestrator rout to partition
    publish("orchestrator.replies", idempotency_key, task)



def handle_payment_request(message: PaymentRequest):
    app.logger.info(" Got payment request | key=%s", message.idempotency_key)
    cached = get_operation_record(message.idempotency_key)
    if cached is not None and cached.success: ## do not retry an operation if it was succesfull the first time
        reply = PaymentReply(
            type = "PaymentReply",
            idempotency_key=message.idempotency_key,
            order_id=message.order_id,
            user_id=message.user_id,
            amount=message.amount,
            success=cached.success,
            error=cached.error,
        )

        _submit_kafka_task(
            topic="payment.replies",
            idempotency_key=message.idempotency_key,
            key=message.order_id,
            message=reply,
            partition=_partition_from_order_id(message.order_id),
        )

        return

    try:
        # subtract money and log that you have subtracted money
        user_entry = remove_credit_core(message.user_id, int(message.amount))
        pipe = db.pipeline()
        pipe.set(message.user_id, msgpack.encode(user_entry))
        pipe.set(operation_record_key(message.idempotency_key),
                 msgpack.encode(IdempotentReplyRecord(success=True, error=None)))
        pipe.execute()

        reply = PaymentReply(
            type="PaymentReply",
            idempotency_key=message.idempotency_key,
            order_id=message.order_id,
            user_id=message.user_id,
            amount=message.amount,
            success=True,
        )
    except Exception as e:
        store_operation_record(message.idempotency_key, False, str(e))
        reply = PaymentReply(
            type="PaymentReply",
            idempotency_key=message.idempotency_key,
            order_id=message.order_id,
            user_id=message.user_id,
            amount=message.amount,
            success=False,
            error=str(e),
        )
    _submit_kafka_task(
        topic="payment.replies",
        idempotency_key=message.idempotency_key,
        key=message.order_id,
        message=reply,
        partition=_partition_from_order_id(message.order_id),
    )


def handle_message(message: BaseMessage):
    if isinstance(message, PaymentRequest):
        handle_payment_request(message)
        return
    if isinstance(message, RollbackPaymentRequest):
        handle_rollback_payment_request(message)
        return
    app.logger.warning("No handler for message type=%s", message.type)


def handle_http_command(command: dict):
    # Handle gateway Kafka commands by calling existing local HTTP routes.
    request_id = str(command.get("request_id", ""))
    method = str(command.get("method", "GET")).upper()
    action = str(command.get("action", "")).lstrip("/")
    if not request_id or not action:
        return

    url = f"http://127.0.0.1:5000/{action}"
    try:
        req = urllib.request.Request(url=url, method=method)
        with urllib.request.urlopen(req, timeout=10) as response:
            payload = response.read().decode()
            content_type = response.headers.get("Content-Type", "")
            if "application/json" in content_type:
                body: object = msgspec.json.decode(payload.encode(), type=dict)
            else:
                body = payload
            status_code = int(response.status)
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        status_code = int(e.code)
    except Exception as e:
        body = {"error": str(e)}
        status_code = 500

    publish_raw(
        topic="gateway.payment.replies",
        key=request_id,
        payload={
            "request_id": request_id,
            "status_code": status_code,
            "body": body,
        },
        partition=KAFKA_CONSUMER_PARTITION,
    )



def handle_rollback_payment_request(message: RollbackPaymentRequest):
    # Idempotency check — if we already successfully rolled this back, just reply
    cached = get_operation_record(message.idempotency_key)
    if cached is not None and cached.success:
        reply = RollbackPaymentReply(
            type="RollbackPaymentReply",
            idempotency_key=message.idempotency_key,
            order_id=message.order_id,
            user_id=message.user_id,
            amount=message.amount,
            success=True,
            error=None,
        )
        _submit_kafka_task(
            topic="rollback.payment.replies",
            idempotency_key=message.idempotency_key,
            key=message.order_id,
            message=reply,
            partition=_partition_from_order_id(message.order_id),
        )
        return

    # Check if the original payment actually succeeded
    # If it never succeeded, there is nothing to refund
    payment_idem_key = f"order:{message.order_id}:payment_request:{message.user_id}:{message.amount}"
    payment_record = get_operation_record(payment_idem_key)

    if payment_record is None or not payment_record.success:
        # Payment was never actually taken — nothing to refund
        app.logger.info(
            "Rollback skipped for user=%s order=%s — payment never succeeded",
            message.user_id, message.order_id
        )
        store_operation_record(message.idempotency_key, True, None)
        reply = RollbackPaymentReply(
            type="RollbackPaymentReply",
            idempotency_key=message.idempotency_key,
            order_id=message.order_id,
            user_id=message.user_id,
            amount=message.amount,
            success=True,  # nothing to roll back, not an error
        )
        _submit_kafka_task(
            topic="rollback.payment.replies",
            idempotency_key=message.idempotency_key,
            key=message.order_id,
            message=reply,
            partition=_partition_from_order_id(message.order_id),
        )
        return

    # Payment was successfully taken — refund it

    user_entry = get_user_from_db(message.user_id)
    user_entry.credit += int(message.amount)

    # so that if things fail in between there is idempotency
    pipe = db.pipeline()
    pipe.set(message.user_id, msgpack.encode(user_entry))
    pipe.set(operation_record_key(message.idempotency_key),
             msgpack.encode(IdempotentReplyRecord(success=True, error=None)))
    pipe.execute()

    reply = RollbackPaymentReply(
        type="RollbackPaymentReply",
        idempotency_key=message.idempotency_key,
        order_id=message.order_id,
        user_id=message.user_id,
        amount=message.amount,
        success=True,
    )
    _submit_kafka_task(
        topic="rollback.payment.replies",
        idempotency_key=message.idempotency_key,
        key=message.order_id,
        message=reply,
        partition=_partition_from_order_id(message.order_id),
    )


def consumer_loop():
    app.logger.info(
        "payment consumer loop starting partition=%s instance_id=%s",
        KAFKA_CONSUMER_PARTITION,
        KAFKA_CONSUMER_INSTANCE_ID,
    )
    consumer = create_consumer(
        group_id="payment-service",
        topics=["payment", "rollback.payment", "gateway.payment.commands"],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        partition=KAFKA_CONSUMER_PARTITION,
        group_instance_id=KAFKA_CONSUMER_INSTANCE_ID,
    )
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            error_str = str(msg.error())
            if "NOT_COORDINATOR" in error_str:
                app.logger.warning("Kafka coordinator not ready yet: %s", msg.error())
            else:
                app.logger.error("Kafka error: %s", msg.error())
            continue

        key = msg.key().decode() if msg.key() else str(msg.offset())
        try:
            if msg.topic() == "gateway.payment.commands":
                command = msgspec.json.decode(msg.value(), type=dict)
                handle_http_command(command)
                consumer.commit(message=msg)
                continue
            message = decode_message(msg.value())
            handle_message(message)
            consumer.commit(message=msg)
            _consumer_retry_counts.pop(key, None)
        except Exception as e:
            _consumer_retry_counts[key] = _consumer_retry_counts.get(key, 0) + 1
            attempt = _consumer_retry_counts[key]
            app.logger.exception(
                "Processing error (attempt %d/%d) key=%s: %s", attempt, MAX_RETRIES, key, e
            )
            if attempt >= MAX_RETRIES:
                app.logger.error("Max consumer retries reached for key=%s — skipping", key)
                consumer.commit(message=msg)
                _consumer_retry_counts.pop(key, None)
            else:
                time.sleep(min(2 ** attempt, 30))


def start_consumer():
    global _consumer_thread
    if _consumer_thread is not None and _consumer_thread.is_alive():
        return
    thread = threading.Thread(target=consumer_loop, daemon=True)
    thread.start()
    _consumer_thread = thread
    app.logger.info("payment consumer thread started")


@app.post('/create_user')
def create_user():
    key = f"p{KAFKA_CONSUMER_PARTITION}_{uuid.uuid4()}"
    value = msgpack.encode(UserValue(credit=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {}
    for i in range(n):
        kv_pairs[f"p{KAFKA_CONSUMER_PARTITION}_{i}"] = msgpack.encode(UserValue(credit=starting_money))
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    try:
        user_entry = remove_credit_core(user_id, int(amount))
        db.set(user_id, msgpack.encode(user_entry))
    except ValueError as e:
        abort(400, str(e))
    except RuntimeError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


# If the completion of the order fails we need to refund the customer (not sure if necessary)
@app.post('/refund/<user_id>/<amount>')
def refund(user_id: str, amount: int):
    app.logger.debug(f"Refunding {amount} credit to user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit += int(amount)
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    return Response(
        f"Refunded {amount} to user: {user_id}. New credit: {user_entry.credit}",
        status=200
    )


if __name__ == '__main__':
    if os.getenv("DISABLE_KAFKA_CONSUMER") != "1":
        start_consumer()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    if os.getenv("DISABLE_KAFKA_CONSUMER") != "1":
        start_consumer()
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
