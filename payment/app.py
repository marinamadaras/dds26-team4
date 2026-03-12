import logging
import os
import atexit
import threading
import uuid
import urllib.error
import urllib.request

import msgspec
import redis

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


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


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
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError as e:
        raise RuntimeError(DB_ERROR_STR) from e
    return user_entry


def handle_payment_request(message: PaymentRequest):
    try:
        remove_credit_core(message.user_id, int(message.amount))
        reply = PaymentReply(
            order_id=message.order_id,
            user_id=message.user_id,
            amount=message.amount,
            success=True,
        )
    except Exception as e:
        reply = PaymentReply(
            order_id=message.order_id,
            user_id=message.user_id,
            amount=message.amount,
            success=False,
            error=str(e),
        )
    publish(topic="payment.replies", key=message.order_id, value=reply)


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


# todo: implement payment rollback logic and adapt the reply down here
def handle_rollback_payment_request(message: RollbackPaymentRequest):
    reply = RollbackPaymentReply(
        order_id=message.order_id,
        user_id=message.user_id,
        amount=message.amount,
        success=False,
        error="rollback payment not implemented",
    )
    publish(topic="rollback.payment.replies", key=message.order_id, value=reply)


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
        try:
            if msg.topic() == "gateway.payment.commands":
                command = msgspec.json.decode(msg.value(), type=dict)
                handle_http_command(command)
                consumer.commit(message=msg)
                continue
            message = decode_message(msg.value())
            handle_message(message)
            consumer.commit(message=msg)
        except Exception as e:
            app.logger.exception("Processing error in payment consumer: %s", e)
            consumer.commit(message=msg)


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
    key = f"s{KAFKA_CONSUMER_PARTITION}_{uuid.uuid4()}"
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
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
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
    except ValueError as e:
        abort(400, str(e))
    except RuntimeError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


if __name__ == '__main__':
    start_consumer()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    start_consumer()
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
