import logging
import os
import atexit
import threading
import re
import uuid
import urllib.error
import urllib.request
import zlib

import msgspec
import redis
import time

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from kafka_client import publish, publish_raw, create_consumer, decode_message
from messages import (
    BaseMessage,
    FindStock,
    FindStockReply,
    SubtractStock,
    StockSubtractedReply,
    RollbackStockRequest,
    RollbackStockReply,
)


DB_ERROR_STR = "DB error"
KAFKA_CONSUMER_PARTITION = int(os.getenv("KAFKA_CONSUMER_PARTITION", "0"))
KAFKA_CONSUMER_INSTANCE_ID = os.getenv("KAFKA_CONSUMER_INSTANCE_ID", "stock-service-0")
ORCHESTRATOR_PARTITIONS = int(os.getenv("ORCHESTRATOR_PARTITIONS", "3"))

app = Flask("stock-service")
_consumer_thread: threading.Thread | None = None

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

MAX_RETRIES = 5
_consumer_retry_counts: dict[str, int] = {}


def _partition_from_order_id(order_id: str) -> int:
    match = re.match(r"^s(\d+)_", order_id)
    if match:
        return int(match.group(1))
    return KAFKA_CONSUMER_PARTITION

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


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


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry


def get_item_from_db_nullable(item_id: str) -> StockValue | None:
    entry: bytes = db.get(item_id)
    return msgpack.decode(entry, type=StockValue) if entry else None


def subtract_stock_core(item_id: str, amount: int) -> StockValue:
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        raise ValueError(f"Item: {item_id} stock cannot get reduced below zero!")
    return item_entry

def _submit_kafka_task(topic: str, idempotency_key: str, key: str, message: BaseMessage, partition: int) -> None:
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

    publish(
        "orchestrator.replies",
        idempotency_key,
        task,
        partition=_orchestrator_partition_for_idempotency_key(idempotency_key),
    )


def _orchestrator_partition_for_idempotency_key(idempotency_key: str) -> int:
    return zlib.crc32(idempotency_key.encode()) % ORCHESTRATOR_PARTITIONS



def handle_find_stock(message: FindStock, order_id: str):
    cached = get_operation_record(message.idempotency_key)
    if cached is not None and cached.success:
        # already processed — but we don't store the reply so we can't replay it
        # just skip, order service already has the item
        return

    app.logger.info("received find.stock request order_id=%s item_id=%s qty=%s", order_id, message.item_id, message.quantity)
    item_entry = get_item_from_db_nullable(message.item_id)
    if item_entry is None:
        reply = FindStockReply(
            type="FindStockReply",
            idempotency_key=message.idempotency_key,
            order_id=order_id,
            item_id=message.item_id,
            quantity=message.quantity,
            found=False,
        )
    else:
        reply = FindStockReply(
            type="FindStockReply",
            idempotency_key=message.idempotency_key,
            order_id=order_id,
            item_id=message.item_id,
            quantity=message.quantity,
            found=True,
            stock=item_entry.stock,
            price=item_entry.price,
        )

    store_operation_record(message.idempotency_key, True, None)
    _submit_kafka_task(
        topic="find.stock.replies",
        idempotency_key=reply.idempotency_key,
        key=order_id,
        message=reply,
        partition=_partition_from_order_id(order_id),
    )
    app.logger.info("published find.stock.replies order_id=%s item_id=%s found=%s", order_id, message.item_id, reply.found)


def handle_message(message: BaseMessage, key: str):
    if isinstance(message, FindStock):
        handle_find_stock(message, key)
        return
    if isinstance(message, SubtractStock):
        handle_subtract_stock(message)
        return
    if isinstance(message, RollbackStockRequest):
        handle_rollback_stock(message)
        return
    app.logger.warning(f"No handler registered for message type: {message.type}")


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
        topic="gateway.stock.replies",
        key=request_id,
        payload={
            "request_id": request_id,
            "status_code": status_code,
            "body": body,
        },
        partition=KAFKA_CONSUMER_PARTITION,
    )


def handle_subtract_stock(message: SubtractStock):
    cached = get_operation_record(message.idempotency_key)
    if cached is not None and cached.success:
        reply = StockSubtractedReply(
            type="StockSubtractedReply",
            idempotency_key=message.idempotency_key,
            order_id=message.order_id,
            item_id=message.item_id,
            quantity=message.quantity,
            success=cached.success,
            error=cached.error,
        )
        _submit_kafka_task(
            topic="subtract.stock.replies",
            idempotency_key=message.idempotency_key,
            key=message.order_id,
            message=reply,
            partition=_partition_from_order_id(message.order_id),
        )
        return

    try:
        item_entry = subtract_stock_core(message.item_id, int(message.quantity))
        pipe = db.pipeline()
        pipe.set(message.item_id, msgpack.encode(item_entry))
        pipe.set(operation_record_key(message.idempotency_key),
                 msgpack.encode(IdempotentReplyRecord(success=True, error=None)))
        pipe.execute()

        reply = StockSubtractedReply(
            type="StockSubtractedReply",
            idempotency_key=message.idempotency_key,
            order_id=message.order_id,
            item_id=message.item_id,
            quantity=message.quantity,
            success=True,
        )
    except Exception as e:
        store_operation_record(message.idempotency_key, False, str(e))
        reply = StockSubtractedReply(
            type="StockSubtractedReply",
            idempotency_key=message.idempotency_key,
            order_id=message.order_id,
            item_id=message.item_id,
            quantity=message.quantity,
            success=False,
            error=str(e),
        )

    _submit_kafka_task(
        topic="subtract.stock.replies",
        idempotency_key=message.idempotency_key,
        key=message.order_id,
        message=reply,
        partition=_partition_from_order_id(message.order_id),
    )

def handle_rollback_stock(message: RollbackStockRequest):
    # Idempotency check — if we already successfully rolled this back, just reply
    cached = get_operation_record(message.idempotency_key)
    if cached is not None and cached.success:
        reply = RollbackStockReply(
            type="RollbackStockReply",
            idempotency_key=message.idempotency_key,
            order_id=message.order_id,
            item_id=message.item_id,
            quantity=message.quantity,
            success=True,
            error=None,
        )
        _submit_kafka_task(
            topic="rollback.stock.replies",
            idempotency_key=message.idempotency_key,
            key=message.order_id,
            message=reply,
            partition=_partition_from_order_id(message.order_id),
        )
        return

    # Check if the original subtraction actually succeeded
    # If it never succeeded, there is nothing to roll back
    subtract_idem_key = f"order:{message.order_id}:subtract_stock:{message.item_id}:{message.quantity}"
    subtract_record = get_operation_record(subtract_idem_key)

    if subtract_record is None or not subtract_record.success:
        # Stock was never actually subtracted — nothing to restore
        app.logger.info(
            "Rollback skipped for item=%s order=%s — subtraction never succeeded",
            message.item_id, message.order_id
        )
        store_operation_record(message.idempotency_key, True, None)
        reply = RollbackStockReply(
            type="RollbackStockReply",
            idempotency_key=message.idempotency_key,
            order_id=message.order_id,
            item_id=message.item_id,
            quantity=message.quantity,
            success=True,
        )
        _submit_kafka_task(
            topic="rollback.stock.replies",
            idempotency_key=message.idempotency_key,
            key=message.order_id,
            message=reply,
            partition=_partition_from_order_id(message.order_id),
        )
        return

    # Stock was successfully subtracted — restore it

    item_entry = get_item_from_db(message.item_id)
    item_entry.stock += int(message.quantity)

    pipe = db.pipeline()
    pipe.set(message.item_id, msgpack.encode(item_entry))
    pipe.set(operation_record_key(message.idempotency_key),
             msgpack.encode(IdempotentReplyRecord(success=True, error=None)))
    pipe.execute()

    reply = RollbackStockReply(
        type="RollbackStockReply",
        idempotency_key=message.idempotency_key,
        order_id=message.order_id,
        item_id=message.item_id,
        quantity=message.quantity,
        success=True,
    )

    _submit_kafka_task(
        topic="rollback.stock.replies",
        idempotency_key=message.idempotency_key,
        key=message.order_id,
        message=reply,
        partition=_partition_from_order_id(message.order_id),
    )

def consumer_loop():
    app.logger.info(
        "stock consumer loop starting partition=%s instance_id=%s",
        KAFKA_CONSUMER_PARTITION,
        KAFKA_CONSUMER_INSTANCE_ID,
    )
    consumer = create_consumer(
        group_id="stock-service",
        topics=["find.stock", "subtract.stock", "rollback.stock", "gateway.stock.commands"],
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
            if "NOT_COORDINATOR" in error_str: # this is when the coordinator is not setup yet but the queues are waiting for it
                app.logger.warning("Kafka coordinator not ready yet: %s", msg.error())
            else:
                app.logger.error("Kafka error: %s", msg.error())
            continue

        key = msg.key().decode() if msg.key() else str(msg.offset())
        try:
            if msg.topic() == "gateway.stock.commands":
                command = msgspec.json.decode(msg.value(), type=dict)
                handle_http_command(command)
                consumer.commit(message=msg)
                continue
            message = decode_message(msg.value())
            app.logger.info("consumed topic=%s key=%s type=%s", msg.topic(), key, message.type)
            handle_message(message, key)
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
    app.logger.info("stock consumer thread started")


@app.post('/item/create/<price>')
def create_item(price: int):
    key = f"t{KAFKA_CONSUMER_PARTITION}_{uuid.uuid4()}"
    app.logger.debug(f"Item: {key} created")
    value = msgpack.encode(StockValue(stock=0, price=int(price)))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {}
    for i in range(n):
        kv_pairs[f"t{KAFKA_CONSUMER_PARTITION}_{i}"] = msgpack.encode(
            StockValue(stock=starting_stock, price=item_price)
        )
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    try:
        item_entry = subtract_stock_core(item_id, int(amount))
        db.set(item_id, msgpack.encode(item_entry))
    except ValueError as e:
        abort(400, str(e))
    except RuntimeError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


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
