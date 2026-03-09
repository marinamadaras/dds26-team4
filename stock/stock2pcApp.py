import logging
import os
import atexit
import threading
import uuid
from collections import defaultdict

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request
from kafka_client import publish, create_consumer, decode_message
from messages import (
    BaseMessage,
    FindStock,
    FindStockReply,
    SubtractStock,
    StockSubtractedReply,
    RollbackStockRequest,
    RollbackStockReply,
    PrepareStockRequest,
    PrepareStockReply,
    StockDecisionRequest,
    StockDecisionReply,
)


DB_ERROR_STR = "DB error"

app = Flask("stock-service")
_consumer_thread: threading.Thread | None = None

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int
    # Units reserved during prepare but not yet finally deducted.
    reserved: int = 0


class StockTransaction(Struct):
    # PREPARED/COMMITTED/ABORTED states make participant calls idempotent.
    state: str
    items: list[tuple[str, int]]


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


def aggregate_items(items: list[tuple[str, int]]) -> list[tuple[str, int]]:
    # Consolidate duplicates so reservation checks are done per unique item id.
    totals: dict[str, int] = defaultdict(int)
    for item_id, quantity in items:
        totals[str(item_id)] += int(quantity)
    return [(item_id, quantity) for item_id, quantity in totals.items()]


def get_stock_tx(tx_id: str) -> StockTransaction | None:
    try:
        tx_data: bytes | None = db.get(f"tx:{tx_id}")
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return msgpack.decode(tx_data, type=StockTransaction) if tx_data else None


def get_item_from_db_nullable(item_id: str) -> StockValue | None:
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return None
    return msgpack.decode(entry, type=StockValue) if entry else None


def subtract_stock_core(item_id: str, amount: int) -> StockValue:
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        raise ValueError(f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError as e:
        raise RuntimeError(DB_ERROR_STR) from e
    return item_entry


def handle_find_stock(message: FindStock, order_id: str):
    app.logger.info("received find.stock request order_id=%s item_id=%s qty=%s", order_id, message.item_id, message.quantity)
    item_entry = get_item_from_db_nullable(message.item_id)
    if item_entry is None:
        reply = FindStockReply(
            order_id=order_id,
            item_id=message.item_id,
            quantity=message.quantity,
            found=False,
        )
    else:
        reply = FindStockReply(
            order_id=order_id,
            item_id=message.item_id,
            quantity=message.quantity,
            found=True,
            stock=item_entry.stock,
            price=item_entry.price,
        )

    publish(
        topic="find.stock.replies",
        key=order_id,
        value=reply,
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
    if isinstance(message, PrepareStockRequest):
        handle_prepare_stock_message(message)
        return
    if isinstance(message, StockDecisionRequest):
        handle_stock_decision_message(message)
        return
    app.logger.warning(f"No handler registered for message type: {message.type}")


def handle_subtract_stock(message: SubtractStock):
    try:
        subtract_stock_core(message.item_id, int(message.quantity))
        reply = StockSubtractedReply(
            order_id=message.order_id,
            item_id=message.item_id,
            quantity=message.quantity,
            success=True,
        )
    except Exception as e:
        reply = StockSubtractedReply(
            order_id=message.order_id,
            item_id=message.item_id,
            quantity=message.quantity,
            success=False,
            error=str(e),
        )

    publish(
        topic="subtract.stock.replies",
        key=message.order_id,
        value=reply,
    )

# todo: implement stock rollback logic and adapt the reply down here
def handle_rollback_stock(message: RollbackStockRequest):
    reply = RollbackStockReply(
        order_id=message.order_id,
        item_id=message.item_id,
        quantity=message.quantity,
        success=False,
        error="rollback stock not implemented",
    )
    publish(
        topic="rollback.stock.replies",
        key=message.order_id,
        value=reply,
    )


def handle_prepare_stock_message(message: PrepareStockRequest):
    success, error = prepare_stock_tx(message.tx_id, message.items)
    reply = PrepareStockReply(tx_id=message.tx_id, success=success, error=error)
    publish(topic="2pc.stock.prepare.replies", key=message.tx_id, value=reply)


def handle_stock_decision_message(message: StockDecisionRequest):
    decision = str(message.decision).upper()
    if decision == "COMMIT":
        success, error = commit_stock_tx(message.tx_id)
    elif decision == "ABORT":
        success, error = abort_stock_tx(message.tx_id)
    else:
        success, error = False, "Unsupported decision"
    reply = StockDecisionReply(
        tx_id=message.tx_id,
        decision=decision,
        success=success,
        error=error,
    )
    publish(topic="2pc.stock.decision.replies", key=message.tx_id, value=reply)

def consumer_loop():
    app.logger.info("stock consumer loop starting")
    consumer = create_consumer(
        group_id="stock-service",
        topics=[
            "find.stock",
            "subtract.stock",
            "rollback.stock",
            "2pc.stock.prepare",
            "2pc.stock.decision",
        ],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            app.logger.error("Kafka error: %s", msg.error())
            continue
        try:
            message = decode_message(msg.value())
            key = msg.key().decode() if msg.key() else ""
            app.logger.info("consumed topic=%s key=%s type=%s", msg.topic(), key, message.type)
            handle_message(message, key)
            consumer.commit(message=msg)
        except Exception as e:
            app.logger.exception("Processing error in stock consumer: %s", e)
            # prevent one malformed record from blocking the partition forever
            consumer.commit(message=msg)


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
    key = str(uuid.uuid4())
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
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
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
    except ValueError as e:
        abort(400, str(e))
    except RuntimeError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/prepare/<tx_id>')
def prepare_stock(tx_id: str):
    payload = request.get_json(silent=True) or {}

    raw_items = payload.get("items")
    if not isinstance(raw_items, list) or len(raw_items) == 0:
        abort(400, "items payload is required")
    parsed_items = [(str(item_id), int(quantity)) for item_id, quantity in raw_items]
    success, error = prepare_stock_tx(tx_id, parsed_items)
    if not success:
        abort(400, error or "Stock prepare failed")
    return Response("Stock prepare acknowledged", status=200)


def prepare_stock_tx(tx_id: str, raw_items: list[tuple[str, int]]) -> tuple[bool, str | None]:
    if len(raw_items) == 0:
        return False, "items payload is required"
    items = aggregate_items([(str(item_id), int(quantity)) for item_id, quantity in raw_items])
    tx_key = f"tx:{tx_id}"

    for _ in range(5):
        pipe = db.pipeline()
        try:
            item_keys = [item_id for item_id, _ in items]
            watch_keys = [tx_key] + item_keys
            pipe.watch(*watch_keys)

            existing_tx_raw = pipe.get(tx_key)
            if existing_tx_raw:
                existing_tx: StockTransaction = msgpack.decode(existing_tx_raw, type=StockTransaction)
                if existing_tx.state in {"PREPARED", "COMMITTED"}:
                    return True, None
                if existing_tx.state == "ABORTED":
                    return False, "Transaction already aborted"
                return False, "Transaction in invalid state"

            entries = pipe.mget(item_keys)
            decoded_items: list[StockValue] = []
            item_quantities = [q for _, q in items]
            for item_id, quantity, entry in zip(item_keys, item_quantities, entries):
                decoded = msgpack.decode(entry, type=StockValue) if entry else None
                if decoded is None:
                    return False, f"Item: {item_id} not found!"
                if decoded.stock - decoded.reserved < quantity:
                    return False, f'Out of stock on item_id: {item_id}'
                decoded_items.append(decoded)

            pipe.multi()
            for (item_id, quantity), item_entry in zip(items, decoded_items):
                item_entry.reserved += quantity
                pipe.set(item_id, msgpack.encode(item_entry))
            pipe.set(tx_key, msgpack.encode(StockTransaction(state="PREPARED", items=items)))
            pipe.execute()
            return True, None
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, DB_ERROR_STR
        finally:
            pipe.reset()
    return False, "Concurrent update conflict while preparing stock"


@app.post('/commit/<tx_id>')
def commit_stock(tx_id: str):
    success, error = commit_stock_tx(tx_id)
    if not success:
        abort(400, error or "Stock commit failed")
    return Response("Stock commit acknowledged", status=200)


def commit_stock_tx(tx_id: str) -> tuple[bool, str | None]:
    tx_key = f"tx:{tx_id}"
    tx: StockTransaction | None = get_stock_tx(tx_id)
    if tx is None:
        return False, "Transaction not found"
    if tx.state == "COMMITTED":
        return True, None
    if tx.state == "ABORTED":
        return False, "Transaction already aborted"
    if tx.state != "PREPARED":
        return False, "Transaction not prepared"

    for _ in range(5):
        pipe = db.pipeline()
        try:
            item_keys = [item_id for item_id, _ in tx.items]
            pipe.watch(tx_key, *item_keys)
            current_tx_raw = pipe.get(tx_key)
            if not current_tx_raw:
                return False, "Transaction not found"
            current_tx: StockTransaction = msgpack.decode(current_tx_raw, type=StockTransaction)
            if current_tx.state == "COMMITTED":
                return True, None
            if current_tx.state == "ABORTED":
                return False, "Transaction already aborted"
            if current_tx.state != "PREPARED":
                return False, "Transaction not prepared"

            entries = pipe.mget(item_keys)
            decoded_items: list[StockValue] = []
            tx_items = current_tx.items
            item_quantities = [q for _, q in tx_items]
            for item_id, quantity, entry in zip(item_keys, item_quantities, entries):
                decoded = msgpack.decode(entry, type=StockValue) if entry else None
                if decoded is None or decoded.reserved < quantity:
                    return False, f"Invalid stock reservation for item: {item_id}"
                decoded_items.append(decoded)

            pipe.multi()
            for (item_id, quantity), item_entry in zip(tx_items, decoded_items):
                item_entry.reserved -= quantity
                item_entry.stock -= quantity
                if item_entry.stock < 0:
                    return False, f"Item: {item_id} stock cannot get reduced below zero!"
                pipe.set(item_id, msgpack.encode(item_entry))
            pipe.set(tx_key, msgpack.encode(StockTransaction(state="COMMITTED", items=tx_items)))
            pipe.execute()
            return True, None
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, DB_ERROR_STR
        finally:
            pipe.reset()
    return False, "Concurrent update conflict while committing stock"


@app.post('/abort/<tx_id>')
def abort_stock(tx_id: str):
    success, error = abort_stock_tx(tx_id)
    if not success:
        abort(400, error or "Stock abort failed")
    return Response("Stock abort acknowledged", status=200)


def abort_stock_tx(tx_id: str) -> tuple[bool, str | None]:
    tx_key = f"tx:{tx_id}"
    tx: StockTransaction | None = get_stock_tx(tx_id)
    if tx is None:
        return True, None
    if tx.state == "ABORTED":
        return True, None
    if tx.state == "COMMITTED":
        return False, "Transaction already committed"
    if tx.state != "PREPARED":
        return False, "Transaction not prepared"

    for _ in range(5):
        pipe = db.pipeline()
        try:
            item_keys = [item_id for item_id, _ in tx.items]
            pipe.watch(tx_key, *item_keys)
            current_tx_raw = pipe.get(tx_key)
            if not current_tx_raw:
                return True, None
            current_tx: StockTransaction = msgpack.decode(current_tx_raw, type=StockTransaction)
            if current_tx.state == "ABORTED":
                return True, None
            if current_tx.state == "COMMITTED":
                return False, "Transaction already committed"
            if current_tx.state != "PREPARED":
                return False, "Transaction not prepared"

            entries = pipe.mget(item_keys)
            decoded_items: list[StockValue] = []
            tx_items = current_tx.items
            item_quantities = [q for _, q in tx_items]
            for item_id, quantity, entry in zip(item_keys, item_quantities, entries):
                decoded = msgpack.decode(entry, type=StockValue) if entry else None
                if decoded is None or decoded.reserved < quantity:
                    return False, f"Invalid stock reservation for item: {item_id}"
                decoded_items.append(decoded)

            pipe.multi()
            for item_id, quantity, item_entry in zip(item_keys, item_quantities, decoded_items):
                item_entry.reserved -= quantity
                pipe.set(item_id, msgpack.encode(item_entry))
            pipe.set(tx_key, msgpack.encode(StockTransaction(state="ABORTED", items=tx_items)))
            pipe.execute()
            return True, None
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, DB_ERROR_STR
        finally:
            pipe.reset()
    return False, "Concurrent update conflict while aborting stock"


if __name__ == '__main__':
    start_consumer()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    start_consumer()
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
