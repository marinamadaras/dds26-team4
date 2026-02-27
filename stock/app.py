import logging
import os
import atexit
import threading
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
from stock.utils.kafka_client import publish, create_consumer, decode_message
from stock.utils.messages import BaseMessage, FindStock, FindStockReply


DB_ERROR_STR = "DB error"

app = Flask("stock-service")

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


_consumer_started = False


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
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return None
    return msgpack.decode(entry, type=StockValue) if entry else None


def handle_find_stock(message: FindStock, order_id: str):
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


def handle_message(message: BaseMessage, key: str):
    if isinstance(message, FindStock):
        handle_find_stock(message, key)
        return
    app.logger.warning(f"No handler registered for message type: {message.type}")


def consumer_loop():
    consumer = create_consumer(
        group_id="stock-service",
        topics=["find.stock"],
    )
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            app.logger.debug(f"Kafka error: {msg.error()}")
            continue
        try:
            message = decode_message(msg.value())
            key = msg.key().decode() if msg.key() else ""
            handle_message(message, key)
            consumer.commit(message=msg)
        except Exception as e:
            app.logger.debug(f"Processing error: {e}")


def start_consumer():
    global _consumer_started
    if _consumer_started:
        return
    thread = threading.Thread(target=consumer_loop, daemon=True)
    thread.start()
    _consumer_started = True


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
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


if __name__ == '__main__':
    start_consumer()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
