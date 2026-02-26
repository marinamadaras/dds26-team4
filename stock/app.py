import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request
from werkzeug.exceptions import HTTPException


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


class IdempotencyResult(Struct):
    status: int
    body: str


def idempotency_record_key(scope: str, operation_id: str) -> str:
    # Scope keeps different endpoints from colliding on the same key.
    return f"_idem:{scope}:{operation_id}"


def get_idempotency_result(scope: str) -> IdempotencyResult | None:
    operation_id = request.headers.get("Idempotency-Key")
    if not operation_id: # if the client did not send an idempotency key, then it is not applied, the key is generated at order service
        return None
    try:
        entry: bytes = db.get(idempotency_record_key(scope, operation_id))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return msgpack.decode(entry, type=IdempotencyResult) if entry else None


def store_idempotency_result(scope: str, status: int, body: str):
    operation_id = request.headers.get("Idempotency-Key")
    if not operation_id:
        return
    result = IdempotencyResult(status=status, body=body)
    try:
        db.set(idempotency_record_key(scope, operation_id), msgpack.encode(result))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


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
    # If this operation ID was already applied, return the stored response.
    cached = get_idempotency_result("add_stock")
    if cached is not None:
        return Response(cached.body, status=cached.status)

    try:
        item_entry: StockValue = get_item_from_db(item_id)
        # update stock, serialize and update database
        item_entry.stock += int(amount)
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    except HTTPException as error:
        body = str(error.description)
        status = int(error.code) if error.code else 500
        # Record failure too, so retries return the same outcome.
        store_idempotency_result("add_stock", status, body)
        return Response(body, status=status)
    try:
        response_body = f"Item: {item_id} stock updated to: {item_entry.stock}"
        # Persist first successful result for deduplication on retries.
        store_idempotency_result("add_stock", 200, response_body)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(response_body, status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    # If this operation ID was already applied, return the stored response.
    cached = get_idempotency_result("remove_stock")
    if cached is not None:
        return Response(cached.body, status=cached.status)

    try:
        item_entry: StockValue = get_item_from_db(item_id)
        # update stock, serialize and update database
        item_entry.stock -= int(amount)
        app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
        if item_entry.stock < 0:
            abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    except HTTPException as error:
        body = str(error.description)
        status = int(error.code) if error.code else 500
        # Record failure too, so retries return the same outcome.
        store_idempotency_result("remove_stock", status, body)
        return Response(body, status=status)
    try:
        response_body = f"Item: {item_id} stock updated to: {item_entry.stock}"
        # Persist first successful result for deduplication on retries.
        store_idempotency_result("remove_stock", 200, response_body)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(response_body, status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
