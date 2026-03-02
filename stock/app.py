import logging
import os
import atexit
import uuid
from collections import defaultdict

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request


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


@app.post('/prepare/<tx_id>')
def prepare_stock(tx_id: str):
    payload = request.get_json(silent=True) or {}

    raw_items = payload.get("items")
    if not isinstance(raw_items, list) or len(raw_items) == 0:
        abort(400, "items payload is required")

    items = aggregate_items([(str(item_id), int(quantity)) for item_id, quantity in raw_items])
    tx_key = f"tx:{tx_id}"

    # WATCH/MULTI keeps "reserve stock + persist tx" as one atomic step.
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
                    # Duplicate prepare for same tx_id is intentionally accepted.
                    return Response("Stock prepare acknowledged", status=200)
                if existing_tx.state == "ABORTED":
                    abort(400, "Transaction already aborted")
                abort(400, "Transaction in invalid state")

            entries = pipe.mget(item_keys)
            decoded_items: list[StockValue] = []
            item_quantities = [q for _, q in items]
            for item_id, quantity, entry in zip(item_keys, item_quantities, entries):
                decoded = msgpack.decode(entry, type=StockValue) if entry else None
                if decoded is None:
                    abort(400, f"Item: {item_id} not found!")
                # Only currently unreserved stock can be reserved by this tx.
                if decoded.stock - decoded.reserved < quantity:
                    abort(400, f'Out of stock on item_id: {item_id}')
                decoded_items.append(decoded)

            pipe.multi()
            for (item_id, quantity), item_entry in zip(items, decoded_items):
                item_entry.reserved += quantity
                pipe.set(item_id, msgpack.encode(item_entry))
            pipe.set(tx_key, msgpack.encode(StockTransaction(state="PREPARED", items=items)))
            pipe.execute()
            return Response("Stock prepare acknowledged", status=200)
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
        finally:
            pipe.reset()
    abort(409, "Concurrent update conflict while preparing stock")


@app.post('/commit/<tx_id>')
def commit_stock(tx_id: str):
    tx_key = f"tx:{tx_id}"
    tx: StockTransaction | None = get_stock_tx(tx_id)
    if tx is None:
        abort(400, "Transaction not found")
    if tx.state == "COMMITTED":
        return Response("Stock commit acknowledged", status=200)
    if tx.state == "ABORTED":
        abort(400, "Transaction already aborted")
    if tx.state != "PREPARED":
        abort(400, "Transaction not prepared")

    # Commit turns reservation into final deduction; repeats are idempotent.
    for _ in range(5):
        pipe = db.pipeline()
        try:
            item_keys = [item_id for item_id, _ in tx.items]
            pipe.watch(tx_key, *item_keys)
            current_tx_raw = pipe.get(tx_key)
            if not current_tx_raw:
                abort(400, "Transaction not found")
            current_tx: StockTransaction = msgpack.decode(current_tx_raw, type=StockTransaction)
            if current_tx.state == "COMMITTED":
                return Response("Stock commit acknowledged", status=200)
            if current_tx.state == "ABORTED":
                abort(400, "Transaction already aborted")
            if current_tx.state != "PREPARED":
                abort(400, "Transaction not prepared")

            entries = pipe.mget(item_keys)
            decoded_items: list[StockValue] = []
            tx_items = current_tx.items
            item_quantities = [q for _, q in tx_items]
            for item_id, quantity, entry in zip(item_keys, item_quantities, entries):
                decoded = msgpack.decode(entry, type=StockValue) if entry else None
                if decoded is None or decoded.reserved < quantity:
                    abort(400, f"Invalid stock reservation for item: {item_id}")
                decoded_items.append(decoded)

            pipe.multi()
            for (item_id, quantity), item_entry in zip(tx_items, decoded_items):
                item_entry.reserved -= quantity
                item_entry.stock -= quantity
                if item_entry.stock < 0:
                    abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
                pipe.set(item_id, msgpack.encode(item_entry))
            pipe.set(tx_key, msgpack.encode(StockTransaction(state="COMMITTED", items=tx_items)))
            pipe.execute()
            return Response("Stock commit acknowledged", status=200)
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
        finally:
            pipe.reset()
    abort(409, "Concurrent update conflict while committing stock")


@app.post('/abort/<tx_id>')
def abort_stock(tx_id: str):
    tx_key = f"tx:{tx_id}"
    tx: StockTransaction | None = get_stock_tx(tx_id)
    if tx is None:
        return Response("Stock abort acknowledged", status=200)
    if tx.state == "ABORTED":
        return Response("Stock abort acknowledged", status=200)
    if tx.state == "COMMITTED":
        abort(400, "Transaction already committed")
    if tx.state != "PREPARED":
        abort(400, "Transaction not prepared")

    # Abort only releases reservations and never decrements final stock.
    for _ in range(5):
        pipe = db.pipeline()
        try:
            item_keys = [item_id for item_id, _ in tx.items]
            pipe.watch(tx_key, *item_keys)
            current_tx_raw = pipe.get(tx_key)
            if not current_tx_raw:
                return Response("Stock abort acknowledged", status=200)
            current_tx: StockTransaction = msgpack.decode(current_tx_raw, type=StockTransaction)
            if current_tx.state == "ABORTED":
                return Response("Stock abort acknowledged", status=200)
            if current_tx.state == "COMMITTED":
                abort(400, "Transaction already committed")
            if current_tx.state != "PREPARED":
                abort(400, "Transaction not prepared")

            entries = pipe.mget(item_keys)
            decoded_items: list[StockValue] = []
            tx_items = current_tx.items
            item_quantities = [q for _, q in tx_items]
            for item_id, quantity, entry in zip(item_keys, item_quantities, entries):
                decoded = msgpack.decode(entry, type=StockValue) if entry else None
                if decoded is None or decoded.reserved < quantity:
                    abort(400, f"Invalid stock reservation for item: {item_id}")
                decoded_items.append(decoded)

            pipe.multi()
            for item_id, quantity, item_entry in zip(item_keys, item_quantities, decoded_items):
                item_entry.reserved -= quantity
                pipe.set(item_id, msgpack.encode(item_entry))
            pipe.set(tx_key, msgpack.encode(StockTransaction(state="ABORTED", items=tx_items)))
            pipe.execute()
            return Response("Stock abort acknowledged", status=200)
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
        finally:
            pipe.reset()
    abort(409, "Concurrent update conflict while aborting stock")


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
