import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request
from werkzeug.exceptions import HTTPException

DB_ERROR_STR = "DB error"


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


class IdempotencyResult(Struct):
    status: int
    body: str


def idempotency_record_key(scope: str, operation_id: str) -> str:
    # Scope keeps different endpoints from colliding on the same key.
    return f"_idem:{scope}:{operation_id}"


def get_idempotency_result(scope: str) -> IdempotencyResult | None:
    operation_id = request.headers.get("Idempotency-Key")
    if not operation_id:
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


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
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
    # If this operation ID was already applied, return the stored response.
    cached = get_idempotency_result("add_credit")
    if cached is not None:
        return Response(cached.body, status=cached.status)

    try:
        user_entry: UserValue = get_user_from_db(user_id)
        # update credit, serialize and update database
        user_entry.credit += int(amount)
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    except HTTPException as error:
        body = str(error.description)
        status = int(error.code) if error.code else 500
        # Record failure too, so retries return the same outcome.
        store_idempotency_result("add_credit", status, body)
        return Response(body, status=status)
    try:
        response_body = f"User: {user_id} credit updated to: {user_entry.credit}"
        # Persist first successful result for deduplication on retries.
        store_idempotency_result("add_credit", 200, response_body)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(response_body, status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    # If this operation ID was already applied, return the stored response.
    cached = get_idempotency_result("remove_credit")
    if cached is not None:
        return Response(cached.body, status=cached.status)

    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    try:
        user_entry: UserValue = get_user_from_db(user_id)
        # update credit, serialize and update database
        user_entry.credit -= int(amount)
        if user_entry.credit < 0:
            abort(400, f"User: {user_id} credit cannot get reduced below zero!")
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    except HTTPException as error:
        body = str(error.description)
        status = int(error.code) if error.code else 500
        # Record failure too, so retries return the same outcome.
        store_idempotency_result("remove_credit", status, body)
        return Response(body, status=status)
    try:
        response_body = f"User: {user_id} credit updated to: {user_entry.credit}"
        # Persist first successful result for deduplication on retries.
        store_idempotency_result("remove_credit", 200, response_body)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(response_body, status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
