import logging
import os
import atexit
import uuid

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request

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
    # Funds reserved during prepare but not yet finally debited.
    reserved: int = 0


class PaymentTransaction(Struct):
    # PREPARED/COMMITTED/ABORTED terminal states are used for idempotency.
    state: str
    user_id: str
    amount: int


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


def get_payment_tx(tx_id: str) -> PaymentTransaction | None:
    try:
        tx_data: bytes | None = db.get(f"tx:{tx_id}")
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return msgpack.decode(tx_data, type=PaymentTransaction) if tx_data else None


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
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/prepare/<tx_id>')
def prepare_payment(tx_id: str):
    payload = request.get_json(silent=True) or {}
    user_id = payload.get("user_id")
    amount = payload.get("amount")
    if user_id is None or amount is None:
        abort(400, "user_id and amount payload are required")
    user_id = str(user_id)
    amount = int(amount) 
    if amount < 0:
        abort(400, "amount cannot be negative")

    tx_key = f"tx:{tx_id}"
    user_key = user_id

    #ensures reservation and tx record are written atomically.
    for _ in range(5):
        pipe = db.pipeline()
        try:
            pipe.watch(tx_key, user_key)
            existing_tx_raw = pipe.get(tx_key)
            if existing_tx_raw:
                existing_tx: PaymentTransaction = msgpack.decode(existing_tx_raw, type=PaymentTransaction)
                # Duplicate prepare for same tx_id is safe and returns success.
                if existing_tx.state in {"PREPARED", "COMMITTED"}:
                    return Response("Payment prepare acknowledged", status=200)
                if existing_tx.state == "ABORTED":
                    abort(400, "Transaction already aborted")
                abort(400, "Transaction in invalid state")

            user_raw = pipe.get(user_key)
            user_entry: UserValue | None = msgpack.decode(user_raw, type=UserValue) if user_raw else None
            if user_entry is None:
                abort(400, f"User: {user_id} not found!")
            # Only unreserved credit can be newly reserved.
            if user_entry.credit - user_entry.reserved < amount:
                abort(400, f"User: {user_id} credit cannot get reduced below zero!")

            user_entry.reserved += amount
            pipe.multi()
            pipe.set(user_key, msgpack.encode(user_entry))
            pipe.set(tx_key, msgpack.encode(PaymentTransaction(
                state="PREPARED",
                user_id=user_id,
                amount=amount
            )))
            pipe.execute()
            return Response("Payment prepare acknowledged", status=200)
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
        finally:
            pipe.reset()
    abort(409, "Concurrent update conflict while preparing payment")


@app.post('/commit/<tx_id>')
def commit_payment(tx_id: str):
    tx_key = f"tx:{tx_id}"
    tx: PaymentTransaction | None = get_payment_tx(tx_id)
    if tx is None:
        abort(400, "Transaction not found")
    if tx.state == "COMMITTED":
        return Response("Payment commit acknowledged", status=200)
    if tx.state == "ABORTED":
        abort(400, "Transaction already aborted")
    if tx.state != "PREPARED":
        abort(400, "Transaction not prepared")

    # Commit consumes previously reserved funds; repeated commit is idempotent.
    for _ in range(5):
        pipe = db.pipeline()
        try:
            pipe.watch(tx_key, tx.user_id)
            current_tx_raw = pipe.get(tx_key)
            if not current_tx_raw:
                abort(400, "Transaction not found")
            current_tx: PaymentTransaction = msgpack.decode(current_tx_raw, type=PaymentTransaction)
            if current_tx.state == "COMMITTED":
                return Response("Payment commit acknowledged", status=200)
            if current_tx.state == "ABORTED":
                abort(400, "Transaction already aborted")
            if current_tx.state != "PREPARED":
                abort(400, "Transaction not prepared")

            user_raw = pipe.get(current_tx.user_id)
            user_entry: UserValue | None = msgpack.decode(user_raw, type=UserValue) if user_raw else None
            if user_entry is None or user_entry.reserved < current_tx.amount:
                abort(400, "Invalid payment reservation")

            user_entry.reserved -= current_tx.amount
            user_entry.credit -= current_tx.amount
            if user_entry.credit < 0:
                abort(400, f"User: {current_tx.user_id} credit cannot get reduced below zero!")

            pipe.multi()
            pipe.set(current_tx.user_id, msgpack.encode(user_entry))
            pipe.set(tx_key, msgpack.encode(PaymentTransaction(
                state="COMMITTED",
                user_id=current_tx.user_id,
                amount=current_tx.amount
            )))
            pipe.execute()
            return Response("Payment commit acknowledged", status=200)
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
        finally:
            pipe.reset()
    abort(409, "Concurrent update conflict while committing payment")


@app.post('/abort/<tx_id>')
def abort_payment(tx_id: str):
    tx_key = f"tx:{tx_id}"
    tx: PaymentTransaction | None = get_payment_tx(tx_id)
    if tx is None:
        return Response("Payment abort acknowledged", status=200)
    if tx.state == "ABORTED":
        return Response("Payment abort acknowledged", status=200)
    if tx.state == "COMMITTED":
        abort(400, "Transaction already committed")
    if tx.state != "PREPARED":
        abort(400, "Transaction not prepared")

    # Abort only releases the reservation and never modifies final credit.
    for _ in range(5):
        pipe = db.pipeline()
        try:
            pipe.watch(tx_key, tx.user_id)
            current_tx_raw = pipe.get(tx_key)
            if not current_tx_raw:
                return Response("Payment abort acknowledged", status=200)
            current_tx: PaymentTransaction = msgpack.decode(current_tx_raw, type=PaymentTransaction)
            if current_tx.state == "ABORTED":
                return Response("Payment abort acknowledged", status=200)
            if current_tx.state == "COMMITTED":
                abort(400, "Transaction already committed")
            if current_tx.state != "PREPARED":
                abort(400, "Transaction not prepared")

            user_raw = pipe.get(current_tx.user_id)
            user_entry: UserValue | None = msgpack.decode(user_raw, type=UserValue) if user_raw else None
            if user_entry is None or user_entry.reserved < current_tx.amount:
                abort(400, "Invalid payment reservation")

            user_entry.reserved -= current_tx.amount
            pipe.multi()
            pipe.set(current_tx.user_id, msgpack.encode(user_entry))
            pipe.set(tx_key, msgpack.encode(PaymentTransaction(
                state="ABORTED",
                user_id=current_tx.user_id,
                amount=current_tx.amount
            )))
            pipe.execute()
            return Response("Payment abort acknowledged", status=200)
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            abort(400, DB_ERROR_STR)
        finally:
            pipe.reset()
    abort(409, "Concurrent update conflict while aborting payment")


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
