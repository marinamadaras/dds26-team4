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

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request
from kafka_client import publish, publish_raw, create_consumer, decode_message
from messages import (
    BaseMessage,
    PaymentRequest,
    PaymentReply,
    RollbackPaymentRequest,
    RollbackPaymentReply,
    PreparePaymentRequest,
    PreparePaymentReply,
    PaymentDecisionRequest,
    PaymentDecisionReply,
)
from span_logger import span
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


def _partition_from_order_id(order_id: str) -> int:
    match = re.match(r"^s(\d+)_", order_id)
    if match:
        return int(match.group(1))
    return KAFKA_CONSUMER_PARTITION


class UserValue(Struct):
    credit: int
    # Funds reserved during prepare but not yet finally debited.
    reserved: int = 0


class PaymentTransaction(Struct):
    # PREPARED/COMMITTED/ABORTED terminal states are used for idempotency.
    state: str
    user_id: str
    amount: int


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
    if isinstance(message, PreparePaymentRequest):
        handle_prepare_payment_message(message)
        return
    if isinstance(message, PaymentDecisionRequest):
        handle_payment_decision_message(message)
        return
    app.logger.warning("No handler for message type=%s", message.type)


# todo: implement payment rollback logic and adapt the reply down here
def handle_rollback_payment_request(message: RollbackPaymentRequest):
    reply = RollbackPaymentReply(
        order_id=message.order_id,
        user_id=message.user_id,
        amount=message.amount,
        success=False,
        error="rollback payment not implemented",
    )


    _submit_kafka_task(
        topic="rollback.payment.replies",
        idempotency_key=message.idempotency_key,
        key=message.order_id,
        message=reply,
        partition=_partition_from_order_id(message.order_id),
    )


def handle_prepare_payment_message(message: PreparePaymentRequest):
    with span(
            app.logger,
            "payment",
            "payment.prepare.handle",
            trace_id=message.tx_id,
            user_id=message.user_id,
            amount=message.amount,
    ):
        success, error = prepare_payment_tx(message.tx_id, message.user_id, int(message.amount))
        reply = PreparePaymentReply(
            tx_id=message.tx_id,
            coordinator_partition=message.coordinator_partition,
            success=success,
            error=error,
        )
        with span(
                app.logger,
                "payment",
                "payment.prepare.reply_publish",
                trace_id=message.tx_id,
                success=success,
        ):
            _submit_kafka_task(
                topic="2pc.payment.prepare.replies",
                idempotency_key=message.idempotency_key,
                key=message.tx_id,
                message=reply,
                partition=message.coordinator_partition
            )


def handle_payment_decision_message(message: PaymentDecisionRequest):
    decision = str(message.decision).upper()
    with span(
            app.logger,
            "payment",
            "payment.decision.handle",
            trace_id=message.tx_id,
            decision=decision,
    ):
        if decision == "COMMIT":
            success, error = commit_payment_tx(message.tx_id)
        elif decision == "ABORT":
            success, error = abort_payment_tx(message.tx_id)
        else:
            success, error = False, "Unsupported decision"
        reply = PaymentDecisionReply(
            tx_id=message.tx_id,
            coordinator_partition=message.coordinator_partition,
            decision=decision,
            success=success,
            error=error,
        )
        with span(
                app.logger,
                "payment",
                "payment.decision.reply_publish",
                trace_id=message.tx_id,
                decision=decision,
                success=success,
        ):
            _submit_kafka_task(
                topic="2pc.payment.decision.replies",
                idempotency_key=message.idempotency_key,
                key=message.tx_id,
                message=reply,
                partition=message.coordinator_partition,
            )


def consumer_loop():
    app.logger.info(
        "payment consumer loop starting partition=%s instance_id=%s",
        KAFKA_CONSUMER_PARTITION,
        KAFKA_CONSUMER_INSTANCE_ID,
    )
    consumer = create_consumer(
        group_id="payment-service",
        topics=[
            "payment",
            "rollback.payment",
            "2pc.payment.prepare",
            "2pc.payment.decision",
            "gateway.payment.commands",
        ],
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
    except ValueError as e:
        abort(400, str(e))
    except RuntimeError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/prepare/<tx_id>')
def prepare_payment(tx_id: str):
    payload = request.get_json(silent=True) or {}
    user_id = payload.get("user_id")
    amount = payload.get("amount")
    if user_id is None or amount is None:
        abort(400, "user_id and amount payload are required")
    success, error = prepare_payment_tx(tx_id, str(user_id), int(amount))
    if not success:
        abort(400, error or "Payment prepare failed")
    return Response("Payment prepare acknowledged", status=200)


def prepare_payment_tx(tx_id: str, user_id: str, amount: int) -> tuple[bool, str | None]:
    user_id = str(user_id)
    amount = int(amount)
    if amount < 0:
        return False, "amount cannot be negative"

    tx_key = f"tx:{tx_id}"
    user_key = user_id

    # Ensures reservation and tx record are written atomically.
    for _ in range(5):
        pipe = db.pipeline()
        try:
            pipe.watch(tx_key, user_key)
            existing_tx_raw = pipe.get(tx_key)
            if existing_tx_raw:
                existing_tx: PaymentTransaction = msgpack.decode(existing_tx_raw, type=PaymentTransaction)
                if existing_tx.state in {"PREPARED", "COMMITTED"}:
                    return True, None
                if existing_tx.state == "ABORTED":
                    return False, "Transaction already aborted"
                return False, "Transaction in invalid state"

            user_raw = pipe.get(user_key)
            user_entry: UserValue | None = msgpack.decode(user_raw, type=UserValue) if user_raw else None
            if user_entry is None:
                return False, f"User: {user_id} not found!"
            if user_entry.credit - user_entry.reserved < amount:
                return False, f"User: {user_id} credit cannot get reduced below zero!"

            user_entry.reserved += amount
            pipe.multi()
            pipe.set(user_key, msgpack.encode(user_entry))
            pipe.set(tx_key, msgpack.encode(PaymentTransaction(
                state="PREPARED",
                user_id=user_id,
                amount=amount
            )))
            pipe.execute()
            return True, None
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, DB_ERROR_STR
        finally:
            pipe.reset()
    return False, "Concurrent update conflict while preparing payment"


@app.post('/commit/<tx_id>')
def commit_payment(tx_id: str):
    success, error = commit_payment_tx(tx_id)
    if not success:
        abort(400, error or "Payment commit failed")
    return Response("Payment commit acknowledged", status=200)


def commit_payment_tx(tx_id: str) -> tuple[bool, str | None]:
    tx_key = f"tx:{tx_id}"
    tx: PaymentTransaction | None = get_payment_tx(tx_id)
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
            pipe.watch(tx_key, tx.user_id)
            current_tx_raw = pipe.get(tx_key)
            if not current_tx_raw:
                return False, "Transaction not found"
            current_tx: PaymentTransaction = msgpack.decode(current_tx_raw, type=PaymentTransaction)
            if current_tx.state == "COMMITTED":
                return True, None
            if current_tx.state == "ABORTED":
                return False, "Transaction already aborted"
            if current_tx.state != "PREPARED":
                return False, "Transaction not prepared"

            user_raw = pipe.get(current_tx.user_id)
            user_entry: UserValue | None = msgpack.decode(user_raw, type=UserValue) if user_raw else None
            if user_entry is None or user_entry.reserved < current_tx.amount:
                return False, "Invalid payment reservation"

            user_entry.reserved -= current_tx.amount
            user_entry.credit -= current_tx.amount
            if user_entry.credit < 0:
                return False, f"User: {current_tx.user_id} credit cannot get reduced below zero!"

            pipe.multi()
            pipe.set(current_tx.user_id, msgpack.encode(user_entry))
            pipe.set(tx_key, msgpack.encode(PaymentTransaction(
                state="COMMITTED",
                user_id=current_tx.user_id,
                amount=current_tx.amount
            )))
            pipe.execute()
            return True, None
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, DB_ERROR_STR
        finally:
            pipe.reset()
    return False, "Concurrent update conflict while committing payment"


@app.post('/abort/<tx_id>')
def abort_payment(tx_id: str):
    success, error = abort_payment_tx(tx_id)
    if not success:
        abort(400, error or "Payment abort failed")
    return Response("Payment abort acknowledged", status=200)


def abort_payment_tx(tx_id: str) -> tuple[bool, str | None]:
    tx_key = f"tx:{tx_id}"
    tx: PaymentTransaction | None = get_payment_tx(tx_id)
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
            pipe.watch(tx_key, tx.user_id)
            current_tx_raw = pipe.get(tx_key)
            if not current_tx_raw:
                return True, None
            current_tx: PaymentTransaction = msgpack.decode(current_tx_raw, type=PaymentTransaction)
            if current_tx.state == "ABORTED":
                return True, None
            if current_tx.state == "COMMITTED":
                return False, "Transaction already committed"
            if current_tx.state != "PREPARED":
                return False, "Transaction not prepared"

            user_raw = pipe.get(current_tx.user_id)
            user_entry: UserValue | None = msgpack.decode(user_raw, type=UserValue) if user_raw else None
            if user_entry is None or user_entry.reserved < current_tx.amount:
                return False, "Invalid payment reservation"

            user_entry.reserved -= current_tx.amount
            pipe.multi()
            pipe.set(current_tx.user_id, msgpack.encode(user_entry))
            pipe.set(tx_key, msgpack.encode(PaymentTransaction(
                state="ABORTED",
                user_id=current_tx.user_id,
                amount=current_tx.amount
            )))
            pipe.execute()
            return True, None
        except redis.exceptions.WatchError:
            continue
        except redis.exceptions.RedisError:
            return False, DB_ERROR_STR
        finally:
            pipe.reset()
    return False, "Concurrent update conflict while aborting payment"


if __name__ == '__main__':
    start_consumer()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    start_consumer()
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
