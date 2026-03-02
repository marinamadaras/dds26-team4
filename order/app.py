import logging
import os
import atexit
import random
import uuid
import time
import threading
from collections import defaultdict

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from kafka_client import publish, create_consumer, decode_message
from messages import (
    BaseMessage,
    SubtractStock,
    StockSubtractedReply,
    FindStock,
    FindStockReply,
    PaymentRequest,
    PaymentReply,
    RollbackStockRequest,
    RollbackStockReply,
    RollbackPaymentRequest,
    RollbackPaymentReply,
    CheckoutRequested,
)

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")
_consumer_thread: threading.Thread | None = None

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()

def start_consumer():
    global _consumer_thread
    if _consumer_thread is not None and _consumer_thread.is_alive():
        return
    thread = threading.Thread(target=consumer_loop, daemon=True)
    thread.start()
    _consumer_thread = thread
    app.logger.info("order consumer thread started")

atexit.register(close_db_connection)

def publish_find_stock(order_id: str, item_id: str, quantity: int):
    message = FindStock(item_id=item_id, quantity=int(quantity))

    publish(
        topic="find.stock",
        key=order_id,
        value=message,
    )


def publish_subtract_stock(order_id: str, item_id: str, quantity: int):
    message = SubtractStock(order_id=order_id, item_id=item_id, quantity=int(quantity))
    publish(
        topic="subtract.stock",
        key=message.order_id,
        value=message,
    )


def publish_payment(order_id: str, user_id: str, amount: int):
    message = PaymentRequest(order_id=order_id, user_id=user_id, amount=int(amount))
    publish(
        topic="payment",
        key=order_id,
        value=message,
    )


def publish_rollback_stock(order_id: str, item_id: str, quantity: int):
    message = RollbackStockRequest(order_id=order_id, item_id=item_id, quantity=int(quantity))
    publish(
        topic="rollback.stock",
        key=order_id,
        value=message,
    )


def publish_rollback_payment(order_id: str, user_id: str, amount: int):
    message = RollbackPaymentRequest(order_id=order_id, user_id=user_id, amount=int(amount))
    publish(
        topic="rollback.payment",
        key=order_id,
        value=message,
    )


def publish_checkout_requested(order_id: str):
    message = CheckoutRequested(order_id=order_id)
    publish(
        topic="checkout.requested",
        key=order_id,
        value=message,
    )


def handle_find_stock_reply(message: FindStockReply):
    app.logger.info(f"received find stock reply: {message.order_id}")
    if not message.found:
        app.logger.warning(
            f"Stock not found for item {message.item_id} in order {message.order_id}"
        )
        return  # nothing to update

    try:
        order_entry: OrderValue = get_order_from_db(message.order_id)

        order_entry.items.append(
            (message.item_id, int(message.quantity))
        )

        order_entry.total_cost += (
            int(message.quantity) * message.price
        )

        db.set(message.order_id, msgpack.encode(order_entry))

        app.logger.info(
            f"Order {message.order_id} updated asynchronously"
        )

    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error: {e}")
        raise  # todo: let consumer retry?


def handle_stock_subtracted_reply(message: StockSubtractedReply):
    # todo: implement with sagas, we need to keep track of all pending items somehow
    # since we are async, we send multiple requests to the stock service for each item
    # and we can only proceed with the payment when we have received all of them
    app.logger.info("stock subtracted not yet implemented", message.order_id)


def handle_payment_reply(message: PaymentReply):
    # todo: implement with sagas, we need to keep track of all pending checkouts somehow
    app.logger.info("stock subtracted not yet implemented", message.order_id)


def handle_rollback_stock_reply(message: RollbackStockReply):
    app.logger.info(
        "rollback.stock.replies received order_id=%s item_id=%s success=%s",
        message.order_id,
        message.item_id,
        message.success,
    )

def handle_rollback_payment_reply(message: RollbackPaymentReply):
    app.logger.info(
        "rollback.payment.replies received order_id=%s user_id=%s success=%s",
        message.order_id,
        message.user_id,
        message.success,
    )


def handle_checkout_requested(message: CheckoutRequested):
    process_checkout_async(message.order_id)


def handle_message(message: BaseMessage):
    if isinstance(message, FindStockReply):
        handle_find_stock_reply(message)
        return
    if isinstance(message, CheckoutRequested):
        handle_checkout_requested(message)
        return
    if isinstance(message, StockSubtractedReply):
        handle_stock_subtracted_reply(message)
        return
    if isinstance(message, PaymentReply):
        handle_payment_reply(message)
        return
    if isinstance(message, RollbackStockReply):
        handle_rollback_stock_reply(message)
        return
    if isinstance(message, RollbackPaymentReply):
        handle_rollback_payment_reply(message)
        return
    app.logger.warning(f"No handler registered for message type: {message.type}")


def consumer_loop():
    app.logger.info("order consumer loop starting")
    consumer = create_consumer(
        group_id="order-service",
        topics=[
            "checkout.requested",
            "find.stock.replies",
            "subtract.stock.replies",
            "payment.replies",
            "rollback.stock.replies",
            "rollback.payment.replies",
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
            handle_message(message)
            consumer.commit(message=msg)

        except Exception as e:
            app.logger.exception("Processing error in order consumer: %s", e)
            consumer.commit(message=msg)
            # todo: perform retry logic here
            # no commit -> message will be retried


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


class CheckoutTransaction(Struct):
    # PREPARING -> DECIDED -> COMPLETED
    state: str
    # COMMIT or ABORT once coordinator reaches a durable decision
    decision: str
    order_id: str
    user_id: str
    total_cost: int
    items: list[tuple[str, int]]


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


def get_order_if_exists(order_id: str) -> OrderValue | None:
    try:
        entry: bytes | None = db.get(order_id)
    except redis.exceptions.RedisError:
        return None
    return msgpack.decode(entry, type=OrderValue) if entry else None


def get_tx_from_db(tx_id: str) -> CheckoutTransaction | None:
    try:
        entry: bytes | None = db.get(f"tx:{tx_id}")
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)
    return msgpack.decode(entry, type=CheckoutTransaction) if entry else None


def set_tx(tx_id: str, tx: CheckoutTransaction):
    # Coordinator decision/state must be persisted before contacting participants.
    try:
        db.set(f"tx:{tx_id}", msgpack.encode(tx))
    except redis.exceptions.RedisError:
        abort(400, DB_ERROR_STR)


def aggregate_items(items: list[tuple[str, int]]) -> list[tuple[str, int]]:
    # Merge duplicate item lines so participants reserve/commit once per item id.
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in items:
        items_quantities[item_id] += quantity
    return [(item_id, quantity) for item_id, quantity in items_quantities.items()]


def send_post_request(url: str, json_payload: dict | None = None):
    try:
        response = requests.post(url, json=json_payload, timeout=5)
    except requests.exceptions.RequestException:
        return None
    return response


def send_get_request(url: str):
    try:
        response = requests.get(url, timeout=5)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def checkout_lock_key(order_id: str) -> str:
    return f"checkout:lock:{order_id}"


def try_acquire_checkout_lock(order_id: str) -> bool:
    try:
        return bool(db.set(checkout_lock_key(order_id), "1", ex=60, nx=True))
    except redis.exceptions.RedisError:
        return False


def release_checkout_lock(order_id: str):
    try:
        db.delete(checkout_lock_key(order_id))
    except redis.exceptions.RedisError:
        app.logger.warning("Failed to release checkout lock for order %s", order_id)


def is_checkout_in_progress(order_id: str) -> bool:
    try:
        return bool(db.exists(checkout_lock_key(order_id)))
    except redis.exceptions.RedisError:
        return False


def execute_decision(tx_id: str, tx: CheckoutTransaction) -> bool:
    # Phase 2: fan out the already-persisted decision to all participants.
    if tx.decision == "COMMIT":
        stock_reply = send_post_request(f"{GATEWAY_URL}/stock/commit/{tx_id}")
        payment_reply = send_post_request(f"{GATEWAY_URL}/payment/commit/{tx_id}")
    else:
        stock_reply = send_post_request(f"{GATEWAY_URL}/stock/abort/{tx_id}")
        payment_reply = send_post_request(f"{GATEWAY_URL}/payment/abort/{tx_id}")

    return (
        stock_reply is not None
        and payment_reply is not None
        and stock_reply.status_code == 200
        and payment_reply.status_code == 200
    )


def apply_commit_effects(tx: CheckoutTransaction) -> bool:
    # Order is marked paid only after both participants acknowledge COMMIT.
    order_entry = get_order_if_exists(tx.order_id)
    if order_entry is None:
        return False
    if order_entry.paid:
        return True
    order_entry.paid = True
    try:
        db.set(tx.order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return False
    return True


def finalize_transaction(tx_id: str, tx: CheckoutTransaction) -> bool:
    # Short retry loop keeps request path responsive; unresolved tx is recovered on startup.
    for _ in range(3):
        if execute_decision(tx_id, tx):
            if tx.decision == "COMMIT" and not apply_commit_effects(tx):
                return False
            tx.state = "COMPLETED"
            set_tx(tx_id, tx)
            return True
        time.sleep(0.2)
    return False


def recover_in_doubt_transactions():
    # Recovery rule:
    # - PREPARING has no durable decision yet, force ABORT.
    # - DECIDED replays COMMIT/ABORT until participants acknowledge.
    try:
        tx_keys = list(db.scan_iter("tx:*"))
    except redis.exceptions.RedisError:
        return

    for raw_key in tx_keys:
        tx_key = raw_key.decode()
        tx_id = tx_key.split("tx:", 1)[1]
        try:
            tx_raw: bytes | None = db.get(tx_key)
        except redis.exceptions.RedisError:
            continue
        if not tx_raw:
            continue

        tx: CheckoutTransaction = msgpack.decode(tx_raw, type=CheckoutTransaction)
        if tx.state == "COMPLETED":
            continue

        if tx.state == "PREPARING":
            tx.decision = "ABORT"
            tx.state = "DECIDED"
            try:
                db.set(tx_key, msgpack.encode(tx))
            except redis.exceptions.RedisError:
                continue

        if tx.state == "DECIDED":
            finalize_transaction(tx_id, tx)


def process_checkout_async(order_id: str):
    if not try_acquire_checkout_lock(order_id):
        app.logger.info("checkout already in progress for order=%s", order_id)
        return
# protected
    try:
        order_entry = get_order_if_exists(order_id)
        if order_entry is None:
            app.logger.warning("async checkout ignored: order %s not found", order_id)
            return
        if order_entry.paid:
            app.logger.info("async checkout skipped: order %s already paid", order_id)
            return

#create tx
        tx_id = str(uuid.uuid4())
        items = aggregate_items(order_entry.items)
        tx = CheckoutTransaction(
            state="PREPARING",
            decision="UNKNOWN",
            order_id=order_id,
            user_id=order_entry.user_id,
            total_cost=order_entry.total_cost,
            items=items
        )
        #store db
        set_tx(tx_id, tx)

#start 2pc  
        stock_prepare_reply = send_post_request(
            f"{GATEWAY_URL}/stock/prepare/{tx_id}",
            {"items": items}
        )
        payment_prepare_reply = send_post_request(
            f"{GATEWAY_URL}/payment/prepare/{tx_id}",
            {"user_id": order_entry.user_id, "amount": order_entry.total_cost}
        )

        prepare_success = (
            stock_prepare_reply is not None
            and payment_prepare_reply is not None
            and stock_prepare_reply.status_code == 200
            and payment_prepare_reply.status_code == 200
        )

        if prepare_success:
            tx.decision = "COMMIT"
            tx.state = "DECIDED"
            set_tx(tx_id, tx)
            if not finalize_transaction(tx_id, tx):
                app.logger.error("async checkout commit pending recovery tx=%s order=%s", tx_id, order_id)
                return
            app.logger.info("async checkout committed order=%s tx=%s", order_id, tx_id)
            return

        tx.decision = "ABORT"
        tx.state = "DECIDED"
        set_tx(tx_id, tx)
        if not finalize_transaction(tx_id, tx):
            app.logger.error("async checkout abort pending recovery tx=%s order=%s", tx_id, order_id)
            return
        app.logger.info("async checkout aborted order=%s tx=%s", order_id, tx_id)
    finally:
        release_checkout_lock(order_id)


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(paid=False, items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    publish_find_stock(order_id, item_id, quantity)

    return Response(
        f"Stock check requested for item {item_id} in order {order_id} ",
        status=202
    )


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Queueing checkout for {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    if order_entry.paid:
        return Response("Order already paid", status=200)
    if is_checkout_in_progress(order_id):
        return Response("Checkout already in progress", status=202)

    try:
        publish_checkout_requested(order_id)
    except Exception:
        abort(503, "Failed to enqueue checkout")

    return Response("Checkout queued", status=202)


recover_in_doubt_transactions()


if __name__ == '__main__':
    start_consumer()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    start_consumer()
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
