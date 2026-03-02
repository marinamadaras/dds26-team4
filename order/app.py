import logging
import os
import atexit
import random
import uuid
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

ORDER_CREATED = "CREATED"
STOCK_PENDING = "STOCK_PENDING"
STOCK_RESERVED = "STOCK_RESERVED"
PAYMENT_PENDING = "PAYMENT_PENDING"
ORDER_COMPLETED = "COMPLETED"
ORDER_CANCELLED = "CANCELLED"


def close_db_connection():
    db.close()


class ReplyProcessingRecord(Struct):
    success: bool


def get_reply_processing_record(idempotency_key: str) -> ReplyProcessingRecord | None:
    if not idempotency_key:
        return None
    record_key = f"_idem:{idempotency_key}"
    try:
        raw = db.get(record_key)
    except redis.exceptions.RedisError as e:
        raise RuntimeError(DB_ERROR_STR) from e
    return msgpack.decode(raw, type=ReplyProcessingRecord) if raw else None


def store_reply_processing_record(idempotency_key: str, success: bool):
    if not idempotency_key:
        return
    record_key = f"_idem:{idempotency_key}"
    record = ReplyProcessingRecord(success=success)
    try:
        db.set(record_key, msgpack.encode(record))
    except redis.exceptions.RedisError as e:
        raise RuntimeError(DB_ERROR_STR) from e


def should_skip_reply_processing(idempotency_key: str) -> bool:
    cached = get_reply_processing_record(idempotency_key)
    return bool(cached is not None and cached.success) # should only ignore a reply if it was successfull


def make_idempotency_key(order_id: str, action: str, item_id: str = "", quantity: int = 0) -> str:
    return f"order:{order_id}:{action}:{item_id}:{int(quantity)}"

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
    message = FindStock(
        idempotency_key=make_idempotency_key(order_id, "find_stock", item_id, quantity),
        item_id=item_id,
        quantity=int(quantity),
    )

    publish(
        topic="find.stock",
        key=order_id,
        value=message,
    )


def publish_subtract_stock(order_id: str, item_id: str, quantity: int):
    message = SubtractStock(
        idempotency_key=make_idempotency_key(order_id, "subtract_stock", item_id, quantity),
        order_id=order_id,
        item_id=item_id,
        quantity=int(quantity),
    )
    publish(
        topic="subtract.stock",
        key=message.order_id,
        value=message,
    )


def publish_payment(order_id: str, user_id: str, amount: int):
    message = PaymentRequest(
        idempotency_key=make_idempotency_key(order_id, "payment_request", user_id, amount),
        order_id=order_id,
        user_id=user_id,
        amount=int(amount),
    )
    publish(
        topic="payment",
        key=order_id,
        value=message,
    )


def publish_rollback_stock(order_id: str, item_id: str, quantity: int):
    message = RollbackStockRequest(
        idempotency_key=make_idempotency_key(order_id, "rollback_stock", item_id, quantity),
        order_id=order_id,
        item_id=item_id,
        quantity=int(quantity),
    )
    publish(
        topic="rollback.stock",
        key=order_id,
        value=message,
    )


def publish_rollback_payment(order_id: str, user_id: str, amount: int):
    message = RollbackPaymentRequest(
        idempotency_key=make_idempotency_key(order_id, "rollback_payment", user_id, amount),
        order_id=order_id,
        user_id=user_id,
        amount=int(amount),
    )
    publish(
        topic="rollback.payment",
        key=order_id,
        value=message,
    )


def handle_find_stock_reply(message: FindStockReply):
    app.logger.info(f"received find stock reply: {message.order_id}")
    if should_skip_reply_processing(message.idempotency_key):
        return
    if not message.found:
        store_reply_processing_record(message.idempotency_key, False)
        app.logger.warning(
            f"Stock not found for item {message.item_id} in order {message.order_id}"
        )
        return  # nothing to update
        # Shouldn't we throw error or something

    try:
        order_entry: OrderValue = get_order_from_db(message.order_id)

        order_entry.items.append(
            (message.item_id, int(message.quantity))
        )

        order_entry.total_cost += (
            int(message.quantity) * message.price
        )

        db.set(message.order_id, msgpack.encode(order_entry))
        store_reply_processing_record(message.idempotency_key, True)

        app.logger.info(
            f"Order {message.order_id} updated asynchronously"
        )

    except redis.exceptions.RedisError as e:
        app.logger.error(f"DB error: {e}")
        raise  # todo: let consumer retry?



def handle_stock_subtracted_reply(message: StockSubtractedReply):
    if should_skip_reply_processing(message.idempotency_key):
        return
    order_id = message.order_id
    order = get_order_from_db(order_id)

    if order.status != STOCK_PENDING:
        store_reply_processing_record(message.idempotency_key, True) # this reply is no longer needed so store it as success
        return

    if not message.success:
        order.status = ORDER_CANCELLED
        db.set(order_id, msgpack.encode(order))
        store_reply_processing_record(message.idempotency_key, False)
        return

    order.stock_confirmations += 1

    if order.stock_confirmations == order.expected_items:
        order.status = PAYMENT_PENDING
        db.set(order_id, msgpack.encode(order))

        publish_payment(
            order_id,
            order.user_id,
            order.total_cost
        )
        store_reply_processing_record(message.idempotency_key, True)
    else:
        db.set(order_id, msgpack.encode(order))
        store_reply_processing_record(message.idempotency_key, True)



def handle_payment_reply(message: PaymentReply):
    app.logger.info("payment reply received order_id=%s success=%s",
                    message.order_id, message.success)
    if should_skip_reply_processing(message.idempotency_key):
        return

    order_id = message.order_id

    order = get_order_from_db(order_id)

    if order.status != PAYMENT_PENDING:
        store_reply_processing_record(message.idempotency_key, True)
        return

    if message.success:
        try:
            order.status = ORDER_COMPLETED
            db.set(order_id, msgpack.encode(order))
            store_reply_processing_record(message.idempotency_key, True)
        except Exception as e:
            app.logger.error("DB failure after payment → refunding : %s", e)

            # Refund payment
            publish_rollback_payment(
                message.order_id,
                order.user_id,
                order.total_cost
            )

            # Rollback stock
            for item_id, quantity in order.items:
                publish_rollback_stock(message.order_id, item_id, quantity)

            abort(400, DB_ERROR_STR)

        app.logger.info("Order completed: %s", order_id)

    else:
        order.status = ORDER_CANCELLED
        db.set(order_id, msgpack.encode(order))
        store_reply_processing_record(message.idempotency_key, False)

        for item_id, quantity in order.items:
            publish_rollback_stock(order_id, item_id, quantity)

        app.logger.info("Payment failed, stock rollback started")


def handle_rollback_stock_reply(message: RollbackStockReply):
    if should_skip_reply_processing(message.idempotency_key):
        return
    store_reply_processing_record(message.idempotency_key, message.success)
    app.logger.info(
        "rollback.stock.replies received order_id=%s item_id=%s success=%s",
        message.order_id,
        message.item_id,
        message.success,
    )

def handle_rollback_payment_reply(message: RollbackPaymentReply):
    if should_skip_reply_processing(message.idempotency_key):
        return
    store_reply_processing_record(message.idempotency_key, message.success)
    app.logger.info(
        "rollback.payment.replies received order_id=%s user_id=%s success=%s",
        message.order_id,
        message.user_id,
        message.success,
    )


def handle_message(message: BaseMessage):
    if isinstance(message, FindStockReply):
        handle_find_stock_reply(message)
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
    status: str
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    expected_items: int
    stock_confirmations: int


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


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(status=ORDER_CREATED, items=[], user_id=user_id, total_cost=0,expected_items=0,
            stock_confirmations=0))
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
            "status": order_entry.status,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):

    publish_find_stock(order_id, item_id, quantity)

    return Response(
        f"Stock check requested for item {item_id} in order {order_id} ",
        status=202
    )


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)

    # Check if it was already processed
    if order_entry.status != ORDER_CREATED:
        return Response(f"Order already processed: {order_entry.status}", status=200)

    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    # Update order state
    order_entry.expected_items = len(items_quantities)
    order_entry.stock_confirmations = 0
    order_entry.status = STOCK_PENDING
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    for item_id, quantity in items_quantities.items():
        publish_subtract_stock(order_id, item_id, quantity)
    return Response("Checkout stock subtraction requested asynchronously", status=202)




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
