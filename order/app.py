import logging
import os
import atexit
import random
import uuid
import threading
from collections import defaultdict

import msgspec
import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from kafka_client import publish, publish_raw, create_consumer, decode_message
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
KAFKA_CONSUMER_PARTITION = int(os.getenv("KAFKA_CONSUMER_PARTITION", "0"))
KAFKA_CONSUMER_INSTANCE_ID = os.getenv("KAFKA_CONSUMER_INSTANCE_ID", "order-service-0")

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
    app.logger.info(
        "stock subtracted not yet implemented order_id=%s",
        message.order_id,
    )


def handle_payment_reply(message: PaymentReply):
    # todo: implement with sagas, we need to keep track of all pending checkouts somehow
    app.logger.info(
        "payment reply handling not yet implemented order_id=%s",
        message.order_id,
    )


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


def handle_http_command(command: dict):
    # the messages from the api-gateway is handled here, to reuse the http logic, it makes a local http request but the actual communication is async
    request_id = str(command.get("request_id", ""))
    method = str(command.get("method", "GET")).upper()
    action = str(command.get("action", "")).lstrip("/")
    if not request_id or not action:
        return

    url = f"http://127.0.0.1:5000/{action}"
    try:
        if method == "POST":
            response = requests.post(url, timeout=10)
        else:
            response = requests.get(url, timeout=10)
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            body = response.json()
        else:
            body = response.text
        status_code = response.status_code
    except Exception as e:
        status_code = 500
        body = {"error": str(e)}

    publish_raw(
        topic="gateway.order.replies",
        key=request_id,
        payload={
            "request_id": request_id,
            "status_code": status_code,
            "body": body,
        },
        partition=KAFKA_CONSUMER_PARTITION,
    )


def consumer_loop():
    app.logger.info(
        "order consumer loop starting partition=%s instance_id=%s",
        KAFKA_CONSUMER_PARTITION,
        KAFKA_CONSUMER_INSTANCE_ID,
    )
    consumer = create_consumer(
        group_id="order-service",
        topics=[
            "find.stock.replies",
            "subtract.stock.replies",
            "payment.replies",
            "rollback.stock.replies",
            "rollback.payment.replies",
            "gateway.order.commands",
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
            error_str = str(msg.error())
            if "NOT_COORDINATOR" in error_str:
                app.logger.warning("Kafka coordinator not ready yet: %s", msg.error())
            else:
                app.logger.error("Kafka error: %s", msg.error())
            continue

        try:
            if msg.topic() == "gateway.order.commands":
                command = msgspec.json.decode(msg.value(), type=dict)
                handle_http_command(command)
                consumer.commit(message=msg)
                continue

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
    key = f"s{KAFKA_CONSUMER_PARTITION}_{uuid.uuid4()}"
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


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    # removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        publish_subtract_stock(order_id, item_id, quantity)
    return Response("Checkout stock subtraction requested asynchronously", status=202)

    #     stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
    #     if stock_reply.status_code != 200:
    #         # If one item does not have enough stock we need to rollback
    #         rollback_stock(removed_items)
    #         abort(400, f'Out of stock on item_id: {item_id}')
    #     removed_items.append((item_id, quantity))
    # user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    # if user_reply.status_code != 200:
    #     # If the user does not have enough credit we need to rollback all the item stock subtractions
    #     rollback_stock(removed_items)
    #     abort(400, "User out of credit")
    # order_entry.paid = True
    # try:
    #     db.set(order_id, msgpack.encode(order_entry))
    # except redis.exceptions.RedisError:
    #     return abort(400, DB_ERROR_STR)
    # app.logger.debug("Checkout successful")
    # return Response("Checkout successful", status=200)


if __name__ == '__main__':
    start_consumer()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    start_consumer()
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
