import logging
import os
import atexit
import random
import uuid
import threading
from collections import defaultdict

import redis
import requests
import time
import json

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from msgspec import Struct
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


MAX_RETRIES    = 5
RETRY_TIMEOUT  = 30
RETRY_INTERVAL = 10   # how often the checker wakes up

_consumer_retry_counts: dict[str, int] = {}

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


class OutgoingMessageRecord(Struct):
    idempotency_key: str
    message_type: str
    sent_at: float
    reply_received: bool
    retry_count: int
    order_id: str
    payload: bytes  # serialized message for retry

def log_outgoing_message(idempotency_key: str, message_type: str, order_id: str, message: BaseMessage):
    """Log before sending any message"""
    record = OutgoingMessageRecord(
        idempotency_key=idempotency_key,
        message_type=message_type,
        sent_at=time.time(),
        reply_received=False,
        retry_count=0,
        order_id=order_id,
        payload=msgpack.encode(message)
    )
    key = f"_outgoing:{idempotency_key}"
    db.set(key, msgpack.encode(record))
    db.zadd("_outgoing_pending", {idempotency_key: time.time()})
    app.logger.info(f"Logged outgoing message: {idempotency_key}")

def mark_reply_received(idempotency_key: str):
    """Mark that we received a reply for this message"""
    key = f"_outgoing:{idempotency_key}"
    raw = db.get(key)
    if raw:
        record = msgpack.decode(raw, type=OutgoingMessageRecord)
        record.reply_received = True
        db.set(key, msgpack.encode(record))
        db.zrem("_outgoing_pending", idempotency_key)
        app.logger.info(f"Marked reply received: {idempotency_key}")


# RETRY

def _resend_outgoing_record(record: OutgoingMessageRecord) -> None:
    type_map = {
        "subtract_stock": ("subtract.stock", SubtractStock),
        "payment": ("payment", PaymentRequest),
        "rollback_stock": ("rollback.stock", RollbackStockRequest),
        "rollback_payment": ("rollback.payment", RollbackPaymentRequest),
    }

    entry = type_map.get(record.message_type)
    if not entry:
        app.logger.error("Unknown message_type in outgoing record: %s", record.message_type)
        return

    topic, message_cls = entry
    message = msgpack.decode(record.payload, type=message_cls)

    publish(topic=topic, key=record.order_id, value=message)
    app.logger.info(
        "retried %s idem_key=%s attempt=%d",
        record.message_type, record.idempotency_key, record.retry_count
    )


def check_and_retry_pending_messages() -> None:
    now = time.time()
    cutoff = now - RETRY_TIMEOUT


    try:
        timed_out_keys = db.zrangebyscore("_outgoing_pending", 0, cutoff)
    except redis.exceptions.RedisError as e:
        app.logger.error("Redis error reading _outgoing_pending: %s", e)
        return

    for idem_key in timed_out_keys:
        idem_key_str = idem_key.decode()
        try:
            raw = db.get(f"_outgoing:{idem_key_str}")
            if not raw:
                db.zrem("_outgoing_pending", idem_key_str)
                continue

            record = msgpack.decode(raw, type=OutgoingMessageRecord)
            if record.reply_received:
                db.zrem("_outgoing_pending", idem_key_str)
                continue

            if record.retry_count >= MAX_RETRIES:
                order = get_order_from_db(record.order_id)
                latest = get_latest_log_event(record.order_id)
                if latest not in ("cancelled", "saga_end"):
                    app.logger.error(
                        "Max retries exceeded idem_key=%s — compensating order=%s",
                        record.idempotency_key, record.order_id
                    )
                    compensate_order(order, record.order_id)
                else:
                    app.logger.critical(
                        "MANUAL INTERVENTION REQUIRED — rollback failed after max retries "
                        "idem_key=%s order=%s message_type=%s",
                        record.idempotency_key, record.order_id, record.message_type
                    )
                db.zrem("_outgoing_pending", idem_key_str)
                continue

            record.retry_count += 1
            record.sent_at = now
            db.set(f"_outgoing:{idem_key_str}", msgpack.encode(record))
            db.zadd("_outgoing_pending", {idem_key_str: now})
            _resend_outgoing_record(record)

        except Exception as e:
            app.logger.exception("Error retrying idem_key=%s: %s", idem_key_str, e)



#TODO: part of  PARALLEL. Used to check if an order is stuck between states.
def recover_stuck_orders() -> None:
    app.logger.info("Scanning for stuck orders...")

    # get orders that are started but not finished
    try:
        pending_order_ids = db.zrange("_pending_orders", 0, -1)
    except redis.exceptions.RedisError as e:
        app.logger.error("Redis error reading _pending_orders: %s", e)
        return

    for order_id_bytes in pending_order_ids:
        order_id = order_id_bytes.decode()
        try:
            order = get_order_from_db(order_id)
            latest = get_latest_log_event(order_id)
            if latest in ("saga_started", "stock_pending"):
                # crashed before or just after sending subtract_stock messages
                items_quantities: dict[str, int] = defaultdict(int)
                for item_id, quantity in order.items:
                    items_quantities[item_id] += quantity
                order.expected_items = len(items_quantities)
                order.stock_confirmations = 0
                db.set(order_id, msgpack.encode(order))
                for item_id, quantity in items_quantities.items():
                    idem_key = make_idempotency_key(order_id, "subtract_stock", item_id, quantity)
                    # check if the message was never sent then we need to resend it
                    if not db.get(f"_outgoing:{idem_key}"):
                        app.logger.warning(
                            "Resuming order=%s at stock_pending item=%s", order_id, item_id
                        )
                        publish_subtract_stock(order_id, item_id, quantity)

            elif latest in ("stock_subtracted","payment_pending"):
                # check if outgoing message for payment was sent
                idem_key = make_idempotency_key(order_id, "payment_request", order.user_id, order.total_cost)
                if not db.get(f"_outgoing:{idem_key}"):
                    app.logger.warning("Resuming order=%s at payment_pending", order_id)
                    publish_payment(order_id, order.user_id, order.total_cost)

            elif latest in ("saga_end", "cancelled"):
                mark_order_done(order_id)
            elif latest == "paid":
                append_log(order_id, {"event": "saga_end"})
                mark_order_done(order_id)

            elif latest == "rollback_stock_started":
                # crashed after logging rollback_stock_started but before sending message
                # find which items have no outgoing rollback record
                items_quantities: dict[str, int] = defaultdict(int)
                for item_id, quantity in order.items:
                    items_quantities[item_id] += quantity
                for item_id, quantity in items_quantities.items():
                    idem_key = make_idempotency_key(order_id, "rollback_stock", item_id, quantity)
                    if not db.get(f"_outgoing:{idem_key}"):
                        app.logger.warning(
                            "Resuming order=%s missing rollback_stock item=%s",
                            order_id, item_id
                        )
                        publish_rollback_stock(order_id, item_id, quantity)

            elif latest == "rollback_payment_started":
                # crashed after logging rollback_payment_started but before sending message
                idem_key = make_idempotency_key(order_id, "rollback_payment", order.user_id, order.total_cost)
                if not db.get(f"_outgoing:{idem_key}"):
                    app.logger.warning(
                        "Resuming order=%s missing rollback_payment — no outgoing record",
                        order_id
                    )
                    publish_rollback_payment(order_id, order.user_id, order.total_cost)

        except Exception as e:
            app.logger.exception("Error recovering order=%s: %s", order_id, e)




def recover_on_startup() -> None:
    """
    Called once at startup. Scans all pending outgoing messages and retries
    anything that never got a reply. This handles the case where the service
    crashed mid-saga and needs to resume or compensate.
    """
    app.logger.info("Running startup recovery scan…")
    try:
        check_and_retry_pending_messages()
        recover_stuck_orders()
    except Exception as e:
        app.logger.error("Startup recovery error: %s", e)
    app.logger.info("Startup recovery scan complete.")


def start_retry_checker() -> None:
    """Periodic background thread that retries timed-out outgoing messages."""
    def retry_loop():
        while True:
            try:
                check_and_retry_pending_messages()
            except Exception as e:
                app.logger.error("Retry checker error: %s", e)
            time.sleep(RETRY_INTERVAL)

    thread = threading.Thread(target=retry_loop, daemon=True)
    thread.start()
    app.logger.info(
        "Retry checker started (interval=%ds, timeout=%ds)", RETRY_INTERVAL, RETRY_TIMEOUT
    )

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
    idempotency_key = make_idempotency_key(order_id, "subtract_stock", item_id, quantity)
    message = SubtractStock(
        idempotency_key=idempotency_key,
        order_id=order_id,
        item_id=item_id,
        quantity=int(quantity),
    )

    # log before sending
    log_outgoing_message(idempotency_key, "subtract_stock", order_id, message)

    publish(
        topic="subtract.stock",
        key=message.order_id,
        value=message,
    )

    app.logger.info(f"Sent subtract_stock: {idempotency_key}")



def publish_payment(order_id: str, user_id: str, amount: int):
    idem_key = make_idempotency_key(order_id, "payment_request", user_id, amount)
    message = PaymentRequest(
        idempotency_key=idem_key,
        order_id=order_id,
        user_id=user_id,
        amount=int(amount),
    )
    log_outgoing_message(idem_key, "payment", order_id, message)
    publish(topic="payment", key=order_id, value=message)
    app.logger.info("sent payment idem_key=%s", idem_key)



def publish_rollback_stock(order_id: str, item_id: str, quantity: int):
    idem_key = make_idempotency_key(order_id, "rollback_stock", item_id, quantity)
    message = RollbackStockRequest(
        idempotency_key=idem_key,
        order_id=order_id,
        item_id=item_id,
        quantity=int(quantity),
    )
    log_outgoing_message(idem_key, "rollback_stock", order_id, message)
    publish(topic="rollback.stock", key=order_id, value=message)
    app.logger.info("sent rollback_stock idem_key=%s", idem_key)


def publish_rollback_payment(order_id: str, user_id: str, amount: int):
    idem_key = make_idempotency_key(order_id, "rollback_payment", user_id, amount)
    message = RollbackPaymentRequest(
        idempotency_key=idem_key,
        order_id=order_id,
        user_id=user_id,
        amount=int(amount),
    )
    log_outgoing_message(idem_key, "rollback_payment", order_id, message)
    publish(topic="rollback.payment", key=order_id, value=message)
    app.logger.info("sent rollback_payment idem_key=%s", idem_key)


def handle_find_stock_reply(message: FindStockReply):
    # Mark that we received this reply
    mark_reply_received(message.idempotency_key)

    if should_skip_reply_processing(message.idempotency_key):
        return
    if not message.found:
        store_reply_processing_record(message.idempotency_key, False)
        app.logger.warning(
            f"Stock not found for item {message.item_id} in order {message.order_id}"
        )
        return  # nothing to update


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


def handle_stock_subtracted_reply(message: StockSubtractedReply):
    mark_reply_received(message.idempotency_key)

    if should_skip_reply_processing(message.idempotency_key):
        return

    order_id = message.order_id
    order = get_order_from_db(order_id)
    latest = get_latest_log_event(order_id)

    TERMINAL_EVENTS = ("cancelled", "saga_end", "rollback_stock_started",
                       "rollback_stock_finished", "rollback_payment_started",
                       "rollback_payment_finished")

    if latest in TERMINAL_EVENTS:
        store_reply_processing_record(message.idempotency_key, True)
        return

    if not message.success:
        app.logger.warning("Stock subtraction failed for order %s — cancelling", order_id)
        compensate_order(order, order_id)
        store_reply_processing_record(message.idempotency_key, False)
        return

    order.stock_confirmations += 1

    if order.stock_confirmations == order.expected_items:
        append_log(order_id, {"event": "stock_subtracted"})
        append_log(order_id, {"event": "payment_pending"})
        db.set(order_id, msgpack.encode(order))
        publish_payment(order_id, order.user_id, order.total_cost)

    else:
        db.set(order_id, msgpack.encode(order))

    store_reply_processing_record(message.idempotency_key, True)



def handle_payment_reply(message: PaymentReply):
    mark_reply_received(message.idempotency_key)

    if should_skip_reply_processing(message.idempotency_key):
        return

    order_id = message.order_id
    order = get_order_from_db(order_id)
    latest = get_latest_log_event(order_id)

    if latest not in ("payment_pending", "stock_subtracted"):
        store_reply_processing_record(message.idempotency_key, True)
        return


    if message.success:
        append_log(order_id, {"event": "paid"})
        append_log(order_id, {"event": "saga_end"})
        db.set(order_id, msgpack.encode(order))
        mark_order_done(order_id)
        store_reply_processing_record(message.idempotency_key, True)
        app.logger.info("Order completed: %s", order_id)
    else:
        app.logger.info("Payment failed for order %s — rolling back stock", order_id)
        compensate_order(order, order_id)
        store_reply_processing_record(message.idempotency_key, False)


def handle_rollback_stock_reply(message: RollbackStockReply):
    mark_reply_received(message.idempotency_key)

    if should_skip_reply_processing(message.idempotency_key):
        return

    order = get_order_from_db(message.order_id)
    order.rollback_stock_confirmations += 1
    db.set(message.order_id, msgpack.encode(order))

    store_reply_processing_record(message.idempotency_key, message.success)

    if order.rollback_stock_confirmations == order.expected_items:
        append_log(message.order_id, {
            "event": "rollback_stock_finished",
            "success": message.success,
        })

def handle_rollback_payment_reply(message: RollbackPaymentReply):
    mark_reply_received(message.idempotency_key)

    if should_skip_reply_processing(message.idempotency_key):
        return
    append_log(message.order_id, {
        "event": "rollback_payment_finished",
        "success": message.success,
    })
    store_reply_processing_record(message.idempotency_key, message.success)
    app.logger.info("rollback_payment_finished order=%s success=%s",
                    message.order_id, message.success)

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

        key = msg.key().decode() if msg.key() else ""
        try:
            message = decode_message(msg.value())
            app.logger.info("consumed topic=%s key=%s type=%s", msg.topic(), key, message.type)
            handle_message(message)
            consumer.commit(message=msg)
            _consumer_retry_counts.pop(key, None)

        except Exception as e:

            _consumer_retry_counts[key] = _consumer_retry_counts.get(key, 0) + 1
            attempt = _consumer_retry_counts[key]
            app.logger.exception(
                "Processing error (attempt %d/%d) key=%s: %s", attempt, MAX_RETRIES, key, e
            )
            if attempt >= MAX_RETRIES:
                app.logger.error("Max consumer retries reached for key=%s — skipping", key)
                consumer.commit(message=msg)
                _consumer_retry_counts.pop(key, None)
            else:
                time.sleep(min(2 ** attempt, 30))  # exponential back-off



#TODO: for PARAlLEL update this to track payment and stock
class OrderValue(Struct):
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    expected_items: int
    stock_confirmations: int
    rollback_stock_confirmations: int

# ROLL BACK
#TODO: change for PARALLEL
def compensate_order(order: OrderValue, order_id: str) -> None:

    latest = get_latest_log_event(order_id)

    if latest in ("cancelled", "saga_end"):
        app.logger.warning("compensate_order called on already finished order=%s", order_id)
        return

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order.items:
        items_quantities[item_id] += quantity

    # we need to rollback stock
    if latest in ("stock_pending", "stock_subtracted", "payment_pending"):
        append_log(order_id, {"event": "rollback_stock_started"})
        order.rollback_stock_confirmations = 0
        order.expected_items = len(items_quantities)
        db.set(order_id, msgpack.encode(order))

        for item_id, quantity in items_quantities.items():
            publish_rollback_stock(order_id, item_id, quantity)

    # need to rollback payment and stock
    elif latest == "paid":
        append_log(order_id, {"event": "rollback_stock_started"})
        order.rollback_stock_confirmations = 0
        order.expected_items = len(items_quantities)
        db.set(order_id, msgpack.encode(order))

        for item_id, quantity in items_quantities.items():
            publish_rollback_stock(order_id, item_id, quantity)
        append_log(order_id, {"event": "rollback_payment_started"})
        publish_rollback_payment(order_id, order.user_id, order.total_cost)


    append_log(order_id, {"event": "cancelled"})
    db.set(order_id, msgpack.encode(order))
    mark_order_done(order_id)

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
    value = msgpack.encode(OrderValue(items=[], user_id=user_id, total_cost=0,expected_items=0,
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
        value = OrderValue(
            items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
            user_id=f"{user_id}",
            total_cost=2 * item_price,
            expected_items=2,
            stock_confirmations=0,
        )
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
    latest = get_latest_log_event(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "status": latest or "unknown",
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


# For logging
def append_log(order_id: str, entry: dict):
    key = f"_log:{order_id}"
    raw = db.get(key)
    logs = json.loads(raw) if raw else []
    logs.append({**entry, "timestamp": time.time()})
    db.set(key, json.dumps(logs))

def get_latest_log_event(order_id: str) -> str | None:
    key = f"_log:{order_id}"
    raw = db.get(key)
    if not raw:
        return None
    logs = json.loads(raw)
    return logs[-1]["event"] if logs else None

def log_saga_start(order_id: str):
    append_log(order_id, {"event": "saga_started"})
    db.zadd("_pending_orders", {order_id: time.time()})

def mark_order_done(order_id: str):
    db.zrem("_pending_orders", order_id)


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    log_saga_start(order_id)
    app.logger.debug(f"Checking out {order_id}")

    order_entry: OrderValue = get_order_from_db(order_id)

    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    # Update order state
    order_entry.expected_items = len(items_quantities)
    order_entry.stock_confirmations = 0
    append_log(order_id, {"event": "stock_pending"})
    db.set(order_id, msgpack.encode(order_entry))

    for item_id, quantity in items_quantities.items():
        publish_subtract_stock(order_id, item_id, quantity)
    return Response("Checkout stock subtraction requested asynchronously", status=202)




if __name__ == '__main__':
    if os.getenv("DISABLE_KAFKA_CONSUMER") != "1":
        recover_on_startup()
        start_consumer()
        start_retry_checker()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    if os.getenv("DISABLE_KAFKA_CONSUMER") != "1":
        recover_on_startup()
        start_consumer()
        start_retry_checker()
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
