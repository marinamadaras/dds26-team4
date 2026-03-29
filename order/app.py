import logging
import os
import atexit
import random
import re
import uuid
import threading
import zlib
from collections import defaultdict

import msgspec
import redis
import requests
import time
import json

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

from msgspec import Struct
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
    Ack,
    Failure

)

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']
KAFKA_CONSUMER_PARTITION = int(os.getenv("KAFKA_CONSUMER_PARTITION", "0"))
KAFKA_CONSUMER_INSTANCE_ID = os.getenv("KAFKA_CONSUMER_INSTANCE_ID", "order-service-0")
ORCHESTRATOR_PARTITIONS = int(os.getenv("ORCHESTRATOR_PARTITIONS", "3"))

# All outgoing Kafka messages are routed through the orchestrator.
ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://orchestrator-service:5000")

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



def _submit_kafka_task(topic: str, idempotency_key: str, key: str, message: BaseMessage, partition: int) -> None:
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
    app.logger.info("Submitting Kafka task: %s -> %s", topic, idempotency_key)
    publish(
        "orchestrator.requests",
        idempotency_key,
        task,
        partition=_orchestrator_partition_for_idempotency_key(idempotency_key),
    )


def _orchestrator_partition_for_idempotency_key(idempotency_key: str) -> int:
    return zlib.crc32(idempotency_key.encode()) % ORCHESTRATOR_PARTITIONS



def _partition_from_order_id(order_id: str) -> int:
    match = re.match(r"^s(\d+)_", order_id)
    if match:
        return int(match.group(1))
    return KAFKA_CONSUMER_PARTITION


def _partition_from_item_id(item_id: str) -> int:
    match = re.match(r"^t(\d+)_", item_id)
    if match:
        return int(match.group(1))
    return KAFKA_CONSUMER_PARTITION


def _partition_from_user_id(user_id: str) -> int:
    match = re.match(r"^p(\d+)_", user_id)
    if match:
        return int(match.group(1))
    return KAFKA_CONSUMER_PARTITION


def _stock_service_url(item_id: str, path: str) -> str:
    partition = _partition_from_item_id(item_id)
    return f"http://stock-service-{partition}:5000/{path.lstrip('/')}"

def close_db_connection():
    db.close()


class ReplyProcessingRecord(Struct):
    success: bool


class OrderValue(Struct):
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    expected_items: int
    stock_confirmations: int
    rollback_stock_confirmations: int
    # These fields are the minimal state needed to coordinate the payment
    # branch in parallel with stock while keeping state aligned with log events.
    payment_reply_received: bool = False
    payment_success: bool = False
    checkout_finalized: bool = False

class OutgoingMessageRecord(Struct):
    idempotency_key: str
    message_type: str
    sent_at: float
    reply_received: bool
    retry_count: int
    order_id: str
    payload: bytes  # serialized message for retry

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

def mark_ack_received(idempotency_key: str):
    """Mark that we received an ack for this message"""
    app.logger.info("Received ACK for: %s", idempotency_key)
    key = f"_outgoing:{idempotency_key}"
    raw = db.get(key)
    if raw:
        record = msgpack.decode(raw, type=OutgoingMessageRecord)
        record.reply_received = True
        db.set(key, msgpack.encode(record))
        db.zrem("_outgoing_pending", idempotency_key)

# RETRY

def publish_missing_messages(
    order_id: str,
    order: OrderValue,
    action: str,  # "subtract_stock" or "rollback_stock"
    topic_fn,     # publish_subtract_stock or publish_rollback_stock
) -> None:
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order.items:
        items_quantities[item_id] += quantity
    for item_id, quantity in items_quantities.items():
        idem_key = make_idempotency_key(order_id, action, item_id, quantity)
        if not db.get(f"_outgoing:{idem_key}"):
            topic_fn(order_id, item_id, quantity)

def _resend_outgoing_record(record: OutgoingMessageRecord) -> None:
    type_map = {
        "subtract_stock": ("subtract.stock", SubtractStock),
        "payment": ("payment", PaymentRequest),
        "rollback_stock": ("rollback.stock", RollbackStockRequest),
        "rollback_payment": ("rollback.payment", RollbackPaymentRequest),
    }

    entry = type_map.get(record.message_type)
    if not entry:
        return

    topic, message_cls = entry
    message = msgpack.decode(record.payload, type=message_cls)

    payload = {f: getattr(message, f) for f in message.__struct_fields__}
    payload["type"] = message_cls.__name__

    _submit_kafka_task(topic=topic, idempotency_key= record.idempotency_key,
                       key=record.order_id, message=message,
                       partition=_partition_from_order_id(record.order_id))

# for when you have sent  a message but did not receive and answer
def check_and_retry_pending_messages() -> None:
    now = time.time()
    cutoff = now - RETRY_TIMEOUT

    try:
        timed_out_keys = db.zrangebyscore("_outgoing_pending", 0, cutoff)
    except redis.exceptions.RedisError as e:
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
                    compensate_order( record.order_id)

                db.zrem("_outgoing_pending", idem_key_str)
                continue

            record.retry_count += 1
            record.sent_at = now
            db.set(f"_outgoing:{idem_key_str}", msgpack.encode(record))
            db.zadd("_outgoing_pending", {idem_key_str: now})
            _resend_outgoing_record(record)

        except Exception as e:
            pass



# Used to check if an order is stuck between states.
def recover_stuck_orders() -> None:

    # get orders that are started but not finished
    try:
        pending_order_ids = db.zrange("_pending_orders", 0, -1)
    except redis.exceptions.RedisError as e:
        return

    for order_id_bytes in pending_order_ids:
        order_id = order_id_bytes.decode()
        try:
            order = get_order_from_db(order_id)
            events = get_log_events(order_id)
            event_set = set(events)
            sync_order_state_from_events(order, event_set)
            db.set(order_id, msgpack.encode(order))

            if "saga_end" in event_set:
                order.checkout_finalized = True
                db.set(order_id, msgpack.encode(order))
                mark_order_done(order_id)
                continue

            if "cancelled" in event_set: # it might be cancelled but we are not sure
                if "rollback_stock_started" in event_set and "rollback_stock_finished" not in event_set:
                    publish_missing_messages(order_id, order, "rollback_stock", publish_rollback_stock)

                if "rollback_payment_started" in event_set and "rollback_payment_finished" not in event_set:
                    # crashed after logging rollback_payment_started but before sending message
                    idem_key = make_idempotency_key(order_id, "rollback_payment", order.user_id, order.total_cost)
                    if not db.get(f"_outgoing:{idem_key}"):
                        publish_rollback_payment(order_id, order.user_id, order.total_cost)

                maybe_finish_cancelled_order(order_id, order)
                continue

            stock_pending = "stock_pending" in event_set and "stock_subtracted" not in event_set
            payment_pending = "payment_pending" in event_set and "payment_confirmed" not in event_set \
                and "payment_failed" not in event_set and "paid" not in event_set

            if stock_pending or payment_pending:
                retry_stock_and_payment(
                    order,
                    order_id,
                    recover_stock=stock_pending,
                    recover_payment=payment_pending,
                )

            if "rollback_stock_started" in event_set and "rollback_stock_finished" not in event_set:
                # crashed after logging rollback_stock_started but before sending message
                # find which items have no outgoing rollback record
                publish_missing_messages(order_id, order, "rollback_stock", publish_rollback_stock)

            if "rollback_payment_started" in event_set and "rollback_payment_finished" not in event_set:
                # crashed after logging rollback_payment_started but before sending message
                idem_key = make_idempotency_key(order_id, "rollback_payment", order.user_id, order.total_cost)
                if not db.get(f"_outgoing:{idem_key}"):
                    publish_rollback_payment(order_id, order.user_id, order.total_cost)

        except Exception as e:
            pass


def retry_stock_and_payment(
    order: OrderValue,
    order_id: str,
    recover_stock: bool = True,
    recover_payment: bool = True,
):
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order.items:
        items_quantities[item_id] += quantity
    order.expected_items = len(items_quantities)
    db.set(order_id, msgpack.encode(order))
    if recover_stock:
        publish_missing_messages(order_id, order, "subtract_stock", publish_subtract_stock)

    if recover_payment:
        idem_key = make_idempotency_key(order_id, "payment_request", order.user_id, order.total_cost)
        if not db.get(f"_outgoing:{idem_key}"):
            publish_payment(order_id, order.user_id, order.total_cost)


def sync_order_state_from_events(order: OrderValue, event_set: set[str]) -> None:
    # Recovery can observe log events that were appended just before a crash,
    # while the corresponding OrderValue update was not yet persisted. Rebuild
    # the payment branch state from those events before deciding what to resume.
    if "payment_confirmed" in event_set or "paid" in event_set:
        order.payment_reply_received = True
        order.payment_success = True
    elif "payment_failed" in event_set:
        order.payment_reply_received = True
        order.payment_success = False

    if "saga_end" in event_set or "cancelled" in event_set:
        order.checkout_finalized = True


def maybe_finish_cancelled_order(order_id: str, order: OrderValue) -> None:
    events = set(get_log_events(order_id))

    if "cancelled" not in events:
        return

    stock_rollback_done = "rollback_stock_finished" in events
    payment_rollback_needed = "rollback_payment_started" in events
    payment_rollback_done = "rollback_payment_finished" in events

    # just uodate if we are waiting on some rollbacks or not
    if stock_rollback_done and (not payment_rollback_needed or payment_rollback_done):
        order.checkout_finalized = True
        db.set(order_id, msgpack.encode(order))
        mark_order_done(order_id)


def recover_on_startup() -> None:
    """
    Called once at startup. Scans all pending outgoing messages and retries
    anything that never got a reply. This handles the case where the service
    crashed mid-saga and needs to resume or compensate.
    """
    try:
        check_and_retry_pending_messages()
        recover_stuck_orders()
    except Exception as e:
        pass



def start_retry_checker() -> None:
    """Periodic background thread that retries timed-out outgoing messages."""
    def retry_loop():
        while True:
            try:
                check_and_retry_pending_messages()
            except Exception as e:
                pass
            time.sleep(RETRY_INTERVAL)

    thread = threading.Thread(target=retry_loop, daemon=True)
    thread.start()


def start_consumer():
    global _consumer_thread
    if _consumer_thread is not None and _consumer_thread.is_alive():
        return
    thread = threading.Thread(target=consumer_loop, daemon=True)
    thread.start()
    _consumer_thread = thread

atexit.register(close_db_connection)

def publish_find_stock(order_id: str, item_id: str, quantity: int):

    idempotency_key = make_idempotency_key(order_id, "find_stock", item_id, quantity)
    message = FindStock(
        type = "FindStock",
        idempotency_key=idempotency_key,
        order_id=order_id,
        item_id=item_id,
        quantity=int(quantity),
    )
    _submit_kafka_task(
        topic="find.stock",
        idempotency_key = idempotency_key,
        key = item_id,
        message=message,
        partition=_partition_from_item_id(item_id),
    )


def publish_subtract_stock(order_id: str, item_id: str, quantity: int):
    idempotency_key = make_idempotency_key(order_id, "subtract_stock", item_id, quantity)
    message = SubtractStock(
        type="SubtractStock",
        idempotency_key=idempotency_key,
        order_id=order_id,
        item_id=item_id,
        quantity=int(quantity),
    )

    # log before sending
    log_outgoing_message(idempotency_key, "subtract_stock", order_id, message)

    _submit_kafka_task(
        topic="subtract.stock",
        idempotency_key=idempotency_key,
        key=item_id,
        message=message,
        partition=_partition_from_item_id(item_id),
    )


def publish_payment(order_id: str, user_id: str, amount: int):
    idem_key = make_idempotency_key(order_id, "payment_request", user_id, amount)
    message = PaymentRequest(
        type = "PaymentRequest",
        idempotency_key=idem_key,
        order_id=order_id,
        user_id=user_id,
        amount=int(amount),
    )
    log_outgoing_message(idem_key, "payment", order_id, message)

    _submit_kafka_task(
        topic="payment",
        idempotency_key=idem_key,
        key=user_id,
        message= message,
        partition=_partition_from_user_id(user_id),
    )



def publish_rollback_stock(order_id: str, item_id: str, quantity: int):
    idem_key = make_idempotency_key(order_id, "rollback_stock", item_id, quantity)
    message = RollbackStockRequest(
        type = "RollbackStockRequest",
        idempotency_key=idem_key,
        order_id=order_id,
        item_id=item_id,
        quantity=int(quantity),
    )
    log_outgoing_message(idem_key, "rollback_stock", order_id, message)
    _submit_kafka_task(
        topic="rollback.stock",
        idempotency_key=idem_key,
        key=item_id,
        message=message,
        partition=_partition_from_item_id(item_id),
    )


def publish_rollback_payment(order_id: str, user_id: str, amount: int):
    idem_key = make_idempotency_key(order_id, "rollback_payment", user_id, amount)
    message = RollbackPaymentRequest(
        type = "RollbackPaymentRequest",
        idempotency_key=idem_key,
        order_id=order_id,
        user_id=user_id,
        amount=int(amount),
    )
    log_outgoing_message(idem_key, "rollback_payment", order_id, message)
    _submit_kafka_task(
        topic="rollback.payment",
        idempotency_key=idem_key,
        key=user_id,
        message=message,
        partition=_partition_from_user_id(user_id),
    )


def maybe_commit_checkout(order_id: str, order: OrderValue) -> None:
    # This method is the single success gate for parallel checkout. It commits
    # only after both branches have succeeded, so neither reply handler needs
    # to guess whether the other branch is already done.
    if order.checkout_finalized:
        db.set(order_id, msgpack.encode(order))
        return

    if order.stock_confirmations < order.expected_items:
        db.set(order_id, msgpack.encode(order))
        return

    if not order.payment_reply_received:
        db.set(order_id, msgpack.encode(order))
        return

    if not order.payment_success:
        db.set(order_id, msgpack.encode(order))
        return

    order.checkout_finalized = True
    append_log(order_id, {"event": "paid"})
    append_log(order_id, {"event": "saga_end"})
    db.set(order_id, msgpack.encode(order))
    mark_order_done(order_id)


def handle_find_stock_reply(message: FindStockReply):
    # Mark that we received this reply
    # mark_reply_received(message.idempotency_key)

    if should_skip_reply_processing(message.idempotency_key):
        return
    if not message.found:
        store_reply_processing_record(message.idempotency_key, False)
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


def handle_stock_subtracted_reply(message: StockSubtractedReply):
    # mark_reply_received(message.idempotency_key)

    if should_skip_reply_processing(message.idempotency_key):
        return

    order_id = message.order_id
    order = get_order_from_db(order_id)
    if order.checkout_finalized: # we do not need to do anything if the order is already done
        events = set(get_log_events(order_id))
        # A stock success can arrive after cancellation because stock and
        # payment are now in flight together. In that case we must trigger the
        # rollback for this item now instead of silently ignoring it.
        if message.success and "cancelled" in events:
            publish_rollback_stock(order_id, message.item_id, message.quantity)
        store_reply_processing_record(message.idempotency_key, True)
        return

    if not message.success:
        compensate_order(order_id)
        store_reply_processing_record(message.idempotency_key, False)
        return

    order.stock_confirmations += 1

    if order.stock_confirmations == order.expected_items:
        append_log(order_id, {"event": "stock_subtracted"})
    maybe_commit_checkout(order_id, order)

    store_reply_processing_record(message.idempotency_key, True)



def handle_payment_reply(message: PaymentReply):
    # mark_reply_received(message.idempotency_key)

    if should_skip_reply_processing(message.idempotency_key):
        return

    order_id = message.order_id
    order = get_order_from_db(order_id)
    if order.checkout_finalized:
        events = set(get_log_events(order_id))
        # A payment success can arrive after cancellation if stock failed
        # first. Refund it immediately so the user is not left charged.
        if message.success and "cancelled" in events:
            order.payment_reply_received = True
            order.payment_success = True
            db.set(order_id, msgpack.encode(order))
            publish_rollback_payment(order_id, order.user_id, order.total_cost)
        store_reply_processing_record(message.idempotency_key, True)
        return


    if message.success:
        order.payment_reply_received = True
        order.payment_success = True
        # Keep the payment-branch event separate from final commit so the log
        # reflects that payment succeeded even while stock may still be pending.
        append_log(order_id, {"event": "payment_confirmed"})
        maybe_commit_checkout(order_id, order)
        store_reply_processing_record(message.idempotency_key, True)
    else:
        order.payment_reply_received = True
        order.payment_success = False
        append_log(order_id, {"event": "payment_failed"})
        db.set(order_id, msgpack.encode(order))
        compensate_order(order_id)
        store_reply_processing_record(message.idempotency_key, False)


def handle_rollback_stock_reply(message: RollbackStockReply):
    # mark_reply_received(message.idempotency_key)

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
        maybe_finish_cancelled_order(message.order_id, order)

def handle_rollback_payment_reply(message: RollbackPaymentReply):
    # mark_reply_received(message.idempotency_key)

    if should_skip_reply_processing(message.idempotency_key):
        return
    append_log(message.order_id, {
        "event": "rollback_payment_finished",
        "success": message.success,
    })
    store_reply_processing_record(message.idempotency_key, message.success)
    order = get_order_from_db(message.order_id)
    maybe_finish_cancelled_order(message.order_id, order)

def handle_message(message: BaseMessage):
    if isinstance(message, Ack):
        mark_ack_received(message.idempotency_key)
        return

    if isinstance(message, Failure):
        compensate_order(message.order_id)
        return
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


def handle_http_command(command: dict):
    # Handle gateway Kafka commands by calling existing local HTTP routes.
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
    consumer = create_consumer(
        group_id="order-service",
        topics=[
            "find.stock.replies",
            "subtract.stock.replies",
            "payment.replies",
            "rollback.stock.replies",
            "rollback.payment.replies",
            "gateway.order.commands",
            "orchestrator.feedback",
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
            continue

        key = msg.key().decode() if msg.key() else ""

        try:
            app.logger.info("Processing Kafka message: %s topic=%s", key, msg.topic())
            if msg.topic() == "gateway.order.commands":
                command = msgspec.json.decode(msg.value(), type=dict)
                handle_http_command(command)
                consumer.commit(message=msg)
                continue

            message = decode_message(msg.value())
            handle_message(message)
            consumer.commit(message=msg)
            _consumer_retry_counts.pop(key, None)

        except Exception as e:
            app.logger.exception("Processing error in order consumer: %s", e)

            _consumer_retry_counts[key] = _consumer_retry_counts.get(key, 0) + 1
            attempt = _consumer_retry_counts[key]
            if attempt >= MAX_RETRIES:
                consumer.commit(message=msg)
                _consumer_retry_counts.pop(key, None)
            else:
                time.sleep(min(2 ** attempt, 30))  # exponential back-off




def compensate_order(order_id: str) -> None:
    order = get_order_from_db(order_id)
    events = set(get_log_events(order_id))

    if order.checkout_finalized or "cancelled" in events or "saga_end" in events:
        return

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order.items:
        items_quantities[item_id] += quantity

    # In the parallel flow we always start stock rollback on cancellation. The
    # stock service skips items that were never subtracted, and any late stock
    # success will trigger another rollback message from the handler above.
    if "rollback_stock_started" not in events:
        append_log(order_id, {"event": "rollback_stock_started"})
        order.rollback_stock_confirmations = 0
        order.expected_items = len(items_quantities)
        db.set(order_id, msgpack.encode(order))

        for item_id, quantity in items_quantities.items():
            publish_rollback_stock(order_id, item_id, quantity)

    # Payment rollback is only needed once payment has actually succeeded. If
    # payment succeeds after cancellation, the payment handler above publishes
    # the compensating refund when that late reply arrives.
    if order.payment_reply_received and order.payment_success and "rollback_payment_started" not in events:
        append_log(order_id, {"event": "rollback_payment_started"})
        publish_rollback_payment(order_id, order.user_id, order.total_cost)


    append_log(order_id, {"event": "cancelled"})
    # cancelled is the terminal log event for a compensated order, so mark the
    # stored checkout as no longer eligible for success commit. Keep the order
    # in the recovery set until rollback completion is confirmed.
    order.checkout_finalized = True
    db.set(order_id, msgpack.encode(order))
    # we do not mark the order completed because we need to make sure the rollbacks are completed

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
    value = msgpack.encode(OrderValue(items=[], user_id=user_id, total_cost=0, expected_items=0,
                                      stock_confirmations=0, rollback_stock_confirmations=0))
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
            rollback_stock_confirmations=0,
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
        response = requests.post(url, timeout=5)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url, timeout=5)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(_stock_service_url(item_id, f"/find/{item_id}"))
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")

    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)

# For logging
def append_log(order_id: str, entry: dict):
    key = f"_log:{order_id}"
    raw = db.get(key)
    logs = json.loads(raw) if raw else []
    logs.append({**entry, "timestamp": time.time()})
    db.set(key, json.dumps(logs))

def get_log_events(order_id: str) -> list[str]:
    key = f"_log:{order_id}"
    raw = db.get(key)
    if not raw:
        return []
    logs = json.loads(raw)
    return [entry["event"] for entry in logs]

def get_latest_log_event(order_id: str) -> str | None:
    events = get_log_events(order_id)
    return events[-1] if events else None

def log_saga_start(order_id: str):
    append_log(order_id, {"event": "saga_started"})
    db.zadd("_pending_orders", {order_id: time.time()})

def mark_order_done(order_id: str):
    db.zrem("_pending_orders", order_id)


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    log_saga_start(order_id)
    order_entry: OrderValue = get_order_from_db(order_id)

    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    # Update order state
    order_entry.expected_items = len(items_quantities)
    order_entry.stock_confirmations = 0
    # Reset the parallel branch state at the same transition where checkout is
    # logged as started so the order snapshot matches the event trail.
    order_entry.payment_reply_received = False
    order_entry.payment_success = False
    order_entry.checkout_finalized = False
    # Both branches are launched from checkout, so the log reflects both
    # in-flight operations before publish. Recovery uses the outgoing-message
    # records to resend whichever branch did not actually leave the service.
    append_log(order_id, {"event": "stock_pending"})
    append_log(order_id, {"event": "payment_pending"})
    db.set(order_id, msgpack.encode(order_entry))

    for item_id, quantity in items_quantities.items():
        publish_subtract_stock(order_id, item_id, quantity)
    publish_payment(order_id, order_entry.user_id, order_entry.total_cost)
    return Response("Checkout payment and stock subtraction requested asynchronously", status=202)




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
