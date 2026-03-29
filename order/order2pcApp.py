import logging
import os
import atexit
import random
import re
import time
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
    CheckoutRequested,
    PrepareStockRequest,
    PrepareStockReply,
    PreparePaymentRequest,
    PreparePaymentReply,
    StockDecisionRequest,
    StockDecisionReply,
    PaymentDecisionRequest,
    PaymentDecisionReply,
)
from span_logger import span, log_span_event
KAFKA_CONSUMER_INSTANCE_ID = os.getenv("KAFKA_CONSUMER_INSTANCE_ID", "order-service-0")
KAFKA_CONSUMER_PARTITION = int(os.getenv("KAFKA_CONSUMER_PARTITION", "0"))
DB_ERROR_STR = "DB error"
app = Flask("order-service")
_consumer_thread: threading.Thread | None = None
_tx_span_state: dict[str, dict[str, float]] = {}

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


def _partition_from_prefixed_id(entity_id: str, prefix: str) -> int:
    match = re.match(rf"^{prefix}(\d+)_", entity_id)
    if match:
        return int(match.group(1))
    return KAFKA_CONSUMER_PARTITION


def _partition_from_order_id(order_id: str) -> int:
    return _partition_from_prefixed_id(order_id, "s")


def _partition_from_item_id(item_id: str) -> int:
    return _partition_from_prefixed_id(item_id, "t")


def _partition_from_user_id(user_id: str) -> int:
    return _partition_from_prefixed_id(user_id, "p")


def _stock_participant_partition(items: list[tuple[str, int]]) -> int:
    if not items:
        return KAFKA_CONSUMER_PARTITION
    partitions = {_partition_from_item_id(item_id) for item_id, _ in items}
    if len(partitions) != 1:
        raise ValueError("2PC checkout requires all items to belong to one stock partition")
    return next(iter(partitions))


def publish_find_stock(order_id: str, item_id: str, quantity: int):
    message = FindStock(order_id=order_id, item_id=item_id, quantity=int(quantity))

    publish(
        topic="find.stock",
        key=item_id,
        value=message,
        partition=_partition_from_item_id(item_id),
    )


def publish_subtract_stock(order_id: str, item_id: str, quantity: int):
    message = SubtractStock(order_id=order_id, item_id=item_id, quantity=int(quantity))
    publish(
        topic="subtract.stock",
        key=message.item_id,
        value=message,
        partition=_partition_from_item_id(message.item_id),
    )


def publish_payment(order_id: str, user_id: str, amount: int):
    message = PaymentRequest(order_id=order_id, user_id=user_id, amount=int(amount))
    publish(
        topic="payment",
        key=user_id,
        value=message,
        partition=_partition_from_user_id(user_id),
    )


def publish_rollback_stock(order_id: str, item_id: str, quantity: int):
    message = RollbackStockRequest(order_id=order_id, item_id=item_id, quantity=int(quantity))
    publish(
        topic="rollback.stock",
        key=item_id,
        value=message,
        partition=_partition_from_item_id(item_id),
    )


def publish_rollback_payment(order_id: str, user_id: str, amount: int):
    message = RollbackPaymentRequest(order_id=order_id, user_id=user_id, amount=int(amount))
    publish(
        topic="rollback.payment",
        key=user_id,
        value=message,
        partition=_partition_from_user_id(user_id),
    )


def publish_checkout_requested(order_id: str):
    message = CheckoutRequested(order_id=order_id)
    publish(
        topic="checkout.requested",
        key=order_id,
        value=message,
        partition=_partition_from_order_id(order_id),
    )


def publish_prepare_stock(tx_id: str, items: list[tuple[str, int]]):
    message = PrepareStockRequest(
        tx_id=tx_id,
        coordinator_partition=KAFKA_CONSUMER_PARTITION,
        items=items,
    )
    publish(
        topic="2pc.stock.prepare",
        key=tx_id,
        value=message,
        partition=_stock_participant_partition(items),
    )


def publish_prepare_payment(tx_id: str, user_id: str, amount: int):
    message = PreparePaymentRequest(
        tx_id=tx_id,
        coordinator_partition=KAFKA_CONSUMER_PARTITION,
        user_id=user_id,
        amount=int(amount),
    )
    publish(
        topic="2pc.payment.prepare",
        key=tx_id,
        value=message,
        partition=_partition_from_user_id(user_id),
    )


def publish_stock_decision(tx_id: str, decision: str, items: list[tuple[str, int]]):
    message = StockDecisionRequest(
        tx_id=tx_id,
        coordinator_partition=KAFKA_CONSUMER_PARTITION,
        decision=decision,
    )
    publish(
        topic="2pc.stock.decision",
        key=tx_id,
        value=message,
        partition=_stock_participant_partition(items),
    )


def publish_payment_decision(tx_id: str, decision: str, user_id: str):
    message = PaymentDecisionRequest(
        tx_id=tx_id,
        coordinator_partition=KAFKA_CONSUMER_PARTITION,
        decision=decision,
    )
    publish(
        topic="2pc.payment.decision",
        key=tx_id,
        value=message,
        partition=_partition_from_user_id(user_id),
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

def handle_http_command(command: dict):
    # Handle gateway Kafka commands by calling existing local HTTP routes.
    request_id = str(command.get("request_id", ""))
    method = str(command.get("method", "GET")).upper()
    action = str(command.get("action", "")).lstrip("/")
    if not request_id or not action:
        return

    url = f"http://127.0.0.1:5000/{action}"
    with span(
        app.logger,
        "order",
        "order.gateway_command.total",
        trace_id=request_id,
        action=action,
        method=method,
        partition=KAFKA_CONSUMER_PARTITION,
    ):
        try:
            with span(
                app.logger,
                "order",
                "order.gateway_command.local_http",
                trace_id=request_id,
                action=action,
                method=method,
            ):
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

        with span(
            app.logger,
            "order",
            "order.gateway_command.reply_publish",
            trace_id=request_id,
            status_code=status_code,
        ):
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


def _is_prepare_phase_complete(tx: "CheckoutTransaction") -> bool:
    return tx.stock_prepare_done and tx.payment_prepare_done


def _is_decision_phase_complete(tx: "CheckoutTransaction") -> bool:
    return tx.stock_decision_done and tx.payment_decision_done


def _set_prepare_reply(tx: "CheckoutTransaction", source: str, success: bool, error: str | None):
    if source == "stock":
        tx.stock_prepare_done = True
        tx.stock_prepare_success = success
    else:
        tx.payment_prepare_done = True
        tx.payment_prepare_success = success
    if not success:
        tx.last_error = error or "participant prepare failed"


def _set_decision_reply(tx: "CheckoutTransaction", source: str, success: bool, error: str | None):
    if source == "stock":
        tx.stock_decision_done = success
    else:
        tx.payment_decision_done = success
    if not success:
        tx.last_error = error or "participant decision execution failed"


def _maybe_decide_and_broadcast(tx_id: str, tx: "CheckoutTransaction"):
    if tx.state != "PREPARING" or not _is_prepare_phase_complete(tx):
        return
    started = _tx_span_state.setdefault(tx_id, {})
    prepare_started_at = started.get("prepare_started_at")
    if prepare_started_at is not None:
        log_span_event(
            app.logger,
            "order",
            "order.checkout.prepare_phase.complete",
            trace_id=tx_id,
            order_id=tx.order_id,
            duration_ms=round((time.perf_counter() - prepare_started_at) * 1000, 3),
        )
    prepare_success = bool(tx.stock_prepare_success) and bool(tx.payment_prepare_success)
    tx.decision = "COMMIT" if prepare_success else "ABORT"
    tx.state = "DECIDED"
    set_tx(tx_id, tx)
    started["decision_started_at"] = time.perf_counter()
    with span(
        app.logger,
        "order",
        "order.checkout.decision_publish",
        trace_id=tx_id,
        order_id=tx.order_id,
        decision=tx.decision,
    ):
        publish_stock_decision(tx_id, tx.decision, tx.items)
        publish_payment_decision(tx_id, tx.decision, tx.user_id)


def _maybe_complete(tx_id: str, tx: "CheckoutTransaction"):
    if tx.state != "DECIDED" or not _is_decision_phase_complete(tx):
        return
    if tx.decision == "COMMIT" and not apply_commit_effects(tx):
        tx.last_error = "failed to apply commit effects"
        set_tx(tx_id, tx)
        return
    tx.state = "COMPLETED"
    set_tx(tx_id, tx)
    release_checkout_lock(tx.order_id)
    started = _tx_span_state.pop(tx_id, {})
    decision_started_at = started.get("decision_started_at")
    if decision_started_at is not None:
        log_span_event(
            app.logger,
            "order",
            "order.checkout.decision_phase.complete",
            trace_id=tx_id,
            order_id=tx.order_id,
            decision=tx.decision,
            duration_ms=round((time.perf_counter() - decision_started_at) * 1000, 3),
        )
    total_started_at = started.get("total_started_at")
    if total_started_at is not None:
        log_span_event(
            app.logger,
            "order",
            "order.checkout.tx.complete",
            trace_id=tx_id,
            order_id=tx.order_id,
            decision=tx.decision,
            duration_ms=round((time.perf_counter() - total_started_at) * 1000, 3),
        )


def handle_prepare_stock_reply(message: PrepareStockReply):
    with span(
        app.logger,
        "order",
        "order.checkout.prepare_stock_reply.handle",
        trace_id=message.tx_id,
        success=message.success,
    ):
        tx = get_tx_from_db(message.tx_id)
        if tx is None or tx.state != "PREPARING":
            return
        _set_prepare_reply(tx, "stock", message.success, message.error)
        set_tx(message.tx_id, tx)
        _maybe_decide_and_broadcast(message.tx_id, tx)


def handle_prepare_payment_reply(message: PreparePaymentReply):
    with span(
        app.logger,
        "order",
        "order.checkout.prepare_payment_reply.handle",
        trace_id=message.tx_id,
        success=message.success,
    ):
        tx = get_tx_from_db(message.tx_id)
        if tx is None or tx.state != "PREPARING":
            return
        _set_prepare_reply(tx, "payment", message.success, message.error)
        set_tx(message.tx_id, tx)
        _maybe_decide_and_broadcast(message.tx_id, tx)


def handle_stock_decision_reply(message: StockDecisionReply):
    with span(
        app.logger,
        "order",
        "order.checkout.stock_decision_reply.handle",
        trace_id=message.tx_id,
        decision=message.decision,
        success=message.success,
    ):
        tx = get_tx_from_db(message.tx_id)
        if tx is None or tx.state != "DECIDED":
            return
        _set_decision_reply(tx, "stock", message.success, message.error)
        set_tx(message.tx_id, tx)
        if not message.success:
            publish_stock_decision(message.tx_id, tx.decision, tx.items)
            return
        _maybe_complete(message.tx_id, tx)


def handle_payment_decision_reply(message: PaymentDecisionReply):
    with span(
        app.logger,
        "order",
        "order.checkout.payment_decision_reply.handle",
        trace_id=message.tx_id,
        decision=message.decision,
        success=message.success,
    ):
        tx = get_tx_from_db(message.tx_id)
        if tx is None or tx.state != "DECIDED":
            return
        _set_decision_reply(tx, "payment", message.success, message.error)
        set_tx(message.tx_id, tx)
        if not message.success:
            publish_payment_decision(message.tx_id, tx.decision, tx.user_id)
            return
        _maybe_complete(message.tx_id, tx)


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
    if isinstance(message, PrepareStockReply):
        handle_prepare_stock_reply(message)
        return
    if isinstance(message, PreparePaymentReply):
        handle_prepare_payment_reply(message)
        return
    if isinstance(message, StockDecisionReply):
        handle_stock_decision_reply(message)
        return
    if isinstance(message, PaymentDecisionReply):
        handle_payment_decision_reply(message)
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
            "2pc.stock.prepare.replies",
            "2pc.payment.prepare.replies",
            "2pc.stock.decision.replies",
            "2pc.payment.decision.replies",
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


class CheckoutTransaction(Struct):
    # PREPARING -> DECIDED -> COMPLETED
    state: str
    # COMMIT or ABORT once coordinator reaches a durable decision
    decision: str
    order_id: str
    user_id: str
    total_cost: int
    items: list[tuple[str, int]]
    stock_prepare_done: bool = False
    payment_prepare_done: bool = False
    stock_prepare_success: bool | None = None
    payment_prepare_success: bool | None = None
    stock_decision_done: bool = False
    payment_decision_done: bool = False
    last_error: str | None = None


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

def checkout_lock_key(order_id: str) -> str:
    return f"checkout:lock:{order_id}"


def try_acquire_checkout_lock(order_id: str) -> bool:
    try:
        return bool(db.set(checkout_lock_key(order_id), "1", ex=300, nx=True))
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


def replay_decision(tx_id: str, tx: CheckoutTransaction):
    if tx.decision not in {"COMMIT", "ABORT"}:
        return
    publish_stock_decision(tx_id, tx.decision, tx.items)
    publish_payment_decision(tx_id, tx.decision, tx.user_id)


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
        try:
            db.set(checkout_lock_key(tx.order_id), "1", ex=300)
        except redis.exceptions.RedisError:
            pass

        if tx.state == "PREPARING":
            tx.decision = "ABORT"
            tx.state = "DECIDED"
            try:
                db.set(tx_key, msgpack.encode(tx))
            except redis.exceptions.RedisError:
                continue

        if tx.state == "DECIDED":
            replay_decision(tx_id, tx)


def process_checkout_async(order_id: str):
    tx_id = str(uuid.uuid4())
    with span(
        app.logger,
        "order",
        "order.checkout.async.total",
        trace_id=tx_id,
        order_id=order_id,
    ):
        if not try_acquire_checkout_lock(order_id):
            app.logger.info("checkout already in progress for order=%s", order_id)
            return
        order_entry = get_order_if_exists(order_id)
        if order_entry is None:
            app.logger.warning("async checkout ignored: order %s not found", order_id)
            release_checkout_lock(order_id)
            return
        if order_entry.paid:
            app.logger.info("async checkout skipped: order %s already paid", order_id)
            release_checkout_lock(order_id)
            return

        items = aggregate_items(order_entry.items)
        tx = CheckoutTransaction(
            state="PREPARING",
            decision="UNKNOWN",
            order_id=order_id,
            user_id=order_entry.user_id,
            total_cost=order_entry.total_cost,
            items=items,
        )
        set_tx(tx_id, tx)
        _tx_span_state[tx_id] = {"total_started_at": time.perf_counter()}

        try:
            _tx_span_state[tx_id]["prepare_started_at"] = time.perf_counter()
            with span(
                app.logger,
                "order",
                "order.checkout.prepare_publish",
                trace_id=tx_id,
                order_id=order_id,
                item_count=len(items),
            ):
                publish_prepare_stock(tx_id, items)
                publish_prepare_payment(tx_id, order_entry.user_id, order_entry.total_cost)
        except ValueError as exc:
            tx.decision = "ABORT"
            tx.state = "COMPLETED"
            tx.last_error = str(exc)
            set_tx(tx_id, tx)
            release_checkout_lock(order_id)
            app.logger.warning("async checkout aborted order=%s tx=%s error=%s", order_id, tx_id, exc)
            _tx_span_state.pop(tx_id, None)
            return
        app.logger.info("async checkout started order=%s tx=%s", order_id, tx_id)


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


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    publish_find_stock(order_id, item_id, quantity)

    return Response(
        f"Stock check requested for item {item_id} in order {order_id} ",
        status=202
    )


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    with span(
        app.logger,
        "order",
        "order.checkout.http",
        trace_id=order_id,
        order_id=order_id,
    ):
        app.logger.debug(f"Queueing checkout for {order_id}")
        order_entry: OrderValue = get_order_from_db(order_id)
        if order_entry.paid:
            return Response("Order already paid", status=200)
        if is_checkout_in_progress(order_id):
            return Response("Checkout already in progress", status=202)

        try:
            with span(
                app.logger,
                "order",
                "order.checkout.enqueue",
                trace_id=order_id,
                order_id=order_id,
            ):
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
