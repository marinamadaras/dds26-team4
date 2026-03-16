import os
import re
import threading
import uuid
import zlib

import msgspec
from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_END
from flask import Flask, jsonify, Response, request

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("KAFKA_REQUEST_TIMEOUT_SECONDS", "10"))
REPLY_TOPICS = (
    "gateway.order.replies",
    "gateway.stock.replies",
    "gateway.payment.replies",
)

producer = Producer({"bootstrap.servers": BOOTSTRAP})
app = Flask("api-gateway")
_reply_consumer_threads: dict[int, threading.Thread] = {}
_reply_consumer_ready = {
    partition: threading.Event() for partition in range(PARTITIONS)
}
_pending_lock = threading.Lock()
_pending_requests: dict[str, dict[str, object]] = {}

# this is more of a helper service for nginx to be able to route to different partitions
def _partition_from_hash(key: str) -> int:
    return zlib.crc32(key.encode()) % PARTITIONS # this extracts which partition to use from the key


def _partition_from_order_id(order_id: str) -> int:
    match = re.match(r"^s(\d+)_", order_id)
    if match:
        return int(match.group(1)) % PARTITIONS
    return _partition_from_hash(order_id)


def _partition_from_item_id(item_id: str) -> int:
    match = re.match(r"^t(\d+)_", item_id)
    if match:
        return int(match.group(1)) % PARTITIONS
    return _partition_from_hash(item_id)


def _partition_from_user_id(user_id: str) -> int:
    match = re.match(r"^p(\d+)_", user_id)
    if match:
        return int(match.group(1)) % PARTITIONS
    return _partition_from_hash(user_id)


def _reply_consumer_loop(partition: int):
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"api-gateway-replies-{partition}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    topic_partitions = [
        TopicPartition(topic, partition, OFFSET_END) for topic in REPLY_TOPICS
    ]
    consumer.assign(topic_partitions)
    consumer.poll(0)
    _reply_consumer_ready[partition].set()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                app.logger.error("Kafka reply consumer error: %s", msg.error())
                continue

            try:
                data = msgspec.json.decode(msg.value(), type=dict)
            except Exception as exc:
                app.logger.warning("Failed to decode gateway reply: %s", exc)
                continue

            request_id = str(data.get("request_id", ""))
            if not request_id:
                continue

            with _pending_lock:
                pending = _pending_requests.get(request_id)

            if pending is None:
                continue
            if msg.topic() != pending["reply_topic"]:
                continue
            if msg.partition() != pending["partition"]:
                continue

            pending["response"] = (
                int(data.get("status_code", 500)),
                data.get("body", ""),
            )
            pending["event"].set()
    finally:
        consumer.close()


def _start_reply_consumers():
    for partition in range(PARTITIONS):
        thread = _reply_consumer_threads.get(partition)
        if thread is not None and thread.is_alive():
            continue
        thread = threading.Thread(
            target=_reply_consumer_loop,
            args=(partition,),
            daemon=True,
        )
        thread.start()
        _reply_consumer_threads[partition] = thread
        app.logger.info(
            "gateway reply consumer thread started partition=%s",
            partition,
        )


def _send_command(
    command_topic: str,
    reply_topic: str,
    key: str,
    partition: int,
    method: str,
    action: str,
):
    request_id = str(uuid.uuid4())
    payload = {
        "request_id": request_id,
        "method": method,
        "action": action,
    }
    if not _reply_consumer_ready[partition].wait(timeout=5):
        return 503, {"error": "Gateway reply consumer not ready"}

    pending = {
        "event": threading.Event(),
        "response": None,
        "reply_topic": reply_topic,
        "partition": partition,
    }
    with _pending_lock:
        _pending_requests[request_id] = pending

    try:
        producer.produce(
            command_topic,
            key=key.encode(),
            value=msgspec.json.encode(payload),
            partition=partition,
        )
        producer.flush(2)

        if not pending["event"].wait(timeout=REQUEST_TIMEOUT_SECONDS):
            return 504, {"error": "Gateway timeout waiting for service reply"}
        return pending["response"]
    finally:
        with _pending_lock:
            _pending_requests.pop(request_id, None)


def _to_response(status_code: int, body: object):
    if isinstance(body, dict):
        return jsonify(body), status_code
    return Response(str(body), status=status_code)


@app.post("/orders/create/<user_id>")
@app.post("/create/<user_id>")
def order_create(user_id: str):
    partition = _partition_from_hash(user_id)
    status, body = _send_command(
        command_topic="gateway.order.commands",
        reply_topic="gateway.order.replies",
        key=user_id,
        partition=partition,
        method="POST",
        action=f"create/{user_id}",
    )
    return _to_response(status, body)


@app.get("/orders/find/<order_id>")
@app.get("/find/<order_id>")
def order_find(order_id: str):
    partition = _partition_from_order_id(order_id)
    status, body = _send_command(
        command_topic="gateway.order.commands",
        reply_topic="gateway.order.replies",
        key=order_id,
        partition=partition,
        method="GET",
        action=f"find/{order_id}",
    )
    return _to_response(status, body)


@app.post("/orders/addItem/<order_id>/<item_id>/<quantity>")
@app.post("/addItem/<order_id>/<item_id>/<quantity>")
def order_add_item(order_id: str, item_id: str, quantity: int):
    partition = _partition_from_order_id(order_id)
    status, body = _send_command(
        command_topic="gateway.order.commands",
        reply_topic="gateway.order.replies",
        key=order_id,
        partition=partition,
        method="POST",
        action=f"addItem/{order_id}/{item_id}/{quantity}",
    )
    return _to_response(status, body)


@app.post("/orders/checkout/<order_id>")
@app.post("/checkout/<order_id>")
def order_checkout(order_id: str):
    partition = _partition_from_order_id(order_id)
    status, body = _send_command(
        command_topic="gateway.order.commands",
        reply_topic="gateway.order.replies",
        key=order_id,
        partition=partition,
        method="POST",
        action=f"checkout/{order_id}",
    )
    return _to_response(status, body)


@app.post("/stock/item/create/<price>")
@app.post("/item/create/<price>")
def stock_create(price: int):
    partition = _partition_from_hash(str(request.remote_addr or "0.0.0.0"))
    status, body = _send_command(
        command_topic="gateway.stock.commands",
        reply_topic="gateway.stock.replies",
        key=str(price),
        partition=partition,
        method="POST",
        action=f"item/create/{price}",
    )
    return _to_response(status, body)


@app.get("/stock/find/<item_id>")
@app.get("/find_stock/<item_id>")
def stock_find(item_id: str):
    partition = _partition_from_item_id(item_id)
    status, body = _send_command(
        command_topic="gateway.stock.commands",
        reply_topic="gateway.stock.replies",
        key=item_id,
        partition=partition,
        method="GET",
        action=f"find/{item_id}",
    )
    return _to_response(status, body)


@app.post("/stock/add/<item_id>/<amount>")
@app.post("/add_stock/<item_id>/<amount>")
def stock_add(item_id: str, amount: int):
    partition = _partition_from_item_id(item_id)
    status, body = _send_command(
        command_topic="gateway.stock.commands",
        reply_topic="gateway.stock.replies",
        key=item_id,
        partition=partition,
        method="POST",
        action=f"add/{item_id}/{amount}",
    )
    return _to_response(status, body)


@app.post("/stock/subtract/<item_id>/<amount>")
@app.post("/subtract_stock/<item_id>/<amount>")
def stock_subtract(item_id: str, amount: int):
    partition = _partition_from_item_id(item_id)
    status, body = _send_command(
        command_topic="gateway.stock.commands",
        reply_topic="gateway.stock.replies",
        key=item_id,
        partition=partition,
        method="POST",
        action=f"subtract/{item_id}/{amount}",
    )
    return _to_response(status, body)


@app.post("/payment/create_user")
@app.post("/create_user")
def payment_create_user():
    partition = _partition_from_hash(str(request.remote_addr or "0.0.0.0"))
    status, body = _send_command(
        command_topic="gateway.payment.commands",
        reply_topic="gateway.payment.replies",
        key="create_user",
        partition=partition,
        method="POST",
        action="create_user",
    )
    return _to_response(status, body)


@app.get("/payment/find_user/<user_id>")
@app.get("/find_user/<user_id>")
def payment_find_user(user_id: str):
    partition = _partition_from_user_id(user_id)
    status, body = _send_command(
        command_topic="gateway.payment.commands",
        reply_topic="gateway.payment.replies",
        key=user_id,
        partition=partition,
        method="GET",
        action=f"find_user/{user_id}",
    )
    return _to_response(status, body)


@app.post("/payment/add_funds/<user_id>/<amount>")
@app.post("/add_funds/<user_id>/<amount>")
def payment_add_funds(user_id: str, amount: int):
    partition = _partition_from_user_id(user_id)
    status, body = _send_command(
        command_topic="gateway.payment.commands",
        reply_topic="gateway.payment.replies",
        key=user_id,
        partition=partition,
        method="POST",
        action=f"add_funds/{user_id}/{amount}",
    )
    return _to_response(status, body)


@app.post("/payment/pay/<user_id>/<amount>")
@app.post("/pay/<user_id>/<amount>")
def payment_pay(user_id: str, amount: int):
    partition = _partition_from_user_id(user_id)
    status, body = _send_command(
        command_topic="gateway.payment.commands",
        reply_topic="gateway.payment.replies",
        key=user_id,
        partition=partition,
        method="POST",
        action=f"pay/{user_id}/{amount}",
    )
    return _to_response(status, body)


if __name__ == "__main__":
    _start_reply_consumers()
    app.run(host="0.0.0.0", port=5000)
else:
    _start_reply_consumers()
