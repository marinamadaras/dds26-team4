import os
import re
import uuid
import zlib
import time

import msgspec
from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING
from flask import Flask, jsonify, Response, request

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("KAFKA_REQUEST_TIMEOUT_SECONDS", "10"))

producer = Producer({"bootstrap.servers": BOOTSTRAP})
app = Flask("api-gateway")

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

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"api-gateway-{request_id}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.assign([TopicPartition(reply_topic, partition, OFFSET_BEGINNING)])
    # Ensure reply consumer is attached before publishing command.
    consumer.poll(0)

    producer.produce(
        command_topic,
        key=key.encode(),
        value=msgspec.json.encode(payload),
        partition=partition,
    )
    producer.flush(2)

    try:
        deadline = time.time() + REQUEST_TIMEOUT_SECONDS
        while True:
            remaining = max(0.0, deadline - time.time())
            if remaining == 0.0:
                return 504, {"error": "Gateway timeout waiting for service reply"}
            msg = consumer.poll(remaining)
            if msg is None:
                continue
            if msg.error():
                return 502, {"error": str(msg.error())}

            data = msgspec.json.decode(msg.value(), type=dict)
            if data.get("request_id") != request_id:
                continue
            return int(data.get("status_code", 500)), data.get("body", "")
    finally:
        consumer.close()


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
    app.run(host="0.0.0.0", port=5000)
