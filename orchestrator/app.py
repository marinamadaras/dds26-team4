import atexit
import json
import os
import threading
import time
import uuid
from urllib.parse import urljoin

from confluent_kafka import Consumer, Producer, TopicPartition, OFFSET_BEGINNING
import redis
import requests
from flask import Flask, Response, jsonify, request


app = Flask("orchestrator-service")

SERVICE_URLS = {
    "order": os.getenv("ORDER_SERVICE_URL", "http://order-service-0:5000"),
    "payment": os.getenv("PAYMENT_SERVICE_URL", "http://payment-service-0:5000"),
    "stock": os.getenv("STOCK_SERVICE_URL", "http://stock-service-0:5000"),
}
SERVICE_PING_PATHS = {
    "order": os.getenv("ORDER_SERVICE_PING_PATH", "/find/healthcheck"),
    "payment": os.getenv("PAYMENT_SERVICE_PING_PATH", "/find_user/healthcheck"),
    "stock": os.getenv("STOCK_SERVICE_PING_PATH", "/find/healthcheck"),
}

REQUEST_TIMEOUT_SECONDS = float(os.getenv("ORCHESTRATOR_REQUEST_TIMEOUT_SECONDS", "5"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SUPPORTED_METHODS = {"GET", "POST"}
SUPPORTED_TASK_PROTOCOLS = {"http", "kafka"}
TASK_KEY_PREFIX = "task:"


db: redis.Redis = redis.Redis(
    host=os.getenv("REDIS_HOST", "orchestrator-db"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    password=os.getenv("REDIS_PASSWORD", "redis"),
    db=int(os.getenv("REDIS_DB", "0")),
    decode_responses=True,
)
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def close_db_connection():
    # Close the Redis client cleanly on shutdown.
    db.close()
    producer.flush(2)


atexit.register(close_db_connection)


def _service_url(service_name: str, path: str = "") -> str | None:
    # Build the target URL for a known upstream service.
    base_url = SERVICE_URLS.get(service_name)
    if base_url is None:
        return None
    return urljoin(f"{base_url.rstrip('/')}/", path.lstrip("/"))


def _get_explicit_partition(spec: dict[str, object], partition_key: str) -> int | None:
    # Read an explicitly provided partition from the task spec.
    partition = spec.get(partition_key)
    if partition is None:
        return None
    if not isinstance(partition, int):
        raise ValueError(f"{partition_key} must be an integer when provided")
    return partition


def _publish_kafka_payload(
    *,
    topic: str,
    key: str | None,
    payload: dict[str, object],
    partition: int | None,
):
    # Publish a raw JSON payload to Kafka with optional explicit partition routing.
    kwargs: dict[str, object] = {}
    if partition is not None:
        kwargs["partition"] = partition
    producer.produce(
        topic,
        key=key.encode() if key is not None else None,
        value=json.dumps(payload).encode(),
        **kwargs,
    )
    producer.flush(2)


def _publish_task_result_reply(task: dict[str, object], result: dict[str, object]) -> dict[str, object] | None:
    # Optionally publish the task execution result back onto Kafka using derived partition routing.
    request_spec = task.get("request", {})
    if not isinstance(request_spec, dict):
        return None
    reply_spec = request_spec.get("task_result_reply")
    if not isinstance(reply_spec, dict):
        return None

    topic = reply_spec.get("topic")
    if not isinstance(topic, str) or not topic:
        raise ValueError("request.task_result_reply.topic is required when task_result_reply is used")

    key = reply_spec.get("key")
    if key is not None and not isinstance(key, str):
        raise ValueError("request.task_result_reply.key must be a string when provided")

    partition = _get_explicit_partition(reply_spec, "partition")

    reply_payload_mode = str(reply_spec.get("payload_mode", "task_result"))
    if reply_payload_mode == "task_result":
        payload = {
            "task_id": task["task_id"],
            "task_name": task["task_name"],
            "status": "completed",
            "result": result,
        }
    elif reply_payload_mode == "reply_payload":
        payload = result.get("reply", {}).get("payload")
        if not isinstance(payload, dict):
            raise ValueError("request.task_result_reply.payload_mode=reply_payload requires a Kafka reply payload")
    else:
        raise ValueError(f"Unsupported task_result_reply.payload_mode: {reply_payload_mode}")

    _publish_kafka_payload(topic=topic, key=key, payload=payload, partition=partition)
    return {
        "topic": topic,
        "key": key,
        "partition": partition,
        "payload_mode": reply_payload_mode,
    }


def _forward(service_name: str, path: str):
    # Proxy a GET/POST request from the orchestrator to one upstream service.
    target_url = _service_url(service_name, path)
    if target_url is None:
        return jsonify({"error": f"Unknown service: {service_name}"}), 404

    method = request.method.upper()
    if method not in SUPPORTED_METHODS:
        return jsonify({"error": f"Unsupported method: {method}"}), 405

    headers: dict[str, str] = {}
    content_type = request.headers.get("Content-Type")
    if content_type:
        headers["Content-Type"] = content_type

    try:
        upstream = requests.request(
            method=method,
            url=target_url,
            params=request.args,
            json=request.get_json(silent=True),
            data=request.get_data() if not request.is_json else None,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        return jsonify(
            {
                "error": "Upstream request failed",
                "service": service_name,
                "target_url": target_url,
                "details": str(exc),
            }
        ), 502

    excluded_headers = {"content-encoding", "content-length", "transfer-encoding", "connection"}
    response_headers = [
        (key, value)
        for key, value in upstream.headers.items()
        if key.lower() not in excluded_headers
    ]
    return Response(upstream.content, status=upstream.status_code, headers=response_headers)


def _create_task(task_type: str, payload: dict[str, object]) -> dict[str, object]:
    # Persist the initial task record before any execution begins.
    task_id = str(uuid.uuid4())
    task = {
        "task_id": task_id,
        "task_name": task_type,
        "status": "pending",
        "source_service": payload["source_service"],
        "target_service": payload["target_service"],
        "protocol": payload["protocol"],
        "request": payload["request"],
        "context": payload.get("context", {}),
        "policy": payload.get("policy", {}),
        "depends_on": payload.get("depends_on", []),
        "result": None,
        "error": None,
        "created_at": time.time(),
        "updated_at": time.time(),
    }
    db.set(f"{TASK_KEY_PREFIX}{task_id}", json.dumps(task))
    return task


def _update_task(task_id: str, **fields: object) -> dict[str, object] | None:
    # Update a stored task in Redis and return the latest snapshot.
    task = _get_task(task_id)
    if task is None:
        return None
    task.update(fields)
    task["updated_at"] = time.time()
    db.set(f"{TASK_KEY_PREFIX}{task_id}", json.dumps(task))
    return task


def _get_task(task_id: str) -> dict[str, object] | None:
    # Load one task record from Redis by its id.
    raw = db.get(f"{TASK_KEY_PREFIX}{task_id}")
    return json.loads(raw) if raw else None


def _execute_task_lifecycle(task_id: str):
    # Run the task shell for one service-to-service action.
    task = _update_task(task_id, status="running")
    if task is None:
        return

    try:
        # TODO: Replace the in-process thread runner with durable async execution.
        result = _dispatch_task_by_protocol(task)
        result_reply = _publish_task_result_reply(task, result)
        if result_reply is not None:
            result["task_result_reply"] = result_reply
        _update_task(task_id, status="completed", result=result, error=None)
    except Exception as exc:
        _update_task(task_id, status="failed", error=str(exc))


def _dispatch_task_by_protocol(task: dict[str, object]) -> dict[str, object]:
    # Interpret the task envelope and return the runnable description.
    protocol = str(task["protocol"])
    if protocol == "http":
        return _execute_http_task(task)
    if protocol == "kafka":
        return _execute_kafka_task(task)
    raise ValueError(f"Unsupported task protocol: {protocol}")


def _render_path(path_template: str, path_params: dict[str, object]) -> str:
    # Render a URL path template with simple named placeholders.
    path = path_template
    for key, value in path_params.items():
        path = path.replace(f"{{{key}}}", str(value))
    return path


def _execute_http_task(task: dict[str, object]) -> dict[str, object]:
    # Describe an HTTP service-call task in a transport-neutral task record.
    request_spec = dict(task["request"])
    method = str(request_spec.get("method", "GET")).upper()
    path_template = str(request_spec.get("path", ""))
    path_params = request_spec.get("path_params", {})
    query_params = request_spec.get("query_params", {})
    body = request_spec.get("body")
    headers = request_spec.get("headers", {})

    if method not in SUPPORTED_METHODS:
        raise ValueError(f"Unsupported HTTP method: {method}")
    if not isinstance(path_params, dict):
        raise ValueError("request.path_params must be a JSON object")
    if not isinstance(query_params, dict):
        raise ValueError("request.query_params must be a JSON object")
    if not isinstance(headers, dict):
        raise ValueError("request.headers must be a JSON object")

    target_service = str(task["target_service"])
    rendered_path = _render_path(path_template, path_params)
    target_url = _service_url(target_service, rendered_path)
    if target_url is None:
        raise ValueError(f"Unknown target service: {target_service}")

    timeout_seconds = float(task["policy"].get("timeout_seconds", REQUEST_TIMEOUT_SECONDS))
    response = requests.request(
        method=method,
        url=target_url,
        params=query_params,
        json=body if isinstance(body, dict) else None,
        data=body if body is not None and not isinstance(body, dict) else None,
        headers={str(key): str(value) for key, value in headers.items()},
        timeout=timeout_seconds,
    )
    content_type = response.headers.get("Content-Type", "")
    if "application/json" in content_type:
        response_body: object = response.json()
    else:
        response_body = response.text

    return {
        "task_name": task["task_name"],
        "source_service": task["source_service"],
        "target_service": target_service,
        "protocol": "http",
        "request": {
            "method": method,
            "path": rendered_path,
            "query_params": query_params,
            "body": body,
            "headers": headers,
        },
        "target_url": target_url,
        "context": task["context"],
        "policy": task["policy"],
        "depends_on": task["depends_on"],
        "dispatched": True,
        "response": {
            "status_code": response.status_code,
            "content_type": content_type,
            "body": response_body,
        },
    }


def _execute_kafka_task(task: dict[str, object]) -> dict[str, object]:
    # Describe a Kafka service-call task in the generic task envelope.
    request_spec = dict(task["request"])
    topic = request_spec.get("topic")
    key = request_spec.get("key")
    payload = request_spec.get("payload")
    reply_topic = request_spec.get("reply_topic")
    correlation_id_field = str(request_spec.get("correlation_id_field", "request_id"))
    await_reply = bool(request_spec.get("await_reply", False))
    reply_timeout_seconds = float(request_spec.get("reply_timeout_seconds", REQUEST_TIMEOUT_SECONDS))

    if not isinstance(topic, str) or not topic:
        raise ValueError("request.topic is required for kafka tasks")
    if key is not None and not isinstance(key, str):
        raise ValueError("request.key must be a string when provided")
    if payload is not None and not isinstance(payload, dict):
        raise ValueError("request.payload must be a JSON object")
    if await_reply and (not isinstance(reply_topic, str) or not reply_topic):
        raise ValueError("request.reply_topic is required when await_reply is true")

    request_partition = _get_explicit_partition(request_spec, "partition")
    reply_partition = _get_explicit_partition(request_spec, "reply_partition")

    outgoing_payload = dict(payload or {})
    correlation_id = str(outgoing_payload.get(correlation_id_field, str(uuid.uuid4())))
    outgoing_payload[correlation_id_field] = correlation_id

    reply_waiter = None
    if await_reply:
        reply_waiter = _prepare_kafka_reply_consumer(reply_topic=reply_topic, partition=reply_partition)

    try:
        _publish_kafka_payload(
            topic=topic,
            key=key,
            payload=outgoing_payload,
            partition=request_partition,
        )

        reply: dict[str, object] | None = None
        if await_reply and reply_waiter is not None:
            reply = _consume_kafka_reply(
                consumer=reply_waiter,
                reply_topic=reply_topic,
                correlation_id_field=correlation_id_field,
                correlation_id=correlation_id,
                timeout_seconds=reply_timeout_seconds,
            )
    finally:
        if reply_waiter is not None:
            reply_waiter.close()

    return {
        "task_name": task["task_name"],
        "source_service": task["source_service"],
        "target_service": task["target_service"],
        "protocol": "kafka",
        "request": {
            "topic": topic,
            "key": key,
            "payload": outgoing_payload,
            "partition": request_partition,
            "reply_topic": reply_topic,
            "reply_partition": reply_partition,
            "await_reply": await_reply,
            "correlation_id_field": correlation_id_field,
        },
        "context": task["context"],
        "policy": task["policy"],
        "depends_on": task["depends_on"],
        "dispatched": True,
        "reply": reply,
    }


def _prepare_kafka_reply_consumer(*, reply_topic: str, partition: int | None) -> Consumer:
    # Create a reply consumer that can scan replies until it finds the correlation id.
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"orchestrator-task-replies-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    if partition is None:
        consumer.subscribe([reply_topic])
        consumer.poll(0)
    else:
        consumer.assign([TopicPartition(reply_topic, partition, OFFSET_BEGINNING)])
        consumer.poll(0)
    return consumer


def _consume_kafka_reply(
    *,
    consumer: Consumer,
    reply_topic: str,
    correlation_id_field: str,
    correlation_id: str,
    timeout_seconds: float,
) -> dict[str, object]:
    # Wait for one correlated Kafka reply on the configured reply topic.
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        data = json.loads(msg.value().decode())
        if str(data.get(correlation_id_field, "")) != correlation_id:
            continue
        return {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "payload": data,
        }
    raise TimeoutError(f"Timed out waiting for Kafka reply on {reply_topic}")


def _validate_task_payload(body: dict[str, object]) -> tuple[str, dict[str, object]]:
    # Validate the generic task envelope before it is accepted.
    # TODO: Enforce deeper schema validation per protocol and per action.
    task_name = str(body.get("task_name", "")).strip()
    if not task_name:
        raise ValueError("task_name is required")

    source_service = body.get("source_service")
    target_service = body.get("target_service")
    protocol = str(body.get("protocol", "")).strip()
    request_spec = body.get("request", {})
    context = body.get("context", {})
    policy = body.get("policy", {})
    depends_on = body.get("depends_on", [])

    if not isinstance(source_service, str) or not source_service:
        raise ValueError("source_service is required")
    if not isinstance(target_service, str) or not target_service:
        raise ValueError("target_service is required")
    if protocol not in SUPPORTED_TASK_PROTOCOLS:
        raise ValueError(f"Unsupported protocol: {protocol}")
    if not isinstance(request_spec, dict):
        raise ValueError("request must be a JSON object")
    if not isinstance(context, dict):
        raise ValueError("context must be a JSON object")
    if not isinstance(policy, dict):
        raise ValueError("policy must be a JSON object")
    if not isinstance(depends_on, list):
        raise ValueError("depends_on must be a JSON array")
    if protocol == "kafka":
        task_result_reply = request_spec.get("task_result_reply")
        if task_result_reply is not None and not isinstance(task_result_reply, dict):
            raise ValueError("request.task_result_reply must be a JSON object")

    payload = {
        "source_service": source_service,
        "target_service": target_service,
        "protocol": protocol,
        "request": request_spec,
        "context": context,
        "policy": policy,
        "depends_on": depends_on,
    }
    return task_name, payload


@app.get("/ping")
def ping():
    # Return a lightweight liveness response for the orchestrator itself.
    return jsonify(
        {
            "service": "orchestrator",
            "status": "ok",
            "services": SERVICE_URLS,
        }
    )


@app.get("/connections")
def connections():
    # Check whether the orchestrator can reach each configured upstream service.
    status: dict[str, dict[str, object]] = {}
    for service_name in SERVICE_URLS:
        target_url = _service_url(service_name, SERVICE_PING_PATHS.get(service_name, ""))
        if target_url is None:
            status[service_name] = {"reachable": False, "error": "Missing service URL"}
            continue
        try:
            response = requests.get(target_url, timeout=REQUEST_TIMEOUT_SECONDS)
            status[service_name] = {
                "reachable": True,
                "status_code": response.status_code,
                "target_url": target_url,
                "ok": response.ok,
            }
        except requests.RequestException as exc:
            status[service_name] = {
                "reachable": False,
                "target_url": target_url,
                "error": str(exc),
            }
    return jsonify(status)


@app.post("/submit_task")
def submit_task():
    # Accept a task definition and queue the runnable in the orchestrator.
    body = request.get_json(silent=True) or {}
    try:
        task_name, payload = _validate_task_payload(body)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    task = _create_task(task_name, payload)
    thread = threading.Thread(target=_execute_task_lifecycle, args=(task["task_id"],), daemon=True)
    thread.start()
    return jsonify(task), 202


@app.get("/status_task/<task_id>")
def status_task(task_id: str):
    # Return the current execution state for a submitted task.
    task = _get_task(task_id)
    if task is None:
        return jsonify({"error": f"Task not found: {task_id}"}), 404
    return jsonify(task)


@app.route("/proxy/<service_name>", defaults={"path": ""}, methods=["GET", "POST"])
@app.route("/proxy/<service_name>/<path:path>", methods=["GET", "POST"])
def proxy(service_name: str, path: str):
    # Forward a request to a specific upstream service path.
    return _forward(service_name, path)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
