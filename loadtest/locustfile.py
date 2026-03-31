import json
import os
import random
import re
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

from locust import HttpUser, SequentialTaskSet, constant, task

from init_orders import NUMBER_OF_ORDERS

URLS_PATH = Path(__file__).resolve().parent / "urls.json"
REPO_ROOT = URLS_PATH.parent.parent
with open(URLS_PATH) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']


def detect_order_partitions() -> int:
    configured = os.getenv("KAFKA_PARTITIONS")
    if configured:
        return int(configured)

    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--services", "--status", "running"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        partitions = {
            int(match.group(1))
            for line in result.stdout.splitlines()
            if (match := re.match(r"^order-service-(\d+)$", line.strip()))
        }
        if partitions:
            return len(partitions)
    except Exception:
        pass

    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        partitions = {
            int(match.group(1))
            for line in result.stdout.splitlines()
            if (
                match := re.search(
                    r"(?:^|-)order-service-(\d+)(?:-\d+)?$",
                    line.strip(),
                )
            )
        }
        if partitions:
            return len(partitions)
    except Exception:
        pass

    return 3


ORDER_PARTITIONS = detect_order_partitions()


def normalize_host(url: str) -> str:
    parsed = urlsplit(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid ORDER_URL in urls.json: {url}")
    # Locust host must not include path components.
    return f"{parsed.scheme}://{parsed.netloc}"


def classify_checkout_response(status_code: int, body: str) -> tuple[str, bool]:
    lowered = body.lower()
    if status_code == 503:
        return "/orders/checkout/[order_id] service-unavailable", False
    if status_code == 504:
        return "/orders/checkout/[order_id] gateway-timeout", False
    if status_code == 500:
        return "/orders/checkout/[order_id] internal-server-error", False
    if 500 <= status_code:
        return "/orders/checkout/[order_id] server-error", False
    if 400 <= status_code < 500:
        return "/orders/checkout/[order_id] client-error", True
    if "in progress" in lowered or "retry" in lowered:
        return "/orders/checkout/[order_id] retrying", False
    if "already paid" in lowered:
        return "/orders/checkout/[order_id] already-paid", False
    if "queued" in lowered or "requested asynchronously" in lowered:
        return "/orders/checkout/[order_id] queued", False
    return "/orders/checkout/[order_id] other", False


class CreateAndCheckoutOrder(SequentialTaskSet):
    @task
    def user_checks_out_order(self):
        # Batch init stores orders as s<partition>_<index>, so Locust must target the same shape.
        order_partition = random.randint(0, ORDER_PARTITIONS - 1)
        order_index = random.randint(0, NUMBER_OF_ORDERS - 1)
        order_id = f"s{order_partition}_{order_index}"
        with self.client.post(
            f"/orders/checkout/{order_id}",
            name="/orders/checkout/[order_id]",
            catch_response=True,
        ) as response:
            request_name, is_failure = classify_checkout_response(response.status_code, response.text)
            response.request_meta["name"] = request_name
            if is_failure:
                response.failure(response.text)
            else:
                response.success()

class MicroservicesUser(HttpUser):
    # Keep host sourced from urls.json so CLI/UI host misconfiguration is less likely.
    host = normalize_host(ORDER_URL)
    wait_time = constant(0)
    tasks = {
        CreateAndCheckoutOrder: 100
    }
