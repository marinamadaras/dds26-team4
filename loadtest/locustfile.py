import json
import random
from pathlib import Path
from urllib.parse import urlsplit

from locust import HttpUser, SequentialTaskSet, constant, task

from init_orders import NUMBER_OF_ORDERS

URLS_PATH = Path(__file__).resolve().parent / "urls.json"
with open(URLS_PATH) as f:
    urls = json.load(f)
    ORDER_URL = urls['ORDER_URL']


def normalize_host(url: str) -> str:
    parsed = urlsplit(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid ORDER_URL in urls.json: {url}")
    # Locust host must not include path components.
    return f"{parsed.scheme}://{parsed.netloc}"


class CreateAndCheckoutOrder(SequentialTaskSet):
    @task
    def user_checks_out_order(self):
        # Load profile focuses on checkout contention against pre-seeded orders.
        order_id = random.randint(0, NUMBER_OF_ORDERS - 1)
        with self.client.post(
            f"/orders/checkout/{order_id}",
            name="/orders/checkout/[order_id]",
            catch_response=True,
        ) as response:
            if 400 <= response.status_code < 500:
                response.failure(response.text)
            else:
                response.success()

class MicroservicesUser(HttpUser):
    # Keep host sourced from urls.json so CLI/UI host misconfiguration is less likely.
    host = normalize_host(ORDER_URL)
    wait_time = constant(1)
    tasks = {
        CreateAndCheckoutOrder: 100
    }
