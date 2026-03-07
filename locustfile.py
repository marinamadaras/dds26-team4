import random
from collections import deque
from threading import Lock

from locust import HttpUser, between, task


RECENT_ORDER_IDS = deque(maxlen=5000)
RECENT_ITEM_IDS = deque(maxlen=5000)
RECENT_LOCK = Lock()


def _extract_id(resp, field: str) -> str | None:
    if resp.status_code < 200 or resp.status_code >= 300:
        return None
    try:
        data = resp.json()
    except Exception:
        return None
    value = data.get(field)
    return str(value) if value is not None else None


class CheckoutUser(HttpUser):
    wait_time = between(0.1, 0.8)

    @task(7)
    def full_checkout_flow(self) -> None:
        user_resp = self.client.post("/payment/create_user", name="/payment/create_user")
        user_id = _extract_id(user_resp, "user_id")
        if not user_id:
            return

        self.client.post(f"/payment/add_funds/{user_id}/1000", name="/payment/add_funds/[user]/[amount]")

        price = random.randint(1, 50)
        item_resp = self.client.post(f"/stock/item/create/{price}", name="/stock/item/create/[price]")
        item_id = _extract_id(item_resp, "item_id")
        if not item_id:
            return

        stock_amount = random.randint(20, 100)
        self.client.post(
            f"/stock/add/{item_id}/{stock_amount}",
            name="/stock/add/[item]/[amount]",
        )

        order_resp = self.client.post(f"/orders/create/{user_id}", name="/orders/create/[user]")
        order_id = _extract_id(order_resp, "order_id")
        if not order_id:
            return

        qty = random.randint(1, 3)
        self.client.post(
            f"/orders/addItem/{order_id}/{item_id}/{qty}",
            name="/orders/addItem/[order]/[item]/[qty]",
        )

        with self.client.post(
            f"/orders/checkout/{order_id}",
            name="/orders/checkout/[order]",
            catch_response=True,
        ) as checkout_resp:
            if 200 <= checkout_resp.status_code < 300:
                checkout_resp.success()
            elif 400 <= checkout_resp.status_code < 500:
                # Business-level failures are expected under load (e.g. insufficient funds/stock).
                checkout_resp.success()
            else:
                checkout_resp.failure(f"Unexpected status {checkout_resp.status_code}")

        with RECENT_LOCK:
            RECENT_ORDER_IDS.append(order_id)
            RECENT_ITEM_IDS.append(item_id)

    @task(2)
    def read_recent_order(self) -> None:
        with RECENT_LOCK:
            order_id = random.choice(RECENT_ORDER_IDS) if RECENT_ORDER_IDS else None
        if not order_id:
            return
        self.client.get(f"/orders/find/{order_id}", name="/orders/find/[order]")

    @task(1)
    def read_recent_item(self) -> None:
        with RECENT_LOCK:
            item_id = random.choice(RECENT_ITEM_IDS) if RECENT_ITEM_IDS else None
        if not item_id:
            return
        self.client.get(f"/stock/find/{item_id}", name="/stock/find/[item]")
