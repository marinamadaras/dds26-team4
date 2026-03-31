"""
Integration tests for the full checkout saga.

These tests hit the real services via the gateway and verify that:
- stock quantity is reduced on success
- user credit is deducted on success
- on cancellation, rollback restores the original stock and credit

Requirements: all services must be running (docker-compose up)
"""

import os
import requests
import time
import pytest

GATEWAY = os.getenv("TEST_GATEWAY", "http://127.0.0.1:8000")

# How long to wait for the async saga to complete
SAGA_TIMEOUT = 15      # seconds total
SAGA_POLL_INTERVAL = 0.5  # how often to check

def wait_for_services(timeout: int = 30):
    """Wait until the gateway is reachable before running tests."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(f"{GATEWAY}/orders/find/healthcheck", timeout=2)
            # any response (even 400) means the gateway is up
            return
        except requests.exceptions.ConnectionError:
            time.sleep(1)
    raise RuntimeError("Services did not become ready in time")


# call it at module level so it runs before any test
wait_for_services()
# ══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ══════════════════════════════════════════════════════════════════════════════

def create_user(credit: int) -> str:
    resp = requests.post(f"{GATEWAY}/payment/create_user")
    resp.raise_for_status()
    user_id = resp.json()["user_id"]

    resp = requests.post(f"{GATEWAY}/payment/add_funds/{user_id}/{credit}")
    resp.raise_for_status()
    return user_id


def get_user_credit(user_id: str) -> int:
    resp = requests.get(f"{GATEWAY}/payment/find_user/{user_id}")
    resp.raise_for_status()
    return resp.json()["credit"]


def create_item(price: int, stock: int) -> str:
    resp = requests.post(f"{GATEWAY}/stock/item/create/{price}")
    resp.raise_for_status()
    item_id = resp.json()["item_id"]

    resp = requests.post(f"{GATEWAY}/stock/add/{item_id}/{stock}")
    resp.raise_for_status()
    return item_id


def get_item_stock(item_id: str) -> int:
    resp = requests.get(f"{GATEWAY}/stock/find/{item_id}")
    resp.raise_for_status()
    return resp.json()["stock"]


def create_order(user_id: str) -> str:
    resp = requests.post(f"{GATEWAY}/orders/create/{user_id}")
    resp.raise_for_status()
    return resp.json()["order_id"]


def add_item_to_order(order_id: str, item_id: str, quantity: int):
    resp = requests.post(f"{GATEWAY}/orders/addItem/{order_id}/{item_id}/{quantity}")
    resp.raise_for_status()


def checkout(order_id: str):
    resp = requests.post(f"{GATEWAY}/orders/checkout/{order_id}")
    resp.raise_for_status()


def get_order_status(order_id: str) -> str:
    resp = requests.get(f"{GATEWAY}/orders/find/{order_id}")
    resp.raise_for_status()
    print(f"DEBUG: {resp.json()}")
    return resp.json()["status"]


def wait_for_order_status(order_id: str, expected_status: str, timeout: int = SAGA_TIMEOUT) -> str:
    """Poll until order reaches expected status or timeout."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = get_order_status(order_id)
        print(f"  polling order {order_id}: status={status}")
        if status == expected_status:
            return status
        if status == "cancelled":
            return status  # no point waiting further
        time.sleep(SAGA_POLL_INTERVAL)
    return get_order_status(order_id)  # return whatever it is after timeout

def wait_for_item_in_order(order_id: str, item_id: str, timeout: int = SAGA_TIMEOUT) -> bool:
    """Wait until the find_stock reply has updated the order with the item."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(f"{GATEWAY}/orders/find/{order_id}")
        data = resp.json()
        items = [i[0] for i in data.get("items", [])]
        if item_id in items:
            return True
        time.sleep(SAGA_POLL_INTERVAL)
    return False
# ══════════════════════════════════════════════════════════════════════════════
#  Tests
# ══════════════════════════════════════════════════════════════════════════════

class TestIntegrationCheckout:

    def test_stock_is_reduced_after_checkout(self):
        """After a successful checkout, the item stock must decrease."""
        # Arrange
        item_id = create_item(price=10, stock=5)
        user_id = create_user(credit=100)
        order_id = create_order(user_id)
        add_item_to_order(order_id, item_id, 2)

        assert wait_for_item_in_order(order_id, item_id), "Item never appeared in order"

        # Act
        checkout(order_id)
        status = wait_for_order_status(order_id, "saga_end")

        # Assert
        assert status == "saga_end", f"Order did not complete, status={status}"
        assert get_item_stock(item_id) == 3  # 5 - 2

    def test_credit_is_deducted_after_checkout(self):
        """After a successful checkout, the user credit must decrease."""
        # Arrange
        item_id = create_item(price=20, stock=10)
        user_id = create_user(credit=200)
        order_id = create_order(user_id)
        add_item_to_order(order_id, item_id, 3)

        assert wait_for_item_in_order(order_id, item_id), "Item never appeared in order"

        # Act
        checkout(order_id)
        status = wait_for_order_status(order_id, "saga_end")

        # Assert
        assert status == "saga_end", f"Order did not complete, status={status}"
        assert get_user_credit(user_id) == 140  # 200 - (3 × 20)

    def test_stock_and_credit_both_correct_after_checkout(self):
        """Both stock and credit are correctly updated in the same order."""
        # Arrange
        item_id = create_item(price=15, stock=10)
        user_id = create_user(credit=500)
        order_id = create_order(user_id)
        add_item_to_order(order_id, item_id, 4)

        assert wait_for_item_in_order(order_id, item_id), "Item never appeared in order"

        # Act
        checkout(order_id)
        status = wait_for_order_status(order_id, "saga_end")

        # Assert
        assert status == "saga_end", f"Order did not complete, status={status}"
        assert get_item_stock(item_id) == 6    # 10 - 4
        assert get_user_credit(user_id) == 440  # 500 - (4 × 15)

    def test_multiple_different_items_all_stock_reduced(self):
        """When an order has two different items, both stocks are reduced."""
        # Arrange
        item_a = create_item(price=10, stock=10)
        item_b = create_item(price=25, stock=10)
        user_id = create_user(credit=1000)
        order_id = create_order(user_id)
        add_item_to_order(order_id, item_a, 2)
        add_item_to_order(order_id, item_b, 1)

        assert wait_for_item_in_order(order_id, item_a), "Item A never appeared in order"
        assert wait_for_item_in_order(order_id, item_b), "Item B never appeared in order"

        # Act
        checkout(order_id)
        status = wait_for_order_status(order_id, "saga_end")

        # Assert
        assert status == "saga_end", f"Order did not complete, status={status}"
        assert get_item_stock(item_a) == 8   # 10 - 2
        assert get_item_stock(item_b) == 9   # 10 - 1
        assert get_user_credit(user_id) == 955  # 1000 - (2×10 + 1×25)

    def test_insufficient_credit_cancels_order_and_restores_initial_state(self):
        """If payment fails, rollback must restore the original stock and credit."""
        # Arrange
        item_id = create_item(price=100, stock=5)
        user_id = create_user(credit=10)   # not enough for price=100
        order_id = create_order(user_id)
        add_item_to_order(order_id, item_id, 1)

        assert wait_for_item_in_order(order_id, item_id), "Item never appeared in order"

        # Act
        checkout(order_id)
        status = wait_for_order_status(order_id, "cancelled")

        # Assert
        assert status == "cancelled", f"Expected cancelled, got={status}"
        assert get_item_stock(item_id) == 5
        assert get_user_credit(user_id) == 10

    def test_insufficient_stock_cancels_order_and_restores_initial_state(self):
        """If stock fails, rollback must restore the original stock and credit."""
        # Arrange
        item_id = create_item(price=10, stock=1)
        user_id = create_user(credit=500)
        order_id = create_order(user_id)
        add_item_to_order(order_id, item_id, 5)  # only 1 in stock

        assert wait_for_item_in_order(order_id, item_id), "Item never appeared in order"

        # Act
        checkout(order_id)
        status = wait_for_order_status(order_id, "cancelled")

        # Assert
        assert status == "cancelled", f"Expected cancelled, got={status}"
        assert get_item_stock(item_id) == 1
        assert get_user_credit(user_id) == 500


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
