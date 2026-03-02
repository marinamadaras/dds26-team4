import requests
import json
import subprocess
import time
from pathlib import Path
from uuid import uuid4

ORDER_URL = STOCK_URL = PAYMENT_URL = "http://127.0.0.1:8000"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


########################################################################################################################
#   STOCK MICROSERVICE FUNCTIONS
########################################################################################################################
def create_item(price: int) -> dict:
    return requests.post(f"{STOCK_URL}/stock/item/create/{price}").json()


def find_item(item_id: str) -> dict:
    return requests.get(f"{STOCK_URL}/stock/find/{item_id}").json()


def add_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/add/{item_id}/{amount}").status_code


def subtract_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/subtract/{item_id}/{amount}").status_code


def stock_prepare(tx_id: str, items: list[tuple[str, int]]) -> requests.Response:
    payload = {"items": [[item_id, quantity] for item_id, quantity in items]}
    return requests.post(f"{STOCK_URL}/stock/prepare/{tx_id}", json=payload)


def stock_commit(tx_id: str) -> requests.Response:
    return requests.post(f"{STOCK_URL}/stock/commit/{tx_id}")


def stock_abort(tx_id: str) -> requests.Response:
    return requests.post(f"{STOCK_URL}/stock/abort/{tx_id}")


########################################################################################################################
#   PAYMENT MICROSERVICE FUNCTIONS
########################################################################################################################
def payment_pay(user_id: str, amount: int) -> int:
    return requests.post(f"{PAYMENT_URL}/payment/pay/{user_id}/{amount}").status_code


def create_user() -> dict:
    return requests.post(f"{PAYMENT_URL}/payment/create_user").json()


def find_user(user_id: str) -> dict:
    return requests.get(f"{PAYMENT_URL}/payment/find_user/{user_id}").json()


def add_credit_to_user(user_id: str, amount: float) -> int:
    return requests.post(f"{PAYMENT_URL}/payment/add_funds/{user_id}/{amount}").status_code


def payment_prepare(tx_id: str, user_id: str, amount: int) -> requests.Response:
    payload = {"user_id": user_id, "amount": amount}
    return requests.post(f"{PAYMENT_URL}/payment/prepare/{tx_id}", json=payload)


def payment_commit(tx_id: str) -> requests.Response:
    return requests.post(f"{PAYMENT_URL}/payment/commit/{tx_id}")


def payment_abort(tx_id: str) -> requests.Response:
    return requests.post(f"{PAYMENT_URL}/payment/abort/{tx_id}")


########################################################################################################################
#   ORDER MICROSERVICE FUNCTIONS
########################################################################################################################
def create_order(user_id: str) -> dict:
    return requests.post(f"{ORDER_URL}/orders/create/{user_id}").json()


def add_item_to_order(order_id: str, item_id: str, quantity: int) -> int:
    return requests.post(f"{ORDER_URL}/orders/addItem/{order_id}/{item_id}/{quantity}").status_code


def find_order(order_id: str) -> dict:
    return requests.get(f"{ORDER_URL}/orders/find/{order_id}").json()


def checkout_order(order_id: str) -> requests.Response:
    return requests.post(f"{ORDER_URL}/orders/checkout/{order_id}")


def seed_order_transaction(
    tx_id: str,
    state: str,
    decision: str,
    order_id: str,
    user_id: str,
    total_cost: int,
    items: list[tuple[str, int]],
):
    items_json = json.dumps([[item_id, quantity] for item_id, quantity in items])
    script = f"""
import json
from app import db, CheckoutTransaction, msgpack
items = [tuple(pair) for pair in json.loads({items_json!r})]
tx = CheckoutTransaction(
    state={state!r},
    decision={decision!r},
    order_id={order_id!r},
    user_id={user_id!r},
    total_cost={total_cost},
    items=items,
)
db.set(f"tx:{tx_id}", msgpack.encode(tx))
"""
    subprocess.run(
        ["docker", "compose", "exec", "-T", "order-service", "python", "-c", script],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def restart_order_service():
    subprocess.run(
        ["docker", "compose", "restart", "order-service"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    wait_for_order_service()


def wait_for_order_service(timeout_s: int = 30):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            resp = requests.post(f"{ORDER_URL}/orders/create/{uuid4()}")
            if resp.status_code == 200:
                return
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.5)
    raise TimeoutError("order-service did not become ready in time")


########################################################################################################################
#   STATUS CHECKS
########################################################################################################################
def status_code_is_success(status_code: int) -> bool:
    return 200 <= status_code < 300


def status_code_is_failure(status_code: int) -> bool:
    return 400 <= status_code < 500
