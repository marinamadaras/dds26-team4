#!/usr/bin/env python3
"""
Quick regression checks for the 2PC workflow.

This script is intentionally small and direct. It hits the public gateway on
http://127.0.0.1:8000 and verifies the subcases we manually tested:

1. A single order with multiple different items commits correctly.
2. Two separate orders for the same user both commit correctly.
3. Insufficient user credit does not commit the order.
4. Insufficient stock does not commit the order.
5. Repeated checkout on the same order is idempotent from the client's view.
6. Two orders competing for the same scarce item only allow one winner.

Run it after the stack is up in 2PC mode:
    ./.venv/bin/python scripts/verify_2pc_subcases.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "test"))

import utils as tu  # noqa: E402


def wait_until_paid(order_id: str, timeout_s: float = 15.0) -> dict:
    """Poll an order until it becomes paid or the timeout expires."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        order = tu.find_order(order_id)
        if order.get("paid"):
            return order
        time.sleep(0.25)
    return tu.find_order(order_id)


def wait_briefly(seconds: float = 3.0) -> None:
    """Give async checkout time to settle for failure-path checks."""
    time.sleep(seconds)


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def case_multi_item_single_order() -> dict:
    # One order contains two different items. Both stocks should drop and the
    # user should be charged the combined total.
    user_id = tu.create_user()["user_id"]
    tu.add_credit_to_user(user_id, 200)

    item_a = tu.create_item(10)["item_id"]
    item_b = tu.create_item(7)["item_id"]
    tu.add_stock(item_a, 5)
    tu.add_stock(item_b, 6)

    order_id = tu.create_order(user_id)["order_id"]
    tu.add_item_to_order(order_id, item_a, 2)
    tu.add_item_to_order(order_id, item_b, 3)

    checkout = tu.checkout_order(order_id)
    final_order = wait_until_paid(order_id)
    final_user = tu.find_user(user_id)
    final_item_a = tu.find_item(item_a)
    final_item_b = tu.find_item(item_b)

    check(checkout.status_code == 202, "multi-item order should queue checkout")
    check(final_order["paid"] is True, "multi-item order should end as paid")
    check(final_user["credit"] == 159, "multi-item order should deduct 41 credit")
    check(final_item_a["stock"] == 3, "item A stock should go from 5 to 3")
    check(final_item_b["stock"] == 3, "item B stock should go from 6 to 3")

    return {
        "order_id": order_id,
        "checkout_status": checkout.status_code,
        "final_order": final_order,
        "final_user": final_user,
        "final_item_a": final_item_a,
        "final_item_b": final_item_b,
    }


def case_two_orders_same_user() -> dict:
    # The same user places two separate orders. Both should commit and the
    # final credit should reflect the sum of both purchases.
    user_id = tu.create_user()["user_id"]
    tu.add_credit_to_user(user_id, 100)

    item_a = tu.create_item(8)["item_id"]
    item_b = tu.create_item(9)["item_id"]
    tu.add_stock(item_a, 10)
    tu.add_stock(item_b, 10)

    order_a = tu.create_order(user_id)["order_id"]
    order_b = tu.create_order(user_id)["order_id"]
    tu.add_item_to_order(order_a, item_a, 4)
    tu.add_item_to_order(order_b, item_b, 3)

    checkout_a = tu.checkout_order(order_a)
    checkout_b = tu.checkout_order(order_b)
    final_order_a = wait_until_paid(order_a)
    final_order_b = wait_until_paid(order_b)
    final_user = tu.find_user(user_id)

    check(checkout_a.status_code == 202, "first order should queue checkout")
    check(checkout_b.status_code == 202, "second order should queue checkout")
    check(final_order_a["paid"] is True, "first order should commit")
    check(final_order_b["paid"] is True, "second order should commit")
    check(final_user["credit"] == 41, "user credit should be 100 - 32 - 27")

    return {
        "order_a": final_order_a,
        "order_b": final_order_b,
        "final_user": final_user,
        "final_item_a": tu.find_item(item_a),
        "final_item_b": tu.find_item(item_b),
    }


def case_insufficient_credit() -> dict:
    # The order is valid structurally, but the user cannot afford it. The order
    # must remain unpaid and neither credit nor stock should change.
    user_id = tu.create_user()["user_id"]
    tu.add_credit_to_user(user_id, 5)

    item_id = tu.create_item(20)["item_id"]
    tu.add_stock(item_id, 4)

    order_id = tu.create_order(user_id)["order_id"]
    tu.add_item_to_order(order_id, item_id, 1)

    checkout = tu.checkout_order(order_id)
    wait_briefly()
    final_order = tu.find_order(order_id)
    final_user = tu.find_user(user_id)
    final_item = tu.find_item(item_id)

    check(checkout.status_code == 202, "insufficient-credit checkout is still async")
    check(final_order["paid"] is False, "insufficient-credit order must stay unpaid")
    check(final_user["credit"] == 5, "credit must remain unchanged on failure")
    check(final_item["stock"] == 4, "stock must remain unchanged on failure")

    return {
        "order": final_order,
        "user": final_user,
        "item": final_item,
    }


def case_insufficient_stock() -> dict:
    # The user can pay, but the inventory is too small. The order must remain
    # unpaid and nothing should be deducted.
    user_id = tu.create_user()["user_id"]
    tu.add_credit_to_user(user_id, 100)

    item_id = tu.create_item(11)["item_id"]
    tu.add_stock(item_id, 1)

    order_id = tu.create_order(user_id)["order_id"]
    tu.add_item_to_order(order_id, item_id, 2)

    checkout = tu.checkout_order(order_id)
    wait_briefly()
    final_order = tu.find_order(order_id)
    final_user = tu.find_user(user_id)
    final_item = tu.find_item(item_id)

    check(checkout.status_code == 202, "insufficient-stock checkout is still async")
    check(final_order["paid"] is False, "insufficient-stock order must stay unpaid")
    check(final_user["credit"] == 100, "credit must remain unchanged on failure")
    check(final_item["stock"] == 1, "stock must remain unchanged on failure")

    return {
        "order": final_order,
        "user": final_user,
        "item": final_item,
    }


def case_repeat_checkout_same_order() -> dict:
    # Repeated checkout calls should behave sensibly:
    # - first call queues work
    # - second call while still running reports in-progress
    # - later calls after success report already-paid
    user_id = tu.create_user()["user_id"]
    tu.add_credit_to_user(user_id, 100)

    item_id = tu.create_item(13)["item_id"]
    tu.add_stock(item_id, 5)

    order_id = tu.create_order(user_id)["order_id"]
    tu.add_item_to_order(order_id, item_id, 2)

    first = tu.checkout_order(order_id)
    second = tu.checkout_order(order_id)
    final_order = wait_until_paid(order_id)
    third = tu.checkout_order(order_id)

    check(first.status_code == 202, "first checkout should queue")
    check("in progress" in second.text.lower(), "second checkout should report in-progress")
    check(final_order["paid"] is True, "repeated checkout case should still commit once")
    check(third.status_code == 200, "post-commit checkout should return already-paid")

    return {
        "first_status": first.status_code,
        "first_body": first.text,
        "second_status": second.status_code,
        "second_body": second.text,
        "third_status": third.status_code,
        "third_body": third.text,
        "final_order": final_order,
        "final_user": tu.find_user(user_id),
        "final_item": tu.find_item(item_id),
    }


def case_contended_stock_two_orders() -> dict:
    # Two users race to buy more units than exist. Exactly one order should win,
    # stock should only be deducted once, and only the winning user should pay.
    user_a = tu.create_user()["user_id"]
    user_b = tu.create_user()["user_id"]
    tu.add_credit_to_user(user_a, 100)
    tu.add_credit_to_user(user_b, 100)

    item_id = tu.create_item(15)["item_id"]
    tu.add_stock(item_id, 3)

    order_a = tu.create_order(user_a)["order_id"]
    order_b = tu.create_order(user_b)["order_id"]
    tu.add_item_to_order(order_a, item_id, 2)
    tu.add_item_to_order(order_b, item_id, 2)

    checkout_a = tu.checkout_order(order_a)
    checkout_b = tu.checkout_order(order_b)
    wait_briefly(5.0)

    final_order_a = tu.find_order(order_a)
    final_order_b = tu.find_order(order_b)
    final_user_a = tu.find_user(user_a)
    final_user_b = tu.find_user(user_b)
    final_item = tu.find_item(item_id)

    winners = [final_order_a["paid"], final_order_b["paid"]]
    check(checkout_a.status_code == 202, "contended checkout A should queue")
    check(checkout_b.status_code == 202, "contended checkout B should queue")
    check(sum(bool(x) for x in winners) == 1, "exactly one contended order should commit")
    check(final_item["stock"] == 1, "shared stock should drop from 3 to 1 exactly once")

    return {
        "order_a": final_order_a,
        "order_b": final_order_b,
        "user_a": final_user_a,
        "user_b": final_user_b,
        "item": final_item,
    }


def main() -> int:
    cases = [
        ("multi_item_single_order", case_multi_item_single_order),
        ("two_orders_same_user", case_two_orders_same_user),
        ("insufficient_credit", case_insufficient_credit),
        ("insufficient_stock", case_insufficient_stock),
        ("repeat_checkout_same_order", case_repeat_checkout_same_order),
        ("contended_stock_two_orders", case_contended_stock_two_orders),
    ]

    results: dict[str, dict] = {}
    failures: list[str] = []

    for name, fn in cases:
        try:
            results[name] = {"ok": True, "details": fn()}
            print(f"[PASS] {name}")
        except Exception as exc:
            results[name] = {"ok": False, "error": str(exc)}
            failures.append(name)
            print(f"[FAIL] {name}: {exc}")

    print()
    print(json.dumps(results, indent=2, sort_keys=True))

    if failures:
        print()
        print("Failed cases:", ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
