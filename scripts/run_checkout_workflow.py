#!/usr/bin/env python3
"""
Run one happy-path checkout through the public gateway.

This script works against either stack mode:
- Saga/app: order reaches status == "saga_end"
- 2PC: order reaches paid == True

Example:
    ./.venv/bin/python scripts/run_checkout_workflow.py
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one checkout workflow through the gateway.")
    parser.add_argument("--gateway", default="http://127.0.0.1:8000")
    parser.add_argument("--price", type=int, default=15)
    parser.add_argument("--stock", type=int, default=5)
    parser.add_argument("--credit", type=int, default=100)
    parser.add_argument("--quantity", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--json", action="store_true", help="Print the final result as JSON.")
    parser.add_argument("--verbose", action="store_true", help="Log each poll while waiting for checkout.")
    return parser.parse_args()


def request_json(session: requests.Session, method: str, url: str) -> tuple[int, object]:
    response = session.request(method, url, timeout=10)
    response.raise_for_status()
    if "application/json" in response.headers.get("Content-Type", ""):
        return response.status_code, response.json()
    return response.status_code, response.text


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def summarize_order(order: dict) -> str:
    if "paid" in order:
        return (
            f"2pc order_id={order.get('order_id')} paid={order.get('paid')} "
            f"total_cost={order.get('total_cost')} items={order.get('items')}"
        )
    if "status" in order:
        return (
            f"saga order_id={order.get('order_id')} status={order.get('status')} "
            f"total_cost={order.get('total_cost')} items={order.get('items')}"
        )
    return repr(order)


def print_human_summary(
    mode: str,
    success: bool,
    user_id: str,
    item_id: str,
    order_id: str,
    price: int,
    quantity: int,
    initial_credit: int,
    initial_stock: int,
    final_order: dict,
    final_item: dict,
    final_user: dict,
    expected_stock: int,
    expected_credit: int,
) -> None:
    print("Checkout workflow")
    print(f"Mode: {mode}")
    print(f"Result: {'SUCCESS' if success else 'FAILURE'}")
    print()
    print("Entities")
    print(f"User: {user_id}")
    print(f"Item: {item_id}")
    print(f"Order: {order_id}")
    print()
    print("Before")
    print(f"User credit: {initial_credit}")
    print(f"Item stock: {initial_stock}")
    print(f"Order state: created, not checked out")
    print()
    print("Requested checkout")
    print(f"Item price: {price}")
    print(f"Quantity: {quantity}")
    print(f"Expected total cost: {price * quantity}")
    print()
    print("After")
    if mode == "2pc":
        print(f"Order paid: {final_order.get('paid')}")
    else:
        print(f"Order status: {final_order.get('status')}")
    print(f"Order total cost: {final_order.get('total_cost')}")
    print(f"Order items: {final_order.get('items')}")
    print(f"User credit: {final_user.get('credit')} (expected {expected_credit})")
    print(f"Item stock: {final_item.get('stock')} (expected {expected_stock})")


def wait_for_terminal_order(
    session: requests.Session,
    gateway: str,
    order_id: str,
    timeout_s: float,
    poll_interval_s: float,
    verbose: bool,
) -> tuple[str, dict]:
    deadline = time.time() + timeout_s
    mode = "unknown"
    last_order: dict = {}
    last_summary = None

    while time.time() < deadline:
        _, body = request_json(session, "GET", f"{gateway}/orders/find/{order_id}")
        if not isinstance(body, dict):
            raise RuntimeError(f"Unexpected order response: {body!r}")
        last_order = body
        summary = summarize_order(body)
        if verbose or summary != last_summary:
            log(f"[poll] {summary}")
            last_summary = summary

        if "paid" in body:
            mode = "2pc"
            if body.get("paid") is True:
                return mode, body
        elif "status" in body:
            mode = "saga"
            if body.get("status") in {"saga_end", "cancelled"}:
                return mode, body

        time.sleep(poll_interval_s)

    return mode, last_order


def main() -> int:
    args = parse_args()
    session = requests.Session()
    gateway = args.gateway.rstrip("/")
    initial_credit = args.credit
    initial_stock = args.stock

    _, user_body = request_json(session, "POST", f"{gateway}/payment/create_user")
    user_id = user_body["user_id"]
    log(f"[1/7] Created user {user_id}")

    request_json(session, "POST", f"{gateway}/payment/add_funds/{user_id}/{args.credit}")
    log(f"[2/7] Added credit={args.credit} to user {user_id}")

    _, item_body = request_json(session, "POST", f"{gateway}/stock/item/create/{args.price}")
    item_id = item_body["item_id"]
    log(f"[3/7] Created item {item_id} with price={args.price}")

    request_json(session, "POST", f"{gateway}/stock/add/{item_id}/{args.stock}")
    log(f"[4/7] Added stock={args.stock} to item {item_id}")

    _, order_body = request_json(session, "POST", f"{gateway}/orders/create/{user_id}")
    order_id = order_body["order_id"]
    log(f"[5/7] Created order {order_id} for user {user_id}")

    add_item_status, add_item_body = request_json(
        session,
        "POST",
        f"{gateway}/orders/addItem/{order_id}/{item_id}/{args.quantity}",
    )
    log(
        f"[6/7] Added quantity={args.quantity} of item {item_id} to order {order_id} "
        f"(status={add_item_status})"
    )

    checkout_status, checkout_body = request_json(
        session,
        "POST",
        f"{gateway}/orders/checkout/{order_id}",
    )
    log(f"[7/7] Checkout requested for order {order_id} (status={checkout_status})")
    log("[wait] Polling order state until it reaches a terminal result")

    mode, final_order = wait_for_terminal_order(
        session,
        gateway,
        order_id,
        args.timeout,
        args.poll_interval,
        args.verbose,
    )

    _, final_item = request_json(session, "GET", f"{gateway}/stock/find/{item_id}")
    _, final_user = request_json(session, "GET", f"{gateway}/payment/find_user/{user_id}")

    expected_stock = args.stock - args.quantity
    expected_credit = args.credit - (args.price * args.quantity)

    if mode == "2pc":
        success = (
            final_order.get("paid") is True
            and final_item.get("stock") == expected_stock
            and final_user.get("credit") == expected_credit
        )
    elif mode == "saga":
        success = (
            final_order.get("status") == "saga_end"
            and final_item.get("stock") == expected_stock
            and final_user.get("credit") == expected_credit
        )
    else:
        success = False

    log(f"[done] mode={mode} success={success}")
    log(f"[done] final order: {summarize_order(final_order)}")
    log(
        f"[done] final item stock={final_item.get('stock')} expected_stock={expected_stock} "
        f"final user credit={final_user.get('credit')} expected_credit={expected_credit}"
    )

    result = {
        "mode": mode,
        "success": success,
        "user_id": user_id,
        "item_id": item_id,
        "order_id": order_id,
        "add_item_status": add_item_status,
        "add_item_body": add_item_body,
        "checkout_status": checkout_status,
        "checkout_body": checkout_body,
        "final_order": final_order,
        "final_item": final_item,
        "final_user": final_user,
        "expected_stock": expected_stock,
        "expected_credit": expected_credit,
    }
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print_human_summary(
            mode=mode,
            success=success,
            user_id=user_id,
            item_id=item_id,
            order_id=order_id,
            price=args.price,
            quantity=args.quantity,
            initial_credit=initial_credit,
            initial_stock=initial_stock,
            final_order=final_order,
            final_item=final_item,
            final_user=final_user,
            expected_stock=expected_stock,
            expected_credit=expected_credit,
        )
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
