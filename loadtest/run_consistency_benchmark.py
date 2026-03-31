#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(asctime)s - %(name)s - %(message)s",
    datefmt="%I:%M:%S",
)
logger = logging.getLogger("consistency-benchmark")

URLS_PATH = Path(__file__).resolve().parent / "urls.json"
with open(URLS_PATH) as f:
    urls = json.load(f)
    ORDER_URL = urls["ORDER_URL"].rstrip("/")
    PAYMENT_URL = urls["PAYMENT_URL"].rstrip("/")
    STOCK_URL = urls["STOCK_URL"].rstrip("/")


@dataclass
class BenchmarkConfig:
    users: int
    stock: int
    price: int
    item_prices: list[int]
    credit: int
    quantity: int
    request_timeout_s: float
    settle_timeout_s: float
    poll_interval_s: float
    parallelism: int
    report_file: str | None
    json_output: bool


class BenchmarkSetupError(RuntimeError):
    pass


@dataclass
class TerminalOrder:
    order_id: str
    outcome: str
    observed_credit: int


@dataclass
class SharedItem:
    item_id: str
    price: int


@dataclass
class OrderPlan:
    order_id: str
    user_id: str
    item_id: str
    price: int
    total_cost: int


class BenchmarkClient:
    def __init__(self, session: aiohttp.ClientSession, parallelism: int):
        self.session = session
        self.semaphore = asyncio.Semaphore(parallelism)

    async def request_json(self, method: str, url: str, ok_statuses: set[int]) -> dict[str, Any]:
        async with self.semaphore:
            async with self.session.request(method, url) as response:
                text = await response.text()
                if response.status not in ok_statuses:
                    raise RuntimeError(f"{method} {url} -> {response.status}: {text}")
                try:
                    return json.loads(text)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(f"{method} {url} returned non-JSON body: {text}") from exc

    async def request_status(self, method: str, url: str, ok_statuses: set[int]) -> int:
        async with self.semaphore:
            async with self.session.request(method, url) as response:
                text = await response.text()
                if response.status not in ok_statuses:
                    raise RuntimeError(f"{method} {url} -> {response.status}: {text}")
                return response.status

    async def request_status_any(self, method: str, url: str) -> int:
        async with self.semaphore:
            async with self.session.request(method, url) as response:
                await response.text()
                return response.status


async def create_users(client: BenchmarkClient, config: BenchmarkConfig) -> list[str]:
    logger.info("Creating %s users", config.users)
    create_tasks = [
        client.request_json("POST", f"{PAYMENT_URL}/payment/create_user", {200})
        for _ in range(config.users)
    ]
    created = await asyncio.gather(*create_tasks)
    user_ids = [entry["user_id"] for entry in created]

    logger.info("Funding users with %s credit each", config.credit)
    fund_tasks = [
        client.request_status("POST", f"{PAYMENT_URL}/payment/add_funds/{user_id}/{config.credit}", {200})
        for user_id in user_ids
    ]
    await asyncio.gather(*fund_tasks)
    return user_ids


async def create_shared_items(client: BenchmarkClient, config: BenchmarkConfig) -> list[SharedItem]:
    logger.info(
        "Creating %s shared item(s) with stock=%s and prices=%s",
        len(config.item_prices),
        config.stock,
        config.item_prices,
    )
    created = await asyncio.gather(
        *[
            client.request_json("POST", f"{STOCK_URL}/stock/item/create/{price}", {200})
            for price in config.item_prices
        ]
    )
    item_ids = [entry["item_id"] for entry in created]
    await asyncio.gather(
        *[
            client.request_status("POST", f"{STOCK_URL}/stock/add/{item_id}/{config.stock}", {200})
            for item_id in item_ids
        ]
    )
    return [SharedItem(item_id=item_id, price=price) for item_id, price in zip(item_ids, config.item_prices)]


async def create_orders(client: BenchmarkClient, user_ids: list[str]) -> list[str]:
    logger.info("Creating %s orders", len(user_ids))
    order_tasks = [
        client.request_json("POST", f"{ORDER_URL}/orders/create/{user_id}", {200})
        for user_id in user_ids
    ]
    created = await asyncio.gather(*order_tasks)
    return [entry["order_id"] for entry in created]


async def wait_for_item_on_order(
    client: BenchmarkClient,
    order_id: str,
    item_id: str,
    expected_total_cost: int,
    config: BenchmarkConfig,
) -> None:
    deadline = asyncio.get_running_loop().time() + config.settle_timeout_s
    while asyncio.get_running_loop().time() < deadline:
        order = await client.request_json("GET", f"{ORDER_URL}/orders/find/{order_id}", {200})
        item_ids = [item[0] for item in order.get("items", [])]
        if item_id in item_ids and order.get("total_cost") == expected_total_cost:
            return
        await asyncio.sleep(config.poll_interval_s)
    raise BenchmarkSetupError(
        "Setup failure: addItem did not settle before timeout "
        f"(order_id={order_id}, item_id={item_id}, expected_total_cost={expected_total_cost})"
    )


async def attach_items_to_orders(
    client: BenchmarkClient,
    order_ids: list[str],
    user_ids: list[str],
    items: list[SharedItem],
    config: BenchmarkConfig,
) -> list[OrderPlan]:
    logger.info("Attaching shared items to all orders")
    order_plans = [
        OrderPlan(
            order_id=order_id,
            user_id=user_id,
            item_id=items[index % len(items)].item_id,
            price=items[index % len(items)].price,
            total_cost=items[index % len(items)].price * config.quantity,
        )
        for index, (order_id, user_id) in enumerate(zip(order_ids, user_ids))
    ]
    add_tasks = [
        client.request_status(
            "POST",
            f"{ORDER_URL}/orders/addItem/{plan.order_id}/{plan.item_id}/{config.quantity}",
            {200, 202},
        )
        for plan in order_plans
    ]
    await asyncio.gather(*add_tasks)

    wait_tasks = [
        wait_for_item_on_order(client, plan.order_id, plan.item_id, plan.total_cost, config)
        for plan in order_plans
    ]
    await asyncio.gather(*wait_tasks)
    return order_plans


async def perform_concurrent_checkouts(
    client: BenchmarkClient,
    order_plans: list[OrderPlan],
) -> Counter[int]:
    logger.info("Running %s concurrent checkouts", len(order_plans))
    statuses = await asyncio.gather(
        *[
            client.request_status("POST", f"{ORDER_URL}/orders/checkout/{plan.order_id}", {200, 202})
            for plan in order_plans
        ]
    )
    return Counter(statuses)


async def wait_for_terminal_order(
    client: BenchmarkClient,
    order_plan: OrderPlan,
    config: BenchmarkConfig,
) -> TerminalOrder:
    deadline = asyncio.get_running_loop().time() + config.settle_timeout_s
    success_credit = config.credit - order_plan.total_cost
    cancelled_credit = config.credit

    while asyncio.get_running_loop().time() < deadline:
        user = await client.request_json("GET", f"{PAYMENT_URL}/payment/find_user/{order_plan.user_id}", {200})
        credit = int(user["credit"])
        if credit == success_credit:
            return TerminalOrder(order_id=order_plan.order_id, outcome="success", observed_credit=credit)
        if credit == cancelled_credit:
            return TerminalOrder(order_id=order_plan.order_id, outcome="cancelled", observed_credit=credit)

        await asyncio.sleep(config.poll_interval_s)

    user = await client.request_json("GET", f"{PAYMENT_URL}/payment/find_user/{order_plan.user_id}", {200})
    credit = int(user["credit"])
    return TerminalOrder(
        order_id=order_plan.order_id,
        outcome="timeout",
        observed_credit=credit,
    )


async def fetch_total_user_credit(client: BenchmarkClient, user_ids: list[str]) -> int:
    user_entries = await asyncio.gather(
        *[
            client.request_json("GET", f"{PAYMENT_URL}/payment/find_user/{user_id}", {200})
            for user_id in user_ids
        ]
    )
    return sum(entry["credit"] for entry in user_entries)


async def fetch_item_stocks(client: BenchmarkClient, items: list[SharedItem]) -> dict[str, int]:
    stock_entries = await asyncio.gather(
        *[
            client.request_json("GET", f"{STOCK_URL}/stock/find/{item.item_id}", {200})
            for item in items
        ]
    )
    return {item.item_id: entry["stock"] for item, entry in zip(items, stock_entries)}


def parse_args() -> BenchmarkConfig:
    parser = argparse.ArgumentParser(description="Run the shared-item consistency benchmark.")
    parser.add_argument("--users", type=int, default=1000)
    parser.add_argument("--stock", type=int, default=100)
    parser.add_argument("--price", type=int, default=1)
    parser.add_argument(
        "--price-list",
        help="Comma-separated shared item prices, for example 1,2,3. Defaults to the single --price value.",
    )
    parser.add_argument("--credit", type=int, default=1)
    parser.add_argument("--quantity", type=int, default=1)
    parser.add_argument("--request-timeout", type=float, default=10.0)
    parser.add_argument("--settle-timeout", type=float, default=20.0)
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--parallelism", type=int, default=200)
    parser.add_argument("--report-file")
    parser.add_argument("--json", action="store_true", help="Also print the raw JSON report.")
    args = parser.parse_args()
    item_prices = [int(part) for part in args.price_list.split(",")] if args.price_list else [args.price]
    return BenchmarkConfig(
        users=args.users,
        stock=args.stock,
        price=args.price,
        item_prices=item_prices,
        credit=args.credit,
        quantity=args.quantity,
        request_timeout_s=args.request_timeout,
        settle_timeout_s=args.settle_timeout,
        poll_interval_s=args.poll_interval,
        parallelism=args.parallelism,
        report_file=args.report_file,
        json_output=args.json,
    )


def _format_counts(counts: dict[str, int] | dict[int, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items(), key=lambda item: str(item[0])))


def _reported_checkout_status_counts(counts: dict[str | int, int]) -> dict[str | int, int]:
    return {status: count for status, count in counts.items() if status != "socket-timeout"}


def _count_reported_checkout_failures(counts: dict[str | int, int]) -> int:
    return sum(
        count
        for status, count in counts.items()
        if status not in {200, 202, "200", "202", "socket-timeout"}
    )


def print_human_report(report: dict[str, Any]) -> None:
    config = report["config"]
    initial_total_credit = config["users"] * config["credit"]
    total_demand_units = config["users"] * config["quantity"]
    total_demand_cost = report["total_requested_credit"]

    print("Consistency Benchmark")
    print()
    print("Initial state")
    print(f"Users: {config['users']}")
    print(f"Shared items: {len(config['item_prices'])}")
    print(f"Stock per shared item: {config['stock']}")
    print(f"Shared item prices: {', '.join(str(price) for price in config['item_prices'])}")
    print(f"Quantity per order: {config['quantity']}")
    print(f"Credit per user: {config['credit']}")
    print(f"Initial total user credit: {initial_total_credit}")
    print(f"Total requested units if all orders succeeded: {total_demand_units}")
    print(f"Total requested credit if all orders succeeded: {total_demand_cost}")
    print()
    print("Expected outcome")
    print(f"Maximum successful orders by capacity: {report['expected_capacity_successes']}")
    print(f"Expected final stock: {report['expected_stock_from_capacity']}")
    print(f"Expected remaining total credit: {report['expected_credit_from_capacity']}")
    print()
    print("Observed outcome")
    print(f"Checkout HTTP statuses: {_format_counts(report['checkout_status_counts'])}")
    print(f"Terminal outcomes: {_format_counts(report['terminal_outcome_counts'])}")
    print(f"Observed successful orders: {report['observed_successes']}")
    print(f"Observed cancelled orders: {report['observed_cancellations']}")
    print(f"Observed timed out orders: {report['observed_timeouts']}")
    print(f"Orders with unexpected credit after timeout: {report['unexpected_credit_after_timeout_count']}")
    print(f"Observed checkout request failures: {report['actual_checkout_failure_count']}")
    print(
        "Expected final stock from observed successful orders: "
        f"{report['expected_stock_from_observed_successes']}"
    )
    print(
        "Expected remaining total credit from observed successful orders: "
        f"{report['expected_credit_from_observed_successes']}"
    )
    print(f"Final total stock across shared items: {report['final_total_stock']}")
    print(f"Final stock by item: {_format_counts(report['final_stock_by_item'])}")
    print(f"Remaining total user credit: {report['total_remaining_credit']}")
    print()
    print("Core checks")
    for key, value in report["core_checks"].items():
        status = "PASS" if value else "FAIL"
        print(f"{status}: {key}")
    print()
    print("Additional signals")
    for key, value in report["additional_signals"].items():
        status = "PASS" if value else "INFO"
        print(f"{status}: {key}")

    print()
    overall = "PASS" if report["all_passed"] else "FAIL"
    print(f"Overall result: {overall}")


def print_setup_failure(message: str) -> None:
    print("Consistency Benchmark")
    print()
    print("Setup failed")
    print(message)


async def run_benchmark(config: BenchmarkConfig) -> dict[str, Any]:
    timeout = aiohttp.ClientTimeout(total=None, connect=config.request_timeout_s, sock_read=config.request_timeout_s)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = BenchmarkClient(session, parallelism=config.parallelism)

        items = await create_shared_items(client, config)
        user_ids = await create_users(client, config)
        order_ids = await create_orders(client, user_ids)
        order_plans = await attach_items_to_orders(client, order_ids, user_ids, items, config)
        checkout_status_counts = await perform_concurrent_checkouts(client, order_plans)

        logger.info("Waiting for terminal order states")
        terminal_orders = await asyncio.gather(
            *[wait_for_terminal_order(client, order_plan, config) for order_plan in order_plans]
        )

        final_stock_by_item = await fetch_item_stocks(client, items)
        total_remaining_credit = await fetch_total_user_credit(client, user_ids)

    outcome_counts = Counter(entry.outcome for entry in terminal_orders)
    success_count = outcome_counts["success"]
    cancellation_count = outcome_counts["cancelled"]
    timeout_count = outcome_counts["timeout"]
    unexpected_credit_after_timeout_count = sum(
        1
        for entry, order_plan in zip(terminal_orders, order_plans)
        if entry.outcome == "timeout"
        and entry.observed_credit not in {config.credit, config.credit - order_plan.total_cost}
    )
    reported_checkout_status_counts = _reported_checkout_status_counts(dict(checkout_status_counts))
    actual_checkout_failure_count = _count_reported_checkout_failures(reported_checkout_status_counts)

    initial_total_credit = config.users * config.credit
    assigned_orders_per_item = Counter(plan.item_id for plan in order_plans)
    orders_per_price = Counter(plan.price for plan in order_plans)
    expected_capacity_successes = 0
    expected_credit_from_capacity = initial_total_credit
    expected_total_stock_from_capacity = 0
    total_requested_credit = sum(plan.total_cost for plan in order_plans)
    for item in items:
        assigned_count = assigned_orders_per_item[item.item_id]
        affordable_orders = assigned_count if config.credit >= item.price * config.quantity else 0
        successful_orders = min(affordable_orders, config.stock // config.quantity)
        expected_capacity_successes += successful_orders
        expected_credit_from_capacity -= successful_orders * item.price * config.quantity
        expected_total_stock_from_capacity += config.stock - (successful_orders * config.quantity)

    observed_successes_per_item = Counter(
        plan.item_id for plan, terminal in zip(order_plans, terminal_orders) if terminal.outcome == "success"
    )
    expected_credit_from_observed_successes = initial_total_credit - sum(
        observed_successes_per_item[item.item_id] * item.price * config.quantity for item in items
    )
    final_total_stock = sum(final_stock_by_item.values())
    expected_total_stock_from_observed_successes = (len(items) * config.stock) - (success_count * config.quantity)

    report = {
        "config": asdict(config),
        "item_ids": [item.item_id for item in items],
        "orders": len(order_ids),
        "orders_per_price": dict(orders_per_price),
        "checkout_status_counts": reported_checkout_status_counts,
        "terminal_outcome_counts": dict(outcome_counts),
        "total_requested_credit": total_requested_credit,
        "expected_capacity_successes": expected_capacity_successes,
        "observed_successes": success_count,
        "observed_cancellations": cancellation_count,
        "observed_timeouts": timeout_count,
        "unexpected_credit_after_timeout_count": unexpected_credit_after_timeout_count,
        "actual_checkout_failure_count": actual_checkout_failure_count,
        "final_stock_by_item": final_stock_by_item,
        "final_total_stock": final_total_stock,
        "total_remaining_credit": total_remaining_credit,
        "expected_stock_from_capacity": expected_total_stock_from_capacity,
        "expected_credit_from_capacity": expected_credit_from_capacity,
        "expected_stock_from_observed_successes": expected_total_stock_from_observed_successes,
        "expected_credit_from_observed_successes": expected_credit_from_observed_successes,
        "core_checks": {
            "no_actual_checkout_failures": actual_checkout_failure_count == 0,
            "no_timeouts": timeout_count == 0,
            "stock_matches_observed_successes": final_total_stock == expected_total_stock_from_observed_successes,
            "credit_matches_observed_successes": total_remaining_credit == expected_credit_from_observed_successes,
        },
        "additional_signals": {
            "successes_match_capacity": success_count == expected_capacity_successes,
            "stock_matches_capacity": final_total_stock == expected_total_stock_from_capacity,
            "credit_matches_capacity": total_remaining_credit == expected_credit_from_capacity,
        },
    }
    report["all_passed"] = all(report["core_checks"].values())
    return report


def main() -> int:
    config = parse_args()
    try:
        report = asyncio.run(run_benchmark(config))
    except BenchmarkSetupError as exc:
        print_setup_failure(str(exc))
        return 2
    print_human_report(report)
    if config.json_output:
        print()
        print(json.dumps(report, indent=2, sort_keys=True))

    if config.report_file:
        report_path = Path(config.report_file)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        logger.info("Wrote report to %s", report_path)

    return 0 if report["all_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
