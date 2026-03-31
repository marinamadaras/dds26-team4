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

import run_consistency_benchmark as base

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(asctime)s - %(name)s - %(message)s",
    datefmt="%I:%M:%S",
)
logger = logging.getLogger("chaos-consistency-benchmark")

REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class ChaosBenchmarkConfig:
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
    kill_services: list[str]
    kill_delay_s: float
    checkout_spread_s: float


async def run_chaos_action(config: ChaosBenchmarkConfig) -> dict[str, Any]:
    if not config.kill_services:
        return {
            "attempted": False,
            "services": [],
            "delay_seconds": config.kill_delay_s,
            "stdout": "",
            "stderr": "",
            "returncode": 0,
        }

    await asyncio.sleep(config.kill_delay_s)
    logger.info("Killing services: %s", ", ".join(config.kill_services))
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        "kill",
        *config.kill_services,
        cwd=str(REPO_ROOT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return {
        "attempted": True,
        "services": config.kill_services,
        "delay_seconds": config.kill_delay_s,
        "stdout": stdout.decode().strip(),
        "stderr": stderr.decode().strip(),
        "returncode": process.returncode,
    }


async def perform_chaotic_checkouts(
    client: base.BenchmarkClient,
    order_plans: list[base.OrderPlan],
    config: ChaosBenchmarkConfig,
) -> tuple[Counter[str], dict[str, Any]]:
    logger.info(
        "Running %s concurrent checkouts with kill_delay=%ss and checkout_spread=%ss",
        len(order_plans),
        config.kill_delay_s,
        config.checkout_spread_s,
    )

    async def issue_checkout(index: int, plan: base.OrderPlan) -> str:
        if config.checkout_spread_s > 0 and len(order_plans) > 1:
            await asyncio.sleep((config.checkout_spread_s * index) / (len(order_plans) - 1))
        try:
            status = await client.request_status_any(
                "POST",
                f"{base.ORDER_URL}/orders/checkout/{plan.order_id}",
            )
            return str(status)
        except asyncio.TimeoutError:
            return "socket-timeout"
        except aiohttp.ClientError as exc:
            return f"client-error:{exc.__class__.__name__}"
        except RuntimeError:
            return "runtime-error"

    chaos_task = asyncio.create_task(run_chaos_action(config))
    try:
        statuses = await asyncio.gather(
            *[issue_checkout(index, plan) for index, plan in enumerate(order_plans)]
        )
    finally:
        chaos_result = await chaos_task
    return Counter(statuses), chaos_result


def parse_args() -> ChaosBenchmarkConfig:
    parser = argparse.ArgumentParser(description="Run the consistency benchmark while killing containers mid-run.")
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
    parser.add_argument("--settle-timeout", type=float, default=60.0)
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--parallelism", type=int, default=200)
    parser.add_argument("--report-file")
    parser.add_argument("--json", action="store_true", help="Also print the raw JSON report.")
    parser.add_argument(
        "--kill-services",
        default="order-service-2",
        help="Comma-separated docker compose service names to kill during the run.",
    )
    parser.add_argument(
        "--kill-delay",
        type=float,
        default=1.0,
        help="Seconds to wait after checkout traffic starts before killing services.",
    )
    parser.add_argument(
        "--checkout-spread",
        type=float,
        default=2.0,
        help="Spread checkout requests over this many seconds so the kill happens during live traffic.",
    )
    args = parser.parse_args()
    item_prices = [int(part) for part in args.price_list.split(",")] if args.price_list else [args.price]
    kill_services = [service.strip() for service in args.kill_services.split(",") if service.strip()]
    return ChaosBenchmarkConfig(
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
        kill_services=kill_services,
        kill_delay_s=args.kill_delay,
        checkout_spread_s=args.checkout_spread,
    )


def print_human_report(report: dict[str, Any]) -> None:
    chaos = report["chaos"]
    print("Chaos consistency benchmark")
    print()
    print("Chaos plan")
    print(f"Kill services: {', '.join(chaos['services']) if chaos['services'] else 'none'}")
    print(f"Kill delay after checkout start: {chaos['delay_seconds']}s")
    print(f"Checkout spread window: {report['config']['checkout_spread_s']}s")
    print(f"Chaos command return code: {chaos['returncode']}")
    if chaos["stderr"]:
        print(f"Chaos command stderr: {chaos['stderr']}")
    print()
    base.print_human_report(report)


async def run_benchmark(config: ChaosBenchmarkConfig) -> dict[str, Any]:
    timeout = aiohttp.ClientTimeout(total=None, connect=config.request_timeout_s, sock_read=config.request_timeout_s)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        client = base.BenchmarkClient(session, parallelism=config.parallelism)

        items = await base.create_shared_items(client, config)
        user_ids = await base.create_users(client, config)
        order_ids = await base.create_orders(client, user_ids)
        order_plans = await base.attach_items_to_orders(client, order_ids, user_ids, items, config)
        checkout_status_counts, chaos_result = await perform_chaotic_checkouts(client, order_plans, config)

        logger.info("Waiting for terminal order states after chaos event")
        terminal_orders = await asyncio.gather(
            *[base.wait_for_terminal_order(client, order_plan, config) for order_plan in order_plans]
        )

        final_stock_by_item = await base.fetch_item_stocks(client, items)
        total_remaining_credit = await base.fetch_total_user_credit(client, user_ids)

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
    reported_checkout_status_counts = base._reported_checkout_status_counts(dict(checkout_status_counts))
    actual_checkout_failure_count = base._count_reported_checkout_failures(reported_checkout_status_counts)

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
        "chaos": chaos_result,
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
            "chaos_command_succeeded": chaos_result["returncode"] == 0,
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
    report = asyncio.run(run_benchmark(config))
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
