import os
import subprocess
import time
import unittest
from pathlib import Path

import requests
import utils as tu


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_SERVICES = "order-service stock-service payment-service kafka order-db stock-db payment-db"


class TestFailoverRecovery(unittest.TestCase):
    services = os.getenv("FAILOVER_SERVICES", DEFAULT_SERVICES).split()
    kill_sleep = float(os.getenv("FAILOVER_KILL_SLEEP", "3"))
    recovery_timeout = float(os.getenv("FAILOVER_RECOVERY_TIMEOUT", "40"))

    @classmethod
    def setUpClass(cls) -> None:
        cls._compose("up", "-d")
        cls._wait_for_probe(cls._probe_checkout_path, timeout=60)

    @staticmethod
    def _compose(*args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["docker", "compose", *args],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            check=True,
        )

    @classmethod
    def _wait_for_probe(cls, probe_fn, timeout: float) -> None:
        deadline = time.time() + timeout
        last_error = None
        while time.time() < deadline:
            try:
                if probe_fn():
                    return
            except (requests.RequestException, subprocess.CalledProcessError, AssertionError, KeyError, ValueError) as exc:
                last_error = exc
            time.sleep(1)
        raise AssertionError(f"Service did not recover within {timeout:.0f}s. Last error: {last_error}")

    @classmethod
    def _container_id(cls, service: str) -> str:
        result = cls._compose("ps", "-q", service)
        container_id = result.stdout.strip()
        if not container_id:
            raise AssertionError(f"No container id found for service: {service}")
        return container_id

    @classmethod
    def _inspect_state(cls, container_id: str) -> tuple[bool, str, int]:
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Running}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}} {{.RestartCount}}",
                container_id,
            ],
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            check=True,
        )
        parts = result.stdout.strip().split()
        running = parts[0] == "true"
        health = parts[1]
        restart_count = int(parts[2])
        return running, health, restart_count

    @classmethod
    def _wait_for_auto_restart(cls, service: str, prev_restart_count: int, timeout: float) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                container_id = cls._container_id(service)
                running, health, restart_count = cls._inspect_state(container_id)
                if (
                    running
                    and restart_count > prev_restart_count
                    and (health == "none" or health == "healthy")
                ):
                    return
            except (AssertionError, subprocess.CalledProcessError, ValueError):
                pass
            time.sleep(1)
        raise AssertionError(f"Container {service} did not auto-restart within {timeout:.0f}s")

    @staticmethod
    def _probe_payment_path() -> bool:
        user = tu.create_user()
        user_id = user["user_id"]
        data = tu.find_user(user_id)
        return data.get("user_id") == user_id

    @staticmethod
    def _probe_stock_path() -> bool:
        item = tu.create_item(9)
        item_id = item["item_id"]
        data = tu.find_item(item_id)
        return data.get("item_id", item_id) == item_id and data.get("price") == 9

    @staticmethod
    def _probe_order_path() -> bool:
        user = tu.create_user()
        order = tu.create_order(user["user_id"])
        order_id = order["order_id"]
        data = tu.find_order(order_id)
        return data.get("order_id") == order_id

    @staticmethod
    def _probe_checkout_path() -> bool:
        user = tu.create_user()
        user_id = user["user_id"]
        add_funds_status = tu.add_credit_to_user(user_id, 100)
        if not tu.status_code_is_success(add_funds_status):
            return False

        item = tu.create_item(5)
        item_id = item["item_id"]
        add_stock_status = tu.add_stock(item_id, 10)
        if not tu.status_code_is_success(add_stock_status):
            return False

        order = tu.create_order(user_id)
        order_id = order["order_id"]
        add_item_status = tu.add_item_to_order(order_id, item_id, 1)
        if not tu.status_code_is_success(add_item_status):
            return False

        checkout = tu.checkout_order(order_id)
        return tu.status_code_is_success(checkout.status_code)

    @classmethod
    def _probe_for_service(cls, service: str) -> bool:
        if service in {"payment-service", "payment-db"}:
            return cls._probe_payment_path()
        if service in {"stock-service", "stock-db"}:
            return cls._probe_stock_path()
        if service in {"order-service", "order-db"}:
            return cls._probe_order_path()
        if service == "kafka":
            return cls._probe_checkout_path()
        raise ValueError(f"No probe configured for service: {service}")

    def test_endpoints_recover_after_container_failure(self) -> None:
        for service in self.services:
            with self.subTest(service=service):
                container_id = self._container_id(service)
                _, _, prev_restart_count = self._inspect_state(container_id)
                self._compose("kill", service)
                time.sleep(self.kill_sleep)
                self._wait_for_auto_restart(service, prev_restart_count, timeout=self.recovery_timeout)
                self._wait_for_probe(lambda s=service: self._probe_for_service(s), timeout=self.recovery_timeout)


if __name__ == "__main__":
    unittest.main()
