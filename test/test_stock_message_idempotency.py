import os
import sys
import unittest
from pathlib import Path

from msgspec import msgpack

ROOT_DIR = Path(__file__).resolve().parents[1]
TEST_DIR = ROOT_DIR / "test"
SERVICE_DIR = ROOT_DIR / "stock"
os.environ["DISABLE_KAFKA_CONSUMER"] = "1"
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")
os.environ.setdefault("REDIS_PASSWORD", "redis")
os.environ.setdefault("REDIS_DB", "0")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
sys.path.insert(0, str(TEST_DIR))
sys.path.insert(0, str(SERVICE_DIR))

from fake_redis import FakeRedis

import app as stock_app
import messages as stock_messages

## to run DISABLE_KAFKA_CONSUMER=1 .venv/bin/python -m unittest discover -s test -p "test_stock_message_idempotency.py"
class TestStockMessageIdempotency(unittest.TestCase):
    def setUp(self):
        self.db = FakeRedis()
        self.published: list[tuple[str, str, object]] = []

        stock_app.db = self.db
        stock_app.publish = lambda topic, key, value: self.published.append((topic, key, value))

    def _set_item(self, item_id: str, stock: int, price: int = 5):
        self.db.set(item_id, msgpack.encode(stock_app.StockValue(stock=stock, price=price)))

    def _get_item_stock(self, item_id: str) -> int:
        raw = self.db.get(item_id)
        item = msgpack.decode(raw, type=stock_app.StockValue)
        return item.stock

    def test_find_stock_multiple_requests_has_no_side_effects(self):
        self._set_item("i1", stock=10, price=9)
        message = stock_messages.FindStock(
            idempotency_key="find-stock-op-1",
            item_id="i1",
            quantity=2,
        )

        stock_app.handle_find_stock(message, order_id="o1")
        stock_app.handle_find_stock(message, order_id="o1")

        self.assertEqual(self._get_item_stock("i1"), 10)
        self.assertEqual(len(self.published), 2)
        self.assertTrue(self.published[0][2].found)
        self.assertTrue(self.published[1][2].found)

    def test_subtract_stock_duplicate_success_only_applies_once(self):
        self._set_item("i1", stock=10)
        message = stock_messages.SubtractStock(
            idempotency_key="sub-stock-op-1",
            order_id="o1",
            item_id="i1",
            quantity=3,
        )

        stock_app.handle_subtract_stock(message)
        stock_app.handle_subtract_stock(message)

        self.assertEqual(self._get_item_stock("i1"), 7)
        self.assertEqual(len(self.published), 2)
        self.assertTrue(self.published[0][2].success)
        self.assertTrue(self.published[1][2].success)

    def test_subtract_stock_failed_then_retry_same_key_can_succeed(self):
        self._set_item("i1", stock=1)
        message = stock_messages.SubtractStock(
            idempotency_key="sub-stock-op-2",
            order_id="o2",
            item_id="i1",
            quantity=2,
        )

        stock_app.handle_subtract_stock(message)
        self.assertFalse(self.published[-1][2].success)

        self._set_item("i1", stock=5)
        stock_app.handle_subtract_stock(message)

        self.assertEqual(self._get_item_stock("i1"), 3)
        self.assertTrue(self.published[-1][2].success)

    def test_rollback_stock_same_key_retries_when_cached_failure(self):
        message = stock_messages.RollbackStockRequest(
            idempotency_key="rollback-stock-op-1",
            order_id="o3",
            item_id="i9",
            quantity=1,
        )

        stock_app.handle_rollback_stock(message)
        stock_app.handle_rollback_stock(message)

        self.assertEqual(len(self.published), 2)
        self.assertFalse(self.published[0][2].success)
        self.assertFalse(self.published[1][2].success)


if __name__ == "__main__":
    unittest.main()
