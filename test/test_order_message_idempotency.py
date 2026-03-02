import os
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

from msgspec import msgpack

ROOT_DIR = Path(__file__).resolve().parents[1]
TEST_DIR = ROOT_DIR / "test"
SERVICE_DIR = ROOT_DIR / "order"
os.environ["DISABLE_KAFKA_CONSUMER"] = "1"
os.environ.setdefault("GATEWAY_URL", "http://localhost:8000")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")
os.environ.setdefault("REDIS_PASSWORD", "redis")
os.environ.setdefault("REDIS_DB", "0")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
sys.path.insert(0, str(TEST_DIR))
sys.path.insert(0, str(SERVICE_DIR))

from fake_redis import FakeRedis

import app as order_app
import messages as order_messages

# to run DISABLE_KAFKA_CONSUMER=1 .venv/bin/python -m unittest discover -s test -p "test_order_message_idempotency.py"
class TestOrderMessageIdempotency(unittest.TestCase):
    def setUp(self):
        self.db = FakeRedis()
        order_app.db = self.db

        self.published_payment = []
        self.published_rollback_stock = []
        self.published_rollback_payment = []

        order_app.publish_payment = lambda order_id, user_id, amount: self.published_payment.append(
            (order_id, user_id, amount)
        )
        order_app.publish_rollback_stock = lambda order_id, item_id, quantity: self.published_rollback_stock.append(
            (order_id, item_id, quantity)
        )
        order_app.publish_rollback_payment = lambda order_id, user_id, amount: self.published_rollback_payment.append(
            (order_id, user_id, amount)
        )

    def _set_order(self, order_id: str, order_value: order_app.OrderValue):
        self.db.set(order_id, msgpack.encode(order_value))

    def _get_order(self, order_id: str) -> order_app.OrderValue:
        raw = self.db.get(order_id)
        return msgpack.decode(raw, type=order_app.OrderValue)

    def test_find_stock_reply_duplicate_success_updates_once(self):
        order_id = "o1"
        self._set_order(
            order_id,
            order_app.OrderValue(
                status=order_app.ORDER_CREATED,
                items=[],
                user_id="u1",
                total_cost=0,
                expected_items=0,
                stock_confirmations=0,
            ),
        )
        message = order_messages.FindStockReply(
            idempotency_key="find-reply-1",
            order_id=order_id,
            item_id="i1",
            found=True,
            quantity=2,
            stock=10,
            price=5,
        )

        order_app.handle_find_stock_reply(message)
        order_app.handle_find_stock_reply(message)

        order = self._get_order(order_id)
        self.assertEqual(order.items, [("i1", 2)])
        self.assertEqual(order.total_cost, 10)

    def test_find_stock_reply_failed_is_retried(self):
        order_app.app.logger.warning = Mock()
        message = order_messages.FindStockReply(
            idempotency_key="find-reply-2",
            order_id="o2",
            item_id="i2",
            found=False,
            quantity=1,
        )

        order_app.handle_find_stock_reply(message)
        order_app.handle_find_stock_reply(message)

        self.assertEqual(order_app.app.logger.warning.call_count, 2)

    def test_stock_subtracted_reply_duplicate_success_updates_once(self):
        order_id = "o3"
        self._set_order(
            order_id,
            order_app.OrderValue(
                status=order_app.STOCK_PENDING,
                items=[("i1", 1)],
                user_id="u1",
                total_cost=9,
                expected_items=1,
                stock_confirmations=0,
            ),
        )
        message = order_messages.StockSubtractedReply(
            idempotency_key="sub-reply-1",
            order_id=order_id,
            item_id="i1",
            quantity=1,
            success=True,
        )

        order_app.handle_stock_subtracted_reply(message)
        order_app.handle_stock_subtracted_reply(message)

        order = self._get_order(order_id)
        self.assertEqual(order.stock_confirmations, 1)
        self.assertEqual(order.status, order_app.PAYMENT_PENDING)
        self.assertEqual(len(self.published_payment), 1)

    def test_payment_reply_duplicate_success_updates_once(self):
        order_id = "o4"
        self._set_order(
            order_id,
            order_app.OrderValue(
                status=order_app.PAYMENT_PENDING,
                items=[("i1", 1)],
                user_id="u1",
                total_cost=9,
                expected_items=1,
                stock_confirmations=1,
            ),
        )
        message = order_messages.PaymentReply(
            idempotency_key="payment-reply-1",
            order_id=order_id,
            user_id="u1",
            amount=9,
            success=True,
        )

        order_app.handle_payment_reply(message)
        order_app.handle_payment_reply(message)

        order = self._get_order(order_id)
        self.assertEqual(order.status, order_app.ORDER_COMPLETED)
        self.assertEqual(len(self.published_rollback_stock), 0)
        self.assertEqual(len(self.published_rollback_payment), 0)

    def test_rollback_replys_same_success_key_do_not_repeat_effects(self):
        order_app.app.logger.info = Mock()
        stock_message = order_messages.RollbackStockReply(
            idempotency_key="rollback-stock-reply-1",
            order_id="o5",
            item_id="i7",
            quantity=1,
            success=True,
        )
        payment_message = order_messages.RollbackPaymentReply(
            idempotency_key="rollback-payment-reply-1",
            order_id="o5",
            user_id="u9",
            amount=3,
            success=True,
        )

        order_app.handle_rollback_stock_reply(stock_message)
        order_app.handle_rollback_stock_reply(stock_message)
        order_app.handle_rollback_payment_reply(payment_message)
        order_app.handle_rollback_payment_reply(payment_message)

        self.assertEqual(order_app.app.logger.info.call_count, 2)


if __name__ == "__main__":
    unittest.main()
