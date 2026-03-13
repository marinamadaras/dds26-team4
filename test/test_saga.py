import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from msgspec import msgpack

ROOT_DIR = Path(__file__).resolve().parents[1]
SERVICE_DIR = ROOT_DIR / "order"
os.environ["DISABLE_KAFKA_CONSUMER"] = "1"
os.environ.setdefault("GATEWAY_URL", "http://localhost")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")
os.environ.setdefault("REDIS_PASSWORD", "")
os.environ.setdefault("REDIS_DB", "0")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
sys.path.insert(0, str(SERVICE_DIR))

import app as order_app
import messages as order_messages


class FakeRedis:
    def __init__(self):
        self.store: dict[str, bytes] = {}
        self.sorted_sets: dict[str, dict[str, float]] = {}

    def get(self, key: str):
        return self.store.get(key)

    def set(self, key: str, value, nx: bool = False):
        if nx and key in self.store:
            return None
        if isinstance(value, str):
            value = value.encode()
        self.store[key] = value
        return True

    def mset(self, values: dict[str, bytes]):
        for key, value in values.items():
            self.set(key, value)
        return True

    def zadd(self, key: str, mapping: dict[str, float]):
        bucket = self.sorted_sets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[str(member)] = float(score)
        return True

    def zrem(self, key: str, member: str):
        bucket = self.sorted_sets.setdefault(key, {})
        bucket.pop(str(member), None)
        return True

    def zrange(self, key: str, start: int, stop: int):
        bucket = self.sorted_sets.get(key, {})
        members = [member.encode() for member, _ in sorted(bucket.items(), key=lambda item: item[1])]
        if stop == -1:
            stop = len(members) - 1
        return members[start:stop + 1]

    def zrangebyscore(self, key: str, minimum: float, maximum: float):
        bucket = self.sorted_sets.get(key, {})
        return [
            member.encode()
            for member, score in sorted(bucket.items(), key=lambda item: item[1])
            if minimum <= score <= maximum
        ]

    def close(self):
        return None


def make_order(user_id: str = "user-1") -> str:
    order_id = "order-1"
    order = order_app.OrderValue(
        items=[],
        user_id=user_id,
        total_cost=0,
        expected_items=0,
        stock_confirmations=0,
        rollback_stock_confirmations=0,
    )
    order_app.db.set(order_id, msgpack.encode(order))
    return order_id


def get_order(order_id: str) -> order_app.OrderValue:
    return msgpack.decode(order_app.db.get(order_id), type=order_app.OrderValue)


def get_events(order_id: str) -> list[str]:
    return order_app.get_log_events(order_id)


def make_find_stock_reply(order_id: str, item_id: str, quantity: int, price: int):
    return order_messages.FindStockReply(
        idempotency_key=order_app.make_idempotency_key(order_id, "find_stock", item_id, quantity),
        order_id=order_id,
        item_id=item_id,
        quantity=quantity,
        price=price,
        found=True,
    )


def make_stock_subtracted_reply(order_id: str, item_id: str, quantity: int, success: bool = True):
    return order_messages.StockSubtractedReply(
        idempotency_key=order_app.make_idempotency_key(order_id, "subtract_stock", item_id, quantity),
        order_id=order_id,
        item_id=item_id,
        quantity=quantity,
        success=success,
    )


def make_payment_reply(order_id: str, user_id: str, amount: int, success: bool = True):
    return order_messages.PaymentReply(
        idempotency_key=order_app.make_idempotency_key(order_id, "payment_request", user_id, amount),
        order_id=order_id,
        user_id=user_id,
        amount=amount,
        success=success,
    )


class TestParallelCheckoutSaga(unittest.TestCase):
    def setUp(self):
        order_app.db = FakeRedis()
        order_app.publish = MagicMock()

    def test_happy_path_commits_only_after_both_branches_succeed(self):
        order_id = make_order("user-42")
        order_app.handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 2, 15))

        with order_app.app.test_client() as client:
            response = client.post(f"/checkout/{order_id}")

        self.assertEqual(response.status_code, 202)
        topics = [call.kwargs["topic"] for call in order_app.publish.call_args_list]
        self.assertEqual(topics.count("subtract.stock"), 1)
        self.assertEqual(topics.count("payment"), 1)

        order_app.handle_payment_reply(make_payment_reply(order_id, "user-42", 30, success=True))
        order = get_order(order_id)
        self.assertTrue(order.payment_reply_received)
        self.assertTrue(order.payment_success)
        self.assertFalse(order.checkout_finalized)
        self.assertNotIn("paid", get_events(order_id))

        order_app.handle_stock_subtracted_reply(make_stock_subtracted_reply(order_id, "item-A", 2, success=True))
        order = get_order(order_id)
        self.assertEqual(order.stock_confirmations, 1)
        self.assertTrue(order.checkout_finalized)
        self.assertIn("payment_confirmed", get_events(order_id))
        self.assertIn("stock_subtracted", get_events(order_id))
        self.assertEqual(get_events(order_id)[-1], "saga_end")

    def test_payment_failure_cancels_and_starts_stock_rollback(self):
        order_id = make_order("user-7")
        order_app.handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 1, 20))
        order_app.handle_find_stock_reply(make_find_stock_reply(order_id, "item-B", 2, 10))

        with order_app.app.test_client() as client:
            client.post(f"/checkout/{order_id}")

        order_app.publish.reset_mock()
        order_app.handle_payment_reply(make_payment_reply(order_id, "user-7", 40, success=False))

        topics = [call.kwargs["topic"] for call in order_app.publish.call_args_list]
        self.assertEqual(topics.count("rollback.stock"), 2)
        self.assertNotIn("rollback.payment", topics)

        order = get_order(order_id)
        self.assertTrue(order.checkout_finalized)
        self.assertTrue(order.payment_reply_received)
        self.assertFalse(order.payment_success)
        self.assertIn("payment_failed", get_events(order_id))
        self.assertIn("cancelled", get_events(order_id))

    def test_stock_failure_after_payment_success_cancels_and_rolls_back_payment(self):
        order_id = make_order("user-99")
        order_app.handle_find_stock_reply(make_find_stock_reply(order_id, "item-X", 3, 10))

        with order_app.app.test_client() as client:
            client.post(f"/checkout/{order_id}")

        order_app.publish.reset_mock()
        order_app.handle_payment_reply(make_payment_reply(order_id, "user-99", 30, success=True))
        order_app.publish.reset_mock()

        order_app.handle_stock_subtracted_reply(make_stock_subtracted_reply(order_id, "item-X", 3, success=False))

        topics = [call.kwargs["topic"] for call in order_app.publish.call_args_list]
        self.assertIn("rollback.stock", topics)
        self.assertIn("rollback.payment", topics)

        order = get_order(order_id)
        self.assertTrue(order.checkout_finalized)
        self.assertTrue(order.payment_reply_received)
        self.assertTrue(order.payment_success)
        self.assertIn("payment_confirmed", get_events(order_id))
        self.assertIn("cancelled", get_events(order_id))


if __name__ == "__main__":
    unittest.main(verbosity=2)
