import os
import sys
import unittest
from pathlib import Path

from msgspec import msgpack

ROOT_DIR = Path(__file__).resolve().parents[1]
TEST_DIR = ROOT_DIR / "test"
SERVICE_DIR = ROOT_DIR / "payment"
os.environ["DISABLE_KAFKA_CONSUMER"] = "1"
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")
os.environ.setdefault("REDIS_PASSWORD", "redis")
os.environ.setdefault("REDIS_DB", "0")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
sys.path.insert(0, str(TEST_DIR))
sys.path.insert(0, str(SERVICE_DIR))

from fake_redis import FakeRedis

import app as payment_app
import messages as payment_messages

# to run use: DISABLE_KAFKA_CONSUMER=1 .venv/bin/python -m unittest discover -s test -p "test_payment_message_idempotency.py"
class TestPaymentMessageIdempotency(unittest.TestCase):
    def setUp(self):
        self.db = FakeRedis()
        self.published: list[tuple[str, str, object]] = []

        payment_app.db = self.db
        payment_app.publish = lambda topic, key, value: self.published.append((topic, key, value))

    def _set_user_credit(self, user_id: str, credit: int):
        self.db.set(user_id, msgpack.encode(payment_app.UserValue(credit=credit)))

    def _get_user_credit(self, user_id: str) -> int:
        raw = self.db.get(user_id)
        user = msgpack.decode(raw, type=payment_app.UserValue)
        return user.credit

    def test_payment_request_duplicate_success_only_applies_once(self):
        self._set_user_credit("u1", 10)
        message = payment_messages.PaymentRequest(
            idempotency_key="pay-op-1",
            order_id="o1",
            user_id="u1",
            amount=3,
        )

        payment_app.handle_payment_request(message)
        payment_app.handle_payment_request(message)

        self.assertEqual(self._get_user_credit("u1"), 7)
        self.assertEqual(len(self.published), 2)
        self.assertTrue(self.published[0][2].success)
        self.assertTrue(self.published[1][2].success)

    def test_payment_request_failed_then_retry_same_key_can_succeed(self):
        self._set_user_credit("u1", 2)
        message = payment_messages.PaymentRequest(
            idempotency_key="pay-op-2",
            order_id="o2",
            user_id="u1",
            amount=5,
        )

        payment_app.handle_payment_request(message)
        self.assertFalse(self.published[-1][2].success)

        self._set_user_credit("u1", 20)
        payment_app.handle_payment_request(message)

        self.assertEqual(self._get_user_credit("u1"), 15)
        self.assertTrue(self.published[-1][2].success)

    def test_rollback_request_with_same_key_retries_when_cached_failure(self):
        message = payment_messages.RollbackPaymentRequest(
            idempotency_key="rollback-pay-op-1",
            order_id="o3",
            user_id="u1",
            amount=4,
        )

        payment_app.handle_rollback_payment_request(message)
        payment_app.handle_rollback_payment_request(message)

        self.assertEqual(len(self.published), 2)
        self.assertFalse(self.published[0][2].success)
        self.assertFalse(self.published[1][2].success)


if __name__ == "__main__":
    unittest.main()
