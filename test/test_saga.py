"""
Happy-path tests for the order service checkout saga.

What is tested:
  1. Create an order
  2. Add items (via mocked FindStockReply)
  3. Checkout (transitions to STOCK_PENDING, publishes subtract_stock per item)
  4. Receive StockSubtractedReply for each item (transitions to PAYMENT_PENDING,
     publishes payment when last confirmation arrives)
  5. Receive PaymentReply(success=True) → order transitions to COMPLETED

All Redis calls and Kafka publishes are mocked so the tests run with no
external dependencies.
"""

import os
import time
import unittest
from unittest.mock import MagicMock, patch, call
from collections import defaultdict

# ── Minimal environment so the module can be imported ─────────────────────────
os.environ.setdefault("GATEWAY_URL", "http://localhost")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")
os.environ.setdefault("REDIS_PASSWORD", "")
os.environ.setdefault("REDIS_DB", "0")
os.environ["DISABLE_KAFKA_CONSUMER"] = "1"   # don't start threads during import

# ── Stub out kafka_client and messages before importing the service ────────────
import sys
from types import ModuleType

# --- kafka_client stub --------------------------------------------------------
kafka_stub = ModuleType("kafka_client")
kafka_stub.publish = MagicMock()
kafka_stub.create_consumer = MagicMock()
kafka_stub.decode_message = MagicMock()
sys.modules["kafka_client"] = kafka_stub

# --- messages stub ------------------------------------------------------------
from msgspec import Struct

class BaseMessage(Struct):
    type: str = ""

class FindStock(Struct):
    idempotency_key: str
    item_id: str
    quantity: int
    type: str = "FindStock"

class FindStockReply(Struct):
    idempotency_key: str
    order_id: str
    item_id: str
    quantity: int
    price: int
    found: bool
    type: str = "FindStockReply"

class SubtractStock(Struct):
    idempotency_key: str
    order_id: str
    item_id: str
    quantity: int
    type: str = "SubtractStock"

class StockSubtractedReply(Struct):
    idempotency_key: str
    order_id: str
    item_id: str
    quantity: int
    success: bool
    type: str = "StockSubtractedReply"

class PaymentRequest(Struct):
    idempotency_key: str
    order_id: str
    user_id: str
    amount: int
    type: str = "PaymentRequest"

class PaymentReply(Struct):
    idempotency_key: str
    order_id: str
    user_id: str
    success: bool
    type: str = "PaymentReply"

class RollbackStockRequest(Struct):
    idempotency_key: str
    order_id: str
    item_id: str
    quantity: int
    type: str = "RollbackStockRequest"

class RollbackStockReply(Struct):
    idempotency_key: str
    order_id: str
    item_id: str
    success: bool
    type: str = "RollbackStockReply"

class RollbackPaymentRequest(Struct):
    idempotency_key: str
    order_id: str
    user_id: str
    amount: int
    type: str = "RollbackPaymentRequest"

class RollbackPaymentReply(Struct):
    idempotency_key: str
    order_id: str
    user_id: str
    success: bool
    type: str = "RollbackPaymentReply"

messages_stub = ModuleType("messages")
for cls in [
    BaseMessage, FindStock, FindStockReply,
    SubtractStock, StockSubtractedReply,
    PaymentRequest, PaymentReply,
    RollbackStockRequest, RollbackStockReply,
    RollbackPaymentRequest, RollbackPaymentReply,
]:
    setattr(messages_stub, cls.__name__, cls)

sys.modules["messages"] = messages_stub

# ── Now import the service ─────────────────────────────────────────────────────
import app as svc
from app import (
    OrderValue,
    ORDER_CREATED, STOCK_PENDING, PAYMENT_PENDING, ORDER_COMPLETED,
    handle_find_stock_reply, handle_stock_subtracted_reply, handle_payment_reply,
    make_idempotency_key,
)
from msgspec import msgpack


# ══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ══════════════════════════════════════════════════════════════════════════════

def make_order(user_id="user-1") -> tuple[str, OrderValue]:
    """Return (order_id, order) and store it in the fake Redis."""
    import uuid
    order_id = str(uuid.uuid4())
    order = OrderValue(
        status=ORDER_CREATED,
        items=[],
        user_id=user_id,
        total_cost=0,
        expected_items=0,
        stock_confirmations=0,
    )
    svc.db.set(order_id, msgpack.encode(order))
    return order_id, order


def get_order(order_id: str) -> OrderValue:
    raw = svc.db.get(order_id)
    return msgpack.decode(raw, type=OrderValue)


def make_find_stock_reply(order_id, item_id, quantity, price, found=True) -> FindStockReply:
    return FindStockReply(
        idempotency_key=make_idempotency_key(order_id, "find_stock", item_id, quantity),
        order_id=order_id,
        item_id=item_id,
        quantity=quantity,
        price=price,
        found=found,
    )


def make_stock_subtracted_reply(order_id, item_id, quantity, success=True) -> StockSubtractedReply:
    return StockSubtractedReply(
        idempotency_key=make_idempotency_key(order_id, "subtract_stock", item_id, quantity),
        order_id=order_id,
        item_id=item_id,
        quantity=quantity,
        success=success,
    )


def make_payment_reply(order_id, user_id, amount, success=True) -> PaymentReply:
    return PaymentReply(
        idempotency_key=make_idempotency_key(order_id, "payment_request", user_id, amount),
        order_id=order_id,
        user_id=user_id,
        success=success,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Fake Redis
#  Simple in-memory dict that supports get / set / mset / scan
# ══════════════════════════════════════════════════════════════════════════════

class FakeRedis:
    def __init__(self):
        self._store: dict[str, bytes] = {}

    def get(self, key):
        return self._store.get(key)

    def set(self, key, value):
        self._store[key] = value
        return True

    def mset(self, mapping):
        self._store.update(mapping)
        return True

    def scan(self, cursor, match="*", count=100):
        import fnmatch
        all_keys = [k for k in self._store if fnmatch.fnmatch(k, match)]
        return 0, all_keys

    def close(self):
        pass

    def clear(self):
        self._store.clear()


# ══════════════════════════════════════════════════════════════════════════════
#  Test cases
# ══════════════════════════════════════════════════════════════════════════════

class TestHappyPathCheckout(unittest.TestCase):

    def setUp(self):
        # Replace the real Redis with our fake one
        self.fake_redis = FakeRedis()
        svc.db = self.fake_redis
        # Reset the Kafka publish mock between tests
        kafka_stub.publish.reset_mock()

    # ── 1. Create order ────────────────────────────────────────────────────────

    def test_create_order_stores_in_db(self):
        with svc.app.test_client() as client:
            resp = client.post("/create/user-1")
        self.assertEqual(resp.status_code, 200)
        order_id = resp.get_json()["order_id"]
        order = get_order(order_id)
        self.assertEqual(order.status, ORDER_CREATED)
        self.assertEqual(order.user_id, "user-1")
        self.assertEqual(order.items, [])
        self.assertEqual(order.total_cost, 0)

    # ── 2. Add item (find_stock round-trip) ────────────────────────────────────

    def test_add_item_publishes_find_stock(self):
        order_id, _ = make_order()
        with svc.app.test_client() as client:
            resp = client.post(f"/addItem/{order_id}/item-A/2")
        self.assertEqual(resp.status_code, 202)
        kafka_stub.publish.assert_called_once()
        call_kwargs = kafka_stub.publish.call_args
        self.assertEqual(call_kwargs.kwargs["topic"], "find.stock")

    def test_find_stock_reply_updates_order(self):
        order_id, _ = make_order()
        reply = make_find_stock_reply(order_id, "item-A", quantity=2, price=50)
        handle_find_stock_reply(reply)
        order = get_order(order_id)
        self.assertIn(("item-A", 2), order.items)
        self.assertEqual(order.total_cost, 100)   # 2 × 50

    def test_add_two_different_items(self):
        order_id, _ = make_order()
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 1, price=30))
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-B", 3, price=10))
        order = get_order(order_id)
        self.assertEqual(order.total_cost, 60)    # 1×30 + 3×10
        self.assertEqual(len(order.items), 2)

    # ── 3. Checkout ────────────────────────────────────────────────────────────

    def test_checkout_transitions_to_stock_pending(self):
        order_id, _ = make_order()
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 2, price=50))
        with svc.app.test_client() as client:
            resp = client.post(f"/checkout/{order_id}")
        self.assertEqual(resp.status_code, 202)
        order = get_order(order_id)
        self.assertEqual(order.status, STOCK_PENDING)

    def test_checkout_publishes_subtract_stock_per_item(self):
        order_id, _ = make_order()
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 1, price=20))
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-B", 2, price=10))
        kafka_stub.publish.reset_mock()

        with svc.app.test_client() as client:
            client.post(f"/checkout/{order_id}")

        topics = [c.kwargs["topic"] for c in kafka_stub.publish.call_args_list]
        # One subtract.stock per unique item, plus two _outgoing log entries
        # (logged via db.set, not publish) — just check the Kafka topics
        subtract_calls = [t for t in topics if t == "subtract.stock"]
        self.assertEqual(len(subtract_calls), 2)

    def test_checkout_sets_expected_items(self):
        order_id, _ = make_order()
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 1, price=20))
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-B", 2, price=10))
        with svc.app.test_client() as client:
            client.post(f"/checkout/{order_id}")
        order = get_order(order_id)
        self.assertEqual(order.expected_items, 2)
        self.assertEqual(order.stock_confirmations, 0)

    def test_checkout_is_idempotent(self):
        """Calling checkout twice should not double-publish."""
        order_id, _ = make_order()
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 1, price=20))
        with svc.app.test_client() as client:
            client.post(f"/checkout/{order_id}")
            kafka_stub.publish.reset_mock()
            resp = client.post(f"/checkout/{order_id}")
        # Second call should return 200 (already processed) and not publish anything
        self.assertEqual(resp.status_code, 200)
        subtract_calls = [
            c for c in kafka_stub.publish.call_args_list
            if c.kwargs.get("topic") == "subtract.stock"
        ]
        self.assertEqual(len(subtract_calls), 0)

    # ── 4. Stock subtracted replies ────────────────────────────────────────────

    def test_partial_stock_confirmation_stays_stock_pending(self):
        """With 2 items, first confirmation should NOT trigger payment yet."""
        order_id, _ = make_order()
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 1, price=10))
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-B", 1, price=10))
        with svc.app.test_client() as client:
            client.post(f"/checkout/{order_id}")
        kafka_stub.publish.reset_mock()

        # Only first item confirmed
        handle_stock_subtracted_reply(make_stock_subtracted_reply(order_id, "item-A", 1))

        order = get_order(order_id)
        self.assertEqual(order.status, STOCK_PENDING)
        self.assertEqual(order.stock_confirmations, 1)
        payment_calls = [c for c in kafka_stub.publish.call_args_list if c.kwargs.get("topic") == "payment"]
        self.assertEqual(len(payment_calls), 0)

    def test_all_stock_confirmed_transitions_to_payment_pending(self):
        order_id, _ = make_order()
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 1, price=10))
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-B", 1, price=20))
        with svc.app.test_client() as client:
            client.post(f"/checkout/{order_id}")
        kafka_stub.publish.reset_mock()

        handle_stock_subtracted_reply(make_stock_subtracted_reply(order_id, "item-A", 1))
        handle_stock_subtracted_reply(make_stock_subtracted_reply(order_id, "item-B", 1))

        order = get_order(order_id)
        self.assertEqual(order.status, PAYMENT_PENDING)

    def test_all_stock_confirmed_publishes_payment(self):
        order_id, _ = make_order("user-42")
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 2, price=15))
        with svc.app.test_client() as client:
            client.post(f"/checkout/{order_id}")
        kafka_stub.publish.reset_mock()

        handle_stock_subtracted_reply(make_stock_subtracted_reply(order_id, "item-A", 2))

        payment_calls = [c for c in kafka_stub.publish.call_args_list if c.kwargs.get("topic") == "payment"]
        self.assertEqual(len(payment_calls), 1)
        # Verify the payment message carries the right amount
        msg: PaymentRequest = payment_calls[0].kwargs["value"]
        self.assertEqual(msg.amount, 30)   # 2 × 15
        self.assertEqual(msg.user_id, "user-42")
        self.assertEqual(msg.order_id, order_id)

    def test_duplicate_stock_reply_is_ignored(self):
        """Same StockSubtractedReply delivered twice should not double-count."""
        order_id, _ = make_order()
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 1, price=10))
        with svc.app.test_client() as client:
            client.post(f"/checkout/{order_id}")

        reply = make_stock_subtracted_reply(order_id, "item-A", 1)
        handle_stock_subtracted_reply(reply)
        handle_stock_subtracted_reply(reply)   # duplicate

        order = get_order(order_id)
        # stock_confirmations must not exceed expected_items
        self.assertLessEqual(order.stock_confirmations, order.expected_items)

    # ── 5. Payment reply ───────────────────────────────────────────────────────

    def test_payment_success_completes_order(self):
        order_id, _ = make_order("user-1")
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 1, price=50))
        with svc.app.test_client() as client:
            client.post(f"/checkout/{order_id}")
        handle_stock_subtracted_reply(make_stock_subtracted_reply(order_id, "item-A", 1))

        reply = make_payment_reply(order_id, "user-1", amount=50, success=True)
        handle_payment_reply(reply)

        order = get_order(order_id)
        self.assertEqual(order.status, ORDER_COMPLETED)

    def test_duplicate_payment_reply_is_ignored(self):
        """Same PaymentReply delivered twice should not cause any issues."""
        order_id, _ = make_order("user-1")
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 1, price=50))
        with svc.app.test_client() as client:
            client.post(f"/checkout/{order_id}")
        handle_stock_subtracted_reply(make_stock_subtracted_reply(order_id, "item-A", 1))
        kafka_stub.publish.reset_mock()

        reply = make_payment_reply(order_id, "user-1", amount=50, success=True)
        handle_payment_reply(reply)
        handle_payment_reply(reply)   # duplicate

        order = get_order(order_id)
        self.assertEqual(order.status, ORDER_COMPLETED)
        # No rollbacks should have been triggered
        rollback_calls = [
            c for c in kafka_stub.publish.call_args_list
            if "rollback" in c.kwargs.get("topic", "")
        ]
        self.assertEqual(len(rollback_calls), 0)

    # ── 6. Full end-to-end happy path ──────────────────────────────────────────

    def test_full_checkout_single_item(self):
        """End-to-end: create → add item → checkout → stock reply → payment reply → COMPLETED."""
        with svc.app.test_client() as client:
            # Create
            resp = client.post("/create/user-99")
            order_id = resp.get_json()["order_id"]

            # Add item (simulated via direct handler call — same as receiving Kafka reply)
            handle_find_stock_reply(make_find_stock_reply(order_id, "item-X", 3, price=10))

            # Checkout
            resp = client.post(f"/checkout/{order_id}")
            self.assertEqual(resp.status_code, 202)

        # Stock confirmed
        handle_stock_subtracted_reply(make_stock_subtracted_reply(order_id, "item-X", 3))
        self.assertEqual(get_order(order_id).status, PAYMENT_PENDING)

        # Payment succeeds
        handle_payment_reply(make_payment_reply(order_id, "user-99", amount=30, success=True))
        self.assertEqual(get_order(order_id).status, ORDER_COMPLETED)

    def test_full_checkout_multiple_items(self):
        """End-to-end with two distinct items, both need stock confirmation before payment."""
        with svc.app.test_client() as client:
            resp = client.post("/create/user-7")
            order_id = resp.get_json()["order_id"]

            handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 1, price=100))
            handle_find_stock_reply(make_find_stock_reply(order_id, "item-B", 2, price=25))

            client.post(f"/checkout/{order_id}")

        # Confirm stock for item-A first — not yet PAYMENT_PENDING
        handle_stock_subtracted_reply(make_stock_subtracted_reply(order_id, "item-A", 1))
        self.assertEqual(get_order(order_id).status, STOCK_PENDING)

        # Confirm stock for item-B — now all confirmed
        handle_stock_subtracted_reply(make_stock_subtracted_reply(order_id, "item-B", 2))
        self.assertEqual(get_order(order_id).status, PAYMENT_PENDING)

        # Payment succeeds
        total = 1 * 100 + 2 * 25  # 150
        handle_payment_reply(make_payment_reply(order_id, "user-7", amount=total, success=True))
        self.assertEqual(get_order(order_id).status, ORDER_COMPLETED)
        self.assertEqual(get_order(order_id).total_cost, total)

    # ── 7. find_order endpoint ─────────────────────────────────────────────────

    def test_find_order_returns_correct_data(self):
        order_id, _ = make_order("user-5")
        handle_find_stock_reply(make_find_stock_reply(order_id, "item-A", 2, price=10))
        with svc.app.test_client() as client:
            resp = client.get(f"/find/{order_id}")
        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        self.assertEqual(data["order_id"], order_id)
        self.assertEqual(data["user_id"], "user-5")
        self.assertEqual(data["total_cost"], 20)
        self.assertEqual(data["status"], ORDER_CREATED)


if __name__ == "__main__":
    unittest.main(verbosity=2)