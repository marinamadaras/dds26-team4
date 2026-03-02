import unittest
import uuid
import subprocess

import utils as tu


class TestMicroservices(unittest.TestCase):

    def test_stock(self):
        # Test /stock/item/create/<price>
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        # Test /stock/find/<item_id>
        item: dict = tu.find_item(item_id)
        self.assertEqual(item['price'], 5)
        self.assertEqual(item['stock'], 0)

        # Test /stock/add/<item_id>/<number>
        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(200 <= int(add_stock_response) < 300)

        stock_after_add: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_add, 50)

        # Test /stock/subtract/<item_id>/<number>
        over_subtract_stock_response = tu.subtract_stock(item_id, 200)
        self.assertTrue(tu.status_code_is_failure(int(over_subtract_stock_response)))

        subtract_stock_response = tu.subtract_stock(item_id, 15)
        self.assertTrue(tu.status_code_is_success(int(subtract_stock_response)))

        stock_after_subtract: int = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_subtract, 35)

    def test_payment(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # Test /users/credit/add/<user_id>/<amount>
        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(add_credit_response))

        # add item to the stock service
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item)

        item_id: str = item['item_id']

        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))

        payment_response = tu.payment_pay(user_id, 10)
        self.assertTrue(tu.status_code_is_success(payment_response))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 5)

    def test_order(self):
        # Test /payment/pay/<user_id>/<order_id>
        user: dict = tu.create_user()
        self.assertIn('user_id', user)

        user_id: str = user['user_id']

        # create order in the order service and add item to the order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order)

        order_id: str = order['order_id']

        # add item to the stock service
        item1: dict = tu.create_item(5)
        self.assertIn('item_id', item1)
        item_id1: str = item1['item_id']
        add_stock_response = tu.add_stock(item_id1, 15)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        # add item to the stock service
        item2: dict = tu.create_item(5)
        self.assertIn('item_id', item2)
        item_id2: str = item2['item_id']
        add_stock_response = tu.add_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_stock_response))

        add_item_response = tu.add_item_to_order(order_id, item_id1, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        add_item_response = tu.add_item_to_order(order_id, item_id2, 1)
        self.assertTrue(tu.status_code_is_success(add_item_response))
        subtract_stock_response = tu.subtract_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(subtract_stock_response))

        checkout_response = tu.checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 15)

        add_stock_response = tu.add_stock(item_id2, 15)
        self.assertTrue(tu.status_code_is_success(int(add_stock_response)))

        credit_after_payment: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 0)

        checkout_response = tu.checkout_order(order_id).status_code
        self.assertTrue(tu.status_code_is_failure(checkout_response))

        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(int(add_credit_response)))

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 15)

        stock: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock, 15)

        checkout_response = tu.checkout_order(order_id)
        print(checkout_response.text)
        self.assertTrue(tu.status_code_is_success(checkout_response.status_code))

        stock_after_subtract: int = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after_subtract, 14)

        credit: int = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 5)

    def test_2pc_participant_endpoints_are_idempotent(self):
        user: dict = tu.create_user()
        user_id: str = user['user_id']
        self.assertTrue(tu.status_code_is_success(tu.add_credit_to_user(user_id, 100)))

        item: dict = tu.create_item(7)
        item_id: str = item['item_id']
        self.assertTrue(tu.status_code_is_success(tu.add_stock(item_id, 10)))

        tx_id = str(uuid.uuid4())
        amount = 21

        stock_prepare_1 = tu.stock_prepare(tx_id, [(item_id, 3)])
        stock_prepare_2 = tu.stock_prepare(tx_id, [(item_id, 3)])
        payment_prepare_1 = tu.payment_prepare(tx_id, user_id, amount)
        payment_prepare_2 = tu.payment_prepare(tx_id, user_id, amount)

        self.assertEqual(stock_prepare_1.status_code, 200)
        self.assertEqual(stock_prepare_2.status_code, 200)
        self.assertEqual(payment_prepare_1.status_code, 200)
        self.assertEqual(payment_prepare_2.status_code, 200)

        stock_commit_1 = tu.stock_commit(tx_id)
        stock_commit_2 = tu.stock_commit(tx_id)
        payment_commit_1 = tu.payment_commit(tx_id)
        payment_commit_2 = tu.payment_commit(tx_id)

        self.assertEqual(stock_commit_1.status_code, 200)
        self.assertEqual(stock_commit_2.status_code, 200)
        self.assertEqual(payment_commit_1.status_code, 200)
        self.assertEqual(payment_commit_2.status_code, 200)

        item_after = tu.find_item(item_id)
        user_after = tu.find_user(user_id)
        self.assertEqual(item_after["stock"], 7)
        self.assertEqual(user_after["credit"], 79)

        # Abort after commit should fail because the terminal state is COMMITTED.
        self.assertTrue(tu.status_code_is_failure(tu.stock_abort(tx_id).status_code))
        self.assertTrue(tu.status_code_is_failure(tu.payment_abort(tx_id).status_code))

    def test_2pc_recovery_commits_decided_transaction_on_restart(self):
        user_id: str = tu.create_user()['user_id']
        self.assertTrue(tu.status_code_is_success(tu.add_credit_to_user(user_id, 100)))

        item_id: str = tu.create_item(11)['item_id']
        self.assertTrue(tu.status_code_is_success(tu.add_stock(item_id, 8)))

        order_id: str = tu.create_order(user_id)['order_id']
        self.assertTrue(tu.status_code_is_success(tu.add_item_to_order(order_id, item_id, 2)))

        tx_id = str(uuid.uuid4())
        self.assertEqual(tu.stock_prepare(tx_id, [(item_id, 2)]).status_code, 200)
        self.assertEqual(tu.payment_prepare(tx_id, user_id, 22).status_code, 200)

        try:
            tu.seed_order_transaction(
                tx_id=tx_id,
                state="DECIDED",
                decision="COMMIT",
                order_id=order_id,
                user_id=user_id,
                total_cost=22,
                items=[(item_id, 2)],
            )
            tu.restart_order_service()
        except (subprocess.CalledProcessError, TimeoutError, FileNotFoundError, PermissionError) as exc:
            self.skipTest(f"Docker compose control unavailable for recovery test: {exc}")

        order_after = tu.find_order(order_id)
        item_after = tu.find_item(item_id)
        user_after = tu.find_user(user_id)

        self.assertTrue(order_after["paid"])
        self.assertEqual(item_after["stock"], 6)
        self.assertEqual(user_after["credit"], 78)

    def test_2pc_recovery_aborts_preparing_transaction_on_restart(self):
        user_id: str = tu.create_user()['user_id']
        self.assertTrue(tu.status_code_is_success(tu.add_credit_to_user(user_id, 50)))

        item_id: str = tu.create_item(10)['item_id']
        self.assertTrue(tu.status_code_is_success(tu.add_stock(item_id, 5)))

        order_id: str = tu.create_order(user_id)['order_id']
        self.assertTrue(tu.status_code_is_success(tu.add_item_to_order(order_id, item_id, 4)))

        tx_id = str(uuid.uuid4())
        self.assertEqual(tu.stock_prepare(tx_id, [(item_id, 4)]).status_code, 200)
        self.assertEqual(tu.payment_prepare(tx_id, user_id, 40).status_code, 200)

        try:
            tu.seed_order_transaction(
                tx_id=tx_id,
                state="PREPARING",
                decision="UNKNOWN",
                order_id=order_id,
                user_id=user_id,
                total_cost=40,
                items=[(item_id, 4)],
            )
            tu.restart_order_service()
        except (subprocess.CalledProcessError, TimeoutError, FileNotFoundError, PermissionError) as exc:
            self.skipTest(f"Docker compose control unavailable for recovery test: {exc}")

        # If abort cleanup succeeded, the reserved resources are released and a full-size prepare works again.
        tx_id_2 = str(uuid.uuid4())
        self.assertEqual(tu.stock_prepare(tx_id_2, [(item_id, 5)]).status_code, 200)
        self.assertEqual(tu.payment_prepare(tx_id_2, user_id, 50).status_code, 200)
        self.assertEqual(tu.stock_abort(tx_id_2).status_code, 200)
        self.assertEqual(tu.payment_abort(tx_id_2).status_code, 200)

        self.assertFalse(tu.find_order(order_id)["paid"])


if __name__ == '__main__':
    unittest.main()
