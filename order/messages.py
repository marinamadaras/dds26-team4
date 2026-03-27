import msgspec


class BaseMessage(msgspec.Struct, kw_only=True):
    pass


# outgoing messages from Order service
class FindStock(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    idempotency_key: str = ""
    type: str = "FindStock"


class SubtractStock(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    idempotency_key: str = ""
    type: str = "SubtractStock"

class PaymentRequest(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    idempotency_key: str = ""
    type: str = "PaymentRequest"


class RollbackStockRequest(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    idempotency_key: str = ""
    type: str = "RollbackStockRequest"


class RollbackPaymentRequest(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    idempotency_key: str = ""
    type: str = "RollbackPaymentRequest"


class CheckoutRequested(BaseMessage):
    order_id: str
    type: str = "CheckoutRequested"


class PrepareStockRequest(BaseMessage):
    tx_id: str
    coordinator_partition: int
    items: list[tuple[str, int]]
    type: str = "PrepareStockRequest"

class CompensateOrderRequest(BaseMessage):
    order_id: str
    idempotency_key: str = ""
    error: str | None = None
    type: str = "CompensateOrderRequest"


class PrepareStockReply(BaseMessage):
    tx_id: str
    coordinator_partition: int
    success: bool
    error: str | None = None
    type: str = "PrepareStockReply"


class PreparePaymentRequest(BaseMessage):
    tx_id: str
    coordinator_partition: int
    user_id: str
    amount: int
    type: str = "PreparePaymentRequest"


class PreparePaymentReply(BaseMessage):
    tx_id: str
    coordinator_partition: int
    success: bool
    error: str | None = None
    type: str = "PreparePaymentReply"


class StockDecisionRequest(BaseMessage):
    tx_id: str
    coordinator_partition: int
    decision: str
    type: str = "StockDecisionRequest"


class StockDecisionReply(BaseMessage):
    tx_id: str
    coordinator_partition: int
    decision: str
    success: bool
    error: str | None = None
    type: str = "StockDecisionReply"


class PaymentDecisionRequest(BaseMessage):
    tx_id: str
    coordinator_partition: int
    decision: str
    type: str = "PaymentDecisionRequest"


class PaymentDecisionReply(BaseMessage):
    tx_id: str
    coordinator_partition: int
    decision: str
    success: bool
    error: str | None = None
    type: str = "PaymentDecisionReply"


# incoming message to Order service
class FindStockReply(BaseMessage):
    order_id: str
    item_id: str
    found: bool
    quantity: int
    idempotency_key: str = ""
    stock: int | None = None
    price: int | None = None
    type: str = "FindStockReply"


class StockSubtractedReply(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    success: bool
    idempotency_key: str = ""
    error: str | None = None
    type: str = "StockSubtractedReply"

class PaymentReply(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    success: bool
    idempotency_key: str = ""
    error: str | None = None
    type: str = "PaymentReply"


class RollbackStockReply(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    success: bool
    idempotency_key: str = ""
    error: str | None = None
    type: str = "RollbackStockReply"


class RollbackPaymentReply(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    success: bool
    idempotency_key: str = ""
    error: str | None = None
    type: str = "RollbackPaymentReply"

class Ack(BaseMessage):
    idempotency_key: str = ""
    type: str = "Ack"

class Failure(BaseMessage):
    order_id: str
    idempotency_key: str = ""
    type: str = "Failure"


MESSAGE_TYPES: dict[str, type[BaseMessage]] = {
    "SubtractStock": SubtractStock,
    "StockSubtractedReply": StockSubtractedReply,
    "FindStock": FindStock,
    "FindStockReply": FindStockReply,
    "PaymentRequest": PaymentRequest,
    "PaymentReply": PaymentReply,
    "RollbackStockRequest": RollbackStockRequest,
    "RollbackStockReply": RollbackStockReply,
    "RollbackPaymentRequest": RollbackPaymentRequest,
    "RollbackPaymentReply": RollbackPaymentReply,
    "CheckoutRequested": CheckoutRequested,
    "PrepareStockRequest": PrepareStockRequest,
    "PrepareStockReply": PrepareStockReply,
    "PreparePaymentRequest": PreparePaymentRequest,
    "PreparePaymentReply": PreparePaymentReply,
    "StockDecisionRequest": StockDecisionRequest,
    "StockDecisionReply": StockDecisionReply,
    "PaymentDecisionRequest": PaymentDecisionRequest,
    "PaymentDecisionReply": PaymentDecisionReply,
    "Ack": Ack,
    "Failure" : Failure
}
