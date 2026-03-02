import msgspec


class BaseMessage(msgspec.Struct, kw_only=True):
    pass


# outgoing messages from Order service
class FindStock(BaseMessage):
    item_id: str
    quantity: int
    idempotency_key: str
    type: str = "FindStock"


class SubtractStock(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    idempotency_key: str
    type: str = "SubtractStock"

class PaymentRequest(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    idempotency_key: str
    type: str = "PaymentRequest"


class RollbackStockRequest(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    idempotency_key: str
    type: str = "RollbackStockRequest"


class RollbackPaymentRequest(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    idempotency_key: str
    type: str = "RollbackPaymentRequest"

# incoming message to Order service
class FindStockReply(BaseMessage):
    order_id: str
    item_id: str
    found: bool
    quantity: int
    idempotency_key: str
    stock: int | None = None
    price: int | None = None
    type: str = "FindStockReply"


class StockSubtractedReply(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    success: bool
    idempotency_key: str
    error: str | None = None
    type: str = "StockSubtractedReply"

class PaymentReply(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    success: bool
    idempotency_key: str
    error: str | None = None
    type: str = "PaymentReply"


class RollbackStockReply(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    success: bool
    idempotency_key: str
    error: str | None = None
    type: str = "RollbackStockReply"


class RollbackPaymentReply(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    success: bool
    idempotency_key: str
    error: str | None = None
    type: str = "RollbackPaymentReply"


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
}
