import msgspec


class BaseMessage(msgspec.Struct, kw_only=True):
    pass


# outgoing messages from Order service
class FindStock(BaseMessage):
    idempotency_key: str = ""
    item_id: str
    quantity: int
    type: str = "FindStock"


class SubtractStock(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    item_id: str
    quantity: int
    type: str = "SubtractStock"

class PaymentRequest(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    user_id: str
    amount: int
    type: str = "PaymentRequest"


class RollbackStockRequest(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    item_id: str
    quantity: int
    type: str = "RollbackStockRequest"


class RollbackPaymentRequest(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    user_id: str
    amount: int
    type: str = "RollbackPaymentRequest"

# incoming message to Order service
class FindStockReply(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    item_id: str
    found: bool
    quantity: int
    stock: int | None = None
    price: int | None = None
    type: str = "FindStockReply"


class StockSubtractedReply(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    item_id: str
    quantity: int
    success: bool
    error: str | None = None
    type: str = "StockSubtractedReply"

class PaymentReply(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    user_id: str
    amount: int
    success: bool
    error: str | None = None
    type: str = "PaymentReply"


class RollbackStockReply(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    item_id: str
    quantity: int
    success: bool
    error: str | None = None
    type: str = "RollbackStockReply"


class RollbackPaymentReply(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    user_id: str
    amount: int
    success: bool
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
