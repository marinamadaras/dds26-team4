import msgspec


class BaseMessage(msgspec.Struct, kw_only=True):
    pass


# outgoing messages from Order service
class FindStock(BaseMessage):
    item_id: str
    quantity: int
    type: str = "FindStock"


class SubtractStock(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    type: str = "SubtractStock"

class PaymentRequest(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    type: str = "PaymentRequest"


class RollbackStockRequest(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    type: str = "RollbackStockRequest"


class RollbackPaymentRequest(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    type: str = "RollbackPaymentRequest"


class CheckoutRequested(BaseMessage):
    order_id: str
    type: str = "CheckoutRequested"


# incoming message to Order service
class FindStockReply(BaseMessage):
    order_id: str
    item_id: str
    found: bool
    quantity: int
    stock: int | None = None
    price: int | None = None
    type: str = "FindStockReply"


class StockSubtractedReply(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    success: bool
    error: str | None = None
    type: str = "StockSubtractedReply"

class PaymentReply(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    success: bool
    error: str | None = None
    type: str = "PaymentReply"


class RollbackStockReply(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    success: bool
    error: str | None = None
    type: str = "RollbackStockReply"


class RollbackPaymentReply(BaseMessage):
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
    "CheckoutRequested": CheckoutRequested,
}
