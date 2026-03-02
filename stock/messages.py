import msgspec


class BaseMessage(msgspec.Struct, kw_only=True):
    pass


# outgoing messages from Stock service
class FindStockReply(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    item_id: str
    found: bool
    quantity: int
    stock: int | None = None
    price: int | None = None
    type: str = "FindStockReply"


# incoming message to Stock service
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


class StockSubtractedReply(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    item_id: str
    quantity: int
    success: bool
    error: str | None = None
    type: str = "StockSubtractedReply"


class RollbackStockRequest(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    item_id: str
    quantity: int
    type: str = "RollbackStockRequest"


class RollbackStockReply(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    item_id: str
    quantity: int
    success: bool
    error: str | None = None
    type: str = "RollbackStockReply"


MESSAGE_TYPES: dict[str, type[BaseMessage]] = {
    "FindStock": FindStock,
    "FindStockReply": FindStockReply,
    "SubtractStock": SubtractStock,
    "StockSubtractedReply": StockSubtractedReply,
    "RollbackStockRequest": RollbackStockRequest,
    "RollbackStockReply": RollbackStockReply,
}
