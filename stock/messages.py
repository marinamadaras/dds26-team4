import msgspec


class BaseMessage(msgspec.Struct, kw_only=True):
    pass


# outgoing messages from Stock service
class FindStockReply(BaseMessage):
    order_id: str
    item_id: str
    found: bool
    quantity: int
    idempotency_key: str = ""
    stock: int | None = None
    price: int | None = None
    type: str = "FindStockReply"


# incoming message to Stock service
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


class StockSubtractedReply(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    success: bool
    idempotency_key: str = ""
    error: str | None = None
    type: str = "StockSubtractedReply"


class RollbackStockRequest(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    idempotency_key: str = ""
    type: str = "RollbackStockRequest"


class RollbackStockReply(BaseMessage):
    order_id: str
    item_id: str
    quantity: int
    success: bool
    idempotency_key: str = ""
    error: str | None = None
    type: str = "RollbackStockReply"


class PrepareStockRequest(BaseMessage):
    tx_id: str
    coordinator_partition: int
    items: list[tuple[str, int]]
    type: str = "PrepareStockRequest"
    idempotency_key: str = ""


class PrepareStockReply(BaseMessage):
    tx_id: str
    coordinator_partition: int
    success: bool
    error: str | None = None
    type: str = "PrepareStockReply"
    idempotency_key: str = ""


class StockDecisionRequest(BaseMessage):
    tx_id: str
    coordinator_partition: int
    decision: str
    type: str = "StockDecisionRequest"
    idempotency_key: str = ""


class StockDecisionReply(BaseMessage):
    tx_id: str
    coordinator_partition: int
    decision: str
    success: bool
    error: str | None = None
    type: str = "StockDecisionReply"
    idempotency_key: str = ""


MESSAGE_TYPES: dict[str, type[BaseMessage]] = {
    "FindStock": FindStock,
    "FindStockReply": FindStockReply,
    "SubtractStock": SubtractStock,
    "StockSubtractedReply": StockSubtractedReply,
    "RollbackStockRequest": RollbackStockRequest,
    "RollbackStockReply": RollbackStockReply,
    "PrepareStockRequest": PrepareStockRequest,
    "PrepareStockReply": PrepareStockReply,
    "StockDecisionRequest": StockDecisionRequest,
    "StockDecisionReply": StockDecisionReply,
}
