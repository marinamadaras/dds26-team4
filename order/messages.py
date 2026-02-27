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
    type: str = "RemoveStock"


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
    type: str = "StockRemovedReply"


MESSAGE_TYPES: dict[str, type[BaseMessage]] = {
    "RemoveStock": SubtractStock,
    "StockRemovedReply": StockSubtractedReply,
    "FindStock": FindStock,
    "FindStockReply": FindStockReply,
}
