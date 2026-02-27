import msgspec


class BaseMessage(msgspec.Struct, kw_only=True):
    type: str

# outgoing messages from Order service

## to topic: find.stock
class FindStock(BaseMessage):
    item_id: str
    quantity: int
    type: str = "FindStock"

## to topic: subtract.stock
class SubtractStock(BaseMessage):
    order_id: str
    type: str = "RemoveStock"


# incoming message to Order service

## from topic find.stock.replies
class FindStockReply(BaseMessage):
    order_id: str
    item_id: str
    found: bool
    quantity: int
    stock: int | None = None
    price: int | None = None
    type: str = "FindStockReply"

## from topic subtract.stock.replies
class StockSubtractedReply(BaseMessage):
    order_id: str
    type: str = "StockRemovedReply"


MESSAGE_TYPES: dict[str, type[BaseMessage]] = {
    SubtractStock.type: SubtractStock,
    StockSubtractedReply.type: StockSubtractedReply,
    FindStock.type: FindStock,
    FindStockReply.type: FindStockReply,
}
