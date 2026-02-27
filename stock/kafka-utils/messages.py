import msgspec


class BaseMessage(msgspec.Struct, kw_only=True):
    type: str

# outgoing messages from Stock service

## to topic: find.stock.replies
class FindStockReply(BaseMessage):
    order_id: str
    item_id: str
    found: bool
    quantity: int
    stock: int | None = None
    price: int | None = None
    type: str = "FindStockReply"

# incoming message to Stock service
## from topic: find.stock
class FindStock(BaseMessage):
    item_id: str
    quantity: int
    type: str = "FindStock"

MESSAGE_TYPES: dict[str, type[BaseMessage]] = {
    FindStock.type: FindStock,
    FindStockReply.type: FindStockReply,
}
