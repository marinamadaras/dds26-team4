import msgspec


class BaseMessage(msgspec.Struct, kw_only=True):
    pass


# outgoing messages from Stock service
class FindStockReply(BaseMessage):
    order_id: str
    item_id: str
    found: bool
    quantity: int
    stock: int | None = None
    price: int | None = None
    type: str = "FindStockReply"


# incoming message to Stock service
class FindStock(BaseMessage):
    item_id: str
    quantity: int
    type: str = "FindStock"


MESSAGE_TYPES: dict[str, type[BaseMessage]] = {
    "FindStock": FindStock,
    "FindStockReply": FindStockReply,
}
