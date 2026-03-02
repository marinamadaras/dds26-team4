import msgspec


class BaseMessage(msgspec.Struct, kw_only=True):
    pass


class PaymentRequest(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    user_id: str
    amount: int
    type: str = "PaymentRequest"


class PaymentReply(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    user_id: str
    amount: int
    success: bool
    error: str | None = None
    type: str = "PaymentReply"


class RollbackPaymentRequest(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    user_id: str
    amount: int
    type: str = "RollbackPaymentRequest"


class RollbackPaymentReply(BaseMessage):
    idempotency_key: str = ""
    order_id: str
    user_id: str
    amount: int
    success: bool
    error: str | None = None
    type: str = "RollbackPaymentReply"


MESSAGE_TYPES: dict[str, type[BaseMessage]] = {
    "PaymentRequest": PaymentRequest,
    "PaymentReply": PaymentReply,
    "RollbackPaymentRequest": RollbackPaymentRequest,
    "RollbackPaymentReply": RollbackPaymentReply,
}
