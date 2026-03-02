import msgspec


class BaseMessage(msgspec.Struct, kw_only=True):
    pass


class PaymentRequest(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    idempotency_key: str
    type: str = "PaymentRequest"


class PaymentReply(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    success: bool
    idempotency_key: str
    error: str | None = None
    type: str = "PaymentReply"


class RollbackPaymentRequest(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    idempotency_key: str
    type: str = "RollbackPaymentRequest"


class RollbackPaymentReply(BaseMessage):
    order_id: str
    user_id: str
    amount: int
    success: bool
    idempotency_key: str
    error: str | None = None
    type: str = "RollbackPaymentReply"


MESSAGE_TYPES: dict[str, type[BaseMessage]] = {
    "PaymentRequest": PaymentRequest,
    "PaymentReply": PaymentReply,
    "RollbackPaymentRequest": RollbackPaymentRequest,
    "RollbackPaymentReply": RollbackPaymentReply,
}
