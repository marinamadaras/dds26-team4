class OutgoingMessageRecord(Struct):
    idempotency_key: str
    message_type: str
    sent_at: float
    reply_received: bool
    retry_count: int
    order_id: str
    payload: bytes  # serialized message for retry

def log_outgoing_message(idempotency_key: str, message_type: str, order_id: str, message: BaseMessage):
    """Log before sending any message"""
    record = OutgoingMessageRecord(
        idempotency_key=idempotency_key,
        message_type=message_type,
        sent_at=time.time(),
        reply_received=False,
        retry_count=0,
        order_id=order_id,
        payload=msgpack.encode(message)
    )
    key = f"_outgoing:{idempotency_key}"
    db.set(key, msgpack.encode(record))
    app.logger.info(f"Logged outgoing message: {idempotency_key}")

def mark_reply_received(idempotency_key: str):
    """Mark that we received a reply for this message"""
    key = f"_outgoing:{idempotency_key}"
    raw = db.get(key)
    if raw:
        record = msgpack.decode(raw, type=OutgoingMessageRecord)
        record.reply_received = True
        db.set(key, msgpack.encode(record))
        app.logger.info(f"Marked reply received: {idempotency_key}")
