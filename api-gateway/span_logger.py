import json
import time
from contextlib import contextmanager


def _normalize(value):
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in value.items()}
    return str(value)


@contextmanager
def span(logger, service: str, name: str, *, trace_id: str = "", **fields):
    started_at = time.perf_counter()
    try:
        yield
    except Exception as exc:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
        payload = {
            "service": service,
            "span": name,
            "trace_id": trace_id,
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(exc),
        }
        payload.update({key: _normalize(value) for key, value in fields.items()})
        logger.info("SPAN %s", json.dumps(payload, sort_keys=True))
        raise
    else:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
        payload = {
            "service": service,
            "span": name,
            "trace_id": trace_id,
            "duration_ms": duration_ms,
            "status": "ok",
        }
        payload.update({key: _normalize(value) for key, value in fields.items()})
        logger.info("SPAN %s", json.dumps(payload, sort_keys=True))


def log_span_event(logger, service: str, name: str, *, trace_id: str = "", **fields):
    payload = {
        "service": service,
        "span": name,
        "trace_id": trace_id,
        "status": "event",
    }
    payload.update({key: _normalize(value) for key, value in fields.items()})
    logger.info("SPAN %s", json.dumps(payload, sort_keys=True))
