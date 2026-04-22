from __future__ import annotations

import json
import secrets
import time
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any


def decimal_to_str(value: Decimal | float | int | str) -> str:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    text = format(decimal_value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=True, sort_keys=True, default=_json_default)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_to_str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def utc_now() -> datetime:
    return datetime.now(UTC)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def new_client_order_id(prefix: str, symbol: str) -> str:
    symbol_part = symbol.upper()[:5]
    millis = format(int(time.time() * 1000), "x")
    nonce = secrets.token_hex(2)
    candidate = f"{prefix[:4]}-{symbol_part}-{millis}-{nonce}"
    return candidate[:36]


def normalize_param_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return decimal_to_str(value)
    if isinstance(value, Enum):
        return str(value.value)
    return str(value)
