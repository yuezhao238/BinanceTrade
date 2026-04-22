from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class BinanceTradeError(Exception):
    """Base application exception."""


class ConfigError(BinanceTradeError):
    """Configuration is incomplete or invalid."""


@dataclass(slots=True)
class BinanceAPIError(BinanceTradeError):
    http_status: int
    message: str
    code: int | None = None
    payload: Any | None = None
    retry_after: str | None = None

    def __str__(self) -> str:
        prefix = f"http={self.http_status}"
        if self.code is not None:
            prefix += f" code={self.code}"
        return f"{prefix} {self.message}"


class BinanceExecutionUnknown(BinanceAPIError):
    """The matching engine status is unknown and must be reconciled."""


@dataclass(slots=True)
class RiskRejected(BinanceTradeError):
    reasons: list[str]

    def __str__(self) -> str:
        return "; ".join(self.reasons)
