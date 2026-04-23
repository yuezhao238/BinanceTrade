from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class BinanceTradeError(Exception):
    """Base application exception."""


class ConfigError(BinanceTradeError):
    """Configuration is incomplete or invalid."""


def with_restricted_location_hint(message: str, *, environment: str, market_type: str) -> str:
    lowered = message.lower()
    if "restricted location" not in lowered and "eligibility" not in lowered:
        return message

    if market_type == "spot":
        if environment == "binance_us":
            hint = (
                "This request is already targeting Binance.US. Verify that your Binance.US account, KYC state, "
                "and server IP are eligible for the requested service."
            )
        else:
            hint = (
                "This IP cannot access Binance.com Spot/Testnet. If you are using a Binance.US account, set "
                "BINANCE_ENV=binance_us. Otherwise run the bot from a Binance.com-supported jurisdiction/IP. "
                "The project cannot bypass exchange eligibility."
            )
    else:
        hint = (
            "This IP cannot access Binance.com USDⓈ-M Futures. Binance.US support in this project is spot-only, "
            "so futures require a Binance.com-eligible jurisdiction/IP and account."
        )

    return f"{message} Hint: {hint}"


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
