from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


class BinanceTradeError(Exception):
    """Base application exception."""


class ConfigError(BinanceTradeError):
    """Configuration is incomplete or invalid."""


class NetworkError(BinanceTradeError):
    """Network transport to Binance failed."""


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


def format_transport_error(exc: Exception, *, target: str, trust_env: bool, attempts: int = 1) -> str:
    attempt_note = f" after {attempts} attempts" if attempts > 1 else ""
    detail = str(exc).strip() or exc.__class__.__name__

    if isinstance(exc, httpx.ProxyError):
        hint = (
            "The process attempted to use an HTTP(S) proxy and the proxy rejected the request. "
            "If Binance should be reached directly, set NETWORK_TRUST_ENV=false or clear "
            "HTTP_PROXY, HTTPS_PROXY, ALL_PROXY, and NO_PROXY."
        )
        if not trust_env:
            hint = (
                "A proxy failure happened even though NETWORK_TRUST_ENV=false. Verify local network interception, "
                "VPN/proxy software, or an upstream gateway."
            )
        return f"proxy_error{attempt_note} target={target} detail={detail}. {hint}"

    if isinstance(exc, httpx.TimeoutException):
        return (
            f"timeout{attempt_note} target={target} detail={detail}. "
            "Verify connectivity to the selected Binance endpoint and increase REQUEST_TIMEOUT_SECONDS only if the "
            "network path is actually healthy."
        )

    if isinstance(exc, httpx.ConnectError):
        return (
            f"connect_error{attempt_note} target={target} detail={detail}. "
            "Check DNS resolution, outbound connectivity, proxy settings, and exchange reachability."
        )

    return (
        f"network_error{attempt_note} target={target} detail={detail}. "
        "Check connectivity, proxy settings, and Binance endpoint availability."
    )


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
