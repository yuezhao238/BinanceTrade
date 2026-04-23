from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import httpx

from .config import Settings
from .exceptions import BinanceAPIError, BinanceExecutionUnknown, ConfigError
from .filters import SymbolRules, parse_symbol_rules
from .signing import Authenticator
from .types import MarketType, OrderRequest

LOGGER = logging.getLogger(__name__)


class BinanceFuturesRestClient:
    def __init__(self, settings: Settings, authenticator: Authenticator | None = None) -> None:
        self.settings = settings
        self.authenticator = authenticator
        headers = {"User-Agent": "binance-trade/0.1.0"}
        if authenticator:
            headers["X-MBX-APIKEY"] = authenticator.api_key
        self._client = httpx.AsyncClient(
            base_url=self.settings.resolved_futures_rest_base_url,
            timeout=self.settings.request_timeout_seconds,
            headers=headers,
        )
        self._rules_cache: dict[str, SymbolRules] = {}

    async def __aenter__(self) -> "BinanceFuturesRestClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    async def _request_public(self, method: str, path: str, *, params: dict[str, Any] | None = None) -> Any:
        response = await self._client.request(method.upper(), path, params=params)
        return self._handle_response(response)

    async def _request_signed(self, method: str, path: str, *, params: dict[str, Any] | None = None) -> Any:
        if not self.authenticator:
            raise ConfigError("API credentials are required for signed requests")

        payload = self.authenticator.build_rest_signed_payload(params or {})
        method = method.upper()
        if method in {"GET", "DELETE"}:
            response = await self._client.request(method, f"{path}?{payload}")
        else:
            response = await self._client.request(
                method,
                path,
                content=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        return self._handle_response(response)

    async def _request_api_key(self, method: str, path: str, *, params: dict[str, Any] | None = None) -> Any:
        if not self.authenticator:
            raise ConfigError("API credentials are required for API-key requests")
        response = await self._client.request(method.upper(), path, params=params)
        return self._handle_response(response)

    def _handle_response(self, response: httpx.Response) -> Any:
        if response.status_code >= 400:
            self._raise_for_status(response)
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError:
            return response.text

    def _raise_for_status(self, response: httpx.Response) -> None:
        retry_after = response.headers.get("Retry-After")
        try:
            payload = response.json()
        except ValueError:
            payload = {"msg": response.text}

        code = payload.get("code") if isinstance(payload, dict) else None
        message = payload.get("msg") if isinstance(payload, dict) else str(payload)
        error_cls: type[BinanceAPIError] = BinanceAPIError
        lowered = str(message).lower()
        if code == -1007 or "status unknown" in lowered or "execution status unknown" in lowered or "unknown error" in lowered:
            error_cls = BinanceExecutionUnknown

        raise error_cls(
            http_status=response.status_code,
            code=code,
            message=str(message),
            payload=payload,
            retry_after=retry_after,
        )

    async def ping(self) -> bool:
        await self._request_public("GET", "/fapi/v1/ping")
        return True

    async def server_time(self) -> int:
        payload = await self._request_public("GET", "/fapi/v1/time")
        return int(payload["serverTime"])

    async def sync_time(self) -> int:
        server_time = await self.server_time()
        if self.authenticator:
            self.authenticator.set_server_time(server_time)
        return server_time

    async def get_exchange_info(self) -> dict[str, Any]:
        return await self._request_public("GET", "/fapi/v1/exchangeInfo")

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        symbol = symbol.upper()
        cached = self._rules_cache.get(symbol)
        if cached:
            return cached
        exchange_info = await self.get_exchange_info()
        for item in exchange_info.get("symbols", []):
            if item.get("symbol") == symbol:
                rules = parse_symbol_rules(item, market_type=MarketType.FUTURES)
                self._rules_cache[symbol] = rules
                return rules
        raise BinanceAPIError(http_status=404, message=f"symbol {symbol} not found in futures exchangeInfo")

    async def get_price(self, symbol: str) -> Decimal:
        payload = await self._request_public("GET", "/fapi/v1/ticker/price", params={"symbol": symbol.upper()})
        return Decimal(str(payload["price"]))

    async def get_klines(self, symbol: str, interval: str, *, limit: int = 500) -> list[dict[str, Any]]:
        payload = await self._request_public(
            "GET",
            "/fapi/v1/klines",
            params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
        )
        return [
            {
                "open_time": item[0],
                "open": item[1],
                "high": item[2],
                "low": item[3],
                "close": item[4],
                "volume": item[5],
                "close_time": item[6],
                "quote_volume": item[7],
                "trade_count": item[8],
                "taker_buy_base_volume": item[9],
                "taker_buy_quote_volume": item[10],
                "is_closed": True,
            }
            for item in payload
        ]

    async def get_account(self) -> dict[str, Any]:
        return await self._request_signed("GET", "/fapi/v3/account")

    async def get_positions(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol.upper()} if symbol else None
        payload = await self._request_signed("GET", "/fapi/v3/positionRisk", params=params)
        return list(payload)

    async def place_order(self, order: OrderRequest) -> dict[str, Any]:
        LOGGER.info("placing futures order symbol=%s clientOrderId=%s", order.symbol, order.new_client_order_id)
        return await self._request_signed("POST", "/fapi/v1/order", params=order.to_rest_params())

    async def test_order(self, order: OrderRequest) -> dict[str, Any]:
        LOGGER.info("sending futures test order symbol=%s clientOrderId=%s", order.symbol, order.new_client_order_id)
        return await self._request_signed("POST", "/fapi/v1/order/test", params=order.to_rest_params())

    async def get_order(self, symbol: str, *, client_order_id: str | None = None, order_id: int | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"symbol": symbol.upper()}
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        if order_id is not None:
            params["orderId"] = order_id
        return await self._request_signed("GET", "/fapi/v1/order", params=params)

    async def cancel_order(self, symbol: str, *, client_order_id: str | None = None, order_id: int | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"symbol": symbol.upper()}
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        if order_id is not None:
            params["orderId"] = order_id
        return await self._request_signed("DELETE", "/fapi/v1/order", params=params)

    async def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol.upper()} if symbol else None
        payload = await self._request_signed("GET", "/fapi/v1/openOrders", params=params)
        return list(payload)

    async def set_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        return await self._request_signed("POST", "/fapi/v1/leverage", params={"symbol": symbol.upper(), "leverage": leverage})

    async def set_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        return await self._request_signed("POST", "/fapi/v1/marginType", params={"symbol": symbol.upper(), "marginType": margin_type})

    async def start_user_stream(self) -> str:
        payload = await self._request_api_key("POST", "/fapi/v1/listenKey")
        return str(payload["listenKey"])

    async def keepalive_user_stream(self, listen_key: str) -> dict[str, Any]:
        return await self._request_api_key("PUT", "/fapi/v1/listenKey", params={"listenKey": listen_key})

    async def close_user_stream(self, listen_key: str) -> dict[str, Any]:
        return await self._request_api_key("DELETE", "/fapi/v1/listenKey", params={"listenKey": listen_key})
