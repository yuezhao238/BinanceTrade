from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any

import httpx

from .config import Settings
from .exceptions import BinanceAPIError, BinanceExecutionUnknown, ConfigError, NetworkError, format_transport_error, with_restricted_location_hint
from .filters import SymbolRules, parse_symbol_rules
from .signing import Authenticator
from .types import MarketType, OrderRequest

LOGGER = logging.getLogger(__name__)


class BinanceSpotRestClient:
    def __init__(self, settings: Settings, authenticator: Authenticator | None = None) -> None:
        self.settings = settings
        self.authenticator = authenticator
        headers = {"User-Agent": "binance-trade/0.1.0"}
        if authenticator:
            headers["X-MBX-APIKEY"] = authenticator.api_key
        self._client = httpx.AsyncClient(
            base_url=self.settings.resolved_rest_base_url,
            timeout=self.settings.request_timeout_seconds,
            headers=headers,
            trust_env=self.settings.network_trust_env,
        )
        self._rules_cache: dict[str, SymbolRules] = {}

    async def __aenter__(self) -> "BinanceSpotRestClient":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self._client.aclose()

    def _raise_transport_error(self, exc: httpx.TransportError, *, method: str, path: str, attempts: int) -> None:
        target = f"{method.upper()} {self.settings.resolved_rest_base_url}{path}"
        raise NetworkError(
            format_transport_error(
                exc,
                target=target,
                trust_env=self.settings.network_trust_env,
                attempts=attempts,
            )
        ) from exc

    async def _request_public(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        attempts = 3
        for attempt in range(1, attempts + 1):
            try:
                response = await self._client.request(method.upper(), path, params=params)
                return self._handle_response(response)
            except httpx.TransportError as exc:
                if attempt >= attempts:
                    self._raise_transport_error(exc, method=method, path=path, attempts=attempt)
                await asyncio.sleep(0.5 * attempt)
        raise RuntimeError("unreachable")

    async def _request_signed(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        if not self.authenticator:
            raise ConfigError("API credentials are required for signed requests")

        payload = self.authenticator.build_rest_signed_payload(params or {})
        method = method.upper()
        try:
            if method in {"GET", "DELETE"}:
                response = await self._client.request(method, f"{path}?{payload}")
            else:
                response = await self._client.request(
                    method,
                    path,
                    content=payload,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
        except httpx.TransportError as exc:
            self._raise_transport_error(exc, method=method, path=path, attempts=1)
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
        if response.status_code == 451 or "restricted location" in lowered or "eligibility" in lowered:
            message = with_restricted_location_hint(
                str(message),
                environment=self.settings.binance_env.value,
                market_type=MarketType.SPOT.value,
            )
        if code == -1007 or "status unknown" in lowered or "execution status unknown" in lowered:
            error_cls = BinanceExecutionUnknown

        raise error_cls(
            http_status=response.status_code,
            code=code,
            message=str(message),
            payload=payload,
            retry_after=retry_after,
        )

    async def ping(self) -> bool:
        await self._request_public("GET", "/api/v3/ping")
        return True

    async def server_time(self) -> int:
        payload = await self._request_public("GET", "/api/v3/time")
        return int(payload["serverTime"])

    async def sync_time(self) -> int:
        server_time = await self.server_time()
        if self.authenticator:
            self.authenticator.set_server_time(server_time)
        return server_time

    async def get_exchange_info(self, symbol: str | None = None) -> dict[str, Any]:
        params = {"symbol": symbol.upper()} if symbol else None
        return await self._request_public("GET", "/api/v3/exchangeInfo", params=params)

    async def get_symbol_rules(self, symbol: str) -> SymbolRules:
        symbol = symbol.upper()
        cached = self._rules_cache.get(symbol)
        if cached:
            return cached
        exchange_info = await self.get_exchange_info(symbol)
        symbols = exchange_info.get("symbols", [])
        if not symbols:
            raise BinanceAPIError(http_status=404, message=f"symbol {symbol} not found in exchangeInfo")
        rules = parse_symbol_rules(symbols[0], market_type=MarketType.SPOT)
        self._rules_cache[symbol] = rules
        return rules

    async def get_price(self, symbol: str) -> Decimal:
        payload = await self._request_public("GET", "/api/v3/ticker/price", params={"symbol": symbol.upper()})
        return Decimal(str(payload["price"]))

    async def get_klines(self, symbol: str, interval: str, *, limit: int = 500) -> list[dict[str, Any]]:
        return await self.get_klines_window(symbol, interval, limit=limit)

    async def get_klines_window(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int = 500,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        payload = await self._request_public(
            "GET",
            "/api/v3/klines",
            params=params,
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
        return await self._request_signed("GET", "/api/v3/account")

    async def get_wallet_balance(self, *, quote_asset: str = "USDT") -> Any:
        return await self._request_signed("GET", "/sapi/v1/asset/wallet/balance", params={"quoteAsset": quote_asset.upper()})

    async def get_user_assets(self, *, asset: str | None = None) -> Any:
        params: dict[str, Any] = {}
        if asset:
            params["asset"] = asset.upper()
        return await self._request_signed("POST", "/sapi/v3/asset/getUserAsset", params=params)

    async def get_funding_assets(self, *, asset: str | None = None) -> Any:
        params: dict[str, Any] = {}
        if asset:
            params["asset"] = asset.upper()
        return await self._request_signed("POST", "/sapi/v1/asset/get-funding-asset", params=params)

    async def get_simple_earn_account(self) -> Any:
        return await self._request_signed("GET", "/sapi/v1/simple-earn/account")

    async def get_simple_earn_flexible_positions(self, *, asset: str | None = None, size: int = 100) -> Any:
        params: dict[str, Any] = {"size": size}
        if asset:
            params["asset"] = asset.upper()
        return await self._request_signed("GET", "/sapi/v1/simple-earn/flexible/position", params=params)

    async def get_simple_earn_locked_positions(self, *, asset: str | None = None, size: int = 100) -> Any:
        params: dict[str, Any] = {"size": size}
        if asset:
            params["asset"] = asset.upper()
        return await self._request_signed("GET", "/sapi/v1/simple-earn/locked/position", params=params)

    async def redeem_simple_earn_flexible(
        self,
        *,
        product_id: str,
        amount: Decimal | None = None,
        redeem_all: bool = False,
        dest_account: str = "SPOT",
    ) -> Any:
        params: dict[str, Any] = {
            "productId": product_id,
            "redeemAll": "true" if redeem_all else "false",
            "destAccount": dest_account.upper(),
        }
        if amount is not None:
            params["amount"] = str(amount)
        return await self._request_signed("POST", "/sapi/v1/simple-earn/flexible/redeem", params=params)

    async def place_order(self, order: OrderRequest) -> dict[str, Any]:
        LOGGER.info("placing live order symbol=%s clientOrderId=%s", order.symbol, order.new_client_order_id)
        return await self._request_signed("POST", "/api/v3/order", params=order.to_rest_params())

    async def test_order(self, order: OrderRequest) -> dict[str, Any]:
        LOGGER.info("sending test order symbol=%s clientOrderId=%s", order.symbol, order.new_client_order_id)
        return await self._request_signed("POST", "/api/v3/order/test", params=order.to_rest_params())

    async def get_order(
        self,
        symbol: str,
        *,
        client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"symbol": symbol.upper()}
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        if order_id is not None:
            params["orderId"] = order_id
        return await self._request_signed("GET", "/api/v3/order", params=params)

    async def cancel_order(
        self,
        symbol: str,
        *,
        client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"symbol": symbol.upper()}
        if client_order_id:
            params["origClientOrderId"] = client_order_id
        if order_id is not None:
            params["orderId"] = order_id
        return await self._request_signed("DELETE", "/api/v3/order", params=params)

    async def get_open_orders(self, symbol: str | None = None) -> list[dict[str, Any]]:
        params = {"symbol": symbol.upper()} if symbol else None
        payload = await self._request_signed("GET", "/api/v3/openOrders", params=params)
        return list(payload)
