from __future__ import annotations

import time
from dataclasses import replace
from decimal import Decimal
from typing import Any

from .config import Settings
from .exceptions import BinanceExecutionUnknown, ConfigError, RiskRejected
from .filters import SymbolRules
from .rest import BinanceSpotRestClient
from .risk import RiskGate
from .signing import Authenticator, build_signer
from .state import SQLiteStateStore
from .strategy import DipBuyStrategy
from .types import OrderRequest, OrderSide, OrderType, SubmissionMode, TimeInForce
from .utils import decimal_to_str, new_client_order_id
from .ws_market import MarketStreamClient
from .ws_user import UserDataStreamClient


class TradingService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        signer = build_signer(settings) if settings.binance_api_key else None
        self.authenticator = None
        if signer and settings.binance_api_key:
            self.authenticator = Authenticator(
                api_key=settings.binance_api_key,
                signer=signer,
                recv_window_ms=Decimal(str(settings.recv_window_ms)),
            )

        self.state = SQLiteStateStore(settings.state_db_path)
        self.risk = RiskGate(settings, self.state)
        self.rest = BinanceSpotRestClient(settings, self.authenticator)
        self.market = MarketStreamClient(settings)
        self.user_stream = UserDataStreamClient(settings, self.authenticator)

    async def __aenter__(self) -> "TradingService":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self.rest.close()

    def _resolve_submission_mode(self, *, live: bool = False, test_order: bool = False, submission_mode: SubmissionMode | None = None) -> SubmissionMode:
        if submission_mode is not None:
            return submission_mode
        if live and test_order:
            raise ValueError("--live and --test-order are mutually exclusive")
        if live:
            return SubmissionMode.LIVE
        if test_order:
            return SubmissionMode.TEST
        return SubmissionMode.DRY_RUN if self.settings.dry_run else SubmissionMode.LIVE

    async def doctor(self, symbol: str | None = None) -> dict[str, Any]:
        selected_symbol = (symbol or self.settings.default_symbol).upper()
        await self.rest.ping()
        server_time = await self.rest.sync_time()
        local_time = int(time.time() * 1000)
        rules = await self.rest.get_symbol_rules(selected_symbol)
        payload: dict[str, Any] = {
            "environment": self.settings.binance_env.value,
            "rest_base_url": self.settings.resolved_rest_base_url,
            "market_ws_url": self.settings.resolved_market_ws_url,
            "ws_api_url": self.settings.resolved_ws_api_url,
            "server_time_ms": server_time,
            "clock_skew_ms": server_time - local_time,
            "symbol_rules": rules.summary(),
        }
        if self.authenticator:
            account = await self.rest.get_account()
            balances = []
            for balance in account.get("balances", []):
                free = Decimal(str(balance["free"]))
                locked = Decimal(str(balance["locked"]))
                if free > 0 or locked > 0:
                    balances.append(balance)
            payload["account"] = {
                "canTrade": account.get("canTrade"),
                "canWithdraw": account.get("canWithdraw"),
                "canDeposit": account.get("canDeposit"),
                "balances": balances[:20],
            }
        return payload

    async def price(self, symbol: str) -> dict[str, Any]:
        price = await self.rest.get_price(symbol.upper())
        return {"symbol": symbol.upper(), "price": decimal_to_str(price)}

    async def account(self) -> dict[str, Any]:
        if not self.authenticator:
            raise ConfigError("API credentials are required for account queries")
        return await self.rest.get_account()

    async def _prepare_order(self, order: OrderRequest) -> tuple[OrderRequest, SymbolRules, Decimal | None]:
        order = order.normalized()
        rules = await self.rest.get_symbol_rules(order.symbol)
        quantity = order.quantity
        price = order.price
        if quantity is not None:
            quantity = rules.adjust_quantity(quantity, market=order.order_type is OrderType.MARKET)
        if price is not None:
            price = rules.adjust_price(price)

        prepared = replace(
            order,
            quantity=quantity,
            price=price,
            new_client_order_id=order.new_client_order_id or new_client_order_id(self.settings.order_prefix, order.symbol),
        )
        prepared.validate()

        reference_price = None
        if prepared.order_type is OrderType.MARKET or prepared.quote_order_qty is not None:
            reference_price = await self.rest.get_price(prepared.symbol)
        return prepared, rules, reference_price

    async def submit_order(
        self,
        order: OrderRequest,
        *,
        submission_mode: SubmissionMode,
    ) -> dict[str, Any]:
        prepared, rules, reference_price = await self._prepare_order(order)
        decision = self.risk.evaluate(prepared, rules, reference_price=reference_price)
        if not decision.allowed:
            self.state.record_order_request(prepared, submission_mode)
            rejection = {
                "status": "LOCAL_REJECTED",
                "clientOrderId": prepared.new_client_order_id,
                "symbol": prepared.symbol,
                "reasons": decision.reasons,
                "estimatedNotional": None if decision.estimated_notional is None else decimal_to_str(decision.estimated_notional),
            }
            self.state.record_order_result(prepared.new_client_order_id or "", rejection, fallback_status="LOCAL_REJECTED")
            raise RiskRejected(decision.reasons)

        self.state.record_order_request(prepared, submission_mode)

        if submission_mode is SubmissionMode.DRY_RUN:
            result = {
                "status": "DRY_RUN",
                "clientOrderId": prepared.new_client_order_id,
                "symbol": prepared.symbol,
                "estimatedNotional": None if decision.estimated_notional is None else decimal_to_str(decision.estimated_notional),
                "request": prepared.to_rest_params(),
            }
            self.state.record_order_result(prepared.new_client_order_id or "", result, fallback_status="DRY_RUN")
            return result

        if submission_mode is SubmissionMode.TEST:
            if not self.authenticator:
                raise ConfigError("API credentials are required for test orders")
            response = await self.rest.test_order(prepared)
            result = {
                "status": "TEST_ACCEPTED",
                "clientOrderId": prepared.new_client_order_id,
                "symbol": prepared.symbol,
                "request": prepared.to_rest_params(),
                "binance": response,
            }
            self.state.record_order_result(prepared.new_client_order_id or "", result, fallback_status="TEST_ACCEPTED")
            return result

        if not self.authenticator:
            raise ConfigError("API credentials are required for live orders")

        try:
            response = await self.rest.place_order(prepared)
        except BinanceExecutionUnknown as exc:
            unknown = {
                "status": "PENDING_UNKNOWN",
                "clientOrderId": prepared.new_client_order_id,
                "symbol": prepared.symbol,
                "error": str(exc),
                "payload": exc.payload,
            }
            self.state.record_order_result(prepared.new_client_order_id or "", unknown, fallback_status="PENDING_UNKNOWN")
            raise

        self.state.record_order_result(prepared.new_client_order_id or "", response, fallback_status="LIVE_ACCEPTED")
        return response

    async def buy_market(
        self,
        symbol: str,
        *,
        quote_order_qty: Decimal,
        submission_mode: SubmissionMode,
    ) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quote_order_qty=quote_order_qty,
            ),
            submission_mode=submission_mode,
        )

    async def sell_market(
        self,
        symbol: str,
        *,
        quantity: Decimal,
        submission_mode: SubmissionMode,
    ) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=quantity,
            ),
            submission_mode=submission_mode,
        )

    async def buy_limit(
        self,
        symbol: str,
        *,
        quantity: Decimal,
        price: Decimal,
        submission_mode: SubmissionMode,
    ) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=quantity,
                price=price,
                time_in_force=TimeInForce.GTC,
            ),
            submission_mode=submission_mode,
        )

    async def sell_limit(
        self,
        symbol: str,
        *,
        quantity: Decimal,
        price: Decimal,
        submission_mode: SubmissionMode,
    ) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                quantity=quantity,
                price=price,
                time_in_force=TimeInForce.GTC,
            ),
            submission_mode=submission_mode,
        )

    async def order_status(
        self,
        symbol: str,
        *,
        client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> dict[str, Any]:
        payload = await self.rest.get_order(symbol, client_order_id=client_order_id, order_id=order_id)
        self.state.apply_exchange_order_snapshot(payload)
        return payload

    async def cancel(
        self,
        symbol: str,
        *,
        client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> dict[str, Any]:
        payload = await self.rest.cancel_order(symbol, client_order_id=client_order_id, order_id=order_id)
        self.state.apply_exchange_order_snapshot(payload)
        return payload

    async def reconcile(self, symbol: str | None = None) -> dict[str, Any]:
        open_orders = await self.rest.get_open_orders(symbol)
        for order in open_orders:
            self.state.apply_exchange_order_snapshot(order)
        return {"open_orders": open_orders}

    async def market_messages(self, symbol: str, stream: str, *, reconnect: bool = True):
        stream_name = stream if "@" in stream else f"{symbol.lower()}@{stream}"
        async for message in self.market.listen([stream_name], reconnect=reconnect):
            yield message

    async def user_messages(self, *, reconnect: bool = True):
        async for message in self.user_stream.listen(reconnect=reconnect):
            self.state.apply_user_stream_message(message)
            yield message

    async def run_demo_strategy(
        self,
        symbol: str,
        *,
        quote_order_qty: Decimal,
        lookback: int,
        trigger_pct: Decimal,
        submission_mode: SubmissionMode,
    ) -> dict[str, Any]:
        strategy = DipBuyStrategy(
            symbol=symbol.upper(),
            quote_order_qty=quote_order_qty,
            lookback=lookback,
            trigger_pct=trigger_pct,
        )
        return await strategy.run(self, submission_mode=submission_mode)
