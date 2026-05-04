from __future__ import annotations

import time
from dataclasses import replace
from decimal import Decimal
from itertools import chain
from typing import Any

from .config import Settings
from .exceptions import BinanceAPIError, BinanceExecutionUnknown, BinanceTradeError, ConfigError, RiskRejected
from .filters import SymbolRules
from .futures_rest import BinanceFuturesRestClient
from .futures_ws_user import FuturesUserDataStreamClient
from .rest import BinanceSpotRestClient
from .risk import RiskGate, RiskProfile
from .signing import Authenticator, build_signer
from .state import SQLiteStateStore
from .strategy import DipBuyStrategy
from .strategy_runtime import StrategyRunner
from .types import MarketType, OrderRequest, OrderSide, OrderType, PositionSide, SubmissionMode, TimeInForce
from .utils import decimal_to_str, new_client_order_id
from .ws_market import MarketStreamClient
from .ws_user import UserDataStreamClient


def _build_authenticator(settings: Settings) -> Authenticator | None:
    signer = build_signer(settings) if settings.binance_api_key else None
    if signer and settings.binance_api_key:
        return Authenticator(
            api_key=settings.binance_api_key,
            signer=signer,
            recv_window_ms=Decimal(str(settings.recv_window_ms)),
        )
    return None


def _decimal_or_zero(value: Any) -> Decimal:
    if value in (None, "", False):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _filter_rows_with_positive_total(rows: list[dict[str, Any]], *, fields: tuple[str, ...]) -> list[dict[str, Any]]:
    positive_rows: list[dict[str, Any]] = []
    for row in rows:
        total = sum((_decimal_or_zero(row.get(field)) for field in fields), start=Decimal("0"))
        if total > 0:
            positive_rows.append(row)
    return positive_rows


def _spot_account_summary(account: dict[str, Any]) -> dict[str, Any]:
    balances = []
    for balance in account.get("balances", []):
        free = _decimal_or_zero(balance.get("free"))
        locked = _decimal_or_zero(balance.get("locked"))
        if free > 0 or locked > 0:
            balances.append(balance)
    return {
        "canTrade": account.get("canTrade"),
        "canWithdraw": account.get("canWithdraw"),
        "canDeposit": account.get("canDeposit"),
        "balances": balances,
    }


def _simple_earn_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.get("rows")
        if isinstance(rows, list):
            return [item for item in rows if isinstance(item, dict)]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _simple_earn_summary(payload: Any, *, amount_fields: tuple[str, ...]) -> dict[str, Any]:
    rows = _simple_earn_rows(payload)
    positive = _filter_rows_with_positive_total(rows, fields=amount_fields)
    return {
        "count": len(positive),
        "positions": positive,
        "raw": payload,
    }


def _simple_earn_position_total(position: dict[str, Any]) -> Decimal:
    for field in ("totalAmount", "totalInvestedAmount", "holdingAmount", "amount", "lockedAmount"):
        value = _decimal_or_zero(position.get(field))
        if value > 0:
            return value
    return Decimal("0")


class SpotTradingService:
    market_type = MarketType.SPOT

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.authenticator = _build_authenticator(settings)
        self.state = SQLiteStateStore(settings.state_db_path)
        self.risk = RiskGate(
            RiskProfile(
                market_type=MarketType.SPOT,
                allowed_symbols=settings.allowed_symbols,
                max_order_notional=Decimal(str(settings.max_order_notional)),
                max_open_orders_per_symbol=settings.max_open_orders_per_symbol,
                order_cooldown_seconds=settings.order_cooldown_seconds,
            ),
            self.state,
        )
        self.rest = BinanceSpotRestClient(settings, self.authenticator)
        self.market = MarketStreamClient(settings.resolved_market_ws_url, trust_env=settings.network_trust_env)
        self.user_stream = UserDataStreamClient(
            settings.resolved_ws_api_url,
            self.authenticator,
            trust_env=settings.network_trust_env,
        )

    async def __aenter__(self) -> "SpotTradingService":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self.rest.close()

    def _resolve_submission_mode(
        self,
        *,
        live: bool = False,
        test_order: bool = False,
        submission_mode: SubmissionMode | None = None,
    ) -> SubmissionMode:
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
            "market_type": self.market_type.value,
            "environment": self.settings.binance_env.value,
            "rest_base_url": self.settings.resolved_rest_base_url,
            "market_ws_url": self.settings.resolved_market_ws_url,
            "ws_api_url": self.settings.resolved_ws_api_url,
            "server_time_ms": server_time,
            "clock_skew_ms": server_time - local_time,
            "symbol_rules": rules.summary(),
        }
        if self.authenticator:
            try:
                account = await self.rest.get_account()
            except BinanceTradeError as exc:
                payload["account_error"] = str(exc)
            else:
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

    async def wallet_balance(self, *, quote_asset: str = "USDT") -> dict[str, Any]:
        if not self.authenticator:
            raise ConfigError("API credentials are required for wallet balance queries")
        payload = await self.rest.get_wallet_balance(quote_asset=quote_asset)
        return {
            "environment": self.settings.binance_env.value,
            "quote_asset": quote_asset.upper(),
            "wallet_balance": payload,
        }

    async def user_assets(self, *, asset: str | None = None) -> dict[str, Any]:
        if not self.authenticator:
            raise ConfigError("API credentials are required for user asset queries")
        payload = await self.rest.get_user_assets(asset=asset)
        rows = [item for item in payload if isinstance(item, dict)] if isinstance(payload, list) else []
        positive = _filter_rows_with_positive_total(
            rows,
            fields=("free", "locked", "freeze", "withdrawing", "ipoable", "btcValuation"),
        )
        return {
            "environment": self.settings.binance_env.value,
            "asset_filter": asset.upper() if asset else None,
            "count": len(positive),
            "assets": positive,
            "raw": payload,
        }

    async def portfolio_overview(self, *, quote_asset: str = "USDT", asset: str | None = None) -> dict[str, Any]:
        if not self.authenticator:
            raise ConfigError("API credentials are required for portfolio queries")

        payload: dict[str, Any] = {
            "environment": self.settings.binance_env.value,
            "quote_asset": quote_asset.upper(),
            "asset_filter": asset.upper() if asset else None,
        }

        account = await self.rest.get_account()
        payload["spot_account"] = _spot_account_summary(account)

        sections: list[tuple[str, Any]] = [
            ("wallet_balance", self.rest.get_wallet_balance(quote_asset=quote_asset)),
            ("user_assets", self.rest.get_user_assets(asset=asset)),
            ("funding_wallet", self.rest.get_funding_assets(asset=asset)),
            ("simple_earn_account", self.rest.get_simple_earn_account()),
            ("simple_earn_flexible", self.rest.get_simple_earn_flexible_positions(asset=asset)),
            ("simple_earn_locked", self.rest.get_simple_earn_locked_positions(asset=asset)),
        ]

        for section_name, request in sections:
            try:
                result = await request
            except BinanceTradeError as exc:
                payload[f"{section_name}_error"] = str(exc)
                continue

            if section_name == "user_assets":
                rows = [item for item in result if isinstance(item, dict)] if isinstance(result, list) else []
                positive = _filter_rows_with_positive_total(
                    rows,
                    fields=("free", "locked", "freeze", "withdrawing", "ipoable", "btcValuation"),
                )
                payload[section_name] = {
                    "count": len(positive),
                    "assets": positive,
                    "raw": result,
                }
            elif section_name == "funding_wallet":
                rows = [item for item in result if isinstance(item, dict)] if isinstance(result, list) else []
                positive = _filter_rows_with_positive_total(
                    rows,
                    fields=("free", "locked", "freeze", "withdrawing", "btcValuation"),
                )
                payload[section_name] = {
                    "count": len(positive),
                    "assets": positive,
                    "raw": result,
                }
            elif section_name == "simple_earn_flexible":
                payload[section_name] = _simple_earn_summary(
                    result,
                    amount_fields=("totalAmount", "totalInvestedAmount", "holdingAmount"),
                )
            elif section_name == "simple_earn_locked":
                payload[section_name] = _simple_earn_summary(
                    result,
                    amount_fields=("amount", "holdingAmount", "lockedAmount"),
                )
            else:
                payload[section_name] = result

        balances = payload["spot_account"].get("balances", [])
        funding_assets = payload.get("funding_wallet", {}).get("assets", [])
        user_assets = payload.get("user_assets", {}).get("assets", [])
        flexible_positions = payload.get("simple_earn_flexible", {}).get("positions", [])
        locked_positions = payload.get("simple_earn_locked", {}).get("positions", [])
        payload["summary"] = {
            "spot_balance_count": len(balances),
            "wallet_breakdown_count": len(payload.get("wallet_balance", [])) if isinstance(payload.get("wallet_balance"), list) else None,
            "user_asset_count": len(user_assets),
            "funding_asset_count": len(funding_assets),
            "simple_earn_position_count": len(list(chain(flexible_positions, locked_positions))),
        }
        return payload

    async def redeem_simple_earn_flexible(
        self,
        *,
        asset: str | None = None,
        product_id: str | None = None,
        amount: Decimal | None = None,
        redeem_all: bool = False,
        dest_account: str = "SPOT",
        require_confirmation: bool = True,
        confirmation_text: str | None = None,
    ) -> dict[str, Any]:
        if not self.authenticator:
            raise ConfigError("API credentials are required for Simple Earn redemption")
        if require_confirmation and confirmation_text != "REDEEM":
            raise ConfigError('Simple Earn redemption requires explicit confirmation_text="REDEEM"')
        if not product_id and not asset:
            raise ValueError("asset or product_id is required")
        if amount is not None and amount <= 0:
            raise ValueError("amount must be positive")
        if amount is None:
            redeem_all = True

        resolved_position: dict[str, Any] | None = None
        if product_id:
            positions_payload = await self.rest.get_simple_earn_flexible_positions(size=100)
        else:
            positions_payload = await self.rest.get_simple_earn_flexible_positions(asset=asset, size=100)
        positions = _simple_earn_rows(positions_payload)
        positive_positions = [item for item in positions if _simple_earn_position_total(item) > 0]

        if product_id:
            resolved_position = next((item for item in positive_positions if str(item.get("productId", "")) == product_id), None)
            if resolved_position is None:
                raise ConfigError(f"Simple Earn flexible productId {product_id!r} was not found among positive positions")
        else:
            target_asset = str(asset or "").upper()
            matching = [item for item in positive_positions if str(item.get("asset", "")).upper() in {target_asset, f"LD{target_asset}"}]
            if not matching:
                raise ConfigError(f"No positive Simple Earn flexible position was found for asset {target_asset}")
            resolved_position = matching[0]
            product_id = str(resolved_position.get("productId") or "").strip()
            if not product_id:
                raise ConfigError(f"Simple Earn position for {target_asset} did not include a productId")

        position_total = _simple_earn_position_total(resolved_position)
        if amount is not None and amount > position_total:
            raise ConfigError(
                f"Requested redeem amount {decimal_to_str(amount)} exceeds available flexible position {decimal_to_str(position_total)}"
            )

        response = await self.rest.redeem_simple_earn_flexible(
            product_id=product_id,
            amount=amount,
            redeem_all=redeem_all,
            dest_account=dest_account,
        )
        payload = {
            "status": "OK",
            "asset": None if resolved_position is None else resolved_position.get("asset"),
            "product_id": product_id,
            "amount": None if amount is None else decimal_to_str(amount),
            "redeem_all": redeem_all,
            "dest_account": dest_account.upper(),
            "position_total": decimal_to_str(position_total),
            "resolved_position": resolved_position,
            "binance": response,
        }
        self.state.record_event(
            market_type=MarketType.SPOT,
            channel="wallet_ops",
            event_type="simple_earn_flexible_redeem",
            symbol=None if resolved_position is None else str(resolved_position.get("asset")),
            payload=payload,
        )
        return payload

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int = 500,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[dict[str, Any]]:
        return await self.rest.get_klines_window(symbol, interval, limit=limit, start_time=start_time, end_time=end_time)

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
            market_type=MarketType.SPOT,
            quantity=quantity,
            price=price,
            new_client_order_id=order.new_client_order_id or new_client_order_id(self.settings.order_prefix, order.symbol),
        )
        prepared.validate()

        reference_price = None
        if prepared.order_type is OrderType.MARKET or prepared.quote_order_qty is not None:
            reference_price = await self.rest.get_price(prepared.symbol)
        return prepared, rules, reference_price

    async def submit_order(self, order: OrderRequest, *, submission_mode: SubmissionMode) -> dict[str, Any]:
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
        except BinanceTradeError as exc:
            failed = {
                "status": "SUBMIT_FAILED",
                "clientOrderId": prepared.new_client_order_id,
                "symbol": prepared.symbol,
                "error": str(exc),
                "errorType": exc.__class__.__name__,
            }
            if isinstance(exc, BinanceAPIError):
                failed["httpStatus"] = exc.http_status
                failed["code"] = exc.code
                failed["payload"] = exc.payload
            self.state.record_order_result(prepared.new_client_order_id or "", failed, fallback_status="SUBMIT_FAILED")
            raise

        self.state.record_order_result(prepared.new_client_order_id or "", response, fallback_status="LIVE_ACCEPTED")
        return response

    async def buy_market(self, symbol: str, *, quote_order_qty: Decimal, submission_mode: SubmissionMode) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                market_type=MarketType.SPOT,
                quote_order_qty=quote_order_qty,
            ),
            submission_mode=submission_mode,
        )

    async def sell_market(self, symbol: str, *, quantity: Decimal, submission_mode: SubmissionMode) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                market_type=MarketType.SPOT,
                quantity=quantity,
            ),
            submission_mode=submission_mode,
        )

    async def buy_limit(self, symbol: str, *, quantity: Decimal, price: Decimal, submission_mode: SubmissionMode) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                market_type=MarketType.SPOT,
                quantity=quantity,
                price=price,
                time_in_force=TimeInForce.GTC,
            ),
            submission_mode=submission_mode,
        )

    async def sell_limit(self, symbol: str, *, quantity: Decimal, price: Decimal, submission_mode: SubmissionMode) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                market_type=MarketType.SPOT,
                quantity=quantity,
                price=price,
                time_in_force=TimeInForce.GTC,
            ),
            submission_mode=submission_mode,
        )

    async def order_status(self, symbol: str, *, client_order_id: str | None = None, order_id: int | None = None) -> dict[str, Any]:
        payload = await self.rest.get_order(symbol, client_order_id=client_order_id, order_id=order_id)
        self.state.apply_exchange_order_snapshot(payload, market_type=MarketType.SPOT)
        return payload

    async def cancel(self, symbol: str, *, client_order_id: str | None = None, order_id: int | None = None) -> dict[str, Any]:
        payload = await self.rest.cancel_order(symbol, client_order_id=client_order_id, order_id=order_id)
        self.state.apply_exchange_order_snapshot(payload, market_type=MarketType.SPOT)
        return payload

    async def reconcile(self, symbol: str | None = None) -> dict[str, Any]:
        open_orders = await self.rest.get_open_orders(symbol)
        exchange_client_ids = {str(order.get("clientOrderId")) for order in open_orders if order.get("clientOrderId")}
        for order in open_orders:
            self.state.apply_exchange_order_snapshot(order, market_type=MarketType.SPOT)

        checked_local_orders: list[dict[str, Any]] = []
        for order in self.state.list_local_open_orders(symbol, MarketType.SPOT):
            client_order_id = str(order.get("client_order_id") or "")
            order_symbol = str(order.get("symbol") or "").upper()
            if not client_order_id or client_order_id in exchange_client_ids:
                continue
            try:
                snapshot = await self.rest.get_order(order_symbol, client_order_id=client_order_id)
            except BinanceAPIError as exc:
                if exc.code == -2013:
                    reason = "exchange reports unknown order during reconcile"
                    self.state.mark_order_reconciled_missing(client_order_id, reason=reason, market_type=MarketType.SPOT)
                    checked_local_orders.append(
                        {
                            "client_order_id": client_order_id,
                            "symbol": order_symbol,
                            "status": "RECONCILED_MISSING",
                            "reason": reason,
                        }
                    )
                    continue
                raise
            self.state.apply_exchange_order_snapshot(snapshot, market_type=MarketType.SPOT)
            checked_local_orders.append(
                {
                    "client_order_id": client_order_id,
                    "symbol": order_symbol,
                    "status": snapshot.get("status", "UNKNOWN"),
                    "exchange_order_id": snapshot.get("orderId"),
                }
            )
        return {"open_orders": open_orders, "checked_local_orders": checked_local_orders}

    async def raw_market_messages(self, streams: list[str], *, reconnect: bool = True):
        async for message in self.market.listen(streams, reconnect=reconnect):
            yield message

    async def market_messages(self, symbol: str, stream: str, *, reconnect: bool = True):
        stream_name = stream if "@" in stream else f"{symbol.lower()}@{stream}"
        async for message in self.raw_market_messages([stream_name], reconnect=reconnect):
            yield message

    async def user_messages(self, *, reconnect: bool = True):
        async for message in self.user_stream.listen(reconnect=reconnect):
            self.state.apply_user_stream_message(message, market_type=MarketType.SPOT)
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
        runner = StrategyRunner(
            service=self,
            strategy=DipBuyStrategy(
                symbol=symbol.upper(),
                quote_order_qty=quote_order_qty,
                lookback=lookback,
                trigger_pct=trigger_pct,
            ),
            submission_mode=submission_mode,
        )
        return await runner.run()


class FuturesTradingService:
    market_type = MarketType.FUTURES

    def __init__(self, settings: Settings) -> None:
        settings.assert_futures_supported()
        self.settings = settings
        self.authenticator = _build_authenticator(settings)
        self.state = SQLiteStateStore(settings.state_db_path)
        self.risk = RiskGate(
            RiskProfile(
                market_type=MarketType.FUTURES,
                allowed_symbols=settings.futures_allowed_symbols or settings.allowed_symbols,
                max_order_notional=Decimal(str(settings.futures_max_order_notional if settings.futures_max_order_notional is not None else settings.max_order_notional)),
                max_open_orders_per_symbol=settings.futures_max_open_orders_per_symbol or settings.max_open_orders_per_symbol,
                order_cooldown_seconds=settings.futures_order_cooldown_seconds if settings.futures_order_cooldown_seconds is not None else settings.order_cooldown_seconds,
            ),
            self.state,
        )
        self.rest = BinanceFuturesRestClient(settings, self.authenticator)
        self.market = MarketStreamClient(settings.resolved_futures_market_ws_url, trust_env=settings.network_trust_env)
        self.user_stream = FuturesUserDataStreamClient(
            settings.resolved_futures_user_ws_base_url,
            self.rest,
            trust_env=settings.network_trust_env,
        )

    async def __aenter__(self) -> "FuturesTradingService":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self.rest.close()

    def _resolve_submission_mode(
        self,
        *,
        live: bool = False,
        test_order: bool = False,
        submission_mode: SubmissionMode | None = None,
    ) -> SubmissionMode:
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
        selected_symbol = (symbol or self.settings.futures_default_symbol).upper()
        await self.rest.ping()
        server_time = await self.rest.sync_time()
        local_time = int(time.time() * 1000)
        rules = await self.rest.get_symbol_rules(selected_symbol)
        payload: dict[str, Any] = {
            "market_type": self.market_type.value,
            "environment": self.settings.binance_env.value,
            "rest_base_url": self.settings.resolved_futures_rest_base_url,
            "market_ws_url": self.settings.resolved_futures_market_ws_url,
            "user_ws_base_url": self.settings.resolved_futures_user_ws_base_url,
            "ws_api_url": self.settings.resolved_futures_ws_api_url,
            "server_time_ms": server_time,
            "clock_skew_ms": server_time - local_time,
            "symbol_rules": rules.summary(),
        }
        if self.authenticator:
            try:
                account = await self.rest.get_account()
            except BinanceTradeError as exc:
                payload["account_error"] = str(exc)
            else:
                positions = [item for item in account.get("positions", []) if Decimal(str(item.get("positionAmt", "0"))) != 0]
                assets = [item for item in account.get("assets", []) if Decimal(str(item.get("walletBalance", "0"))) != 0]
                payload["account"] = {
                    "availableBalance": account.get("availableBalance"),
                    "totalMarginBalance": account.get("totalMarginBalance"),
                    "assets": assets[:20],
                    "positions": positions[:20],
                }
        return payload

    async def price(self, symbol: str) -> dict[str, Any]:
        price = await self.rest.get_price(symbol.upper())
        return {"symbol": symbol.upper(), "price": decimal_to_str(price)}

    async def account(self) -> dict[str, Any]:
        if not self.authenticator:
            raise ConfigError("API credentials are required for account queries")
        return await self.rest.get_account()

    async def get_klines(
        self,
        symbol: str,
        interval: str,
        *,
        limit: int = 500,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[dict[str, Any]]:
        return await self.rest.get_klines_window(symbol, interval, limit=limit, start_time=start_time, end_time=end_time)

    async def positions(self, symbol: str | None = None) -> dict[str, Any]:
        if not self.authenticator:
            raise ConfigError("API credentials are required for position queries")
        return {"positions": await self.rest.get_positions(symbol)}

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
            market_type=MarketType.FUTURES,
            quantity=quantity,
            price=price,
            new_client_order_id=order.new_client_order_id or new_client_order_id(self.settings.futures_order_prefix, order.symbol),
            new_order_resp_type=order.new_order_resp_type or "ACK",
        )
        prepared.validate()

        reference_price = None
        if prepared.order_type is OrderType.MARKET or prepared.price is None:
            reference_price = await self.rest.get_price(prepared.symbol)
        return prepared, rules, reference_price

    async def submit_order(self, order: OrderRequest, *, submission_mode: SubmissionMode) -> dict[str, Any]:
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
        except BinanceTradeError as exc:
            failed = {
                "status": "SUBMIT_FAILED",
                "clientOrderId": prepared.new_client_order_id,
                "symbol": prepared.symbol,
                "error": str(exc),
                "errorType": exc.__class__.__name__,
            }
            if isinstance(exc, BinanceAPIError):
                failed["httpStatus"] = exc.http_status
                failed["code"] = exc.code
                failed["payload"] = exc.payload
            self.state.record_order_result(prepared.new_client_order_id or "", failed, fallback_status="SUBMIT_FAILED")
            raise

        self.state.record_order_result(prepared.new_client_order_id or "", response, fallback_status="LIVE_ACCEPTED")
        return response

    async def buy_market(
        self,
        symbol: str,
        *,
        quantity: Decimal,
        submission_mode: SubmissionMode,
        position_side: PositionSide | None = None,
        reduce_only: bool | None = None,
    ) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                market_type=MarketType.FUTURES,
                quantity=quantity,
                new_order_resp_type="ACK",
                position_side=position_side,
                reduce_only=reduce_only,
            ),
            submission_mode=submission_mode,
        )

    async def sell_market(
        self,
        symbol: str,
        *,
        quantity: Decimal,
        submission_mode: SubmissionMode,
        position_side: PositionSide | None = None,
        reduce_only: bool | None = None,
    ) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                market_type=MarketType.FUTURES,
                quantity=quantity,
                new_order_resp_type="ACK",
                position_side=position_side,
                reduce_only=reduce_only,
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
        position_side: PositionSide | None = None,
        reduce_only: bool | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
    ) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                market_type=MarketType.FUTURES,
                quantity=quantity,
                price=price,
                time_in_force=time_in_force,
                new_order_resp_type="ACK",
                position_side=position_side,
                reduce_only=reduce_only,
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
        position_side: PositionSide | None = None,
        reduce_only: bool | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
    ) -> dict[str, Any]:
        return await self.submit_order(
            OrderRequest(
                symbol=symbol,
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                market_type=MarketType.FUTURES,
                quantity=quantity,
                price=price,
                time_in_force=time_in_force,
                new_order_resp_type="ACK",
                position_side=position_side,
                reduce_only=reduce_only,
            ),
            submission_mode=submission_mode,
        )

    async def order_status(self, symbol: str, *, client_order_id: str | None = None, order_id: int | None = None) -> dict[str, Any]:
        payload = await self.rest.get_order(symbol, client_order_id=client_order_id, order_id=order_id)
        self.state.apply_exchange_order_snapshot(payload, market_type=MarketType.FUTURES)
        return payload

    async def cancel(self, symbol: str, *, client_order_id: str | None = None, order_id: int | None = None) -> dict[str, Any]:
        payload = await self.rest.cancel_order(symbol, client_order_id=client_order_id, order_id=order_id)
        self.state.apply_exchange_order_snapshot(payload, market_type=MarketType.FUTURES)
        return payload

    async def reconcile(self, symbol: str | None = None) -> dict[str, Any]:
        open_orders = await self.rest.get_open_orders(symbol)
        for order in open_orders:
            self.state.apply_exchange_order_snapshot(order, market_type=MarketType.FUTURES)
        positions = await self.rest.get_positions(symbol) if self.authenticator else []
        return {"open_orders": open_orders, "positions": positions}

    async def set_leverage(self, symbol: str, leverage: int) -> dict[str, Any]:
        if not self.authenticator:
            raise ConfigError("API credentials are required for leverage changes")
        return await self.rest.set_leverage(symbol, leverage)

    async def set_margin_type(self, symbol: str, margin_type: str) -> dict[str, Any]:
        if not self.authenticator:
            raise ConfigError("API credentials are required for margin type changes")
        return await self.rest.set_margin_type(symbol, margin_type.upper())

    async def raw_market_messages(self, streams: list[str], *, reconnect: bool = True):
        async for message in self.market.listen(streams, reconnect=reconnect):
            yield message

    async def market_messages(self, symbol: str, stream: str, *, reconnect: bool = True):
        stream_name = stream if "@" in stream else f"{symbol.lower()}@{stream}"
        async for message in self.raw_market_messages([stream_name], reconnect=reconnect):
            yield message

    async def user_messages(self, *, reconnect: bool = True):
        async for message in self.user_stream.listen(reconnect=reconnect):
            self.state.apply_user_stream_message(message, market_type=MarketType.FUTURES)
            yield message

    async def run_demo_strategy(
        self,
        symbol: str,
        *,
        quantity: Decimal,
        lookback: int,
        trigger_pct: Decimal,
        submission_mode: SubmissionMode,
    ) -> dict[str, Any]:
        runner = StrategyRunner(
            service=self,
            strategy=DipBuyStrategy(
                symbol=symbol.upper(),
                quantity=quantity,
                lookback=lookback,
                trigger_pct=trigger_pct,
                market_type=MarketType.FUTURES,
            ),
            submission_mode=submission_mode,
        )
        return await runner.run()


TradingService = SpotTradingService
