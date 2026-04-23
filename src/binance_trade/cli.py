from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any

import typer

from .builtin_strategies import create_strategy as create_builtin_strategy
from .builtin_strategies import list_strategies as list_builtin_strategies
from .config import get_settings
from .exceptions import BinanceTradeError
from .logging_utils import setup_logging
from .service import FuturesTradingService, SpotTradingService
from .strategy_runtime import StrategyRunner, load_strategy, parse_strategy_params
from .types import PositionSide, SubmissionMode

app = typer.Typer(no_args_is_help=True, help="Professional Binance Spot and USDⓈ-M Futures trading starter.")


def _print_json(payload: Any, *, pretty: bool = True) -> None:
    typer.echo(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2 if pretty else None,
            sort_keys=True,
            default=str,
        )
    )


def _run(coro: Any, *, pretty: bool = True) -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    try:
        result = asyncio.run(coro)
    except (BinanceTradeError, ValueError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc
    if result is not None:
        _print_json(result, pretty=pretty)


def _decimal(value: str) -> Decimal:
    return Decimal(value)


def _resolve_mode(live: bool, test_order: bool) -> SubmissionMode | None:
    if live and test_order:
        raise ValueError("--live and --test-order are mutually exclusive")
    if live:
        return SubmissionMode.LIVE
    if test_order:
        return SubmissionMode.TEST
    return None


def _position_side(value: str | None) -> PositionSide | None:
    if value is None:
        return None
    return PositionSide(value.upper())


async def _with_spot_service(callback: Any) -> Any:
    async with SpotTradingService(get_settings()) as service:
        return await callback(service)


async def _with_futures_service(callback: Any) -> Any:
    async with FuturesTradingService(get_settings()) as service:
        return await callback(service)


@app.command("doctor")
def doctor(symbol: str | None = typer.Argument(None, help="Spot symbol to validate, defaults to DEFAULT_SYMBOL.")) -> None:
    _run(_with_spot_service(lambda service: service.doctor(symbol)))


@app.command("price")
def price(symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT.")) -> None:
    _run(_with_spot_service(lambda service: service.price(symbol)))


@app.command("account")
def account() -> None:
    _run(_with_spot_service(lambda service: service.account()))


@app.command("buy-market")
def buy_market(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    quote: str = typer.Option(..., "--quote", help="Quote notional, for example 25 USDT."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_spot_service(
            lambda service: service.buy_market(
                symbol,
                quote_order_qty=_decimal(quote),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("sell-market")
def sell_market(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Base asset quantity."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_spot_service(
            lambda service: service.sell_market(
                symbol,
                quantity=_decimal(quantity),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("buy-limit")
def buy_limit(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Base asset quantity."),
    price: str = typer.Option(..., "--price", help="Limit price."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_spot_service(
            lambda service: service.buy_limit(
                symbol,
                quantity=_decimal(quantity),
                price=_decimal(price),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("sell-limit")
def sell_limit(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Base asset quantity."),
    price: str = typer.Option(..., "--price", help="Limit price."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_spot_service(
            lambda service: service.sell_limit(
                symbol,
                quantity=_decimal(quantity),
                price=_decimal(price),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("order-status")
def order_status(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    client_order_id: str | None = typer.Option(None, "--client-order-id", help="Client order id."),
    order_id: int | None = typer.Option(None, "--order-id", help="Exchange order id."),
) -> None:
    _run(_with_spot_service(lambda service: service.order_status(symbol, client_order_id=client_order_id, order_id=order_id)))


@app.command("cancel")
def cancel(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    client_order_id: str | None = typer.Option(None, "--client-order-id", help="Client order id."),
    order_id: int | None = typer.Option(None, "--order-id", help="Exchange order id."),
) -> None:
    _run(_with_spot_service(lambda service: service.cancel(symbol, client_order_id=client_order_id, order_id=order_id)))


@app.command("reconcile")
def reconcile(symbol: str | None = typer.Argument(None, help="Optional spot trading pair.")) -> None:
    _run(_with_spot_service(lambda service: service.reconcile(symbol)))


@app.command("watch-market")
def watch_market(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    stream: str = typer.Option("miniTicker", "--stream", help="trade, miniTicker, bookTicker, kline_1m, or full stream name."),
    reconnect: bool = typer.Option(True, "--reconnect/--no-reconnect", help="Reconnect on disconnect."),
) -> None:
    async def _watch() -> None:
        async with SpotTradingService(get_settings()) as service:
            async for message in service.market_messages(symbol, stream, reconnect=reconnect):
                _print_json(message, pretty=False)

    _run(_watch(), pretty=False)


@app.command("watch-user")
def watch_user(reconnect: bool = typer.Option(True, "--reconnect/--no-reconnect", help="Reconnect on disconnect.")) -> None:
    async def _watch() -> None:
        async with SpotTradingService(get_settings()) as service:
            async for message in service.user_messages(reconnect=reconnect):
                _print_json(message, pretty=False)

    _run(_watch(), pretty=False)


@app.command("run-demo-strategy")
def run_demo_strategy(
    symbol: str = typer.Argument(..., help="Spot trading pair such as BTCUSDT."),
    quote: str = typer.Option("25", "--quote", help="Quote notional to buy when triggered."),
    lookback: int = typer.Option(30, "--lookback", help="Number of miniTicker points for the rolling average."),
    trigger_pct: str = typer.Option("0.003", "--trigger-pct", help="Buy when price is this far below the rolling average."),
    live: bool = typer.Option(False, "--live", help="Send a live order when the signal triggers."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test when the signal triggers."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_spot_service(
            lambda service: service.run_demo_strategy(
                symbol,
                quote_order_qty=_decimal(quote),
                lookback=lookback,
                trigger_pct=_decimal(trigger_pct),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("list-strategies")
def list_strategies() -> None:
    _print_json({"builtin_strategies": list_builtin_strategies()})


@app.command("run-builtin-strategy")
def run_builtin_strategy(
    name: str = typer.Argument(..., help="Built-in strategy name."),
    market: str = typer.Option("spot", "--market", help="spot or futures."),
    params_json: str = typer.Option("{}", "--params-json", help="JSON object passed to the strategy constructor."),
    live: bool = typer.Option(False, "--live", help="Send live orders."),
    test_order: bool = typer.Option(False, "--test-order", help="Send exchange test orders."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    params = parse_strategy_params(params_json)

    async def _run_spot() -> dict[str, Any]:
        async with SpotTradingService(get_settings()) as service:
            runner = StrategyRunner(
                service=service,
                strategy=create_builtin_strategy(name=name, **params),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
            return await runner.run()

    async def _run_futures() -> dict[str, Any]:
        async with FuturesTradingService(get_settings()) as service:
            runner = StrategyRunner(
                service=service,
                strategy=create_builtin_strategy(name=name, **params),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
            return await runner.run()

    if market.lower() == "spot":
        _run(_run_spot())
        return
    if market.lower() == "futures":
        _run(_run_futures())
        return
    raise ValueError("--market must be spot or futures")


@app.command("run-strategy")
def run_strategy(
    strategy_ref: str = typer.Argument(..., help="Module path or file path, e.g. examples/strategies/spot_mean_reversion.py:create_strategy"),
    market: str = typer.Option("spot", "--market", help="spot or futures."),
    params_json: str = typer.Option("{}", "--params-json", help="JSON object passed to the strategy factory."),
    live: bool = typer.Option(False, "--live", help="Send live orders."),
    test_order: bool = typer.Option(False, "--test-order", help="Send exchange test orders."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    params = parse_strategy_params(params_json)

    async def _run_spot() -> dict[str, Any]:
        async with SpotTradingService(get_settings()) as service:
            runner = StrategyRunner(
                service=service,
                strategy=load_strategy(strategy_ref, params),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
            return await runner.run()

    async def _run_futures() -> dict[str, Any]:
        async with FuturesTradingService(get_settings()) as service:
            runner = StrategyRunner(
                service=service,
                strategy=load_strategy(strategy_ref, params),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
            return await runner.run()

    if market.lower() == "spot":
        _run(_run_spot())
        return
    if market.lower() == "futures":
        _run(_run_futures())
        return
    raise ValueError("--market must be spot or futures")


@app.command("futures-doctor")
def futures_doctor(symbol: str | None = typer.Argument(None, help="Futures symbol to validate, defaults to FUTURES_DEFAULT_SYMBOL.")) -> None:
    _run(_with_futures_service(lambda service: service.doctor(symbol)))


@app.command("futures-price")
def futures_price(symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT.")) -> None:
    _run(_with_futures_service(lambda service: service.price(symbol)))


@app.command("futures-account")
def futures_account() -> None:
    _run(_with_futures_service(lambda service: service.account()))


@app.command("futures-positions")
def futures_positions(symbol: str | None = typer.Argument(None, help="Optional futures trading pair.")) -> None:
    _run(_with_futures_service(lambda service: service.positions(symbol)))


@app.command("futures-buy-market")
def futures_buy_market(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Contract quantity."),
    position_side: str | None = typer.Option(None, "--position-side", help="BOTH, LONG, or SHORT."),
    reduce_only: bool = typer.Option(False, "--reduce-only", help="Mark the order reduce-only."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_futures_service(
            lambda service: service.buy_market(
                symbol,
                quantity=_decimal(quantity),
                position_side=_position_side(position_side),
                reduce_only=reduce_only or None,
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("futures-sell-market")
def futures_sell_market(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Contract quantity."),
    position_side: str | None = typer.Option(None, "--position-side", help="BOTH, LONG, or SHORT."),
    reduce_only: bool = typer.Option(False, "--reduce-only", help="Mark the order reduce-only."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_futures_service(
            lambda service: service.sell_market(
                symbol,
                quantity=_decimal(quantity),
                position_side=_position_side(position_side),
                reduce_only=reduce_only or None,
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("futures-buy-limit")
def futures_buy_limit(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Contract quantity."),
    price: str = typer.Option(..., "--price", help="Limit price."),
    position_side: str | None = typer.Option(None, "--position-side", help="BOTH, LONG, or SHORT."),
    reduce_only: bool = typer.Option(False, "--reduce-only", help="Mark the order reduce-only."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_futures_service(
            lambda service: service.buy_limit(
                symbol,
                quantity=_decimal(quantity),
                price=_decimal(price),
                position_side=_position_side(position_side),
                reduce_only=reduce_only or None,
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("futures-sell-limit")
def futures_sell_limit(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Contract quantity."),
    price: str = typer.Option(..., "--price", help="Limit price."),
    position_side: str | None = typer.Option(None, "--position-side", help="BOTH, LONG, or SHORT."),
    reduce_only: bool = typer.Option(False, "--reduce-only", help="Mark the order reduce-only."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_futures_service(
            lambda service: service.sell_limit(
                symbol,
                quantity=_decimal(quantity),
                price=_decimal(price),
                position_side=_position_side(position_side),
                reduce_only=reduce_only or None,
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("futures-order-status")
def futures_order_status(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    client_order_id: str | None = typer.Option(None, "--client-order-id", help="Client order id."),
    order_id: int | None = typer.Option(None, "--order-id", help="Exchange order id."),
) -> None:
    _run(_with_futures_service(lambda service: service.order_status(symbol, client_order_id=client_order_id, order_id=order_id)))


@app.command("futures-cancel")
def futures_cancel(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    client_order_id: str | None = typer.Option(None, "--client-order-id", help="Client order id."),
    order_id: int | None = typer.Option(None, "--order-id", help="Exchange order id."),
) -> None:
    _run(_with_futures_service(lambda service: service.cancel(symbol, client_order_id=client_order_id, order_id=order_id)))


@app.command("futures-reconcile")
def futures_reconcile(symbol: str | None = typer.Argument(None, help="Optional futures trading pair.")) -> None:
    _run(_with_futures_service(lambda service: service.reconcile(symbol)))


@app.command("futures-watch-market")
def futures_watch_market(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    stream: str = typer.Option("miniTicker", "--stream", help="trade, miniTicker, bookTicker, kline_1m, or full stream name."),
    reconnect: bool = typer.Option(True, "--reconnect/--no-reconnect", help="Reconnect on disconnect."),
) -> None:
    async def _watch() -> None:
        async with FuturesTradingService(get_settings()) as service:
            async for message in service.market_messages(symbol, stream, reconnect=reconnect):
                _print_json(message, pretty=False)

    _run(_watch(), pretty=False)


@app.command("futures-watch-user")
def futures_watch_user(reconnect: bool = typer.Option(True, "--reconnect/--no-reconnect", help="Reconnect on disconnect.")) -> None:
    async def _watch() -> None:
        async with FuturesTradingService(get_settings()) as service:
            async for message in service.user_messages(reconnect=reconnect):
                _print_json(message, pretty=False)

    _run(_watch(), pretty=False)


@app.command("futures-set-leverage")
def futures_set_leverage(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    leverage: int = typer.Option(..., "--leverage", help="Target leverage, 1-125."),
) -> None:
    _run(_with_futures_service(lambda service: service.set_leverage(symbol, leverage)))


@app.command("futures-set-margin-type")
def futures_set_margin_type(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    margin_type: str = typer.Option(..., "--margin-type", help="ISOLATED or CROSSED."),
) -> None:
    _run(_with_futures_service(lambda service: service.set_margin_type(symbol, margin_type)))


@app.command("futures-run-demo-strategy")
def futures_run_demo_strategy(
    symbol: str = typer.Argument(..., help="Futures trading pair such as BTCUSDT."),
    quantity: str = typer.Option("0.001", "--quantity", help="Contract quantity to buy when triggered."),
    lookback: int = typer.Option(30, "--lookback", help="Number of miniTicker points for the rolling average."),
    trigger_pct: str = typer.Option("0.003", "--trigger-pct", help="Buy when price is this far below the rolling average."),
    live: bool = typer.Option(False, "--live", help="Send a live order when the signal triggers."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test when the signal triggers."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_futures_service(
            lambda service: service.run_demo_strategy(
                symbol,
                quantity=_decimal(quantity),
                lookback=lookback,
                trigger_pct=_decimal(trigger_pct),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


if __name__ == "__main__":
    app()
