from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any

import typer

from .config import get_settings
from .exceptions import BinanceTradeError
from .logging_utils import setup_logging
from .service import TradingService
from .types import SubmissionMode

app = typer.Typer(no_args_is_help=True, help="Professional Binance Spot trading starter.")


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


async def _with_service(callback: Any) -> Any:
    async with TradingService(get_settings()) as service:
        return await callback(service)


@app.command("doctor")
def doctor(symbol: str | None = typer.Argument(None, help="Symbol to validate, defaults to DEFAULT_SYMBOL.")) -> None:
    _run(_with_service(lambda service: service.doctor(symbol)))


@app.command("price")
def price(symbol: str = typer.Argument(..., help="Trading pair such as BTCUSDT.")) -> None:
    _run(_with_service(lambda service: service.price(symbol)))


@app.command("account")
def account() -> None:
    _run(_with_service(lambda service: service.account()))


@app.command("buy-market")
def buy_market(
    symbol: str = typer.Argument(..., help="Trading pair such as BTCUSDT."),
    quote: str = typer.Option(..., "--quote", help="Quote notional, for example 25 USDT."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_service(
            lambda service: service.buy_market(
                symbol,
                quote_order_qty=_decimal(quote),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("sell-market")
def sell_market(
    symbol: str = typer.Argument(..., help="Trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Base asset quantity."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_service(
            lambda service: service.sell_market(
                symbol,
                quantity=_decimal(quantity),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


@app.command("buy-limit")
def buy_limit(
    symbol: str = typer.Argument(..., help="Trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Base asset quantity."),
    price: str = typer.Option(..., "--price", help="Limit price."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_service(
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
    symbol: str = typer.Argument(..., help="Trading pair such as BTCUSDT."),
    quantity: str = typer.Option(..., "--quantity", help="Base asset quantity."),
    price: str = typer.Option(..., "--price", help="Limit price."),
    live: bool = typer.Option(False, "--live", help="Send a live order."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_service(
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
    symbol: str = typer.Argument(..., help="Trading pair such as BTCUSDT."),
    client_order_id: str | None = typer.Option(None, "--client-order-id", help="Client order id."),
    order_id: int | None = typer.Option(None, "--order-id", help="Exchange order id."),
) -> None:
    _run(_with_service(lambda service: service.order_status(symbol, client_order_id=client_order_id, order_id=order_id)))


@app.command("cancel")
def cancel(
    symbol: str = typer.Argument(..., help="Trading pair such as BTCUSDT."),
    client_order_id: str | None = typer.Option(None, "--client-order-id", help="Client order id."),
    order_id: int | None = typer.Option(None, "--order-id", help="Exchange order id."),
) -> None:
    _run(_with_service(lambda service: service.cancel(symbol, client_order_id=client_order_id, order_id=order_id)))


@app.command("reconcile")
def reconcile(symbol: str | None = typer.Argument(None, help="Optional trading pair.")) -> None:
    _run(_with_service(lambda service: service.reconcile(symbol)))


@app.command("watch-market")
def watch_market(
    symbol: str = typer.Argument(..., help="Trading pair such as BTCUSDT."),
    stream: str = typer.Option("miniTicker", "--stream", help="trade, miniTicker, bookTicker, kline_1m, or full stream name."),
    reconnect: bool = typer.Option(True, "--reconnect/--no-reconnect", help="Reconnect on disconnect."),
) -> None:
    async def _watch() -> None:
        async with TradingService(get_settings()) as service:
            async for message in service.market_messages(symbol, stream, reconnect=reconnect):
                _print_json(message, pretty=False)

    _run(_watch(), pretty=False)


@app.command("watch-user")
def watch_user(
    reconnect: bool = typer.Option(True, "--reconnect/--no-reconnect", help="Reconnect on disconnect."),
) -> None:
    async def _watch() -> None:
        async with TradingService(get_settings()) as service:
            async for message in service.user_messages(reconnect=reconnect):
                _print_json(message, pretty=False)

    _run(_watch(), pretty=False)


@app.command("run-demo-strategy")
def run_demo_strategy(
    symbol: str = typer.Argument(..., help="Trading pair such as BTCUSDT."),
    quote: str = typer.Option("25", "--quote", help="Quote notional to buy when triggered."),
    lookback: int = typer.Option(30, "--lookback", help="Number of miniTicker points for the rolling average."),
    trigger_pct: str = typer.Option("0.003", "--trigger-pct", help="Buy when price is this far below the rolling average."),
    live: bool = typer.Option(False, "--live", help="Send a live order when the signal triggers."),
    test_order: bool = typer.Option(False, "--test-order", help="Send Binance /order/test when the signal triggers."),
) -> None:
    submission_mode = _resolve_mode(live, test_order)
    _run(
        _with_service(
            lambda service: service.run_demo_strategy(
                symbol,
                quote_order_qty=_decimal(quote),
                lookback=lookback,
                trigger_pct=_decimal(trigger_pct),
                submission_mode=service._resolve_submission_mode(submission_mode=submission_mode),
            )
        )
    )


if __name__ == "__main__":
    app()
