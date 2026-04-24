import asyncio
from pathlib import Path

from binance_trade.config import Settings
from binance_trade.service import SpotTradingService


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        BINANCE_ENV="mainnet",
        BINANCE_API_KEY="key",
        BINANCE_API_SECRET="secret",
        STATE_DB_PATH=tmp_path / "state.db",
        RUNTIME_DIR=tmp_path / "runtime",
        DRY_RUN=True,
    )


def test_portfolio_overview_aggregates_spot_wallet_and_earn(tmp_path: Path) -> None:
    service = SpotTradingService(_settings(tmp_path))

    async def fake_account():
        return {
            "canTrade": True,
            "canWithdraw": True,
            "canDeposit": True,
            "balances": [
                {"asset": "USDT", "free": "0.01", "locked": "0"},
                {"asset": "BTC", "free": "0.0013", "locked": "0"},
                {"asset": "ETH", "free": "0", "locked": "0"},
            ],
        }

    async def fake_wallet_balance(*, quote_asset: str = "USDT"):
        assert quote_asset == "USDT"
        return [
            {"walletName": "Spot", "balance": "101.23"},
            {"walletName": "Simple Earn", "balance": "622.71"},
        ]

    async def fake_user_assets(*, asset=None):
        return [
            {"asset": "USDT", "free": "941.71520897", "locked": "0", "freeze": "0", "withdrawing": "0", "ipoable": "0", "btcValuation": "0.011"},
            {"asset": "BTC", "free": "0.0013", "locked": "0", "freeze": "0", "withdrawing": "0", "ipoable": "0", "btcValuation": "0.0013"},
            {"asset": "ETH", "free": "0", "locked": "0", "freeze": "0", "withdrawing": "0", "ipoable": "0", "btcValuation": "0"},
        ]

    async def fake_funding_assets(*, asset=None):
        return [{"asset": "USDT", "free": "10", "locked": "0", "freeze": "0", "withdrawing": "0", "btcValuation": "0.0001"}]

    async def fake_simple_earn_account():
        return {"totalAmountInBTC": "0.02", "totalAmountInUSDT": "620"}

    async def fake_flexible_positions(*, asset=None, size=100):
        return {"rows": [{"asset": "LDUSDT", "totalAmount": "622.71674729"}]}

    async def fake_locked_positions(*, asset=None, size=100):
        return {"rows": []}

    service.rest.get_account = fake_account  # type: ignore[method-assign]
    service.rest.get_wallet_balance = fake_wallet_balance  # type: ignore[method-assign]
    service.rest.get_user_assets = fake_user_assets  # type: ignore[method-assign]
    service.rest.get_funding_assets = fake_funding_assets  # type: ignore[method-assign]
    service.rest.get_simple_earn_account = fake_simple_earn_account  # type: ignore[method-assign]
    service.rest.get_simple_earn_flexible_positions = fake_flexible_positions  # type: ignore[method-assign]
    service.rest.get_simple_earn_locked_positions = fake_locked_positions  # type: ignore[method-assign]

    async def _run() -> None:
        result = await service.portfolio_overview(quote_asset="USDT")
        assert result["spot_account"]["balances"] == [
            {"asset": "USDT", "free": "0.01", "locked": "0"},
            {"asset": "BTC", "free": "0.0013", "locked": "0"},
        ]
        assert result["user_assets"]["count"] == 2
        assert result["funding_wallet"]["count"] == 1
        assert result["simple_earn_flexible"]["count"] == 1
        assert result["summary"]["simple_earn_position_count"] == 1
        await service.close()

    asyncio.run(_run())


def test_redeem_simple_earn_flexible_requires_confirmation_and_resolves_asset(tmp_path: Path) -> None:
    service = SpotTradingService(_settings(tmp_path))

    async def fake_flexible_positions(*, asset=None, size=100):
        assert asset == "USDT"
        return {
            "rows": [
                {"productId": "PID-USDT", "asset": "LDUSDT", "totalAmount": "622.71674729"},
            ]
        }

    async def fake_redeem(*, product_id: str, amount=None, redeem_all=False, dest_account="SPOT"):
        return {
            "success": True,
            "productId": product_id,
            "amount": None if amount is None else str(amount),
            "redeemAll": redeem_all,
            "destAccount": dest_account,
        }

    service.rest.get_simple_earn_flexible_positions = fake_flexible_positions  # type: ignore[method-assign]
    service.rest.redeem_simple_earn_flexible = fake_redeem  # type: ignore[method-assign]

    async def _run() -> None:
        try:
            await service.redeem_simple_earn_flexible(asset="USDT", amount=None, confirmation_text="")
        except Exception as exc:
            assert 'confirmation_text="REDEEM"' in str(exc)
        else:
            raise AssertionError("expected confirmation guard to reject redemption")

        result = await service.redeem_simple_earn_flexible(asset="USDT", amount=None, confirmation_text="REDEEM")
        assert result["product_id"] == "PID-USDT"
        assert result["asset"] == "LDUSDT"
        assert result["redeem_all"] is True
        assert result["dest_account"] == "SPOT"
        await service.close()

    asyncio.run(_run())
