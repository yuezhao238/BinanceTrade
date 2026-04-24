import asyncio

import httpx
import pytest

from binance_trade.config import Settings
from binance_trade.exceptions import NetworkError
from binance_trade.rest import BinanceSpotRestClient


def test_public_request_retries_after_transport_error() -> None:
    client = BinanceSpotRestClient(Settings(BINANCE_ENV="binance_us"))
    attempts = {"count": 0}

    async def fake_request(method: str, path: str, params=None):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise httpx.ConnectError("temporary", request=httpx.Request(method, f"https://api.binance.us{path}"))
        return httpx.Response(
            200,
            request=httpx.Request(method, f"https://api.binance.us{path}"),
            json={"ok": True},
        )

    client._client.request = fake_request  # type: ignore[method-assign]

    async def _run() -> None:
        result = await client._request_public("GET", "/api/v3/ping")
        assert result == {"ok": True}
        await client.close()

    asyncio.run(_run())
    assert attempts["count"] == 3


def test_public_request_wraps_proxy_error_as_network_error() -> None:
    client = BinanceSpotRestClient(Settings(BINANCE_ENV="mainnet", NETWORK_TRUST_ENV=True))

    async def fake_request(method: str, path: str, params=None):
        raise httpx.ProxyError(
            "503 Service Unavailable",
            request=httpx.Request(method, f"https://api.binance.com{path}"),
        )

    client._client.request = fake_request  # type: ignore[method-assign]

    async def _run() -> None:
        with pytest.raises(NetworkError) as excinfo:
            await client._request_public("GET", "/api/v3/ping")
        assert "proxy_error after 3 attempts" in str(excinfo.value)
        assert "NETWORK_TRUST_ENV=false" in str(excinfo.value)
        await client.close()

    asyncio.run(_run())
