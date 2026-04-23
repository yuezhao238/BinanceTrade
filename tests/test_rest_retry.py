import asyncio

import httpx

from binance_trade.config import Settings
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
