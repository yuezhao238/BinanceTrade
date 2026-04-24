from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator

import websockets

from .exceptions import ConfigError
from .futures_rest import BinanceFuturesRestClient

LOGGER = logging.getLogger(__name__)


class FuturesUserDataStreamClient:
    def __init__(self, ws_base_url: str, rest_client: BinanceFuturesRestClient, *, trust_env: bool = False) -> None:
        self.ws_base_url = ws_base_url.rstrip("/")
        self.rest_client = rest_client
        self.proxy = True if trust_env else None

    async def _keepalive_loop(self, listen_key: str) -> None:
        while True:
            await asyncio.sleep(30 * 60)
            await self.rest_client.keepalive_user_stream(listen_key)
            LOGGER.info("futures user stream keepalive listenKey=%s", listen_key)

    async def listen(self, *, reconnect: bool = True) -> AsyncIterator[dict]:
        if not self.rest_client.authenticator:
            raise ConfigError("API credentials are required for the futures user stream")

        backoff_seconds = 1
        while True:
            listen_key = await self.rest_client.start_user_stream()
            keepalive_task: asyncio.Task[None] | None = None
            try:
                url = f"{self.ws_base_url}/ws/{listen_key}"
                LOGGER.info("connecting futures user stream url=%s", url)
                async with websockets.connect(url, ping_interval=None, max_size=None, proxy=self.proxy) as websocket:
                    keepalive_task = asyncio.create_task(self._keepalive_loop(listen_key))
                    backoff_seconds = 1
                    async for raw_message in websocket:
                        yield json.loads(raw_message)
                if not reconnect:
                    return
            except Exception:
                if not reconnect:
                    raise
                LOGGER.exception("futures user stream disconnected, retrying in %ss", backoff_seconds)
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)
            finally:
                if keepalive_task:
                    keepalive_task.cancel()
                    try:
                        await keepalive_task
                    except asyncio.CancelledError:
                        pass
                try:
                    await self.rest_client.close_user_stream(listen_key)
                except Exception:
                    LOGGER.exception("failed to close futures listenKey=%s", listen_key)
