from __future__ import annotations

import asyncio
import json
import logging
import urllib.parse
from collections.abc import AsyncIterator
from typing import Any

import websockets

LOGGER = logging.getLogger(__name__)


class MarketStreamClient:
    def __init__(self, base_url: str, *, trust_env: bool = False) -> None:
        self.base_url = base_url
        self.proxy = True if trust_env else None

    def build_url(self, streams: list[str]) -> str:
        query = urllib.parse.urlencode({"streams": "/".join(streams)})
        return f"{self.base_url}?{query}"

    async def listen(self, streams: list[str], *, reconnect: bool = True) -> AsyncIterator[dict[str, Any]]:
        backoff_seconds = 1
        while True:
            try:
                url = self.build_url(streams)
                LOGGER.info("connecting market stream url=%s", url)
                async with websockets.connect(url, ping_interval=None, max_size=None, proxy=self.proxy) as websocket:
                    backoff_seconds = 1
                    async for raw_message in websocket:
                        yield json.loads(raw_message)
                if not reconnect:
                    return
            except Exception:
                if not reconnect:
                    raise
                LOGGER.exception("market stream disconnected, retrying in %ss", backoff_seconds)
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)
