from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Any

import websockets

from .exceptions import ConfigError
from .signing import Authenticator

LOGGER = logging.getLogger(__name__)


class UserDataStreamClient:
    def __init__(self, ws_api_url: str, authenticator: Authenticator | None) -> None:
        self.ws_api_url = ws_api_url
        self.authenticator = authenticator

    async def _subscribe(self, websocket: Any) -> dict[str, Any]:
        if not self.authenticator:
            raise ConfigError("API credentials are required for the user stream")

        request = {
            "id": str(uuid.uuid4()),
            "method": "userDataStream.subscribe.signature",
            "params": self.authenticator.build_ws_signed_params({}),
        }
        await websocket.send(json.dumps(request))
        response = json.loads(await websocket.recv())
        status = response.get("status")
        if status != 200:
            raise RuntimeError(f"userDataStream.subscribe.signature failed: {response}")
        LOGGER.info("user stream subscribed subscriptionId=%s", response["result"]["subscriptionId"])
        return response

    async def listen(self, *, reconnect: bool = True) -> AsyncIterator[dict[str, Any]]:
        if not self.authenticator:
            raise ConfigError("API credentials are required for the user stream")

        backoff_seconds = 1
        while True:
            try:
                LOGGER.info("connecting user stream url=%s", self.ws_api_url)
                async with websockets.connect(self.ws_api_url, ping_interval=None, max_size=None) as websocket:
                    subscribe_response = await self._subscribe(websocket)
                    yield subscribe_response
                    backoff_seconds = 1
                    async for raw_message in websocket:
                        yield json.loads(raw_message)
                if not reconnect:
                    return
            except Exception:
                if not reconnect:
                    raise
                LOGGER.exception("user stream disconnected, retrying in %ss", backoff_seconds)
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)
