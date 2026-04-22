from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .types import ApiKeyType, Environment


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    binance_env: Environment = Field(default=Environment.TESTNET, alias="BINANCE_ENV")
    binance_api_key: str | None = Field(default=None, alias="BINANCE_API_KEY")
    binance_api_secret: str | None = Field(default=None, alias="BINANCE_API_SECRET")
    binance_api_key_type: ApiKeyType = Field(default=ApiKeyType.HMAC, alias="BINANCE_API_KEY_TYPE")
    binance_private_key_path: Path | None = Field(default=None, alias="BINANCE_PRIVATE_KEY_PATH")
    binance_private_key_passphrase: str | None = Field(default=None, alias="BINANCE_PRIVATE_KEY_PASSPHRASE")

    binance_rest_base_url: str | None = Field(default=None, alias="BINANCE_REST_BASE_URL")
    binance_market_ws_url: str | None = Field(default=None, alias="BINANCE_MARKET_WS_URL")
    binance_ws_api_url: str | None = Field(default=None, alias="BINANCE_WS_API_URL")

    default_symbol: str = Field(default="BTCUSDT", alias="DEFAULT_SYMBOL")
    order_prefix: str = Field(default="bt", alias="ORDER_PREFIX")
    state_db_path: Path = Field(default=Path("var/state.db"), alias="STATE_DB_PATH")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    dry_run: bool = Field(default=True, alias="DRY_RUN")

    max_order_notional: float = Field(default=50.0, alias="MAX_ORDER_NOTIONAL")
    max_open_orders_per_symbol: int = Field(default=5, alias="MAX_OPEN_ORDERS_PER_SYMBOL")
    order_cooldown_seconds: int = Field(default=5, alias="ORDER_COOLDOWN_SECONDS")
    allowed_symbols: list[str] = Field(default_factory=list, alias="ALLOWED_SYMBOLS")

    request_timeout_seconds: float = Field(default=10.0, alias="REQUEST_TIMEOUT_SECONDS")
    recv_window_ms: float = Field(default=5000.0, alias="RECV_WINDOW_MS")

    @field_validator("default_symbol")
    @classmethod
    def _upper_default_symbol(cls, value: str) -> str:
        return value.upper()

    @field_validator("allowed_symbols", mode="before")
    @classmethod
    def _split_allowed_symbols(cls, value: Any) -> list[str]:
        if value is None or value == "":
            return []
        if isinstance(value, list):
            return [str(item).upper() for item in value if str(item).strip()]
        return [part.strip().upper() for part in str(value).split(",") if part.strip()]

    @property
    def resolved_rest_base_url(self) -> str:
        if self.binance_rest_base_url:
            return self.binance_rest_base_url.rstrip("/")
        if self.binance_env is Environment.TESTNET:
            return "https://testnet.binance.vision"
        return "https://api.binance.com"

    @property
    def resolved_market_ws_url(self) -> str:
        if self.binance_market_ws_url:
            return self.binance_market_ws_url.rstrip("/")
        if self.binance_env is Environment.TESTNET:
            return "wss://stream.testnet.binance.vision/stream"
        return "wss://stream.binance.com:9443/stream"

    @property
    def resolved_ws_api_url(self) -> str:
        if self.binance_ws_api_url:
            return self.binance_ws_api_url.rstrip("/")
        if self.binance_env is Environment.TESTNET:
            return "wss://ws-api.testnet.binance.vision/ws-api/v3"
        return "wss://ws-api.binance.com:443/ws-api/v3"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
