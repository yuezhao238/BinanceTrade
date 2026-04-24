from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .exceptions import ConfigError
from .types import ApiKeyType, Environment


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
        enable_decoding=False,
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
    futures_default_symbol: str = Field(default="BTCUSDT", alias="FUTURES_DEFAULT_SYMBOL")
    futures_order_prefix: str = Field(default="bf", alias="FUTURES_ORDER_PREFIX")
    state_db_path: Path = Field(default=Path("var/state.db"), alias="STATE_DB_PATH")
    runtime_dir: Path = Field(default=Path("var/runtime"), alias="RUNTIME_DIR")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    dry_run: bool = Field(default=True, alias="DRY_RUN")

    max_order_notional: float = Field(default=50.0, alias="MAX_ORDER_NOTIONAL")
    max_open_orders_per_symbol: int = Field(default=5, alias="MAX_OPEN_ORDERS_PER_SYMBOL")
    order_cooldown_seconds: int = Field(default=5, alias="ORDER_COOLDOWN_SECONDS")
    allowed_symbols: list[str] = Field(default_factory=list, alias="ALLOWED_SYMBOLS")
    futures_max_order_notional: float | None = Field(default=None, alias="FUTURES_MAX_ORDER_NOTIONAL")
    futures_max_open_orders_per_symbol: int | None = Field(default=None, alias="FUTURES_MAX_OPEN_ORDERS_PER_SYMBOL")
    futures_order_cooldown_seconds: int | None = Field(default=None, alias="FUTURES_ORDER_COOLDOWN_SECONDS")
    futures_allowed_symbols: list[str] = Field(default_factory=list, alias="FUTURES_ALLOWED_SYMBOLS")

    request_timeout_seconds: float = Field(default=10.0, alias="REQUEST_TIMEOUT_SECONDS")
    network_trust_env: bool = Field(default=False, alias="NETWORK_TRUST_ENV")
    recv_window_ms: float = Field(default=5000.0, alias="RECV_WINDOW_MS")
    daemon_heartbeat_interval_seconds: int = Field(default=30, alias="DAEMON_HEARTBEAT_INTERVAL_SECONDS")
    daemon_reconcile_interval_seconds: int = Field(default=300, alias="DAEMON_RECONCILE_INTERVAL_SECONDS")
    daemon_restart_delay_seconds: int = Field(default=5, alias="DAEMON_RESTART_DELAY_SECONDS")
    daemon_max_restart_delay_seconds: int = Field(default=60, alias="DAEMON_MAX_RESTART_DELAY_SECONDS")
    daemon_stale_after_seconds: int = Field(default=90, alias="DAEMON_STALE_AFTER_SECONDS")
    binance_futures_rest_base_url: str | None = Field(default=None, alias="BINANCE_FUTURES_REST_BASE_URL")
    binance_futures_market_ws_url: str | None = Field(default=None, alias="BINANCE_FUTURES_MARKET_WS_URL")
    binance_futures_user_ws_base_url: str | None = Field(default=None, alias="BINANCE_FUTURES_USER_WS_BASE_URL")
    binance_futures_ws_api_url: str | None = Field(default=None, alias="BINANCE_FUTURES_WS_API_URL")

    @field_validator("default_symbol", "futures_default_symbol")
    @classmethod
    def _upper_default_symbol(cls, value: str) -> str:
        return value.upper()

    @field_validator(
        "daemon_heartbeat_interval_seconds",
        "daemon_reconcile_interval_seconds",
        "daemon_restart_delay_seconds",
        "daemon_max_restart_delay_seconds",
        "daemon_stale_after_seconds",
    )
    @classmethod
    def _positive_seconds(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("daemon timing values must be positive")
        return value

    @field_validator("allowed_symbols", "futures_allowed_symbols", mode="before")
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
        if self.binance_env is Environment.BINANCE_US:
            return "https://api.binance.us"
        return "https://api.binance.com"

    @property
    def resolved_market_ws_url(self) -> str:
        if self.binance_market_ws_url:
            return self.binance_market_ws_url.rstrip("/")
        if self.binance_env is Environment.TESTNET:
            return "wss://stream.testnet.binance.vision/stream"
        if self.binance_env is Environment.BINANCE_US:
            return "wss://stream.binance.us:9443/stream"
        return "wss://stream.binance.com:9443/stream"

    @property
    def resolved_ws_api_url(self) -> str:
        if self.binance_ws_api_url:
            return self.binance_ws_api_url.rstrip("/")
        if self.binance_env is Environment.TESTNET:
            return "wss://ws-api.testnet.binance.vision/ws-api/v3"
        if self.binance_env is Environment.BINANCE_US:
            return "wss://ws-api.binance.us:443/ws-api/v3"
        return "wss://ws-api.binance.com:443/ws-api/v3"

    def assert_futures_supported(self) -> None:
        if self.binance_env is Environment.BINANCE_US:
            raise ConfigError(
                "BINANCE_ENV=binance_us is spot-only in this project. Use BINANCE_ENV=mainnet or "
                "BINANCE_ENV=testnet for Binance.com USDⓈ-M futures."
            )

    @property
    def resolved_futures_rest_base_url(self) -> str:
        self.assert_futures_supported()
        if self.binance_futures_rest_base_url:
            return self.binance_futures_rest_base_url.rstrip("/")
        if self.binance_env is Environment.TESTNET:
            return "https://demo-fapi.binance.com"
        return "https://fapi.binance.com"

    @property
    def resolved_futures_market_ws_url(self) -> str:
        self.assert_futures_supported()
        if self.binance_futures_market_ws_url:
            return self.binance_futures_market_ws_url.rstrip("/")
        if self.binance_env is Environment.TESTNET:
            return "wss://fstream.binancefuture.com/stream"
        return "wss://fstream.binance.com/stream"

    @property
    def resolved_futures_user_ws_base_url(self) -> str:
        self.assert_futures_supported()
        if self.binance_futures_user_ws_base_url:
            return self.binance_futures_user_ws_base_url.rstrip("/")
        if self.binance_env is Environment.TESTNET:
            return "wss://fstream.binancefuture.com"
        return "wss://fstream.binance.com"

    @property
    def resolved_futures_ws_api_url(self) -> str:
        self.assert_futures_supported()
        if self.binance_futures_ws_api_url:
            return self.binance_futures_ws_api_url.rstrip("/")
        if self.binance_env is Environment.TESTNET:
            return "wss://testnet.binancefuture.com/ws-fapi/v1"
        return "wss://ws-fapi.binance.com/ws-fapi/v1"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
