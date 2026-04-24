from __future__ import annotations

import base64
import hmac
import time
import urllib.parse
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any, Mapping, Protocol

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding, rsa

from .config import Settings
from .exceptions import ConfigError
from .types import ApiKeyType
from .utils import decimal_to_str, normalize_param_value


class Signer(Protocol):
    def sign(self, payload: bytes) -> str:
        ...


def _clean_params(params: Mapping[str, Any]) -> list[tuple[str, str]]:
    return [(key, normalize_param_value(value)) for key, value in params.items() if value is not None]


def build_rest_query(params: Mapping[str, Any]) -> str:
    return urllib.parse.urlencode(
        _clean_params(params),
        quote_via=urllib.parse.quote,
        safe="",
        encoding="utf-8",
    )


def build_ws_payload(params: Mapping[str, Any]) -> str:
    pairs = sorted(_clean_params(params), key=lambda item: item[0])
    return "&".join(f"{key}={value}" for key, value in pairs)


def _json_safe_ws_param_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return decimal_to_str(value)
    return value


class HMACSigner:
    def __init__(self, secret: str) -> None:
        self._secret = secret.encode("utf-8")

    def sign(self, payload: bytes) -> str:
        return hmac.new(self._secret, payload, sha256).hexdigest()


class RSASigner:
    def __init__(self, key: rsa.RSAPrivateKey) -> None:
        self._key = key

    def sign(self, payload: bytes) -> str:
        signature = self._key.sign(payload, padding.PKCS1v15(), hashes.SHA256())
        return base64.b64encode(signature).decode("ascii")


class Ed25519Signer:
    def __init__(self, key: ed25519.Ed25519PrivateKey) -> None:
        self._key = key

    def sign(self, payload: bytes) -> str:
        signature = self._key.sign(payload)
        return base64.b64encode(signature).decode("ascii")


def _load_private_key(path: Path, passphrase: str | None) -> Any:
    password = passphrase.encode("utf-8") if passphrase else None
    with path.open("rb") as handle:
        return serialization.load_pem_private_key(handle.read(), password=password)


def build_signer(settings: Settings) -> Signer | None:
    if not settings.binance_api_key:
        return None

    if settings.binance_api_key_type is ApiKeyType.HMAC:
        if not settings.binance_api_secret:
            raise ConfigError("BINANCE_API_SECRET is required for HMAC keys")
        return HMACSigner(settings.binance_api_secret)

    if not settings.binance_private_key_path:
        raise ConfigError("BINANCE_PRIVATE_KEY_PATH is required for RSA or ED25519 keys")

    key = _load_private_key(settings.binance_private_key_path, settings.binance_private_key_passphrase)
    if settings.binance_api_key_type is ApiKeyType.RSA:
        if not isinstance(key, rsa.RSAPrivateKey):
            raise ConfigError("BINANCE_PRIVATE_KEY_PATH does not contain an RSA private key")
        return RSASigner(key)

    if not isinstance(key, ed25519.Ed25519PrivateKey):
        raise ConfigError("BINANCE_PRIVATE_KEY_PATH does not contain an Ed25519 private key")
    return Ed25519Signer(key)


@dataclass(slots=True)
class Authenticator:
    api_key: str
    signer: Signer
    recv_window_ms: Decimal
    time_offset_ms: int = 0

    def now_ms(self) -> int:
        return int(time.time() * 1000) + self.time_offset_ms

    def set_server_time(self, server_time_ms: int) -> None:
        self.time_offset_ms = server_time_ms - int(time.time() * 1000)

    def build_rest_signed_payload(self, params: Mapping[str, Any]) -> str:
        payload_params: dict[str, Any] = dict(params)
        payload_params.setdefault("timestamp", self.now_ms())
        payload_params.setdefault("recvWindow", self.recv_window_ms)
        payload = build_rest_query(payload_params)
        payload_params["signature"] = self.signer.sign(payload.encode("utf-8"))
        return build_rest_query(payload_params)

    def build_ws_signed_params(self, params: Mapping[str, Any]) -> dict[str, Any]:
        payload_params: dict[str, Any] = dict(params)
        payload_params.setdefault("apiKey", self.api_key)
        payload_params.setdefault("timestamp", self.now_ms())
        payload_params.setdefault("recvWindow", self.recv_window_ms)
        payload = build_ws_payload(payload_params)
        payload_params["signature"] = self.signer.sign(payload.encode("utf-8"))
        return {key: _json_safe_ws_param_value(value) for key, value in payload_params.items()}
