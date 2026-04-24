import hmac
import json
from decimal import Decimal
from hashlib import sha256

from binance_trade.signing import Authenticator, HMACSigner, build_rest_query, build_ws_payload


def test_build_rest_query_and_hmac_signature_match_binance_example() -> None:
    secret = "NhqPtmdSJYdKjVHj0LTKxj8nxKqVv6YBB0W4r4ewlQwVlQJ6S9T9fQ"
    signer = HMACSigner(secret)
    payload = (
        "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559"
    )
    expected = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), sha256).hexdigest()
    assert signer.sign(payload.encode("utf-8")) == expected
    assert build_rest_query({"symbol": "BTCUSDT", "recvWindow": Decimal("5000")}) == "symbol=BTCUSDT&recvWindow=5000"


def test_build_ws_payload_and_hmac_signature_match_binance_example() -> None:
    secret = "NhqPtmdSJYdKjVHj0LTKxj8nxKqVv6YBB0W4r4ewlQwVlQJ6S9T9fQ"
    signer = HMACSigner(secret)
    payload = (
        "apiKey=vmPUZE6mv9SD5VNHFnV7A5H6mB9xM2yzA2HhRj7jG6B8o7E8P8t7Bv&newOrderRespType=ACK&price=52000.00"
        "&quantity=0.01000000&recvWindow=100&side=SELL&symbol=BTCUSDT&timeInForce=GTC&timestamp=1645423376532&type=LIMIT"
    )
    assert build_ws_payload({
        "symbol": "BTCUSDT",
        "side": "SELL",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": "0.01000000",
        "price": "52000.00",
        "recvWindow": 100,
        "timestamp": 1645423376532,
        "newOrderRespType": "ACK",
        "apiKey": "vmPUZE6mv9SD5VNHFnV7A5H6mB9xM2yzA2HhRj7jG6B8o7E8P8t7Bv",
    }) == payload
    expected = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), sha256).hexdigest()
    assert signer.sign(payload.encode("utf-8")) == expected


def test_authenticator_adds_timestamp_recv_window_and_signature() -> None:
    auth = Authenticator(api_key="key", signer=HMACSigner("secret"), recv_window_ms=Decimal("5000"))
    auth.time_offset_ms = 123
    params = auth.build_ws_signed_params({"symbol": "BTCUSDT"})
    assert params["apiKey"] == "key"
    assert params["recvWindow"] == 5000
    assert "timestamp" in params
    assert "signature" in params
    json.dumps(params)
