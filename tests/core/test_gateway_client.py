from quantide.core.runtime.gateway_client import GatewayClient


def test_gateway_client_ws_url_http():
    client = GatewayClient(
        base_url="http://127.0.0.1:8130",
        username="u",
        password="p",
    )
    assert client.ws_url("/ws/quotes") == "ws://127.0.0.1:8130/ws/quotes"


def test_gateway_client_ws_url_https():
    client = GatewayClient(
        base_url="https://gateway.example.com",
        username="u",
        password="p",
    )
    assert client.ws_url("/ws/quotes") == "wss://gateway.example.com/ws/quotes"
