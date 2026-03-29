from quantide.core.runtime import gateway_market


class DummyClient:
    def __init__(self):
        self.login_calls = 0

    def ensure_login(self) -> None:
        self.login_calls += 1

    def cookie_header(self) -> str:
        return "session=test"

    def ws_url(self, path: str) -> str:
        return f"ws://127.0.0.1:8130{path}"


class DummyThread:
    def __init__(self, target, name: str, daemon: bool):
        self.target = target
        self.name = name
        self.daemon = daemon
        self.started = False

    def is_alive(self) -> bool:
        return self.started

    def start(self) -> None:
        self.started = True


def test_gateway_market_start_does_not_login_synchronously(monkeypatch):
    monkeypatch.setattr(gateway_market.threading, "Thread", DummyThread)
    client = DummyClient()

    market_data = gateway_market.GatewayMarketDataAdapter(client)
    market_data.start()

    assert client.login_calls == 0
    assert market_data._thread is not None
    assert market_data._thread.started is True


def test_gateway_market_build_ws_headers_ensures_login():
    client = DummyClient()
    market_data = gateway_market.GatewayMarketDataAdapter(client)

    headers = market_data._build_ws_headers()

    assert client.login_calls == 1
    assert headers == {"Cookie": "session=test"}