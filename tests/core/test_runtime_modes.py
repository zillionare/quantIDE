import datetime
from types import SimpleNamespace

from pyqmt.core.domain import QuoteSnapshot
from pyqmt.core.enums import BrokerKind
from pyqmt.core.runtime import modes as runtime_modes
from pyqmt.data.sqlite import Asset, Portfolio, db


class DummyGatewayMarketData:
    def __init__(self, client):
        self.client = client
        self.started = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.started = False

    def subscribe(self, symbols: list[str]) -> None:
        _ = symbols

    def unsubscribe(self, symbols: list[str]) -> None:
        _ = symbols

    async def stream(self):
        if False:
            yield None

    def snapshot(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        symbol = symbols[0]
        return {
            symbol: QuoteSnapshot(
                symbol=symbol,
                price=10.0,
                open=10.0,
                high=10.0,
                low=10.0,
                volume=1000,
                amount=10000.0,
                ts=datetime.datetime.now(),
            )
        }


def test_runtime_bootstrap_paper_uses_mock_gateway_market_data(monkeypatch):
    db.init(":memory:")
    db.insert_portfolio(
        Portfolio(
            portfolio_id="paper-account",
            kind=BrokerKind.SIMULATION,
            start=datetime.date(2024, 1, 2),
            name="paper-account",
            status=True,
        )
    )
    db.upsert_asset(
        Asset(
            portfolio_id="paper-account",
            dt=datetime.date(2024, 1, 2),
            principal=100000,
            cash=100000,
            frozen_cash=0,
            market_value=0,
            total=100000,
        )
    )

    runtime_cfg = SimpleNamespace(
        runtime_mode="paper",
        gateway_enabled=True,
        runtime_market_adapter="gateway",
        runtime_broker_adapter="",
        livequote_mode="none",
    )

    monkeypatch.setattr(runtime_modes, "get_runtime_config", lambda: runtime_cfg)
    monkeypatch.setattr(runtime_modes.scheduler, "start", lambda: None)
    monkeypatch.setattr(runtime_modes.GatewayClient, "from_config", staticmethod(lambda: object()))
    monkeypatch.setattr(runtime_modes, "GatewayMarketDataAdapter", DummyGatewayMarketData)

    runtime = runtime_modes.RuntimeBootstrap(mode="paper").bootstrap()

    assert runtime.mode == "paper"
    assert isinstance(runtime.market_data, DummyGatewayMarketData)
    assert runtime.market_data.started is True

    handle = runtime.registry.get(BrokerKind.SIMULATION, "paper-account")
    assert handle is not None
    assert handle.portfolio_id == "paper-account"
    assert handle.portfolio_name == "paper-account"
    assert getattr(handle, "_market_data") is runtime.market_data
