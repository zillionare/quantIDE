from dataclasses import dataclass

from quantide.service.sim_broker import PaperBroker


@dataclass
class DummySnap:
    price: float | None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    volume: float | None = None
    amount: float | None = None


class DummyMarketData:
    def snapshot(self, symbols: list[str]):
        symbol = symbols[0]
        return {
            symbol: DummySnap(
                price=10.5,
                open=10.0,
                high=10.8,
                low=9.9,
                volume=1000,
                amount=10500,
            )
        }


def test_sim_broker_get_quote_from_market_data():
    broker = object.__new__(PaperBroker)
    broker._market_data = DummyMarketData()
    broker._limits = {}

    quote = broker._get_quote("000001.SZ")

    assert quote is not None
    assert quote["lastPrice"] == 10.5
    assert quote["volume"] == 1000


def test_sim_broker_get_price_limits_from_cache():
    broker = object.__new__(PaperBroker)
    broker._market_data = DummyMarketData()
    broker._limits = {"000001.SZ": {"down": 9.1, "up": 11.1}}

    down, up = broker._get_price_limits("000001.SZ")

    assert down == 9.1
    assert up == 11.1
