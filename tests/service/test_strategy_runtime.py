import pytest

from quantide.service.strategy_runtime import StrategyBrokerProxy


class DummyBroker:
    def __init__(self):
        self.calls = []

    async def submit(self, request):
        raise AssertionError("StrategyBrokerProxy should not fallback to submit")

    async def buy_amount(
        self,
        asset,
        amount,
        price=0,
        order_time=None,
        timeout=0.5,
        **kwargs,
    ):
        self.calls.append(
            {
                "asset": asset,
                "amount": amount,
                "price": price,
                "order_time": order_time,
                "timeout": timeout,
                "kwargs": kwargs,
            }
        )
        return {"ok": True}


@pytest.mark.asyncio
async def test_strategy_broker_proxy_uses_high_level_methods_with_strategy_id():
    broker = DummyBroker()
    proxy = StrategyBrokerProxy(broker, "strategy-1")

    result = await proxy.buy_amount("000001.SZ", 5000, price=10)

    assert result == {"ok": True}
    assert broker.calls == [
        {
            "asset": "000001.SZ",
            "amount": 5000,
            "price": 10,
            "order_time": None,
            "timeout": 0.5,
            "kwargs": {"strategy_id": "strategy-1"},
        }
    ]