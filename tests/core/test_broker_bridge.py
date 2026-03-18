import pytest

from pyqmt.core.enums import OrderSide
from pyqmt.core.ports import OrderRequest
from pyqmt.core.runtime import LegacyBrokerPortAdapter
from pyqmt.service.base_broker import TradeResult


class DummyBroker:
    def __init__(self):
        self.calls = []
        self.positions = {}

    async def buy(self, **kwargs):
        self.calls.append(("buy", kwargs))
        return TradeResult("oid-buy", [])

    async def sell(self, **kwargs):
        self.calls.append(("sell", kwargs))
        return TradeResult("oid-sell", [])

    async def buy_amount(self, **kwargs):
        self.calls.append(("buy_amount", kwargs))
        return TradeResult("oid-buy-amount", [])

    async def sell_amount(self, **kwargs):
        self.calls.append(("sell_amount", kwargs))
        return TradeResult("oid-sell-amount", [])

    async def buy_percent(self, **kwargs):
        self.calls.append(("buy_percent", kwargs))
        return TradeResult("oid-buy-percent", [])

    async def sell_percent(self, **kwargs):
        self.calls.append(("sell_percent", kwargs))
        return TradeResult("oid-sell-percent", [])

    async def trade_target_pct(self, **kwargs):
        self.calls.append(("trade_target_pct", kwargs))
        return TradeResult("oid-target", [])

    async def cancel_order(self, order_id):
        self.calls.append(("cancel_order", {"order_id": order_id}))

    async def cancel_all_orders(self, side=None):
        self.calls.append(("cancel_all_orders", {"side": side}))


@pytest.mark.asyncio
async def test_broker_bridge_submit_by_shares():
    broker = DummyBroker()
    adapter = LegacyBrokerPortAdapter(broker, portfolio_id="p1")
    req = OrderRequest(asset="000001.SZ", side=OrderSide.BUY, value=100)

    ack = await adapter.submit(req)

    assert ack.order_id == "oid-buy"
    assert ack.status == "submitted"
    assert broker.calls[0][0] == "buy"


@pytest.mark.asyncio
async def test_broker_bridge_submit_target_pct():
    broker = DummyBroker()
    adapter = LegacyBrokerPortAdapter(broker, portfolio_id="p1")
    req = OrderRequest(
        asset="000001.SZ",
        side=OrderSide.BUY,
        value=0.2,
        style="target_pct",
    )

    ack = await adapter.submit(req)

    assert ack.order_id == "oid-target"
    assert broker.calls[0][0] == "trade_target_pct"


@pytest.mark.asyncio
async def test_broker_bridge_cancel():
    broker = DummyBroker()
    adapter = LegacyBrokerPortAdapter(broker, portfolio_id="p1")

    ack = await adapter.cancel("oid-1")

    assert ack.success is True
    assert broker.calls[0] == ("cancel_order", {"order_id": "oid-1"})
