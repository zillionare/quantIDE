import pytest

from pyqmt.core.enums import OrderSide
from pyqmt.core.ports import OrderRequest
from pyqmt.core.runtime.gateway_broker import GatewayBrokerAdapter


class DummyGatewayClient:
    def __init__(self):
        self.post_calls = []
        self.get_calls = []

    def post_form(self, path, data):
        self.post_calls.append((path, data))
        if path == "/api/trade/buy":
            return {"success": True, "order_id": "1001"}
        return {"success": True}

    def get_json(self, path, params=None):
        self.get_calls.append((path, params))
        if path == "/api/trade/asset":
            return {
                "principal": 100000,
                "total": 101000,
                "cash": 50000,
                "market_value": 51000,
                "frozen_cash": 0,
            }
        if path == "/api/trade/positions":
            return [{"symbol": "000001.SZ", "shares": 200, "avail": 200, "cost": 10, "market_value": 2100}]
        return []


@pytest.mark.asyncio
async def test_gateway_broker_submit_shares():
    adapter = GatewayBrokerAdapter(DummyGatewayClient())
    req = OrderRequest(asset="000001.SZ", side=OrderSide.BUY, value=200, price=10.2)
    ack = await adapter.submit(req)
    assert ack.status == "submitted"
    assert ack.order_id == "1001"


@pytest.mark.asyncio
async def test_gateway_broker_submit_amount_style():
    adapter = GatewayBrokerAdapter(DummyGatewayClient())
    req = OrderRequest(
        asset="000001.SZ",
        side=OrderSide.BUY,
        value=5000,
        style="amount",
        price=10,
    )
    ack = await adapter.submit(req)
    assert ack.status == "submitted"


def test_gateway_broker_query_assets():
    adapter = GatewayBrokerAdapter(DummyGatewayClient())
    assets = adapter.query_assets()
    assert assets is not None
    assert assets.total == 101000


@pytest.mark.asyncio
async def test_gateway_broker_submit_target_pct_style():
    adapter = GatewayBrokerAdapter(DummyGatewayClient())
    req = OrderRequest(
        asset="000001.SZ",
        side=OrderSide.BUY,
        value=0.2,
        style="target_pct",
        price=10,
    )
    ack = await adapter.submit(req)
    assert ack.status == "submitted"
