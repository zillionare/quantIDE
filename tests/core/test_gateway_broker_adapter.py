import pytest

from pyqmt.core.enums import OrderSide
from pyqmt.core.ports import OrderRequest
from pyqmt.core.runtime.gateway_broker import GatewayBrokerAdapter, GatewayBrokerWrapper


class DummyGatewayClient:
    def __init__(self):
        self.post_calls = []
        self.get_calls = []

    def post_form(self, path, data):
        self.post_calls.append((path, data))
        if path == "/api/trade/buy":
            return {"success": True, "qtoid": data.get("qtoid", "q1"), "order_id": "1001"}
        if path == "/api/trade/sell":
            return {"success": True, "qtoid": data.get("qtoid", "q2"), "order_id": "1002"}
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
        if path == "/api/trade/orders":
            return [{"qtoid": "1001", "symbol": "000001.SZ", "side": "buy", "shares": 200, "price": 10, "status": "submitted"}]
        return []


@pytest.mark.asyncio
async def test_gateway_broker_submit_shares():
    client = DummyGatewayClient()
    adapter = GatewayBrokerAdapter(client)
    req = OrderRequest(asset="000001.SZ", side=OrderSide.BUY, value=200, price=10.2)
    ack = await adapter.submit(req)
    assert ack.status == "submitted"
    assert ack.order_id == client.post_calls[-1][1]["qtoid"]


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


@pytest.mark.asyncio
async def test_gateway_broker_buy_amount_returns_execution_result():
    client = DummyGatewayClient()
    adapter = GatewayBrokerAdapter(client)

    result = await adapter.buy_amount("000001.SZ", 5000, price=10, strategy_id="s1")

    assert result.order_id == client.post_calls[-1][1]["qtoid"]
    assert result.status == "submitted"
    assert client.post_calls[-1][1]["strategy_id"] == "s1"


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


@pytest.mark.asyncio
async def test_gateway_broker_trade_target_pct_submits_sell_when_overweight():
    client = DummyGatewayClient()
    adapter = GatewayBrokerAdapter(client)

    result = await adapter.trade_target_pct("000001.SZ", 0, price=10)

    path, payload = client.post_calls[-1]
    assert result.order_id is not None
    assert path == "/api/trade/sell"
    assert payload["symbol"] == "000001.SZ"
    assert payload["qtoid"] == result.order_id


@pytest.mark.asyncio
async def test_gateway_broker_wrapper_submit_amount_and_cancel_all():
    client = DummyGatewayClient()
    adapter = GatewayBrokerAdapter(client)
    wrapper = GatewayBrokerWrapper(adapter)

    result = await wrapper.buy_amount("000001.SZ", 5000, price=10)

    assert result.qt_oid == client.post_calls[-1][1]["qtoid"]

    await wrapper.cancel_all_orders(side=OrderSide.BUY)

    assert client.get_calls[-1] == ("/api/trade/orders", None)
    assert client.post_calls[-1][0] == "/api/trade/cancel"
    assert "qtoid" in client.post_calls[-1][1]


@pytest.mark.asyncio
async def test_gateway_broker_wrapper_trade_target_pct_submits_sell_when_overweight():
    client = DummyGatewayClient()
    adapter = GatewayBrokerAdapter(client)
    wrapper = GatewayBrokerWrapper(adapter)

    await wrapper.trade_target_pct("000001.SZ", 0, price=10)

    path, payload = client.post_calls[-1]
    assert path == "/api/trade/sell"
    assert payload["symbol"] == "000001.SZ"
