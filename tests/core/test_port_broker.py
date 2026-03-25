import pytest

from pyqmt.core.enums import BrokerKind, OrderSide, OrderStatus
from pyqmt.core.runtime.gateway_broker import GatewayBrokerAdapter
from pyqmt.core.runtime.port_broker import PortBackedBroker
from tests.core.test_gateway_broker_adapter import DummyGatewayClient


def test_port_backed_broker_maps_asset_positions_and_orders():
    handle = PortBackedBroker(
        port=GatewayBrokerAdapter(DummyGatewayClient()),
        portfolio_id="gateway",
        kind=BrokerKind.QMT,
        portfolio_name="gateway",
        status=True,
        is_connected=True,
    )

    assert handle.asset.total == 101000
    assert handle.cash == 50000
    assert handle.total_assets == 101000

    positions = handle.positions
    assert "000001.SZ" in positions
    assert positions["000001.SZ"].mv == 2100

    orders = handle.orders
    assert len(orders) == 1
    assert orders[0].asset == "000001.SZ"
    assert orders[0].side == OrderSide.BUY
    assert orders[0].status == OrderStatus.REPORTED


@pytest.mark.asyncio
async def test_port_backed_broker_delegates_high_level_order_submission():
    client = DummyGatewayClient()
    handle = PortBackedBroker(
        port=GatewayBrokerAdapter(client),
        portfolio_id="gateway",
        kind=BrokerKind.QMT,
        portfolio_name="gateway",
        status=True,
        is_connected=True,
    )

    result = await handle.buy_amount("000001.SZ", 5000, price=10, strategy_id="s1")

    assert result.order_id == "1001"
    assert client.post_calls[-1][0] == "/api/trade/buy"