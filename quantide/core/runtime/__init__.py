"""运行时基础模块导出."""

from quantide.core.runtime.adapter_registry import AdapterRegistry, AdapterSpec
from quantide.core.runtime.broker_bridge import LegacyBrokerPortAdapter
from quantide.core.runtime.clock_bridge import BacktestClockAdapter, SystemClockAdapter
from quantide.core.runtime.gateway_broker import GatewayBrokerAdapter
from quantide.core.runtime.gateway_client import GatewayClient
from quantide.core.runtime.gateway_market import GatewayMarketDataAdapter
from quantide.core.runtime.market_bridge import LiveQuoteMarketDataAdapter
from quantide.core.runtime.port_broker import PortBackedBroker
from quantide.core.runtime.registration import (
    register_legacy_broker,
    register_port_backed_broker,
)

__all__ = [
    "AdapterSpec",
    "AdapterRegistry",
    "LegacyBrokerPortAdapter",
    "PortBackedBroker",
    "SystemClockAdapter",
    "BacktestClockAdapter",
    "GatewayClient",
    "GatewayBrokerAdapter",
    "GatewayMarketDataAdapter",
    "LiveQuoteMarketDataAdapter",
    "register_legacy_broker",
    "register_port_backed_broker",
    "RuntimeMode",
    "RuntimeContext",
    "RuntimeBootstrap",
]


def __getattr__(name: str):
    if name in {"RuntimeMode", "RuntimeContext", "RuntimeBootstrap"}:
        from quantide.core.runtime.modes import RuntimeBootstrap, RuntimeContext, RuntimeMode

        mapping = {
            "RuntimeMode": RuntimeMode,
            "RuntimeContext": RuntimeContext,
            "RuntimeBootstrap": RuntimeBootstrap,
        }
        return mapping[name]
    raise AttributeError(name)
