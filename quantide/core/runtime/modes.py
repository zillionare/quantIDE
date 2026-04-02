"""运行时模式装配."""

from dataclasses import dataclass
from typing import Any, Literal

from quantide.config.settings import get_settings
from quantide.core.enums import BrokerKind
from quantide.core.ports import MarketDataPort
from quantide.core.runtime.adapter_registry import AdapterRegistry
from quantide.core.runtime.broker_bridge import LegacyBrokerPortAdapter
from quantide.core.runtime.gateway_broker import GatewayBrokerAdapter
from quantide.core.runtime.gateway_client import GatewayClient
from quantide.core.runtime.gateway_market import GatewayMarketDataAdapter
from quantide.core.runtime.market_bridge import LiveQuoteMarketDataAdapter
from quantide.core.runtime.registration import (
    register_legacy_broker,
    register_port_backed_broker,
)
from quantide.core.scheduler import scheduler
from quantide.data import db
from quantide.service.livequote import live_quote
from quantide.service.registry import BrokerRegistry
from quantide.service.sim_broker import PaperBroker

RuntimeMode = Literal["live", "paper", "backtest"]


@dataclass
class RuntimeContext:
    """运行时上下文."""

    mode: RuntimeMode
    registry: BrokerRegistry
    adapters: AdapterRegistry
    market_data: MarketDataPort

    def register_legacy_broker(
        self,
        broker: Any,
        portfolio_id: str,
        kind: BrokerKind | str,
        *,
        portfolio_name: str = "",
        status: bool | None = None,
        is_connected: bool | None = None,
    ):
        """将 legacy broker 注册到正式运行时。"""
        return register_legacy_broker(
            registry=self.registry,
            adapters=self.adapters,
            broker=broker,
            portfolio_id=portfolio_id,
            kind=kind,
            portfolio_name=portfolio_name,
            status=status,
            is_connected=is_connected,
        )

    def register_port_broker(
        self,
        port: Any,
        portfolio_id: str,
        kind: BrokerKind | str,
        *,
        adapter_name: str | None = None,
        portfolio_name: str = "",
        status: bool = True,
        is_connected: bool | None = None,
        legacy: Any | None = None,
    ):
        """将正式 broker port 注册到运行时。"""
        return register_port_backed_broker(
            registry=self.registry,
            adapters=self.adapters,
            port=port,
            portfolio_id=portfolio_id,
            kind=kind,
            adapter_name=adapter_name,
            portfolio_name=portfolio_name,
            status=status,
            is_connected=is_connected,
            legacy=legacy,
        )


class RuntimeBootstrap:
    """运行时装配器."""

    def __init__(self, mode: RuntimeMode | None = None):
        """初始化装配器.

        Args:
            mode: 指定运行模式，不传则自动解析。
        """
        self._mode = mode or self._resolve_mode()

    def bootstrap(self) -> RuntimeContext:
        """执行运行时装配."""
        scheduler.start()
        adapters = AdapterRegistry()
        market_data = self._build_market_data(adapters)
        registry = BrokerRegistry()
        self._registry_ref = registry
        self._load_accounts_from_db(registry, market_data=market_data)
        self._register_broker_adapters(registry, adapters)
        self._register_gateway_broker_adapter(adapters)
        return RuntimeContext(
            mode=self._mode,
            registry=registry,
            adapters=adapters,
            market_data=market_data,
        )

    def _resolve_mode(self) -> RuntimeMode:
        """解析运行模式."""
        runtime = get_settings()
        raw = runtime.runtime_mode
        if raw in {"live", "paper", "backtest"}:
            return raw  # type: ignore
        if runtime.livequote_mode == "none" or not runtime.gateway_enabled:
            return "backtest"
        return "live"

    def _build_market_data(self, adapters: AdapterRegistry) -> MarketDataPort:
        """构建行情适配器."""
        runtime = get_settings()
        adapter_name = runtime.runtime_market_adapter
        mode_name = runtime.livequote_mode
        use_gateway = runtime.gateway_enabled and (
            adapter_name == "gateway" or mode_name == "gateway"
        )
        if use_gateway:
            client = GatewayClient.from_config()
            market_data = GatewayMarketDataAdapter(client)
            market_data.start()
            adapters.register("market_data", "gateway", market_data)
            return market_data
        live_quote.start()
        market_data = LiveQuoteMarketDataAdapter(live_quote)
        adapters.register("market_data", "live_quote", market_data)
        return market_data

    def _register_broker_adapters(
        self, registry: BrokerRegistry, adapters: AdapterRegistry
    ) -> None:
        """注册已加载账户的 broker 适配器."""
        for item in registry.list():
            kind = item.get("kind")
            portfolio_id = item.get("id")
            if not kind or not portfolio_id:
                continue
            broker = registry.get(kind, portfolio_id)
            if broker is None:
                continue
            name = f"{kind}:{portfolio_id}"
            register_legacy_broker(
                registry=registry,
                adapters=adapters,
                broker=broker,
                portfolio_id=portfolio_id,
                kind=kind,
                adapter_name=name,
            )

    def _load_accounts_from_db(
        self,
        registry: BrokerRegistry,
        market_data: MarketDataPort | None = None,
    ) -> None:
        """从数据库加载账户."""
        try:
            portfolios = db.get_all_portfolios()
        except RuntimeError:
            return
        for portfolio in portfolios:
            if portfolio.kind != BrokerKind.SIMULATION:
                continue
            try:
                broker = PaperBroker.load(
                    portfolio.portfolio_id,
                    market_data=market_data,
                )
                registry.register(BrokerKind.SIMULATION, portfolio.portfolio_id, broker)
            except Exception:
                continue

    def _register_gateway_broker_adapter(self, adapters: AdapterRegistry) -> None:
        """注册 gateway 交易适配器."""
        runtime = get_settings()
        broker_name = runtime.runtime_broker_adapter
        mode_name = runtime.livequote_mode
        use_gateway = runtime.gateway_enabled and (
            broker_name == "gateway" or mode_name == "gateway"
        )
        if not use_gateway:
            return
        client = GatewayClient.from_config()
        adapter = GatewayBrokerAdapter(client)
        registry = getattr(self, "_registry_ref", None)
        if registry:
            register_port_backed_broker(
                registry=registry,
                adapters=adapters,
                port=adapter,
                portfolio_id="gateway",
                kind=BrokerKind.QMT,
                adapter_name="gateway:default",
                portfolio_name="实盘网关",
                status=True,
                is_connected=True,
            )
