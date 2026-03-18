"""运行时模式装配."""

from dataclasses import dataclass
from typing import Literal

from pyqmt.config import cfg
from pyqmt.core.enums import BrokerKind
from pyqmt.core.ports import MarketDataPort
from pyqmt.core.runtime.adapter_registry import AdapterRegistry
from pyqmt.core.runtime.broker_bridge import LegacyBrokerPortAdapter
from pyqmt.core.runtime.gateway_broker import GatewayBrokerAdapter
from pyqmt.core.runtime.gateway_client import GatewayClient
from pyqmt.core.runtime.gateway_market import GatewayMarketDataAdapter
from pyqmt.core.runtime.market_bridge import LiveQuoteMarketDataAdapter
from pyqmt.core.scheduler import scheduler
from pyqmt.data import db
from pyqmt.service.livequote import live_quote
from pyqmt.service.registry import BrokerRegistry
from pyqmt.service.sim_broker import PaperBroker

RuntimeMode = Literal["live", "paper", "backtest"]


@dataclass
class RuntimeContext:
    """运行时上下文."""

    mode: RuntimeMode
    registry: BrokerRegistry
    adapters: AdapterRegistry
    market_data: MarketDataPort


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
        runtime = getattr(cfg, "runtime", None)
        raw = str(getattr(runtime, "mode", "") or "").strip().lower()
        if not raw:
            raw = str(getattr(cfg, "runtime_mode", "") or "").strip().lower()
        if raw in {"live", "paper", "backtest"}:
            return raw  # type: ignore
        if cfg.livequote.mode == "none":
            return "backtest"
        return "live"

    def _build_market_data(self, adapters: AdapterRegistry) -> MarketDataPort:
        """构建行情适配器."""
        runtime = getattr(cfg, "runtime", None)
        adapter_name = str(getattr(runtime, "market_adapter", "") or "").strip().lower()
        mode_name = str(cfg.livequote.mode).strip().lower()
        use_gateway = adapter_name == "gateway" or mode_name == "gateway"
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
            adapters.register(
                "broker",
                name,
                LegacyBrokerPortAdapter(broker, portfolio_id=portfolio_id),
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
        runtime = getattr(cfg, "runtime", None)
        broker_name = str(getattr(runtime, "broker_adapter", "") or "").strip().lower()
        mode_name = str(cfg.livequote.mode).strip().lower()
        use_gateway = broker_name == "gateway" or mode_name == "gateway"
        if not use_gateway:
            return
        client = GatewayClient.from_config()
        adapters.register("broker", "gateway:default", GatewayBrokerAdapter(client))
