import asyncio
import datetime
import json
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger

from pyqmt.config import cfg
from pyqmt.core.enums import BrokerKind, FrameType, OrderSide
from pyqmt.core.ports import OrderRequest
from pyqmt.data.sqlite import db
from pyqmt.core.runtime.gateway_broker import GatewayBrokerAdapter
from pyqmt.core.runtime.gateway_client import GatewayClient
from pyqmt.service.discovery import strategy_loader
from pyqmt.service.registry import BrokerRegistry
from pyqmt.service.sim_broker import PaperBroker


@dataclass
class StrategyRuntime:
    runtime_id: str
    mode: str
    strategy_name: str
    strategy_id: str
    portfolio_id: str
    account_kind: str
    status: str
    config: dict[str, Any]
    symbols: list[str] = field(default_factory=list)
    principal: float = 0.0
    started_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    error: str = ""
    stop_event: threading.Event | None = None
    thread: threading.Thread | None = None
    broker: Any = None


@dataclass
class BacktestRun:
    runtime_id: str
    portfolio_id: str
    strategy_name: str
    config: dict[str, Any]
    interval: str
    start_date: str
    end_date: str
    initial_cash: float
    status: str
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    error: str = ""


class StrategyBrokerProxy:
    def __init__(self, broker: Any, strategy_id: str):
        self._broker = broker
        self._strategy_id = strategy_id

    def __getattr__(self, item):
        return getattr(self._broker, item)

    async def buy(self, asset, shares, price=0, order_time=None, timeout=0.5):
        if hasattr(self._broker, "submit"):
            return await self._broker.submit(
                OrderRequest(
                    asset=asset,
                    side=OrderSide.BUY,
                    value=shares,
                    style="shares",
                    price=price,
                    order_time=order_time,
                    timeout=timeout,
                    extra={"strategy_id": self._strategy_id},
                )
            )
        return await self._broker.buy(
            asset=asset,
            shares=shares,
            price=price,
            order_time=order_time,
            timeout=timeout,
            strategy=self._strategy_id,
        )

    async def sell(self, asset, shares, price=0, order_time=None, timeout=0.5):
        if hasattr(self._broker, "submit"):
            return await self._broker.submit(
                OrderRequest(
                    asset=asset,
                    side=OrderSide.SELL,
                    value=shares,
                    style="shares",
                    price=price,
                    order_time=order_time,
                    timeout=timeout,
                    extra={"strategy_id": self._strategy_id},
                )
            )
        return await self._broker.sell(
            asset=asset,
            shares=shares,
            price=price,
            order_time=order_time,
            timeout=timeout,
            strategy=self._strategy_id,
        )

    async def buy_percent(self, asset, percent, price=0, order_time=None, timeout=0.5):
        if hasattr(self._broker, "submit"):
            return await self._broker.submit(
                OrderRequest(
                    asset=asset,
                    side=OrderSide.BUY,
                    value=percent,
                    style="percent",
                    price=price,
                    order_time=order_time,
                    timeout=timeout,
                    extra={"strategy_id": self._strategy_id},
                )
            )
        return await self._broker.buy_percent(
            asset=asset,
            percent=percent,
            price=price,
            order_time=order_time,
            timeout=timeout,
            strategy=self._strategy_id,
        )

    async def sell_percent(self, asset, percent, price=0, order_time=None, timeout=0.5):
        if hasattr(self._broker, "submit"):
            return await self._broker.submit(
                OrderRequest(
                    asset=asset,
                    side=OrderSide.SELL,
                    value=percent,
                    style="percent",
                    price=price,
                    order_time=order_time,
                    timeout=timeout,
                    extra={"strategy_id": self._strategy_id},
                )
            )
        return await self._broker.sell_percent(
            asset=asset,
            percent=percent,
            price=price,
            order_time=order_time,
            timeout=timeout,
            strategy=self._strategy_id,
        )

    async def buy_amount(self, asset, amount, price=0, order_time=None, timeout=0.5):
        if hasattr(self._broker, "submit"):
            return await self._broker.submit(
                OrderRequest(
                    asset=asset,
                    side=OrderSide.BUY,
                    value=amount,
                    style="amount",
                    price=price,
                    order_time=order_time,
                    timeout=timeout,
                    extra={"strategy_id": self._strategy_id},
                )
            )
        return await self._broker.buy_amount(
            asset=asset,
            amount=amount,
            price=price,
            order_time=order_time,
            timeout=timeout,
            strategy=self._strategy_id,
        )

    async def sell_amount(self, asset, amount, price=0, order_time=None, timeout=0.5):
        if hasattr(self._broker, "submit"):
            return await self._broker.submit(
                OrderRequest(
                    asset=asset,
                    side=OrderSide.SELL,
                    value=amount,
                    style="amount",
                    price=price,
                    order_time=order_time,
                    timeout=timeout,
                    extra={"strategy_id": self._strategy_id},
                )
            )
        return await self._broker.sell_amount(
            asset=asset,
            amount=amount,
            price=price,
            order_time=order_time,
            timeout=timeout,
            strategy=self._strategy_id,
        )


class StrategyRuntimeManager:
    def __init__(self):
        self._lock = threading.RLock()
        self._account_runtimes: dict[str, StrategyRuntime] = {}
        self._strategy_runtimes: dict[str, StrategyRuntime] = {}
        self._backtest_runtimes: dict[str, BacktestRun] = {}
        self._backtest_history: dict[str, BacktestRun] = {}
        self._runtime_specs: dict[str, dict[str, Any]] = {}
        self._registry: BrokerRegistry | None = None
        self._market_data: Any = None
        self._gateway_broker: GatewayBrokerAdapter | None = None

    def bootstrap_from_registry(self, registry: BrokerRegistry, market_data: Any = None) -> None:
        self._registry = registry
        self._market_data = market_data
        self._gateway_broker = GatewayBrokerAdapter(GatewayClient.from_config())
        with self._lock:
            self._account_runtimes = {}
            for item in registry.list():
                kind = item["kind"]
                portfolio_id = item["id"]
                mode = "live" if kind == BrokerKind.QMT.value else "paper"
                runtime_id = f"{mode}:{portfolio_id}"
                self._account_runtimes[runtime_id] = StrategyRuntime(
                    runtime_id=runtime_id,
                    mode=mode,
                    strategy_name="",
                    strategy_id="",
                    portfolio_id=portfolio_id,
                    account_kind=kind,
                    status="idle",
                    config={},
                    broker=registry.get(kind, portfolio_id),
                )
        self._load_specs()
        self._restore_persisted_runtimes()

    def create_backtest_runtime(
        self,
        portfolio_id: str,
        strategy_name: str,
        config: dict[str, Any],
        interval: str,
        start_date: str,
        end_date: str,
        initial_cash: float,
    ) -> None:
        run = BacktestRun(
            runtime_id=f"backtest:{portfolio_id}",
            portfolio_id=portfolio_id,
            strategy_name=strategy_name,
            config=config,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
            initial_cash=initial_cash,
            status="running",
        )
        with self._lock:
            self._backtest_runtimes[portfolio_id] = run
            self._backtest_history[portfolio_id] = run

    def complete_backtest_runtime(self, portfolio_id: str, error: str = "") -> None:
        with self._lock:
            run = self._backtest_history.get(portfolio_id)
            if run is not None:
                run.status = "failed" if error else "finished"
                run.error = error
                run.updated_at = datetime.datetime.now()
            if portfolio_id in self._backtest_runtimes:
                del self._backtest_runtimes[portfolio_id]

    def get_backtest_run(self, portfolio_id: str) -> BacktestRun | None:
        with self._lock:
            return self._backtest_history.get(portfolio_id)

    def deploy_to_paper(
        self,
        portfolio_id: str,
        principal: float,
        registry: BrokerRegistry,
        market_data: Any,
    ) -> StrategyRuntime:
        run = self._resolve_backtest_run(portfolio_id)
        account_id = f"paper-{run.strategy_name}-{uuid.uuid4().hex[:8]}"
        broker = PaperBroker.create(
            portfolio_id=account_id,
            portfolio_name=f"{run.strategy_name}-paper",
            principal=principal,
            market_data=market_data,
        )
        registry.register(BrokerKind.SIMULATION, account_id, broker)
        return self._start_strategy_runtime(
            mode="paper",
            strategy_name=run.strategy_name,
            config=run.config,
            broker=broker,
            portfolio_id=account_id,
            account_kind=BrokerKind.SIMULATION.value,
            interval=run.interval,
            market_data=market_data,
            principal=principal,
        )

    def deploy_to_live(
        self,
        portfolio_id: str,
        account_id: str,
        registry: BrokerRegistry,
        market_data: Any,
    ) -> StrategyRuntime:
        run = self._resolve_backtest_run(portfolio_id)
        broker = self._gateway_broker
        account_kind = "gateway"
        account_id = "gateway:default"
        if broker is None:
            raise RuntimeError("gateway broker 未初始化")
        return self._start_strategy_runtime(
            mode="live",
            strategy_name=run.strategy_name,
            config=run.config,
            broker=broker,
            portfolio_id=account_id,
            account_kind=account_kind,
            interval=run.interval,
            market_data=market_data,
            principal=0.0,
        )

    def stop_strategy_runtime(self, runtime_id: str) -> None:
        with self._lock:
            runtime = self._strategy_runtimes.get(runtime_id)
            if runtime is None:
                raise RuntimeError(f"策略运行时不存在: {runtime_id}")
            if runtime.stop_event is not None:
                runtime.stop_event.set()
            runtime.status = "stopping"
            runtime.updated_at = datetime.datetime.now()
            spec = self._runtime_specs.get(runtime_id)
            if spec is not None:
                spec["status"] = "stopped"
                self._save_specs()

    def start_strategy_runtime(self, runtime_id: str) -> StrategyRuntime:
        with self._lock:
            current = self._strategy_runtimes.get(runtime_id)
            if current is not None and current.status in {"running", "stopping"}:
                return current
            spec = self._runtime_specs.get(runtime_id)
            if spec is None:
                raise RuntimeError(f"策略运行时配置不存在: {runtime_id}")
            spec["status"] = "running"
            self._save_specs()
        return self._start_from_spec(spec)

    def list_runtime_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        with self._lock:
            for item in self._account_runtimes.values():
                rows.append(self._runtime_to_row(item))
            for item in self._backtest_runtimes.values():
                rows.append(
                    {
                        "runtime_id": item.runtime_id,
                        "mode": "backtest",
                        "portfolio_id": item.portfolio_id,
                        "strategy_name": item.strategy_name,
                        "strategy_id": "",
                        "status": item.status,
                        "principal": item.initial_cash,
                        "cash": 0.0,
                        "market_value": 0.0,
                        "total": 0.0,
                        "positions": 0,
                        "orders": 0,
                        "updated_at": item.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
                        "error": item.error,
                    }
                )
            for item in self._strategy_runtimes.values():
                rows.append(self._runtime_to_row(item))
        rows.sort(key=lambda x: (x["mode"], x["portfolio_id"], x["strategy_id"]))
        return rows

    def _runtime_to_row(self, runtime: StrategyRuntime) -> dict[str, Any]:
        cash = 0.0
        mv = 0.0
        total = 0.0
        positions = 0
        orders = 0
        try:
            asset = db.get_asset(runtime.portfolio_id)
            if asset is not None:
                cash = float(asset.cash)
                mv = float(asset.market_value)
                total = float(asset.total)
        except Exception:
            pass
        try:
            pos_df = db.get_positions(runtime.portfolio_id)
            positions = pos_df.height
        except Exception:
            positions = 0
        try:
            ord_df = db.get_orders(runtime.portfolio_id)
            orders = ord_df.height
        except Exception:
            orders = 0
        return {
            "runtime_id": runtime.runtime_id,
            "mode": runtime.mode,
            "portfolio_id": runtime.portfolio_id,
            "strategy_name": runtime.strategy_name,
            "strategy_id": runtime.strategy_id,
            "status": runtime.status,
            "principal": runtime.principal,
            "cash": cash,
            "market_value": mv,
            "total": total,
            "positions": positions,
            "orders": orders,
            "updated_at": runtime.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
            "error": runtime.error,
            "can_stop": runtime.status in {"running", "stopping"},
            "can_start": runtime.status in {"stopped", "failed"},
        }

    def _resolve_backtest_run(self, portfolio_id: str) -> BacktestRun:
        run = self.get_backtest_run(portfolio_id)
        if run is not None:
            return run
        portfolio = db.get_portfolio(portfolio_id)
        if portfolio is None:
            raise RuntimeError(f"回测记录不存在: {portfolio_id}")
        strategy_name = portfolio.name or ""
        if not strategy_name:
            raise RuntimeError("无法识别回测策略名")
        strategies = strategy_loader.load_from_cache()
        strategy_cls = strategies.get(strategy_name)
        config = dict(getattr(strategy_cls, "PARAMS", {})) if strategy_cls else {}
        return BacktestRun(
            runtime_id=f"backtest:{portfolio_id}",
            portfolio_id=portfolio_id,
            strategy_name=strategy_name,
            config=config,
            interval="1m",
            start_date=str(portfolio.start),
            end_date=str(portfolio.end),
            initial_cash=0,
            status="finished",
        )

    def _start_strategy_runtime(
        self,
        mode: str,
        strategy_name: str,
        config: dict[str, Any],
        broker: Any,
        portfolio_id: str,
        account_kind: str,
        interval: str,
        market_data: Any,
        principal: float = 0.0,
        runtime_id: str | None = None,
        strategy_id: str | None = None,
        persist: bool = True,
    ) -> StrategyRuntime:
        strategy_id = strategy_id or f"{strategy_name}-{uuid.uuid4().hex[:8]}"
        runtime_id = runtime_id or f"{mode}:{portfolio_id}:{strategy_id}"
        symbols = self._extract_symbols(config)
        stop_event = threading.Event()
        runtime = StrategyRuntime(
            runtime_id=runtime_id,
            mode=mode,
            strategy_name=strategy_name,
            strategy_id=strategy_id,
            portfolio_id=portfolio_id,
            account_kind=account_kind,
            status="running",
            config=config,
            symbols=symbols,
            principal=principal,
            stop_event=stop_event,
            broker=broker,
        )
        runtime.thread = threading.Thread(
            target=self._run_strategy_loop,
            args=(runtime, interval, market_data),
            daemon=True,
            name=f"strategy-{strategy_id}",
        )
        with self._lock:
            self._strategy_runtimes[runtime_id] = runtime
            if persist:
                self._runtime_specs[runtime_id] = {
                    "runtime_id": runtime_id,
                    "mode": mode,
                    "strategy_name": strategy_name,
                    "strategy_id": strategy_id,
                    "portfolio_id": portfolio_id,
                    "account_kind": account_kind,
                    "status": "running",
                    "config": config,
                    "symbols": symbols,
                    "principal": principal,
                    "interval": interval,
                }
                self._save_specs()
        runtime.thread.start()
        return runtime

    def _run_strategy_loop(self, runtime: StrategyRuntime, interval: str, market_data: Any) -> None:
        asyncio.run(self._strategy_loop(runtime, interval, market_data))

    async def _strategy_loop(self, runtime: StrategyRuntime, interval: str, market_data: Any) -> None:
        strategies = strategy_loader.load_from_cache()
        strategy_cls = strategies.get(runtime.strategy_name)
        if strategy_cls is None:
            runtime.status = "failed"
            runtime.error = f"策略不存在: {runtime.strategy_name}"
            runtime.updated_at = datetime.datetime.now()
            return
        broker = runtime.broker
        if runtime.mode == "live":
            broker = StrategyBrokerProxy(runtime.broker, runtime.strategy_id)
        strategy = strategy_cls(broker, runtime.config)
        frame = FrameType.MIN1 if interval == "1m" else FrameType.DAY
        strategy.interval = frame.value
        try:
            await strategy.init()
            await strategy.on_start(datetime.datetime.now())
            while runtime.stop_event is not None and not runtime.stop_event.is_set():
                now = datetime.datetime.now()
                if hasattr(runtime.broker, "set_clock"):
                    runtime.broker.set_clock(now)
                quotes = self._build_quotes(runtime.symbols, market_data)
                await strategy.on_bar(now, quotes, frame)
                runtime.updated_at = datetime.datetime.now()
                await asyncio.sleep(2)
            runtime.status = "stopped"
        except Exception as exc:
            runtime.status = "failed"
            runtime.error = str(exc)
            logger.exception("strategy runtime failed: {}", exc)
        finally:
            runtime.updated_at = datetime.datetime.now()
            with self._lock:
                spec = self._runtime_specs.get(runtime.runtime_id)
                if spec is not None:
                    spec["status"] = runtime.status
                    self._save_specs()
            try:
                await strategy.on_stop(datetime.datetime.now())
            except Exception:
                pass

    def _build_quotes(self, symbols: list[str], market_data: Any) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        if not symbols:
            symbols = ["000001.SZ"]
        if market_data is None:
            return result
        snaps = market_data.snapshot(symbols)
        for symbol in symbols:
            snap = snaps.get(symbol)
            if snap is None:
                continue
            result[symbol] = {
                "lastPrice": float(snap.price or 0),
                "open": float(snap.open or 0),
                "high": float(snap.high or 0),
                "low": float(snap.low or 0),
                "volume": float(snap.volume or 0),
                "amount": float(snap.amount or 0),
            }
        return result

    def _extract_symbols(self, config: dict[str, Any]) -> list[str]:
        for key in ("symbol", "asset", "security"):
            value = config.get(key)
            if isinstance(value, str) and value:
                return [value]
        for key in ("symbols", "assets", "securities"):
            value = config.get(key)
            if isinstance(value, list):
                symbols = [str(item) for item in value if item]
                if symbols:
                    return symbols
        return []

    def _state_file(self) -> Path:
        home = Path(str(cfg.home))
        home.mkdir(parents=True, exist_ok=True)
        return home / "strategy_runtimes.json"

    def _save_specs(self) -> None:
        file_path = self._state_file()
        payload = {"strategy_runtimes": list(self._runtime_specs.values())}
        file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_specs(self) -> None:
        file_path = self._state_file()
        if not file_path.exists():
            return
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
            items = payload.get("strategy_runtimes") or []
            mapping: dict[str, dict[str, Any]] = {}
            for item in items:
                runtime_id = str(item.get("runtime_id") or "")
                if runtime_id:
                    mapping[runtime_id] = item
            self._runtime_specs = mapping
        except Exception:
            self._runtime_specs = {}

    def _restore_persisted_runtimes(self) -> None:
        specs = list(self._runtime_specs.values())
        for spec in specs:
            if str(spec.get("status") or "").lower() != "running":
                continue
            try:
                self._start_from_spec(spec)
            except Exception as exc:
                spec["status"] = "failed"
                spec["error"] = str(exc)
                self._save_specs()

    def _start_from_spec(self, spec: dict[str, Any]) -> StrategyRuntime:
        if self._registry is None:
            raise RuntimeError("registry 未初始化")
        account_kind = str(spec.get("account_kind") or "")
        portfolio_id = str(spec.get("portfolio_id") or "")
        broker = None
        if account_kind == "gateway":
            broker = self._gateway_broker
        if broker is None:
            broker = self._registry.get(account_kind, portfolio_id)
        if broker is None:
            raise RuntimeError(f"账户不存在: {account_kind}:{portfolio_id}")
        return self._start_strategy_runtime(
            mode=str(spec.get("mode") or ""),
            strategy_name=str(spec.get("strategy_name") or ""),
            config=dict(spec.get("config") or {}),
            broker=broker,
            portfolio_id=portfolio_id,
            account_kind=account_kind,
            interval=str(spec.get("interval") or "1m"),
            market_data=self._market_data,
            principal=float(spec.get("principal") or 0),
            runtime_id=str(spec.get("runtime_id") or ""),
            strategy_id=str(spec.get("strategy_id") or ""),
            persist=False,
        )


strategy_runtime_manager = StrategyRuntimeManager()

