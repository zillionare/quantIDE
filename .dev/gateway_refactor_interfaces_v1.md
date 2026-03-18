# 主体工程重构接口规格（v1）

## 1. 文档定位

本文件给出接口级规划草案，用于后续代码改造前审阅。  
当前不落地实现，仅定义核心抽象、事件语义、运行模式装配与兼容策略。

## 2. 现状映射（基线）

当前主体工程已经具备可复用抽象，主要基线如下：

1. 交易接口基线：`Broker` 抽象  
   - 参考 [base_broker.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/base_broker.py)
2. Broker 扩展基线：`AbstractBroker` + `QMTBroker/SimulationBroker/BacktestBroker`  
   - 参考 [abstract_broker.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/abstract_broker.py)  
   - 参考 [qmt_broker.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/qmt_broker.py)  
   - 参考 [sim_broker.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/sim_broker.py)  
   - 参考 [backtest_broker.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/backtest_broker.py)
3. 运行时基线：`BacktestRunner`  
   - 参考 [runner.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/runner.py)
4. 行情接口基线：`BarsFeed` 协议与 `LiveQuote` 服务  
   - 参考 [datafeed.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/datafeed.py)  
   - 参考 [livequote.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/livequote.py)

## 3. 目标接口分层

主体代码仍放在 `pyqmt/` 目录下，建议新增以下子层（命名可微调）：

1. `pyqmt/core/domain/`：事件与领域实体（订单、成交、行情、时钟）
2. `pyqmt/core/ports/`：外部依赖抽象（行情、交易、时钟、存储）
3. `pyqmt/core/runtime/`：策略运行时与模式装配
4. `pyqmt/adapters/`：内置适配器（paper/backtest）；gateway 适配器可选内置或扩展包

## 4. Port 抽象规格（草案）

### 4.1 MarketDataPort

职责：向运行时提供统一实时/回放事件流，不暴露底层来源（gateway、本地、文件）。

建议最小接口：

1. `start() -> None`
2. `stop() -> None`
3. `subscribe(symbols: list[str]) -> None`
4. `unsubscribe(symbols: list[str]) -> None`
5. `stream() -> AsyncIterator[MarketEvent]`
6. `snapshot(symbols: list[str]) -> dict[str, QuoteSnapshot]`

### 4.2 BrokerPort

职责：统一交易语义，对齐现有 `Broker` 关键能力。

建议最小接口：

1. `submit(order_request) -> OrderAck`
2. `cancel(order_id: str) -> CancelAck`
3. `cancel_all(side: str | None = None) -> int`
4. `query_positions() -> list[PositionView]`
5. `query_assets() -> AssetView`
6. `query_orders(status: str | None = None) -> list[OrderView]`
7. `query_trades(order_id: str | None = None) -> list[TradeView]`

### 4.3 ClockPort

职责：统一实盘时钟与回测时钟。

建议最小接口：

1. `now() -> datetime`
2. `set_now(tm: datetime) -> None`（仅回测实现）
3. `iter_frames(start, end, frame_type) -> Iterable[datetime | date]`

### 4.4 StoragePort

职责：统一持久化，不把 DB 细节泄漏到运行时。

建议最小接口：

1. `save_order(order) -> None`
2. `save_trade(trade) -> None`
3. `save_position(position) -> None`
4. `save_asset(asset) -> None`
5. `save_metric(metric) -> None`

## 5. 统一事件模型（草案）

### 5.1 MarketEvent

字段建议：

1. `event_id`: str
2. `source`: str（gateway/backtest/replay）
3. `event_type`: Literal["tick", "bar", "status"]
4. `symbol`: str
5. `ts`: datetime
6. `payload`: dict

### 5.2 OrderEvent / TradeEvent

字段建议：

1. `order_id`
2. `portfolio_id`
3. `status`
4. `filled_qty`
5. `filled_price`
6. `reason`（拒单/错误说明）
7. `ts`

### 5.3 ErrorEvent

字段建议：

1. `code`（稳定错误码）
2. `category`（network/broker/risk/data）
3. `message`
4. `retryable`（是否可重试）

## 6. 运行模式装配规范

### 6.1 Live

1. MarketDataPort = GatewayMarketDataAdapter  
2. BrokerPort = GatewayBrokerAdapter  
3. ClockPort = SystemClock

### 6.2 Paper

1. MarketDataPort = GatewayMarketDataAdapter  
2. BrokerPort = PaperBrokerAdapter  
3. ClockPort = SystemClock

### 6.3 Backtest

1. MarketDataPort = BacktestDataAdapter  
2. BrokerPort = PaperBrokerAdapter  
3. ClockPort = BacktestClock

## 7. 兼容策略（与现有 Broker 保持一致）

为确保需求语义一致，接口适配应遵循：

1. 保留现有买卖语义：`buy/sell/buy_amount/sell_amount/buy_percent/sell_percent/trade_target_pct`
2. 保留超时等待语义：兼容现有 `wait/awake` 模式
3. 保留订单主键语义：`qtoid` 在内部继续作为策略侧关联键
4. 保留持仓与资产快照语义：回测/仿真/实盘行为对齐

## 8. Adapter 插件约束

为支持新增或替换 adapter，约束如下：

1. 核心层禁止直接 import 具体 adapter 实现类
2. adapter 通过注册表注入（按 `name + capability`）
3. 每个 adapter 必须声明 capability：
   - `market_data`
   - `broker`
   - `clock`
   - `storage`
4. adapter 错误必须映射到统一错误码体系

## 9. 配置规格（草案）

建议新增统一运行配置块：

1. `runtime.mode`: live/paper/backtest
2. `runtime.market_adapter`: gateway/backtest/file_replay
3. `runtime.broker_adapter`: gateway/paper
4. `runtime.clock_adapter`: system/backtest
5. `runtime.storage_adapter`: sqlite/parquet

## 10. 审阅关注点

1. Port 最小集合是否覆盖当前业务
2. 事件字段是否满足策略与风控需求
3. 对现有 `Broker` 语义是否保持兼容
4. 是否满足“主体代码保留在 `pyqmt/`”约束
