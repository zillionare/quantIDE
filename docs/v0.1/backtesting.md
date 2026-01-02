# Backtesting / Simulation 设计

本设计用于实现回测（Backtesting）与仿真（Simulation）两种模式。

两者在架构上应尽可能一致，核心差异仅在于：

1. 回测：行情数据来自历史数据（当前只到日线，未来支持分钟线）
2. 仿真：行情数据来自实时推送（当前可 mock，每秒给出最新价）

## 目标

1. 提供 `BacktestBroker` 与 `SimulationBroker`，实现统一的 `Broker` 接口
2. 所有订单进入订单队列，等待撮合
3. Broker 订阅“推送行情数据的频道”，接收行情事件并对订单队列撮合

## 技术栈与既有实现对齐

1. 数据库存取统一使用 `sqlitedb`（此前 `tradedb` 已重命名为 `sqlitedb`）
2. v0.1 行情频道优先使用进程内消息中心 `msg_hub`
3. Broker 抽象接口以 `pyqmt/service/base_broker.py` 为准，等待/唤醒机制以 `pyqmt/service/abstract_broker.py` 为准

## 为什么要丰富 AbstractBroker

应当将 `AbstractBroker` 丰富为三类 broker 的共用基类，以降低重复实现：

1. 订单参数校验（整手、T+1、涨跌停限制等）
2. 订单入库与状态更新、成交入库
3. 统一的等待/唤醒模型（用于“下单 -> 等待成交/超时”）
4. 通用的撮合引擎驱动方式（仅 backtest/simulation 启用；qmt 侧由柜台撮合）

因此：

- `QMTBroker`：主要重写“委托下发/撤单/查询”等与柜台交互相关的部分，通用校验与持久化由基类复用
- `BacktestBroker`：最少的“行情驱动 + 撮合规则”差异化实现
- `SimulationBroker`：最少的“实时行情驱动 + 撮合规则”差异化实现

## 数据来源

### 回测日线（现有）

1. 历史日线通过 `DailyBars` 获取（本地 parquet + 自动补齐）
2. 涨跌停价格通过 tushare `stk_limit` 获取，已经合并进日线扩展字段（`up_limit/down_limit`）

### 回测分钟线（未来）

1. 分钟线历史行情优先通过 `xtquant` 获取（例如 `download_history_data2` + `get_market_data_ex`）
2. 分钟线的涨跌停价仍可按“当日维度”从日线 `stk_limit` 拼接（同一交易日共享一组 `up_limit/down_limit`）

### 仿真实时（v0.1 可 mock）

1. 使用定时器/任务每秒发布 `QuoteEvent(lastPrice)` 到行情频道
2. 后续可替换为真实 `xtquant` 推送

## 行情频道（v0.1：msg_hub）

统一用事件驱动撮合，事件类型至少包括：

- `BarEvent`：日线/分钟线 bar，含 `open/high/low/close/volume/amount/up_limit/down_limit`
- `QuoteEvent`：秒级最新价，含 `lastPrice`（建议带上当日 `up_limit/down_limit` 以支持涨跌停限制）

频道命名建议：

- `md:bar:1d`
- `md:bar:1m`
- `md:quote:1s`

## 账户与交易规则

### 手续费

手续费按成交金额固定收取 `0.1%`，可配置：

- `fee = amount * commission_rate`
- `commission_rate` 默认 `0.001`

### T+1

实现 A 股 T+1：

- 当日买入的股票当日不得卖出
- 可用数量（`avail`）用于表达“可卖数量”
- 买入成交当日仅增加 `shares`，不增加 `avail`；下一交易日将 `avail` 刷新为 `shares`

### 整手规则

- 买入：必须按 100 股整数倍
- 卖出：若清仓则不限制；否则必须按 100 股整数倍

### 涨跌停限制

- 不允许涨停板上买入
- 不允许跌停板上卖出

具体规则按行情粒度不同而不同（见撮合规则部分）。

## 撮合规则

### 仅日线（当前回测默认）

1. 成交价支持配置为 `open` 或 `close`
2. 若价格 match，则按全成算
3. 涨跌停限制（仅日线下的放宽规则）：
   - 买入：若当日出现过低于涨停价的成交区间（`low < up_limit`），允许买入；否则拒绝
   - 卖出：若当日出现过高于跌停价的成交区间（`high > down_limit`），允许卖出；否则拒绝

### 分钟线（未来回测）

1. 撮合从“订单下达之时起”的下一分钟开始
2. 允许部分成交：
   - 若价格匹配，则允许按该分钟 bar 的成交量撮合部分
3. 当天撮合窗口：
   - 从下一分钟开盘价开始，直到当天结束止
4. 涨跌停限制（严格规则）：
   - 不允许在 `up_limit` 成交的买入
   - 不允许在 `down_limit` 成交的卖出

### 仿真秒级（v0.1 mock）

1. 每秒收到 `lastPrice` 后触发撮合
2. 允许部分成交（按“每秒成交量”或 mock 的成交量）
3. 涨跌停限制按严格规则执行

## 最小实现里程碑（v0.1）

1. 修复并统一 `sqlitedb` 导入路径，保证三类 broker 共用同一套持久化接口
2. 实现 `BacktestBroker`：
   - 日线驱动（使用现有 `DailyBars` 数据）
   - 订单队列
   - 撮合引擎（按日线规则）
   - 手续费、T+1、整手、涨跌停限制
3. 实现 `SimulationBroker`：
   - mock 行情源（每秒 lastPrice）
   - 撮合引擎（按实时规则）
4. 将 `cfg.broker` 扩展为 `qmt/backtest/simulation`，应用启动时注入对应 broker
