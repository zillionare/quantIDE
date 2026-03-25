# Phase 2 - broker 抽象收敛草案（v1）

## 1. 文档定位

本文件是新的 Phase 2 当前执行依据。

目标：

1. 定义主体内部唯一正式 broker 抽象的目标形状。
2. 明确 `service/base_broker.py` 与 `core/ports/broker.py` 的收敛方向。
3. 给 bridge / wrapper 提供退役顺序。

说明：

1. 本文件处理的是主体内部抽象收敛，不涉及 gateway 对外协议设计。
2. 本文件优先级低于主决议，但高于旧的接口草案和桥接现状。

## 2. 当前问题

主体内部目前同时存在两套交易抽象：

1. 旧接口：`pyqmt/service/base_broker.py`
2. 新接口：`pyqmt/core/ports/broker.py`

当前问题不是“名字不同”这么简单，而是两者承担了不同层级的语义：

1. `base_broker.py` 持有高阶业务语义：
   - `buy`
   - `sell`
   - `buy_amount`
   - `sell_amount`
   - `buy_percent`
   - `sell_percent`
   - `trade_target_pct`
   - `TradeResult`
2. `ports/broker.py` 持有更统一的端口抽象：
   - `submit(OrderRequest)`
   - `cancel`
   - `cancel_all`
   - `query_*`

结果是：

1. 运行时里不得不保留 `LegacyBrokerPortAdapter`。
2. gateway 路径里不得不保留 `GatewayBrokerWrapper`。
3. 策略运行时和 UI 对 broker 的期待并不完全一致。

## 3. 收敛原则

### P1. 主体内部只能有一套正式 broker 抽象

发布前必须收敛到单一正式接口。

### P2. 保留高阶交易语义，而不是只保留底层 submit/query

策略框架需要的是稳定业务语义，而不是最低层的订单请求拼装接口。

因此收敛方向不是：

1. 删除高阶语义，只剩 `submit`

而是：

1. 以 port 为正式接口承载层
2. 同时把高阶语义吸收到正式接口中

### P3. `submit(OrderRequest)` 继续保留，但降级为通用入口

它适合作为：

1. adapter 通用实现入口
2. bridge 内部复用入口
3. 运行时统一下单入口

但不应成为策略层唯一可见能力。

### P4. 查询接口继续采用视图模型

`PositionView`、`AssetView`、`OrderView`、`TradeView` 方向是对的。

这部分应保留在 port 层，不再依赖旧 `sqlite` 实体直接暴露给上层。

## 4. 目标接口形状

建议把 `pyqmt/core/ports/broker.py` 收敛为唯一正式接口，并吸收以下语义。

### 4.1 保留的数据结构

保留：

1. `OrderRequest`
2. `OrderStyle`
3. `AssetView`
4. `PositionView`
5. `OrderView`
6. `TradeView`
7. `OrderAck`
8. `CancelAck`

### 4.2 新增或吸收的数据结构

建议新增 `ExecutionResult`，替代旧的 `TradeResult` 作为正式返回类型。

建议字段：

1. `order_id: str | None`
2. `trades: list[TradeView]`
3. `status: str = "submitted"`
4. `message: str = ""`

理由：

1. 避免正式接口继续依赖 `service/base_broker.py`。
2. 避免正式端口返回 `sqlite.Trade` 这类旧模型。
3. 与 `OrderAck` 保持概念边界：
   - `OrderAck` 偏底层适配器响应
   - `ExecutionResult` 偏策略/UI 业务语义

### 4.3 目标 BrokerPort 草案

建议正式接口同时支持高阶方法和通用方法：

1. `submit(request: OrderRequest) -> OrderAck`
2. `buy(...) -> ExecutionResult`
3. `sell(...) -> ExecutionResult`
4. `buy_amount(...) -> ExecutionResult`
5. `sell_amount(...) -> ExecutionResult`
6. `buy_percent(...) -> ExecutionResult`
7. `sell_percent(...) -> ExecutionResult`
8. `trade_target_pct(...) -> ExecutionResult`
9. `cancel(order_id: str) -> CancelAck`
10. `cancel_all(side: OrderSide | None = None) -> int`
11. `query_positions() -> list[PositionView]`
12. `query_assets() -> AssetView | None`
13. `query_orders(status: str | None = None) -> list[OrderView]`
14. `query_trades(order_id: str | None = None) -> list[TradeView]`
15. `record(key: str, value: float, dt: datetime.datetime | None = None, extra: dict | None = None) -> None`

说明：

1. `record` 虽然不是交易本身，但已被旧 broker 语义和策略运行链路使用，建议继续保留。
2. `cash` 和 `positions` 这种属性式访问不再作为正式端口要求。
3. 若上层确实需要 `cash`，应由 `query_assets()` 推导。

## 5. 新旧接口映射

### 5.1 从旧接口吸收的内容

直接吸收进正式 port：

1. `buy`
2. `sell`
3. `buy_amount`
4. `sell_amount`
5. `buy_percent`
6. `sell_percent`
7. `trade_target_pct`
8. `record`
9. `cancel_order` -> 统一命名为 `cancel`
10. `cancel_all_orders` -> 统一命名为 `cancel_all`

### 5.2 从新接口保留的内容

直接保留：

1. `submit`
2. `query_positions`
3. `query_assets`
4. `query_orders`
5. `query_trades`
6. 视图类与请求类

### 5.3 不保留为正式端口的内容

不再作为正式接口要求：

1. `positions` 属性
2. `cash` 属性
3. 直接返回 `sqlite.Position` / `sqlite.Trade`
4. 旧 `TradeResult` 类型

这些可以在兼容层短期保留，但不进入最终正式接口。

## 6. 对现有代码的影响

### 6.1 `LegacyBrokerPortAdapter`

当前角色：

1. 旧 broker -> 新 port 的桥接器

后续命运：

1. 短期保留
2. 中期退化为兼容适配层，仅服务未迁移代码
3. 最终当旧 `Broker` 不再是正式核心后，进入删除名单

### 6.2 `GatewayBrokerWrapper`

当前角色：

1. 新 gateway adapter -> 旧 broker 的反向包装器

后续命运：

1. 优先退役
2. 一旦 UI 与 runtime 都依赖正式 port，它应最先删除

理由：

1. 反向兼容层复杂度最高
2. 它说明主体仍在被旧接口拖拽

### 6.3 `strategy_runtime.py`

当前观察：

1. `StrategyBrokerProxy` 当前采用双轨分发：
   - 如果 broker 有 `submit(...)`，就把 `buy/sell/buy_amount/...` 重新翻译为 `OrderRequest`
   - 否则直接调用旧 `Broker` 高阶方法
2. 这意味着策略层表面上在调用高阶语义，实际却要在运行时猜测底层对象是哪一套抽象
3. 这正是当前收敛工作的核心症状

这说明收敛方向是正确的：

1. 不是删除高阶方法
2. 而是把高阶方法正式化到 port 中

### 6.4 UI 层

UI 当前很多逻辑仍假设 broker 是旧接口对象。

迁移方向：

1. UI 最终应依赖正式 port 或运行时 façade
2. 不应继续依赖 `GatewayBrokerWrapper` 这类反向包装器

### 6.5 运行时装配层

`core/runtime/modes.py` 当前在启动时同时固化了两套注册路径：

1. `_register_broker_adapters(...)` 把旧 broker 通过 `LegacyBrokerPortAdapter` 注册到 `AdapterRegistry`
2. `_register_gateway_broker_adapter(...)` 又把 `GatewayBrokerAdapter` 反向包装成 `GatewayBrokerWrapper` 注册回 `BrokerRegistry`

这说明双抽象并存不是局部问题，而是已经被启动装配层显式放大：

1. 新端口为了兼容旧实现，要保留正向桥接
2. gateway 为了兼容旧 UI，要保留反向包装
3. 如果不改调用面，这两个兼容层都会长期存在

### 6.6 旧 Web API

`pyqmt/web/apis/broker.py` 仍然直接暴露以下高阶 broker 语义：

1. `buy`
2. `sell`
3. `buy_percent`
4. `sell_percent`
5. `buy_amount`
6. `sell_amount`

而且这层当前仍按旧 broker 约定工作：

1. 直接从 request scope 取 broker 对象
2. 直接调用高阶方法
3. 直接使用 backtest 特殊参数校验逻辑

因此，Phase 2 的迁移对象不能只盯着策略 runtime，还必须覆盖这层旧 API 或其替代 façade。

### 6.7 非阻塞区域

并非所有运行时代码都直接依赖旧 broker 抽象。

例如 `StrategyRuntimeManager._runtime_to_row(...)` 当前主要从数据库读取资产、持仓和订单汇总，而不是通过 broker 属性读取。

这意味着：

1. Phase 2 的高风险面主要集中在“下单调用面”
2. 运行状态展示面暂时不是主要阻塞项
3. 可以优先改 broker 协议与代理层，而不必同步重写所有 runtime 展示代码

## 7. 当前依赖面清单

基于当前代码，正式迁移时至少要覆盖以下四类调用面。

### 7.1 策略调用面

文件：

1. `pyqmt/service/strategy_runtime.py`

现状：

1. `StrategyBrokerProxy` 对策略暴露的是高阶方法
2. 这些高阶方法内部却依赖 `hasattr(self._broker, "submit")` 做分支

迁移要求：

1. 正式 port 必须直接提供高阶方法
2. `StrategyBrokerProxy` 不应再自己做“高阶语义 -> OrderRequest”的二次翻译

### 7.2 启动装配面

文件：

1. `pyqmt/core/runtime/modes.py`

现状：

1. 同时注册 `LegacyBrokerPortAdapter`
2. 同时注册 `GatewayBrokerWrapper`

迁移要求：

1. 先让策略 runtime 与 UI 都能消费正式 port
2. 然后删除 `GatewayBrokerWrapper`
3. 最后才有条件删除 `LegacyBrokerPortAdapter`

### 7.3 gateway 实现面

文件：

1. `pyqmt/core/runtime/gateway_broker.py`

现状：

1. `GatewayBrokerAdapter` 实现的是低阶 `BrokerPort`
2. `GatewayBrokerWrapper` 再补出旧高阶 broker 语义

迁移要求：

1. 高阶语义应直接进入正式 port
2. gateway adapter 应直接实现正式 port 的完整能力
3. wrapper 不再承担“语义补全”职责

### 7.4 旧 Web API / UI 调用面

文件：

1. `pyqmt/web/apis/broker.py`

现状：

1. HTTP 路由仍然直接依赖旧 broker 高阶方法
2. 这也是 `GatewayBrokerWrapper` 仍被注册进 `BrokerRegistry` 的直接原因之一

迁移要求：

1. 这层要么改为依赖正式 port
2. 要么引入一个明确的 runtime façade，由 façade 持有正式 port 并暴露稳定 Web 语义

## 8. 推荐的实现顺序

当前已经可以把 Phase 2 从“原则讨论”推进到“接口重构”。

### Step 1

修改 `core/ports/broker.py`，把高阶交易语义正式纳入 `BrokerPort`：

1. 增加 `ExecutionResult`
2. 增加 `buy/sell/buy_amount/sell_amount/buy_percent/sell_percent/trade_target_pct`
3. 增加 `record`
4. 保留 `submit/query_*` 作为通用入口与查询入口

### Step 2

修改 `StrategyBrokerProxy`：

1. 去掉基于 `hasattr(..., "submit")` 的分支
2. 让 proxy 只负责注入 `strategy_id`
3. 不再负责把高阶语义翻译成 `OrderRequest`

### Step 3

调整 gateway 适配器：

1. 让 `GatewayBrokerAdapter` 直接实现扩展后的正式 port
2. 把目前位于 `GatewayBrokerWrapper` 的高阶方法迁回 adapter 或 façade
3. 将 wrapper 降为短期兼容层

### Step 4

调整 Web API 依赖：

1. 让 `pyqmt/web/apis/broker.py` 面向正式 port 或 runtime façade
2. 停止要求 request scope 中一定放旧 `Broker`

### Step 5

删除兼容层：

1. 先删 `GatewayBrokerWrapper`
2. 再删 `LegacyBrokerPortAdapter`

## 9. 退役顺序建议

### Step 1

先扩展 `core/ports/broker.py`：

1. 增加高阶交易语义
2. 增加正式返回类型 `ExecutionResult`

### Step 2

调整策略 runtime 与 UI 的依赖面：

1. 统一依赖正式 port
2. 不再要求旧 `Broker` 属性和旧返回值

### Step 3

将 `service/base_broker.py` 标记为兼容层：

1. 不再新增能力
2. 只做过渡

### Step 4

删除反向兼容层：

1. `GatewayBrokerWrapper`

### Step 5

删除正向兼容层：

1. `LegacyBrokerPortAdapter`

前提：

1. 所有正式调用面都已转向唯一 port

## 10. 当前 Phase 2 的阶段结论

当前 Phase 2 目前已经完成从“是否收敛”到“收敛到哪里、先改哪里”的确认。

已确认：

1. 目标不是保留低阶 port 并删除高阶语义，而是让高阶语义正式进入唯一 port
2. 当前最关键的依赖面是：
   - `strategy_runtime.py`
   - `core/runtime/modes.py`
   - `core/runtime/gateway_broker.py`
   - `web/apis/broker.py`
3. 当前最高优先级改造点不是 broker 实现类，而是调用面契约

因此，本阶段当前落点是四件事：

1. 确认正式接口草案
2. 明确返回类型收敛方案
3. 盘点关键调用面
4. 列出兼容层退役顺序

本文件已完成这四件事。

## 11. 当前验证状态

截至本轮，Phase 2 已完成两类验证：

1. 端口与适配层 focused tests：
   - `tests/core/test_gateway_broker_adapter.py`
   - `tests/core/test_port_broker.py`
   - `tests/service/test_strategy_runtime.py`
2. 示例策略真实回测验证：
   - `tests/strategies/example/test_dual_ma.py`

其中，`tests/strategies/example/test_dual_ma.py` 直接运行：

1. `BacktestRunner`
2. `BacktestBroker`
3. `pyqmt/strategies/example/dual_ma.py`

验证目标不是“mock 通过”，而是确认示例均线策略在真实回测链路中已经完成：

1. 一次金叉买入
2. 一次死叉卖出
3. 对应成交与订单已写入数据库

## 12. Phase 2 关闭结论

本 Phase 现可关闭。

关闭依据：

1. 正式 `BrokerPort` 已吸收高阶交易语义与 `ExecutionResult`。
2. 策略 proxy 已去掉基于 `submit(...)` 的运行时分支。
3. runtime 注册与动态创建账号路径现在统一暴露 `PortBackedBroker`，UI/运行时消费的是正式 port 背后的统一句柄，而不是旧 broker 抽象。
4. `GatewayBrokerWrapper` 已退出正式运行路径；它即使仍保留在代码中，也不再参与 runtime 注册。
5. `LegacyBrokerPortAdapter` 仍存在，但已退化为旧 broker 实现接入正式 port 的内部兼容实现，不再是 UI 或策略运行时直接依赖的抽象边界。
6. 示例均线策略 `pyqmt/strategies/example/dual_ma.py` 已通过真实回测路径验证。

因此，从发布态架构角度看，Phase 2 的目标已经达到：

1. 主体内部对外的正式交易抽象已经收敛到一套
2. UI 与 runtime 的正式路径已经切到这套抽象
3. 后续工作应进入 Phase 3，而不是继续在 Phase 2 扩展兼容层

## 13. 下一步任务

1. 进入 Phase 3，清点并下线主体中的本地 QMT / xtquant 正式路径。
2. 将仍残留在主体中的 QMT 相关模块分类为：删除、迁移、仅兼容保留。
3. 在 Phase 3 中继续压缩并最终删除不再需要的 QMT 兼容代码。
