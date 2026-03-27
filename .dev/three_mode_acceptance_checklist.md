# 三模式验收清单

## 1. 文档定位

本文档用于发布前开发验收，不属于公开发布文档。

约束来源：`.dev/release_architecture_decision_v1.md`

本清单只回答三个问题：

1. `backtest / paper / live` 三种模式分别需要满足什么。
2. 当前代码已经有哪些自动化证据。
3. 还缺哪些最小验证动作，才能认为达到发布前可接受状态。

## 2. 决议要求映射

根据决议 `D5`，三模式统一要求如下：

1. 同一份策略代码跨模式零修改运行。
2. 策略只依赖统一 broker、market data、runtime context。
3. `backtest` 使用历史数据回放 + 本地仿真撮合。
4. `paper` 使用实时行情 + 本地仿真撮合。
5. `live` 使用远程 gateway 行情 + 远程 gateway 交易。

根据决议 `D6`，`live` 不再接受主体本地直连 QMT 作为正式路径。

根据决议 `D3`，运行时配置应由数据库中的 `AppState` 主导。

根据决议 `D10`，三模式的正式入口不以 CLI 为发布接口。

## 3. 当前结论

截至当前工作区状态：

1. `backtest`：已具备策略级自动验收证据。
2. `paper`：已具备 runtime 装配 + broker 行为 + 生命周期级自动验收证据。
3. `live`：已具备 gateway client / broker adapter / port wrapper 级自动验收证据，但仍缺少真实 gateway 集成环境下的最小端到端验收。

换句话说：

1. `backtest` 可以认为“自动化通过”。
2. `paper` 可以认为“自动化通过”。
3. `live` 目前只能认为“核心边界已连通，尚未完成最终验收”。

## 4. 模式清单

### 4.1 backtest

#### 验收目标

1. `RuntimeBootstrap` 不依赖实时行情与 gateway 也能进入回测路径。
2. 策略在历史数据回放中可以正常触发买卖。
3. 回测订单、成交、资产变化能落到主体统一数据模型。
4. 策略代码不需要为回测模式做条件分支。

#### 当前自动化证据

1. `tests/strategies/example/test_dual_ma.py`
   - 使用静态日线数据驱动 `BacktestRunner`
   - 验证 `DualMAStrategy` 在同一策略代码下产生买卖两笔成交
   - 验证订单与成交写入 SQLite
2. 最近定向验证通过：
   - `tests/strategies/example/test_dual_ma.py`

#### 结论

1. `backtest` 的核心业务链路当前已被自动化覆盖。
2. 该模式已达到发布前“可接受”状态。

#### 剩余建议

1. 若要增强发布信心，可额外补一条多标的或多账户回测用例。
2. 这不是当前阻塞项。

### 4.2 paper

#### 验收目标

1. `RuntimeBootstrap(mode="paper")` 能装配实时行情适配器与仿真 broker。
2. `paper` 使用实时行情输入，但撮合和持仓变化在主体本地完成。
3. 支持与正式 broker 抽象一致的高阶交易语义：
   - `buy`
   - `sell`
   - `buy_amount`
   - `sell_amount`
   - `buy_percent`
   - `sell_percent`
   - `trade_target_pct`
   - `cancel_order`
   - `cancel_all_orders`
4. 支持持久化恢复、日终处理、生命周期指标计算。
5. 策略代码不因 `paper` 模式而修改。

#### 当前自动化证据

1. `tests/core/test_runtime_modes.py`
   - 验证 `RuntimeBootstrap(mode="paper")`
   - 验证 gateway 市场数据适配器可被 mock 并注入仿真账户句柄
2. `tests/service/test_sim_broker_paper.py`
   - 覆盖买卖、撤单、金额/比例/目标仓位下单、涨跌停、无成交量、T+1、最小手续费等行为
3. `tests/service/test_sim_broker_paper_lifecycle.py`
   - 覆盖持久化恢复、并发账户共享行情、日终撤单、生命周期 metrics
4. 最近定向验证通过：
   - `tests/core/test_runtime_modes.py`
   - `tests/service/test_sim_broker_paper.py`
   - `tests/service/test_sim_broker_paper_lifecycle.py`

#### 结论

1. `paper` 的 runtime 装配和本地仿真交易主链路已自动化通过。
2. 旧的 Redis/livequote 专用测试路径已经可以视为退役。
3. 该模式已达到发布前“可接受”状态。

#### 剩余建议

1. 后续若要增强信心，可补一条“策略经 `RuntimeBootstrap(mode=paper)` 运行”的端到端策略用例。
2. 这属于增强项，不是当前阻塞项。

### 4.3 live

#### 验收目标

1. 主体不直连 `xtquant/QMT`。
2. `live` 模式通过 `GatewayClient` 获取行情与交易能力。
3. gateway broker adapter 对正式 broker 抽象的高阶语义映射正确。
4. UI/运行时能把 gateway 账户作为正式账户句柄注册到主体运行时。
5. 至少存在一条真实 gateway 环境下的最小集成验收路径。

#### 当前自动化证据

1. `tests/core/test_gateway_client.py`
   - 验证 gateway HTTP/HTTPS 到 WS/WSS URL 组装正确
2. `tests/core/test_gateway_broker_adapter.py`
   - 验证买卖、金额下单、目标仓位下单、查询资产、批量撤单等映射
3. `tests/core/test_port_broker.py`
   - 验证 gateway broker adapter 能接入正式 port 层
4. `tests/config/test_runtime.py`
   - 验证 runtime 配置优先从数据库装配，包括 gateway 配置和模式判定
5. `tests/core/test_runtime_modes.py`
   - 至少验证了 `paper` 模式下的 gateway 行情装配能力，说明 runtime 装配点已支持 gateway market adapter

#### 当前缺口

1. 还没有一条使用真实或近真实 gateway 服务的主体侧端到端自动验收。
2. 还没有证明主体在 `live` 模式下：
   - 能发现远程账户
   - 能拉取远程资产/持仓/订单/成交
   - 能在真实 WS 行情下驱动运行时行为
3. 当前证据更接近“组件级通过”，而不是“模式级通过”。

#### 结论

1. `live` 模式的架构方向已正确。
2. `live` 模式的协议边界和适配器语义已有自动化覆盖。
3. 但它尚未达到发布前“最终验收完成”状态。

#### 最小补验建议

建议至少完成下面 1 条再认为 `live` 可验收：

1. 主体连接一个可控的 gateway stub 或测试服务，完成以下链路：
   - 读取 DB 配置
   - `RuntimeBootstrap(mode="live")`
   - 加载 gateway 账户句柄
   - 拉取一次资产/持仓
   - 发起一次下单请求
   - 收到一条行情或状态更新

如果短期不搭 stub，至少做下面人工集成验收：

1. Windows + QMT + `qmt-gateway` 启动成功。
2. 主体通过 DB 配置连接 gateway。
3. 主体 UI 能看到远程账户。
4. 能完成一次买单、一次撤单、一次资产/持仓刷新。

## 5. 推荐验收命令

当前可以作为回归基线的命令集：

### backtest

`pytest tests/strategies/example/test_dual_ma.py`

### paper

`pytest tests/core/test_runtime_modes.py tests/service/test_sim_broker_paper.py tests/service/test_sim_broker_paper_lifecycle.py`

### live 组件级

`pytest tests/core/test_gateway_client.py tests/core/test_gateway_broker_adapter.py tests/core/test_port_broker.py tests/config/test_runtime.py`

最近一次定向回归结果：24 passed。

## 6. 发布前判定建议

建议用下面分级判断：

1. `backtest`：通过
2. `paper`：通过
3. `live`：条件通过，待补最小集成验收

如果发布目标是“主体发布态架构已基本收敛”，当前已接近可接受。

如果发布目标是“对外宣称三模式均完成发布前验收”，当前还不能这么写，阻塞点只剩 `live` 的最小集成验证。