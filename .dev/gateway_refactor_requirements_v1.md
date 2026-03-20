# 主体工程重构需求与规划（v1）

## 1. 文档目的

本文件用于定义主体工程后续重构需求与边界，目标是支持通过远程 qmt-gateway 实盘交易、接收实时推送、仿真交易、回测，并保持主体工程可独立发布且不依赖 qmt-gateway。

当前阶段仅进行需求与规划，不进入实现。

## 2. 背景与原则

### 2.1 当前背景

主体工程已经具备多 broker 形态（实盘、仿真、回测）的基础能力，qmt-gateway 已提供远程交易与行情能力。下一步需要把两者关系从“耦合式调用”升级为“可插拔适配”。

### 2.2 核心原则

1. 需求一致性优先于代码重写。
2. 重构时充分尊重主体工程既有设计，尤其是 2025 年 11 月以来沉淀的代码与需求语义。
3. 主体工程只依赖抽象接口，不依赖 qmt-gateway 实现细节。
4. 必须预留后续新增/替换其他 adapter 的能力，不限定单一网关实现。

## 3. 范围定义

### 3.1 In Scope

1. 定义统一的行情输入接口（实时推送/历史回放）。
2. 定义统一的交易执行接口（远程实盘/本地仿真）。
3. 统一策略运行时在实盘、仿真、回测三种模式下的装配方式。
4. 定义插件化 adapter 机制，支持新增券商或新网关。
5. 梳理发布边界，确保主体工程可独立打包发布。

### 3.2 Out of Scope（当前阶段）

1. 不实现具体 adapter 代码。
2. 不改动现有业务策略逻辑。
3. 不做 UI 大改。

## 4. 目标需求（必须满足）

### R1 远程实盘交易

主体工程可通过远程 qmt-gateway 下单、撤单、查询状态，且策略侧接口保持统一。

### R2 实时数据推送接入

主体工程可直接接收 qmt-gateway 推送实时行情事件，移除对 Redis 订阅路径的依赖。

### R3 自主仿真交易

主体工程可使用同一实时行情流驱动本地仿真撮合，复用策略运行时。

### R4 回测能力

主体工程可在回测数据源上运行同一套策略接口，保证行为语义一致。

### R5 独立发布

主体工程以独立包发布，qmt-gateway 作为可选 adapter，不作为硬依赖。

### R6 可替换 adapter

主体工程需支持新增或替换其他 adapter（如其他网关/券商），不要求修改核心策略引擎。

### R7 历史兼容

在需求一致前提下，优先兼容现有核心抽象（特别是 broker 与 runner 语义），减少迁移成本。

### R8 目录与发布边界

主体核心功能代码持续放在 `pyqmt/` 目录下演进；允许新增可选 adapter 包，但不得把主体核心迁出 `pyqmt/`。

## 5. 目标架构（规划）

采用 Ports & Adapters（六边形）架构：

1. Core Domain：订单、成交、持仓、行情事件、时钟事件等领域模型。
2. Core Runtime：策略生命周期、事件分发、风控与撮合编排。
3. Ports（抽象接口）：
   - MarketDataPort（订阅/反订阅/事件流）
   - BrokerPort（下单/撤单/查询）
   - ClockPort（实时时钟/回测时钟）
   - StoragePort（状态与结果落地）
4. Adapters（实现层）：
   - GatewayMarketDataAdapter（qmt-gateway 推送）
   - GatewayBrokerAdapter（qmt-gateway 交易）
   - PaperBrokerAdapter（本地仿真撮合）
   - BacktestDataAdapter（历史回放）

## 6. 运行模式统一

1. Live 模式：GatewayMarketDataAdapter + GatewayBrokerAdapter
2. Paper 模式：GatewayMarketDataAdapter + PaperBrokerAdapter
3. Backtest 模式：BacktestDataAdapter + PaperBrokerAdapter

策略层仅依赖统一事件与统一下单接口，不感知下游实现差异。

## 7. 非功能要求

1. 稳定性：推送断连可恢复，具备重连与状态续接策略。
2. 一致性：三种模式共享同一策略接口与订单语义。
3. 可测试性：核心运行时可在无 gateway 环境下完成单元测试。
4. 可演进性：新增 adapter 时，核心层改动应接近零。

## 8. 验收标准（需求级）

1. 同一策略可在 Live/Paper/Backtest 三模式切换运行。
2. 主体工程安装后不安装 qmt-gateway 也能运行回测与基础仿真测试。
3. 接入新 adapter 只需新增实现与注册配置，不修改核心策略接口。
4. Redis 订阅路径可被替换为 gateway 推送路径并保持功能等价。

## 9. 分阶段规划（仅规划）

### Phase 1：抽象收敛

提炼 Port 接口与统一事件模型，梳理现有 broker/runner 语义映射表。

### Phase 2：gateway 适配层

实现远程交易与行情推送 adapter，完成实盘链路联通。

### Phase 3：仿真与回测统一

统一 paper/backtest 运行时装配，保证策略接口零改动迁移。

### Phase 4：发布解耦

拆分核心包与 adapter 包，形成独立发布方案与可选安装项。

## 10. 风险与缓解

1. 语义漂移风险：通过“旧接口到新接口映射清单”逐项对齐。
2. 模式行为不一致：建立 Live/Paper/Backtest 的一致性回归用例。
3. adapter 锁死风险：强制通过 Port 接口注册，不允许核心层直连实现。

## 11. 下一步（待审阅后执行）

1. 产出接口级规格（Protocol/ABC 草案、事件 schema、错误码）。
2. 产出迁移清单（按模块和文件级）。
3. 按阶段执行改造并逐步回归验证。

## 12. 主线执行进度（2026-03）

1. Phase 1 已完成：Port/Event 骨架与旧 Broker 桥接层。
2. Phase 2 已完成：RuntimeBootstrap 装配入口落地并接入 app 初始化。
3. Phase 3 已完成：gateway 交易与行情适配器接入，支持 `livequote.mode=gateway`。
4. Phase 4 已完成关键收敛：BacktestRunner 接入 ClockPort，PaperBroker 接入 MarketDataPort 注入。
5. 发布边界保持：主体核心继续在 `pyqmt/` 目录内演进，gateway 为可选适配路径。

## 13. 验收封板结果（2026-03-17）

1. R1 远程实盘交易：通过（GatewayBrokerAdapter 已支持统一下单入口与查询/撤单）。
2. R2 实时推送接入：通过（GatewayMarketDataAdapter 接入 `/ws/quotes`，可替换 Redis 路径）。
3. R3 自主仿真交易：通过（PaperBroker 支持 MarketDataPort 注入，加载与新建链路均接入）。
4. R4 回测能力：通过（BacktestRunner + ClockPort 已落地并通过回测测试）。
5. R5 独立发布：通过（主体包不依赖 qmt-gateway 实现包）。
6. R6 可替换 adapter：通过（端口抽象 + AdapterRegistry + runtime 配置化选择）。
7. R7 历史兼容：通过（LegacyBrokerPortAdapter + SimulationBroker 兼容别名保留）。
8. R8 目录边界：通过（主体核心持续位于 `pyqmt/`）。

本轮验收测试结论：

1. `tests/service/test_runner.py` 通过。
2. `tests/core/test_gateway_broker_adapter.py` 通过。
3. `tests/core/test_adapter_registry.py`、`tests/core/test_broker_bridge.py`、`tests/core/test_clock_bridge.py`、`tests/core/test_gateway_client.py`、`tests/core/test_sim_broker_market_data.py` 通过。

## 14. 运行时持久化与启停控制（2026-03-18）

1. 新增策略运行时管理能力：
   - live/paper 账户运行时随服务启动加载并在策略页监控。
   - backtest 运行时按需创建，回测结束自动释放。
2. 新增策略运行时持久化：
   - 策略运行配置与状态持久化到 `home/strategy_runtimes.json`。
   - 服务重启后自动恢复 `running` 状态的策略运行时。
3. 新增策略启停控制：
   - 策略页运行时监控表支持对策略运行时执行“启动/停止”。
4. 新增投放闭环：
   - 回测报告页支持“转入仿真（输入本金）”与“转入实盘（选择账户）”。
   - 实盘投放后下单链路自动注入 `strategy_id`。

## 15. 主体去QMT化（2026-03-18）

### 15.1 已完成收敛

1. 主体运行时装配移除本地 `QMTBroker` 自动创建逻辑，启动阶段不再依赖 `xtquant`。
2. 主体配置移除 `livequote.qmt`、`livequote.redis`、`qmt.*` 字段，实时行情默认走 `livequote.mode=gateway`。
3. `livequote.py` 改写为 gateway WebSocket 消费模式，统一接收 `/ws/quotes` 并广播到消息总线。
4. 实盘投放默认绑定 `gateway:default`，不再要求配置本地 QMT 账户。

### 15.2 初始化向导重构需求（待实施）

目标：在保留现有 init-wizard 视觉风格与技术栈（FastHTML + MonsterUI）的前提下，按去QMT化后的能力边界重构初始化流程与配置项。

1. 配置加载约束（R15-0）
   - `pyqmt/config/__init__.py` 仅从同级目录加载 `defaults.yml`（dev 模式）。
   - 初始化向导读取初始值优先级：数据库中的 app_state > `defaults.yml`。
2. 强制重新初始化（R15-1）
   - 支持通过 `/init-wizard?force=true` 强制进入向导。
   - `force=true` 时允许已初始化系统重新配置，不因 `init_completed` 自动跳过。
3. 选项说明与降级告知（R15-2）
   - 向导每个配置项都必须展示用途说明。
   - 向导每个可选项都必须明确标注“不配置将导致哪些功能不可用”。
4. 向导步骤重设计（R15-3）
   - a. 运行环境：配置 `home`、`host`（仅本机访问/开放访问）、`port`、`prefix`。
   - b. 实时行情与交易网关：配置 `server`、`port`、`base_url`、`api_key`，并执行连通性测试；若跳过此步，系统不提供仿真与实盘功能。
   - c. 通知告警：配置 `dingtalk.*`、`mail.*`（可选）。
   - d. 数据初始化：配置 `epoch`、`tushare token`、首次下载数据长度（按年）。
   - e. 数据下载：提供进度条，下载品种包含证券日历、全A证券列表、历史日线行情（含复权因子与涨跌停价格）、ST 数据；品种定义以 `pyqmt/data/fetchers/tushare.py` 为准。
   - f. 完成分流：若已配置 gateway，完成后进入“实盘”菜单；未配置 gateway，进入“策略研究”菜单。
5. 验收口径（R15-4）
   - 已初始化系统可通过 `force=true` 重新进入向导并保存新配置。
   - 跳过网关配置后，UI 与后端能力判定一致地禁用仿真/实盘入口。
   - 完成页跳转路径与 gateway 是否可用保持一致。
