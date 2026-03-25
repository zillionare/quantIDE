# 发布态架构决议（v1）

## 1. 文档定位

本文件是当前阶段的主决议文档，用于指导 `pyqmt` 与 `qmt-gateway` 的收敛方向。

约束如下：

1. 本文件优先级高于 `.dev/` 下此前的接口草案、迁移计划和需求草案。
2. 旧文档保留作为历史讨论记录，不再视为当前发布方向的权威来源。
3. 对外文档暂不发布详细设计，开发期说明仅保留在 `.dev/`。

## 2. 核心判断

### 2.1 这是两个应用，不是一个应用的两个部署形态

`pyqmt` 和 `qmt-gateway` 是两个协作应用：

1. `pyqmt` 是主体应用：负责初始化、数据管理、策略开发、回测、仿真、实盘调度和 Web UI。
2. `qmt-gateway` 是边缘网关：负责运行在 Windows + QMT/xtquant 环境中，对外提供实时行情和交易能力。

因此：

1. 保留两个应用边界是正确的。
2. 主体应用不应继续保留本地 QMT 直连作为长期正式架构。
3. 主体应用应面向远程能力，而不是面向 QMT SDK。

### 2.2 主体内部的两套 broker 抽象属于重复设计，应收敛

当前主体内同时存在：

1. `pyqmt/service/base_broker.py`
2. `pyqmt/core/ports/broker.py`
3. 多个 bridge/wrapper

这不是“两个应用天然需要两套机制”，而是主体内部对同一交易能力的重复表达。

发布前应收敛为：

1. 主体内部只有一套正式 broker 抽象。
2. 所有运行模式都依赖这一套抽象。
3. bridge/wrapper 只作为短期迁移工具，不应成为发布态核心结构。

## 3. 发布态目标

### 3.1 主体应用目标

`pyqmt` 的发布态目标：

1. 不依赖 xtquant。
2. 可运行于 macOS / Linux / Windows。
3. 提供统一策略运行框架。
4. 支持回测、仿真、实盘三种运行模式。
5. 实盘能力通过远程 `qmt-gateway` 获取。

### 3.2 网关应用目标

`qmt-gateway` 的发布态目标：

1. 仅运行于安装了 QMT 的 Windows 机器。
2. 不承担策略框架主体职责。
3. 对主体暴露稳定且有限的能力：
   - 实时行情推送
   - 下单
   - 撤单
   - 资产/持仓/订单/成交查询
4. 不与主体共享内部实现，只共享协议约定。

## 4. 正式架构决议

### D1. 只保留 FastHTML + MonsterUI 作为 UI 技术栈

发布态不再以 FastAPI + Vue 作为目标技术栈。

影响：

1. `README.md`、`docs/`、gateway 公共文档不得再出现 FastAPI + Vue 表述。
2. 所有 UI 相关新设计默认以 FastHTML + MonsterUI 为前提。

### D2. 对外文档冻结，开发期文档统一放 `.dev/`

规则：

1. `README.md` 和 `docs/` 只保留最小占位，直到文档与代码一致。
2. 任何未定稿设计、迁移方案、重构讨论一律放 `.dev/`。
3. gateway 的开发中说明同样放 `.dev/qmt-gateway/`。

### D3. 配置唯一真源迁移到数据库

目标状态：

1. 运行期配置唯一真源是数据库中的 `AppState` 和后续拆分出的配置表。
2. `init-wizard` 负责写入配置。
3. 系统启动后，业务代码不得继续把 `cfg4py` 视为配置核心。

短期允许：

1. `cfg4py` 仅作为初始化默认值来源和过渡 fallback。
2. 新代码禁止继续扩大 `cfg4py` 使用面。

最终目标：

1. 移除 `cfg4py`。
2. 仅保留数据库配置 + 极小量环境变量启动参数。

### D4. 主体内部只保留一套正式 broker 抽象

决议：

1. 保留并演进 `pyqmt/core/ports/broker.py` 作为唯一正式交易能力接口。
2. 将 `pyqmt/service/base_broker.py` 中有业务价值的高阶语义吸收到正式接口中。
3. `base_broker.py` 及其 bridge/wrapper 进入退役路径。

需要保留的业务语义至少包括：

1. `buy`
2. `sell`
3. `buy_amount`
4. `sell_amount`
5. `buy_percent`
6. `sell_percent`
7. `trade_target_pct`
8. `cancel_order`
9. `cancel_all_orders`
10. 查询资产、持仓、订单、成交

说明：

`ports/broker.py` 当前更偏“submit/query”风格，后续应补足高阶交易语义，而不是继续保留两套接口并存。

### D5. 策略代码必须跨模式零修改运行

这是发布态硬性业务目标。

含义：

1. 同一份策略代码，不因运行模式不同而修改源代码。
2. 策略不感知回测/仿真/实盘差异。
3. 策略仅依赖统一的 broker、market data 和 runtime context。

运行模式统一为：

1. `backtest`
2. `paper`
3. `live`

其中：

1. `live` = 远程 gateway 行情 + 远程 gateway 交易
2. `paper` = 实时行情 + 本地仿真撮合
3. `backtest` = 历史数据回放 + 本地仿真撮合

### D6. 主体不再保留本地 QMT 直连作为正式路径

决议：

1. 主体应用中的本地 QMT 直连能力进入下线范围。
2. 所有实盘能力统一走 gateway。
3. 主体代码中不再允许新增长期保留的 xtquant 依赖路径。

### D7. `qtoid` 继续作为统一订单跟踪主键

这是必须保留的业务约束。

规则：

1. `qtoid` 是主体侧订单生命周期的主标识。
2. gateway/QMT 返回的外部订单号是外部标识，不替代 `qtoid`。
3. 订单、成交、UI 展示、策略等待/唤醒都应围绕 `qtoid` 对齐。

### D8. 数据标准字段继续统一为 `asset` + `date`

这是数据层硬约束。

规则：

1. 外部数据源必须先完成字段归一化再入库。
2. Parquet 类存储继续围绕 `asset` + `date` 组织。
3. 不允许在主体内部长期并存多套语义相同但列名不同的数据模型。

### D9. MessageHub 继续保留，但只作为进程内事件总线

决议：

1. `MessageHub` 继续作为主体进程内事件总线。
2. 它不是跨进程配置中心，也不是跨应用通信协议。
3. 主体与 gateway 的跨应用通信应通过明确的 HTTP / WS 协议，而不是 MessageHub。

### D10. 不保留 CLI 作为发布接口

决议：

1. 不再为主体或 gateway 设计 CLI 作为正式使用入口。
2. `pyproject.toml` 中无效或过时的脚本入口应后续清理。
3. 文档中不应继续将 CLI 作为主使用方式。

## 5. 收敛后的推荐结构

### 5.1 主体内部

建议保留以下稳定结构：

1. `pyqmt/core/ports/`
2. `pyqmt/core/runtime/`
3. `pyqmt/data/`
4. `pyqmt/service/` 中仅保留真正业务服务，不再承担重复抽象层职责
5. `pyqmt/web/`

### 5.2 模式装配

`RuntimeBootstrap` 应成为唯一正式装配入口。

它负责：

1. 读取数据库配置
2. 决定模式
3. 选择 market adapter
4. 选择 broker implementation
5. 构造统一 runtime context

### 5.3 注册中心

发布前需要收敛 `BrokerRegistry` 与 `AdapterRegistry` 的职责。

推荐方向：

1. 只保留一个对外清晰的运行时注册/发现模型。
2. adapter 注册属于底层实现细节，不应与 UI 使用的账户注册概念混杂。
3. 若两者保留，必须明确定义边界；否则应合并。

## 6. Phase 路线图与执行状态

本节是后续持续开发时的唯一 Phase 跟踪面板。

状态定义：

1. `completed`：该 Phase 的目标已完成，后续只允许小幅补漏。
2. `in-progress`：该 Phase 已开始，但仍存在关键未完成项。
3. `not-started`：该 Phase 尚未正式进入实施。

### Phase 总览

| Phase | 名称 | 状态 | 当前判断 |
|---|---|---|---|
| 1 | 决议与文档收敛 | completed | 已完成 |
| 2 | broker 抽象收敛 | completed | 正式 port、UI、runtime 已收敛到统一 broker 句柄 |
| 3 | QMT 去主体化 | in-progress | gateway 路径已建立，但主体内遗留物尚未清理完毕 |
| 4 | 配置收敛 | in-progress | 已有数据库配置基础，但 `cfg4py` 仍未退出运行时核心路径 |
| 5 | 发布前清理 | not-started | 依赖前四个 Phase 收敛后再执行 |

### Phase 1. 决议与文档收敛

状态：`completed`

目标：

1. 冻结公开文档为最小占位。
2. 在 `.dev/` 中维护唯一有效的开发期架构决议。
3. 明确旧草案仅作历史记录。

已完成：

1. `README.md` 已改为最小占位。
2. `docs/` 与 gateway 公共文档已改为最小占位。
3. `.dev/release_architecture_decision_v1.md` 已建立并作为当前主决议。
4. `.dev/README.md` 已指向当前主决议文件。

关闭标准：

1. 后续不再把未定稿设计写入公开 README 或 `docs/`。

### Phase 2. broker 抽象收敛

状态：`completed`

目标：

1. 补齐 `core/ports/broker.py` 的高阶业务语义。
2. 让策略运行时、UI、仿真、回测统一依赖它。
3. 将 `service/base_broker.py` 标记为兼容层。
4. 删除不必要 bridge/wrapper。

已知已完成部分：

1. `core/ports/broker.py` 已存在。
2. `RuntimeBootstrap`、adapter、bridge 已落地，说明收敛工作已开始。
3. gateway live 路径已经依赖 port/adapter 思路运行。
4. `BrokerPort` 已开始吸收高阶交易语义与正式 `ExecutionResult`。
5. `LegacyBrokerPortAdapter` 与 `GatewayBrokerAdapter` 已同步实现高阶 port 能力。
6. `StrategyBrokerProxy` 已去掉基于 `submit` 的运行时分支，改为直接依赖高阶正式契约。
7. focused tests 已覆盖 gateway adapter 与 strategy proxy 的首批收敛行为。
8. `example/dual_ma.py` 已通过真实回测路径验证，确认示例均线策略能够完成买入与卖出执行。
9. runtime 注册与动态创建账号路径已统一暴露 `PortBackedBroker`，UI 正式路径不再依赖 `GatewayBrokerWrapper`。

当前缺口：

1. `service/base_broker.py` 仍然作为旧实现接口存在，但已不再是发布态正式边界。
2. `LegacyBrokerPortAdapter` 仍然保留为内部兼容实现，用于旧 broker 接入正式 port。

当前 Phase 的下一步：

1. Phase 2 关闭。
2. 进入 Phase 3，开始 QMT 去主体化清点与迁移。

当前执行依据：

1. `broker_abstraction_convergence_v1.md`

关闭标准：

1. 主体内部仅存在一套正式 broker 抽象。
2. 策略 runtime、UI、仿真、回测、实盘全部依赖这套抽象。
3. bridge/wrapper 不再是发布态核心路径。

### Phase 3. QMT 去主体化

状态：`in-progress`

目标：

1. 下线主体中的本地 QMT 正式路径。
2. 所有 live 交易改为 gateway 路径。
3. 清理 xtquant 相关主体依赖。

已知已完成部分：

1. `qmt-gateway` 已作为独立应用存在。
2. gateway 行情与交易 adapter 已经在主体中接入。
3. 主体架构目标已经明确为不依赖 xtquant。

当前缺口：

1. 主体仓库中仍保留本地 QMT 相关实现与历史路径。
2. 尚未明确哪些文件只保留作过渡，哪些将被删除。
3. 依赖与导入路径尚未完成最终清场。

当前 Phase 的下一步：

1. 清点主体中所有 QMT/xtquant 相关模块和导入。
2. 标记为：保留、迁移、删除、仅测试保留。

关闭标准：

1. 主体安装与运行不要求 xtquant。
2. live 模式仅通过 gateway 获取 QMT 相关能力。

### Phase 4. 配置收敛

状态：`in-progress`

目标：

1. 盘点所有 `cfg4py` 使用点。
2. 明确对应数据库字段或配置服务。
3. 将业务代码切换到数据库配置读取。
4. 将 `cfg4py` 降级为 fallback。

已知已完成部分：

1. 初始化向导与 `AppState` 已存在，数据库配置模型已具备基础。
2. 用户已经明确要求配置以数据库为中心，而不是以 `cfg4py` 为中心。
3. `cfg4py` 使用面清单已完成。

当前缺口：

1. 业务代码中仍存在大量 `cfg` 直接读取路径。
2. 尚未定义统一配置服务层来替代 `cfg4py`。
3. 尚未决定 `AppState` 是否继续承载全部配置，还是拆分独立配置表。

当前 Phase 的下一步：

1. 设计 `ConfigService` 最小接口。
2. 明确每类配置迁移到 `AppState`、独立配置表还是应用常量。
3. 优先替换 runtime mode、gateway connection、notify 三类读取。

当前执行依据：

1. `cfg4py_inventory_v1.md`

关闭标准：

1. 主体运行期逻辑不再依赖 `cfg4py` 作为主配置源。
2. `cfg4py` 仅剩初始化 fallback，或已被完全移除。

### Phase 5. 发布前清理

状态：`not-started`

目标：

1. 删除 CLI 入口。
2. 移除 `cfg4py`。
3. 清理公共文档，恢复真正的发布文档。
4. 建立三模式一致性验收。

说明：

1. 本 Phase 不应提前执行。
2. 必须在 Phase 2、3、4 已达到关闭标准后再进入。

进入条件：

1. 配置真源已收敛。
2. 唯一 broker 抽象已确定并落地。
3. 主体已完成去 QMT 化。

关闭标准：

1. 仓库中不再保留过期入口与过渡性配置依赖。
2. 发布文档与代码状态一致。
3. 回测、仿真、实盘三模式通过统一验收。

## 7. 当前不做的事

1. 不为旧接口长期保留 migration 复杂度。
2. 不把桥接层当正式架构继续扩展。
3. 不在公开 README 和 `docs/` 中提前写最终用户文档。

## 8. 当前最重要的实施任务

按优先级排序：

1. 清点并压缩 `cfg4py` 使用面。
2. 设计唯一 broker 抽象的最终接口形状。
3. 明确 `BrokerRegistry` 与 `AdapterRegistry` 是否合并。
4. 让策略 runtime 只依赖统一接口。
5. 清理主体中的本地 QMT 遗留路径。

## 9. 验收口径

当以下条件成立时，可认为发布态架构收敛完成：

1. 主体不依赖 xtquant。
2. 主体内部只有一套正式 broker 抽象。
3. `cfg4py` 不再是运行期配置核心。
4. 同一策略可在回测、仿真、实盘三模式零修改运行。
5. 公开文档只描述真实发布态，不包含历史草案。
