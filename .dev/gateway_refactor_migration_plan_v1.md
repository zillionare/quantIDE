# 主体工程重构迁移清单（v1）

## 1. 目标

在不迁出 `pyqmt/` 目录的前提下，完成从“实现直连”到“Port + Adapter”模式的迁移。  
本清单仅用于实施前评审，不在本阶段执行代码改造。

## 2. 迁移原则

1. 需求一致优先，不为“重构而重构”
2. 先抽象、后替换、再收敛
3. 每阶段可回滚，确保主分支可运行
4. 对 2025 年 11 月以来行为语义做回归对齐

## 3. 文件级迁移清单

### 3.1 核心保留并逐步适配

1. [pyqmt/service/base_broker.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/base_broker.py)  
   - 动作：保留为兼容层基线，新增与 `BrokerPort` 的映射适配器
2. [pyqmt/service/abstract_broker.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/abstract_broker.py)  
   - 动作：抽出超时等待与事件配对能力，供新 Runtime 复用
3. [pyqmt/service/registry.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/registry.py)  
   - 动作：升级为 AdapterRegistry（支持 capability 声明）
4. [pyqmt/service/runner.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/runner.py)  
   - 动作：拆分为通用 Runtime + Backtest 模式装配

### 3.2 行情链路迁移

1. [pyqmt/service/livequote.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/livequote.py)  
   - 动作：从“QMT/Redis 模式选择器”改为 `MarketDataPort` 实现之一
2. [pyqmt/service/datafeed.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/datafeed.py)  
   - 动作：保留 `BarsFeed` 语义，映射到统一事件/快照接口
3. [pyqmt/subscribe.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/subscribe.py)  
   - 动作：标记为历史路径，逐步下线

### 3.3 Broker 实现迁移

1. [pyqmt/service/qmt_broker.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/qmt_broker.py)  
   - 动作：拆分“QMT 本地实现”与“Gateway 远程实现”公共语义层
2. [pyqmt/service/sim_broker.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/sim_broker.py)  
   - 动作：沉淀为 `PaperBrokerAdapter` 主实现
3. [pyqmt/service/backtest_broker.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/service/backtest_broker.py)  
   - 动作：保留并改为依赖 `ClockPort + MarketDataPort + StoragePort`

### 3.4 应用装配迁移

1. [pyqmt/app.py](file:///c:/Users/aaron/workspace/pyqmt/pyqmt/app.py)  
   - 动作：新增 runtime 装配入口，替换直接创建 broker/livequote 的逻辑
2. Web API 与页面调用层（`pyqmt/web/*`）  
   - 动作：统一从 Runtime 容器取 broker/market 服务，不直连实现类

## 4. 新增文件建议（仍在 pyqmt 下）

1. `pyqmt/core/ports/market_data.py`
2. `pyqmt/core/ports/broker.py`
3. `pyqmt/core/ports/clock.py`
4. `pyqmt/core/ports/storage.py`
5. `pyqmt/core/runtime/engine.py`
6. `pyqmt/core/runtime/modes.py`
7. `pyqmt/core/domain/events.py`
8. `pyqmt/adapters/gateway/`（可先内置，后续再抽成可选包）

## 5. 分阶段实施计划

### Phase 1：接口落地（不改行为）

1. 新增 Port 与 Event 定义
2. 保留旧调用链，通过兼容层桥接
3. 增加接口契约测试

### Phase 2：运行时装配收敛

1. 增加 Runtime 容器与 mode 装配
2. `app.py` 切换到 Runtime 装配入口
3. 保证现有页面/接口无感迁移

### Phase 3：gateway 远程化

1. 增加 GatewayMarketDataAdapter
2. 增加 GatewayBrokerAdapter
3. 移除 Redis 依赖路径

### Phase 4：仿真/回测统一

1. `SimulationBroker` 迁移为 PaperBrokerAdapter
2. `BacktestRunner` 与 BacktestBroker 接入统一 ClockPort
3. 建立跨模式一致性回归

### Phase 5：发布边界收敛

1. 主体持续留在 `pyqmt/` 并发布
2. adapter 通过可选依赖管理
3. 明确最小安装与扩展安装说明

## 6. 验证清单（实施时执行）

1. 回测单测全部通过（基线）
2. 仿真下单流程行为与重构前一致
3. Live/Paper/Backtest 同策略输出一致性在可接受阈值内
4. 不安装 gateway 依赖时，回测与本地测试可运行
5. 新增一个“空实现 adapter”可完成注册并通过健康检查

## 7. 风险与守护

1. 风险：接口抽象过度导致落地成本高  
   - 守护：坚持最小可用 Port，优先映射现有能力
2. 风险：迁移中语义偏差  
   - 守护：建立“旧接口-新接口”对照测试
3. 风险：发布边界漂移  
   - 守护：CI 增加“无 gateway 依赖安装测试”
