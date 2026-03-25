# Phase 4 - cfg4py 使用面清点（v1）

## 1. 文档定位

本文件是新的 Phase 4 执行清单。

目标：

1. 盘点主体 `pyqmt` 中 `cfg4py` / `pyqmt.config.cfg` 的真实使用面。
2. 为每类使用点指定后续迁移方向。
3. 明确哪些属于运行时核心配置，哪些只是默认值、兼容层或遗留物。

说明：

1. `qmt-gateway` 已经有数据库配置管理器，当前问题主要集中在主体 `pyqmt`。
2. 本文档仅处理主体应用；gateway 仅作为对照，不纳入迁移范围。

## 2. 结论摘要

当前 `cfg4py` 在主体中的角色已经不是“唯一配置中心”，但仍然承担以下职责：

1. 启动期默认值提供者。
2. 运行时模式选择器。
3. gateway 连接参数来源。
4. `home`、`epoch`、`TIMEZONE` 等全局基础参数来源。
5. 通知配置来源。
6. 本地 QMT 遗留路径来源。

因此，新的 Phase 4 不应理解为“删除一个库”，而应理解为：

1. 把运行期配置真源切换到数据库配置服务。
2. 把 `cfg4py` 缩减为初始化 fallback。
3. 最终清除 fallback。

## 3. 分类清单

| 分类 | 典型用途 | 主要文件 | 目标归属 | 处理建议 |
|---|---|---|---|---|
| 启动与引导 | `home`、PID 文件、配置初始化、prefix | `pyqmt/app.py`, `pyqmt/config/__init__.py` | 配置服务 + 启动 fallback | 先保留，后迁移 |
| 运行模式选择 | `runtime.mode`, `livequote.mode`, `broker` 判断 | `pyqmt/core/runtime/modes.py`, `pyqmt/web/apis/broker.py` | 数据库配置 | 优先迁移 |
| gateway 连接 | `gateway.base_url`, timeout, 账号信息 | `pyqmt/core/runtime/gateway_client.py`, `pyqmt/service/livequote.py`, `pyqmt/service/init_wizard.py` | 数据库配置 | 优先迁移 |
| 数据目录与路径 | `cfg.home` 作为运行文件和数据目录根 | `pyqmt/app.py`, `pyqmt/service/strategy_runtime.py`, `pyqmt/service/grid_search.py`, `pyqmt/data/services/sector_sync.py` | 配置服务 | 先抽象后迁移 |
| 时区与日期起点 | `cfg.TIMEZONE`, `cfg.epoch` | `pyqmt/data/models/calendar.py`, `pyqmt/data/models/daily_bars.py`, `pyqmt/data/models/stocks.py`, `pyqmt/data/stores/base.py`, `pyqmt/data/services/stock_sync.py`, `pyqmt/data/services/index_sync.py` | 配置服务 / 应用常量 | 先抽象后迁移 |
| 通知配置 | 钉钉与邮件 | `pyqmt/notify/dingtalk.py`, `pyqmt/notify/mail.py` | 数据库配置 | 可直接迁移 |
| 本地 QMT 遗留 | `cfg.qmt.path` 等本地 QMT 配置 | `pyqmt/service/qmt_broker.py`, `pyqmt/core/xtwrapper.py` | 删除或迁出主体 | Phase 3 处理 |
| 旧订阅与历史路径 | `subscribe.py` 等旧启动链路 | `pyqmt/subscribe.py` | 删除 | 作为遗留路径清理 |

## 4. 详细分类

### 4.1 启动与引导

主要文件：

1. `pyqmt/config/__init__.py`
2. `pyqmt/app.py`

当前用途：

1. 初始化 `cfg4py`。
2. 设置 `TIMEZONE`。
3. 展开 `home` 路径。
4. 决定 API prefix。
5. 在启动阶段使用 `cfg.home` 建 PID 文件、初始化数据目录。

判断：

1. 这部分不能直接粗暴删除。
2. 需要一个新的 `ConfigService` 或等价抽象，在数据库可用后接管运行期配置。
3. 启动最早期允许保留最小 fallback。

### 4.2 运行模式选择

主要文件：

1. `pyqmt/core/runtime/modes.py`
2. `pyqmt/web/apis/broker.py`

当前用途：

1. 决定 live / paper / backtest。
2. 决定是否启用 gateway 行情与 gateway broker。
3. 在 Web API 中判断 backtest 特殊分支。

判断：

1. 这部分已经属于运行期核心配置。
2. 应优先从数据库配置迁出。
3. 这是新的 Phase 4 最重要的迁移入口之一。

### 4.3 gateway 连接配置

主要文件：

1. `pyqmt/core/runtime/gateway_client.py`
2. `pyqmt/service/livequote.py`
3. `pyqmt/service/init_wizard.py`
4. `pyqmt/web/pages/init_wizard.py`

当前用途：

1. 读取 gateway 基础地址。
2. 构造 WS URL。
3. 初始化向导默认值回填。

判断：

1. 这部分应当直接归数据库配置管理。
2. `init_wizard` 已经有 `AppState`，迁移条件成熟。

### 4.4 数据目录与路径

主要文件：

1. `pyqmt/app.py`
2. `pyqmt/service/strategy_runtime.py`
3. `pyqmt/service/grid_search.py`
4. `pyqmt/data/services/sector_sync.py`

当前用途：

1. 生成 PID 文件。
2. 初始化本地数据目录。
3. 保存策略运行时文件。
4. 推导数据目录路径。

判断：

1. 这是系统底座配置。
2. 适合通过统一配置服务暴露，而不是由各模块直接访问 `cfg`。
3. 暂不建议直接散点改为数据库查询。

### 4.5 时区与日期起点

主要文件：

1. `pyqmt/data/models/calendar.py`
2. `pyqmt/data/models/daily_bars.py`
3. `pyqmt/data/models/stocks.py`
4. `pyqmt/data/stores/base.py`
5. `pyqmt/data/services/stock_sync.py`
6. `pyqmt/data/services/index_sync.py`
7. `pyqmt/core/scheduler.py`

当前用途：

1. 全局时区运算。
2. 历史数据下载起点。
3. 默认当前时间和交易日历逻辑。

判断：

1. `TIMEZONE` 更像应用常量或统一环境配置。
2. `epoch` 是业务配置，更适合放数据库。
3. 这里需要配置服务抽象先落地，再逐步替换。

### 4.6 通知配置

主要文件：

1. `pyqmt/notify/dingtalk.py`
2. `pyqmt/notify/mail.py`

当前用途：

1. 读取钉钉 token / secret / keyword。
2. 读取邮件服务器和收发件人。

判断：

1. 这部分最适合直接迁移到数据库。
2. 不需要长期保留 `cfg4py` 依赖。

### 4.7 本地 QMT 遗留

主要文件：

1. `pyqmt/service/qmt_broker.py`
2. `pyqmt/core/xtwrapper.py`

当前用途：

1. 读取本地 QMT 路径。
2. 支持主体本地直连 QMT。

判断：

1. 这属于新的 Phase 3 要清理的遗留路径。
2. 不建议为其设计新的数据库配置归宿。
3. 目标应是迁出主体或删除。

### 4.8 旧订阅与历史路径

主要文件：

1. `pyqmt/subscribe.py`

判断：

1. 这是旧路径。
2. 不应纳入新配置体系。
3. 直接放入后续删除清单。

## 5. 迁移优先级

### P1. 立即优先处理

1. `runtime.mode`
2. `livequote.mode`
3. `gateway.base_url` 及相关 gateway 连接参数
4. 通知配置

原因：

1. 这些都已经是运行期配置。
2. 它们与 `AppState` 现有模型高度接近。
3. 迁移收益高，风险相对低。

### P2. 需要先抽象再迁移

1. `home`
2. `TIMEZONE`
3. `epoch`
4. 各类基于 `cfg.home` 推导出的路径

原因：

1. 这些配置被高频、底层、多模块使用。
2. 直接把散点读取改成数据库查询会让依赖更乱。
3. 需要先提供统一配置服务。

### P3. 不迁移，直接进入清理路径

1. `qmt_broker.py` 中的 `cfg.qmt.path`
2. `xtwrapper.py`
3. `subscribe.py`

原因：

1. 这些是主体本地 QMT 或旧链路遗留。
2. 它们属于新的 Phase 3，而不是 Phase 4 的长期配置设计对象。

## 6. 建议的新配置分层

建议按三层处理：

1. 数据库配置：运行期唯一真源。
2. 配置服务：对业务代码暴露统一读取接口。
3. 启动 fallback：仅在数据库未初始化时使用最小默认值。

建议的读取原则：

1. Web、runtime、gateway client、通知模块不得直接依赖 `cfg4py`。
2. 底层数据模型和日历类最终也不应直接依赖 `cfg4py`。
3. 只有应用启动最早期代码允许临时保留 fallback。

## 7. Phase 4 下一步任务

1. 设计 `ConfigService` 最小接口。
2. 明确 `AppState` 是否继续承载全部配置，还是拆分出独立配置表。
3. 优先替换 runtime mode、gateway connection、notify 三类读取。
4. 在决议文档中将本文件登记为 Phase 2 的当前执行依据。
