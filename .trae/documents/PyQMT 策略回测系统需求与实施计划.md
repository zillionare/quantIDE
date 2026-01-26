# PyQMT 策略回测与管理系统需求文档

## 1. BaseStrategy 详细定义

`BaseStrategy` 是所有策略的父类，位于 `pyqmt/core/strategy.py`。

### 1.1 关键属性 (Attributes)
*   `self.broker: Broker`: 绑定的交易柜台实例。策略通过它来获取持仓、资金和报单。
*   `self.config: Dict[str, Any]`: 策略配置参数。

### 1.2 核心生命周期接口 (Lifecycle Methods)
*   `init(self)`: 初始化。
*   `on_start(self)`: 开始前调用。
*   `on_stop(self)`: 结束后调用。
*   `on_day_open(self)`: 每日开盘前调用 (如 09:00)。
*   `on_day_close(self)`: 每日收盘后调用 (如 15:30)。
*   `on_bar(self, tm, quote, frame_type)`: 每个周期的行情驱动。

### 1.3 交易执行 (Execution)
策略**不定义** `buy/sell` 方法，而是直接调用 `self.broker` 的接口：
*   `self.broker.buy(...)`
*   `self.broker.sell(...)`
*   `self.broker.positions` (属性)

---

## 2. BacktestRunner 详细定义

`BacktestRunner` 是回测模式的专用驱动器，位于 `pyqmt/service/runner.py`。

### 2.1 核心功能
1.  **初始化**: 创建 `BacktestBroker`，实例化策略。
2.  **事件循环**:
    *   **每日循环**: 触发 `on_day_open` -> 遍历当日 Bars -> 触发 `on_day_close`。
    *   **Bar 循环**: 设置 Broker 时钟 -> 触发 `strategy.on_bar`。
3.  **统计**: 回测结束后生成报告。

---

## 3. Web API 调整说明

### Phase 4: 任务管理 API (Job Management API)
虽然策略与 Broker 的交互已改为**进程内直接调用**（不再通过 HTTP 交易接口），但我们仍需要 Web API 来**管理回测任务**：
*   **目的**: 供前端页面（Web UI）使用。
*   **功能**:
    1.  `GET /strategies`: 让前端知道有哪些策略可选。
    2.  `POST /backtest/run`: 让前端能触发一个回测任务（服务器端启动 Runner）。
*   **注意**: 原有的 `/buy`, `/sell` 等 HTTP 接口在回测模式下将不再被策略使用。

---

# 实施计划 (Implementation Plan)

## Phase 1: 策略核心 (Strategy Core)
1.  **创建 `pyqmt/core/strategy.py`**:
    *   定义 `BaseStrategy`。
    *   实现 `init`, `on_start`, `on_stop`, `on_bar`, `on_day_open`, `on_day_close` 空方法。
2.  **创建 `pyqmt/strategies/example/dual_ma.py`**:
    *   在 `on_day_open` 中打印日志。
    *   在 `on_bar` 中调用 `self.broker.buy()`。

## Phase 2: 发现服务 (Discovery)
1.  **创建 `pyqmt/service/discovery.py`**: 实现策略扫描。

## Phase 3: 回测运行器 (Backtest Runner)
1.  **创建 `pyqmt/service/runner.py`**:
    *   实现嵌套循环：`for day in days: on_day_open() -> for bar in bars: on_bar() -> on_day_close()`。

## Phase 4: 任务 API (Job API)
1.  **更新 `pyqmt/web/apis/broker.py`**:
    *   仅实现策略列表和回测启动接口，作为系统控制面板。
