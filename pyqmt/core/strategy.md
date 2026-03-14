# 策略开发文档 (Core Strategy)

## 1. 概述
`BaseStrategy` 是所有策略的基类，定义了策略的生命周期和核心回调方法。

## 2. 生命周期
策略的生命周期由 `BacktestRunner` (回测) 或实盘引擎驱动。

### 2.1 初始化
- `__init__(self, broker, config)`: 构造函数，传入 broker 实例和配置字典。
- `init(self)`: 异步初始化方法，在实例化后立即调用。用于加载数据、初始化变量等。

### 2.2 启动与停止
- `on_start(self)`: 回测/实盘开始前调用。
- `on_stop(self)`: 回测/实盘结束后调用。

### 2.3 每日循环
- `on_day_open(self, tm)`: 每日开盘前 (09:30 前) 调用。
- `on_day_close(self, tm)`: 每日收盘后 (15:30 后) 调用。

## 3. 核心驱动
- `on_bar(self, tm, quote, frame_type)`: 核心回调方法。
  - `tm`: 当前时间 (datetime)。
  - `quote`: 当前行情快照 (Dict)。对于回测，包含持仓和关注标的(universe)的最新价格。
  - `frame_type`: 周期类型 (DAY, MIN1, etc.)。

## 4. 交易接口
策略通过 `self.broker` 访问交易接口：
- `buy(asset, shares, price, ...)`: 买入。
- `sell(asset, shares, price, ...)`: 卖出。
- `orders`: 获取订单。
- `positions`: 获取持仓。
- `cash`: 获取现金。

### 4.1 交易注解 (Trade Annotations)
在下单时，可以通过 `extra` 参数传递额外的结构化信息 (Dict)，这些信息会被序列化为 JSON 并存储在订单记录中。
```python
await self.broker.buy(
    asset="000001.SZ",
    shares=100,
    price=10.0,
    extra={
        "reason": "Signal triggered",
        "features": {"ma5": 10.1, "rsi": 30}
    }
)
```

## 5. 数据接口
- `get_history(asset, count, ...)`: 获取历史 K 线数据。

## 7. 策略运行记录 (Strategy Log)
- `record(key, value, dt=None, extra=None)`: 记录策略的中间状态或指标。
  - `key`: 指标名称 (str)
  - `value`: 指标值 (float)
  - `dt`: 时间 (可选)
  - `extra`: 额外信息 (dict, 可选)

这些数据将被存储在 `strategy_logs` 表中，便于后续分析（例如绘制因子曲线、分析信号触发原因等）。

```python
self.record("rsi_60", 45.3, extra={"threshold": 30})
```

## 8. 最佳实践：record vs extra

在策略开发中，我们提供了两种记录数据的方式：`Strategy.record` 和 `Order.extra`。它们的用途有所区别：

| 特性         | Strategy.record                             | Order.extra                                              |
| :----------- | :------------------------------------------ | :------------------------------------------------------- |
| **绑定对象** | 时间点 (Time Series)                        | 交易订单 (Order Event)                                   |
| **主要用途** | 记录连续的指标、状态或因子值                | 记录交易决策的上下文快照                                 |
| **典型场景** | 绘制 RSI 曲线、记录账户杠杆率、监控信号强度 | 记录"为什么在这一刻买入"、当时的止损价、触发交易的信号值 |
| **查询方式** | 查询 `strategy_logs` 表                     | 查询 `orders` 表的 `extra` 字段                          |

**推荐做法**：
*   使用 `record` 来记录**过程数据**（即使没有交易发生，也想观察的数据）。
*   使用 `extra` 来记录**决策依据**（解释这笔交易的原因）。

例如：
```python
# 1. 无论是否交易，都记录 RSI 指标供后续画图
self.record("rsi", rsi_value)

if rsi_value < 30:
    # 2. 发生交易时，在 extra 中记录触发交易的具体 RSI 值和阈值
    await self.buy(..., extra={
        "reason": "RSI_OVERSOLD",
        "rsi_value": rsi_value,
        "threshold": 30
    })
```
