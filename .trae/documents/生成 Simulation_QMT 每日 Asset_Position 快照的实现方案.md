## 文档更新
- 目标：把 Backtest/Simulation 的撮合与日结流程转为 Mermaid 时序图，加入 `docs/v0.1/backtesting.md` 的新章节，便于用户与客户端策略统一理解与验证。
- 位置：在现有设计文档末尾追加 `## 事件与时序（DataFeed–Broker 协调）`，包含 Backtest、Simulation、统一撮合三个时序图与简要说明。

## 内容大纲
- 时序图 1（Backtest：SimClock + ParquetFeed）：订单→advance_to→开盘 QuoteEvent→撮合→收盘事件→次日开盘结转快照。
- 时序图 2（Simulation：SystemClock + 每秒 QuoteEvent）：订单入队→每秒部分成交→收盘统一废单→快照结转。
- 时序图 3（统一 on_quote 撮合）：时间门槛、涨跌停/限价、整手、部分成交与订单完成/保留。
- 验证步骤：按 t0/t1/t2 的事件流，查询 assets/positions 与收益曲线。

## 实施说明
- 你已同意保存文档并开始实施。我将先更新 `docs/v0.1/backtesting.md` 追加上述 Mermaid 时序图与说明，然后继续按既定方案推进后续实现（补齐 AbstractBroker 的日事件与 catch-up、统一撮合路径、DataFeed 抽象与 backtest/sim 接入）。