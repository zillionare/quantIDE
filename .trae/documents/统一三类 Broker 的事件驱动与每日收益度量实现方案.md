## 更新点（QMT说明）
- QMTBroker 订阅 `DayOpen/DayClose` 事件仅做“接口占位”，实际每日快照与收益数据仍以服务器同步为准；暂不接入 DataFeed 计算，也不改撮合路径。
- 当未连接服务器时，QMT 不生成快照；连接与同步逻辑后续单独集成。

## 落地范围（本次实现）
- BacktestBroker：事件化（移除自驱）、即刻撮合、每日快照/收益
- SimulationBroker：保留 QuoteEvent 即时撮合、订阅 day 事件生成每日快照/收益
- 抽象与模块：DataFeed（backtest/simulation 两实现）、AbstractBroker 增强、metrics（收益与组合指标）
- Web API：`/broker/backtest/start`、`/broker/day_open`、`/broker/day_close`、`/broker/backtest/stop`

## DataFeed（仅 backtest/simulation）
- 接口：`get_open/close/last/limits/volume`
- BacktestFeed：来自 DailyBars（含 limits/volume）
- SimulationFeed：分钟/日线 close 优先，缺失回退最后 Quote；limits 从日线拼接

## AbstractBroker 增强
- `on_day_open(event)`：结转上一交易日快照（positions/assets），刷新 T+1 可用量
- `on_day_close(event)`：可选当日结转（默认由下一日 open 结转）
- `snapshot_positions(date)`、`snapshot_asset(date)`、`market_value(date)`（用 DataFeed.get_close）
- 指标：`compute_daily_returns()`、`compute_portfolio_metrics(window?)`

## BacktestBroker
- 即刻撮合：open/close 可配；限价需落在 `low~high`；整手/T+1/涨跌停校验；可选按当日 volume 部分成交
- 事件：订阅 `md:day_open/md:day_close`，由 Web API 驱动；`stop` 清理状态

## SimulationBroker
- 保留按秒 `QuoteEvent` 撮合（已做 pending 分桶与部分成交）
- 事件：订阅 `md:day_open/md:day_close` 结转每日快照与收益

## API 与场景（满足 1–8）
1. `start backtest` 返回配置
2. t0 `buy`：即刻撮合并返回成交，更新内存与落库
3. 即刻返回成交明细
4. t1 `day_open`：生成 t0 的 positions/assets
5. t1 `sell`：即刻撮合并返回成交
6. t2 `day_open`：生成 t1 的 positions/assets
7. 查询 t2 的 asset 与每日收益（assets + metrics）
8. `stop backtest`：结束

## 验证
- 单元：快照、市场价值、收益与指标
- 集成：按 1–8 场景跨 backtest/simulation 验证；QMT 保持原状，不改动路径

确认后我将按此方案开始编码（仅 backtest/simulation 与基类/抽象/API 部分；QMT 保持现状）。