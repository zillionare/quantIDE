- alpha/alpha 里共有 84 个 .py 文件；其中 7 个在 pyqmt 里存在“内容完全一致”的副本， 18 个在 pyqmt 里能找到“同名文件但内容不同”（大概率已迁移后重写/裁剪），其余 59 个在 pyqmt 里“按文件名/内容哈希都找不到明显对应”，基本可以视为仍遗留在 alpha 的功能/代码。
仍遗留在 alpha 的功能/代码（最确定的部分）

- 回测与策略（pyqmt 里未找到相关代码） ： alpha/alpha/strategies/* （例如 alpha/alpha/strategies/priceseer.py:1 ）、 alpha/alpha/backtest/*
- 绘图/形态识别一整套 ： alpha/alpha/plots/* （如 maline.py 、 crossyear.py 、 extendline.py 等）
- 任务调度与数据更新任务（APScheduler） ： alpha/alpha/tasks/* （如 alpha/alpha/tasks/manager.py:18 、 calendar_updater.py 、 daily_bars_updater.py 、 stocklist_updater.py ）
- Streamlit Web 管理台 ： alpha/alpha/web/* （入口 alpha/alpha/web/app.py:1 ，以及 web/pages/* 、 web/pages/data/* ）
- 技术分析/特征工程/杂项核心能力 ： alpha/alpha/core/ta.py:24 、 core/features.py 、 core/triggers.py 、 core/marketglance.py 、 core/decimals.py 、 core/lang.py 、 core/types.py 、 core/monitors/*
- 其它独有模块 ： alpha/alpha/ext/ipolars.py 、 alpha/alpha/visualize/candlestick.py 、 alpha/alpha/services/failed_task_manager.py 、 alpha/alpha/data/factors/momentum.py
已合并/可能已合并（但内容可能已重构）的线索

- 已“内容完全一致”迁移的示例 ：
  - alpha/alpha/data/fetchers/tushare.py == pyqmt/pyqmt/data/fetchers/tushare.py
  - alpha/alpha/data/models/bar.py == pyqmt/pyqmt/data/models/bar.py
  - alpha/alpha/data/models/stock.py == pyqmt/pyqmt/data/models/stock.py
- 同名但内容不同（需要你按功能确认是否等价）的示例 ：
  - alpha/alpha/core/enums.py -> pyqmt/pyqmt/core/enums.py
  - alpha/alpha/data/stores/bars.py -> pyqmt/pyqmt/data/stores/bars.py
  - alpha/alpha/web/app.py -> pyqmt/pyqmt/app.py （但 alpha 这边是 Streamlit， pyqmt 这边是另一套 web 体系）
