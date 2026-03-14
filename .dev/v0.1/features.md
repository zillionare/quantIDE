## 功能

本版本提供以下功能：

### 交易接口
1. 基于 XtTrader 的交易接口
2. backtesting 接口
3. 模拟交易接口

以上功能以 REST 风格提供，接口保持一致。在应用初始化时，通过参数指定以哪种方式运行。

### 基础数据

1. 提供实时行情数据，通过 websocket 和 REDIS stream 提供。


### 不实现的功能

1. 不支持多账号交易
2. 支持多策略运行，但每个策略共享一个交易账户，共享资金分配。



## Reference

xtquant 的文档在这里：
1. xtdata 子模块 https://dict.thinktrader.net/nativeApi/xtdata.html
2. xttrade 子模块 https://dict.thinktrader.net/nativeApi/xttrader.html
