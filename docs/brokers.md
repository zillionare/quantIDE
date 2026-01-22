brokers 由三类 broker 组成，回测、仿真和实盘，以实现各种场景下的委托（buy/sell）、撮合（自己撮合或者交给第三方撮合，比如在实盘情况下），以及账户管理等功能。

在本文档中，portfolio 与 strategy 在使用上有重叠部分。portfolio 是策略选股的结果，一个 portfolio 对应于一个 strategy。

## 账户和策略评估

本系统可以管理多个账户（portfolio），这些账户可以是回测（bt）、仿真（sim）或者实盘（live）账户。每个账户都有一个惟一的，由 uuid4 字符串指示的账户 id (portfolio_id)。

每个账户（portfolio）在运行期间都有自己的broker，以维护独立的 cash, positions，以及时钟（对回测而言）。

使用 quantstats 进行策略评估。策略评估不在 broker 内实现，而是通过单独的 metrics API 完成。metrics.py 接收 portfolio_id，从 assets 表中获取该组合的每日资产并计算每日收益。

## 账户和 broker 的生命期

### 创建portfolio 和 broker

在系统启动时，将会检索 portfolios 表格，对 status 为 True（即为 live） 中的每一个 portfolio，将重新生成对应的 broker 实例。

!!! warning
    例外，启动时将跳过 kind 为 'bt'的记录。因为回测一旦中断，就只能重启。

新的 broker/portfolio 实例将通过以下两种方式创建：

1. 回测账户通过 API 创建， API 指定为 POST /backtest
2. 仿真和实盘账户通过 UI 创建

portfolio 创建之后，将记录存入 portfolio 表格，同时，也将初始化对应的 assets 表格记录（如果不存在的话）

### portfolio/broker 的销毁

sim/live portfolio 可以在 UI 上进行关停。关停后，它们在系统中的 broker 实例也将被摧毁。但是，跟 portfolio 相关的中间状态都应该已经保存到了数据库中，并且仍然可以查询（不依赖broker 实例）。

回测结束之后，broker 实例被摧毁，但相关的交易数据（订单、成交、持仓、账户）会被保存到数据库中，供之后查询。

## 时钟

broker 中隐含了一个时钟概念，对于回测 broker, 这一概念至关重要。

实盘 broker 使用系统时钟。对于每一个委托，都是即收即报，自身无须进行撮合。仿真 broker 也使用系统时钟。两者都要实现收盘时快照功能，以记录当天的持仓和资金表。这些任务由 Apscheduler 来驱动。

回测 broker 的时钟则由委托指令驱动。在初始化及收到每一笔交易指令时，都将更新时钟。在更新时钟时，检查是否存在日期跨越，如果存在，则实现收盘快照功能，记录每日持仓及资金表。注意，在回测时，可能一次跨越多个交易日。

!!! info
    在记录持仓和资金表时，我们使用交易日历，而不是自然日历，这样计算出来的策略指标才是正确的。


### on_day_open

仿真和实盘 broker 都要实现此接口。在调用时， broker 会接收到当日涨跌停价格。

### on_day_close

仿真和实盘 broker 都要实现此接口。在调用时，broker 接受当天资产的收盘报价，需要将当天的资产数据形成快照写入数据库。对于实盘 broker, 它向第三方交易服务器请求状态同步，包括资产信息、持仓、订单和成交信息。

on_day_open 和 on_day_close 都由 apscheduler 驱动。它们将是后台线程任务。

## 数据流

broker 进行撮合、以及在不指定委托量进行委托时，需要两个关键的数据，order_time 发生之后的当天价格，以及当天的涨跌停价。

系统提供一个最新行情缓存（service/livequote.py），供 sim/live broker 在按资金量计算委托数量时使用。在仿真进行撮合时，broker 也会查询一次，以实现撮合。

在livequote 中，它向 qmt/redis 订阅最新的行情数据，以及当天的涨跌停，并缓存。而 broker 会订阅这个 livequote，并在quote 更新之后，撮合方法得到调用。

实盘 broker 和仿真 broker 都需要当天实时行情数据，仿真 broker 还需要涨跌停限价数据。当天实时行情必须以被动接收的方式获取。broker 提供一个 callback 方法，通过 msghub 订阅实时行情数据，并在内部保持对最新行情数据的引用（而非副本）。实时行情数据是全推数据，同时包括所有个股的实时价格。

回测 broker 需要历史行情数据（未复权）及每日涨跌停限价。这些数据应该通过 data_feed 这样的数据接口获得。data_feed 在 backtest 初始化时传入。

## 委托与撮合

一个委托指令通常需要提供委托的资产、数量、价格等信息。委托所属的 portfolio_id 由 broker 实例持有，API 层通过 portfolio_id 选择对应的 broker。

在实盘和仿真时，委托发出之后，一般都无法即刻成交并返回结果。因此，我们在设计 API 时，将给所有的委托指令增加一个 timeout 时间，在实盘/仿真中，如果柜台在指定时间内返回了成交结果，则立即返回；否则将等待到超时，返回结果/订单号。

委托指令通过 portfolio_id 来进行归类和进行策略评估。portfolio_id 由策略的创建者指定，但它应该是一个 uuid4 的字符串，以确保不会与系统中的其他策略冲突。

### 回测撮合

回测撮合的实现依赖于 order_time 以及所使用的数据源。

系统没有显式的开盘买入规则，一切由 order_time 来决定，以便同时适应日线、分钟线撮合。即，如果策略希望是以开盘价买入，则将 order_time 设置为早于（含等于）开盘时间；如果 order_time 为当日盘中交易时间，则在日线行情下，将以**收盘价**成交；在分钟线行情（v0.1 暂不实现）下，则从 order_time 起，按每分钟收盘价、成交量进行匹配，直到当天结束或者订单完成。如果在 order_time 之后，遇到涨、跌停，则涨跌停期间的成交量不参与匹配。

如果数据源是日线，则撮合时不看成交量，只要价格能够匹配，全部允许成交。

在数据源为日线时，如果遇到涨跌停，则不允许交易。比如，如果策略要求的开盘价买入（即将 order_time 设置为小于开盘时间），而开盘即涨停，则不允许交易；但如果开盘涨停，收盘打开涨停，且 order_time 为盘中时间，则允许以收盘价买入。对跌停亦同样处理。

## 仿真撮合

当委托抵达仿真 broker 时，会被存入一个 defaultdict(list)，键值是 asset，值是委托列表。当新的行情抵达时，会检查每个 asset 对应的委托列表，按新报价的成交量进行撮合，直到完全匹配之后，将该 asset 从集合中删除。

###  方法签名

```python
class Broker(metaclass=ABCMeta):
    """交易代理接口类。

    本接口类定义了交易代理的基本功能接口。
    """

    @abstractmethod
    async def buy(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """买入指令

        如果传入价格为 0, 则为市价买入。

        Args:
            asset: 资产代码，"symbol.SZ"风格
            shares: 委托数量
            price: 委托价格
            bit_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。
        """

    @abstractmethod
    async def buy_percent(
        self,
        asset: str,
        percent: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """买入指令按比例买入

        实际执行的结果可能与计划略有出入，因为买入时需要按 100 股为单位取整。

        Args:
            asset: 资产代码，"symbol.SZ"风格
            percent: 买入比例，0-1 之间的浮点数
            price: 订单价格，默认为 0，表示市价
            order_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交（含部成），返回空列表
        """
        ...

    @abstractmethod
    async def buy_amount(
        self,
        asset: str,
        amount: int | float,
        price: int | float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """买入指令按金额买入

        Args:
            asset: 资产代码，"symbol.SZ"风格
            amount: 买入金额
            price: 如果委托价格为 None，则以市价买入
            order_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交（含部成），返回空列表
        """
        ...

    @abstractmethod
    async def sell(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """卖出指令

        如果传入价格为 0, 则为市价卖出。

        Args:
            asset: 资产代码，"symbol.SZ"风格
            shares: 委托数量
            price: 委托价格
            bit_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交数据。如果超时未成交（含部成），返回空列表
        """
        ...

    @abstractmethod
    async def sell_percent(
        self,
        asset: str,
        percent: float,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """卖出指令按比例卖出

        Args:
            asset: 资产代码，"symbol.SZ"风格
            percent: 卖出比例，0-1 之间的浮点数
            order_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交（含部成），返回空列表
        """
        ...

    @abstractmethod
    async def sell_amount(
        self,
        asset: str,
        amount: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """卖出指令按金额卖出

        因为取整（手）的关系，实际卖出金额将可能超过约定金额，以保证回笼足够的现金。

        Args:
            asset: 资产代码，"symbol.SZ"风格
            amount: 卖出金额
            price: 如果委托价格为 None，则以市价卖出
            order_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交（含部成），返回空列表
        """
        ...

    @abstractmethod
    async def cancel_order(self, qt_oid: str):
        """取消订单，用于实盘

        取消指定订单。如果订单不存在或已成交，不做任何操作。

        Args:
            qt_oid: Quantide 订单 ID，是一个 uuid4 惟一值
        """
        ...

    @abstractmethod
    async def cancel_all_orders(self, side: OrderSide | None = None):
        """取消所有订单，用于实盘

        取消所有未成交订单。如果所有订单已成交，不做任何操作。

        Args:
            side: 订单方向，默认为 None，取消所有订单
        """
        ...

    @abstractmethod
    async def trade_target_pct(
        self,
        asset: str,
        target_pct: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """将`asset`的仓位调整到占比`target_pct`

        如果当前仓位大于 target_pct，则卖出；
        如果当前仓位小于 target_pct，则买入，直到现金用尽；在这种情况下，最终`asset`的仓位会小于约定的`target_pct`。

        !!! warning:
            受交易手数取整和手续费影响，最终仓位可能会小于等于约定仓位。

        Args:
            asset: 资产代码，"symbol.SZ"风格
            price: 委托价格
            target_pct: 目标仓位占比，0-1 之间的浮点数
            order_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None
        """
        ...
```

## 系统架构

在 qmt.services 目录下，有以下文件：

1. base_broker.py, 定义公共接口
2. abstract_broker.py, 抽象类。实现超时控制等通用功能。
3. backtest_broker.py, 回测交易代理。
4. simulation_broker.py, 仿真交易代理。
5. qmt_broker.py, QMT 交易代理。
6. metrics.py, 策略评估 API

在 qmt.web.apis 目录下，有 broker.py，以接受客户端发过来的请求，并且调用相应的代理进行处理。通过 BrokerRegistryMiddleware 中间件，根据 portfolio_id 来找到对应的 broker 进行处理。

对于回测，客户端在调用 start backtest 时，也会传入一个 portfolio_id， 这个 id 以及新创建的 backtest broker 会被注册到 BrokerRegistry 中，这样后续的请求就可以通过这个 id 来找到对应的 broker。

## 关键数据结构

### Assets 表格

记录组合的每日资产信息

| 字段名       | 数据类型    | 描述         |
| ------------ | ----------- | ------------ |
| portfolio_id | varchar(64) | 策略 ID      |
| dt           | date        | 资产归属日期 |
| cash         | float(64)   | 可用资金     |
| frozen_cash  | float(64)   | 冻结资金     |
| market_value | float(64)   | 市值         |
| total        | float(64)   | 总资产       |

### Portfolios 表格

记录系统中有多少个组合，以及它们的状态。

| 字段名       | 数据类型 | 字段描述                       |
| ------------ | -------- | ------------------------------ |
| portfolio_id | string   | 组合 ID                        |
| name         | string   | 组合名称                       |
| info         | string   | 组合信息                       |
| kind         | string   | 组合类型，bt, sim, live        |
| start        | date     | 账号开始动作时间，回测必填     |
| end          | date     | 账号结束动作时间，回测必填     |
| status       | bool     | 是否仍在运行。False 表示已封盘 |


### Trades 表格


| 字段名       | 数据类型          | 字段描述                                  |
| ------------ | ----------------- | ----------------------------------------- |
| portfolio_id | str               | 投资组合ID                                |
| tid          | str               | 成交id，主键。可使用代理（比如qmt）返回值 |
| qtoid        | str               | 对应的quantide order id（Orders 主键）    |
| foid         | str               | 代理（比如qmt）给出的order id             |
| asset        | str               | 资产代码                                  |
| shares       | float \| int      | 成交数量                                  |
| price        | float             | 成交价格                                  |
| amount       | float             | 成交金额 = 成交数量 * 成交价格            |
| tm           | datetime.datetime | 成交时间                                  |
| side         | OrderSide         | 成交方向                                  |
| cid          | str               | 柜台合同编号，应与同qtoid中的cid相一致    |
| fee          | float             | 本笔交易手续费（默认值为0）               |
