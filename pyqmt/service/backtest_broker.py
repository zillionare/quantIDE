import datetime
import math
import uuid
from typing import Literal

import numpy as np
import polars as pl
from loguru import logger

from pyqmt.core.enums import BidType, BrokerKind, FrameType, OrderSide, OrderStatus
from pyqmt.core.errors import (
    BadPercent,
    ClockAfterEnd,
    ClockBeforeStart,
    ClockRewind,
    DupPortfolio,
    InsufficientAmount,
    InsufficientCash,
    InsufficientPosition,
    LimitPrice,
    NoDataForMatch,
    NonMultipleOfLotSize,
    PriceNotMeet,
    TradeError,
)
from pyqmt.data.models.calendar import calendar
from pyqmt.data.models.daily_bars import daily_bars
from pyqmt.data.sqlite import Asset, Order, Portfolio, Position, Trade, db
from pyqmt.service.abstract_broker import AbstractBroker
from pyqmt.service.base_broker import TradeResult
from pyqmt.service.datafeed import DataFeed


class BacktestBroker(AbstractBroker):
    def __init__(
        self,
        bt_start: datetime.date | datetime.datetime,
        bt_end: datetime.date | datetime.datetime,
        portfolio_id: str,
        data_feed: DataFeed,
        principal: float = 1_000_000,
        commission: float = 5e-4,
        portfolio_name: str = "backtest",
        match_level: Literal["day", "minute"] = "day",
        desc: str = "",
    ):
        """回测 broker

        Args:
            - portfolio_id, 账户ID
            - principal, 账户初始资金
            - commission, 账户手续费率
            - account, 账户名称
            - data_feed, 行情数据源
            - match_level, 匹配模式，day为日线，minute为分钟线
            - desc, 账户/策略描述
        """
        super().__init__(
            portfolio_id=portfolio_id,
            principal=principal,
            commission=commission,
            portfolio_name=portfolio_name,
        )

        self._data_feed = data_feed
        self._match_level = match_level
        self._bt_start: datetime.datetime = calendar.replace_time(bt_start, 9, 30)
        # End time should cover the closing operations
        self._bt_end: datetime.datetime = calendar.replace_time(bt_end, 16, 0)
        self._bt_stopped: bool = False
        self._desc: str = desc

        # 回测时钟，初始化为 bt_start 的前一个交易日 (Day 0)
        prev = calendar.day_shift(bt_start, -1)
        self._clock: datetime.datetime = calendar.replace_time(prev, 15)

        # 初始化回测数据记录
        self.init_backtest()

        self._positions: dict[str, Position] = {}
        self._last_snapshot_day: datetime.date | None = None

        # Use patched logger to support time-travel logging
        self.logger = logger.bind(portfolio_id=portfolio_id)

    @property
    def positions(self) -> dict[str, Position]:
        return self._positions

    @property
    def cash(self) -> float:
        return self._cash

    def init_backtest(self) -> None:
        """创建回测相关数据记录

        在调用本方法时，数据库中不应该存在与本 portfolio 相关的 portfolios 和 assets 记录。
        """
        # 检查 portfolio 是否已存在
        if db.get_portfolio(self._portfolio_id):
            raise DupPortfolio(self._portfolio_id)

        # 检查 assets 表中是否已有该 portfolio 的记录
        if not db.query_assets(self._portfolio_id).is_empty():
            raise DupPortfolio(self._portfolio_id)

        portfolio = Portfolio(
            portfolio_id=self._portfolio_id,
            kind=BrokerKind.BACKTEST,
            start=self.as_date(self._clock),
            name=self._portfolio_name,
            info=self._desc,
            end=self.as_date(self._bt_end),
            status=True,
        )
        db.insert_portfolio(portfolio)

        # 记录回测前的资产，以便计算 day 0的收益，与 baseline 对齐
        asset = Asset(
            portfolio_id=self._portfolio_id,
            dt=self.as_date(self._clock),
            principal=self._principal,
            cash=self._principal,
            frozen_cash=0,
            market_value=0,
            total=self._principal,
        )
        db.upsert_asset(asset)

    async def stop_backtest(self) -> None:
        """停止回测

        在停止回测时，需要模拟 bt_end 日，按收盘价卖出所有持仓的操作。
        注意：遵守 T+1 规则，仅卖出可用持仓 (avail > 0)。
        """
        # 1. 设置时钟到 bt_end，这会触发补齐逻辑，确保当前 positions 是最新的
        self.set_clock(self._bt_end)

        # 2. 遍历所有持仓并卖出可用部分
        order_time = self._bt_end
        # 获取当前所有持仓资产
        assets_to_sell = [a for a, p in self._positions.items() if p.avail > 0]

        for asset in assets_to_sell:
            pos = self._positions[asset]
            # 执行卖出。使用市价单 (price=0) 在 order_time (15:00) 撮合，将以收盘价成交
            await self.sell(asset=asset, shares=pos.avail, price=0, order_time=order_time)

        # 3. 标记停止并更新数据库状态
        # 计算剩余持仓（冻结部分）的市值
        remaining_mv = self.market_value()

        dt = self.as_date(self._clock)
        asset_info = Asset(
            portfolio_id=self._portfolio_id,
            dt=dt,
            principal=self._principal,
            cash=self._cash,
            frozen_cash=0,
            market_value=remaining_mv,
            total=self._cash + remaining_mv,
        )
        db.upsert_asset(asset_info)

        self._bt_stopped = True
        db.update_portfolio(self._portfolio_id, status=False)

    def _fill_history_gaps(self, end_fill: datetime.date) -> None:
        """填补历史数据缺失，确保资产记录和持仓记录完整

        本方法要求在每一次交易时，都要更新数据库中的 asset 和 position 记录。

        Args:
            end_fill (datetime.date): 填充到的结束日期，一般为 clock 前一日
        """
        # 1. 获取需要填补的日期范围
        # 在回测场景下， latest_asset一定存在，这是 init_backtest 保证的
        latest_asset: Asset = db.get_asset(portfolio_id=self._portfolio_id) # type: ignore

        start_fill = calendar.day_shift(latest_asset.dt, 1)

        fill_dates = calendar.get_frames(start_fill, end_fill, FrameType.DAY)
        if not fill_dates:
            return

        # 2. 获取最新持仓
        latest_pos = db.get_positions(dt=None, portfolio_id=self._portfolio_id)

        if latest_pos.is_empty():
            self._fill_assets_only(fill_dates, latest_asset)
        else:
            self._fill_positions_and_assets(fill_dates, latest_pos, latest_asset)

    def _fill_assets_only(self, fill_dates: list[datetime.date], last_asset: Asset):
        """无持仓时，仅同步资产日期"""
        assets_to_insert = [
            Asset(
                portfolio_id=self._portfolio_id,
                dt=d,
                principal=last_asset.principal,
                cash=last_asset.cash,
                frozen_cash=last_asset.frozen_cash,
                market_value=0.0,
                total=last_asset.cash,
            )
            for d in fill_dates
        ]
        db.upsert_asset(assets_to_insert)

    def _fill_positions_and_assets(
        self, fill_dates: list[datetime.date], latest_pos: pl.DataFrame, last_asset: Asset
    ):
        """有持仓时，计算行情变动及复权折算

        在复权发生时，按现金折算持仓价值，更新资产记录。

        Args:
            fill_dates (list[datetime.date]): 填充的日期列表
            latest_pos (pl.DataFrame): 最新持仓 DataFrame
            last_asset (Asset): 上一个交易日的资产记录
        """
        assets = latest_pos["asset"].unique().to_list()
        start, end = fill_dates[0], fill_dates[-1]

        # 1. 获取行情与复权因子
        prices_df = self._data_feed.get_close_factor(assets, start, end)

        # 2. 获取上一个交易日 (old_dt) 的 factor 作为初始基准
        old_dt = calendar.day_shift(start, -1)
        base_factors = self._data_feed.get_close_factor(assets, old_dt, old_dt).select([
            pl.col("asset"),
            pl.col("factor").alias("base_factor")
        ])

        # 3. 构造填充模板
        pos_template = latest_pos.select([
            pl.col("asset"),
            pl.col("shares"),
            pl.col("price"),
            (pl.col("mv") / pl.col("shares")).alias("last_mkt_price")
        ]).join(base_factors, on="asset", how="left").with_columns(
            pl.col("base_factor").fill_null(1.0)
        )

        fill_df = pl.DataFrame({"dt": fill_dates}).join(pos_template, how="cross")

        # 4. 合并行情并处理缺失值
        fill_df = fill_df.join(
            prices_df, on=["dt", "asset"], how="left"
        ).sort(["asset", "dt"])

        fill_df = fill_df.with_columns([
            pl.col("close").fill_null(strategy="forward").over("asset"),
            pl.col("factor").fill_null(strategy="forward").over("asset"),
        ]).with_columns([
            pl.col("close").fill_null(pl.col("last_mkt_price")).over("asset"),
            pl.col("factor").fill_null(pl.col("base_factor")).over("asset"),
        ])

        # 5. 计算复权变动比例及产生的现金补偿
        # 补偿公式：(new_factor - prev_factor) * shares * close
        fill_df = fill_df.with_columns(
            pl.col("factor").shift(1).over("asset").fill_null(pl.col("base_factor")).alias("prev_factor")
        ).with_columns([
            ((pl.col("factor") - pl.col("prev_factor")) * pl.col("shares") * pl.col("close")).alias("cash_adj"),
            (pl.col("shares") * pl.col("close")).alias("mv") # 新增 mv 列
        ])

        # 6. 汇总每日统计量
        daily_stats = fill_df.group_by("dt").agg([
            pl.col("mv").sum().alias("market_value"),
            pl.col("cash_adj").sum().alias("daily_cash_adj")
        ]).sort("dt")

        # 核心：计算展仓期间每日的现金余额 (累加复权补偿)
        # 每一天的现金 = 初始现金 + 到该日为止的所有复权补偿之和
        daily_stats = daily_stats.with_columns(
            (last_asset.cash + pl.col("daily_cash_adj").cum_sum()).alias("new_cash")
        )

        # 同步更新内存中的现金余额，供展仓后的下一笔交易使用
        self._cash = daily_stats["new_cash"][-1]

        # 7. 批量更新数据库 (资产表和持仓表)
        self._batch_update_history(fill_df, daily_stats, last_asset)

    def _batch_update_history(self, fill_df: pl.DataFrame, daily_stats: pl.DataFrame, last_asset: Asset):
        """执行数据库批量更新"""
        # 批量写入持仓记录
        positions_to_insert = [
            Position(
                portfolio_id=self._portfolio_id,
                dt=row["dt"],
                asset=row["asset"],
                shares=row["shares"],
                price=row["price"],
                avail=row["shares"],
                mv=row["mv"], # 使用 fill_df 中计算好的 mv
                profit=(row["close"] - row["price"]) * row["shares"]
            )
            for row in fill_df.to_dicts()
        ]
        db.upsert_positions(positions_to_insert)

        # 批量写入资产记录 (核心：更新每一天的 cash 字段)
        assets_to_insert = [
            Asset(
                portfolio_id=self._portfolio_id,
                dt=row["dt"],
                principal=last_asset.principal,
                cash=row["new_cash"], # 正确反映每日现金变动
                frozen_cash=last_asset.frozen_cash,
                market_value=row["market_value"],
                total=row["new_cash"] + row["market_value"]
            )
            for row in daily_stats.to_dicts()
        ]
        db.upsert_asset(assets_to_insert)


    def set_clock(self, dt: datetime.datetime) -> None:
        """设置回测时钟。

        在设置回测时钟时，如果 dt 对应的日期与 self._clock 对应的日期不相同，则需要调用 fill_history_gap 以填实跨越的日期 gap。
        """
        if dt < self._bt_start:
            raise ClockBeforeStart(dt, self._bt_start)
        if dt > self._bt_end:
            raise ClockAfterEnd(dt, self._bt_end)
        if dt < self._clock:
            raise ClockRewind(dt, self._clock)

        if not calendar.is_trade_day(self.as_date(dt)):
            self.logger.warning(f"{dt} is not a valid trade day, skip set_clock")
            return

        new_dt = self.as_date(dt)
        old_dt = self.as_date(self._clock)

        if new_dt > old_dt:
            # 1. 执行展仓逻辑，填补从 clock 到 new_dt 前一天的缺失数据
            prev_dt =  calendar.day_shift(new_dt, -1)
            self._fill_history_gaps(prev_dt)

            # 2. 内存状态迁移到新日期 (T+1 规则：所有持仓变为可用)
            for pos in self._positions.values():
                pos.avail = pos.shares
                pos.dt = new_dt

        self._clock = dt

    async def buy(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> TradeResult:
        # 在回测中，timeout 参数无效，直接忽略
        _ = timeout

        assert order_time is not None, "order_time must be present in backtest mode"
        self.set_clock(order_time)

        # 1. 份额必须是 100 的整数倍
        if int(shares) % 100 != 0:
            raise NonMultipleOfLotSize(asset, shares)

        # 2. 获取撮合所需的行情数据
        bars = self._data_feed.get_price_for_match(asset, order_time)
        if bars is None or bars.is_empty():
            self.logger.warning(f"failed to match {asset}, no data at {order_time}")
            raise NoDataForMatch(asset, order_time)

        # 3. 根据报单价格（考虑涨停）和 shares 确定现金是否充足
        # 日线取第一行，分钟线也取第一行作为报单时的参考价
        row = bars.row(0, named=True)
        up_limit = row.get("up_limit", 0.0)

        # 处理 extra 信息
        extra = kwargs.get("extra", {})
        import json
        extra_json = json.dumps(extra) if extra else ""

        # 4. 创建 Order 记录（忠实记录用户请求的份额）
        order = Order(
            portfolio_id=self._portfolio_id,
            asset=asset,
            price= price or up_limit,
            shares=shares,
            side=OrderSide.BUY,
            bid_type=BidType.MARKET if price == 0 else BidType.FIXED,
            tm=order_time,
            extra=extra_json
        )
        db.insert_order(order)

        # 5. 执行撮合
        try:
            if self._match_level == "minute":
                trade = self._match_bid_minute(order, bars)
            else:
                trade =self._match_bid_day(order, bars)

            return TradeResult(order.qtoid, [trade])
        except TradeError as e:
            # 在废单的情况下，没必要返回 Order id，但可以记录状态
            order.status = OrderStatus.JUNK
            order.status_msg = str(e)
            db.insert_order(order)
            raise e

    def _match_bid_day(
        self,
        order: Order,
        bars: pl.DataFrame
    )->Trade:
        """日线撮合逻辑：不考虑成交量，直接全额成交

        Args:
            order: 订单
            bars: 日线行情数据

        Raises:
            InsufficientCash: 资金不足

        """
        row = bars.row(0, named=True)

        up_limit = row.get("up_limit", 0.0)
        bid_price = order.price

        # 用以计算当天的市值
        close = row["close"]

        market_open = datetime.time(9, 30)
        match_price = row["open"] if order.tm.time() <= market_open else row["close"]

        # 成交时间点已涨停，不允许成交
        if up_limit > 0 and match_price >= up_limit:
            self.logger.warning(f"资产 {order.asset} 在 {row['date']} 处于涨停，无法成交")
            raise LimitPrice(order.asset, match_price)

        if bid_price > 0 and bid_price < match_price:
            self.logger.warning(
                f"资产 {order.asset} 委托价 {bid_price} 低于撮合价 {match_price}，无法成交"
            )
            raise PriceNotMeet(order.asset, bid_price, match_price)

        required_cash = order.shares * bid_price * (1 + self._commission)
        if required_cash > self._cash:
            self.logger.info(
                f"委买失败：{order.asset}, 资金({self._cash:.2f})不足以按价格 {bid_price:.2f} 购买 {order.shares} 股。"
            )
            raise InsufficientCash(self._portfolio_name, required_cash, self._cash)

        return self._execute_trade(order, match_price, close=close)


    def _match_bid_minute(
        self,
        order: Order,
        bars: pl.DataFrame
    ) -> Trade:
        """分钟线撮合逻辑：按成交量匹配，并计算出成交均价

        Args:
            order: 订单
            bars: 分钟线行情数据

        Returns:
            成交数量、均价和完成时间
        """
        # feed 保证返回的 bars 一定有数据，否则返回 None
        row = bars.row(0, named=True)
        up_limit = row.get("up_limit", 0.0)

        # 提取当天收盘价，计算市值时使用
        close = bars.row(-1, named=True)["close"]

        bars = self._remove_for_bid(bars, order.price, up_limit)

        if bars.is_empty():
            raise NoDataForMatch(order.asset, order.tm)

        mean_price, filled, tm = self._match_shares(bars, order.shares)

        if filled == 0:
            raise NoDataForMatch(order.asset, order.tm)

        return self._execute_trade(
            order,
            mean_price,
            close=close,
            tm=tm,
            shares=filled,
        )

    def _execute_trade(
        self,
        order: Order,
        price: float,
        close: float,
        tm: datetime.datetime|None = None,
        shares: float | None = None
    )->Trade:
        """执行成交并更新状态

        将根据 order 方法来计算 cash， 创建 Trade， 创建（更新） assets, positions 表格
        Args:
            order: 订单
            price: 成交均价
            close: 当日收盘价
            tm: 成交时间
            shares: 成交数量

        Returns:
            成交结果
        """
        shares = shares or order.shares
        tm = tm or order.tm

        # 1. 创建成交记录
        amount = shares * price

        fee = amount * self._commission

        trade = Trade(
            portfolio_id=self._portfolio_id,
            tid=uuid.uuid4().hex,
            qtoid=order.qtoid,
            foid="",
            asset=order.asset,
            shares=shares,
            price=price,
            amount=amount,
            tm=tm,
            side=order.side,
            cid="",
            fee=fee,
        )
        db.insert_trades(trade)

        # 2. 更新内存中的现金和持仓
        if order.side == OrderSide.BUY:
            self._cash -= amount + fee
            pos = self._positions.get(order.asset)
            if pos:
                total_shares = pos.shares + shares
                new_price = (pos.shares * pos.price + amount) / total_shares
                pos.shares = total_shares
                pos.price = new_price
                pos.profit = 0
                pos.mv = pos.shares * close  # 更新市值为当前收盘价 * 总持仓
                # avail 保持不变
            else:
                self._positions[order.asset] = Position(
                    portfolio_id=self._portfolio_id,
                    dt=self.as_date(tm),
                    asset=order.asset,
                    shares=shares,
                    price=price,
                    avail=0,  # T+1 规则：当日买入的持仓 avail 为 0
                    mv=shares * close,
                    profit=0,
                )
        else:
            self._cash += amount - fee
            pos = self._positions.get(order.asset)
            if pos:
                pos.shares -= shares
                pos.avail -= shares
                pos.profit = 0
                pos.mv = pos.shares * close  # 更新市值为当前收盘价 * 剩余持仓
                if pos.shares <= 0:
                    del self._positions[order.asset]

        # 更新持仓记录
        db.upsert_positions(self._positions.values())

        # 更新 asset 记录
        mv = np.sum([pos.mv for pos in self._positions.values()])
        total = self._cash + mv
        # 更新资产记录
        db.update_asset(
            dt=self.as_date(tm),
            portfolio_id=self._portfolio_id,
            cash=self._cash,
            total=total,
            market_value=mv,
            principal=self._principal,
        )

        return trade

    def market_value(self) -> float:
        """获取当前总市值

        本方法使用内存中的 positions 缓存快速计算，不刷新数据，最终结果为近似值。
        """
        return np.sum([pos.mv for pos in self._positions.values()])

    def total_asset(self)-> float:
        """获取当前总资产

        本方法使用内存中的 positions 缓存与 cash 快速计算，不刷新数据，最终结果为近似值。
        """
        return self._cash + self.market_value()

    async def buy_percent(
        self,
        asset: str,
        percent: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> TradeResult:
        _ = timeout

        if not 0 < percent <= 1:
            raise BadPercent(percent)

        return await self.buy_amount(
            asset, self._cash * percent, price, order_time, timeout, **kwargs
        )

    async def buy_amount(
        self,
        asset: str,
        amount: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> TradeResult:
        _ = timeout
        assert order_time is not None, "order_time must be present in backtest mode"

        # 获取预估价格
        if price > 0:
            est_price = price
        else:
            _, up_limit = self._data_feed.get_trade_price_limits(
                asset, self.as_date(order_time)
            )
            est_price = up_limit

        if est_price <= 0:
            raise NoDataForMatch(asset, order_time)

        shares = amount / (est_price * (1 + self._commission))

        shares = int(shares // 100) * 100
        if shares == 0:
            return TradeResult.empty()

        return await self.buy(
            asset, shares, price, order_time, timeout, **kwargs
        )

    async def sell(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> TradeResult:
        _ = timeout

        assert order_time is not None, "order_time must be present in backtest mode"
        self.set_clock(order_time)

        # 1. 获取撮合所需的行情数据
        bars = self._data_feed.get_price_for_match(asset, order_time)
        if bars is None:
            logger.warning(f"failed to match {asset}, no data at {order_time}")
            raise NoDataForMatch(asset, order_time)

        # 2. 确定基准价格
        row = bars.row(0, named=True)
        down_limit = row.get("down_limit", 0.0)
        ask_price = price or down_limit

        if ask_price == 0:
            logger.warning(f"failed to match {asset}, no valid price at {order_time}")
            raise NoDataForMatch(asset, order_time)

        # 3. 严格校验：份额必须是 100 的整数倍（除非清仓）且有可用持仓
        pos = self._positions.get(asset)
        if pos is None:
            raise InsufficientPosition(asset, shares)

        self._validate_sell_shares(pos, shares)

        # 处理 extra 信息
        extra = kwargs.get("extra", {})
        import json
        extra_json = json.dumps(extra) if extra else ""

        # 4. 创建 Order 记录
        order = Order(
            portfolio_id=self._portfolio_id,
            asset=asset,
            price= ask_price,
            shares=shares,
            side=OrderSide.SELL,
            bid_type=BidType.MARKET if price == 0 else BidType.FIXED,
            tm=order_time,
            extra=extra_json
        )
        db.insert_order(order)

        # 5. 执行撮合
        try:
            if self._match_level == "minute":
                trade = self._match_ask_minute(order, bars)
            else:
                trade = self._match_ask_day(order, bars)

            return TradeResult(order.qtoid, [trade])
        except TradeError as e:
            # 在废单的情况下，没必要返回 Order id
            order.status = OrderStatus.JUNK
            order.status_msg = str(e)
            db.insert_order(order)
            raise e

    def _match_ask_day(
        self,
        order: Order,
        bars: pl.DataFrame
    ) -> Trade:
        """日线卖出撮合逻辑"""
        row = bars.row(0, named=True)

        down_limit = row.get("down_limit", 0.0)
        ask_price = order.price or down_limit

        # 用以计算当天的市值
        close = row["close"]


        market_open = datetime.time(9, 30)
        match_price = row["open"] if order.tm.time() <= market_open else row["close"]

        if down_limit > 0 and match_price <= down_limit:
            logger.warning(f"资产 {order.asset} 在 {row['date']} 处于跌停，无法成交")
            raise LimitPrice(order.asset, match_price)

        if ask_price > 0 and ask_price > match_price:
            logger.warning(
                f"资产 {order.asset} 委托价 {ask_price} 高于撮合价 {match_price}，无法成交"
            )
            raise PriceNotMeet(order.asset, ask_price, match_price)

        return self._execute_trade(order, match_price, close=close)

    def _match_ask_minute(
        self,
        order: Order,
        bars: pl.DataFrame
    ) -> Trade:
        """分钟线撮合逻辑：按成交量匹配"""
        # feed 保证返回的 bars 一定有数据，否则返回 None
        row = bars.row(0, named=True)
        down_limit = row.get("down_limit", 0.0)

        # 提取当天收盘价，计算市值时使用
        close = bars.row(-1, named=True)["close"]

        bars = self._remove_for_ask(bars, order.price, down_limit)
        if bars.is_empty():
            raise NoDataForMatch(order.asset, order.tm)

        mean_price, filled, tm = self._match_shares(bars, order.shares)

        if filled == 0:
            raise NoDataForMatch(order.asset, order.tm)

        return self._execute_trade(
            order,
            mean_price,
            close=close,
            tm=tm,
            shares=filled,
        )

    def _match_shares(self, bars: pl.DataFrame, shares: float)-> tuple[float, float, datetime.datetime]:
        """对分钟线匹配成交量，返回均价，filled 及最后匹配的时间

        Args:
            bars (pl.DataFrame): 已移除掉无法匹配的分钟线的数据
            shares (float): 要匹配的股数

        Returns:
            均价，filled 及最后成交时间
        """
        # 来自 zillionare-backtesting
        c = bars["price"].to_numpy()

        # volume 是手数，转换成股数，以便与 shares 比较
        v = bars["volume"].to_numpy() * 100
        times = bars["tm"].to_list()

        cum_v = np.cumsum(v)

        # until i the order can be filled
        where_total_filled = np.argwhere(cum_v >= shares)
        if len(where_total_filled) == 0:
            i = len(v) - 1
        else:
            i = np.min(where_total_filled)

        # 也许到当天结束，都没有足够的股票
        filled = min(cum_v[i], shares)

        # 最后一周期，只需要成交剩余的部分
        vol = v[: i + 1].copy()
        vol[-1] = filled - np.sum(vol[:-1])

        money = sum(c[: i + 1] * vol)
        mean_price = money / filled

        return mean_price, filled, times[i]

    def _remove_for_bid(
        self, bars: pl.DataFrame, bid_price: float, up_limit: float
    ) -> pl.DataFrame:
        """移除涨停和价格高于委买价的 bar

        用在按分钟线成交时。
        """
        # 涨停判断：close >= up_limit (容差处理)
        return bars.filter(
            (pl.col("close") < up_limit)
            & (pl.col("close") <= bid_price)
            & (pl.col("volume").is_not_null())
            & (pl.col("volume") > 0)
        )

    def _remove_for_ask(
        self, bars: pl.DataFrame, ask_price: float, down_limit: float
    ) -> pl.DataFrame:
        """移除跌停和价格低于委卖价的 bar"""
        return bars.filter(
            (pl.col("close") > down_limit)
            & (pl.col("close") >= ask_price)
            & (pl.col("volume").is_not_null())
            & (pl.col("volume") > 0)
        )

    async def sell_percent(
        self,
        asset: str,
        percent: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> TradeResult:
        """按持仓百分比卖出"""
        _ = timeout

        assert order_time is not None, "order_time must be present in backtest mode"
        # buy/sell会设置时钟

        if not 0 < percent <= 1:
            raise BadPercent(percent)

        pos = self._positions.get(asset)
        if pos is None:
            raise InsufficientPosition(asset, 0)

        # sell will validate shares
        shares = pos.avail * percent

        return await self.sell(
            asset, shares, price, order_time, timeout, **kwargs
        )

    async def sell_amount(
        self,
        asset: str,
        amount: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        method: Literal["ceil", "floor"] = "ceil",
        **kwargs,
    ) -> TradeResult:
        _ = timeout

        assert order_time is not None, "order_time must be present in backtest mode"

        if price > 0:
            est_price = price
        else:
            down_limit, _ = self._data_feed.get_trade_price_limits(
                asset, self.as_date(order_time)
            )
            est_price = down_limit

        # 没提供报价，又未能获得跌停价
        if est_price <= 0:
            raise NoDataForMatch(asset, order_time)

        if method == "ceil":
            shares = math.ceil(amount / est_price / 100) * 100
        else:
            shares = int(amount / est_price / 100) * 100

        if shares == 0:
            return TradeResult.empty()

        return await self.sell(
            asset, shares, est_price, order_time, timeout, **kwargs
        )

    async def trade_target_pct(
        self,
        asset: str,
        target_pct: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5
    ) -> TradeResult:
        """将`asset`仓位调整到占总市值的`target_pct`

        受市值波动影响，以及可能还有其它标的的仓位也要调整，所以最终仓位可能与目标仓位不完全一致。
        如果当前仓位非常接近目标仓位（差别不足一手），则不进行调整，此时返回`TradeResult.empty()`

        Args:
            asset: 资产代码，"symbol.SZ"风格
            target_pct: 目标仓位占比，0-1 之间的浮点数
            price: 委托价格. Defaults to 0.
            order_time: 委托时间.

        Returns:
            TradeResult: 交易结果
        """
        _ = timeout

        total = self.total_asset()

        target_mv = total * target_pct
        pos = self._positions.get(asset)
        if pos is None:
            return await self.buy_amount(asset, target_mv, price, order_time)

        margin = target_mv - pos.mv
        if margin > 0:
            return await self.buy_amount(asset, margin, price, order_time)
        else:
            return await self.sell_amount(asset, -margin, price, order_time, method="floor")

    async def cancel_order(self, qt_oid: str):
        """回测模式下，不支持取消订单"""
        return None

    async def cancel_all_orders(self, side=None):
        """回测模式下，不支持取消订单"""
        return None

    def get_history(
        self,
        asset: str,
        count: int,
        end_dt: datetime.datetime | None = None,
        frame_type: str = "1d",
        skip_suspended: bool = True,
        fill_value: bool = True,
    ) -> pl.DataFrame:
        if frame_type != "1d":
            # 目前只支持日线，后续可扩展
            raise NotImplementedError("BacktestBroker currently only supports 1d history")

        end_date = self.as_date(end_dt) if end_dt else self.as_date(self._clock)

        # 使用 daily_bars 获取历史数据
        return daily_bars.get_bars(
            n=count,
            end=end_date,
            assets=[asset],
            adjust="qfq"
        )
