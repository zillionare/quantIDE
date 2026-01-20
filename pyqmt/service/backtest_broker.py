import datetime
import uuid
from typing import Literal

import numpy as np
import polars as pl
from loguru import logger

from pyqmt.core.enums import BidType, BrokerKind, FrameType, OrderSide
from pyqmt.core.errors import (
    CashError,
    NoDataForMatch,
    TradeError,
    TradeErrors,
    VolumeNotMeet,
)
from pyqmt.data.models.calendar import calendar
from pyqmt.data.models.daily_bars import daily_bars
from pyqmt.data.sqlite import Asset, Order, Portfolio, Position, Trade, db
from pyqmt.service.abstract_broker import AbstractBroker
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
        self._bt_start: datetime.date = bt_start
        self._bt_end: datetime.date = bt_end
        self._bt_stopped: bool = False
        self._desc: str = desc

        # 回测时钟，初始化为 bt_start 的前一个交易日 (Day 0)
        self._clock: datetime.date | datetime.datetime = calendar.day_shift(
            self.as_date(bt_start), -1
        )

        # 初始化回测数据记录
        self.init_backtest()

        self._pending: dict[str, list[str]] = {}
        self._positions: dict[str, Position] = {}
        self._last_snapshot_day: datetime.date | None = None

    def init_backtest(self) -> None:
        """创建回测相关数据记录

        在调用本方法时，数据库中不应该存在与本 portfolio 相关的 portfolios 和 assets 记录。
        """
        # 检查 portfolio 是否已存在
        if db.get_portfolio(self._portfolio_id):
            raise TradeError(
                TradeErrors.ERROR_ALREADY_EXISTS,
                f"Portfolio {self._portfolio_id} already exists",
            )

        # 检查 assets 表中是否已有该 portfolio 的记录
        if not db.query_assets(self._portfolio_id).is_empty():
            raise TradeError(
                TradeErrors.ERROR_ALREADY_EXISTS,
                f"Assets for portfolio {self._portfolio_id} already exist",
            )

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
        """
        # 1. 设置时钟到 bt_end，这会触发补齐逻辑，确保当前 positions 是最新的
        self.set_clock(self._bt_end)

        # 2. 遍历所有持仓并卖出，先将下单时间设置为 bt_end 15:00
        bid_time = datetime.datetime.combine(
            self._bt_end, datetime.time(15, 0)
        )
        # 获取当前所有持仓资产
        assets_to_sell = [a for a, p in self._positions.items() if p.shares > 0]

        for asset in assets_to_sell:
            pos = self._positions[asset]
            # 执行卖出。使用市价单 (price=0) 在 bid_time (15:00) 撮合，将以收盘价成交
            await self.sell(asset=asset, shares=pos.shares, price=0, bid_time=bid_time)

        # 3. 标记停止并更新数据库状态
        # 记录最后一天的日终快照
        self._write_daily_snapshot(self.as_date(self._clock))

        self._bt_stopped = True
        db.update_portfolio(self._portfolio_id, status=False)

    def _fill_history_gaps(self, old_dt: datetime.date, new_dt: datetime.date) -> None:
        """填补历史数据缺失，确保资产记录和持仓记录完整"""
        # 1. 获取需要填补的日期范围
        latest_asset = db.get_asset(portfolio_id=self._portfolio_id)
        start_fill = calendar.day_shift(latest_asset.dt, 1) if latest_asset else old_dt
        end_fill = calendar.day_shift(new_dt, -1)

        fill_dates = calendar.get_frames(start_fill, end_fill, FrameType.DAY)
        if not fill_dates:
            return

        # 过滤已存在的记录
        existing_assets = db.query_assets(self._portfolio_id, start_fill, end_fill)
        if not existing_assets.is_empty():
            exist_set = set(existing_assets["dt"].to_list())
            fill_dates = [d for d in fill_dates if d not in exist_set]
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

        # 调试输出
        # print(f"DEBUG: fill_df before fill_null:\n{fill_df}")

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

    def _get_bar(self, dt: datetime.date, asset: str) -> dict | None:
        df = daily_bars.get_bars_in_range(
            dt, dt, assets=[asset], adjust=None, eager_mode=True
        )
        if df.height == 0:
            return None
        row = df.row(0, named=True)
        if isinstance(row["date"], datetime.datetime):
            row["date"] = row["date"].date()
        return row

    def set_clock(self, dt: datetime.datetime | datetime.date) -> None:
        """设置回测时钟。

        在设置回测时钟时，如果 dt 对应的日期与 self._clock 对应的日期不相同，则需要调用 fill_history_gap 以填实跨越的日期 gap。
        """
        new_dt = self.as_date(dt)
        old_dt = self.as_date(self._clock)

        if new_dt > old_dt:
            # 1. 既然要跨天了，先给 old_dt 存个档（日终快照）
            # 只有在不是 init_backtest 后的第一个 set_clock 时才需要快照
            # 因为 init_backtest 已经手动创建了 Day 0 的 asset 记录
            latest_asset = db.get_asset(dt=old_dt, portfolio_id=self._portfolio_id)
            if not latest_asset:
                self._write_daily_snapshot(old_dt)

            # 2. 执行展仓逻辑，填补从 old_dt 到 new_dt 之间的空隙
            self._fill_history_gaps(old_dt, new_dt)

            # 3. 内存状态迁移到新日期 (T+1 规则：所有持仓变为可用)
            for pos in self._positions.values():
                pos.avail = pos.shares
                pos.dt = new_dt

        self._clock = dt

    async def buy(
        self,
        asset: str,
        shares: int | float,
        portfolio: str = "",
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        strategy: str = "",
        timeout: float = 0.5,
    ) -> pl.DataFrame | None:
        if bid_time is None:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "bid_time must be provided")
        if self._bt_stopped:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "backtest not started")

        self.set_clock(bid_time)

        # 1. 获取撮合所需的行情数据
        bars = self._data_feed.get_price_for_match(asset, bid_time)
        if bars is None:
            logger.warning(f"failed to match {asset}, no data at {bid_time}")
            raise NoDataForMatch(asset, bid_time)

        # 2. 确定基准价格（用于初步份额校验）
        # 日线取第一行，分钟线也取第一行作为报单时的参考价
        row = bars.row(0, named=True)
        up_limit = row.get("up_limit", 0.0)
        bid_price = float(price) or up_limit
        if self._match_level == "day":
            market_open = datetime.time(9, 30)
            match_price = (
                row["open"] if bid_time.time() <= market_open else row["close"]
            )
            bid_price = float(price) if price > 0 else match_price

        if not bid_price:
            logger.warning(f"failed to match {asset}, no valid price at {bid_time}")
            raise NoDataForMatch(asset, bid_time)

        # 3. 严格校验：份额必须是 100 的整数倍且资金充足
        if int(shares) % 100 != 0:
            raise TradeError(
                TradeErrors.ERROR_BAD_PARAMS,
                f"无效的参数: shares必须是100的整数倍: {shares}",
            )

        required_cash = shares * bid_price * (1 + self._commission)
        if required_cash > self._cash:
            logger.info(
                f"委买失败：{asset}, 资金({self._cash:.2f})不足以按价格 {bid_price:.2f} 购买 {shares} 股。"
            )
            raise CashError(self._portfolio_name, required_cash, self._cash)

        # 4. 创建 Order 记录（忠实记录用户请求的份额）
        order = Order(
            portfolio_id=portfolio or self._portfolio_id,
            asset=asset,
            price=float(price),
            shares=shares,
            side=OrderSide.BUY,
            bid_type=BidType.MARKET if price == 0 else BidType.FIXED,
            tm=bid_time,
            strategy=strategy,
        )
        qtoid = db.insert_order(order)

        # 5. 执行撮合
        if self._match_level == "minute":
            match_bars = self._remove_for_buy(bars, bid_price, up_limit)
            filled, _avg_price, tm = self._match_bid_minute(
                match_bars, shares, asset, bid_time
            )
        else:
            filled = self._match_bid_day(order, bars, shares, bid_price, up_limit)

        if filled == 0:
            raise VolumeNotMeet(asset, bid_price)

        return db.query_trade(qtoid=qtoid)

    def _match_bid_day(
        self,
        order: Order,
        bars: pl.DataFrame,
        shares: float,
        bid_price: float,
        up_limit: float,
    ) -> float:
        """日线撮合逻辑：不考虑成交量，直接全额成交"""
        row = bars.row(0, named=True)
        market_open = datetime.time(9, 30)
        match_price = row["open"] if order.tm.time() <= market_open else row["close"]
        if up_limit > 0 and match_price >= up_limit:
            logger.warning(f"资产 {order.asset} 在 {row['date']} 处于涨停，无法成交")
            return 0.0

        if bid_price > 0 and bid_price < match_price:
            logger.warning(
                f"资产 {order.asset} 委托价 {bid_price} 低于撮合价 {match_price}，无法成交"
            )
            return 0.0

        self._execute_trade(order, match_price, row["date"], shares=shares)
        return shares

    def _match_bid_minute(
        self,
        bars: pl.DataFrame,
        shares: float,
        asset: str,
        tm: datetime.datetime,
    ) ->  tuple[float, float, datetime.datetime]:
        """分钟线撮合逻辑：按成交量匹配，并计算出成交均价

        Args:
            bars: 分钟线行情数据
            shares: 委托数量
            asset: 资产代码
            tm: 订单时间

        Returns:
            成交数量、均价和完成时间
        """
        if bars.is_empty():
            logger.warning("match bars is empty, 撮合失败")
            raise NoDataForMatch(asset, tm)

        c = bars["price"].to_numpy()
        v = bars["volume"].to_numpy()
        times = bars["tm"].to_numpy()

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

    def _execute_trade(
        self,
        order: Order,
        price: float,
        tm: datetime.datetime,
        shares: float | None = None,
    ):
        """执行成交并更新状态"""
        # 1. 创建成交记录
        if shares is None:
            shares = order.shares
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
                new_shares = pos.shares + shares
                new_price = (pos.shares * pos.price + amount) / new_shares
                pos.shares = new_shares
                pos.price = new_price
                pos.profit = 0
                pos.mv = pos.shares * price  # 更新市值为当前成交价 * 总持仓
            else:
                self._positions[order.asset] = Position(
                    portfolio_id=self._portfolio_id,
                    dt=self.as_date(tm),
                    asset=order.asset,
                    shares=shares,
                    price=price,
                    avail=0,  # T+1 规则：当日买入的持仓 avail 为 0
                    mv=amount,
                    profit=0,
                )
        else:
            self._cash += amount - fee
            pos = self._positions.get(order.asset)
            if pos:
                pos.shares -= shares
                pos.avail -= shares
                pos.profit = 0
                pos.mv = pos.shares * price  # 更新市值为当前成交价 * 剩余持仓
                if pos.shares <= 0:
                    del self._positions[order.asset]

    def _write_daily_snapshot(self, dt: datetime.date):
        """记录指定日期的日终快照到数据库"""
        self._last_snapshot_day = dt
        # 1. 批量获取持仓资产的当前价格
        assets = list(self._positions.keys())
        prices = {}
        if assets:
            df = daily_bars.get_bars_in_range(dt, dt, assets=assets, eager_mode=True)
            prices = {row["asset"]: row["close"] for row in df.to_dicts()}

        # 2. 获取上一个交易日的持仓（用于行情缺失时的市值参考）
        prev_positions = {}
        missing_assets = [a for a in assets if a not in prices]
        if missing_assets:
            prev_dt = calendar.day_shift(dt, -1)
            if prev_dt:
                # 从数据库中查询上一日的持仓记录
                prev_pos_df = db.get_positions(
                    dt=prev_dt, portfolio_id=self._portfolio_id
                )
                if not prev_pos_df.is_empty():
                    prev_positions = {
                        row["asset"]: row for row in prev_pos_df.to_dicts()
                    }

        # 3. 更新持仓并计算市值
        positions_to_upsert = []
        for asset, pos in self._positions.items():
            pos.dt = dt
            if asset in prices:
                # 优先使用当日收盘价
                pos.mv = pos.shares * prices[asset]
            elif asset in prev_positions:
                # 行情缺失时，使用昨日单价 (mv/shares) * 今日股数
                prev_p = prev_positions[asset]
                if prev_p["shares"] > 0:
                    last_price = prev_p["mv"] / prev_p["shares"]
                    pos.mv = pos.shares * last_price
                else:
                    # 如果昨日股数为0（可能是昨日刚清仓今日又买入，但今日无行情），回退到成本价
                    # 由于 buy 逻辑已增加行情预检，理论上不应出现此情况，除非数据异常
                    pos.mv = pos.shares * pos.price
                    logger.error(
                        f"日期 {dt} 资产 {asset} 严重数据异常：缺失行情且昨日股数为0，临时使用成本价 {pos.price:.2f} 估值"
                    )
            else:
                # 既无当日行情也无昨日记录（可能是新买入且无行情）
                # 由于 buy 逻辑已增加行情预检，理论上不应出现此情况，除非数据异常
                pos.mv = pos.shares * pos.price
                logger.error(
                    f"日期 {dt} 资产 {asset} 严重数据异常：既无当日行情也无历史记录，临时使用成本价 {pos.price:.2f} 估值"
                )

            positions_to_upsert.append(pos)

        if positions_to_upsert:
            db.upsert_positions(positions_to_upsert)

        # 更新资产
        market_value = sum(pos.mv for pos in self._positions.values())
        asset_info = Asset(
            portfolio_id=self._portfolio_id,
            dt=dt,
            principal=self._principal,
            cash=self._cash,
            frozen_cash=0,
            market_value=market_value,
            total=self._cash + market_value,
        )
        db.upsert_asset([asset_info])

    async def buy_percent(
        self,
        asset: str,
        percent: float,
        portfolio: str = "",
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        strategy: str = "",
        timeout: float = 0.5,
    ) -> pl.DataFrame | None:
        """按账户总资产百分比买入"""
        if bid_time is None:
            bid_time = self._clock

        # 简单实现：根据当前总资产计算金额，再调用 buy
        asset_info = db.get_asset(self.as_date(bid_time), self._portfolio_id)
        if not asset_info:
            # 如果当天还没 snapshot，取内存中的最新状态估算
            market_value = sum(pos.mv for pos in self._positions.values())
            total_assets = self._cash + market_value
        else:
            total_assets = asset_info.total

        target_amount = total_assets * percent
        return await self.buy_amount(
            asset, target_amount, portfolio, price, bid_time, strategy, timeout
        )

    async def buy_amount(
        self,
        asset: str,
        amount: float,
        portfolio: str = "",
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        strategy: str = "",
        timeout: float = 0.5,
    ) -> pl.DataFrame | None:
        """按金额买入"""
        if bid_time is None:
            bid_time = self._clock

        # 获取预估价格
        if price > 0:
            est_price = price
        else:
            _, up_limit = self._data_feed.get_trade_price_limits(
                asset, self.as_date(bid_time)
            )
            est_price = up_limit

        if est_price <= 0:
            raise NoDataForMatch(asset, bid_time)

        shares = amount / (est_price * (1 + self._commission))
        return await self.buy(
            asset, shares, portfolio, price, bid_time, strategy, timeout
        )

    async def sell(
        self,
        asset: str,
        shares: int | float,
        portfolio: str = "",
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        strategy: str = "",
        timeout: float = 0.5,
    ) -> pl.DataFrame | None:
        if bid_time is None:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "bid_time must be provided")
        if self._bt_stopped:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "backtest not started")

        self.set_clock(bid_time)

        # 1. 获取撮合所需的行情数据
        bars = self._data_feed.get_price_for_match(asset, bid_time)
        if bars is None or (isinstance(bars, pl.DataFrame) and bars.is_empty()):
            logger.warning(f"failed to match {asset}, no data at {bid_time}")
            raise NoDataForMatch(asset, bid_time)

        # 2. 确定基准价格
        row = bars.row(0, named=True)
        down_limit = row.get("down_limit", 0.0)
        bid_price = float(price) or down_limit

        if not bid_price:
            logger.warning(f"failed to match {asset}, no valid price at {bid_time}")
            raise NoDataForMatch(asset, bid_time)

        # 3. 严格校验：份额必须是 100 的整数倍（除非清仓）且有可用持仓
        pos = self._positions.get(asset)
        if not pos or pos.avail <= 0:
            logger.warning(f"委卖失败：{asset}, 无可用持仓")
            return pl.DataFrame()

        # 忠实记录请求的份额，不满足规则则拒绝
        if shares > pos.avail:
            logger.warning(
                f"委卖失败：{asset}, 请求卖出 {shares} 股超过可用持仓 {pos.avail} 股"
            )
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, f"可用持仓不足: {asset}")

        # 除非清仓，否则必须是 100 的整数倍
        if shares < pos.avail and int(shares) % 100 != 0:
            raise TradeError(
                TradeErrors.ERROR_BAD_PARAMS,
                f"无效的参数: 非清仓卖出必须是100的整数倍: {shares}",
            )

        # 4. 创建 Order 记录
        order = Order(
            portfolio_id=portfolio or self._portfolio_id,
            asset=asset,
            price=float(price),
            shares=shares,
            side=OrderSide.SELL,
            bid_type=BidType.MARKET if price == 0 else BidType.FIXED,
            tm=bid_time,
            strategy=strategy,
        )
        qtoid = db.insert_order(order)

        # 5. 执行撮合
        if self._match_level == "minute":
            filled = self._match_ask_minute(order, bars, shares, bid_price, down_limit)
        else:
            filled = self._match_ask_day(order, bars, shares, bid_price, down_limit)

        if filled == 0:
            raise VolumeNotMeet(asset, bid_price)

        return db.query_trade(qtoid=qtoid)

    def _match_ask_day(
        self,
        order: Order,
        bars: pl.DataFrame,
        shares: float,
        bid_price: float,
        down_limit: float,
    ) -> float:
        """日线卖出撮合逻辑"""
        row = bars.row(0, named=True)
        market_open = datetime.time(9, 30)
        match_price = row["open"] if order.tm.time() <= market_open else row["close"]
        if down_limit > 0 and match_price <= down_limit:
            logger.warning(f"资产 {order.asset} 在 {row['date']} 处于跌停，无法成交")
            return 0.0

        if bid_price > 0 and bid_price > match_price:
            logger.warning(
                f"资产 {order.asset} 委托价 {bid_price} 高于撮合价 {match_price}，无法成交"
            )
            return 0.0

        self._execute_trade(order, match_price, row["date"], shares=shares)
        return shares

    def _match_ask_minute(
        self,
        order: Order,
        bars: pl.DataFrame,
        shares: float,
        bid_price: float,
        down_limit: float,
    ) -> float:
        """分钟线撮合逻辑：按成交量匹配"""
        match_bars = self._remove_for_sell(bars, bid_price, down_limit)
        if match_bars.is_empty():
            logger.warning(
                f"资产 {order.asset} 在 {order.tm} 处于跌停或价格低于委托价，无法成交"
            )
            return 0.0

        remaining_shares = shares
        total_filled = 0
        for row in match_bars.to_dicts():
            match_price = row["close"]
            volume = row.get("volume", float("inf"))
            if volume is None or volume <= 0:
                volume = float("inf")

            fill_shares = min(remaining_shares, volume)
            if fill_shares <= 0:
                continue

            self._execute_trade(order, match_price, row["date"], shares=fill_shares)
            total_filled += fill_shares
            remaining_shares -= fill_shares
            if remaining_shares <= 0:
                break
        return total_filled

    def _remove_for_buy(
        self, bars: pl.DataFrame, bid_price: float, up_limit: float
    ) -> pl.DataFrame:
        """移除涨停和价格高于委买价的 bar"""
        # 涨停判断：close >= up_limit (容差处理)
        return bars.filter(
            (pl.col("close") < up_limit)
            & (pl.col("close") <= bid_price)
            & (pl.col("volume").is_not_null())
            & (pl.col("volume") > 0)
        )

    def _remove_for_sell(
        self, bars: pl.DataFrame, bid_price: float, down_limit: float
    ) -> pl.DataFrame:
        """移除跌停和价格低于委卖价的 bar"""
        return bars.filter(
            (pl.col("close") > down_limit)
            & (pl.col("close") >= bid_price)
            & (pl.col("volume").is_not_null())
            & (pl.col("volume") > 0)
        )

    async def sell_percent(
        self,
        asset: str,
        percent: float,
        portfolio: str = "",
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        strategy: str = "",
        timeout: float = 0.5,
    ) -> pl.DataFrame | None:
        """按持仓百分比卖出"""
        pos = self._positions.get(asset)
        if not pos:
            return None
        shares = pos.avail * percent
        return await self.sell(
            asset, shares, portfolio, price, bid_time, strategy, timeout
        )

    async def sell_amount(
        self,
        asset: str,
        amount: int | float,
        portfolio: str,
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "unsupported")

    def cancel_order(self, qtoid: str) -> None:
        """撤单：回测模式下暂不支持中途撤单"""
        pass

    def cancel_all_orders(self, side: OrderSide | None = None) -> None:
        """撤单：回测模式下暂不支持中途撤单"""
        pass

    def trade_target_pct(
        self,
        asset: str,
        target_pct: float,
        portfolio: str,
        price: float = 0,
        bid_type: BidType = BidType.MARKET,
    ) -> list[Trade]:
        raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "unsupported")

    def get_position(self, asset: str) -> dict:
        pos = self._positions.get(asset)
        if pos is None:
            return {"asset": asset, "shares": 0.0, "avail": 0.0}
        return {"asset": asset, "shares": float(pos.shares), "avail": float(pos.avail)}

    def get_account_info(self, date: datetime.date | None = None) -> bytes:
        dt = date or (self._last_snapshot_day or self._bt_start)
        a = db.get_asset(dt, self.portfolio_id)
        if a is None:
            # 如果没找到，尝试获取最近的一个
            a = db._get_latest_asset(self.portfolio_id)

        positions = [
            (k, float(p.shares), float(p.avail), float(p.price), float(p.mv))
            for k, p in self._positions.items()
        ]
        result = {
            "name": self.portfolio_name,
            "principal": float(self._principal),
            "assets": float(a.total) if a else float(self._principal),
            "start": self._bt_start,
            "last_trade": dt,
            "end": self._bt_end,
            "available": float(a.cash) if a else float(self._cash),
            "market_value": float(a.market_value) if a else 0.0,
            "positions": positions,
        }
        return pl.DataFrame([result]).to_pandas().to_json().encode("utf-8")

    def bills(self) -> dict:
        """获取本组合的所有账单记录"""
        trades = db.trades_all()
        if trades is not None and trades.height > 0:
            trades = trades.filter(
                pl.col("portfolio_id") == self._portfolio_id
            ).to_dicts()
        else:
            trades = []

        assets = db.assets_all(portfolio_id=self._portfolio_id)
        if assets is not None and assets.height > 0:
            assets = assets.to_dicts()
        else:
            assets = []

        return {
            "trades": trades,
            "positions": [
                {"asset": a, "shares": float(p.shares), "avail": float(p.avail)}
                for a, p in self._positions.items()
            ],
            "assets": assets,
        }
