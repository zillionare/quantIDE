"""仿真交易 Broker 实现。

本模块实现了 SimulationBroker，用于仿真交易。它订阅实时行情，维护账户状态，并模拟撮合交易。
"""

import asyncio
import datetime
import threading
from collections import defaultdict
from typing import Any, List

from loguru import logger

from pyqmt.core.enums import BidType, BrokerKind, OrderSide, OrderStatus, Topics
from pyqmt.core.errors import (
    InsufficientCash,
    InsufficientPosition,
    NonMultipleOfLotSize,
    TradeError,
)
from pyqmt.core.message import msg_hub
from pyqmt.data.sqlite import Asset, Order, Portfolio, Position, Trade, db
from pyqmt.service.abstract_broker import AbstractBroker
from pyqmt.service.base_broker import TradeResult
from pyqmt.service.livequote import live_quote


class SimulationBroker(AbstractBroker):
    """仿真交易 Broker。

    SimulationBroker 模拟真实的交易环境，订阅实时行情，并在本地进行撮合。
    它维护自己的账户状态（现金、持仓），并将交易记录保存到数据库。
    """

    def __init__(
        self,
        portfolio_id: str,
        principal: float = 1_000_000,
        commission: float = 1e-4,
        portfolio_name: str = "simulation",
        info: str = "",
    ):
        """初始化 SimulationBroker。

        Args:
            portfolio_id: 账户ID
            principal: 初始资金
            commission: 佣金费率
            portfolio_name: 账户名称
            info: 账户描述信息
        """
        super().__init__(
            portfolio_id=portfolio_id,
            principal=principal,
            commission=commission,
            kind=BrokerKind.SIMULATION,
            portfolio_name=portfolio_name,
            info=info,
        )
        self._active_orders = defaultdict(list)
        self._positions: dict[str, Position] = {}
        # 缓存订单的成交记录，用于在完全成交或取消时返回完整结果
        self._order_trades: dict[str, list[Trade]] = defaultdict(list)

        # 初始化或加载状态
        self._init_or_sync_state()

        # 订阅行情
        msg_hub.subscribe(Topics.QUOTES_ALL.value, self._on_quote_update)

    def _validate_data_consistency(self):
        """校验数据一致性。

        检查数据库中的 portfolio, asset, position 记录是否一致。

        Raises:
            RuntimeError: 如果发现数据不一致
        """
        pf = db.get_portfolio(self.portfolio_id)
        asset = db.get_asset(dt=None, portfolio_id=self.portfolio_id)
        positions_df = db.get_positions(dt=None, portfolio_id=self.portfolio_id)

        if pf is None:
            if asset is not None:
                raise RuntimeError(
                    f"Data Inconsistency: Portfolio {self.portfolio_id} missing but has asset records. Please clean up the database."
                )
            if not positions_df.is_empty():
                raise RuntimeError(
                    f"Data Inconsistency: Portfolio {self.portfolio_id} missing but has position records. Please clean up the database."
                )
        else:
            if asset is None:
                raise RuntimeError(
                    f"Data Inconsistency: Portfolio {self.portfolio_id} exists but has no asset records. Please clean up the database."
                )

    def _init_or_sync_state(self):
        """初始化或同步账户状态。

        如果在数据库中找不到对应的 portfolio，则创建新的。
        如果存在，则从数据库加载资产和持仓信息。
        """
        # 数据一致性检查
        self._validate_data_consistency()

        # 检查是否已有 portfolio 记录
        pf = db.get_portfolio(self.portfolio_id)
        # 检查资产记录
        asset = db.get_asset(dt=None, portfolio_id=self.portfolio_id)

        if not pf:
            # 创建新的 portfolio
            pf = Portfolio(
                portfolio_id=self.portfolio_id,
                kind=BrokerKind.SIMULATION,
                start=datetime.date.today(),
                name=self.portfolio_name,
                info=self.info,
                status=True,
            )
            db.insert_portfolio(pf)

        if asset:
            self._cash = asset.cash
            self._principal = asset.principal
        else:
            # 初始化资产
            asset = Asset(
                portfolio_id=self.portfolio_id,
                dt=datetime.date.today(),
                principal=self._principal,
                cash=self._principal,
                frozen_cash=0,
                market_value=0,
                total=self._principal,
            )
            db.upsert_asset(asset)
            self._cash = self._principal

        # 同步持仓
        # 注意：这里需要加载最新的持仓，而不是所有历史记录
        # 我们假设数据库中 dt 是日期类型
        today = datetime.date.today()
        # 尝试加载今天的持仓（如果有的话，比如重启）
        positions_df = db.get_positions(dt=today, portfolio_id=self.portfolio_id)

        if positions_df.is_empty():
            # 如果今天没有记录，尝试查找最近的一个交易日的记录
            # db.get_positions(dt=None) 会自动返回最新日期的持仓
            positions_df = db.get_positions(dt=None, portfolio_id=self.portfolio_id)

        if not positions_df.is_empty():
            for row in positions_df.to_dicts():
                pos = Position(**row)
                self._positions[pos.asset] = pos

    @property
    def total_assets(self) -> float:
        """计算当前总资产（现金 + 持仓市值）。

        Returns:
            当前总资产
        """
        market_value = sum(p.mv for p in self._positions.values())
        return self._cash + market_value

    @property
    def positions(self) -> List[Position]:
        """获取当前持仓列表。"""
        return list(self._positions.values())

    @property
    def cash(self) -> float:
        return self._cash

    @property
    def principal(self) -> float:
        return self._principal

    def _on_quote_update(self, data: dict):
        """行情更新回调，执行撮合逻辑。

        本方法将在 MessageHub 线程中运行。更新持仓市值，并尝试撮合待处理订单。

        !!! warning
            本方法假设 quote 的成交量可以全部匹配给待成交订单。在实际交易中，只有你的订单在时间上是最早的，或者价格是最低的，才会是这样。如果 quote 没有提供成交量，则只要价格符合条件，就会全部成交。

        Args:
            data: 行情数据，格式为 {asset: quote_info}
        """
        # 这里的 data 是 {asset: quote_info}
        # 在 MessageHub 线程中运行
        with self._lock:
            # 1. 更新内存中的持仓市值 (仅用于内存计算，不触发写库)
            # Broker 需要维护实时的 total_assets 以支持按比例下单等风控逻辑
            for asset, pos in self._positions.items():
                quote = data.get(asset)
                if quote:
                    last_price = quote.get("lastPrice", 0)
                    if last_price > 0:
                        pos.mv = pos.shares * last_price
                        pos.profit = pos.mv - (pos.shares * pos.price)

            if not self._active_orders:
                return

            # 2. 撮合订单
            # traded_assets 仅记录因交易导致持仓数量或成本变化的资产
            # 只有这些资产需要同步到数据库
            traded_assets = set()

            # 遍历所有待处理订单的资产
            # 使用 list() 复制 keys，因为可能会在迭代中修改字典
            for asset in list(self._active_orders.keys()):
                if asset not in data:
                    continue

                quote = data[asset]
                orders = self._active_orders[asset]

                # 初始化剩余成交量
                # 如果 quote 中没有 volume 字段，则假设成交量无限大（只要价格满足条件即可全额成交）
                remaining_shares = float('inf')
                if "volume" in quote:
                    remaining_shares = quote["volume"] * 100

                # 撮合该资产的所有订单
                remaining_orders = []
                for order in orders:
                    # 如果剩余量<=0，跳过撮合
                    if remaining_shares <= 0:
                        remaining_orders.append(order)
                        continue

                    matched_shares, trade = self._try_match(order, quote, available_shares_limit=remaining_shares)
                    if matched_shares > 0 and trade:
                        # 更新剩余成交量
                        remaining_shares -= matched_shares

                        # 撮合成功（部分或全部）
                        order.filled += matched_shares
                        self._order_trades[order.qtoid].append(trade)

                        # 立即持久化成交记录
                        db.insert_trades(trade)

                        # 应用成交到内存状态 (Cash, Shares, Cost)
                        self._apply_trade_to_portfolio(trade)

                        # 因为持仓数量变了，立即更新内存中的 MV 和 Profit
                        # 确保后续逻辑（如 total_assets）读取到的是最新状态
                        pos = self._positions.get(asset)
                        if pos:
                            pos.mv = pos.shares * quote["lastPrice"]
                            pos.profit = pos.mv - (pos.shares * pos.price)
                            traded_assets.add(asset)

                        if order.filled >= order.shares:
                             # 完全成交
                             order.status = OrderStatus.SUCCEEDED
                             # 更新 Order 状态到 DB
                             db.update_order(order.qtoid, status=order.status.value, filled=order.filled)

                             # 唤醒等待的协程，返回所有成交记录
                             all_trades = self._order_trades.pop(order.qtoid)
                             self.awake(order.qtoid, TradeResult(order.qtoid, all_trades))
                        else:
                             # 部分成交
                             order.status = OrderStatus.PART_SUCC
                             # 更新 Order 状态到 DB
                             db.update_order(order.qtoid, status=order.status.value, filled=order.filled)
                             remaining_orders.append(order)
                    else:
                        remaining_orders.append(order)

                if not remaining_orders:
                    del self._active_orders[asset]
                else:
                    self._active_orders[asset] = remaining_orders

            # 3. 持久化 (仅在发生交易时)
            # 我们只关心 Shares, Cost 的持久化。
            # Asset (Cash, MV, Total) 可以在日终清算时统一更新，盘中交易不实时更新 Asset 表
            # 但是为了 crash recovery，必须更新 Asset 表以保存 Cash 状态
            if traded_assets:
                today = datetime.date.today()

                # 1. 持久化 Positions
                to_upsert = [self._positions[a] for a in traded_assets]
                for p in to_upsert:
                    p.dt = today
                db.upsert_positions(to_upsert)

                # 2. 持久化 Asset (Cash)
                # 计算当前总市值
                market_value = sum(p.mv for p in self._positions.values())
                asset = Asset(
                    portfolio_id=self.portfolio_id,
                    dt=today,
                    principal=self._principal,
                    cash=self._cash,
                    frozen_cash=0, # 简化处理，暂不追踪冻结资金
                    market_value=market_value,
                    total=self._cash + market_value
                )
                db.upsert_asset(asset)

    def _try_match(self, order: Order, quote: dict, available_shares_limit: float = float('inf')) -> tuple[float, Trade | None]:
        """尝试撮合单个订单。

        检查价格、涨跌停限制等条件，如果满足则生成成交记录。
        如果 quote 中包含 volume 字段（单位：手），则限制成交数量。

        Args:
            order: 待撮合的订单
            quote: 该资产的最新行情
            available_shares_limit: 可用成交量限制（股）。默认为无限大。

        Returns:
            (matched_shares, trade) 元组。
            matched_shares > 0 表示撮合成功的部分或全部数量。
            matched_shares == 0 表示撮合失败。
            trade 为生成的成交记录，如果 matched_shares == 0 则为 None。
        """
        last_price = quote.get("lastPrice", 0)
        if last_price <= 0:
            return 0.0, None

        # 检查涨跌停
        # 使用 live_quote 获取最新的涨跌停数据，而不是依赖 quote 中的字段
        down_limit, up_limit = live_quote.get_price_limits(order.asset)

        # 严格规则：涨停不买，跌停不卖
        # 注意：如果是新股等无涨跌停限制的情况（limit=0），则允许交易
        if order.side == OrderSide.BUY and up_limit > 0 and last_price >= up_limit:
            return 0.0, None
        if order.side == OrderSide.SELL and down_limit > 0 and last_price <= down_limit:
            return 0.0, None

        # 价格判断
        match_price = last_price
        if order.bid_type == BidType.FIXED:
            if order.side == OrderSide.BUY and order.price < last_price:
                return 0.0, None
            if order.side == OrderSide.SELL and order.price > last_price:
                return 0.0, None
            # 限价单成交价逻辑：
            # 买入：如果 last_price <= price，以 last_price 成交（更优价）
            # 卖出：如果 last_price >= price，以 last_price 成交

        # 计算成交数量
        needed_shares = order.shares - order.filled

        # 限制成交量
        matched_shares = min(needed_shares, available_shares_limit)

        if matched_shares <= 0:
            return 0.0, None

        # 生成成交记录
        trade = Trade(
            portfolio_id=self.portfolio_id,
            tid=f"t_{order.qtoid}_{int(order.filled + matched_shares)}", # 简单生成 tid
            qtoid=order.qtoid,
            foid="",
            asset=order.asset,
            shares=matched_shares,
            price=match_price,
            amount=match_price * matched_shares,
            tm=datetime.datetime.now(),
            side=order.side,
            cid="",
            fee=0 # 暂不计算手续费
        )
        # 计算手续费
        trade.fee = self._calculate_commission(trade.amount)
        return matched_shares, trade

    def _calculate_commission(self, amount: float) -> float:
        """计算手续费。

        Args:
            amount: 成交金额

        Returns:
            手续费
        """
        return max(5.0, amount * self._commission)

    def _apply_trade_to_portfolio(self, trade: Trade):
        """应用成交到投资组合（仅更新内存状态）。

        更新现金、持仓数量、持仓成本。
        注意：不更新市值（MV）和盈亏，也不负责数据库同步。

        Args:
            trade: 成交记录
        """
        today = datetime.date.today()

        if trade.side == OrderSide.BUY:
            self._cash -= (trade.amount + trade.fee)
            if trade.asset in self._positions:
                pos = self._positions[trade.asset]
                # 更新成本价 (简单加权平均)
                # Cost = (OldShares * OldPrice + NewShares * TradePrice) / TotalShares
                new_shares = pos.shares + trade.shares
                if new_shares > 0:
                    pos.price = (pos.shares * pos.price + trade.shares * trade.price) / new_shares
                pos.shares = new_shares
                # T+1: avail 不变
            else:
                # 新建持仓
                pos = Position(
                    portfolio_id=self.portfolio_id,
                    dt=today,
                    asset=trade.asset,
                    shares=trade.shares,
                    price=trade.price,
                    avail=0, # T+1
                    mv=trade.amount, # 此时成交额即为市值
                    profit= 0
                )
                self._positions[trade.asset] = pos
        else:
            self._cash += (trade.amount - trade.fee)
            if trade.asset in self._positions:
                pos = self._positions[trade.asset]
                pos.shares -= trade.shares
                pos.avail -= trade.shares # Reduce avail immediately

                if pos.shares <= 0:
                    del self._positions[trade.asset]
                # 注意：卖出不影响剩余持仓的成本价 (pos.price)

    async def buy(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """买入指令。

        Args:
            asset: 资产代码
            shares: 买入数量
            price: 买入价格，0 表示市价
            order_time: 下单时间
            timeout: 超时时间（秒）

        Returns:
            成交结果

        Raises:
            InsufficientCash: 资金不足
        """
        if int(shares) % 100 != 0:
            raise NonMultipleOfLotSize(asset, shares)

        est_price = price
        if est_price == 0:
            _, up_limit = live_quote.get_price_limits(asset)
            est_price = up_limit
            # 如果 up_limit 为 0 (无涨跌停限制，如新股)，则使用 lastPrice 估算
            if est_price <= 0:
                quote = live_quote.get_quote(asset)
                if quote:
                    est_price = quote.get("lastPrice", 0)

        if est_price > 0:
            est_cost = est_price * shares * (1 + self._commission)
            if est_cost > self._cash:
                raise InsufficientCash(asset, est_cost, self._cash)

        # 2. 创建订单
        order = Order(
            portfolio_id=self.portfolio_id,
            asset=asset,
            price=price,
            shares=shares,
            side=OrderSide.BUY,
            bid_type=BidType.MARKET if price == 0 else BidType.FIXED,
            tm=order_time or datetime.datetime.now(),
        )
        db.insert_order(order)

        # 3. 加入等待队列
        with self._lock:
            self._active_orders[asset].append(order)

        # 4. 等待结果
        res, _ = await self.wait(order.qtoid, timeout)

        if res is None:
            # 超时，返回已有的部分成交记录（如果有）
            partial_trades = self._order_trades.get(order.qtoid, [])
            return TradeResult(order.qtoid, list(partial_trades))
        return res

    async def sell(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """卖出指令。

        Args:
            asset: 资产代码
            shares: 卖出数量
            price: 卖出价格，0 表示市价
            order_time: 下单时间
            timeout: 超时时间（秒）

        Returns:
            成交结果

        Raises:
            InsufficientPosition: 持仓不足
            NonMultipleOfLotSize: 卖出数量不符合手数限制（非清仓时）
        """
        # 1. 检查持仓
        if asset not in self._positions:
            raise InsufficientPosition(security=asset, amount=shares)
        pos = self._positions[asset]
        self._validate_sell_shares(pos, shares)

        # 2. 创建订单
        order = Order(
            portfolio_id=self.portfolio_id,
            asset=asset,
            price=price,
            shares=shares,
            side=OrderSide.SELL,
            bid_type=BidType.MARKET if price == 0 else BidType.FIXED,
            tm=order_time or datetime.datetime.now(),
        )
        db.insert_order(order)

        with self._lock:
            self._active_orders[asset].append(order)

        res, _ = await self.wait(order.qtoid, timeout)
        if res is None:
            # 超时，返回已有的部分成交记录（如果有）
            partial_trades = self._order_trades.get(order.qtoid, [])
            return TradeResult(order.qtoid, list(partial_trades))
        return res

    async def buy_percent(
        self,
        asset: str,
        percent: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """按总资产比例买入。

        Args:
            asset: 资产代码
            percent: 目标比例 (0.0 - 1.0)
            price: 买入价格，0 表示市价
            order_time: 下单时间
            timeout: 超时时间（秒）

        Returns:
            成交结果
        """
        # 计算数量
        quote = live_quote.get_quote(asset)
        if not quote:
             return TradeResult("", [])

        if price > 0:
            p = price
        else:
            _, up_limit = live_quote.get_price_limits(asset)
            p = up_limit or quote.get("lastPrice", 0)
        if p <= 0:
            return TradeResult("", [])

        # 使用总资产计算
        target_value = self.total_assets * percent

        shares = int(target_value / p / 100) * 100
        if shares == 0:
             return TradeResult("", [])

        return await self.buy(asset, shares, price, order_time, timeout)

    async def buy_amount(
        self,
        asset: str,
        amount: int | float,
        price: int | float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """按金额买入。

        Args:
            asset: 资产代码
            amount: 买入金额
            price: 买入价格，0 表示市价
            order_time: 下单时间
            timeout: 超时时间（秒）

        Returns:
            成交结果
        """
        quote = live_quote.get_quote(asset)
        if not quote and price == 0:
             return TradeResult("", [])

        if price > 0:
            p = price
        else:
            _, up_limit = live_quote.get_price_limits(asset)
            p = up_limit or quote.get("lastPrice", 0)
        if p <= 0:
             return TradeResult("", [])

        shares = int(amount / p / 100) * 100
        if shares == 0:
             return TradeResult("", [])

        return await self.buy(asset, shares, price, order_time, timeout)

    async def sell_percent(
        self,
        asset: str,
        percent: float,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """按持仓比例卖出。

        Args:
            asset: 资产代码
            percent: 卖出比例 (0.0 - 1.0)。如果 >= 0.9999 则视为清仓。
            order_time: 下单时间
            timeout: 超时时间（秒）

        Returns:
            成交结果
        """
        if asset not in self._positions:
            return TradeResult("", [])

        pos = self._positions[asset]
        # 如果是 1.0 (100%)，则是清仓
        if percent >= 0.9999:
            shares = pos.shares
        else:
            shares = int(pos.shares * percent / 100) * 100

        if shares == 0:
            return TradeResult("", [])

        return await self.sell(asset, shares, 0, order_time, timeout)

    async def sell_amount(
        self,
        asset: str,
        amount: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """按金额卖出。

        Args:
            asset: 资产代码
            amount: 卖出金额
            price: 卖出价格，0 表示市价
            order_time: 下单时间
            timeout: 超时时间（秒）

        Returns:
            成交结果
        """
        quote = live_quote.get_quote(asset)
        if not quote and price == 0:
             return TradeResult("", [])

        if price > 0:
            p = price
        else:
            down_limit, _ = live_quote.get_price_limits(asset)
            p = down_limit or quote.get("lastPrice", 0)
        if p <= 0:
             return TradeResult("", [])

        shares = int(amount / p / 100) * 100
        if shares == 0:
            # 如果金额不足1手，是否尝试卖出1手？通常不。
            # 但是如果 amount 很大导致 shares > pos.shares，sell 方法会检查并抛出异常
            return TradeResult("", [])

        return await self.sell(asset, shares, price, order_time, timeout)

    async def trade_target_pct(
        self,
        asset: str,
        target_pct: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """调整持仓至目标比例。

        如果当前比例低于目标，则买入；如果高于目标，则卖出。

        Args:
            asset: 资产代码
            target_pct: 目标持仓占总资产的比例 (0.0 - 1.0)
            price: 交易价格，0 表示市价
            order_time: 下单时间
            timeout: 超时时间（秒）

        Returns:
            成交结果
        """
        quote = live_quote.get_quote(asset)
        if not quote and price == 0:
             return TradeResult("", [])

        # 计算当前价值和目标价值应该统一使用参考价格（通常是 lastPrice）
        # 只有在计算买入/卖出数量时，为了保守起见，才可能使用 limit price
        ref_price = price
        if ref_price == 0:
            ref_price = quote.get("lastPrice", 0)
            if ref_price <= 0:
                # Fallback to limit price if lastPrice is invalid
                down_limit, up_limit = live_quote.get_price_limits(asset)
                # 如果没有 lastPrice，尝试使用涨停价作为参考
                ref_price = up_limit if up_limit > 0 else 0

        if ref_price <= 0:
             return TradeResult("", [])

        total = self.total_assets
        target_val = total * target_pct

        current_val = 0.0
        if asset in self._positions:
            current_val = self._positions[asset].shares * ref_price

        diff = target_val - current_val

        if diff > 0:
            # 买入
            return await self.buy_amount(asset, diff, price, order_time, timeout)
        elif diff < 0:
            # 卖出
            # 注意：sell_amount 会向下取整 100 股。
            # 如果 diff 很小，可能不交易。
            # 如果希望精确清仓，可能需要特殊处理 sell_percent(0) 的情况?
            # trade_target_pct(0) -> sell all.
            if target_pct < 0.0001:
                return await self.sell_percent(asset, 1.0, order_time, timeout)
            return await self.sell_amount(asset, -diff, price, order_time, timeout)
        else:
            return TradeResult("", [])

    async def cancel_order(self, qt_oid: str):
        """取消订单。

        Args:
            qt_oid: 订单 ID (Quantide Order ID)
        """
        with self._lock:
            # 查找并移除
            found = False
            for asset, orders in self._active_orders.items():
                for i, order in enumerate(orders):
                    if order.qtoid == qt_oid:
                        # 从列表中移除
                        orders.pop(i)
                        found = True

                        # 更新状态
                        if order.filled > 0:
                            order.status = OrderStatus.PARTSUCC_CANCEL
                        else:
                            order.status = OrderStatus.CANCELED

                        order.status_msg = "Canceled by user"
                        db.update_order(qt_oid, status=order.status.value, status_msg="Canceled by user", filled=order.filled)

                        # 唤醒等待者（如果还在等待）
                        all_trades = self._order_trades.pop(qt_oid, [])
                        self.awake(qt_oid, TradeResult(qt_oid, all_trades))
                        break
                if found:
                    if not orders:
                        del self._active_orders[asset]
                    break

    async def cancel_all_orders(self, side: OrderSide | None = None):
        """取消所有订单。

        Args:
            side: 订单方向。如果指定，则只取消该方向的订单；否则取消所有订单。
        """
        with self._lock:
            assets_to_remove = []
            for asset, orders in self._active_orders.items():
                remaining = []
                for order in orders:
                    if side is None or order.side == side:
                        # Cancel
                        if order.filled > 0:
                            order.status = OrderStatus.PARTSUCC_CANCEL
                        else:
                            order.status = OrderStatus.CANCELED

                        order.status_msg = "Canceled by user"
                        db.update_order(order.qtoid, status=order.status.value, status_msg="Canceled by user", filled=order.filled)

                        all_trades = self._order_trades.pop(order.qtoid, [])
                        self.awake(order.qtoid, TradeResult(order.qtoid, all_trades))
                    else:
                        remaining.append(order)

                if not remaining:
                    assets_to_remove.append(asset)
                else:
                    self._active_orders[asset] = remaining

            for asset in assets_to_remove:
                del self._active_orders[asset]

    async def on_day_open(self, limits: dict[str, dict[str, float]] | None = None):
        """开盘处理。

        Args:
            limits: 涨跌停限制 {asset: {'up': float, 'down': float}}
        """
        # 仿真 broker 实时从 quote 获取 limit，此处暂不需要特殊处理
        pass

    async def on_day_close(self, close_prices: dict[str, float] | None = None):
        """收盘后处理：生成资产快照。

        Args:
            close_prices: 收盘价 {asset: price}
        """
        with self._lock:
            today = datetime.date.today()

            # Sync positions with latest prices
            snapshot_positions = []
            market_value = 0.0

            for asset, pos in self._positions.items():
                pos.dt = today

                price = 0
                if close_prices and asset in close_prices:
                    price = close_prices[asset]
                else:
                    # Try live quote or fallback to cost
                    quote = live_quote.get_quote(asset)
                    price = quote.get("lastPrice", 0) if quote else 0
                    if price <= 0:
                        price = pos.price

                pos.mv = pos.shares * price
                pos.profit = (price - pos.price) * pos.shares
                snapshot_positions.append(pos)
                market_value += pos.mv

            if snapshot_positions:
                db.upsert_positions(snapshot_positions)

            # Update Asset
            total_asset = self._cash + market_value
            asset_record = Asset(
                portfolio_id=self.portfolio_id,
                dt=today,
                principal=self._principal,
                cash=self._cash,
                frozen_cash=0,
                market_value=market_value,
                total=total_asset
            )
            db.upsert_asset(asset_record)
