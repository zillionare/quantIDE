"""实盘交易接口的 XtQuantTrader 实现。

## 委托 (Order)

当客户端提交一个 buy/sell 指令时，系统会将它实例化为一个 OrderData 对象，该对象包含一个 oid 字段，用于唯一标识该订单；同时还包含其它必要的、用于交易的字段。

QMTBroker 会先验证订单、存入数据库，再向 XtTrader发起异步请求，在用户指定的超时时间内等待成交，并返回成交结果。如果在指定的超时内未成交，则返回空列表。

委托的后续状态变化以及最终成交情况，都可以通过 orders 表来查询（使用 oid 作为主键）

# todo
1. 收盘后（两次）从 qmt 获取资产和持仓信息，更新到数据库

"""

import asyncio
import datetime
import importlib
import math
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from loguru import logger
from tenacity import RetryCallState, retry, stop_after_attempt, wait_fixed


from xtquant.xttrader import XtQuantTrader
from xtquant.xttrader import XtQuantTraderCallback
from xtquant.xttype import StockAccount
from xtquant.xttype import XtAsset
from xtquant.xttype import XtCancelError
from xtquant.xttype import XtOrder
from xtquant.xttype import XtOrderError
from xtquant.xttype import XtOrderResponse
from xtquant.xttype import XtPosition
from xtquant.xttype import XtTrade
from xtquant import xtconstant


from pyqmt.config import cfg
from pyqmt.core.enums import BidType, OrderSide, OrderStatus
from pyqmt.core.errors import (
    TradeError,
    TradeErrors,
    XtQuantTradeError,
    XtTradeConnectError,
)
from pyqmt.data.sqlite import Asset, Order, Position, Trade, db, new_uuid_id
from pyqmt.notify.dingtalk import ding
from pyqmt.service.abstract_broker import AbstractBroker


# helpers
def as_asset(xt_asset: XtAsset, principal: float, portfolio_id: str) -> Asset:
    return Asset(
        portfolio_id=portfolio_id,
        total=xt_asset.total_asset,
        cash=xt_asset.cash,
        market_value=xt_asset.market_value,
        frozen_cash=xt_asset.frozen_cash,
        dt=datetime.date.today(),
        principal=principal
    )
def as_position(xt_position: XtPosition, portfolio_id: str) -> Position:
    return Position(
        portfolio_id=portfolio_id,
        dt=datetime.date.today(),
        asset=xt_position.stock_code,
        shares=xt_position.volume,
        avail=xt_position.can_use_volume,
        price=xt_position.avg_price,
        profit=xt_position.profit_rate,
        mv=xt_position.market_value,
    )


def as_xt_bid_type(bid_type: BidType, price: float, asset: str) -> tuple[int, float]:
    """将通用的委托类型转换成XtQuant的委托类型"""
    market = asset.split(".")[1].upper()
    if price == 0 or bid_type == BidType.MARKET:
        # 用户要求采用市价
        if market == "SH":
            return xtconstant.MARKET_SH_CONVERT_5_CANCEL, 0
        elif market == "SZ":
            return xtconstant.MARKET_SZ_CONVERT_5_CANCEL, 0
        else:
            return xtconstant.LATEST_PRICE, 0

    price_type_map = {
        BidType.FIXED: (xtconstant.FIX_PRICE, price),
        BidType.LATEST: (xtconstant.LATEST_PRICE, 0),
        BidType.MINE_FIRST: (xtconstant.MARKET_SH_CONVERT_5_CANCEL, 0),
        BidType.PEER_FIRST: (xtconstant.MARKET_SZ_CONVERT_5_CANCEL, 0),
    }

    # 如果匹配错误，则降级到最新价买入
    logger.warning("无法转换的委托类型{}，降级为最新价买入。", bid_type)
    return price_type_map.get(bid_type, (xtconstant.LATEST_PRICE, 0))


def as_xt_order_side(side: OrderSide) -> int:
    """将通用的订单方向转换成XtQuant的订单方向"""
    side_map = {
        OrderSide.BUY: xtconstant.STOCK_BUY,
        OrderSide.SELL: xtconstant.STOCK_SELL,
    }

    return side_map[side]


def as_bid_type(xt_bid_type: int) -> BidType:
    """将XtQuant的委托类型转换成通用的委托类型

    遇到无法转换的类型，默认返回 "UNKNOWN"。当执行到此处时，bid_type 信息多用于记录，因此
    不必抛出异常。
    """
    bid_type_map = {
        xtconstant.FIX_PRICE: BidType.FIXED,
        xtconstant.LATEST_PRICE: BidType.MARKET,
    }

    return bid_type_map.get(xt_bid_type, BidType.UNKNOWN)


def as_order_side(xt_order_side: int) -> OrderSide:
    """将XtQuant的订单方向转换成通用的订单方向

    遇到无法转换的类型，默认返回 "UNKNOWN"。当执行到此处时，bid_type 信息多用于记录，因此
    不必抛出异常。
    """
    side_map = {
        xtconstant.STOCK_BUY: OrderSide.BUY,
        xtconstant.STOCK_SELL: OrderSide.SELL,
    }

    return side_map.get(xt_order_side, OrderSide.UNKNOWN)


def as_order_status(xt_order_status: int) -> OrderStatus:
    """将XtQuant的订单状态转换成通用的订单状态
    遇到无法转换的类型，默认返回 "UNKNOWN"。当执行到此处时，order_status 信息多用于记录，因此
    不必抛出异常。
    """
    try:
        return OrderStatus(xt_order_status)
    except ValueError:
        return OrderStatus.UNKNOWN


def as_order(xt_order: XtOrder, portfolio_id: str) -> Order:
    """将XtQuant的订单转换成通用的订单模型"""
    model = Order(
        portfolio_id=portfolio_id,
        qtoid=xt_order.order_remark,
        shares=xt_order.order_volume,
        price=xt_order.price,
        asset=xt_order.stock_code,
        side=as_order_side(xt_order.order_type),
        bid_type=as_bid_type(xt_order.price_type),
        tm=datetime.datetime.fromtimestamp(xt_order.order_time),
        foid=str(xt_order.order_id),
        cid=xt_order.order_sysid,
        status=as_order_status(xt_order.order_status),
        status_msg=xt_order.status_msg,
        strategy=xt_order.strategy_name,
    )  # 构造方法会产生一个无效的 oid, 需要在接下来进行替换

    db_order = db.get_order_by_foid(xt_order.order_id)
    if db_order is None:
        logger.warning(
            "qmt 返回定单{}时，无法找到 qtide 定单{}",
            xt_order.order_id,
            xt_order.order_remark,
        )
        # 指标调用者，此 order 需要新建
        model.qtoid = ""
    elif db_order.qtoid != model.qtoid:
        logger.warning(
            "qmt 返回定单的order_remark {} 与数据库记录的 qtoid {} 不一致，已采用数据库记录",
            model.qtoid,
            db_order.qtoid,
        )
        model.qtoid = db_order.qtoid

    return model


def as_xt_order_params(order: Order, acc: StockAccount) -> dict:
    """将通用的订单转换成 order_stock 需要的参数

    !!! attention
        我们将 `order.qtoid` 作为 order_remark 传递给 XT，便于记录关联。
    """
    price_type, price = as_xt_bid_type(order.bid_type, order.price, order.asset)
    return dict(
        account=acc,
        stock_code=order.asset,
        order_type=as_xt_order_side(order.side),
        order_volume=order.shares,
        price_type=price_type,
        price=price,
        strategy_name=order.strategy,
        order_remark=order.qtoid,
    )


def as_trade(xt_trade: XtTrade, portfolio_id: str) -> Trade:
    """将XtQuant的成交转换成通用的成交模型"""
    model = Trade(
        portfolio_id=portfolio_id,
        tid=xt_trade.traded_id,
        qtoid=xt_trade.order_remark,
        foid=xt_trade.order_id,
        asset=xt_trade.stock_code,
        shares=xt_trade.traded_volume,
        price=xt_trade.traded_price,
        amount=xt_trade.traded_amount,
        tm=datetime.datetime.fromtimestamp(xt_trade.traded_time, tz=cfg.TIMEZONE),
        side=as_order_side(xt_trade.order_type),
        cid=xt_trade.order_sysid,
    )

    db_order = db.get_order_by_foid(xt_trade.order_id)
    if db_order is None:
        logger.warning(
            "qmt 返回成交{}时，无法找到原 qtide 定单记录：{}",
            xt_trade.traded_id,
            xt_trade.order_remark,
        )
    elif db_order.qtoid != xt_trade.order_remark:
        logger.info(
            "qmt 返回成交{}时，order_remark {} 与数据库qtoid 记录{} 不匹配，已使用数据库记录",
            xt_trade.traded_id,
            xt_trade.order_remark,
            db_order.qtoid,
        )
        model.qtoid = db_order.qtoid

    return model


class MyXtQuantTraderCallback(XtQuantTraderCallback):
    def __init__(self, broker: AbstractBroker):
        self.broker = broker

    def on_disconnected(self):
        """连接断开"""
        global xt_trader

        logger.warning("connection lost, 交易接口断开。")
        xt_trader = None

    def on_stock_order(self, order: XtOrder):
        """委托回报推送，可以得知委托状态变化，比如 未报 > 待报 > 已报 > 部成 ...


        Args:
            xt_order: XtOrder对象
        """
        logger.info(f"on order callback: {order.stock_code} {order.order_status}")
        _order = as_order(order, self.broker.portfolio_id)

        params = {}

        # 避免将数据库中有值的数据更改为 None
        for key in ("status", "status_msg", "foid", "cid"):
            if getattr(_order, key, None) is not None:
                params[key] = getattr(_order, key)

        # todo: check if xtorder 包含trade信息更新
        traded_vol = order.traded_volume
        traded_price = order.traded_price
        logger.info("on_stock_order 返回成交信息: {traded_vol} {traded_price}")

        db.update_order(_order.qtoid, **params)

    def on_stock_asset(self, asset: XtAsset):
        """
        资金变动推送 注意，该回调函数目前不生效
        :param asset: XtAsset对象
        :return:
        """
        logger.info(f"on asset callback {asset}")
        self.broker.on_sync_asset(
            asset.total_asset, asset.cash, asset.frozen_cash, asset.market_value
        )
        # logger.info(asset.account_id, asset.cash, asset.total_asset)

    def on_stock_trade(self, trade: XtTrade):
        """
        成交变动推送
        :param trade: XtTrade对象
        :return:
        """
        logger.info(trade.account_id, trade.stock_code, trade.order_id)
        _trade = as_trade(trade, self.broker.portfolio_id)
        db.insert_trades(_trade)

    def on_stock_position(self, position: XtPosition):
        """
        持仓变动推送 注意，该回调函数目前不生效
        :param position: XtPosition对象
        :return:
        """
        logger.info(f"on position callback {position}")
        _position = as_position(position, self.broker.portfolio_id)
        db.upsert_positions(_position)

    def on_order_error(self, order_error: XtOrderError):
        """
        委托失败推送
        :param order_error:XtOrderError 对象
        :return:
        """
        # logger.info(f"on order_error callback {order_error}")
        logger.info(
            f"order_error {order_error.account_id}, {order_error.strategy_name}, {order_error.error_id}, {order_error.error_msg}"
        )

        qtoid = order_error.order_remark
        if qtoid is None or not qtoid.startswith("qtide-"):
            # 完全的废单，无法关联。
            logger.warning(
                "qmt 废单{}时，无法找到 qtide 定单{}",
                order_error.order_id,
                order_error.strategy_name,
            )
            return

        error = f"{order_error.error_id}:{order_error.error_msg}"
        db.update_order(qtoid, error=error)

    def on_cancel_error(self, cancel_error: XtCancelError):
        """
        撤单失败推送
        :param cancel_error: XtCancelError 对象
        :return:
        """
        # todo: 需要向 UI 报告
        logger.info(f"on cancel_error callback {cancel_error}")
        # logger.info(cancel_error.order_id, cancel_error.error_id, cancel_error.error_msg)

    def on_order_stock_async_response(self, response: XtOrderResponse) -> None:
        """
        异步下单回报推送
        :param response: XtOrderResponse 对象
        :return:
        """
        order_id, seq = response.order_id, response.seq
        logger.info("on_order_stock_async_response {}, {}", order_id, seq)
        self.broker.awake(seq, order_id)

        # logger.info(response.account_id, response.order_id, response.seq)

    def on_account_status(self, status):
        """
        :param response: XtAccountStatus 对象
        :return:
        """
        # 账号状态映射
        status_map = {
            -1: "无效",
            0: "正常",
            1: "连接中",
            2: "登陆中",
            3: "失败",
            4: "初始化中",
            5: "数据刷新校正中",
            6: "收盘后",
            7: "穿透副链接断开",
            8: "系统停用（密码错误超限）",
            9: "用户停用",
        }

        status_text = status_map.get(status.status, f"未知状态({status.status})")
        logger.info(
            f"on_account_status {status.account_id} {status.account_type} {status.status}({status_text})"
        )


class QMTBroker(AbstractBroker):
    def __init__(self, account_id: str, portfolio_id: str = "qmt"):
        super().__init__(portfolio_id=portfolio_id)
        if xtconstant is None:
            raise ImportError("xtquant is required for QMTBroker")
        self.account_id = account_id
        self.account_type = "stock"

        assert Path(cfg.qmt.path).exists(), "qmt安装路径不存在"

        self.acc = StockAccount(self.account_id, self.account_type) # type: ignore
        self.path = cfg.qmt.path

        # xt接口需要的区分不同账号的session_id，同一个账号可以多次登录
        self.session_id = int(self.account_id)

        self._trade_api: XtQuantTrader | None = None

        # 资产表
        self._asset: Asset | None = None

    @staticmethod
    def before_retry_sleep(rs: RetryCallState):
        """用于重试，记录日志及重连 XtTrader

        1. tenacity 不支持使用 loguru 的 logger，通过此方法实现重试日志。
        2. 本方法会导致重连接 XtTrader
        """
        instance: QMTBroker = rs.args[0]

        # 断开连接
        try:
            if instance._trade_api is not None:
                instance._trade_api.stop()
        except Exception:
            pass

        # 重置之后，self.trade_api 就会自动重连
        instance._trade_api = None

        if rs.outcome is not None:
            expt = rs.outcome.exception()
        else:
            expt = None

        logger.debug(
            "{} retrying {}th time, reason: {}", rs.fn, rs.attempt_number, expt
        )

    @staticmethod
    def on_final_failure(rs):
        """用以重试。在重试无法恢复之后，发出通知消息

        Args:
            rs: tenacity.RetryCallState
        """
        if rs.outcome is not None:
            expt = rs.outcome.exception()
        else:
            expt = None

        msg = f"{rs.fn.__name__}重试第{rs.attempt_number}次后，仍然失败，原因{expt}"
        ding(msg)

    @property
    def asset(self) -> Asset:
        if self._asset is None:
            self._asset = self.query_asset_info()

        return self._asset

    @property
    def trade_api(self) -> XtQuantTrader:
        if self._trade_api is None:
            self.connect_trade_api()

        if self._trade_api is None:
            raise XtTradeConnectError(TradeErrors.ERROR_UNKNOWN, "交易API未连接")

        return self._trade_api

    def connect_trade_api(self):
        """初始化或重新连接TradeAPI"""
        global xt_trader

        self.session_id += 1

        xt_trader = XtQuantTrader(self.path, self.session_id)
        callback = MyXtQuantTraderCallback(self)
        xt_trader.register_callback(callback)
        xt_trader.start()  # 启动交易线程

        connect_result = xt_trader.connect()
        logger.info(f"{self.account_id} connect_result={connect_result}")

        if connect_result == 0:
            logger.info(f"{self.account_id} connected to TradeAPI success")

            subscribe_result = xt_trader.subscribe(self.acc)

            status = "成功" if subscribe_result == 0 else "失败"
            logger.info("{} 订阅 {} 成功", self.account_id, status)

            self._trade_api = xt_trader
        else:
            raise XtTradeConnectError(TradeErrors.ERROR_UNKNOWN, "连接交易API失败")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        before_sleep=before_retry_sleep,
        retry_error_callback=on_final_failure,
    )
    def query_asset_info(self) -> Asset:
        """查询最新的资产信息"""
        response = self.trade_api.query_stock_asset(self.acc)

        if response is None:
            raise XtQuantTradeError(TradeErrors.ERROR_UNKNOWN, "查询资产信息失败")

        return as_asset(response, self._principal, self.portfolio_id)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        before_sleep=before_retry_sleep,
        retry_error_callback=on_final_failure,
    )
    async def query_positions(self) -> pl.DataFrame:
        """查询最新的持仓信息"""
        positions = []
        response = self.trade_api.query_stock_positions(self.acc) or []
        for pos in response:
            position = as_position(pos, self.portfolio_id)
            positions.append(position)

        db.upsert_positions(positions)
        return pl.DataFrame(positions)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(1),
        before_sleep=before_retry_sleep,
        retry_error_callback=on_final_failure,
    )
    async def query_orders(self, cancelable_only: bool = False) -> pl.DataFrame|None:
        """查询最新的订单信息，并列新数据库中保存的状态。

        qmt 返回的都是当日委托。

        Args:
            cancelable_only: 是否只查询可取消订单

        Returns:
            订单列表
        """
        orders = []
        response = self.trade_api.query_stock_orders(self.acc, cancelable_only) or []

        for order in response:
            order_ = as_order(order, self.portfolio_id)

            if order_.qtoid == "":
                order_.qtoid = order.order_remark or new_uuid_id()
                db.insert_order(order_)
            else:
                db.update_order(
                    order_.qtoid,
                    **{
                        "status": order_.status,
                        "status_msg": order_.status_msg,
                        "foid": order_.foid,
                        "cid": order_.cid,
                    },
                )
            orders.append(order_)

        return db.query_order_by_date(datetime.date.today())

    async def buy(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        strategy: str = "",
        timeout: float = 0.5,
    ) -> pl.DataFrame|None:
        """买入指令

        如果传入价格为0或者 None，则为市价买入。

        Args:
            asset: 资产代码, "symbol.SZ"风格
            price: 委托价格
            shares: 委托数量
            bit_time: 下单时间，实盘时可省略传入，测试时必须传入
            strategy: 策略名称
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果
        """
        bid_type = BidType.MARKET if price == 0 else BidType.FIXED

        cash = self.asset.cash

        if price != 0:  # 检查账户余额是否充足
            if cash < price * shares:
                shares = math.floor(cash / price / 100) * 100
        shares = self._normalize_buy_shares(shares)

        order = Order(
            portfolio_id=self.portfolio_id,
            asset=asset,
            price=price,
            shares=shares,
            side=OrderSide.BUY,
            bid_type=bid_type,
            tm = bid_time or datetime.datetime.now(),
            strategy=strategy
         )
        qtoid = db.insert_order(order)

        xt_order_side = as_xt_order_side(OrderSide.BUY)
        xt_bid_type = as_xt_bid_type(bid_type, price, asset)

        seq = self.trade_api.order_stock_async(
            self.acc,
            asset,
            xt_order_side,
            shares,
            xt_bid_type,
            price,
            strategy,
            order_remark=qtoid,
        )

        if seq == -1:  # 委托失败
            logger.warning("买入{}, {}, {}, {}下单失败", asset, price, shares, strategy)
            raise TradeError(TradeErrors.ERROR_XT_ORDER_FAIL, f"Order {qtoid} 下单失败")

        order_response, remaining_time = await self.wait(seq, timeout)
        if order_response is None:
            logger.warning(
                "买入{}, {}, {}, {}下单等待超时", asset, price, shares, strategy
            )
            return None

        foid = order_response
        db.update_order(qtoid, foid=str(foid))

        # 在 remaining_time 内，等待成交
        while remaining_time > 0:
            trades = db.query_trade(qtoid=qtoid)
            if trades is not None:
                break
            await asyncio.sleep(0.01)
            remaining_time -= 0.01

        # 查询成交
        return db.query_trade(qtoid=qtoid)

    async def buy_percent(self,
        asset: str,
        percent: float,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> pl.DataFrame|None:
        pass


    async def buy_amount(
        self,
        asset: str,
        amount: int | float,
        price: int | float | None = None,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        """买入指令按金额买入

        Args:
            asset: 资产代码, "symbol.SZ"风格
            amount: 买入金额
            price: 如果委托价格为 None，则以市价买入
            bid_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交(含部成），返回空列表
        """
        ...

    async def sell(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        """卖出指令

        如果传入价格为0, 则为市价卖出。

        Args:
            asset: 资产代码, "symbol.SZ"风格
            shares: 委托数量
            price: 委托价格
            bit_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交数据。如果超时未成交(含部成），返回空列表
        """
        ...


    async def sell_percent(
        self,
        asset: str,
        percent: float,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        """卖出指令按比例卖出

        Args:
            asset: 资产代码, "symbol.SZ"风格
            percent: 卖出比例，0-1之间的浮点数
            bid_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交(含部成），返回空列表
        """
        ...


    async def sell_amount(
        self,
        asset: str,
        amount: int | float,
        price: int | float | None = None,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        """卖出指令按金额卖出

        因为取整（手）的关系，实际卖出金额将可能超过约定金额，以保证回笼足够的现金。

        Args:
            asset: 资产代码, "symbol.SZ"风格
            amount: 卖出金额
            price: 如果委托价格为 None，则以市价卖出
            bid_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交(含部成），返回空列表
        """
        ...

    def cancel_order(self, order_id: str):
        """取消订单，用于实盘

        取消指定订单。如果订单不存在或已成交，不做任何操作。

        Args:
            order_id: 订单 ID
        """
        ...

    def cancel_all_orders(self, side: OrderSide | None = None):
        """取消所有订单，用于实盘

        取消所有未成交订单。如果所有订单已成交，不做任何操作。

        Args:
            side: 订单方向，默认为 None，取消所有订单
        """
        ...

    def trade_target_pct(
        self,
        asset: str,
        price: float,
        target_pct: float,
        bid_type: BidType = BidType.MARKET,
    ) -> list[Trade]:
        """将`asset`的仓位调整到占比`target_pct`

        如果当前仓位大于 target_pct，则卖出；
        如果当前仓位小于 target_pct，则买入，直到现金用尽；在这种情况下，最终`asset`的仓位会小于约定的`target_pct`。

        !!! warning:
            受交易手数取整和手续费影响，最终仓位可能会小于等于约定仓位。

        Args:
            asset: 资产代码, "symbol.SZ"风格
            price: 委托价格
            target_pct: 目标仓位占比，0-1之间的浮点数
            bid_type: 委托类型，市价或限价
        """
        ...
