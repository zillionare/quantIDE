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
from typing import TYPE_CHECKING, Any

import polars as pl
from loguru import logger
from tenacity import RetryCallState, retry, stop_after_attempt, wait_fixed

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

# xtquant 模块（延迟导入）
_xtquant_modules: dict[str, Any] = {}


def _get_xtquant(name: str):
    """延迟获取 xtquant 模块或类"""
    if name not in _xtquant_modules:
        if name == "xtconstant":
            from xtquant import xtconstant as mod
        elif name == "XtQuantTrader":
            from xtquant.xttrader import XtQuantTrader as mod
        elif name == "XtQuantTraderCallback":
            from xtquant.xttrader import XtQuantTraderCallback as mod
        elif name == "StockAccount":
            from xtquant.xttype import StockAccount as mod
        elif name == "XtAsset":
            from xtquant.xttype import XtAsset as mod
        elif name == "XtCancelError":
            from xtquant.xttype import XtCancelError as mod
        elif name == "XtOrder":
            from xtquant.xttype import XtOrder as mod
        elif name == "XtOrderError":
            from xtquant.xttype import XtOrderError as mod
        elif name == "XtOrderResponse":
            from xtquant.xttype import XtOrderResponse as mod
        elif name == "XtPosition":
            from xtquant.xttype import XtPosition as mod
        elif name == "XtTrade":
            from xtquant.xttype import XtTrade as mod
        else:
            raise ImportError(f"Unknown xtquant module: {name}")
        _xtquant_modules[name] = mod
    return _xtquant_modules[name]


# helpers
def as_asset(xt_asset, principal: float, portfolio_id: str) -> Asset:
    XtAsset = _get_xtquant("XtAsset")
    return Asset(
        portfolio_id=portfolio_id,
        total=xt_asset.total_asset,
        cash=xt_asset.cash,
        market_value=xt_asset.market_value,
        frozen_cash=xt_asset.frozen_cash,
        dt=datetime.date.today(),
        principal=principal,
    )


def as_position(xt_position, portfolio_id: str) -> Position:
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
    xtconstant = _get_xtquant("xtconstant")
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
    xtconstant = _get_xtquant("xtconstant")
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
    xtconstant = _get_xtquant("xtconstant")
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
    xtconstant = _get_xtquant("xtconstant")
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


def as_order(xt_order, portfolio_id: str) -> Order:
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


def as_xt_order_params(order: Order, acc) -> dict:
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


def as_trade(xt_trade, portfolio_id: str) -> Trade:
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


class MyXtQuantTraderCallback:
    """XtQuantTrader 回调实现"""

    def __init__(self, broker: AbstractBroker):
        self.broker = broker

    def on_disconnected(self):
        """连接断开"""
        global xt_trader

        logger.warning("connection lost, 交易接口断开。")
        xt_trader = None

    def on_stock_order(self, order):
        """委托回报推送，可以得知委托状态变化，比如 未报 > 待报 > 已报 > 部成 ...


        Args:
            xt_order: XtOrder对象
        """
        logger.info(f"on order callback: {order.stock_code} {order.order_status}")
        _order = as_order(order, self.broker._portfolio_id)

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

    def on_stock_asset(self, asset):
        """资产回报推送"""
        logger.info(f"on asset callback: {asset}")

    def on_stock_position(self, position):
        """持仓回报推送"""
        logger.info(f"on position callback: {position}")

    def on_stock_trade(self, trade):
        """成交回报推送"""
        logger.info(f"on trade callback: {trade}")
        _trade = as_trade(trade, self.broker._portfolio_id)
        db.insert_trade(_trade)

        # 更新订单状态
        db_order = db.get_order_by_foid(_trade.foid)
        if db_order is None:
            logger.warning(
                "qmt 返回成交{}时，无法找到原 qtide 定单记录：{}",
                _trade.tid,
                _trade.qtoid,
            )
        else:
            db.update_order(
                db_order.qtoid,
                status=OrderStatus.FILLED,
            )

    def on_order_error(self, order_error):
        """委托失败推送"""
        logger.error(f"on order error: {order_error}")

    def on_cancel_error(self, cancel_error):
        """撤单失败推送"""
        logger.error(f"on cancel error: {cancel_error}")

    def on_order_response(self, response):
        """委托响应推送"""
        logger.info(f"on order response: {response}")

    def on_cancel_response(self, response):
        """撤单响应推送"""
        logger.info(f"on cancel response: {response}")

    def on_account_status(self, status):
        """账户状态推送"""
        status_map = {
            0: "正常",
            1: "禁用",
            2: "锁定",
            3: "注销",
        }

        status_text = status_map.get(status.status, f"未知状态({status.status})")
        logger.info(
            f"on_account_status {status.account_id} {status.account_type} {status.status}({status_text})"
        )


class QMTBroker(AbstractBroker):
    def __init__(self, account_id: str, portfolio_id: str = "qmt"):
        super().__init__(portfolio_id=portfolio_id)
        self.account_id = account_id
        self.account_type = "stock"

        assert Path(cfg.qmt.path).exists(), "qmt安装路径不存在"

        StockAccount = _get_xtquant("StockAccount")
        self.acc = StockAccount(self.account_id, self.account_type)
        self.path = cfg.qmt.path

        # xt接口需要的区分不同账号的session_id，同一个账号可以多次登录
        self.session_id = int(self.account_id)

        self._trade_api = None

        # 资产表
        self._asset: Asset | None = None

    @staticmethod
    def before_retry_sleep(rs: RetryCallState):
        """用于重试，记录日志及重连 XtTrader

        1. tenacity 不支持使用 loguru 的 logger，通过此方法实现重试日志。
        2. 本方法会导致重连接 XtTrader
        """
        instance = rs.args[0]

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
            logger.warning(
                "第{}次重试: {}. 等待{}秒后重试...",
                rs.attempt_number,
                expt,
                rs.next_action.sleep,
            )

    @property
    def trade_api(self):
        """获取交易 API，如果不存在则创建"""
        if self._trade_api is None:
            XtQuantTrader = _get_xtquant("XtQuantTrader")
            XtQuantTraderCallback = _get_xtquant("XtQuantTraderCallback")

            self._trade_api = XtQuantTrader(
                str(self.path), self.session_id
            )
            self._callback = MyXtQuantTraderCallback(self)
            self._trade_api.register_callback(self._callback)
            self._trade_api.start()

            connect_result = self._trade_api.connect()
            if connect_result != 0:
                self._trade_api.stop()
                self._trade_api = None
                raise XtTradeConnectError(
                    f"无法连接到交易接口，错误码: {connect_result}"
                )

            subscribe_result = self._trade_api.subscribe(self.acc)
            if subscribe_result != 0:
                self._trade_api.stop()
                self._trade_api = None
                raise XtTradeConnectError(
                    f"无法订阅账户，错误码: {subscribe_result}"
                )

        return self._trade_api

    def _disconnect(self):
        """断开交易接口连接"""
        if self._trade_api is not None:
            try:
                self._trade_api.stop()
            except Exception as e:
                logger.error("断开交易接口失败: {}", e)
            finally:
                self._trade_api = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        sleep=before_retry_sleep,
        reraise=True,
    )
    async def _place_order(
        self,
        asset: str,
        side: OrderSide,
        shares: int,
        price: float = 0,
        bid_type: BidType = BidType.MARKET,
        strategy: str = "",
    ) -> list[Trade]:
        """下单并等待成交

        1. 先验证订单、存入数据库
        2. 再向 XtTrader发起异步请求
        3. 在用户指定的超时时间内等待成交，并返回成交结果
        4. 如果在指定的超时内未成交，则返回空列表
        """
        # 构造订单
        order = Order(
            portfolio_id=self._portfolio_id,
            asset=asset,
            side=side,
            shares=shares,
            price=price,
            bid_type=bid_type,
            strategy=strategy,
        )

        # 验证订单
        self._validate_order(order)

        # 存入数据库
        db.insert_order(order)

        # 下单
        try:
            order_id = self.trade_api.order_stock(
                **as_xt_order_params(order, self.acc)
            )
        except Exception as e:
            logger.error("下单失败: {}", e)
            raise XtQuantTradeError(f"下单失败: {e}")

        if order_id == -1:
            raise XtQuantTradeError("下单失败，返回 -1")

        # 更新订单的 foid
        db.update_order(order.qtoid, foid=str(order_id))

        # 等待成交
        trades = await self._wait_for_trades(order.qtoid)

        return trades

    async def _wait_for_trades(self, qtoid: str, timeout: int = 30) -> list[Trade]:
        """等待订单成交

        Args:
            qtoid: 订单 ID
            timeout: 超时时间（秒）

        Returns:
            成交列表
        """
        import asyncio

        start_time = datetime.datetime.now()
        trades = []

        while (datetime.datetime.now() - start_time).seconds < timeout:
            # 查询成交
            trades = db.get_trades_by_qtoid(qtoid)
            if trades:
                return trades

            await asyncio.sleep(0.1)

        return trades

    def _validate_order(self, order: Order):
        """验证订单"""
        if order.shares <= 0:
            raise TradeError("订单数量必须大于0")

        if order.price < 0:
            raise TradeError("订单价格不能为负数")

    async def buy(
        self,
        asset: str,
        shares: int,
        price: float = 0,
        bid_type: BidType = BidType.MARKET,
        strategy: str = "",
    ) -> list[Trade]:
        """买入"""
        return await self._place_order(
            asset=asset,
            side=OrderSide.BUY,
            shares=shares,
            price=price,
            bid_type=bid_type,
            strategy=strategy,
        )

    async def sell(
        self,
        asset: str,
        shares: int,
        price: float = 0,
        bid_type: BidType = BidType.MARKET,
        strategy: str = "",
    ) -> list[Trade]:
        """卖出"""
        return await self._place_order(
            asset=asset,
            side=OrderSide.SELL,
            shares=shares,
            price=price,
            bid_type=bid_type,
            strategy=strategy,
        )

    async def cancel(self, qtoid: str):
        """撤单"""
        order = db.get_order_by_qtoid(qtoid)
        if order is None:
            raise TradeError(f"订单不存在: {qtoid}")

        if order.foid is None:
            raise TradeError(f"订单没有 foid: {qtoid}")

        try:
            self.trade_api.cancel_order_stock(self.acc, int(order.foid))
        except Exception as e:
            logger.error("撤单失败: {}", e)
            raise XtQuantTradeError(f"撤单失败: {e}")

    def get_positions(self) -> list[Position]:
        """获取持仓"""
        try:
            xt_positions = self.trade_api.query_stock_positions(self.acc)
        except Exception as e:
            logger.error("查询持仓失败: {}", e)
            raise XtQuantTradeError(f"查询持仓失败: {e}")

        if xt_positions is None:
            return []

        return [as_position(p, self._portfolio_id) for p in xt_positions]

    def get_position(self, asset: str) -> Position | None:
        """获取单个持仓"""
        try:
            xt_position = self.trade_api.query_stock_position(self.acc, asset)
        except Exception as e:
            logger.error("查询持仓失败: {}", e)
            raise XtQuantTradeError(f"查询持仓失败: {e}")

        if xt_position is None:
            return None

        return as_position(xt_position, self._portfolio_id)

    def get_asset(self) -> Asset:
        """获取资产"""
        try:
            xt_asset = self.trade_api.query_stock_asset(self.acc)
        except Exception as e:
            logger.error("查询资产失败: {}", e)
            raise XtQuantTradeError(f"查询资产失败: {e}")

        if xt_asset is None:
            raise XtQuantTradeError("查询资产返回 None")

        self._asset = as_asset(xt_asset, self._principal, self._portfolio_id)
        return self._asset

    def get_orders(
        self, status: OrderStatus | None = None, start: datetime.date | None = None, end: datetime.date | None = None
    ) -> list[Order]:
        """获取订单列表"""
        return db.get_orders(
            portfolio_id=self._portfolio_id,
            status=status,
            start=start,
            end=end,
        )

    def get_trades(
        self, start: datetime.date | None = None, end: datetime.date | None = None
    ) -> list[Trade]:
        """获取成交列表"""
        return db.get_trades(
            portfolio_id=self._portfolio_id,
            start=start,
            end=end,
        )


# 全局 xt_trader 实例
xt_trader = None
