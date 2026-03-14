"""交易 API

提供账户资金、持仓、订单查询和交易执行功能。
"""

import datetime
from fasthtml.common import *
from loguru import logger

from qmt_gateway.config import config
from qmt_gateway.db.models import Asset, Position
from qmt_gateway.db.sqlite import db
from qmt_gateway.services.trade_service import trade_service

DEFAULT_PORTFOLIO_ID = "default"


def _as_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_value(item, key: str, default=None):
    if isinstance(item, dict):
        return item.get(key, default)
    return getattr(item, key, default)


def _latest_dt(table_name: str, portfolio_id: str) -> datetime.date | None:
    row = db.conn.execute(
        f"select max(dt) from {table_name} where portfolio_id = ?",
        (portfolio_id,),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return datetime.date.fromisoformat(str(row[0]))


def _get_latest_asset(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> Asset | None:
    dt = _latest_dt("assets", portfolio_id)
    if dt is None:
        return None
    return db.get_asset(portfolio_id, dt)


def _get_latest_positions(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> list[Position]:
    dt = _latest_dt("positions", portfolio_id)
    if dt is None:
        return []
    return db.get_positions(portfolio_id, dt)


def _snapshot_asset(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> Asset | None:
    live = trade_service.get_asset()
    if not live:
        return None
    today = datetime.date.today()
    asset = Asset(
        portfolio_id=portfolio_id,
        dt=today,
        principal=_as_float(_get_value(live, "total", 0)),
        cash=_as_float(_get_value(live, "cash", 0)),
        frozen_cash=_as_float(_get_value(live, "frozen_cash", 0)),
        market_value=_as_float(_get_value(live, "market_value", 0)),
        total=_as_float(_get_value(live, "total", 0)),
    )
    db["assets"].upsert(asset.to_dict(), pk=Asset.__pk__)
    return asset


def _snapshot_positions(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> None:
    rows = trade_service.get_positions()
    if not rows:
        return
    today = datetime.date.today()
    for row in rows:
        symbol = str(_get_value(row, "symbol", "")).strip()
        if not symbol:
            continue
        price = _as_float(_get_value(row, "cost", _get_value(row, "price", 0)))
        position = Position(
            portfolio_id=portfolio_id,
            dt=today,
            asset=symbol,
            shares=_as_float(_get_value(row, "shares", 0)),
            avail=_as_float(_get_value(row, "avail", 0)),
            price=price,
            profit=_as_float(_get_value(row, "profit", 0)),
            mv=_as_float(_get_value(row, "market_value", 0)),
        )
        db["positions"].upsert(position.to_dict(), pk=Position.__pk__)


def get_latest_asset_data(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> dict:
    asset = _get_latest_asset(portfolio_id)
    if asset is None:
        asset = _snapshot_asset(portfolio_id)
    if asset is None:
        return {
            "principal": 0,
            "total": 0,
            "cash": 0,
            "market_value": 0,
            "frozen_cash": 0,
        }
    return {
        "principal": asset.principal,
        "total": asset.total,
        "cash": asset.cash,
        "market_value": asset.market_value,
        "frozen_cash": asset.frozen_cash,
    }


def get_latest_positions_data(portfolio_id: str = DEFAULT_PORTFOLIO_ID) -> list[dict]:
    positions = _get_latest_positions(portfolio_id)
    if not positions:
        _snapshot_positions(portfolio_id)
        positions = _get_latest_positions(portfolio_id)
    total = get_latest_asset_data(portfolio_id).get("total", 0)
    data = []
    for p in positions:
        hold_cost = p.shares * p.price
        float_profit = hold_cost * p.profit / 100 if p.profit else 0
        position_ratio = (p.mv / total * 100) if total > 0 else 0
        data.append(
            {
                "symbol": p.asset,
                "name": "",
                "shares": p.shares,
                "avail": p.avail,
                "price": p.price,
                "cost": p.price,
                "profit_ratio": p.profit,
                "float_profit": float_profit,
                "buy_avg": p.price,
                "market_value": p.mv,
                "hold_cost": hold_cost,
                "sell_profit": 0,
                "position_ratio": position_ratio,
            }
        )
    return data


def login_required(request):
    """检查用户是否登录"""
    user = request.scope.get("session", {}).get("user")
    if not user:
        raise HTTPException(status_code=401, detail="未登录")
    return user


def register_routes(app):
    """注册交易路由"""

    @app.on_event("startup")
    async def startup_event():
        """启动时连接交易接口"""
        try:
            if config.qmt_account_id and config.qmt_path:
                success = trade_service.connect(
                    account_id=config.qmt_account_id,
                    qmt_path=str(config.qmt_path),
                )
                if success:
                    logger.info("交易接口已连接")
                else:
                    logger.warning("交易接口连接失败")
        except Exception as e:
            logger.error(f"启动时连接交易接口失败: {e}")

    @app.on_event("shutdown")
    async def shutdown_event():
        """关闭时断开交易接口"""
        trade_service.disconnect()

    @app.get("/api/trade/asset")
    def get_asset(request):
        """获取账户资金"""
        login_required(request)
        return get_latest_asset_data()

    @app.get("/api/trade/positions")
    def get_positions(request, view: str = "json"):
        """获取持仓列表

        Args:
            view: 返回格式，json 或 table
        """
        login_required(request)
        positions_data = get_latest_positions_data()

        if view == "table":
            from qmt_gateway.web.pages.trading import PositionTable
            return PositionTable(positions_data)

        return positions_data

    @app.get("/api/trade/orders")
    def get_orders(request, status: str = "all", view: str = "json"):
        """获取订单列表

        Args:
            status: 订单状态过滤
            view: 返回格式，json 或 table
        """
        login_required(request)
        orders = trade_service.get_orders()

        orders_data = [
            {
                "time": _get_value(o, "time", ""),
                "symbol": _get_value(o, "symbol", ""),
                "name": _get_value(o, "name", ""),
                "side": _get_value(o, "side", "buy"),
                "price": _as_float(_get_value(o, "price", 0)),
                "shares": _as_float(_get_value(o, "shares", 0)),
                "filled": _as_float(_get_value(o, "filled", 0)),
                "status": _get_value(o, "status", "pending"),
                "qtoid": _get_value(o, "qtoid", ""),
            }
            for o in orders
        ]

        if view == "table":
            from qmt_gateway.web.pages.trading import OrdersTable
            return OrdersTable(orders_data)

        return orders_data

    @app.get("/api/trade/trades")
    def get_trades(request):
        """获取成交列表"""
        login_required(request)
        trades = trade_service.get_trades()
        return [
            {
                "time": _get_value(t, "time", ""),
                "symbol": _get_value(t, "symbol", ""),
                "name": _get_value(t, "name", ""),
                "side": _get_value(t, "side", "buy"),
                "price": _as_float(_get_value(t, "price", 0)),
                "shares": _as_float(_get_value(t, "shares", 0)),
                "amount": _as_float(_get_value(t, "amount", 0)),
            }
            for t in trades
        ]

    @app.post("/api/trade/buy")
    def buy_stock(request, symbol: str, price: float, shares: int):
        """买入股票"""
        login_required(request)
        result = trade_service.buy(symbol, price, shares)
        return result

    @app.post("/api/trade/sell")
    def sell_stock(request, symbol: str, price: float, shares: int):
        """卖出股票"""
        login_required(request)
        result = trade_service.sell(symbol, price, shares)
        return result

    @app.post("/api/trade/cancel")
    def cancel_order(request, order_id: str):
        """撤单"""
        login_required(request)
        result = trade_service.cancel_order(order_id)
        return result

    @app.post("/api/asset/principal")
    def update_principal(request, principal: float):
        """修改本金

        修改本金会在 asset 表中插入一条新的记录。
        如果当天已有记录，则更新该记录的本金字段。

        Args:
            principal: 新本金金额

        Returns:
            JSONResponse: 操作结果
        """
        user = login_required(request)

        if principal <= 0:
            return JSONResponse(
                {"code": 1, "message": "本金金额必须大于0"},
                status_code=400
            )

        try:
            today = datetime.date.today()
            portfolio_id = DEFAULT_PORTFOLIO_ID

            latest_dt = _latest_dt("assets", portfolio_id) or today
            existing = db.get_asset(portfolio_id, latest_dt)

            if existing:
                existing.principal = principal
                db["assets"].upsert(existing.to_dict(), pk=Asset.__pk__)
                logger.info(
                    f"更新本金: portfolio_id={portfolio_id}, dt={latest_dt}, principal={principal}"
                )
            else:
                current_asset = get_latest_asset_data(portfolio_id)
                new_asset = Asset(
                    portfolio_id=portfolio_id,
                    dt=latest_dt,
                    principal=principal,
                    cash=_as_float(current_asset.get("cash", 0)),
                    frozen_cash=_as_float(current_asset.get("frozen_cash", 0)),
                    market_value=_as_float(current_asset.get("market_value", 0)),
                    total=_as_float(current_asset.get("total", principal), principal),
                )

                db.insert_asset(new_asset)
                logger.info(
                    f"插入新本金记录: portfolio_id={portfolio_id}, dt={latest_dt}, principal={principal}"
                )

            return JSONResponse({
                "code": 0,
                "message": "本金修改成功",
                "data": {
                    "principal": principal,
                    "date": latest_dt.isoformat(),
                }
            })

        except Exception as e:
            logger.error(f"修改本金失败: {e}")
            return JSONResponse(
                {"code": 1, "message": f"修改本金失败: {str(e)}"},
                status_code=500
            )
