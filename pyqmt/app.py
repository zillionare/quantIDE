#!/usr/bin/env python3

"""
Main application entry point for the PyQMT system.
This file sets up the FastHTML application with MonsterUI styling.
"""

from pathlib import Path

from fasthtml.common import *
from monsterui.all import *
from starlette.middleware import Middleware
from starlette.responses import RedirectResponse
from starlette.routing import Route
from starlette.staticfiles import StaticFiles

from pyqmt.config import cfg, init_config
from pyqmt.core.enums import BrokerKind
from pyqmt.core.errors import BaseTradeError
from pyqmt.core.scheduler import scheduler
from pyqmt.data import init_data
from pyqmt.service.livequote import live_quote
from pyqmt.service.registry import BrokerRegistry
from pyqmt.service.sim_broker import SimulationBroker
from pyqmt.web.apis.broker import app as broker_api_app
from pyqmt.web.auth.manager import AuthManager
from pyqmt.web.middleware import BrokerRegistryMiddleware, exception_handler
from pyqmt.web.apis.analysis import index_router, kline_router, search_router, sector_router
from pyqmt.web.pages.accounts import accounts_app, accounts_list
from pyqmt.web.pages.analysis import analysis_handler
from pyqmt.web.pages.history_orders import history_orders_list
from pyqmt.web.pages.history_positions import history_positions_list
from pyqmt.web.pages.history_trades import history_trades_list
from pyqmt.web.pages.home import home_app
from pyqmt.web.pages.live import live_app
from pyqmt.web.pages.login import login_app
from pyqmt.web.pages.strategy import strategy_app
from pyqmt.web.pages.trade import trade_app
from pyqmt.web.pages.trade_main import trade_main_page, set_active_account


def _load_accounts_from_db(registry: BrokerRegistry):
    """从数据库加载所有账户到 BrokerRegistry"""
    from pyqmt.data.sqlite import db
    from pyqmt.service.sim_broker import SimulationBroker

    # 从数据库加载所有 portfolio
    portfolios = db.get_all_portfolios()
    for pf in portfolios:
        if pf.kind == BrokerKind.SIMULATION:
            try:
                # 加载已存在的模拟账户
                broker = SimulationBroker.load(pf.portfolio_id)
                registry.register(BrokerKind.SIMULATION, pf.portfolio_id, broker)
            except Exception as e:
                print(f"Failed to load simulation account {pf.portfolio_id}: {e}")


def init():
    init_config()

    init_data(cfg.home)

    scheduler.start()

    live_quote.start()

    reg = BrokerRegistry()

    # 从数据库加载已有账户
    _load_accounts_from_db(reg)

    auth = AuthManager(config={"login_path": "/login"})

    # 合并 Theme headers 和 HTMX
    from fasthtml.common import Script
    htmx_script = Script(src="https://unpkg.com/htmx.org@1.9.12")
    headers = list(Theme.blue.headers()) + [htmx_script]

    app, rt = fast_app(
        hdrs=headers,
        before=auth.create_beforeware(),
        middleware=[Middleware(BrokerRegistryMiddleware, registry=reg)],
        exception_handlers={
            Exception: exception_handler,
            BaseTradeError: exception_handler,
        },
        routes=[
            Mount("/static", StaticFiles(directory=str(Path(__file__).resolve().parent / "web" / "static")), name="static"),
            Mount("/login", login_app),
            Mount("/home", home_app),
            Mount("/trade/simulation", trade_app),
            Mount("/trade/live", live_app),
            Route("/trade/positions/history", history_positions_list, methods=["GET"]),
            Route("/trade/orders/history", history_orders_list, methods=["GET"]),
            Route("/trade/records/history", history_trades_list, methods=["GET"]),
            Route("/trade", trade_main_page),
            Route("/trade/", trade_main_page),
            Route("/system/accounts", accounts_list, methods=["GET"]),
            Route("/system/accounts/", accounts_list, methods=["GET"]),
            Mount("/system/accounts", accounts_app),
            Route("/strategy", lambda req: RedirectResponse("/strategy/")),
            Mount("/strategy", strategy_app),
            Route("/analysis", analysis_handler, methods=["GET"]),
            Mount("/broker", broker_api_app),
            # 分析导航 API
            Mount("/api/v1/sectors", sector_router),
            Mount("/api/v1/indices", index_router),
            Mount("/api/v1/kline", kline_router),
            Mount("/api/v1/search", search_router),
            Mount("/", home_app),
        ],
    )

    auth.initialize(app, prefix="/auth")

    # 添加交易页面路由
    @rt("/trade/set-active", methods=["POST"])
    async def trade_set_active(req, session):
        return await set_active_account(req, session)

    return app


app = init()

if __name__ == "__main__":
    serve()
