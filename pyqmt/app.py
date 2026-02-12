#!/usr/bin/env python3

"""
Main application entry point for the PyQMT system.
This file sets up the FastHTML application with MonsterUI styling.
"""

from pathlib import Path

from starlette.staticfiles import StaticFiles
from fasthtml.common import *
from monsterui.all import *
from starlette.middleware import Middleware

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
from pyqmt.web.pages.home import home_app
from pyqmt.web.pages.login import login_app
from pyqmt.web.pages.trade import trade_app
from pyqmt.web.pages.live import live_app


def init():
    init_config()

    init_data(cfg.home)

    scheduler.start()

    live_quote.start()

    reg = BrokerRegistry()
    try:
        sim_broker = SimulationBroker(portfolio_id="sim_demo", portfolio_name="演示账户", principal=1000000)
        reg.register(BrokerKind.SIMULATION, "sim_demo", sim_broker)
    except Exception as e:
        print(f"Failed to create demo broker: {e}")

    auth = AuthManager(config={"login_path": "/login"})

    app, rt = fast_app(
        hdrs=Theme.blue.headers(),
        before=auth.create_beforeware(),
        middleware=[Middleware(BrokerRegistryMiddleware, registry=reg)],
        exception_handlers={
            Exception: exception_handler,
            BaseTradeError: exception_handler,
        },
        routes=[
            Mount("/login", login_app),
            Mount("/home", home_app),
            Mount("/trade/simulation", trade_app),
            Mount("/trade/live", live_app),
            Mount("/broker", broker_api_app),
            Mount("/", home_app),
        ],
    )

    static_dir = Path(__file__).resolve().parent / "web" / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    auth.initialize(app, prefix="/auth")

    return app


app = init()

if __name__ == "__main__":
    serve()
