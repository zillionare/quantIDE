#!/usr/bin/env python3

"""
Main application entry point for the PyQMT system.
This file sets up the FastHTML application with MonsterUI styling.
"""

from pathlib import Path

import cfg4py
from fastapi.staticfiles import StaticFiles
from fasthtml.common import *
from monsterui.all import *
from starlette.middleware import Middleware

from pyqmt.config import cfg, init_config
from pyqmt.core.errors import BaseTradeError
from pyqmt.core.scheduler import scheduler
from pyqmt.data import init_data

# from pyqmt.service.sync import start_intraday_sync
from pyqmt.web.apis.broker import app as broker_api_app
from pyqmt.web.auth.manager import AuthManager
from pyqmt.web.middleware import BrokerRegistryMiddleware, exception_handler
from pyqmt.web.pages.home import home_app
from pyqmt.web.pages.login import login_app


def init():
    cfg4py.init(get_config_dir())

    # 初始化交易数据库
    init_data(cfg.home)  # type: ignore

    # 启动任务调度器
    scheduler.start()
    #start_intraday_sync()

    # 初始化 Registry
    reg = BrokerRegistry()
    # 临时：创建一个默认仿真账户方便测试
    # TODO: 应该从数据库加载活跃的 portfolios
    try:
        sim_broker = SimulationBroker(portfolio_id="sim_demo", portfolio_name="演示账户", principal=1000000)
        reg.register(sim_broker)
    except Exception as e:
        print(f"Failed to create demo broker: {e}")

    # 初始化 auth 管理器，配置登录路径
    auth = AuthManager(config={"login_path": "/login"})

    # 创建主应用
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
            Mount("/broker", broker_api_app),
            Mount("/", home_app),
        ],
    )

    static_dir = Path(__file__).resolve().parent / "web" / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # 初始化认证系统并注册路由
    auth.initialize(app, prefix="/auth")

    return app


app = init()

if __name__ == "__main__":
    serve()
