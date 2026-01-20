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

from pyqmt.config import cfg, get_config_dir
from pyqmt.core.errors import BaseTradeError
from pyqmt.data import init_data
from pyqmt.web.apis.broker import app as broker_api_app
from pyqmt.web.auth.manager import AuthManager
from pyqmt.web.middleware import BrokerRegistryMiddleware, exception_handler
from pyqmt.web.pages.home import home_app
from pyqmt.web.pages.login import login_app


def init():
    cfg4py.init(get_config_dir())

    # 初始化交易数据库
    init_data(cfg.home)  # type: ignore

    # 初始化 auth 管理器，配置登录路径
    auth = AuthManager(config={"login_path": "/login"})

    # 创建主应用
    app, rt = fast_app(
        hdrs=Theme.blue.headers(),
        before=auth.create_beforeware(),
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
