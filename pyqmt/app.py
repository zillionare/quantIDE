#!/usr/bin/env python3

"""
Main application entry point for the PyQMT system.
This file sets up the FastHTML application with MonsterUI styling.
"""

import cfg4py
from fasthtml.common import *
from monsterui.all import *

from pyqmt.config import cfg, get_config_dir
from pyqmt.dal.tradedb import db
from pyqmt.service.qmt_broker import QMTBroker
from pyqmt.web.apis.broker import app as broker_api_app
from pyqmt.web.auth.manager import AuthManager
from pyqmt.web.middleware import BrokerMiddleware
from pyqmt.web.pages.home import home_app
from pyqmt.web.pages.login import login_app


def init():
    cfg4py.init(get_config_dir())

    # 初始化交易数据库
    db.init(cfg.db.path)

    broker = None
    if cfg.broker == "qmt":
        broker = QMTBroker(cfg)

    # 初始化 auth 管理器，配置登录路径
    auth = AuthManager(config={"login_path": "/login"})

    # 创建主应用
    app, rt = fast_app(
        hdrs=Theme.blue.headers(),
        before=auth.create_beforeware(),
        routes=[
            Mount("/login", login_app),
            Mount("/home", home_app),
            Mount("/broker", broker_api_app),
            Mount("/", home_app)
        ],
    )

    if broker is not None:
        app.add_middleware(BrokerMiddleware, broker=broker)

    # 初始化认证系统并注册路由
    auth.initialize(app, prefix="/auth")

    return app

app = init()

if __name__ == "__main__":
    serve()
