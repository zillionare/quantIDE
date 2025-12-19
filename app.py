#!/usr/bin/env python3

"""
Main application entry point for the PyQMT system.
This file sets up the FastHTML application with MonsterUI styling.
"""

from fasthtml.common import *
from monsterui.all import *

# 导入各个页面的应用
from pyqmt.web.pages.login import login_app
from pyqmt.web.pages.home import home_app
from pyqmt.web.pages.backtest import backtest_app
from pyqmt.web.pages.backtest_results import backtest_results_app
from pyqmt.web.pages.settings import settings_app
from pyqmt.web.auth.manager import AuthManager

# 初始化 auth 管理器，配置登录路径
auth = AuthManager(config={
    "login_path": "/login"
})

# 初始化认证系统
auth.initialize()

# 创建主应用
app, rt = fast_app(
    hdrs=Theme.blue.headers(), 
    before=auth.create_beforeware(),
    routes=[
        Mount("/login", login_app),
        Mount("/home", home_app),
        Mount("/backtest", backtest_app),
        Mount("/backtest/results", backtest_results_app),
        Mount("/settings", settings_app)
    ]
)

@rt('/', methods="get")
def home(sess):
    return RedirectResponse('/home', status_code=303)

if __name__ == '__main__':
    serve()
