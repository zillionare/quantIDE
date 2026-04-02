#!/usr/bin/env python3

"""
Main application entry point for the Quantide system.
This file sets up the FastHTML application with MonsterUI styling.
"""

import atexit
import datetime
import os
import sys
from pathlib import Path

from fasthtml.common import *
from monsterui.all import *
from fasthtml.common import Script
from loguru import logger
from starlette.middleware import Middleware
from starlette.responses import RedirectResponse
from starlette.routing import Route
from starlette.staticfiles import StaticFiles

from quantide.config.paths import get_app_db_path, get_pid_file_path
from quantide.config.settings import get_data_home
from quantide.core.errors import BaseTradeError
from quantide.core.runtime import RuntimeBootstrap
from quantide.data import init_data
from quantide.data.sqlite import db
from quantide.service.registry import BrokerRegistry
from quantide.service.strategy_runtime import strategy_runtime_manager
from quantide.web.apis.analysis import kline_router, search_router
from quantide.web.apis.broker import app as broker_api_app
from quantide.web.auth.manager import AuthManager
from quantide.web.middleware import BrokerRegistryMiddleware, exception_handler
from quantide.web.middleware_feature import FeatureCheckMiddleware
from quantide.web.middleware_init import InitCheckMiddleware
from quantide.web.pages.accounts import accounts_app, accounts_list
from quantide.web.pages.analysis import analysis_handler
from quantide.web.pages.data_calendar import data_calendar_app
from quantide.web.pages.data_db import data_db_app
from quantide.web.pages.data_market import data_market_app
from quantide.web.pages.data_stocks import data_stocks_app
from quantide.web.pages.history_orders import history_orders_list
from quantide.web.pages.history_positions import history_positions_list
from quantide.web.pages.history_trades import history_trades_list
from quantide.web.pages.home import home_app
from quantide.web.pages.init_wizard import init_wizard, init_wizard_app
from quantide.web.pages.live import live_app
from quantide.web.pages.strategy import strategy_app
from quantide.web.pages.trade import trade_app
from quantide.web.pages.trade_main import set_active_account, trade_main_page
from quantide.web.theme import AppTheme


def _check_single_instance():
    """检查是否已有实例在运行，防止多实例启动"""
    pid_file = get_pid_file_path()

    if pid_file.exists():
        try:
            with open(pid_file, "r") as f:
                pid = int(f.read().strip())

            if sys.platform == "win32":
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(1, False, pid)
                if handle != 0:
                    kernel32.CloseHandle(handle)
                    raise RuntimeError(
                        f"Quantide 已经在运行 (PID: {pid})。"
                        f"请先停止现有实例，或删除 {pid_file} 后重试。"
                    )
            else:
                # Unix/Linux/Mac
                os.kill(pid, 0)
                raise RuntimeError(
                    f"Quantide 已经在运行 (PID: {pid})。"
                    f"请先停止现有实例，或删除 {pid_file} 后重试。"
                )
        except (ValueError, OSError, ProcessLookupError):
            # 进程不存在，删除旧文件
            pid_file.unlink()

    # 写入当前 PID
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))

    # 注册退出时清理
    def cleanup_pid():
        if pid_file.exists():
            pid_file.unlink()

    atexit.register(cleanup_pid)


def _initialize_app_database() -> Path:
    """Initialize the fixed sqlite database in the config directory.

    If the existing database file is unreadable, move it aside and create a
    fresh one so the init wizard can continue.
    """
    db_path = get_app_db_path()

    try:
        db.init(db_path)
        return db_path
    except Exception as exc:
        if not db_path.exists():
            raise

        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        backup_path = db_path.with_name(f"{db_path.stem}.corrupt.{timestamp}{db_path.suffix}")
        logger.warning(f"配置数据库无法读取，已备份到 {backup_path}: {exc}")
        try:
            db_path.rename(backup_path)
        except Exception:
            logger.exception("备份损坏配置数据库失败")
            raise

        db._initialized = False
        db.init(db_path)
        return db_path


def init():
    db_path = _initialize_app_database()

    # 检查是否已有实例在运行
    _check_single_instance()

    runtime = None
    reg = BrokerRegistry()
    try:
        if init_wizard.is_initialized():
            init_data(get_data_home(), init_db=False)
            runtime = RuntimeBootstrap().bootstrap()
            reg = runtime.registry
            strategy_runtime_manager.bootstrap_from_runtime(runtime)
    except Exception as e:
        logger.warning(f"应用运行时初始化失败，将进入初始化向导模式: {e}")

    auth = AuthManager(db_path=str(db_path), config={"login_path": "/auth/login"})

    app, rt = fast_app(
        hdrs=AppTheme.headers(),
        before=auth.create_beforeware(),
        middleware=[
            Middleware(InitCheckMiddleware),
            Middleware(FeatureCheckMiddleware),
            Middleware(BrokerRegistryMiddleware, registry=reg),
        ],
        exception_handlers={
            Exception: exception_handler,
            BaseTradeError: exception_handler,
        },
        routes=[
            Mount("/static", StaticFiles(directory=str(Path(__file__).resolve().parent / "web" / "static")), name="static"),
            Route("/init-wizard", lambda req: RedirectResponse(f"/init-wizard/?{req.url.query}" if req.url.query else "/init-wizard/")),
            Mount("/init-wizard", init_wizard_app),
            Route("/login", lambda req: RedirectResponse("/auth/login", status_code=303), methods=["GET"]),
            Route("/login/", lambda req: RedirectResponse("/auth/login", status_code=303), methods=["GET"]),
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
            # 分析 API（板块/指数入口已下线）
            Mount("/api/v1/kline", kline_router),
            Mount("/api/v1/search", search_router),
            Mount("/data/calendar", data_calendar_app),
            Mount("/data/market", data_market_app),
            Mount("/data/stocks", data_stocks_app),
            Mount("/data/db", data_db_app),
            Mount("/", home_app),
        ],
    )

    auth.initialize(app, prefix="/auth")
    app.state.runtime = runtime
    app.state.strategy_runtime_manager = strategy_runtime_manager

    # 添加交易页面路由
    @rt("/trade/set-active", methods=["POST"])
    async def trade_set_active(req, session):
        return await set_active_account(req, session)

    return app


app = init()

if __name__ == "__main__":
    serve()
