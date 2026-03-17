"""FastHTML 应用主入口

组装所有路由和中间件。
"""

import datetime
from contextlib import asynccontextmanager
from pathlib import Path

from fasthtml.common import *
from loguru import logger

from qmt_gateway.apis import (
    login_required,
    quote_ws,
    register_auth_routes,
    register_history_routes,
    register_quotes_routes,
    register_stock_routes,
    register_trade_routes,
)
from qmt_gateway.apis.auth import hash_password
from qmt_gateway.config import config
from qmt_gateway.db import db
from qmt_gateway.db.models import Asset, Settings, User
from qmt_gateway.runtime import runtime
from qmt_gateway.services.scheduler import scheduler
from qmt_gateway.web.pages.data_mgmt import DataMgmtPage
from qmt_gateway.web.pages.init_wizard import InitWizardForm, InitWizardPage
from qmt_gateway.web.pages.trading import TradingPage


# 存储向导表单数据的临时缓存
_wizard_data: dict = {}


def check_init_required():
    """检查是否需要初始化"""
    try:
        settings = db.get_settings()
        return not settings.init_completed
    except Exception:
        return True


def create_app():
    """创建 FastHTML 应用"""

    # 初始化运行时
    runtime.init()

    # 创建 FastHTML 应用
    app = FastHTML(
        hdrs=[
            # Tailwind CSS
            Script(src="https://cdn.tailwindcss.com"),
            # DaisyUI
            Link(
                rel="stylesheet",
                href="https://cdn.jsdelivr.net/npm/daisyui@4.12.10/dist/full.min.css",
            ),
            # HTMX
            Script(src="https://unpkg.com/htmx.org@1.9.12"),
        ],
        session_cookie="qmt_gateway_session",
    )

    # 注册 API 路由
    register_auth_routes(app)
    register_trade_routes(app)
    register_quotes_routes(app)
    register_stock_routes(app)
    register_history_routes(app)

    # 初始化向导路由
    @app.get("/init-wizard")
    def init_wizard(force: str = None):
        """初始化向导页面

        Args:
            force: 如果为 "true"，强制重新运行初始化向导
        """
        # 检查是否需要初始化，或者强制重新初始化
        if force != "true" and not check_init_required():
            return RedirectResponse("/", status_code=302)

        # 强制重新初始化时，重置初始化状态
        if force == "true":
            try:
                settings = db.get_settings()
                settings.init_completed = False
                settings.init_step = 0
                db.save_settings(settings)
                logger.info("强制重新初始化向导")
            except Exception as e:
                logger.error(f"重置初始化状态失败: {e}")

        return InitWizardPage(step=1)

    @app.post("/init-wizard/step/{step}")
    async def wizard_step(step: int, request):
        """向导步骤处理"""
        global _wizard_data

        # 保存当前步骤的数据（从表单数据获取）
        form_data = await request.form()
        form_dict = {k: v for k, v in form_data.items()}
        _wizard_data.update(form_dict)
        logger.info(f"接收到表单数据: {form_dict}")

        # 第2步（管理员设置）点击下一步时，校验密码一致性
        if step == 3:  # 即将进入第3步（服务器设置）
            password = _wizard_data.get("password", "")
            password_confirm = _wizard_data.get("password_confirm", "")
            logger.info(f"第2步密码校验: password='{password}', password_confirm='{password_confirm}'")
            if password != password_confirm:
                # 返回第2步，并显示错误信息
                return InitWizardForm(step=2, form_data=_wizard_data, error="两次输入的密码不一致，请重新输入")

        # 返回表单部分（用于 HTMX 更新）
        return InitWizardForm(step=step, form_data=_wizard_data)

    @app.post("/init-wizard/complete")
    async def wizard_complete(request):
        """完成初始化"""
        global _wizard_data

        try:
            # 保存最后一步的表单数据（从表单数据获取）
            form_data = await request.form()
            form_dict = {k: v for k, v in form_data.items()}
            _wizard_data.update(form_dict)

            # 保存管理员账号（强制重新初始化时覆盖已有用户）
            username = _wizard_data.get("username", "admin")
            password = _wizard_data.get("password", "")

            if password:
                # 检查用户是否已存在
                existing_user = db.get_user(username)
                if existing_user:
                    # 更新现有用户
                    existing_user.password_hash = hash_password(password)
                    existing_user.auto_login = False
                    db.save_user(existing_user)
                    logger.info(f"更新现有用户: {username}")
                else:
                    # 创建新用户
                    user = User(
                        username=username,
                        password_hash=hash_password(password),
                        auto_login=False,
                    )
                    db.save_user(user)
                    logger.info(f"创建新用户: {username}")

            # 保存服务器设置
            settings = db.get_settings()
            settings.server_port = int(_wizard_data.get("server_port", 8130))
            settings.log_path = str(Path(_wizard_data.get("log_path", "~/.qmt-gateway/log")).expanduser().resolve())
            settings.log_rotation = _wizard_data.get("log_rotation", "10 MB")
            settings.log_retention = int(_wizard_data.get("log_retention", 10))

            # 保存 QMT 配置（规范化路径格式）
            settings.qmt_account_id = _wizard_data.get("qmt_account_id", "")
            settings.qmt_path = str(Path(_wizard_data.get("qmt_path", "")).expanduser().resolve()) if _wizard_data.get("qmt_path") else ""
            settings.xtquant_path = str(Path(_wizard_data.get("xtquant_path", "")).expanduser().resolve()) if _wizard_data.get("xtquant_path") else ""

            principal = float(_wizard_data.get("principal", 1000000))
            if principal <= 0:
                principal = 1000000

            today = datetime.date.today()
            portfolio_id = "default"
            asset_row = db.conn.execute(
                """
                select cash, frozen_cash, market_value, total
                from assets
                where portfolio_id = ?
                order by dt desc
                limit 1
                """,
                (portfolio_id,),
            ).fetchone()
            if asset_row:
                cash = float(asset_row[0] or 0)
                frozen_cash = float(asset_row[1] or 0)
                market_value = float(asset_row[2] or 0)
                total = float(asset_row[3] or principal)
            else:
                cash = principal
                frozen_cash = 0.0
                market_value = 0.0
                total = principal

            initial_asset = Asset(
                portfolio_id=portfolio_id,
                dt=today,
                principal=principal,
                cash=cash,
                frozen_cash=frozen_cash,
                market_value=market_value,
                total=total if total > 0 else principal,
            )
            db["assets"].upsert(initial_asset.to_dict(), pk=Asset.__pk__)

            settings.init_step = 5
            db.save_settings(settings)

            # 重新加载配置
            config.reload()

            # 测试 xtquant 和 QMT 连接
            test_result = test_xtquant_connection()
            if not test_result["success"]:
                return Div(
                    P(f"✗ 连接测试失败", cls="text-red-600 font-bold mb-4"),
                    P(f"错误: {test_result['error']}", cls="text-gray-600 mb-4"),
                    Button(
                        "返回修改配置",
                        cls="btn btn-secondary px-6 py-2",
                        hx_get="/init-wizard/step/5",
                        hx_target="#wizard-form-container",
                    ),
                    cls="text-center py-8",
                )

            # 标记初始化完成
            settings.init_completed = True
            settings.init_completed_at = datetime.datetime.now()
            db.save_settings(settings)

            logger.info("初始化完成")
            return RedirectResponse("/", status_code=302)

        except Exception as e:
            logger.error(f"完成初始化失败: {e}")
            return Div(f"初始化失败: {e}", cls="text-red-500")

    def test_xtquant_connection():
        """测试 xtquant 和 QMT 连接"""
        try:
            from qmt_gateway.core import add_xtquant_path, require_xtdata

            # 添加 xtquant 路径
            settings = db.get_settings()
            xtquant_path = settings.xtquant_path if settings.xtquant_path else None
            qmt_path = settings.qmt_path if settings.qmt_path else None

            add_xtquant_path(
                xtquant_path=xtquant_path,
                qmt_path=qmt_path,
            )

            # 测试导入 xtquant
            xtdata = require_xtdata(
                xtquant_path=xtquant_path,
                qmt_path=qmt_path,
            )

            # 测试获取市场列表
            markets = xtdata.get_stock_list_in_sector("沪深A股")
            if markets and len(markets) > 0:
                logger.info(f"xtquant 连接测试成功，获取到 {len(markets)} 只股票")
                return {"success": True, "message": f"连接成功，共 {len(markets)} 只股票"}
            else:
                return {"success": False, "error": "无法获取股票列表，请检查 QMT 是否已登录"}

        except Exception as e:
            logger.error(f"xtquant 连接测试失败: {e}")
            return {"success": False, "error": str(e)}

    # 主页面路由
    @app.get("/")
    def index(request):
        """首页"""
        if check_init_required():
            return RedirectResponse("/init-wizard", status_code=302)

        # 获取当前用户
        user = request.scope.get("session", {}).get("user")
        if not user:
            return RedirectResponse("/login", status_code=302)

        # 获取交易数据
        from qmt_gateway.apis.trade import get_latest_asset_data, get_latest_positions_data, trade_service
        asset = get_latest_asset_data()
        positions = get_latest_positions_data()
        orders = trade_service.get_orders()
        trades = trade_service.get_trades()

        return TradingPage(
            asset=asset,
            positions=positions,
            orders=orders,
            trades=trades,
            user=user,
        )

    @app.get("/trading")
    def trading_page(request):
        """实盘交易页面"""
        if check_init_required():
            return RedirectResponse("/init-wizard", status_code=302)

        user = request.scope.get("session", {}).get("user")
        if not user:
            return RedirectResponse("/login", status_code=302)

        from qmt_gateway.apis.trade import get_latest_asset_data, get_latest_positions_data, trade_service
        asset = get_latest_asset_data()
        positions = get_latest_positions_data()
        orders = trade_service.get_orders()
        trades = trade_service.get_trades()

        return TradingPage(
            asset=asset,
            positions=positions,
            orders=orders,
            trades=trades,
            user=user,
        )

    @app.get("/data")
    def data_page(request, sector_type: str = ""):
        """数据管理页面"""
        if check_init_required():
            return RedirectResponse("/init-wizard", status_code=302)

        user = request.scope.get("session", {}).get("user")
        if not user:
            return RedirectResponse("/login", status_code=302)

        return DataMgmtPage(
            selected_type=sector_type,
            sectors=[],
            user=user,
        )

    # 启动服务
    @app.on_event("startup")
    async def startup():
        """应用启动时执行"""
        if not check_init_required():
            # 启动定时任务
            scheduler.start()
            # 启动行情服务
            quote_ws.start()
            logger.info("应用启动完成")

    @app.on_event("shutdown")
    async def shutdown():
        """应用关闭时执行"""
        scheduler.stop()
        quote_ws.stop()
        logger.info("应用已关闭")

    return app


# 创建应用实例
app = create_app()
