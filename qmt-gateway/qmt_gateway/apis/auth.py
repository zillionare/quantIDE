"""认证 API

提供登录、登出和修改密码功能。
"""

import bcrypt
from fasthtml.common import *
from loguru import logger

from qmt_gateway.db import db
from qmt_gateway.db.models import User


def hash_password(password: str) -> str:
    """哈希密码"""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, hashed: str) -> bool:
    """验证密码"""
    return bcrypt.checkpw(password.encode(), hashed.encode())


def login_required(handler):
    """登录验证装饰器"""
    def wrapper(request, *args, **kwargs):
        # 从 session 获取用户信息
        user = request.scope.get("session", {}).get("user")
        if not user:
            return RedirectResponse("/login", status_code=302)
        return handler(request, *args, **kwargs)
    return wrapper


def login_page(error: str = ""):
    """登录页面"""
    from qmt_gateway.web.layouts.base import create_base_page
    from qmt_gateway.web.theme import PRIMARY_COLOR, PrimaryButton

    error_msg = Div(error, cls="text-red-500 text-sm mb-4") if error else ""

    return create_base_page(
        Div(
            H3("QMT Gateway", cls="text-2xl font-bold text-center mb-2", style=f"color: {PRIMARY_COLOR};"),
            P("请登录以继续", cls="text-gray-500 text-center mb-6"),
            error_msg,
            Form(
                Div(
                    Label("用户名", cls="label"),
                    Input(
                        type="text",
                        name="username",
                        placeholder="请输入用户名",
                        cls="input input-bordered w-full",
                        required=True,
                    ),
                    cls="mb-4",
                ),
                Div(
                    Label("密码", cls="label"),
                    Input(
                        type="password",
                        name="password",
                        placeholder="请输入密码",
                        cls="input input-bordered w-full",
                        required=True,
                    ),
                    cls="mb-4",
                ),
                Div(
                    Input(
                        type="checkbox",
                        name="auto_login",
                        id="auto_login",
                        cls="w-4 h-4 text-red-600 border-gray-300 rounded focus:ring-red-500",
                    ),
                    Label("本机自动登录", _for="auto_login", cls="ml-2 text-sm text-gray-700 cursor-pointer"),
                    cls="flex items-center mb-6",
                ),
                PrimaryButton("登录", type="submit", cls="w-full"),
                action="/auth/login",
                method="post",
            ),
            cls="max-w-md mx-auto bg-white rounded-lg shadow p-8 mt-12",
        ),
        page_title="登录 - QMT Gateway",
    )


def handle_login(request, username: str, password: str, auto_login: bool = False):
    """处理登录请求"""
    user = db.get_user(username)

    if not user:
        return login_page("用户名或密码错误")

    if not verify_password(password, user.password_hash):
        return login_page("用户名或密码错误")

    # 更新自动登录设置
    if auto_login != user.auto_login:
        user.auto_login = auto_login
        db.save_user(user)

    # 设置 session
    request.scope["session"]["user"] = {
        "id": user.id,
        "username": user.username,
        "is_admin": user.is_admin,
    }

    logger.info(f"用户登录成功: {username}, 自动登录: {auto_login}")
    return RedirectResponse("/", status_code=302)


def handle_logout(request):
    """处理登出请求"""
    request.scope["session"].clear()
    return RedirectResponse("/login", status_code=302)


def handle_change_password(request, old_password: str, new_password: str):
    """处理修改密码请求"""
    user_info = request.scope.get("session", {}).get("user")
    if not user_info:
        return {"success": False, "message": "未登录"}

    user = db.get_user(user_info["username"])
    if not user:
        return {"success": False, "message": "用户不存在"}

    if not verify_password(old_password, user.password_hash):
        return {"success": False, "message": "原密码错误"}

    # 更新密码
    user.password_hash = hash_password(new_password)
    db.save_user(user)

    logger.info(f"用户修改密码成功: {user.username}")
    return {"success": True, "message": "密码修改成功"}


def register_routes(app):
    """注册认证路由"""

    @app.get("/login")
    def get_login():
        return login_page()

    @app.post("/auth/login")
    def post_login(request, username: str, password: str, auto_login: str = ""):
        return handle_login(request, username, password, auto_login=auto_login == "on")

    @app.get("/auth/logout")
    def get_logout(request):
        return handle_logout(request)

    @app.post("/auth/password")
    def post_change_password(request, old_password: str, new_password: str):
        return handle_change_password(request, old_password, new_password)
