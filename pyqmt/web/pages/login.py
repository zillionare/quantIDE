from fasthtml.common import *
from monsterui.all import *

from pyqmt.web.layouts.login import LoginLayout

login_app, rt = fast_app()


@rt("/", methods="get")
def _(req, sess):
    # 检查用户是否已经登录
    if "auth" in sess:
        return RedirectResponse("/home", status_code=303)

    # 显示登录页面
    layout = LoginLayout()
    return layout.render()


@rt("/", methods="post")
async def post(req, sess):
    # 使用认证系统来验证用户
    form = await req.form()
    username = form.get("username", "").strip()
    password = form.get("password", "")

    # 获取全局 auth 实例
    from pyqmt.web.auth.manager import AuthManager

    auth = AuthManager.get_instance()

    # 使用认证管理器验证用户
    user = auth.user_repo.authenticate(username, password)

    if user:
        # 设置会话
        sess["auth"] = user.username
        sess["user_id"] = user.id
        sess["role"] = user.role
        return RedirectResponse("/home", status_code=303)
    else:
        # 登录失败，显示错误信息
        layout = LoginLayout(error="用户名或密码无效")
        return layout.render()
