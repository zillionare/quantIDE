from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import cfg4py

from pyqmt.web.auth import create_jwt

router = APIRouter()
templates = Jinja2Templates(directory="pyqmt/web/templates")

# 获取配置实例
cfg = cfg4py.get_instance()

def get_user(username: str):
    """从配置中获取用户信息"""
    for user in cfg.users:
        # 配置中的用户对象是字典类型
        if user['name'] == username:
            return user
    return None

@router.get("/login", response_class=HTMLResponse)
async def login_form(request: Request, next: str = ""):
    # 使用模板渲染登录页面
    return templates.TemplateResponse("login.html", {"request": request, "next_url": next})


@router.post("/login", response_class=HTMLResponse)
async def login(request: Request, username: str = Form(...), password: str = Form(...), next: str = Form("")):
    user = get_user(username)
    if user and user['password'] == password:
        token = create_jwt(username)
        response = RedirectResponse(url=next or "/dashboard", status_code=303)
        response.set_cookie(key="pyqmt_session", value=token, httponly=True)
        return response
    else:
        # 登录失败，重新显示登录表单并传递错误信息
        return templates.TemplateResponse("login.html", {
            "request": request,
            "next_url": next,
            "error": "用户名或密码错误"
        })
