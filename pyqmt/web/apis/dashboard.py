from fastapi import APIRouter, Request, Depends
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
import datetime

from pyqmt.web.auth import auth, get_current_user

router = APIRouter()
templates = Jinja2Templates(directory="pyqmt/web/templates")

@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    # 主页应该对所有人开放，但如果有用户登录则显示用户信息
    # 尝试获取当前用户，如果未登录则为None
    try:
        # 手动检查cookie中的JWT令牌
        token = request.cookies.get("pyqmt_session")
        if token:
            from pyqmt.web.auth import verify_jwt
            user = verify_jwt(token)
            if user:
                return templates.TemplateResponse("index.html", {"request": request, "user": user})
    except:
        pass
    
    # 如果没有有效的用户会话，则不显示用户信息
    return templates.TemplateResponse("index.html", {"request": request, "user": None})

@router.get("/protected", response_class=HTMLResponse)
@auth
async def protected(request: Request, user: str):
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return templates.TemplateResponse("protected.html", {"request": request, "user": user, "current_time": current_time})

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    # 重定向到主页而不是使用不存在的dashboard.html模板
    return RedirectResponse(url="/")

@router.get("/strategies", response_class=HTMLResponse)
@auth
async def strategies(request: Request, user: str):
    return templates.TemplateResponse("strategies.html", {"request": request, "user": user})

@router.get("/api/me")
async def read_users_me(current_user: str = Depends(get_current_user)):
    return {"user": current_user}
