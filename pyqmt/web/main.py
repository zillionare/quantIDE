from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment, FileSystemLoader
from .auth import create_jwt, verify_jwt, SESSION_COOKIE, get_current_user
from fastapi import Depends
import os

app = FastAPI(title="pyqmt-web-poc")

# Simple in-memory user store for PoC. Replace/manage securely in prod.
USERS = {"alice": "password123", "bob": "hunter2"}

SECRET = os.environ.get("PYQMT_SECRET", "dev-secret-change-me")

# Setup Jinja2 templates lookup
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), "templates")

env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

def render_template(name: str, **context) -> str:
    template = env.get_template(name)
    return template.render(**context)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    user = verify_jwt(token) if token else None
    if not user:
        return RedirectResponse(url="/login")
    html_content = render_template("index.html", user=user)
    return HTMLResponse(html_content)


@app.get("/login", response_class=HTMLResponse)
async def get_login(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    if token and verify_jwt(token):
        return RedirectResponse("/")
    
    # 获取用户尝试访问的原始URL
    next_url = request.query_params.get("next", "/")
    return HTMLResponse(render_template("login.html", error=None, next_url=next_url))


@app.post("/login")
async def post_login(request: Request, username: str = Form(...), password: str = Form(...), next: str = Form(default="/")):
    # simple check
    if username in USERS and USERS[username] == password:
        token = create_jwt(username)
        # 如果next_url是登录页面本身，则重定向到首页
        redirect_url = next if next != "/login" else "/"
        resp = RedirectResponse(url=redirect_url, status_code=302)
        # For PoC: httponly cookie, secure=False (dev). In prod set secure=True and SameSite as needed.
        resp.set_cookie(SESSION_COOKIE, token, httponly=True)
        return resp
    # invalid creds
    # 保留原始的next参数以便重新登录时仍然能跳转到目标页面
    return HTMLResponse(render_template("login.html", error="Invalid username or password", next_url=next))


from datetime import datetime

@app.get("/protected", response_class=HTMLResponse)
async def protected(request: Request, user: str = Depends(get_current_user)):
    print(f"Protected page called with user: {user}")  # 调试信息
    # `user` injected by Depends(get_current_user)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    html_content = render_template("protected.html", user=user, current_time=current_time)
    return HTMLResponse(html_content)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, user: str = Depends(get_current_user)):
    print(f"Dashboard page called with user: {user}")  # 调试信息
    html_content = render_template("protected.html", user=user)
    return HTMLResponse(html_content)


@app.get("/strategies", response_class=HTMLResponse)
async def strategies(request: Request, user: str = Depends(get_current_user)):
    print(f"Strategies page called with user: {user}")  # 调试信息
    html_content = render_template("protected.html", user=user)
    return HTMLResponse(html_content)


@app.get("/positions", response_class=HTMLResponse)
async def positions(request: Request, user: str = Depends(get_current_user)):
    print(f"Positions page called with user: {user}")  # 调试信息
    html_content = render_template("protected.html", user=user)
    return HTMLResponse(html_content)


@app.get("/orders", response_class=HTMLResponse)
async def orders(request: Request, user: str = Depends(get_current_user)):
    html_content = render_template("protected.html", user=user)
    return HTMLResponse(html_content)


@app.get("/risk", response_class=HTMLResponse)
async def risk(request: Request, user: str = Depends(get_current_user)):
    html_content = render_template("protected.html", user=user)
    return HTMLResponse(html_content)


@app.get("/reports/daily", response_class=HTMLResponse)
async def reports_daily(request: Request, user: str = Depends(get_current_user)):
    html_content = render_template("protected.html", user=user)
    return HTMLResponse(html_content)


@app.get("/reports/weekly", response_class=HTMLResponse)
async def reports_weekly(request: Request, user: str = Depends(get_current_user)):
    html_content = render_template("protected.html", user=user)
    return HTMLResponse(html_content)


@app.get("/reports/monthly", response_class=HTMLResponse)
async def reports_monthly(request: Request, user: str = Depends(get_current_user)):
    html_content = render_template("protected.html", user=user)
    return HTMLResponse(html_content)


@app.get("/logout")
async def logout():
    resp = RedirectResponse(url="/login")
    resp.delete_cookie(SESSION_COOKIE)
    return resp


@app.get("/api/me")
async def api_me(user: str = Depends(get_current_user)):
    """Example protected API endpoint using dependency injection.

    Returns 401 if not authenticated (cookie missing or token invalid).
    """
    return {"user": user}


if __name__ == "__main__":
    # Run with: uvicorn pyqmt.web.main:app --reload --port 8000
    import uvicorn
    uvicorn.run("pyqmt.web.main:app", host="127.0.0.1", port=8000, reload=True)
