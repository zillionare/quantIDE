import os
from typing import Optional
import datetime
import logging
from functools import wraps
from typing import Callable, Any
import inspect
from fastapi import Request, Header
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi import HTTPException, status, Depends

import jwt
import cfg4py
from pyqmt.config.schema import Config

# 设置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

SECRET = os.environ.get("PYQMT_SECRET", "dev-secret-change-me")
SESSION_COOKIE = "pyqmt_session"
ALGORITHM = "HS256"
# default expiration in minutes for access tokens
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.environ.get("PYQMT_ACCESS_EXPIRE_MINUTES", "60"))

# 获取配置实例
cfg: Config = cfg4py.get_instance()


def create_jwt(username: str, secret: Optional[str] = None, expires_minutes: Optional[int] = None) -> str:
    """Create a JWT access token with `sub` claim = username.

    Returns a compact JWT (string).
    """
    if secret is None:
        secret = SECRET
    now = datetime.datetime.utcnow()
    if expires_minutes is None:
        expires_minutes = ACCESS_TOKEN_EXPIRE_MINUTES
    exp = now + datetime.timedelta(minutes=expires_minutes)
    payload = {"sub": username, "iat": now, "exp": exp}
    token = jwt.encode(payload, secret, algorithm=ALGORITHM)
    # PyJWT >=2 returns str, older versions may return bytes
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    return token


def verify_jwt(token: str, secret: Optional[str] = None) -> Optional[str]:
    """Verify JWT and return `sub` (username) if valid, otherwise None."""
    if not token:
        return None
    if secret is None:
        secret = SECRET
    try:
        payload = jwt.decode(token, secret, algorithms=[ALGORITHM])
        return payload.get("sub")
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


# Decorator to protect FastAPI endpoints. Usage:
#
# from pyqmt.web.auth import auth
#
# @app.get("/protected")
# @auth
# async def protected(request: Request, user: str):
#     ...
#


def auth(func: Callable) -> Callable:
    """Decorator to protect a FastAPI path operation by verifying JWT from cookie.

    If the token is invalid or missing, returns a RedirectResponse to /login.
    If valid, calls the underlying function and injects `user` as a keyword argument.
    Works with sync and async handlers.
    """

    @wraps(func)
    async def _async_wrapper(*args, **kwargs) -> Any:
        logger.debug(f"Auth decorator called for function: {func.__name__}")
        # FastAPI passes Request as a parameter; try to find it
        request: Request | None = None
        for a in args:
            if isinstance(a, Request):
                request = a
                break
        if request is None:
            request = kwargs.get("request")
        logger.debug(f"Request object: {request}")
        
        if request is None:
            # Cannot authenticate without Request
            logger.warning("Cannot authenticate without Request object")
            return RedirectResponse(url="/login")

        token = request.cookies.get(SESSION_COOKIE)
        logger.debug(f"Token from cookie: {token}")
        user = verify_jwt(token) if token else None
        logger.debug(f"User from token: {user}")
        
        if not user:
            # 保存用户尝试访问的原始URL，登录后可以跳转回去
            original_url = str(request.url)
            logger.debug(f"User not authenticated, redirecting to login with next={original_url}")
            return RedirectResponse(url=f"/login?next={original_url}")

        # inject user kwarg if the function accepts it
        if "user" in inspect.signature(func).parameters:
            kwargs["user"] = user
            logger.debug(f"Injected user parameter: {user}")

        # call underlying function (may be sync or async)
        logger.debug(f"Calling function {func.__name__} with args={args}, kwargs={kwargs}")
        if inspect.iscoroutinefunction(func):
            result = await func(*args, **kwargs)
        else:
            # run in threadpool for sync function
            from fastapi.concurrency import run_in_threadpool

            result = await run_in_threadpool(func, *args, **kwargs)
        logger.debug(f"Function {func.__name__} returned: {result}")
        return result

    return _async_wrapper



async def get_current_user(request: Request) -> str:
    """FastAPI dependency that returns the authenticated username from cookie.

    Raises HTTPException 401 if missing/invalid.
    """
    token: Optional[str] = request.cookies.get(SESSION_COOKIE)
    user = verify_jwt(token) if token else None
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    return user
