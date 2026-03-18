"""qmt-gateway 远程调用客户端."""

import json
import urllib.parse
import urllib.request
from http.cookiejar import CookieJar
from typing import Any

from pyqmt.config import cfg


class GatewayClient:
    """qmt-gateway HTTP 会话客户端."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: float = 10.0,
    ):
        """初始化客户端.

        Args:
            base_url: 网关地址。
            username: 用户名。
            password: 密码。
            timeout: 超时秒数。
        """
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self._cookies = CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self._cookies)
        )
        self._logged_in = False

    @classmethod
    def from_config(cls) -> "GatewayClient":
        """从配置创建客户端."""
        gateway = cfg.gateway
        return cls(
            base_url=gateway.base_url,
            username=gateway.username,
            password=gateway.password,
            timeout=float(gateway.timeout),
        )

    def ensure_login(self) -> None:
        """确保会话已登录."""
        if self._logged_in:
            return
        form = urllib.parse.urlencode(
            {
                "username": self.username,
                "password": self.password,
                "auto_login": "false",
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            url=f"{self.base_url}/auth/login",
            data=form,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        with self._opener.open(req, timeout=self.timeout) as resp:
            code = resp.getcode()
            if code < 200 or code >= 400:
                raise RuntimeError(f"gateway login failed: {code}")
        self._logged_in = True

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """调用 GET 接口."""
        self.ensure_login()
        query = ""
        if params:
            query = "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url=f"{self.base_url}{path}{query}",
            method="GET",
        )
        with self._opener.open(req, timeout=self.timeout) as resp:
            body = resp.read().decode("utf-8")
            if not body:
                return None
            return json.loads(body)

    def post_form(self, path: str, data: dict[str, Any]) -> Any:
        """调用 POST 表单接口."""
        self.ensure_login()
        form = urllib.parse.urlencode(data).encode("utf-8")
        req = urllib.request.Request(
            url=f"{self.base_url}{path}",
            data=form,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with self._opener.open(req, timeout=self.timeout) as resp:
            body = resp.read().decode("utf-8")
            if not body:
                return None
            return json.loads(body)

    def cookie_header(self) -> str:
        """导出 Cookie 头字符串."""
        items = []
        for c in self._cookies:
            items.append(f"{c.name}={c.value}")
        return "; ".join(items)

    def ws_url(self, path: str) -> str:
        """生成 WS 地址."""
        base = self.base_url
        if base.startswith("https://"):
            return "wss://" + base.removeprefix("https://").rstrip("/") + path
        if base.startswith("http://"):
            return "ws://" + base.removeprefix("http://").rstrip("/") + path
        return "ws://" + base.rstrip("/") + path
