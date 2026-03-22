"""应用程序初始化向导服务

提供初始化状态管理、配置保存、历史数据下载等功能。
"""

import datetime
import urllib.parse
import urllib.request
from typing import Any

from loguru import logger

from pyqmt.config import cfg
from pyqmt.data.models.app_state import AppState
from pyqmt.data.sqlite import db


class InitWizardService:
    """初始化向导服务

    管理应用程序的初始化流程，包括：
    1. 初始化状态检查
    2. 配置保存和读取
    3. 历史数据下载
    4. 初始化完成标志管理
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._state: AppState | None = None
        self._initialized = True

    def _build_default_state(self) -> AppState:
        """基于 defaults.yml 构建默认状态.

        Returns:
            AppState: 默认初始化状态。
        """
        state = AppState()
        server = getattr(cfg, "server", None)
        state.app_home = str(getattr(cfg, "home", "") or "")
        state.app_host = str(getattr(server, "host", "0.0.0.0") or "0.0.0.0")
        state.app_port = int(getattr(server, "port", 8130) or 8130)
        state.app_prefix = str(getattr(server, "prefix", "/zillionare-qmt") or "/")

        gateway = getattr(cfg, "gateway", None)
        base_url = str(getattr(gateway, "base_url", "") or "").strip()
        parsed = urllib.parse.urlparse(base_url)
        state.gateway_base_url = parsed.path or "/"
        state.gateway_server = parsed.hostname or ""
        state.gateway_port = parsed.port or (443 if parsed.scheme == "https" else 80)
        state.gateway_enabled = bool(parsed.hostname)
        apikeys = getattr(cfg, "apikeys", None)
        clients = getattr(apikeys, "clients", []) or []
        if clients and isinstance(clients, list):
            first = clients[0] or {}
            state.gateway_api_key = str(first.get("key", "") or "")

        notify = getattr(cfg, "notify", None)
        dingtalk = getattr(notify, "dingtalk", None)
        if dingtalk is not None:
            state.notify_dingtalk_access_token = str(
                getattr(dingtalk, "access_token", "") or ""
            )
            state.notify_dingtalk_secret = str(getattr(dingtalk, "secret", "") or "")
            state.notify_dingtalk_keyword = str(getattr(dingtalk, "keyword", "") or "")

        mail = getattr(notify, "mail", None)
        if mail is not None:
            state.notify_mail_to = str(getattr(mail, "mail_to", "") or "")
            state.notify_mail_from = str(getattr(mail, "mail_from", "") or "")
            state.notify_mail_server = str(getattr(mail, "mail_server", "") or "")

        epoch = getattr(cfg, "epoch", datetime.date(2005, 1, 1))
        if isinstance(epoch, str):
            epoch = datetime.datetime.strptime(epoch, "%Y-%m-%d").date()
        state.epoch = epoch
        state.history_years = 3
        state.history_start_date = self._compute_history_start_date(
            epoch=state.epoch,
            years=state.history_years,
        )
        return state

    def _compute_history_start_date(
        self, epoch: datetime.date, years: int
    ) -> datetime.date:
        """计算历史下载起始日期.

        Args:
            epoch: 数据起点。
            years: 下载年数。

        Returns:
            datetime.date: 起始日期。
        """
        years = max(1, years)
        by_years = datetime.date.today() - datetime.timedelta(days=365 * years)
        return max(epoch, by_years)

    def _ensure_db(self):
        """确保数据库已初始化"""
        if not db._initialized:
            raise RuntimeError("数据库未初始化，请先调用 db.init()")

    def get_state(self, force_refresh: bool = False) -> AppState:
        """获取当前应用状态

        Args:
            force_refresh: 是否强制从数据库刷新，忽略缓存

        Returns:
            AppState: 应用状态对象（如果不存在则返回默认状态）
        """
        self._ensure_db()

        if not force_refresh and self._state is not None:
            return self._state

        try:
            row = db["app_state"].get(1)
            if row:
                self._state = AppState.from_dict(dict(row))
                return self._state
        except Exception as e:
            logger.warning(f"读取应用状态失败: {e}")

        self._state = self._build_default_state()
        return self._state

    def save_state(self, state: AppState | None = None) -> None:
        """保存应用状态

        Args:
            state: 要保存的状态，None 则保存当前缓存的状态
        """
        self._ensure_db()

        if state is not None:
            self._state = state

        if self._state is None:
            self._state = AppState()

        self._state.updated_at = datetime.datetime.now()

        try:
            db["app_state"].upsert(self._state.to_dict(), pk="id")
            # 强制提交，确保其他连接能看到更新
            db.conn.commit()
            logger.info("应用状态已保存")
        except Exception as e:
            logger.error(f"保存应用状态失败: {e}")
            raise

    def is_initialized(self) -> bool:
        """检查应用是否已完成初始化

        Returns:
            bool: True 表示已完成初始化
        """
        state = self.get_state()
        return state.is_fully_initialized

    def get_feature_status(self) -> dict[str, bool]:
        """获取各功能的可用状态

        Returns:
            功能状态字典，包含:
            - backtest: 回测功能是否可用
            - simulation: 仿真交易是否可用
            - live_trading: 实盘交易是否可用
        """
        state = self.get_state()

        return {
            "backtest": state.can_use_backtest(),
            "simulation": state.can_use_live_trading(),
            "live_trading": state.can_use_live_trading(),
        }

    def start_initialization(self, reset_step: bool = False) -> AppState:
        """开始初始化流程

        Returns:
            AppState: 当前状态对象
        """
        state = self.get_state(force_refresh=True)
        if state.init_started_at is None:
            state.init_started_at = datetime.datetime.now()
        if reset_step:
            state.init_step = 1
        elif state.init_step <= 0:
            state.init_step = 1
        self.save_state(state)
        logger.info("初始化流程开始")
        return state

    def update_step(self, step: int) -> None:
        """更新当前初始化步骤

        Args:
            step: 当前步骤（1-7）
        """
        state = self.get_state()
        state.init_step = step
        self.save_state(state)
        logger.info(f"初始化步骤更新为: {step}")

    def save_runtime_config(
        self,
        home: str,
        host: str,
        port: int,
        prefix: str,
    ) -> None:
        """保存运行环境配置.

        Args:
            home: home 目录。
            host: 服务地址。
            port: 服务端口。
            prefix: 服务前缀。
        """
        state = self.get_state()
        state.app_home = home.strip()
        state.app_host = host.strip()
        state.app_port = int(port)
        state.app_prefix = prefix.strip() or "/"
        self.save_state(state)
        logger.info("运行环境配置已保存")

    def save_gateway_config(
        self,
        enabled: bool,
        server: str,
        port: int,
        prefix: str,
        api_key: str,
    ) -> None:
        """保存网关配置.

        Args:
            enabled: 是否启用 gateway。
            server: 网关服务地址。
            port: 网关端口。
            prefix: 网关路径前缀。
            api_key: 网关 key。
        """
        state = self.get_state()
        state.gateway_enabled = bool(enabled)
        state.gateway_server = server.strip()
        state.gateway_port = int(port)
        state.gateway_base_url = prefix.strip() or "/"
        state.gateway_api_key = api_key.strip()
        self.save_state(state)
        logger.info("网关配置已保存")

    def save_notify_config(
        self,
        dingtalk_access_token: str,
        dingtalk_secret: str,
        dingtalk_keyword: str,
        mail_to: str,
        mail_from: str,
        mail_server: str,
    ) -> None:
        """保存通知配置.

        Args:
            dingtalk_access_token: 钉钉 token。
            dingtalk_secret: 钉钉 secret。
            dingtalk_keyword: 钉钉 keyword。
            mail_to: 邮件收件人。
            mail_from: 邮件发件人。
            mail_server: 邮件服务器。
        """
        state = self.get_state()
        state.notify_dingtalk_access_token = dingtalk_access_token.strip()
        state.notify_dingtalk_secret = dingtalk_secret.strip()
        state.notify_dingtalk_keyword = dingtalk_keyword.strip()
        state.notify_mail_to = mail_to.strip()
        state.notify_mail_from = mail_from.strip()
        state.notify_mail_server = mail_server.strip()
        self.save_state(state)
        logger.info("通知配置已保存")

    def save_data_init_config(
        self,
        epoch: datetime.date,
        tushare_token: str,
        history_years: int,
    ) -> None:
        """保存数据初始化配置.

        Args:
            epoch: 数据起点日期。
            tushare_token: tushare token。
            history_years: 历史下载年数。
        """
        years = max(1, int(history_years))
        state = self.get_state()
        state.epoch = epoch
        state.tushare_token = tushare_token.strip()
        state.history_years = years
        state.history_start_date = self._compute_history_start_date(epoch, years)
        self.save_state(state)
        logger.info("数据初始化配置已保存")

    def test_gateway_connection(
        self, server: str, port: int, prefix: str = "/", timeout: float = 3.0
    ) -> tuple[bool, str]:
        """测试网关连通性.

        Args:
            server: 网关地址。
            port: 网关端口。
            prefix: 网关路径前缀。
            timeout: 超时秒数。

        Returns:
            tuple[bool, str]: 测试结果与提示信息。
        """
        server = str(server or "").strip()
        if not server:
            return False, "未填写 gateway server"
        normalized = self._compose_gateway_url(server, int(port), prefix)

        targets = [
            f"{normalized}/api/health",
            f"{normalized}/health",
            normalized,
        ]
        for url in targets:
            try:
                req = urllib.request.Request(url, method="GET")
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    code = resp.getcode()
                    if code < 500:
                        return True, f"连通性测试通过（{url} -> {code}）"
            except Exception as e:
                logger.warning(f"网关连通性测试失败: {url}, {e}")
        return False, "无法连接 gateway，请检查 server/port/prefix 或网络策略"

    def _compose_gateway_url(self, server: str, port: int, prefix: str) -> str:
        host = str(server).strip()
        base = self._normalize_gateway_url(f"http://{host}:{int(port)}")
        p = str(prefix or "/").strip() or "/"
        if not p.startswith("/"):
            p = "/" + p
        if p == "/":
            return base
        return f"{base}{p.rstrip('/')}"

    def _normalize_gateway_url(self, base_url: str) -> str:
        value = str(base_url or "").strip()
        if not value or value == "/":
            return ""
        if value.startswith("/"):
            return ""
        parsed = urllib.parse.urlparse(value)
        if parsed.scheme and parsed.netloc:
            return value.rstrip("/")
        if parsed.netloc:
            return f"http:{value}".rstrip("/")
        if parsed.path and "." in parsed.path:
            return f"http://{parsed.path}".rstrip("/")
        return ""

    def complete_initialization(self) -> None:
        """完成初始化流程"""
        state = self.get_state()
        state.init_completed = True
        state.init_completed_at = datetime.datetime.now()
        state.init_step = 7
        self.save_state(state)
        logger.info("初始化流程完成")

    def get_completion_redirect(self) -> str:
        """获取初始化完成后的跳转目标.

        Returns:
            str: 目标路径。
        """
        return "/login"

    def get_progress(self) -> dict[str, Any]:
        """获取初始化进度信息

        Returns:
            包含进度信息的字典
        """
        state = self.get_state()

        steps = [
            {"id": 1, "name": "欢迎", "completed": state.init_step > 1},
            {"id": 2, "name": "运行环境", "completed": state.init_step > 2},
            {"id": 3, "name": "行情与交易网关", "completed": state.init_step > 3},
            {"id": 4, "name": "通知告警", "completed": state.init_step > 4},
            {"id": 5, "name": "数据初始化", "completed": state.init_step > 5},
            {"id": 6, "name": "数据下载", "completed": state.init_step > 6},
            {"id": 7, "name": "完成", "completed": state.init_step >= 7},
        ]

        return {
            "current_step": state.init_step,
            "total_steps": 7,
            "init_completed": state.init_completed,
            "steps": steps,
            "started_at": state.init_started_at,
            "completed_at": state.init_completed_at,
        }

    def reset_initialization(self) -> None:
        """重置初始化状态（用于重新初始化）"""
        state = self._build_default_state()
        self.save_state(state)
        logger.warning("初始化状态已重置")


# 全局服务实例
init_wizard = InitWizardService()
