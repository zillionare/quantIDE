"""应用程序初始化向导服务

提供初始化状态管理、配置保存、历史数据下载等功能。
"""

import datetime
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from loguru import logger

from quantide.core.init_wizard_steps import (
    WIZARD_FINAL_STEP,
    WIZARD_TOTAL_STEPS,
    build_wizard_steps,
)
from quantide.config.paths import get_app_db_path, normalize_data_home
from quantide.config.settings import (
    get_data_source,
    get_dingtalk_access_token,
    get_dingtalk_keyword,
    get_dingtalk_secret,
    get_mail_receivers,
    get_mail_sender,
    get_mail_server,
    get_settings,
    get_tushare_token,
)
from quantide.data.fetchers.registry import register_builtin_fetchers
from quantide.data.models.app_state import AppState
from quantide.data.sqlite import db
from quantide.web.auth.manager import AuthManager


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
        settings = get_settings()

        state.app_home = settings.app_home
        state.app_host = settings.app_host
        state.app_port = settings.app_port
        state.app_prefix = settings.app_prefix

        base_url = settings.gateway_base_url.strip()
        parsed = urllib.parse.urlparse(base_url)
        state.gateway_base_url = parsed.path or "/"
        state.gateway_scheme = parsed.scheme or settings.gateway_scheme or "http"
        state.gateway_server = parsed.hostname or ""
        state.gateway_port = parsed.port or settings.gateway_port
        state.gateway_enabled = bool(parsed.hostname)
        state.gateway_username = settings.gateway_username
        state.gateway_password = settings.gateway_password
        state.gateway_timeout = int(settings.gateway_timeout)
        state.livequote_mode = settings.livequote_mode
        state.runtime_mode = settings.runtime_mode
        state.runtime_market_adapter = settings.runtime_market_adapter
        state.runtime_broker_adapter = settings.runtime_broker_adapter
        state.gateway_api_key = settings.gateway_api_key
        state.data_source = settings.data_source
        state.notify_dingtalk_access_token = get_dingtalk_access_token()
        state.notify_dingtalk_secret = get_dingtalk_secret()
        state.notify_dingtalk_keyword = get_dingtalk_keyword()
        state.notify_mail_to = ",".join(get_mail_receivers())
        state.notify_mail_from = get_mail_sender()
        state.notify_mail_server = get_mail_server()
        state.tushare_token = get_tushare_token()
        state.epoch = settings.epoch
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
            db["app_state"].upsert(self._state.to_dict(), pk=AppState.__pk__)  # type: ignore[arg-type]
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
        live_trading_available = self._is_gateway_available(state)

        return {
            "backtest": state.can_use_backtest(),
            "simulation": live_trading_available,
            "live_trading": live_trading_available,
        }

    def _is_gateway_available(self, state: AppState) -> bool:
        """判断 gateway 是否处于可用状态。"""
        if not state.can_use_live_trading():
            return False
        if not str(state.gateway_server or "").strip():
            return False
        if not str(state.gateway_api_key or "").strip():
            return False

        ok, _ = self.test_gateway_connection(
            server=state.gateway_server,
            port=state.gateway_port,
            prefix=state.gateway_base_url or "/",
            timeout=min(float(state.gateway_timeout or 3), 1.0),
        )
        return ok

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
            step: 当前步骤（1-6）
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
        normalized_home = normalize_data_home(home)

        state = self.get_state()
        state.app_home = normalized_home
        state.app_host = host.strip()
        state.app_port = int(port)
        state.app_prefix = prefix.strip() or "/"
        self.save_state(state)
        auth = AuthManager.get_instance()
        if auth is not None:
            db_path = str(get_app_db_path().resolve())
            if getattr(auth.auth_db, "db_path", None) != db_path:
                auth.rebind_database(db_path)
        logger.info("运行环境配置已保存")

    def save_gateway_config(
        self,
        enabled: bool,
        server: str,
        port: int,
        prefix: str,
        api_key: str,
        timeout: int | None = None,
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
        normalized_server = str(server or "").strip()
        normalized_api_key = str(api_key or "").strip()
        if enabled and not normalized_server:
            raise ValueError("启用 gateway 时必须填写服务器地址")
        if enabled and not normalized_api_key:
            raise ValueError("启用 gateway 时必须填写访问密钥")
        parsed = urllib.parse.urlparse(normalized_server)
        if parsed.scheme and parsed.hostname:
            state.gateway_scheme = parsed.scheme
            state.gateway_server = parsed.hostname
            if parsed.port is not None:
                port = parsed.port
        else:
            state.gateway_scheme = "http"
            state.gateway_server = normalized_server
        state.gateway_enabled = bool(enabled)
        state.gateway_port = int(port)
        state.gateway_base_url = prefix.strip() or "/"
        state.gateway_api_key = normalized_api_key
        if timeout is not None:
            state.gateway_timeout = max(1, int(timeout))
        self.save_state(state)
        logger.info("网关配置已保存")

    def save_admin_password(self, password: str) -> None:
        """保存管理员密码。

        Args:
            password: 新管理员密码。
        """
        secret = str(password or "").strip()
        # 密码长度不再限制，仅做非空检查
        if not secret:
            raise ValueError("管理员密码不能为空")

        auth = AuthManager.get_instance() or AuthManager()
        if auth.user_repo is None:
            auth.initialize()
        repo = auth.user_repo
        if repo is None:
            raise RuntimeError("认证仓库尚未初始化")

        admin = repo.get_by_username("admin")
        if admin is None:
            repo.create(
                username="admin",
                email="admin@system.local",
                password=secret,
                role="admin",
            )
            logger.info("初始化向导已创建管理员账号")
            return

        if admin.id is None:
            raise RuntimeError("管理员账号缺少主键")
        if not repo.update(admin.id, password=secret):
            raise RuntimeError("更新管理员密码失败")
        logger.info("初始化向导已更新管理员密码")

    def save_data_init_config(
        self,
        epoch: datetime.date,
        tushare_token: str,
        history_years: int,
        data_source: str = "tushare",
    ) -> None:
        """保存数据初始化配置.

        Args:
            epoch: 数据起点日期。
            data_source: 当前数据源。
            tushare_token: tushare token。
            history_years: 历史下载年数。
        """
        years = max(1, int(history_years))
        normalized_source = (
            str(data_source or get_data_source() or "tushare").strip().lower()
            or "tushare"
        )
        normalized_token = str(tushare_token or "").strip()
        registry = register_builtin_fetchers()
        if normalized_source not in registry.list_names():
            raise ValueError(f"不支持的数据源: {normalized_source}")
        if normalized_source == "tushare" and not normalized_token:
            raise ValueError("必须填写 Tushare Token")
        state = self.get_state()
        state.epoch = epoch
        state.data_source = normalized_source
        state.tushare_token = normalized_token
        state.history_years = years
        state.history_start_date = self._compute_history_start_date(epoch, years)
        self.save_state(state)
        logger.info("数据初始化配置已保存")

    def test_gateway_connection(
        self, server: str, port: int, prefix: str = "/", timeout: float = 3.0
    ) -> tuple[bool, str]:
        """测试网关连通性.

        调用 http://gateway:port/prefix/ping 检查是否能返回200.

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

        # 构建 ping URL
        host = str(server).strip()
        p = str(prefix or "/").strip() or "/"
        if not p.startswith("/"):
            p = "/" + p
        if not p.endswith("/"):
            p = p + "/"
        url = f"http://{host}:{int(port)}{p}ping"

        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                code = resp.getcode()
                if code == 200:
                    return True, f"连通性测试通过（{url} -> {code}）"
                else:
                    return False, f"网关返回非200状态码: {code}"
        except urllib.error.HTTPError as e:
            logger.warning(f"网关连通性测试失败: {url}, HTTP {e.code}")
            return False, f"无法连接 gateway（HTTP {e.code}），请检查配置或暂时不勾选启用"
        except Exception as e:
            logger.warning(f"网关连通性测试失败: {url}, {e}")
            return False, "无法连接 gateway，请检查 server/port/prefix 或网络策略，或暂时不勾选启用"

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
        state.init_step = WIZARD_FINAL_STEP
        self.save_state(state)
        logger.info("初始化流程完成")

    def get_completion_redirect(self) -> str:
        """获取初始化完成后的跳转目标.

        Returns:
            str: 目标路径。
        """
        return "/"

    def get_progress(self) -> dict[str, Any]:
        """获取初始化进度信息

        Returns:
            包含进度信息的字典
        """
        state = self.get_state()

        return {
            "current_step": state.init_step,
            "total_steps": WIZARD_TOTAL_STEPS,
            "init_completed": state.init_completed,
            "steps": build_wizard_steps(state.init_step),
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
