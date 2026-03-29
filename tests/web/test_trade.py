"""测试交易模块页面"""
from email.message import Message
import urllib.error
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from starlette.responses import RedirectResponse
from starlette.testclient import TestClient
from starlette.routing import Route
from starlette.staticfiles import StaticFiles
from starlette.middleware import Middleware
from fasthtml.common import fast_app, Mount
from monsterui.all import Theme

from pyqmt.core.enums import BrokerKind
from pyqmt.data.sqlite import db as _db
from pyqmt.service.registry import BrokerRegistry
from pyqmt.service.sim_broker import SimulationBroker


@pytest.fixture(scope="module")
def test_app(cfg):
    """创建测试应用"""
    from pyqmt.config import init_config
    from pyqmt.core.scheduler import scheduler
    from pyqmt.service.livequote import live_quote

    init_config()

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        test_db_path = f.name

    try:
        _db._initialized = False
        _db.init(test_db_path)

        scheduler.start()
        live_quote.start()

        reg = BrokerRegistry()
        try:
            sim_broker = SimulationBroker(portfolio_id="sim_demo", portfolio_name="演示账户", principal=1000000)
            reg.register(BrokerKind.SIMULATION, "sim_demo", sim_broker)
        except Exception as e:
            print(f"Failed to create demo broker: {e}")

        from pyqmt.web.auth.manager import AuthManager
        from pyqmt.web.middleware import BrokerRegistryMiddleware, exception_handler
        from pyqmt.core.errors import BaseTradeError
        from pyqmt.web.pages.home import home_app
        from pyqmt.web.pages.trade import trade_app
        from pyqmt.web.pages.live import live_app
        from pyqmt.web.apis.broker import app as broker_api_app

        auth = AuthManager(db_path=test_db_path, config={"login_path": "/auth/login"})

        app, rt = fast_app(
            hdrs=tuple(Theme.blue.headers()),
            before=auth.create_beforeware(),
            middleware=(Middleware(BrokerRegistryMiddleware, registry=reg),),
            exception_handlers={
                Exception: exception_handler,
                BaseTradeError: exception_handler,
            },
            routes=(
                Route("/login", lambda req: RedirectResponse("/auth/login", status_code=303), methods=["GET"]),
                Route("/login/", lambda req: RedirectResponse("/auth/login", status_code=303), methods=["GET"]),
                Mount("/home", home_app),
                Mount("/trade/simulation", trade_app),
                Mount("/trade/live", live_app),
                Mount("/broker", broker_api_app),
                Mount("/", home_app),
            ),
        )

        static_dir = Path(__file__).resolve().parent.parent.parent / "pyqmt" / "web" / "static"
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

        auth.initialize(app, prefix="/auth")

        yield app
    finally:
        import os
        try:
            os.unlink(test_db_path)
        except OSError:
            pass


@pytest.fixture
def test_client(test_app):
    """创建测试客户端"""
    client = TestClient(test_app)
    yield client


@pytest.fixture
def auth_headers():
    """认证头"""
    return {"Authorization": "Bearer test_token"}


class TestSimulationTrade:
    """仿真交易测试"""

    def test_simulation_list_page(self, test_client):
        """测试仿真账户列表页面"""
        response = test_client.get("/trade/simulation", follow_redirects=False)
        assert response.status_code in [200, 302, 303, 404]


class TestLoginRoutes:
    """测试登录入口兼容性."""

    def test_login_redirect_path(self, test_client):
        response = test_client.get("/login", follow_redirects=False)

        assert response.status_code == 303
        assert response.headers["location"] == "/auth/login"

    def test_login_slash_redirect_path(self, test_client):
        response = test_client.get("/login/", follow_redirects=False)

        assert response.status_code == 303
        assert response.headers["location"] == "/auth/login"

    def test_auth_login_get_page(self, test_client):
        response = test_client.get("/auth/login", follow_redirects=False)

        assert response.status_code == 200
        assert "匡醍量化" in response.text
        assert "business@quantide.cn" in response.text

    def test_auth_login_post_succeeds(self, test_client):
        response = test_client.post(
            "/auth/login",
            data={"username": "admin", "password": "admin123"},
            follow_redirects=False,
        )

        assert response.status_code == 303
        assert response.headers["location"] == "/"

    def test_home_tolerates_broker_asset_errors(self, test_client, monkeypatch):
        headers = Message()

        class BrokenBroker:
            portfolio_id = "gateway"

            @property
            def asset(self):
                raise urllib.error.HTTPError(
                    url="http://localhost:8000/api/trade/asset",
                    code=404,
                    msg="Not Found",
                    hdrs=headers,
                    fp=None,
                )

            @property
            def positions(self):
                raise urllib.error.HTTPError(
                    url="http://localhost:8000/api/trade/positions",
                    code=404,
                    msg="Not Found",
                    hdrs=headers,
                    fp=None,
                )

        from pyqmt.web.pages import home as home_page

        monkeypatch.setattr(home_page, "_get_broker", lambda req: BrokenBroker())

        with test_client as client:
            login = client.post(
                "/auth/login",
                data={"username": "admin", "password": "admin123"},
                follow_redirects=False,
            )
            assert login.status_code == 303

            response = client.get("/", follow_redirects=False)

        assert response.status_code == 200
        assert "首页" in response.text

    def test_create_simulation_account_modal(self, test_client):
        """测试创建仿真账户对话框"""
        response = test_client.get("/trade/simulation/create")
        assert response.status_code == 200
        assert "创建仿真账户" in response.text or "账户名称" in response.text

    def test_create_simulation_account(self, test_client):
        """测试创建仿真账户"""
        data = {
            "name": "测试账户",
            "principal": "1000000",
            "commission": "0.0001",
            "market_value_update_interval": "10",
            "info": "测试描述",
        }
        response = test_client.post("/trade/simulation/create", data=data, follow_redirects=False)
        assert response.status_code in [200, 302, 303]

    def test_simulation_account_detail(self, test_client):
        """测试仿真账户详情页面"""
        registry = BrokerRegistry()
        portfolio_id = "test_sim_account"

        try:
            broker = SimulationBroker.create(
                portfolio_id=portfolio_id,
                portfolio_name="测试账户",
                principal=1000000,
            )
            registry.register(BrokerKind.SIMULATION, portfolio_id, broker)

            response = test_client.get(f"/trade/simulation/{portfolio_id}")
            assert response.status_code == 200
            assert "持仓信息" in response.text or "总资产" in response.text
        finally:
            registry.unregister(BrokerKind.SIMULATION, portfolio_id)

    def test_get_positions(self, test_client):
        """测试获取持仓信息"""
        registry = BrokerRegistry()
        portfolio_id = "test_positions"

        try:
            broker = SimulationBroker.create(
                portfolio_id=portfolio_id,
                portfolio_name="测试账户",
                principal=1000000,
            )
            registry.register(BrokerKind.SIMULATION, portfolio_id, broker)

            response = test_client.get(f"/trade/simulation/{portfolio_id}/positions")
            assert response.status_code == 200
            assert "持仓信息" in response.text or "暂无持仓" in response.text
        finally:
            registry.unregister(BrokerKind.SIMULATION, portfolio_id)


class TestLiveTrade:
    """实盘交易测试"""

    def test_live_list_page(self, test_client):
        """测试实盘账户列表页面"""
        response = test_client.get("/trade/live", follow_redirects=False)
        assert response.status_code in [200, 302, 303, 404]

    def test_create_live_account_modal(self, test_client):
        """测试创建实盘账户对话框"""
        response = test_client.get("/trade/live/create")
        assert response.status_code == 200
        assert "Gateway" in response.text or "qmt-gateway" in response.text


class TestBrokerRegistry:
    """测试 BrokerRegistry"""

    def test_register_and_get(self):
        """测试注册和获取 broker"""
        registry = BrokerRegistry()
        portfolio_id = "test_registry"

        try:
            broker = SimulationBroker.create(
                portfolio_id=portfolio_id,
                portfolio_name="测试",
                principal=1000000,
            )
            registry.register(BrokerKind.SIMULATION, portfolio_id, broker)

            retrieved = registry.get(BrokerKind.SIMULATION, portfolio_id)
            assert retrieved is not None
            assert retrieved.portfolio_id == portfolio_id
        finally:
            registry.unregister(BrokerKind.SIMULATION, portfolio_id)

    def test_list_by_kind(self):
        """测试按类型列出 broker"""
        registry = BrokerRegistry()
        portfolio_id = "test_list_kind"

        try:
            broker = SimulationBroker.create(
                portfolio_id=portfolio_id,
                portfolio_name="测试",
                principal=1000000,
            )
            registry.register(BrokerKind.SIMULATION, portfolio_id, broker)

            brokers = registry.list_by_kind(BrokerKind.SIMULATION)
            assert len(brokers) > 0
            assert any(b["id"] == portfolio_id for b in brokers)
        finally:
            registry.unregister(BrokerKind.SIMULATION, portfolio_id)

    def test_unregister(self):
        """测试注销 broker"""
        registry = BrokerRegistry()
        portfolio_id = "test_unregister"

        broker = SimulationBroker.create(
            portfolio_id=portfolio_id,
            portfolio_name="测试",
            principal=1000000,
        )
        registry.register(BrokerKind.SIMULATION, portfolio_id, broker)

        assert registry.get(BrokerKind.SIMULATION, portfolio_id) is not None

        registry.unregister(BrokerKind.SIMULATION, portfolio_id)
        assert registry.get(BrokerKind.SIMULATION, portfolio_id) is None


class TestSimulationBroker:
    """测试 SimulationBroker"""

    def test_create_with_custom_interval(self):
        """测试使用自定义市值更新间隔创建账户"""
        portfolio_id = "test_interval"
        broker = SimulationBroker.create(
            portfolio_id=portfolio_id,
            portfolio_name="测试",
            principal=1000000,
            market_value_update_interval=5.0,
        )

        assert broker._market_value_update_interval == 5.0

        from pyqmt.service.registry import BrokerRegistry

        registry = BrokerRegistry()
        registry.unregister(BrokerKind.SIMULATION, portfolio_id)

    def test_default_interval(self):
        """测试默认市值更新间隔"""
        portfolio_id = "test_default_interval"
        broker = SimulationBroker.create(
            portfolio_id=portfolio_id,
            portfolio_name="测试",
            principal=1000000,
        )

        assert broker._market_value_update_interval == 10.0

        from pyqmt.service.registry import BrokerRegistry

        registry = BrokerRegistry()
        registry.unregister(BrokerKind.SIMULATION, portfolio_id)
