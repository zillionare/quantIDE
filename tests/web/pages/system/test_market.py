"""系统维护 - 行情数据页面测试"""

from starlette.testclient import TestClient

import pytest


@pytest.fixture(scope="module")
def client():
    """创建测试客户端"""
    from quantide.app_factory import create_app

    app = create_app(enforce_single_instance=False)
    with TestClient(app) as c:
        yield c


class TestMarketPage:
    """行情数据页面测试"""

    def test_market_page_ok(self, client):
        """页面返回 200"""
        resp = client.get("/system/market/", follow_redirects=True)
        assert resp.status_code == 200

    def test_market_redirect(self, client):
        """不带尾部斜杠时 303 重定向"""
        resp = client.get("/system/market", follow_redirects=False)
        assert resp.status_code == 303
        assert "/system/market/" in resp.headers["location"]

    def test_market_has_layout(self, client):
        """页面包含布局元素"""
        resp = client.get("/system/market/", follow_redirects=True)
        assert "<nav" in resp.text.lower()
        assert "<aside" in resp.text.lower()

    def test_market_has_filter(self, client):
        """页面包含筛选条件"""
        resp = client.get("/system/market/", follow_redirects=True)
        assert "证券代码" in resp.text

    def test_market_has_adjust_buttons(self, client):
        """页面包含复权按钮"""
        resp = client.get("/system/market/", follow_redirects=True)
        assert "不复权" in resp.text
        assert "前复权" in resp.text
