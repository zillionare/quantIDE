"""系统设置 - 交易网关页面测试"""

from starlette.testclient import TestClient

import pytest


@pytest.fixture(scope="module")
def client():
    """创建测试客户端"""
    from quantide.app_factory import create_app

    app = create_app(enforce_single_instance=False)
    with TestClient(app) as c:
        yield c


class TestGatewayPage:
    """交易网关页面测试"""

    def test_gateway_page_ok(self, client):
        """页面返回 200"""
        resp = client.get("/system/gateway/", follow_redirects=True)
        assert resp.status_code == 200

    def test_gateway_redirect(self, client):
        """不带尾部斜杠时 303 重定向"""
        resp = client.get("/system/gateway", follow_redirects=False)
        assert resp.status_code == 303
        assert "/system/gateway/" in resp.headers["location"]

    def test_gateway_has_layout(self, client):
        """页面包含布局元素（header, sidebar）"""
        resp = client.get("/system/gateway/", follow_redirects=True)
        assert "<nav" in resp.text.lower()
        assert "<aside" in resp.text.lower()

    def test_gateway_has_content(self, client):
        """页面包含交易网关内容"""
        resp = client.get("/system/gateway/", follow_redirects=True)
        assert "交易网关" in resp.text
        assert "连接状态" in resp.text or "连接配置" in resp.text

    def test_gateway_has_test_button(self, client):
        """页面包含连接测试按钮"""
        resp = client.get("/system/gateway/", follow_redirects=True)
        assert "连接测试" in resp.text


class TestGatewayConnectionTest:
    """交易网关连接测试功能"""

    def test_gateway_test_endpoint(self, client):
        """测试连接端点返回内容"""
        resp = client.get("/system/gateway/test", follow_redirects=True)
        assert resp.status_code == 200
        # 应该返回状态卡片 HTML
        assert "网关" in resp.text or "连接" in resp.text
