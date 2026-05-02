"""系统维护 - 交易日历页面测试"""

from starlette.testclient import TestClient

import pytest


@pytest.fixture(scope="module")
def client():
    """创建测试客户端"""
    from quantide.app_factory import create_app

    app = create_app(enforce_single_instance=False)
    with TestClient(app) as c:
        yield c


class TestCalendarPage:
    """交易日历页面测试"""

    def test_calendar_page_ok(self, client):
        """页面返回 200"""
        resp = client.get("/system/calendar/", follow_redirects=True)
        assert resp.status_code == 200

    def test_calendar_redirect(self, client):
        """不带尾部斜杠时 303 重定向"""
        resp = client.get("/system/calendar", follow_redirects=False)
        assert resp.status_code == 303
        assert "/system/calendar/" in resp.headers["location"]

    def test_calendar_has_layout(self, client):
        """页面包含布局元素（header, sidebar）"""
        resp = client.get("/system/calendar/", follow_redirects=True)
        assert "<title>" in resp.text.lower()
        assert "<nav" in resp.text.lower()
        assert "<aside" in resp.text.lower()

    def test_calendar_has_content(self, client):
        """页面包含日历内容"""
        resp = client.get("/system/calendar/", follow_redirects=True)
        assert "交易日历" in resp.text

    def test_calendar_navigation_links(self, client):
        """页面包含导航链接"""
        resp = client.get("/system/calendar/?year=2024&month=6", follow_redirects=True)
        assert "year=2023" in resp.text  # 上一年
        assert "year=2025" in resp.text  # 下一年

    def test_calendar_selectors_auto_submit(self, client):
        """年月选择器变更后自动提交"""
        resp = client.get("/system/calendar/?year=2024&month=6", follow_redirects=True)
        assert 'id="year-select"' in resp.text
        assert 'id="month-select"' in resp.text
        assert 'onchange="this.form.submit()"' in resp.text
