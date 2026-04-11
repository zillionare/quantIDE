"""系统设置 - 定时任务页面测试"""

from starlette.testclient import TestClient

import pytest


@pytest.fixture(scope="module")
def client():
    """创建测试客户端"""
    from quantide.app_factory import create_app

    app = create_app(enforce_single_instance=False)
    with TestClient(app) as c:
        yield c


class TestJobsPage:
    """定时任务页面测试"""

    def test_jobs_page_ok(self, client):
        """页面返回 200"""
        resp = client.get("/system/jobs/", follow_redirects=True)
        assert resp.status_code == 200

    def test_jobs_redirect(self, client):
        """不带尾部斜杠时 303 重定向"""
        resp = client.get("/system/jobs", follow_redirects=False)
        assert resp.status_code == 303
        assert "/system/jobs/" in resp.headers["location"]

    def test_jobs_has_layout(self, client):
        """页面包含布局元素（header, sidebar）"""
        resp = client.get("/system/jobs/", follow_redirects=True)
        assert "<nav" in resp.text.lower()
        assert "<aside" in resp.text.lower()

    def test_jobs_has_content(self, client):
        """页面包含定时任务内容"""
        resp = client.get("/system/jobs/", follow_redirects=True)
        assert "定时任务" in resp.text
        assert "日线数据同步" in resp.text

    def test_jobs_has_table(self, client):
        """页面包含任务表格"""
        resp = client.get("/system/jobs/", follow_redirects=True)
        assert "<table" in resp.text.lower()
        assert "任务名称" in resp.text
        assert "状态" in resp.text
