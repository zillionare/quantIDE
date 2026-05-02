"""系统维护 - 行情数据页面测试"""

import datetime

import polars as pl
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


def test_market_defaults_to_latest_page_data(monkeypatch):
    """无筛选条件时默认返回最近一个交易日首页数据"""
    from quantide.web.pages.system import market as market_page

    sample = pl.DataFrame(
        [
            {
                "date": datetime.date(2024, 6, 3),
                "asset": "000001.SZ",
                "open": 10.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.2,
                "volume": 1000.0,
                "amount": 10000.0,
                "up_limit": 11.0,
                "down_limit": 9.0,
                "adjust": 1.0,
                "is_st": False,
            },
            {
                "date": datetime.date(2024, 6, 3),
                "asset": "000002.SZ",
                "open": 20.0,
                "high": 20.5,
                "low": 19.8,
                "close": 20.2,
                "volume": 2000.0,
                "amount": 20000.0,
                "up_limit": 22.0,
                "down_limit": 18.0,
                "adjust": 1.0,
                "is_st": False,
            },
        ]
    )

    monkeypatch.setattr(market_page.daily_bars, "get_bars", lambda *args, **kwargs: sample)

    data, total = market_page._get_market_data(page=1, per_page=20)

    assert total == 2
    assert data[0]["asset"] == "000001.SZ"
    assert data[1]["asset"] == "000002.SZ"
