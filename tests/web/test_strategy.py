from fasthtml.common import to_xml

from quantide.web.pages import strategy as strategy_page


def test_runtime_monitor_polls_only_when_page_is_active(monkeypatch):
    monkeypatch.setattr(
        strategy_page.strategy_runtime_manager,
        "list_runtime_rows",
        lambda: [
            {
                "mode": "live",
                "runtime_id": "runtime-1",
                "portfolio_id": "paper-1",
                "strategy_name": "demo",
                "strategy_id": "runtime-1",
                "status": "idle",
                "total": 100000.0,
                "positions": 0,
                "orders": 0,
                "updated_at": "2026-04-01 12:00:00",
                "can_start": True,
                "can_stop": False,
            }
        ],
    )

    html = to_xml(strategy_page._runtime_table_card())

    assert 'hx-get="/strategy/runtime/table"' in html
    assert "load, every 5s" not in html
    assert "document.visibilityState === 'visible'" in html
    assert "document.hasFocus()" in html
    assert 'hx-swap="innerHTML"' in html
    assert 'hx-post="/strategy/runtime/start"' in html
    assert 'hx-target="#runtime-monitor"' in html


def test_runtime_table_route_returns_refresh_content_without_rebinding_poll(monkeypatch):
    monkeypatch.setattr(
        strategy_page.strategy_runtime_manager,
        "list_runtime_rows",
        lambda: [],
    )

    html = to_xml(strategy_page.runtime_table(None))

    assert 'id="runtime-monitor"' not in html
    assert 'hx-get="/strategy/runtime/table"' not in html
    assert "运行时监控" in html
    assert "暂无运行时实例" in html
