import copy

from fasthtml.common import *
from monsterui.all import *

from quantide.core.enums import BrokerKind
from quantide.service.registry import BrokerRegistry
from quantide.service.init_wizard import init_wizard

from ..components.header import header_component
from ..components.sidebar import sidebar_component
from .base import BaseLayout


HOME_MENU = [
    {
        "title": "概览",
        "url": "/",
        "icon_path": "M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6",
    },
    {
        "title": "账号管理",
        "url": "/system/accounts",
        "icon_path": "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z",
    },
    {
        "title": "交易记录",
        "url": "/trade/records",
        "icon_path": "M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z",
    },
]

SYSTEM_MAINTENANCE_MENU = [
    {
        "title": "数据维护",
        "icon": "database",
        "children": [
            {"title": "交易日历", "url": "/system/calendar", "icon_path": "M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"},
            {"title": "股票列表", "url": "/system/stocks", "icon_path": "M4 6h16M4 10h16M4 14h16M4 18h16"},
            {"title": "行情数据", "url": "/system/market", "icon_path": "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"},
        ],
    },
    {
        "title": "系统设置",
        "icon": "settings",
        "children": [
            {"title": "定时任务", "url": "/system/jobs", "icon_path": "M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"},
            {"title": "交易网关", "url": "/system/gateway", "icon_path": "M13 10V3L4 14h7v7l9-11h-7z"},
            {"title": "数据源", "url": "/system/datasource", "icon_path": "M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4"},
        ],
    },
]

TRADING_MENU = [
    {"title": "下单", "url": "/trade", "icon_path": "M13 7h8m0 0v8m0-8l-8 8-4-4-6 6"},
    {"title": "历史持仓", "url": "/trade/positions/history", "icon_path": "M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4"},
    {"title": "历史委托", "url": "/trade/orders/history", "icon_path": "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"},
    {"title": "历史成交", "url": "/trade/records/history", "icon_path": "M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"},
    {"title": "账号管理", "url": "/system/accounts", "icon_path": "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"},
]

MARKET_MENU = [
    {"title": "股票列表", "url": "/system/stocks", "icon_path": "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"},
    {"title": "自选股", "url": "/system/watchlist", "icon_path": "M11.049 2.927c.3-.921 1.603-.921 1.902 0l1.519 4.674a1 1 0 00.95.69h4.915c.969 0 1.371 1.24.588 1.81l-3.976 2.888a1 1 0 00-.363 1.118l1.518 4.674c.3.922-.755 1.688-1.538 1.118l-3.976-2.888a1 1 0 00-1.176 0l-3.976 2.888c-.783.57-1.838-.197-1.538-1.118l1.518-4.674a1 1 0 00-.363-1.118l-3.976-2.888c-.784-.57-.38-1.81.588-1.81h4.914a1 1 0 00.951-.69l1.519-4.674z"},
]

STRATEGY_MENU = [
    {"title": "策略列表", "url": "/strategy", "icon_path": "M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2"},
    {"title": "回测管理", "url": "/strategy/backtest", "icon_path": "M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"},
    {"title": "实盘运行", "url": "/strategy/live", "icon_path": "M13 10V3L4 14h7v7l9-11h-7z"},
]

ANALYSIS_MENU = [
    {"title": "分析导航", "url": "/analysis", "icon_path": "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"},
    {"title": "收益分析", "url": "/analysis/returns", "icon_path": "M7 12l3-3 3 3 4-4M8 21l4-4 4 4M3 4h18M4 4h16v12a1 1 0 01-1 1H5a1 1 0 01-1-1V4z"},
    {"title": "风险分析", "url": "/analysis/risk", "icon_path": "M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"},
    {"title": "交易统计", "url": "/analysis/trades", "icon_path": "M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"},
]

SIDEBAR_MENUS = {
    "首页": HOME_MENU,
    "系统维护": SYSTEM_MAINTENANCE_MENU,
    "数据管理": SYSTEM_MAINTENANCE_MENU,
    "仿真": TRADING_MENU,
    "实盘": TRADING_MENU,
    "交易": TRADING_MENU,
    "行情": MARKET_MENU,
    "策略": STRATEGY_MENU,
    "分析": ANALYSIS_MENU,
}

HEADER_MENU = [
    {"title": "策略", "url": "/strategy"},
    {"title": "系统维护", "url": "/system"},
    {"title": "实盘", "url": "/live"},
    {"title": "仿真", "url": "/papertrade"},
]


def build_header_menu(trade_enabled: bool) -> list[dict[str, object]]:
    menu = copy.deepcopy(HEADER_MENU)
    if trade_enabled:
        return menu

    for item in menu:
        if item["title"] in {"实盘", "仿真"}:
            item["requires_gateway"] = True
    return menu


def _menu_titles(menu_items: list[dict[str, object]]) -> set[str]:
    return {
        str(item.get("title", "")).strip()
        for item in menu_items
        if str(item.get("title", "")).strip()
    }


class MainLayout(BaseLayout):
    """主页面布局，包含 header 和 sidebar。"""

    def __init__(self, title: str = "Quantide系统", user: str | None = None):
        super().__init__(page_title=title)
        self.title = title
        self.user = user
        self.header_accounts: list[dict] = []
        self.active_account: dict | None = None
        self.header_active = ""
        self.header_menu = copy.deepcopy(HEADER_MENU)
        self.sidebar_menu: list[dict] | None = None
        self._sidebar_active_url: str = "/"

    def set_sidebar_active(self, url: str):
        """设置当前高亮的 sidebar 菜单项。"""
        self._sidebar_active_url = url

    def _resolve_header_active(self) -> str:
        explicit = str(self.header_active or "").strip()
        available_titles = _menu_titles(self.header_menu)
        if explicit in available_titles:
            return explicit

        inferred = self._infer_header_active()
        if inferred:
            return inferred

        if self._trade_entries_enabled():
            return "实盘"

        return "策略"

        legacy_aliases = {
            "数据管理": "系统维护",
        }
        return legacy_aliases.get(explicit, "")

    def _infer_header_active(self) -> str:
        url = str(self._sidebar_active_url or "").strip()
        title = str(self.title or "")

        if url.startswith("/data") or url.startswith("/system"):
            return "系统维护"
        if url.startswith("/strategy") or "策略" in title:
            return "策略"
        if url.startswith("/trade/live") or url.startswith("/live") or "实盘" in title:
            return "实盘"
        if url.startswith("/trade/simulation") or url.startswith("/papertrade") or "仿真" in title:
            return "仿真"
        if url.startswith("/trade"):
            return "仿真"

        return ""

    def _get_sidebar_menu(self) -> list[dict]:
        """根据当前选中的一级菜单获取对应的 sidebar 菜单。"""
        menu_source = self.sidebar_menu
        if menu_source is None:
            menu_key = self._resolve_header_active() or "首页"
            menu_source = SIDEBAR_MENUS.get(menu_key, SIDEBAR_MENUS["首页"])

        menu = copy.deepcopy(menu_source)
        if self._sidebar_active_url and self._sidebar_active_url != "/":
            for item in menu:
                item["active"] = item.get("url") == self._sidebar_active_url
                if "children" in item:
                    for child in item["children"]:
                        child["active"] = child.get("url") == self._sidebar_active_url
        return menu

    def _trade_entries_enabled(self) -> bool:
        try:
            status = init_wizard.get_feature_status()
        except Exception:
            return False
        return bool(status.get("simulation") and status.get("live_trading"))

    def main_block(self):
        """主内容块，子类需要重写此方法。"""
        return Div(H1(self.title), P("这是页面的主要内容区域。"), cls="p-4")

    def render(self):
        """渲染主页面。"""
        accounts = list(self.header_accounts)
        active_account = self.active_account
        if not accounts:
            reg = BrokerRegistry()
            default_account = reg.get_default()
            for kind in [BrokerKind.QMT, BrokerKind.SIMULATION]:
                for info in reg.list_by_kind(kind):
                    account = {
                        "id": info.get("id"),
                        "name": info.get("name") or info.get("id"),
                        "kind": kind.value,
                        "label": "实盘" if kind == BrokerKind.QMT else "仿真",
                        "status": info.get("status", False),
                        "is_live": kind == BrokerKind.QMT,
                        "switch_url": f"/home?kind={kind.value}&id={info.get('id')}",
                    }
                    accounts.append(account)
                    if default_account and default_account[0] == kind.value and default_account[1] == info.get("id"):
                        active_account = account

        from quantide.web.theme import AppTheme

        return (
            Title(self.title),
            *AppTheme.headers(),
            Div(
                header_component(
                    logo="/static/logo.png",
                    brand="匡醍量化",
                    nav_items=build_header_menu(self._trade_entries_enabled()),
                    user=self.user,
                    accounts=accounts,
                    active_account=active_account,
                    active_title=self._resolve_header_active(),
                ),
                Div(
                    sidebar_component(self._get_sidebar_menu()),
                    Main(self.main_block(), cls="flex-1 p-6 w-full"),
                    cls="flex max-w-[1280px] mx-auto w-full flex-1",
                ),
                cls="flex flex-col min-h-screen",
            ),
        )