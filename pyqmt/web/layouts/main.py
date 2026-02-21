from fasthtml.common import *
from monsterui.all import *

from pyqmt.core.enums import BrokerKind
from pyqmt.service.registry import BrokerRegistry

from ..components.header import header_component
from ..components.sidebar import sidebar_component
from .base import BaseLayout


class MainLayout(BaseLayout):
    """主页面布局，包含header和sidebar"""

    def __init__(self, title: str = "PyQMT系统", user: str | None = None):
        super().__init__(title)
        self.user = user
        self.header_accounts: list[dict] = []
        self.active_account: dict | None = None
        self.header_active = "首页"
        self.header_menu = [
            ("首页", "/"),
            ("交易", "/trade/simulation"),
            ("行情", "/system/stocks"),
            ("策略", "/strategy"),
            ("分析", "/analysis"),
        ]

        self.sidebar_menu = [
            {
                "title": "概览",
                "url": "/",
                "icon_path": "M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6",
                "active": True,
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
            {
                "title": "系统设置",
                "url": "/system",
                "icon_path": "M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z M15 12a3 3 0 11-6 0 3 3 0 016 0z",
            },
        ]

    def main_block(self):
        """主内容块，子类需要重写此方法"""
        return Div(H1(self.title), P("这是页面的主要内容区域。"), cls="p-4")

    def render(self):
        """渲染主页面"""
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

        return (
            Title(self.title),
            *Theme.blue.headers(),
            Div(
                header_component(
                    logo="/static/logo.png",
                    brand="匡醍",
                    nav_items=self.header_menu,
                    user=self.user,
                    accounts=accounts,
                    active_account=active_account,
                    active_title=self.header_active,
                ),
                Div(
                    sidebar_component(self.sidebar_menu),
                    Main(self.main_block(), cls="flex-1 p-6 w-full"),
                    cls="flex max-w-[1280px] mx-auto w-full flex-1",
                ),
                cls="flex flex-col min-h-screen",
            )
        )
