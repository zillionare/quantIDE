from fasthtml.common import *
from monsterui.all import *

from ..components.header import header_component
from ..components.sidebar import sidebar_component
from .base import BaseLayout


class MainLayout(BaseLayout):
    """主页面布局，包含header和sidebar"""

    def __init__(self, title: str = "PyQMT系统", user: str | None = None):
        super().__init__(title)
        self.user = user
        self.header_menu = [
            ("交易", "/trade/simulation"),
            ("策略", "/strategy"),
            ("系统", "/system"),
        ]

        self.sidebar_menu = [
            {
                "title": "行情数据",
                "children": [
                    {"title": "交易日历", "url": "/system/calendar"},
                    {"title": "股票列表", "url": "/system/stocks"},
                    {"title": "计划任务", "url": "/system/jobs"},
                    {"title": "数据库管理", "url": "/system/db"},
                    {"title": "配置管理", "url": "/system/config"},
                ],
            }
        ]

    def main_block(self):
        """主内容块，子类需要重写此方法"""
        return Div(H1(self.title), P("这是页面的主要内容区域。"), cls="p-4")

    def render(self):
        """渲染主页面"""
        # FastHTML will automatically wrap this in Html/Head/Body if we return Title/Meta/etc as tuple
        # But since we want to control the layout structure (Header, Sidebar, Main), we construct Body content

        return (
            Title(self.title),
            # Meta tags are handled by FastHTML default if we don't use Html() wrapper
            # But we might want custom ones

            # Explicitly add Theme headers
            *Theme.blue.headers(),

            # Body content
            Div(
                # Header
                header_component(
                    logo="/static/logo.png",
                    brand="匡醍",
                    nav_items=self.header_menu,
                    user=self.user,
                ),
                # Main content area (Sidebar + Main)
                Div(
                    # Sidebar
                    sidebar_component(self.sidebar_menu),
                    # Main content
                    Main(self.main_block(), cls="flex-1 p-6 w-full"),

                    cls="flex max-w-[1280px] mx-auto w-full flex-1",
                ),
                cls="flex flex-col min-h-screen",
            )
        )
