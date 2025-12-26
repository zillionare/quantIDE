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
        self.header_menu = []

        self.sidebar_menu = [
            {"title": "仪表板", "url": "/home", "icon": "home", "section": "dashboard"},
            {
                "title": "回测",
                "url": "#",
                "icon": "activity",
                "children": [
                    {
                        "title": "策略列表",
                        "url": "/backtest/strategies",
                        "section": "strategies",
                    },
                    {
                        "title": "回测结果",
                        "url": "/backtest/results",
                        "section": "results",
                    },
                ],
            },
            {
                "title": "设置",
                "url": "/settings",
                "icon": "settings",
                "section": "settings",
            },
        ]

    def main_block(self):
        """主内容块，子类需要重写此方法"""
        return Div(H1(self.title), P("这是页面的主要内容区域。"), cls="p-4")

    def render(self):
        """渲染主页面"""

        return Html(
            Head(
                Meta(charset="utf-8"),
                Meta(name="viewport", content="width=device-width, initial-scale=1.0"),
                Title(self.title),
                *(Theme.blue.headers())
            ),
            Body(
                # Header
                header_component(
                    logo="/static/logo.png",
                    brand="PyQMT量化交易系统",
                    nav_items=self.header_menu,
                    user=self.user,
                ),
                Div(
                    sidebar_component(self.sidebar_menu),
                    # 页面主要内容
                    Main(self.main_block(), cls="flex-1 p-6"),
                    cls="flex flex-1",
                ),
                cls="flex flex-col min-h-screen",
            ),
        )
