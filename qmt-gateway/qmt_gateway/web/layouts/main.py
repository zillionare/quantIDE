"""主布局模块

提供带 header 和 sidebar 的主页面布局。
"""

from fasthtml.common import *
from monsterui.all import *

from qmt_gateway.web.theme import AppTheme, PRIMARY_COLOR


def Header(user: dict | None = None):
    """顶部导航栏

    Args:
        user: 当前用户信息
    """
    user_menu = Div(
        Div(
            Div(
                user.get("username", "User")[0].upper() if user else "U",
                cls="w-8 h-8 rounded-full bg-gray-300 flex items-center justify-center text-sm font-bold",
            ),
            cls="dropdown dropdown-end",
        ),
        Ul(
            Li(A("修改密码", href="/auth/change-password")),
            Li(A("退出登录", href="/auth/logout")),
            cls="dropdown-content menu p-2 shadow bg-base-100 rounded-box w-52 mt-4",
        ),
        cls="dropdown dropdown-end",
    ) if user else Div()

    return Div(
        Div(
            # Logo 和 Brand
            Div(
                Span("QMT", cls="text-xl font-bold", style=f"color: {PRIMARY_COLOR};"),
                Span("Gateway", cls="text-xl font-light text-gray-600 ml-1"),
                cls="flex items-center",
            ),
            # 右侧：Alarm 和 User
            Div(
                # Alarm 图标（占位）
                Button(
                    "🔔",
                    cls="btn btn-ghost btn-circle",
                ),
                user_menu,
                cls="flex items-center gap-2",
            ),
            cls="flex justify-between items-center px-6 py-3 bg-white shadow-sm",
        ),
        cls="w-full",
    )


def Sidebar(active_menu: str = ""):
    """侧边栏

    Args:
        active_menu: 当前激活的菜单项
    """
    menu_items = [
        {"id": "trading", "name": "实盘", "icon": "📈", "href": "/trading"},
        {"id": "data", "name": "数据", "icon": "📊", "href": "/data"},
    ]

    items = []
    for item in menu_items:
        is_active = item["id"] == active_menu
        bg_style = f"background: {PRIMARY_COLOR}; color: white;" if is_active else ""
        text_style = f"color: {PRIMARY_COLOR};" if is_active else "color: #374151;"

        items.append(
            Li(
                A(
                    Span(item["icon"], cls="mr-2"),
                    item["name"],
                    href=item["href"],
                    cls="flex items-center py-3 px-4 rounded-lg",
                    style=bg_style or text_style,
                ),
            )
        )

    return Div(
        Ul(*items, cls="menu p-4 w-56 bg-base-100 text-base-content"),
        cls="h-full bg-white shadow-sm",
    )


class MainLayout:
    """主布局类

    包含 header、sidebar 和 main content。
    """

    def __init__(
        self,
        *content,
        page_title: str = "QMT Gateway",
        active_menu: str = "",
        user: dict | None = None,
    ):
        self.content = content
        self.page_title = page_title
        self.active_menu = active_menu
        self.user = user

    def __ft__(self):
        return Html(
            Head(
                Title(self.page_title),
                *AppTheme.headers(),
            ),
            Body(
                # Header
                Header(self.user),
                # Main Content Area
                Div(
                    # Sidebar
                    Sidebar(self.active_menu),
                    # Content
                    Div(
                        *self.content,
                        cls="flex-1 p-6 overflow-auto",
                    ),
                    cls="flex flex-1 overflow-hidden",
                ),
                cls="min-h-screen flex flex-col bg-gray-50",
            ),
        )


def create_main_page(
    *content,
    page_title: str = "QMT Gateway",
    active_menu: str = "",
    user: dict | None = None,
):
    """创建主页面

    Args:
        content: 页面内容
        page_title: 页面标题
        active_menu: 当前激活的菜单
        user: 当前用户信息

    Returns:
        FastHTML 页面
    """
    layout = MainLayout(
        *content,
        page_title=page_title,
        active_menu=active_menu,
        user=user,
    )
    return layout
