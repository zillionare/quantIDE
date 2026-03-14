"""基础布局模块

提供基础页面布局，用于不需要侧边栏的简单页面。
"""

from fasthtml.common import *
from monsterui.all import *

from qmt_gateway.web.theme import AppTheme


class BaseLayout:
    """基础布局类

    用于登录页、初始化向导等简单页面。
    """

    def __init__(self, *content, page_title: str = "QMT Gateway"):
        self.content = content
        self.page_title = page_title

    def __ft__(self):
        return Title(self.page_title), Container(
            *self.content,
            cls="min-h-screen bg-gray-50 dark:bg-gray-900 py-8",
        )


def create_base_page(*content, page_title: str = "QMT Gateway"):
    """创建基础页面

    Args:
        content: 页面内容
        page_title: 页面标题

    Returns:
        FastHTML 页面
    """
    return Html(
        Head(
            Title(page_title),
            *AppTheme.headers(),
        ),
        Body(
            Container(
                *content,
                cls="min-h-screen bg-gray-50 py-8",
            ),
        ),
    )
