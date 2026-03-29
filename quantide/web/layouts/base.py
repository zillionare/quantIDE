from fasthtml.common import *
from monsterui.all import *


class BaseLayout:
    """基础布局类

    用于不需要侧边栏的简单页面，如登录页、初始化向导等。
    """

    def __init__(self, *content, page_title: str = "Quantide"):
        """初始化基础布局

        Args:
            content: 页面主要内容
            page_title: 页面标题
        """
        self.content = content
        self.page_title = page_title

    def __ft__(self):
        """渲染页面"""
        return Title(self.page_title), Container(
            *self.content,
            cls="min-h-screen bg-gray-50 dark:bg-gray-900 py-8",
        )
