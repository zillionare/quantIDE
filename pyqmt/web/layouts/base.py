from fasthtml.common import *
from monsterui.all import *


class BaseLayout:
    """基础布局类，所有布局都继承自此类"""

    def __init__(self, title: str = "PyQMT系统"):
        self.title = title

    def render(self):
        """渲染页面，子类需要重写此方法"""
        raise NotImplementedError("子类必须实现render方法")

    def main_block(self):
        """主要内容块，子类需要重写此方法"""
        raise NotImplementedError("子类必须实现main_block方法")
