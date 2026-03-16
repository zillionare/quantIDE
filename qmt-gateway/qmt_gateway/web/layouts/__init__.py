"""布局模块

提供页面布局组件。
"""

from qmt_gateway.web.layouts.base import BaseLayout, create_base_page
from qmt_gateway.web.layouts.main import MainLayout, create_main_page, Header

__all__ = [
    "BaseLayout",
    "create_base_page",
    "MainLayout",
    "create_main_page",
    "Header",
]
