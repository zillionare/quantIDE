"""Sidebar 组件。"""

from fasthtml.common import *
from monsterui.all import *


def sidebar_component(menu_items: list[dict] | None = None):
    """渲染侧边栏一级菜单。

    Args:
        menu_items: 菜单列表
    """
    items = []
    for item in menu_items or []:
        is_active = item.get("active", False)
        base_cls = "w-full flex items-center justify-between px-3 py-2 rounded-lg"
        if is_active:
            base_cls += " bg-gray-100 text-blue-600"
        else:
            base_cls += " text-gray-700 hover:bg-gray-100"

        icon = None
        icon_path = item.get("icon_path")
        if icon_path:
            icon = Svg(
                Path(
                    d=icon_path,
                    **{
                        "stroke-linecap": "round",
                        "stroke-linejoin": "round",
                        "stroke-width": "2",
                    },
                ),
                cls="w-5 h-5",
                fill="none",
                stroke="currentColor",
                viewBox="0 0 24 24",
            )

        items.append(
            A(
                Div(
                    icon,
                    Span(item.get("title", "")),
                    cls="flex items-center space-x-3",
                ),
                href=item.get("url", "#"),
                cls=base_cls,
            )
        )
        children = item.get("children", [])
        if children:
            child_items = []
            for child in children:
                child_active = child.get("active", False)
                child_cls = "block px-3 py-2 rounded-lg text-sm"
                if child_active:
                    child_cls += " bg-blue-50 text-blue-600"
                else:
                    child_cls += " text-gray-600 hover:bg-gray-100"
                child_items.append(
                    A(
                        child.get("title", ""),
                        href=child.get("url", "#"),
                        cls=child_cls,
                    )
                )
            items.append(Div(*child_items, cls="pl-9 space-y-1"))

    return Aside(
        Nav(Div(*items, cls="p-4 space-y-1")),
        cls="w-64 bg-white min-h-screen border-r border-gray-200",
    )
