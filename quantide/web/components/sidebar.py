"""Sidebar 组件。"""

from fasthtml.common import *
from monsterui.all import *


def sidebar_component(menu_items: list[dict] | None = None):
    """渲染侧边栏一级菜单。

    Args:
        menu_items: 菜单列表
    """
    items = []

    base_text_cls = "text-[#2c3030]"
    hover_cls = "hover:bg-[#fcfcfc]"
    active_cls = "text-[#e41815] font-medium"

    for item in menu_items or []:
        children = item.get("children", [])
        has_children = bool(children)

        item_active = item.get("active", False)
        any_child_active = any(child.get("active", False) for child in children)
        is_open = item_active or any_child_active

        icon_path = item.get("icon_path")
        icon_name = item.get("icon", "folder" if has_children else "file")
        
        if icon_path:
            # 使用自定义 SVG path
            icon_cmp = Svg(
                Path(d=icon_path, stroke="currentColor", fill="none", stroke_width="2", stroke_linecap="round", stroke_linejoin="round"),
                cls="w-5 h-5"
            )
        else:
            icon_cmp = UkIcon(icon_name, cls="w-5 h-5")

        if has_children:
            child_items = []
            for child in children:
                child_active = child.get("active", False)
                c_cls = "block px-3 py-2 rounded-lg text-sm transition-colors "
                if child_active:
                    c_cls += f"bg-[#fcfcfc] {active_cls}"
                else:
                    c_cls += f"{base_text_cls} {hover_cls}"
                
                # 支持子菜单项的 icon_path
                child_icon_path = child.get("icon_path")
                child_icon_name = child.get("icon", "file")
                
                if child_icon_path:
                    child_icon = Svg(
                        Path(d=child_icon_path, stroke="currentColor", fill="none", stroke_width="2", stroke_linecap="round", stroke_linejoin="round"),
                        cls="w-4 h-4 mr-2"
                    )
                else:
                    child_icon = UkIcon(child_icon_name, cls="w-4 h-4 mr-2")

                child_items.append(
                    A(
                        Div(
                            child_icon,
                            Span(child.get("title", "")),
                            cls="flex items-center gap-2"
                        ),
                        href=child.get("url", "#"),
                        cls=c_cls,
                    )
                )

            s_cls = "flex items-center justify-between px-3 py-2 rounded-lg cursor-pointer list-none [&::-webkit-details-marker]:hidden transition-colors"
            s_cls += f" {active_cls if item_active and not any_child_active else base_text_cls} {hover_cls}"

            items.append(
                Details(
                    Summary(
                        Div(
                            icon_cmp,
                            Span(item.get("title", "")),
                            cls="flex items-center gap-3"
                        ),
                        UkIcon("chevron-down", cls="w-4 h-4 transition-transform duration-200 group-open:-rotate-180"),
                        cls=s_cls
                    ),
                    Div(*child_items, cls="pl-9 pr-3 py-1 space-y-1"),
                    cls="group",
                    open=is_open
                )
            )
        else:
            a_cls = "w-full flex items-center px-3 py-2 rounded-lg transition-colors "
            if item_active:
                a_cls += f"bg-[#fcfcfc] {active_cls}"
            else:
                a_cls += f"{base_text_cls} {hover_cls}"

            items.append(
                A(
                    Div(
                        icon_cmp,
                        Span(item.get("title", "")),
                        cls="flex items-center gap-3",
                    ),
                    href=item.get("url", "#"),
                    cls=a_cls,
                )
            )

    return Aside(
        Nav(Div(*items, cls="p-4 space-y-1")),
        cls="w-64 bg-white min-h-screen border-r border-gray-200"
    )
