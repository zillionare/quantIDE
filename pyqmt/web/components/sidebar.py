from fasthtml.common import *
from monsterui.all import *


def sidebar_component(
    menu_items: list[dict] | None = None,
):
    """创建侧边栏导航菜单

    Args:
        menu_items: 侧边栏的菜单项。
        active_section: 当前激活的菜单项，用于高亮显示。

    Returns:
        A Sidebar component with navigation
    """
    # 构建菜单项
    menu_links = []
    for i, item in enumerate(menu_items or []):
        # 检查是否有子菜单
        if "children" in item and item["children"]:
            # 创建带有子菜单的项
            children_links = []
            for j, child in enumerate(item["children"]):
                child_active = "font-bold" if j == 0 else ""
                children_links.append(
                    Li(
                        A(
                            child["title"],
                            href=child["url"],
                            cls=f"text-gray-600 hover:text-blue-600 {child_active}",
                        ),
                        cls="ml-4 py-1",
                    )
                )

            # 父级菜单项
            parent_active = "font-bold" if i == 0 else ""
            menu_links.append(
                Li(
                    A(
                        item["title"],
                        href=item["url"],
                        cls=f"text-gray-800 hover:text-blue-600 {parent_active}",
                    ),
                    Ul(*children_links, cls="pl-4 mt-1"),
                    cls="py-2",
                )
            )
        else:
            # 普通菜单项
            active_class = "font-bold" if i == 0 else ""
            menu_links.append(
                Li(
                    A(
                        item["title"],
                        href=item["url"],
                        cls=f"text-gray-800 hover:text-blue-600 {active_class}",
                    ),
                    cls="py-2",
                )
            )

    return Aside(
        Nav(Ul(*menu_links, cls="space-y-1"), cls="p-4"),
        cls="w-64 bg-gray-100 min-h-screen border-r border-gray-200",
    )
