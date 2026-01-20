from fasthtml.common import *
from monsterui.all import *


def sidebar_component(menu_items: list[dict] | None = None):
    sections = []
    for i, item in enumerate(menu_items or []):
        header_cls = "bg-gray-200 text-gray-800 font-bold rounded px-3 py-2"
        children = []
        for child in item.get("children", []):
            children.append(
                Li(
                    A(
                        child["title"],
                        href=child["url"],
                        cls="text-gray-700 hover:text-blue-600",
                    ),
                    cls="py-1",
                )
            )
        sections.append(
            Div(
                P(item["title"], cls=header_cls),
                Ul(*children, cls="mt-2 space-y-1"),
                cls="mb-4",
            )
        )
    return Aside(
        Nav(Div(*sections, cls="p-4")),
        cls="w-64 bg-gray-100 min-h-screen border-r border-gray-200",
    )
