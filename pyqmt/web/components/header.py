from fasthtml.common import *
from monsterui.all import *


def header_component(
    logo: str, brand: str, nav_items: list[tuple], user: str | None = None
):
    nav_links = []
    for title, url in reversed(nav_items or []):
        nav_links.append(
            A(
                title,
                href=url,
                cls="text-white hover:text-gray-200 px-3 py-2 text-sm font-medium",
            )
        )
    user_menu = (
        Div()
        if not user
        else Div(
            Div(
                Span("用户", cls="text-white text-sm"),
                A("退出", href="/auth/logout", cls="text-white text-xs ml-3"),
                cls="flex items-center gap-2",
            ),
            cls="flex items-center",
        )
    )
    return Header(
        Div(
            Div(
                A(Img(src=logo, alt=brand, cls="h-8 w-8 rounded"), href="/home"),
                cls="flex items-center",
            ),
            Div(
                Span(brand, cls="text-white text-xl font-bold"),
                cls="flex justify-center",
            ),
            Div(
                Nav(*nav_links, cls="hidden md:flex items-center gap-4"),
                user_menu,
                cls="flex items-center gap-4 justify-end",
            ),
            cls="max-w-[1280px] mx-auto grid grid-cols-3 items-center px-4 py-3",
        ),
        cls="bg-blue-600 shadow-md",
    )
