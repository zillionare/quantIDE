from fasthtml.common import *
from monsterui.all import *


def header_component(
    logo: str, brand: str, nav_items: list[tuple], user: str | None = None
):
    nav_links = []
    # nav_items is passed as list of (title, url)
    # But wait, logic below seems wrong if nav_items is [("交易", "/trade/simulation"), ...]
    # The reversed loop logic is strange if it was building a right-aligned menu, but let's check structure.

    # Original code:
    # for title, url in reversed(nav_items or []):
    #    nav_links.append(A(title, href=url, ...))

    # If we want them in order left-to-right, we should iterate normally.
    for title, url in (nav_items or []):
         nav_links.append(
            A(
                title,
                href=url,
                cls="text-white hover:text-white/80 px-3 py-2 text-sm font-medium transition-colors",
            )
        )

    user_menu = (
        Div(cls="hidden") # Return empty div if no user
        if not user
        else Div(
             Span(f"User: {user}", cls="text-white/80 text-xs mr-2"),
             A("Logout", href="/auth/logout", cls="btn btn-xs btn-ghost text-white"),
             cls="flex items-center"
        )
    )

    # Simplified header structure using MonsterUI/DaisyUI classes
    return NavBar(
        # Brand/Logo area
        A(
            Img(src=logo, alt=brand, cls="h-8 w-8 mr-2 rounded-md"),
            Span(brand, cls="text-xl font-bold tracking-tight"),
            href="/",
            cls="btn btn-ghost text-white normal-case text-xl px-2 flex items-center"
        ),

        # Navigation Links (Center/Left)
        Div(
            *nav_links,
            cls="flex items-center gap-1 mx-4"
        ),

        # Right side (User menu)
        Div(
            user_menu,
            cls="ml-auto"
        ),
        cls="bg-blue-600 text-white shadow-lg px-4 flex items-center"
    )
