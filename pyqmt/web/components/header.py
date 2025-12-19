from fasthtml.common import *
from monsterui.all import *

def header_component(logo: str, brand:str, nav_items: list[tuple], user: str|None = None):
    """创建包含 logo, brand, 导航栏和用户菜单的 Header 组件。

    Args:
        logo: path/url of the logo
        brand: brand name
        nav_items: list of tuples (title, url) for navigation items
        user: 当前登录用户信息，用于显示欢迎消息和退出链接

    Returns:
        A Header component with navigation
    """
    # 创建导航项
    nav_links = []
    for title, url in nav_items:
        nav_links.append(
            A(
                title,
                href=url,
                cls="text-white hover:text-gray-300 px-3 py-2 rounded-md text-sm font-medium"
            )
        )
    
    # 用户菜单
    user_menu = Div() if not user else Div(
        P(f"欢迎, {user}", cls="text-white mr-4"),
        A("退出", href="/auth/logout", cls="text-white hover:text-gray-300")
    )
    
    return Header(
        Div(
            # Logo 和品牌名称
            A(
                Img(src=logo, alt=brand, cls="h-8 w-auto"),
                Span(brand, cls="text-white text-xl font-bold ml-2"),
                href="/home",
                cls="flex items-center"
            ),
            
            # 导航菜单
            Nav(
                *nav_links,
                cls="hidden md:flex space-x-4"
            ),
            
            # 用户菜单
            user_menu,
            
            cls="container mx-auto flex items-center justify-between px-4 py-3"
        ),
        cls="bg-blue-600 shadow-md"
    )
