from fasthtml.common import *
from monsterui.all import *


def header_component(
    logo: str,
    brand: str,
    nav_items: list[tuple],
    user: str | None = None,
    accounts: list[dict] | None = None,
    active_account: dict | None = None,
):
    """构建主导航栏。

    Args:
        logo: Logo 地址
        brand: 品牌名称
        nav_items: 顶部导航
        user: 用户名
        accounts: 账号列表
        active_account: 当前选中账号
    """
    nav_links = []
    for title, url in (nav_items or []):
        is_active = title == "首页"
        active_cls = "text-blue-600 dark:text-blue-400 border-b-2 border-blue-600 dark:border-blue-400"
        inactive_cls = "text-gray-700 dark:text-gray-300 hover:text-blue-600 dark:hover:text-blue-400"
        nav_links.append(
            A(
                title,
                href=url,
                cls="px-4 py-2 font-medium " + (active_cls if is_active else inactive_cls),
            )
        )

    initial = (user or "U")[:1]
    user_name = user or "用户"

    accounts = accounts or []
    current = active_account or (accounts[0] if accounts else None)
    current_label = ""
    current_status = ""
    current_star = ""
    if current:
        current_label = current.get("label", "")
        current_status = "已连接" if current.get("status", False) else "未连接"
        current_star = "★" if current.get("is_live") else ""

    return Header(
        Div(
            Div(
                Div(
                    cls="h-8 w-8 rounded bg-center bg-cover",
                    style=f"background-image: url('{logo}')",
                    role="img",
                    aria_label=brand,
                ),
                Span(brand, cls="text-xl font-bold text-gray-900 dark:text-white"),
                cls="flex items-center space-x-3",
            ),
            Div(
                Nav(*nav_links, cls="flex items-center space-x-1"),
                Div(cls="h-6 w-px bg-gray-300 dark:bg-gray-600"),
                Button(
                    Svg(
                        Path(
                            d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9",
                            **{
                                "stroke-linecap": "round",
                                "stroke-linejoin": "round",
                                "stroke-width": "2",
                            },
                        ),
                        cls="w-6 h-6",
                        fill="none",
                        stroke="currentColor",
                        viewBox="0 0 24 24",
                    ),
                    Span(cls="absolute top-1 right-1 h-2 w-2 bg-red-500 rounded-full"),
                    cls="relative p-2 text-gray-600 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg",
                    type="button",
                ),
                Button(
                    Svg(
                        Path(
                            d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z",
                            **{
                                "stroke-linecap": "round",
                                "stroke-linejoin": "round",
                                "stroke-width": "2",
                            },
                        ),
                        cls="w-6 h-6",
                        fill="none",
                        stroke="currentColor",
                        viewBox="0 0 24 24",
                    ),
                    cls="p-2 text-gray-600 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg",
                    type="button",
                ),
                Div(
                    Div(
                        Button(
                            Div(
                                Span(initial, cls="h-8 w-8 rounded-full bg-blue-500 flex items-center justify-center text-white font-medium"),
                                Span(user_name, cls="text-sm text-gray-700 dark:text-gray-300"),
                                Svg(
                                    Path(
                                        d="M19 9l-7 7-7-7",
                                        **{
                                            "stroke-linecap": "round",
                                            "stroke-linejoin": "round",
                                            "stroke-width": "2",
                                        },
                                    ),
                                    cls="w-4 h-4 text-gray-500",
                                    fill="none",
                                    stroke="currentColor",
                                    viewBox="0 0 24 24",
                                ),
                                cls="flex items-center space-x-2",
                            ),
                            cls="flex items-center space-x-2 px-3 py-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700",
                            onclick="toggleUserMenu()",
                            type="button",
                        ),
                        Div(
                            Div(
                                Div("当前账号", cls="text-xs text-gray-500 mb-2"),
                                Div(
                                    Div(
                                        Span(current_star, cls="text-yellow-500"),
                                        Span(
                                            f"{current.get('name', '暂无账号')}({current_label})"
                                            if current
                                            else "暂无账号",
                                            cls="text-sm font-medium text-gray-900 dark:text-white",
                                        ),
                                        cls="flex items-center space-x-2",
                                    ),
                                    Span(current_status, cls="text-xs text-green-500"),
                                    cls="flex items-center justify-between",
                                ),
                                cls="p-3 border-b border-gray-200 dark:border-gray-700",
                            ),
                            Div(
                                Div("切换账号", cls="text-xs text-gray-500 px-2 py-1"),
                                *[
                                    A(
                                        Span(acc.get("name", acc.get("id", "")), cls="text-sm text-gray-700 dark:text-gray-300"),
                                        Span(acc.get("label", ""), cls="text-xs text-gray-400"),
                                        href=acc.get("switch_url", "#"),
                                        cls="w-full flex items-center justify-between px-2 py-2 text-left hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg",
                                    )
                                    for acc in accounts
                                    if current is None or acc.get("id") != current.get("id") or acc.get("kind") != current.get("kind")
                                ],
                                cls="p-2 border-b border-gray-200 dark:border-gray-700",
                            ),
                            Div(
                                A(
                                    "账号管理",
                                    href="/system/accounts",
                                    cls="block px-2 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg",
                                ),
                                A(
                                    "退出登录",
                                    href="/auth/logout",
                                    cls="block px-2 py-2 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg",
                                ),
                                cls="p-2",
                            ),
                            id="user-dropdown",
                            cls="hidden absolute right-0 mt-2 w-72 bg-white dark:bg-gray-800 rounded-lg shadow-lg border border-gray-200 dark:border-gray-700 z-50",
                        ),
                        id="user-menu",
                        cls="relative",
                    ),
                    cls="flex items-center",
                ),
                cls="flex items-center space-x-6",
            ),
            cls="h-full flex items-center justify-between px-4",
        ),
        Script(
            "function toggleUserMenu(){const d=document.getElementById('user-dropdown');if(d){d.classList.toggle('hidden');}}"
            "document.addEventListener('click',function(e){const m=document.getElementById('user-menu');const d=document.getElementById('user-dropdown');if(m&&d&&!m.contains(e.target)){d.classList.add('hidden');}});"
        ),
        cls="bg-white dark:bg-gray-800 shadow-sm border-b border-gray-200 dark:border-gray-700 h-16 sticky top-0 z-50",
    )
