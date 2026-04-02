from fasthtml.common import *
from monsterui.all import *

from quantide.web.theme import PRIMARY_COLOR


def header_component(
    logo: str,
    brand: str,
    nav_items: list[tuple],
    user: str | None = None,
    accounts: list[dict] | None = None,
    active_account: dict | None = None,
    active_title: str = "",
    unread_count: int = 0,
):
    """构建主导航栏。"""
    nav_links = []
    for title, url in (nav_items or []):
        is_active = title == active_title
        active_cls = "border-b-2 border-white bg-white/12 text-white"
        inactive_cls = "text-white/80 hover:bg-white/10 hover:text-white"
        nav_links.append(
            A(
                title,
                href=url,
                cls="inline-flex items-center rounded-t-xl px-4 py-5 text-sm font-medium transition "
                + (active_cls if is_active else inactive_cls),
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

    account_summary = "未选择交易账号"
    if current:
        account_summary = f"{current_star}{current.get('name', '未知账号')}"
        if current_label:
            account_summary += f" · {current_label}"
        if current_status:
            account_summary += f" · {current_status}"

    def menu_action(label: str, href: str, icon_name: str):
        return A(
            Div(
                UkIcon(icon_name, size=16, cls="text-primary"),
                Span(label, cls="text-sm font-medium"),
                cls="flex items-center gap-3",
            ),
            href=href,
            cls="flex items-center rounded-xl px-3 py-3 text-[#2c3030] transition hover:bg-[#fff3f2] hover:text-primary",
        )

    unread_badge = None
    if unread_count > 0:
        unread_badge = Span(
            "99+" if unread_count > 99 else str(unread_count),
            cls="absolute -right-1 -top-1 inline-flex min-w-[1.1rem] items-center justify-center rounded-full bg-white px-1 text-[11px] font-semibold leading-4 text-primary shadow-sm",
        )

    return Header(
        Div(
            Div(
                Div(
                    cls="h-10 w-10 rounded-xl bg-white/95 bg-center bg-contain bg-no-repeat shadow-sm",
                    style=f"background-image: url('{logo}')",
                    role="img",
                    aria_label=brand,
                ),
                Span(brand, cls="text-lg font-semibold tracking-[0.08em] text-white"),
                cls="flex items-center gap-3",
            ),
            Div(cls="flex-1"),
            Div(
                Nav(*nav_links, cls="hidden items-center self-stretch lg:flex"),
                Button(
                    UkIcon("bell", size=18, cls="text-white"),
                    unread_badge,
                    cls="relative inline-flex h-10 w-10 items-center justify-center rounded-full border border-white/12 bg-white/10 text-white transition hover:bg-white/18",
                    type="button",
                    aria_label="消息中心",
                ),
                Div(
                    Div(
                        Button(
                            Div(
                                Span(user_name, cls="max-w-28 truncate text-sm font-medium text-white"),
                                Div(
                                    Span(
                                        initial,
                                        cls="flex h-9 w-9 items-center justify-center rounded-full bg-white text-sm font-semibold uppercase text-primary shadow-sm",
                                    ),
                                    Div(
                                        UkIcon("chevron-down", size=10, cls="text-primary"),
                                        cls="absolute -bottom-1 -right-1 flex h-4 w-4 items-center justify-center rounded-full bg-white shadow-sm",
                                    ),
                                    cls="relative",
                                ),
                                cls="flex items-center gap-3",
                            ),
                            cls="flex items-center rounded-full border border-white/12 bg-white/10 px-3 py-1.5 text-left transition hover:bg-white/18",
                            onclick="toggleUserMenu(event)",
                            type="button",
                            id="user-menu-button",
                            aria_haspopup="menu",
                            aria_expanded="false",
                        ),
                        Div(
                            Div(
                                Div("当前登录用户", cls="text-[11px] tracking-[0.18em] text-[#8b8b8b]"),
                                Div(user_name, cls="mt-1 text-sm font-semibold text-[#2c3030]"),
                                Div(account_summary, cls="mt-2 text-xs leading-5 text-[#6b7280]"),
                                cls="border-b border-[#f2d6d6] px-4 py-4",
                            ),
                            Div(
                                menu_action("重设密码", "/auth/profile#password-settings", "settings"),
                                menu_action("退出登录", "/auth/logout", "log-out"),
                                cls="p-2",
                            ),
                            id="user-dropdown",
                            cls="quantide-surface absolute right-0 top-[calc(100%+12px)] z-50 hidden w-64 overflow-hidden rounded-2xl bg-white",
                            role="menu",
                        ),
                        id="user-menu",
                        cls="relative",
                    ),
                    cls="flex items-center",
                ),
                cls="flex items-center gap-3",
            ),
            cls="mx-auto flex h-full max-w-[1280px] items-center gap-4 px-5",
        ),
        Script(
            "function setUserMenu(open){const button=document.getElementById('user-menu-button');const dropdown=document.getElementById('user-dropdown');if(!button||!dropdown){return;}if(open){dropdown.classList.remove('hidden');button.setAttribute('aria-expanded','true');}else{dropdown.classList.add('hidden');button.setAttribute('aria-expanded','false');}}"
            "function toggleUserMenu(event){if(event){event.stopPropagation();}const dropdown=document.getElementById('user-dropdown');if(!dropdown){return;}setUserMenu(dropdown.classList.contains('hidden'));}"
            "document.addEventListener('click',function(event){const menu=document.getElementById('user-menu');if(menu&&!menu.contains(event.target)){setUserMenu(false);}});"
            "document.addEventListener('keydown',function(event){if(event.key==='Escape'){setUserMenu(false);}});"
        ),
        cls="sticky top-0 z-50 h-16 border-b border-black/5 shadow-[0_10px_30px_rgba(120,12,12,0.18)]",
        style=f"background-color: {PRIMARY_COLOR};",
    )
