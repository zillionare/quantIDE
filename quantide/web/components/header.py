from fasthtml.common import *
from monsterui.all import *

from quantide.web.theme import PRIMARY_COLOR


def _normalize_nav_item(item):
    if isinstance(item, dict):
        return item
    title, url = item
    return {"title": title, "url": url}


def header_component(
    logo: str,
    brand: str,
    nav_items: list[tuple] | list[dict],
    user: str | None = None,
    accounts: list[dict] | None = None,
    active_account: dict | None = None,
    active_title: str = "",
    unread_count: int = 0,
):
    """构建主导航栏。"""
    nav_links = []
    for raw_item in nav_items or []:
        item = _normalize_nav_item(raw_item)
        title = str(item.get("title", ""))
        url = str(item.get("url", "#"))
        requires_gateway = bool(item.get("requires_gateway", False))
        is_active = title == active_title
        active_cls = "border-b-2 border-primary text-primary"
        inactive_cls = "text-gray-600 hover:text-gray-900 border-b-2 border-transparent"
        attrs = {}
        if requires_gateway:
            attrs = {
                "onclick": "showGatewayRequiredModal(event)",
                "aria_disabled": "true",
                "title": "请先配置交易网关",
            }
        nav_links.append(
            A(
                title,
                href=url,
                cls="inline-flex items-center px-4 py-5 text-sm font-medium transition "
                + (active_cls if is_active else inactive_cls),
                **attrs,
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

    def menu_action(label: str, href: str, icon_name: str, attrs: dict = None):
        attrs = attrs or {}
        return A(
            Div(
                UkIcon(icon_name, size=16, cls="text-primary"),
                Span(label, cls="text-sm font-medium"),
                cls="flex items-center gap-3",
            ),
            href=href,
            cls="flex items-center rounded-xl px-3 py-3 text-[#2c3030] transition hover:bg-[#fff3f2] hover:text-primary",
            **attrs
        )

    unread_badge = None
    if unread_count > 0:
        unread_badge = Span(
            "99+" if unread_count > 99 else str(unread_count),
            cls="absolute top-1 right-1 inline-flex min-w-[14px] h-[14px] items-center justify-center rounded-full bg-[#10b981] px-1 text-[9px] font-bold text-white shadow-sm border border-white",
        )

    return Header(
        Div(
            Div(
                Div(
                    cls="h-10 w-10 rounded-xl bg-gray-50 bg-center bg-contain bg-no-repeat shadow-sm",
                    style=f"background-image: url('{logo}')",
                    role="img",
                    aria_label=brand,
                ),
                Span(brand, cls="text-lg font-semibold tracking-[0.08em] text-[#e41815]"),
                cls="flex items-center gap-3",
            ),
            Div(cls="flex-1"),
            Div(
                Nav(*nav_links, cls="hidden items-center self-stretch lg:flex"),
                Button(
                    UkIcon("bell", cls="w-[18px] h-[18px]"),
                    unread_badge,
                    cls="relative inline-flex h-10 w-10 items-center justify-center rounded-full bg-gray-100 text-gray-600 transition hover:bg-gray-200 hover:text-gray-900",
                    type="button",
                    aria_label="消息中心",
                ),
                Div(
                    Div(
                        Button(
                            Div(
                                Div(
                                    Span(
                                        initial,
                                        cls="flex h-9 w-9 items-center justify-center rounded-full bg-gray-200 text-sm font-semibold uppercase text-gray-600 shadow-md",
                                    ),
                                    cls="relative",
                                ),
                                UkIcon("chevron-down", cls="w-[14px] h-[14px]"),
                                cls="flex items-center gap-2",
                            ),
                            cls="flex items-center rounded-full p-1 text-left transition hover:bg-gray-100 focus:outline-none bg-transparent border-none outline-none",
                            onclick="toggleUserMenu(event)",
                            type="button",
                            id="user-menu-button",
                            aria_haspopup="menu",
                            aria_expanded="false",
                        ),
                        Div(
                            Div(
                                cls="absolute -top-2 left-1/2 -translate-x-1/2 w-4 h-4 bg-white border-l border-t border-gray-100 rotate-45 z-50",
                            ),
                            Div(
                                menu_action("重设密码", "#", "settings", {"onclick": "showGlobalResetPasswordModal(event)"}),
                                menu_action("退出登录", "/auth/logout", "log-out"),
                                cls="p-2 relative z-10 bg-white rounded-xl",
                            ),
                            id="user-dropdown",
                            cls="quantide-surface absolute top-[calc(100%+12px)] left-1/2 -translate-x-1/2 z-50 hidden w-32 overflow-hidden rounded-xl bg-white shadow-lg border border-gray-100",
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
        Div(
            Div(
                Div(
                    UkIcon("alert-triangle", cls="w-8 h-8 text-amber-500"),
                    cls="flex justify-center",
                ),
                H3("请先配置交易网关", cls="mt-4 text-lg font-semibold text-gray-900 text-center"),
                P(
                    "当前尚未启用 gateway，因此还不能进入实盘或仿真交易。请先完成交易网关配置。",
                    cls="mt-2 text-sm leading-6 text-gray-600 text-center",
                ),
                Div(
                    A(
                        "前往交易网关",
                        href="/system/gateway/",
                        cls="inline-flex items-center justify-center rounded-lg bg-[#e41815] px-4 py-2 text-sm font-medium text-white hover:bg-[#c91412]",
                    ),
                    Button(
                        "稍后配置",
                        type="button",
                        cls="inline-flex items-center justify-center rounded-lg border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50",
                        onclick="closeGatewayRequiredModal()",
                    ),
                    cls="mt-6 flex justify-center gap-3",
                ),
                cls="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl",
            ),
            id="gateway-required-modal",
            cls="fixed inset-0 z-[60] hidden items-center justify-center bg-black/40 px-4",
            onclick="if(event.target===this){closeGatewayRequiredModal()}"
        ),
        Div(id="global-reset-password-modal-container"),
        Script(
            "function setUserMenu(open){const button=document.getElementById('user-menu-button');const dropdown=document.getElementById('user-dropdown');if(!button||!dropdown){return;}if(open){dropdown.classList.remove('hidden');button.setAttribute('aria-expanded','true');}else{dropdown.classList.add('hidden');button.setAttribute('aria-expanded','false');}}"
            "function toggleUserMenu(event){if(event){event.stopPropagation();}const dropdown=document.getElementById('user-dropdown');if(!dropdown){return;}setUserMenu(dropdown.classList.contains('hidden'));}"
            "document.addEventListener('click',function(event){const menu=document.getElementById('user-menu');if(menu&&!menu.contains(event.target)){setUserMenu(false);}});"
            "document.addEventListener('keydown',function(event){if(event.key==='Escape'){setUserMenu(false);}});"
            "function showGlobalResetPasswordModal(event){if(event){event.preventDefault(); event.stopPropagation();} setUserMenu(false); htmx.ajax('GET', '/auth/modal/reset-password', {target: '#global-reset-password-modal-container'});}"
            "function showGatewayRequiredModal(event){if(event){event.preventDefault(); event.stopPropagation();} const modal=document.getElementById('gateway-required-modal'); if(!modal){return;} modal.classList.remove('hidden'); modal.classList.add('flex');}"
            "function closeGatewayRequiredModal(){const modal=document.getElementById('gateway-required-modal'); if(!modal){return;} modal.classList.remove('flex'); modal.classList.add('hidden');}"
        ),
        cls="sticky top-0 z-50 h-16 border-b border-black/5 shadow-[0_4px_15px_rgba(0,0,0,0.1)] bg-white",
    )
