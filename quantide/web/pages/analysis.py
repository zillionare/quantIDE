"""Analysis page.

The subject application no longer exposes sector or index analysis management
as a published feature.
"""

from fasthtml.common import Div, H2, P, Title, to_xml
from starlette.requests import Request
from starlette.responses import HTMLResponse

from quantide.web.components.header import header_component


def analysis_page(request: Request):
    """Render the retired analysis page placeholder."""
    session = request.scope.get("session", {})
    user = session.get("auth")

    header_menu = [
        ("首页", "/"),
        ("交易", "/trade"),
        ("行情", "/system/stocks"),
        ("策略", "/strategy"),
        ("分析", "/analysis"),
    ]

    body = Div(
        Div(
            H2("板块与指数分析已下线", cls="text-2xl font-semibold mb-4"),
            P(
                "主体工程已不再提供板块/指数同步、维护和分析入口。",
                cls="text-gray-700 mb-3",
            ),
            P(
                "发布态仅保留股票行情、策略、仿真与 gateway 实盘链路。",
                cls="text-gray-700 mb-3",
            ),
            P(
                "如需保留历史板块数据，请直接访问数据库或离线导出结果。",
                cls="text-sm text-gray-500",
            ),
            cls="max-w-2xl mx-auto mt-16 bg-white border rounded-xl p-8 shadow-sm",
        ),
        cls="flex-1 bg-gray-50 min-h-screen",
    )

    page_content = Div(
        header_component(
            logo="/static/logo.png",
            brand="匡醍",
            nav_items=header_menu,
            user=user,
            accounts=[],
            active_account=None,
            active_title="分析",
        ),
        body,
        cls="min-h-screen bg-gray-50",
    )

    from quantide.web.theme import AppTheme

    html_content = to_xml((
        Title("分析"),
        *AppTheme.headers(),
        page_content,
    ))
    return HTMLResponse(html_content)


async def analysis_handler(request: Request):
    """HTTP handler for the retired analysis page."""
    return analysis_page(request)