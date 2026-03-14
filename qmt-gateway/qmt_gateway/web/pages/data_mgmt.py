"""数据管理界面

板块类别展示和板块列表管理。
"""

from fasthtml.common import *
from monsterui.all import *

from qmt_gateway.web.layouts.main import create_main_page
from qmt_gateway.web.theme import PRIMARY_COLOR


def SectorTypeList(selected_type: str = ""):
    """板块类别列表"""
    types = [
        {"id": "index", "name": "指数", "icon": "📊"},
        {"id": "concept", "name": "概念[同花顺]", "icon": "💡"},
        {"id": "industry_sw2", "name": "SW二级行业", "icon": "🏭"},
    ]

    items = []
    for t in types:
        is_selected = t["id"] == selected_type
        bg_style = f"background: {PRIMARY_COLOR}; color: white;" if is_selected else ""
        text_style = f"color: {PRIMARY_COLOR};" if is_selected else "color: #374151;"

        items.append(
            Li(
                A(
                    Span(t["icon"], cls="mr-2"),
                    t["name"],
                    href=f"/data?type={t['id']}",
                    cls="flex items-center py-3 px-4 rounded-lg",
                    style=bg_style or text_style,
                ),
            )
        )

    return Card(
        CardHeader(H4("板块类别", cls="text-lg font-semibold")),
        CardBody(
            Ul(*items, cls="menu p-0"),
        ),
        cls="h-full",
    )


def SectorTable(sectors: list[dict] | None = None):
    """板块列表表格"""
    if sectors is None:
        sectors = []

    headers = [
        "板块名称",
        "成份股更新日期",
        "成份股个数",
        "历史行情起始日期",
        "历史行情截止日期",
    ]

    rows = []
    for sector in sectors:
        rows.append(
            Tr(
                Td(sector.get("name", "")),
                Td(sector.get("constituent_update_date", "-")),
                Td(str(sector.get("constituent_count", 0))),
                Td(sector.get("bar_start_date", "-")),
                Td(sector.get("bar_end_date", "-")),
            )
        )

    if not rows:
        rows.append(
            Tr(
                Td(
                    "请选择板块类别",
                    colspan=len(headers),
                    cls="text-center text-gray-500 py-8",
                ),
            )
        )

    return Card(
        CardHeader(H4("板块列表", cls="text-lg font-semibold")),
        CardBody(
            Div(
                Table(
                    Thead(Tr(*[Th(h, cls="text-left") for h in headers])),
                    Tbody(*rows),
                    cls="w-full",
                ),
                cls="overflow-x-auto",
            ),
        ),
        cls="h-full",
    )


def DataMgmtPage(
    selected_type: str = "",
    sectors: list[dict] | None = None,
    user: dict | None = None,
):
    """数据管理页面"""
    return create_main_page(
        Div(
            # 左侧：板块类别
            Div(SectorTypeList(selected_type), cls="w-64 mr-4"),
            # 右侧：板块列表
            Div(SectorTable(sectors), cls="flex-1"),
            cls="flex h-full",
        ),
        page_title="数据管理 - QMT Gateway",
        active_menu="data",
        user=user,
    )
