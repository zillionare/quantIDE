"""板块列表组件"""

from fasthtml.common import *


class SectorList:
    """板块列表组件"""

    def __init__(self, sectors: list[dict] = None, selected_id: str = None):
        self.sectors = sectors or []
        self.selected_id = selected_id

    def render(self) -> FT:
        """渲染板块列表"""
        if not self.sectors:
            return Div(
                P("暂无板块数据", cls="text-gray-500 text-center py-4"),
                cls="bg-white rounded-lg shadow p-4",
            )

        rows = []
        for sector in self.sectors:
            is_selected = sector.get("id") == self.selected_id
            row_cls = (
                "flex items-center justify-between p-3 cursor-pointer rounded "
                + ("bg-blue-50 border-blue-200" if is_selected else "hover:bg-gray-50 border-transparent")
                + " border"
            )

            sector_type_colors = {
                "custom": "bg-gray-100 text-gray-600",
                "industry": "bg-blue-100 text-blue-600",
                "concept": "bg-purple-100 text-purple-600",
            }
            type_cls = sector_type_colors.get(sector.get("sector_type"), "bg-gray-100 text-gray-600")

            rows.append(
                Div(
                    Div(
                        Div(
                            sector.get("name", ""),
                            cls="font-medium text-gray-900",
                        ),
                        Div(
                            sector.get("sector_type", ""),
                            cls=f"text-xs px-2 py-0.5 rounded {type_cls}",
                        ),
                        cls="flex items-center gap-2",
                    ),
                    Div(
                        f"{sector.get('stock_count', 0)}只",
                        cls="text-sm text-gray-500",
                    ),
                    cls=row_cls,
                    onclick=f"selectSector('{sector.get('id')}')",
                    data_sector_id=sector.get("id"),
                )
            )

        return Div(
            Div(
                Input(
                    type="text",
                    placeholder="搜索板块...",
                    cls="w-full px-3 py-2 border rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500",
                    oninput="filterSectors(this.value)",
                ),
                cls="mb-4",
            ),
            Div(*rows, cls="space-y-2 max-h-96 overflow-y-auto"),
            cls="bg-white rounded-lg shadow p-4",
        )

    @staticmethod
    def toolbar() -> FT:
        """渲染工具栏"""
        return Div(
            Button(
                "+ 新建板块",
                cls="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm",
                onclick="showCreateSectorModal()",
            ),
            Button(
                "刷新",
                cls="px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 text-sm",
                onclick="refreshSectors()",
            ),
            cls="flex gap-2 mb-4",
        )
