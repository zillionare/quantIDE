"""股票列表组件"""

from fasthtml.common import *


class StockList:
    """股票列表组件"""

    def __init__(self, stocks: list[dict] = None, selected_symbol: str = None):
        self.stocks = stocks or []
        self.selected_symbol = selected_symbol

    def render(self) -> FT:
        """渲染股票列表"""
        if not self.stocks:
            return Div(
                P("暂无股票数据", cls="text-gray-500 text-center py-4"),
                cls="bg-white rounded-lg shadow p-4",
            )

        # 表头
        header = Div(
            Div("代码", cls="font-medium text-gray-600"),
            Div("名称", cls="font-medium text-gray-600"),
            Div("操作", cls="font-medium text-gray-600 text-right"),
            cls="grid grid-cols-3 gap-2 px-3 py-2 bg-gray-50 rounded-t-lg text-sm",
        )

        # 行
        rows = []
        for stock in self.stocks:
            is_selected = stock.get("symbol") == self.selected_symbol
            row_cls = (
                "grid grid-cols-3 gap-2 px-3 py-2 cursor-pointer text-sm "
                + ("bg-blue-50" if is_selected else "hover:bg-gray-50")
            )

            rows.append(
                Div(
                    Div(stock.get("symbol", ""), cls="font-mono text-gray-900"),
                    Div(stock.get("name", ""), cls="text-gray-700"),
                    Div(
                        Button(
                            "查看",
                            cls="text-blue-600 hover:text-blue-800 text-xs",
                            onclick=f"viewStock('{stock.get('symbol')}')",
                        ),
                        cls="text-right",
                    ),
                    cls=row_cls,
                    onclick=f"selectStock('{stock.get('symbol')}')",
                    data_symbol=stock.get("symbol"),
                )
            )

        return Div(
            header,
            Div(*rows, cls="divide-y divide-gray-100 max-h-96 overflow-y-auto"),
            cls="bg-white rounded-lg shadow",
        )

    @staticmethod
    def toolbar(sector_name: str = "") -> FT:
        """渲染工具栏"""
        return Div(
            Div(
                f"成分股 ({sector_name})" if sector_name else "成分股",
                cls="font-semibold text-gray-800",
            ),
            Div(
                Button(
                    "+ 添加",
                    cls="px-3 py-1 bg-blue-600 text-white rounded text-sm hover:bg-blue-700",
                    onclick="showAddStockModal()",
                ),
                Button(
                    "导入",
                    cls="px-3 py-1 bg-gray-100 text-gray-700 rounded text-sm hover:bg-gray-200",
                    onclick="showImportModal()",
                ),
                cls="flex gap-2",
            ),
            cls="flex justify-between items-center mb-4",
        )
