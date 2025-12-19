from fasthtml.common import *
from pyqmt.web.layouts.main import MainLayout

backtest_results_app, rt = fast_app()

@rt("/backtest/results", methods="get")
def index(req, auth):
    """Backtest Results page"""
    # 定义菜单项
    menu_items = [
        {"title": "仪表板", "url": "/home", "icon": "home", "section": "dashboard"},
        {"title": "回测", "url": "#", "icon": "activity", "children": [
            {"title": "策略列表", "url": "/backtest/strategies", "section": "strategies"},
            {"title": "回测结果", "url": "/backtest/results", "section": "results"}
        ]},
        {"title": "设置", "url": "/settings", "icon": "settings", "section": "settings"}
    ]
    
    # 创建布局实例
    layout = MainLayout(
        title="回测结果",
        user=auth,
        menu_items=menu_items,
        active_section="results"
    )
    
    # 重写main_block方法以添加特定内容
    def custom_main_block():
        return Div(
            H2("回测结果", cls="text-2xl font-bold mb-6"),
            Div(
                Div(
                    H3("历史回测", cls="text-xl font-semibold"),
                    P("在这里您可以查看历史回测的结果和性能指标。", cls="text-gray-600 mt-2"),
                    cls="bg-white p-6 rounded-lg shadow"
                ),
                Div(
                    H3("最近回测结果", cls="text-xl font-semibold mt-6 mb-4"),
                    Div(
                        Table(
                            Thead(
                                Tr(
                                    Th("策略名称"),
                                    Th("回测时间"),
                                    Th("收益率"),
                                    Th("最大回撤"),
                                    Th("夏普比率")
                                )
                            ),
                            Tbody(
                                Tr(
                                    Td("均线策略"),
                                    Td("2023-05-15"),
                                    Td("+15.2%"),
                                    Td("-8.3%"),
                                    Td("1.25")
                                ),
                                Tr(
                                    Td("动量策略"),
                                    Td("2023-05-10"),
                                    Td("+22.7%"),
                                    Td("-12.1%"),
                                    Td("1.42")
                                ),
                                Tr(
                                    Td("套利策略"),
                                    Td("2023-05-05"),
                                    Td("+8.9%"),
                                    Td("-5.2%"),
                                    Td("1.08")
                                )
                            ),
                            cls="min-w-full"
                        ),
                        cls="bg-white p-6 rounded-lg shadow overflow-x-auto"
                    ),
                    cls="mt-4"
                ),
                cls="w-full"
            ),
            cls="w-full"
        )
    
    # 替换布局的main_block方法
    layout.main_block = custom_main_block
    
    return layout.render()
