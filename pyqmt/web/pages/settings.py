from fasthtml.common import *
from pyqmt.web.layouts.main import MainLayout

settings_app, rt = fast_app()

@rt("/settings", methods="get")
def index(req, auth):
    """Settings page"""
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
        title="系统设置",
        user=auth,
        menu_items=menu_items,
        active_section="settings"
    )
    
    # 重写main_block方法以添加特定内容
    def custom_main_block():
        return Div(
            H2("系统设置", cls="text-2xl font-bold mb-6"),
            Div(
                Div(
                    H3("账户设置", cls="text-xl font-semibold"),
                    P("在这里您可以修改您的账户设置和偏好。", cls="text-gray-600 mt-2"),
                    cls="bg-white p-6 rounded-lg shadow"
                ),
                Div(
                    H3("通知设置", cls="text-xl font-semibold mt-6 mb-4"),
                    Div(
                        P("暂无设置选项。", cls="text-gray-500 italic"),
                        cls="bg-white p-6 rounded-lg shadow"
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
