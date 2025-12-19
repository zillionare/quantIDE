from fasthtml.common import *
from pyqmt.web.layouts.main import MainLayout

home_app, rt = fast_app()

@rt("/", methods="get")
def index(req, auth):
    """home page"""
    # 定义菜单项

    
    # 创建布局实例
    layout = MainLayout(
        title="仪表板",
        user=auth
    )
    
    # 重写main_block方法以添加特定内容
    def custom_main_block():
        return Div(
            H2("仪表板", cls="text-2xl font-bold mb-6"),
            Div(
                Div(
                    H3("欢迎使用PyQMT系统", cls="text-xl font-semibold"),
                    P("这是系统的主仪表板页面。", cls="text-gray-600 mt-2"),
                    P("您可以在这里查看系统状态、市场数据和交易信息。", cls="text-gray-600 mt-2"),
                    cls="bg-white p-6 rounded-lg shadow"
                ),
                Div(
                    H3("系统状态", cls="text-xl font-semibold mt-6 mb-4"),
                    Div(
                        Div(
                            P("连接状态", cls="font-medium"),
                            P("已连接", cls="text-green-600"),
                            cls="flex justify-between py-2 border-b"
                        ),
                        Div(
                            P("市场数据", cls="font-medium"),
                            P("正常", cls="text-green-600"),
                            cls="flex justify-between py-2 border-b"
                        ),
                        Div(
                            P("策略运行", cls="font-medium"),
                            P("0个运行中", cls="text-yellow-600"),
                            cls="flex justify-between py-2"
                        ),
                        cls="bg-white p-4 rounded-lg shadow"
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
