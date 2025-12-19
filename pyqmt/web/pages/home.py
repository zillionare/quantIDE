from fasthtml.common import *
from pyqmt.web.layouts.main import MainLayout

home_app, rt = fast_app()

def main_block():
    return Div(
            H2("仪表板", cls="text-2xl font-bold mb-6"),
            Div(
                Div(
                    H3("欢迎使用PyQMT系统", cls="text-xl font-semibold"),
                    P("这是系统的主仪表板页面。", cls="text-gray-600 mt-2"),
                    P("您可以在这里查看系统状态、市场数据和交易信息。", cls="text-gray-600 mt-2"),
                )
            )
    )
@rt("/", methods="get")
def index(session):
    """home page"""
    # 创建布局实例
    layout = MainLayout(
        title="交易",
        user=session.get("auth"),
    )
    
    layout.main_block = main_block()
    
    return layout.render()
