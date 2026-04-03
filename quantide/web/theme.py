"""主题配置模块

统一管理应用的主题颜色和样式。
"""

from fasthtml.common import *
from monsterui.all import Theme


# 主色调
PRIMARY_COLOR = "#e41815"
SECONDARY_COLOR = "#f3f4f6"
TEXT_COLOR = "#2c3030"
BORDER_COLOR = "#d1d5db"


class AppTheme:
    """应用主题配置类"""

    @staticmethod
    def headers():
        """获取主题 headers（CSS 和脚本）"""
        # 使用 monsterui 的 Theme 获取基础 headers（包含 Tailwind, FrankenUI, DaisyUI）
        base_headers = Theme.red.headers()
        
        # 添加自定义主题 CSS
        custom_css = Style(f"""
            :root {{
                --p: 1 83% 49%;
                --pf: 1 83% 42%;
                --pc: 0 0% 100%; /* primary content */
            }}
            body {{
                background-color: #f5f5f5;
                color: {TEXT_COLOR};
            }}
            .btn-primary {{
                background-color: {PRIMARY_COLOR} !important;
                border-color: {PRIMARY_COLOR} !important;
            }}
            .text-primary {{
                color: {PRIMARY_COLOR} !important;
            }}
            .bg-primary {{
                background-color: {PRIMARY_COLOR} !important;
            }}
            .border-primary {{
                border-color: {PRIMARY_COLOR} !important;
            }}
            .quantide-surface {{
                background: #ffffff;
                border: 1px solid rgba(44, 48, 48, 0.08);
                box-shadow: 0 18px 45px rgba(44, 48, 48, 0.08);
            }}
        """)
        
        # 添加 HTMX
        htmx_script = Script(src="https://unpkg.com/htmx.org@1.9.12")
        
        return list(base_headers) + [custom_css, htmx_script]
