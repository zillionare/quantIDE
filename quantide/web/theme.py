"""主题配置模块

统一管理应用的主题颜色和样式。
"""

from fasthtml.common import *


# 主色调
PRIMARY_COLOR = "#D13527"
SECONDARY_COLOR = "#f3f4f6"
TEXT_COLOR = "#374151"
BORDER_COLOR = "#d1d5db"


class AppTheme:
    """应用主题配置类"""

    @staticmethod
    def headers():
        """获取主题 headers（CSS 和脚本）"""
        # 使用 DaisyUI 的 red 主题作为基础，然后通过自定义 CSS 覆盖
        return [
            # Tailwind CSS
            Script(src="https://cdn.tailwindcss.com"),
            # DaisyUI
            Link(
                rel="stylesheet",
                href="https://cdn.jsdelivr.net/npm/daisyui@4.12.10/dist/full.min.css",
            ),
            # 自定义主题 CSS
            Style(f"""
                :root {{
                    --p: 4 90% 58%;  /* primary color in HSL: #D13527 approx */
                    --pf: 4 90% 48%; /* primary focus */
                    --pc: 0 0% 100%; /* primary content */
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
            """),
            # HTMX
            Script(src="https://unpkg.com/htmx.org@1.9.12"),
        ]
