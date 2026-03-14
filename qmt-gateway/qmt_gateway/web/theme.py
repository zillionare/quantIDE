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


def PrimaryButton(text, cls="", **kwargs):
    """主色调按钮"""
    base_cls = "btn px-6 py-2 rounded"
    if cls:
        base_cls += f" {cls}"
    return Button(
        text,
        cls=base_cls,
        style=f"background: {PRIMARY_COLOR}; color: white; border: none;",
        **kwargs,
    )


def SecondaryButton(text, cls="", **kwargs):
    """次要按钮"""
    base_cls = "btn px-6 py-2 rounded"
    if cls:
        base_cls += f" {cls}"
    return Button(
        text,
        cls=base_cls,
        style="background: #f3f4f6; color: #374151; border: 1px solid #d1d5db;",
        **kwargs,
    )


def PrimaryTitle(text, **kwargs):
    """主色调标题"""
    return H3(text, style=f"color: {PRIMARY_COLOR};", **kwargs)


def PrimarySubtitle(text, **kwargs):
    """主色调副标题"""
    return H4(text, style=f"color: {PRIMARY_COLOR};", **kwargs)
